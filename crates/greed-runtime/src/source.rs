use crate::config::RuntimeConfig;
use crate::market_stream::MarketStreamHub;
use anyhow::{anyhow, Result};
use greed_kernel::{
    AccountFrame, Candle, CandleSeries, CrossVenueState, DataQuality, DerivativesState,
    InstrumentFrame, MarketFrame, MarketKind, ObservationMeta,
};
use greed_strategy::StrategyConfig;
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Default, serde::Serialize)]
struct EndpointTelemetry {
    requests: u64,
    successes: u64,
    failures: u64,
    rate_limits: u64,
    total_latency_ms: u64,
    max_latency_ms: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
struct ApiTelemetry {
    requests: u64,
    successes: u64,
    failures: u64,
    rate_limits: u64,
    retries: u64,
    fallback_attempts: u64,
    fallback_successes: u64,
    total_latency_ms: u64,
    max_latency_ms: u64,
    frames_requested: u64,
    frames_succeeded: u64,
    frames_failed: u64,
    latest_used_weight_1m: Option<u64>,
    max_used_weight_1m: u64,
    endpoints: BTreeMap<String, EndpointTelemetry>,
}

pub struct BinanceMarketSource {
    config: RuntimeConfig,
    http: Client,
    oi_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    last_success_ms: Option<i64>,
    last_error: Option<String>,
    telemetry: Mutex<ApiTelemetry>,
    discovery_prices: BTreeMap<String, VecDeque<(i64, f64)>>,
    eligible_contracts: BTreeSet<String>,
    stream: Option<MarketStreamHub>,
    candle_cache: BTreeMap<(String, String), CandleSeries>,
    derivatives_cache: BTreeMap<String, CachedValue<DerivativesState>>,
    cross_venue_cache: BTreeMap<String, CachedValue<CrossVenueState>>,
    cross_venue_refreshed_ms: i64,
}

#[derive(Debug, Clone)]
struct CachedValue<T> {
    refreshed_ms: i64,
    value: T,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UniverseDiscovery {
    pub as_of_ms: i64,
    pub symbols: Vec<String>,
    pub eligible_contracts: usize,
    pub liquid_contracts: usize,
    pub rolling_history_ready: usize,
    pub leaders: Vec<UniverseLeader>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UniverseLeader {
    pub symbol: String,
    pub quote_volume_24h_usd: f64,
    pub change_24h_pct: f64,
    pub return_1m_pct: Option<f64>,
    pub return_5m_pct: Option<f64>,
    pub return_15m_pct: Option<f64>,
    pub return_1h_pct: Option<f64>,
    pub anomaly_score: f64,
}

type DiscoveryRow = (
    String,
    f64,
    f64,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

// The all-market ticker stream only includes contracts whose rolling ticker
// changed in that update. A quiet but liquid contract must not fall out of the
// universe merely because it was absent from a few one-second arrays.
const DISCOVERY_TICKER_TTL_MS: i64 = 60_000;

fn median_abs(values: impl Iterator<Item = Option<f64>>, floor: f64) -> f64 {
    let mut values: Vec<_> = values.flatten().map(f64::abs).collect();
    values.sort_by(f64::total_cmp);
    values
        .get(values.len() / 2)
        .copied()
        .unwrap_or(floor)
        .max(floor)
}

impl BinanceMarketSource {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .user_agent("greed-demo/0.1")
            .timeout(Duration::from_secs(config.request_timeout_seconds));
        if let Some(proxy) = config.proxy.as_deref() {
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        let http = builder.build()?;
        Ok(Self {
            config,
            http,
            oi_history: BTreeMap::new(),
            last_success_ms: None,
            last_error: None,
            telemetry: Mutex::new(ApiTelemetry::default()),
            discovery_prices: BTreeMap::new(),
            eligible_contracts: BTreeSet::new(),
            stream: None,
            candle_cache: BTreeMap::new(),
            derivatives_cache: BTreeMap::new(),
            cross_venue_cache: BTreeMap::new(),
            cross_venue_refreshed_ms: 0,
        })
    }

    pub async fn start_market_stream(&mut self, strategy: &StrategyConfig) -> Result<()> {
        let stream = MarketStreamHub::start(self.config.binance_futures_ws_base.clone());
        stream.set_symbols(&strategy.symbols);
        self.stream = Some(stream);
        let deadline = chrono::Utc::now().timestamp_millis()
            + i64::try_from(self.config.stream_warmup_seconds).unwrap_or(5) * 1_000;
        let tickers = loop {
            let values = self
                .stream
                .as_ref()
                .expect("market stream inserted above")
                .tickers();
            if !values.is_empty() || chrono::Utc::now().timestamp_millis() >= deadline {
                break values;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        self.eligible_contracts = tickers
            .keys()
            .filter(|symbol| symbol.ends_with("USDT"))
            .cloned()
            .collect();
        if self.eligible_contracts.is_empty() {
            let telemetry = self
                .stream
                .as_ref()
                .expect("market stream inserted above")
                .telemetry();
            return Err(anyhow!(
                "market websocket ticker cache did not warm up: {:?}",
                telemetry
            ));
        }
        Ok(())
    }

    pub fn set_stream_symbols(&self, strategy: &StrategyConfig) {
        if let Some(stream) = &self.stream {
            stream.set_symbols(&strategy.symbols);
        }
    }

    pub async fn discover_universe(
        &mut self,
        strategy: &StrategyConfig,
        now_ms: i64,
    ) -> Result<UniverseDiscovery> {
        if !strategy.universe.dynamic_enabled {
            return Ok(UniverseDiscovery {
                as_of_ms: now_ms,
                symbols: strategy.symbols.clone(),
                eligible_contracts: strategy.symbols.len(),
                liquid_contracts: strategy.symbols.len(),
                rolling_history_ready: 0,
                leaders: Vec::new(),
            });
        }
        let tickers = self
            .stream
            .as_ref()
            .ok_or_else(|| anyhow!("market websocket is not started"))?
            .tickers();
        if tickers.is_empty() {
            return Err(anyhow!("market websocket ticker cache is not warm"));
        }
        self.eligible_contracts.extend(
            tickers
                .keys()
                .filter(|symbol| symbol.ends_with("USDT"))
                .cloned(),
        );
        let eligible: BTreeSet<_> = self.eligible_contracts.iter().cloned().collect();
        let mut rows = Vec::new();
        for (symbol, ticker) in tickers {
            if !eligible.contains(&symbol) || now_ms - ticker.received_ms > DISCOVERY_TICKER_TTL_MS
            {
                continue;
            }
            let price = ticker.price;
            let quote_volume = ticker.quote_volume_24h;
            let change_24h = ticker.change_24h();
            if price <= 0.0 || quote_volume < strategy.universe.min_24h_quote_volume_usd {
                continue;
            }
            let history = self.discovery_prices.entry(symbol.clone()).or_default();
            history.push_back((now_ms, price));
            while history
                .front()
                .is_some_and(|(ts, _)| *ts < now_ms - 2 * 3_600_000)
            {
                history.pop_front();
            }
            let return_1h = history
                .iter()
                .rev()
                .find(|(ts, _)| *ts <= now_ms - 45 * 60_000)
                .map(|(_, prior)| price / prior - 1.0);
            rows.push((
                symbol,
                quote_volume,
                change_24h,
                ticker.return_1m,
                ticker.return_5m,
                ticker.return_15m,
                return_1h,
            ));
        }
        let rolling_history_ready = rows
            .iter()
            .filter(|(_, _, _, _, return_5m, _, _)| return_5m.is_some())
            .count();
        let mut selected: BTreeSet<String> = strategy
            .symbols
            .iter()
            .filter(|symbol| eligible.contains(*symbol))
            .cloned()
            .collect();
        let mut by_liquidity = rows.clone();
        by_liquidity.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (symbol, _, _, _, _, _, _) in by_liquidity
            .iter()
            .take(strategy.universe.top_liquidity_names)
        {
            selected.insert(symbol.clone());
        }
        let mut by_movement = rows.clone();
        let scale_1m = median_abs(rows.iter().map(|row| row.3), 0.0005);
        let scale_5m = median_abs(rows.iter().map(|row| row.4), 0.0015);
        let scale_15m = median_abs(rows.iter().map(|row| row.5), 0.0030);
        let anomaly_score = |row: &DiscoveryRow| {
            row.3.unwrap_or_default().abs() / scale_1m * 0.50
                + row.4.unwrap_or_default().abs() / scale_5m * 0.30
                + row.5.unwrap_or_default().abs() / scale_15m * 0.15
                + row.2.abs() * 0.05 / 0.03
        };
        by_movement.sort_by(|a, b| anomaly_score(b).total_cmp(&anomaly_score(a)));
        for (symbol, _, _, _, _, _, _) in by_movement.iter().take(strategy.universe.top_mover_names)
        {
            selected.insert(symbol.clone());
        }
        // Liquidity and mover rankings overlap heavily in trending markets.
        // Fill the remainder from executable, liquid contracts so max_symbols
        // is the active target rather than an accidental upper bound.
        for (symbol, _, _, _, _, _, _) in &by_liquidity {
            if selected.len() >= strategy.universe.max_symbols {
                break;
            }
            selected.insert(symbol.clone());
        }
        let selected_scores: BTreeMap<_, _> = rows
            .iter()
            .map(|row| (&row.0, anomaly_score(row) + row.1.ln_1p() * 1e-6))
            .collect();
        let mut symbols: Vec<_> = selected.into_iter().collect();
        symbols.sort_by(|a, b| {
            selected_scores
                .get(b)
                .copied()
                .unwrap_or(0.0)
                .total_cmp(&selected_scores.get(a).copied().unwrap_or(0.0))
        });
        symbols.truncate(strategy.universe.max_symbols);
        let selected_symbols: BTreeSet<_> = symbols.iter().map(String::as_str).collect();
        let leaders = by_movement
            .into_iter()
            .filter(|(symbol, _, _, _, _, _, _)| selected_symbols.contains(symbol.as_str()))
            .take(20)
            .map(|row| UniverseLeader {
                symbol: row.0.clone(),
                quote_volume_24h_usd: row.1,
                change_24h_pct: row.2 * 100.0,
                return_1m_pct: row.3.map(|value| value * 100.0),
                return_5m_pct: row.4.map(|value| value * 100.0),
                return_15m_pct: row.5.map(|value| value * 100.0),
                return_1h_pct: row.6.map(|value| value * 100.0),
                anomaly_score: anomaly_score(&row),
            })
            .collect();
        Ok(UniverseDiscovery {
            as_of_ms: now_ms,
            symbols,
            eligible_contracts: eligible.len(),
            liquid_contracts: rows.len(),
            rolling_history_ready,
            leaders,
        })
    }
    pub fn health(&self) -> Value {
        let telemetry = self
            .telemetry
            .lock()
            .expect("telemetry mutex poisoned")
            .clone();
        let average_latency_ms = (telemetry.requests > 0)
            .then_some(telemetry.total_latency_ms as f64 / telemetry.requests as f64);
        let stream = self.stream.as_ref().map(MarketStreamHub::telemetry);
        serde_json::json!({"last_success_ms":self.last_success_ms,"last_error":self.last_error,"paper_only":true,"transport":"websocket_primary","cross_venue_symbols":self.cross_venue_cache.len(),"hyperliquid_last_refresh_ms":self.cross_venue_refreshed_ms,"stream":stream,"telemetry":telemetry,"average_latency_ms":average_latency_ms})
    }
    fn endpoint_key(url: &str) -> String {
        reqwest::Url::parse(url)
            .ok()
            .map(|value| format!("{}{}", value.host_str().unwrap_or("unknown"), value.path()))
            .unwrap_or_else(|| "invalid_url".into())
    }
    fn record_request(&self, url: &str, elapsed_ms: u64, success: bool, rate_limited: bool) {
        let mut telemetry = self.telemetry.lock().expect("telemetry mutex poisoned");
        telemetry.requests += 1;
        telemetry.total_latency_ms += elapsed_ms;
        telemetry.max_latency_ms = telemetry.max_latency_ms.max(elapsed_ms);
        if success {
            telemetry.successes += 1;
        } else {
            telemetry.failures += 1;
        }
        if rate_limited {
            telemetry.rate_limits += 1;
        }
        let endpoint = telemetry
            .endpoints
            .entry(Self::endpoint_key(url))
            .or_default();
        endpoint.requests += 1;
        endpoint.total_latency_ms += elapsed_ms;
        endpoint.max_latency_ms = endpoint.max_latency_ms.max(elapsed_ms);
        if success {
            endpoint.successes += 1;
        } else {
            endpoint.failures += 1;
        }
        if rate_limited {
            endpoint.rate_limits += 1;
        }
    }
    async fn get(&self, url: String) -> Result<Value> {
        let mut error = None;
        for attempt in 0..2 {
            if attempt > 0 {
                self.telemetry
                    .lock()
                    .expect("telemetry mutex poisoned")
                    .retries += 1;
            }
            tokio::time::sleep(Duration::from_millis(self.config.request_spacing_ms)).await;
            let started = Instant::now();
            match self.http.get(&url).send().await {
                Ok(response) if response.status().is_success() => {
                    let used_weight = response
                        .headers()
                        .get("x-mbx-used-weight-1m")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok());
                    if let Some(weight) = used_weight {
                        let mut telemetry =
                            self.telemetry.lock().expect("telemetry mutex poisoned");
                        telemetry.latest_used_weight_1m = Some(weight);
                        telemetry.max_used_weight_1m = telemetry.max_used_weight_1m.max(weight);
                    }
                    let result = response
                        .text()
                        .await
                        .map_err(anyhow::Error::from)
                        .and_then(|body| serde_json::from_str(&body).map_err(Into::into));
                    self.record_request(
                        &url,
                        started.elapsed().as_millis() as u64,
                        result.is_ok(),
                        false,
                    );
                    if result.is_ok() {
                        return result;
                    }
                    error = result.err();
                }
                Ok(response)
                    if response.status() == StatusCode::TOO_MANY_REQUESTS
                        || response.status().as_u16() == 418 =>
                {
                    self.record_request(&url, started.elapsed().as_millis() as u64, false, true);
                    let retry = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1)
                        .min(30);
                    error = Some(anyhow!("rate limited by {}", url));
                    tokio::time::sleep(Duration::from_secs(retry)).await;
                }
                Ok(response) => {
                    self.record_request(&url, started.elapsed().as_millis() as u64, false, false);
                    error = Some(anyhow!("{} returned {}", url, response.status()));
                }
                Err(value) => {
                    self.record_request(&url, started.elapsed().as_millis() as u64, false, false);
                    error = Some(value.into());
                }
            }
            if attempt < 1 {
                tokio::time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
            }
        }
        Err(error.unwrap_or_else(|| anyhow!("request failed: {url}")))
    }
    async fn get_from_bases(&self, bases: &[String], suffix: &str) -> Result<Value> {
        let mut errors = Vec::new();
        for (index, base) in bases.iter().enumerate() {
            if index > 0 {
                self.telemetry
                    .lock()
                    .expect("telemetry mutex poisoned")
                    .fallback_attempts += 1;
            }
            match self.get(format!("{base}{suffix}")).await {
                Ok(value) => {
                    if index > 0 {
                        self.telemetry
                            .lock()
                            .expect("telemetry mutex poisoned")
                            .fallback_successes += 1;
                    }
                    return Ok(value);
                }
                Err(error) => errors.push(format!("{base}: {error}")),
            }
        }
        Err(anyhow!(errors.join(" | ")))
    }
    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        let started = Instant::now();
        let response = self.http.post(url).json(&body).send().await?;
        let rate_limited = response.status() == StatusCode::TOO_MANY_REQUESTS;
        if !response.status().is_success() {
            let status = response.status();
            self.record_request(
                url,
                started.elapsed().as_millis() as u64,
                false,
                rate_limited,
            );
            return Err(anyhow!("{url} returned {status}"));
        }
        let value = response.json::<Value>().await;
        self.record_request(
            url,
            started.elapsed().as_millis() as u64,
            value.is_ok(),
            false,
        );
        Ok(value?)
    }
    fn futures_bases(&self) -> Vec<String> {
        std::iter::once(self.config.binance_futures_base.clone())
            .chain(self.config.binance_futures_fallbacks.clone())
            .collect()
    }
    fn meta(now: i64, ttl: i64, source: &str, quality: DataQuality) -> ObservationMeta {
        ObservationMeta {
            event_ms: now,
            received_ms: now,
            expires_ms: now + ttl,
            source: source.into(),
            quality,
        }
    }
    #[allow(clippy::too_many_arguments)]
    async fn klines_at_interval(
        &self,
        bases: &[String],
        path: &str,
        symbol: &str,
        market: MarketKind,
        interval: &str,
        interval_ms: i64,
        limit: usize,
        now: i64,
    ) -> Result<CandleSeries> {
        let suffix = format!("{path}?symbol={symbol}&interval={interval}&limit={limit}");
        let values = self.get_from_bases(bases, &suffix).await?;
        let rows = values
            .as_array()
            .ok_or_else(|| anyhow!("kline response is not an array"))?;
        let mut bars = Vec::with_capacity(rows.len());
        for row in rows {
            let v = row.as_array().ok_or_else(|| anyhow!("invalid kline row"))?;
            let n = |index: usize| -> Result<f64> {
                v.get(index)
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing kline field {index}"))?
                    .parse()
                    .map_err(Into::into)
            };
            bars.push(Candle {
                open_ms: v[0].as_i64().unwrap_or(0),
                open: n(1)?,
                high: n(2)?,
                low: n(3)?,
                close: n(4)?,
                quote_volume: n(7)?,
                close_ms: v[6].as_i64().unwrap_or(0),
                taker_buy_quote: v
                    .get(10)
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok()),
                closed: v[6].as_i64().unwrap_or(i64::MAX) < now,
            });
        }
        Ok(CandleSeries {
            venue: "binance".into(),
            market,
            interval_ms,
            meta: Self::meta(now, 120_000, "binance_klines", DataQuality::Complete),
            values: bars,
        })
    }

    async fn streamed_klines(
        &mut self,
        symbol: &str,
        interval: &str,
        interval_ms: i64,
        limit: usize,
        now: i64,
    ) -> Result<CandleSeries> {
        let key = (symbol.to_string(), interval.to_string());
        if !self.candle_cache.contains_key(&key) {
            let bootstrap = self
                .klines_at_interval(
                    &self.futures_bases(),
                    "/fapi/v1/klines",
                    symbol,
                    MarketKind::Perpetual,
                    interval,
                    interval_ms,
                    limit,
                    now,
                )
                .await?;
            self.candle_cache.insert(key.clone(), bootstrap);
        }
        let (updates, updated_ms) = self
            .stream
            .as_ref()
            .ok_or_else(|| anyhow!("market websocket is not started"))?
            .candles(symbol, interval);
        let series = self
            .candle_cache
            .get_mut(&key)
            .expect("candle cache inserted above");
        for candle in updates {
            if let Some(existing) = series
                .values
                .iter_mut()
                .find(|value| value.open_ms == candle.open_ms)
            {
                *existing = candle;
            } else {
                series.values.push(candle);
            }
        }
        series.values.sort_by_key(|value| value.open_ms);
        if series.values.len() > limit {
            series.values.drain(..series.values.len() - limit);
        }
        let fresh = updated_ms.is_some_and(|value| now - value <= 15_000);
        series.meta = ObservationMeta {
            event_ms: updated_ms.unwrap_or(now),
            received_ms: updated_ms.unwrap_or(now),
            expires_ms: updated_ms.unwrap_or_default() + 15_000,
            source: "binance_ws_kline".into(),
            quality: if fresh {
                DataQuality::Complete
            } else {
                DataQuality::Stale
            },
        };
        Ok(series.clone())
    }

    fn stream_observation_klines(
        &self,
        symbol: &str,
        interval: &str,
        interval_ms: i64,
        now: i64,
    ) -> Option<CandleSeries> {
        let (values, updated_ms) = self.stream.as_ref()?.candles(symbol, interval);
        if values.is_empty() {
            return None;
        }
        let fresh = updated_ms.is_some_and(|value| now - value <= 15_000);
        Some(CandleSeries {
            venue: "binance".into(),
            market: MarketKind::Perpetual,
            interval_ms,
            meta: ObservationMeta {
                event_ms: updated_ms.unwrap_or(now),
                received_ms: updated_ms.unwrap_or(now),
                expires_ms: updated_ms.unwrap_or_default() + 15_000,
                source: "binance_ws_observation_kline".into(),
                quality: if fresh {
                    DataQuality::Complete
                } else {
                    DataQuality::Stale
                },
            },
            values,
        })
    }

    async fn refresh_derivatives(
        &mut self,
        symbol: &str,
        fallback_price: f64,
        now: i64,
    ) -> Result<DerivativesState> {
        let bases = self.futures_bases();
        let oi_value = self
            .get_from_bases(&bases, &format!("/fapi/v1/openInterest?symbol={symbol}"))
            .await?;
        let mark = self.stream.as_ref().and_then(|stream| stream.mark(symbol));
        let oi_units: f64 = oi_value["openInterest"].as_str().unwrap_or("0").parse()?;
        let oi = oi_units
            * mark
                .as_ref()
                .map(|value| value.mark_price)
                .unwrap_or(fallback_price);
        let history = self.oi_history.entry(symbol.into()).or_default();
        history.push_back((now, oi));
        while history
            .front()
            .is_some_and(|(ts, _)| now - ts > 30 * 60_000)
        {
            history.pop_front();
        }
        // Do not interpret one noisy poll as an OI regime.  The first ten
        // minutes intentionally remain Unknown after a cold start.
        let change = history
            .iter()
            .find(|(ts, value)| now - ts >= 10 * 60_000 && *value > 0.0)
            .map(|(_, previous)| oi / previous - 1.0);
        Ok(DerivativesState {
            meta: Self::meta(
                now,
                i64::try_from(self.config.oi_refresh_seconds).unwrap_or(120) * 2_000,
                "binance_ws_mark_rest_oi",
                if mark
                    .as_ref()
                    .is_some_and(|value| now - value.received_ms <= 15_000)
                {
                    DataQuality::Complete
                } else {
                    DataQuality::Partial
                },
            ),
            open_interest_usd: Some(oi),
            open_interest_change_pct: change,
            funding_rate: mark.as_ref().and_then(|value| value.funding_rate),
            basis_pct: mark
                .as_ref()
                .filter(|value| value.index_price > 0.0)
                .map(|value| value.mark_price / value.index_price - 1.0),
            long_liquidations_usd: None,
            short_liquidations_usd: None,
        })
    }

    async fn cached_derivatives(
        &mut self,
        symbol: &str,
        price: f64,
        now: i64,
        force_refresh: bool,
    ) -> Result<DerivativesState> {
        let refresh_ms = i64::try_from(self.config.oi_refresh_seconds).unwrap_or(120) * 1_000;
        if self.derivatives_cache.get(symbol).is_none_or(|cached| {
            now - cached.refreshed_ms >= if force_refresh { 15_000 } else { refresh_ms }
        }) {
            let value = self.refresh_derivatives(symbol, price, now).await?;
            self.derivatives_cache.insert(
                symbol.into(),
                CachedValue {
                    refreshed_ms: now,
                    value,
                },
            );
        }
        let mut value = self
            .derivatives_cache
            .get(symbol)
            .expect("derivatives cache inserted above")
            .value
            .clone();
        match self.stream.as_ref().and_then(|stream| stream.mark(symbol)) {
            Some(mark) => {
                value.funding_rate = mark.funding_rate;
                value.basis_pct =
                    (mark.index_price > 0.0).then_some(mark.mark_price / mark.index_price - 1.0);
                value.meta.event_ms = mark.event_ms;
                value.meta.received_ms = mark.received_ms;
                value.meta.expires_ms = mark.received_ms + 15_000;
                value.meta.quality = if now - mark.received_ms <= 15_000 {
                    DataQuality::Complete
                } else {
                    DataQuality::Stale
                };
            }
            None => {
                value.meta.quality = DataQuality::Stale;
                value.meta.expires_ms = 0;
            }
        }
        Ok(value)
    }

    async fn refresh_cross_venue(&mut self, symbols: &[String], now: i64) -> Result<()> {
        let url = self.config.hyperliquid_info_url.clone();
        let contexts = self
            .post_json(&url, serde_json::json!({"type":"metaAndAssetCtxs"}))
            .await?;
        let predicted = self
            .post_json(&url, serde_json::json!({"type":"predictedFundings"}))
            .await?;
        let universe = contexts
            .get(0)
            .and_then(|v| v.get("universe"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Hyperliquid metadata is missing universe"))?;
        let values = contexts
            .get(1)
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Hyperliquid metadata is missing asset contexts"))?;
        let mut funding_by_coin: BTreeMap<String, (Option<f64>, Option<f64>)> = BTreeMap::new();
        if let Some(rows) = predicted.as_array() {
            for row in rows {
                let Some(parts) = row.as_array() else {
                    continue;
                };
                let Some(coin) = parts.first().and_then(Value::as_str) else {
                    continue;
                };
                let mut hyper = None;
                let mut binance = None;
                if let Some(venues) = parts.get(1).and_then(Value::as_array) {
                    for venue in venues {
                        let Some(pair) = venue.as_array() else {
                            continue;
                        };
                        let Some(name) = pair.first().and_then(Value::as_str) else {
                            continue;
                        };
                        let Some(data) = pair.get(1).filter(|v| v.is_object()) else {
                            continue;
                        };
                        let rate = data
                            .get("fundingRate")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse::<f64>().ok());
                        let hours = data
                            .get("fundingIntervalHours")
                            .and_then(Value::as_u64)
                            .unwrap_or(8)
                            .max(1) as f64;
                        match name {
                            "HlPerp" => hyper = rate.map(|v| v / hours),
                            "BinPerp" => binance = rate.map(|v| v / hours),
                            _ => {}
                        }
                    }
                }
                funding_by_coin.insert(coin.to_string(), (hyper, binance));
            }
        }
        let wanted: BTreeSet<_> = symbols.iter().map(|s| s.trim_end_matches("USDT")).collect();
        for (meta, ctx) in universe.iter().zip(values) {
            let Some(coin) = meta.get("name").and_then(Value::as_str) else {
                continue;
            };
            if !wanted.contains(coin) {
                continue;
            }
            let number = |key: &str| {
                ctx.get(key)
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse::<f64>().ok())
            };
            let Some(mark) = number("markPx") else {
                continue;
            };
            let oi = number("openInterest").unwrap_or(0.0) * mark;
            let premium = number("premium");
            let (hyper_funding, binance_funding) = funding_by_coin
                .get(coin)
                .copied()
                .unwrap_or((number("funding"), None));
            let hyper_funding = hyper_funding.unwrap_or(0.0);
            let symbol = format!("{coin}USDT");
            let binance_mark = self
                .stream
                .as_ref()
                .and_then(|stream| stream.mark(&symbol))
                .map(|m| m.mark_price);
            let value = CrossVenueState {
                meta: Self::meta(
                    now,
                    i64::try_from(self.config.hyperliquid_refresh_seconds).unwrap_or(60) * 2_000,
                    "hyperliquid_public_info",
                    DataQuality::Complete,
                ),
                hyper_mark_price: mark,
                hyper_open_interest_usd: oi,
                hyper_funding_per_hour: hyper_funding,
                hyper_premium_pct: premium,
                binance_funding_per_hour: binance_funding,
                funding_gap_per_hour: binance_funding.map(|v| hyper_funding - v),
                mark_premium_pct: binance_mark.filter(|v| *v > 0.0).map(|v| mark / v - 1.0),
            };
            self.cross_venue_cache.insert(
                symbol,
                CachedValue {
                    refreshed_ms: now,
                    value,
                },
            );
        }
        self.cross_venue_refreshed_ms = now;
        Ok(())
    }
    pub async fn fetch_frame(
        &mut self,
        strategy: &StrategyConfig,
        account: AccountFrame,
    ) -> Result<MarketFrame> {
        let now = chrono::Utc::now().timestamp_millis();
        self.telemetry
            .lock()
            .expect("telemetry mutex poisoned")
            .frames_requested += 1;
        self.last_error = None;
        let result = self.fetch_inner(strategy, account, now).await;
        match &result {
            Ok(_) => {
                self.last_success_ms = Some(now);
                self.telemetry
                    .lock()
                    .expect("telemetry mutex poisoned")
                    .frames_succeeded += 1;
            }
            Err(error) => {
                self.last_error = Some(error.to_string());
                self.telemetry
                    .lock()
                    .expect("telemetry mutex poisoned")
                    .frames_failed += 1;
            }
        }
        result
    }
    async fn fetch_inner(
        &mut self,
        strategy: &StrategyConfig,
        account: AccountFrame,
        now: i64,
    ) -> Result<MarketFrame> {
        let mut instruments = BTreeMap::new();
        let mut warnings = Vec::new();
        self.set_stream_symbols(strategy);
        let hyper_refresh_ms =
            i64::try_from(self.config.hyperliquid_refresh_seconds).unwrap_or(60) * 1_000;
        if strategy.lanes.cross_venue_crowding_enabled
            && now - self.cross_venue_refreshed_ms >= hyper_refresh_ms
        {
            if let Err(error) = self.refresh_cross_venue(&strategy.symbols, now).await {
                warnings.push(format!("hyperliquid context: {error}"));
            }
        }
        for symbol in &strategy.symbols {
            let perpetual = match self
                .streamed_klines(symbol, "15m", 900_000, self.config.candle_limit, now)
                .await
            {
                Ok(series) => series,
                Err(error) => {
                    warnings.push(format!("perpetual klines {symbol}: {error}"));
                    continue;
                }
            };
            let price = perpetual.values.last().map(|bar| bar.close).unwrap_or(0.0);
            let fast_perpetual = match self.streamed_klines(symbol, "5m", 300_000, 120, now).await {
                Ok(value) => Some(value),
                Err(error) => {
                    warnings.push(format!("fast perpetual klines {symbol}: {error}"));
                    None
                }
            };
            let micro_perpetual = self.stream_observation_klines(symbol, "1m", 60_000, now);
            let book = self
                .stream
                .as_ref()
                .and_then(|stream| stream.book(symbol, now));
            if book.as_ref().is_none_or(|value| !value.meta.usable_at(now)) {
                warnings.push(format!("websocket depth stale or missing {symbol}"));
            }
            let microstructure = self
                .stream
                .as_ref()
                .and_then(|stream| stream.microstructure(symbol, now));
            let liquidation_burst = microstructure.as_ref().is_some_and(|value| {
                value.long_liquidations_60s + value.short_liquidations_60s
                    >= strategy.lanes.liquidation_min_notional_usd
            });
            let derivatives = if price > 0.0 {
                match self
                    .cached_derivatives(symbol, price, now, liquidation_burst)
                    .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("derivatives {symbol}: {error}"));
                        None
                    }
                }
            } else {
                None
            };
            let cross_venue = self
                .cross_venue_cache
                .get(symbol)
                .map(|cached| cached.value.clone());
            instruments.insert(
                symbol.clone(),
                InstrumentFrame {
                    symbol: symbol.clone(),
                    price,
                    perpetual,
                    fast_perpetual,
                    micro_perpetual,
                    book,
                    derivatives,
                    microstructure,
                    cross_venue,
                },
            );
        }
        if !warnings.is_empty() {
            self.last_error = Some(warnings.join(" | "));
        }
        Ok(MarketFrame {
            as_of_ms: now,
            instruments,
            account,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_tracks_failures_rate_limits_and_latency() {
        let source = BinanceMarketSource::new(RuntimeConfig::default()).unwrap();
        source.record_request(
            "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT",
            25,
            false,
            true,
        );
        source.record_request(
            "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT",
            15,
            true,
            false,
        );
        let health = source.health();
        assert_eq!(health["telemetry"]["requests"], 2);
        assert_eq!(health["telemetry"]["failures"], 1);
        assert_eq!(health["telemetry"]["rate_limits"], 1);
        assert_eq!(health["average_latency_ms"], 20.0);
    }
}
