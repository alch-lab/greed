use crate::config::RuntimeConfig;
use crate::market_stream::{MarketStreamHub, StreamTicker};
use anyhow::{anyhow, Result};
use futures_util::{stream, StreamExt};
use greed_kernel::{
    AccountFrame, Candle, CandleSeries, DataQuality, InstrumentFrame, MarketFrame, MarketKind,
    ObservationMeta, OpenInterestPoint, OpenInterestSeries,
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
    last_success_ms: Option<i64>,
    last_error: Option<String>,
    telemetry: Mutex<ApiTelemetry>,
    discovery_prices: BTreeMap<String, VecDeque<(i64, f64)>>,
    eligible_contracts: BTreeSet<String>,
    stream: Option<MarketStreamHub>,
    candle_cache: BTreeMap<(String, String), CandleSeries>,
    candle_bootstrap_retry_after: BTreeMap<(String, String), i64>,
    candle_bootstrap_pending: usize,
    candle_bootstrap_last_error: Option<String>,
    open_interest_cache: BTreeMap<String, OpenInterestSeries>,
    open_interest_retry_after: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UniverseDiscovery {
    pub as_of_ms: i64,
    pub symbols: Vec<String>,
    pub eligible_contracts: usize,
    pub liquid_contracts: usize,
    pub surge_contracts: usize,
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
    pub admission: &'static str,
}

type DiscoveryRow = (
    String,
    f64,
    f64,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    bool,
);

// The all-market ticker stream only includes contracts whose rolling ticker
// changed in that update. A quiet but liquid contract must not fall out of the
// universe merely because it was absent from a few one-second arrays.
const DISCOVERY_TICKER_TTL_MS: i64 = 60_000;
// Newly discovered contracts need REST history before websocket candle updates
// can extend them. Bound that cold-start work so a slow Binance edge cannot
// freeze account reconciliation and status publication for minutes.
const MAX_KLINE_BOOTSTRAP_REQUESTS_PER_FRAME: usize = 8;
const KLINE_BOOTSTRAP_CONCURRENCY: usize = 4;
const KLINE_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5);
const KLINE_BOOTSTRAP_RETRY_DELAY_MS: i64 = 60_000;
const OI_REFRESH_MS: i64 = 5 * 60_000;
const MAX_OI_REQUESTS_PER_FRAME: usize = 4;

fn heal_elapsed_candle_closes(values: &mut [Candle], now_ms: i64) {
    for candle in values {
        if !candle.closed && candle.close_ms < now_ms {
            candle.closed = true;
        }
    }
}

fn median_abs(values: impl Iterator<Item = Option<f64>>, floor: f64) -> f64 {
    let mut values: Vec<_> = values.flatten().map(f64::abs).collect();
    values.sort_by(f64::total_cmp);
    values
        .get(values.len() / 2)
        .copied()
        .unwrap_or(floor)
        .max(floor)
}

fn is_surge_admission(ticker: &StreamTicker, universe: &greed_strategy::UniverseConfig) -> bool {
    ticker.quote_volume_24h >= universe.surge_min_24h_quote_volume_usd
        && (ticker.change_24h().abs() >= universe.surge_min_abs_change_24h
            || ticker
                .return_15m
                .is_some_and(|value| value.abs() >= universe.surge_min_abs_return_15m))
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
            last_success_ms: None,
            last_error: None,
            telemetry: Mutex::new(ApiTelemetry::default()),
            discovery_prices: BTreeMap::new(),
            eligible_contracts: BTreeSet::new(),
            stream: None,
            candle_cache: BTreeMap::new(),
            candle_bootstrap_retry_after: BTreeMap::new(),
            candle_bootstrap_pending: 0,
            candle_bootstrap_last_error: None,
            open_interest_cache: BTreeMap::new(),
            open_interest_retry_after: BTreeMap::new(),
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

    /// Share the live mainnet websocket cache with the execution layer. This
    /// lets a resting Demo order be canceled as soon as its original signal
    /// fails, without adding REST polling or Binance request weight.
    pub fn market_stream_handle(&self) -> Option<MarketStreamHub> {
        self.stream.clone()
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
                surge_contracts: 0,
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
            if price <= 0.0 {
                continue;
            }
            let core_admission = quote_volume >= strategy.universe.min_24h_quote_volume_usd;
            let surge_admission =
                !core_admission && is_surge_admission(&ticker, &strategy.universe);
            if !core_admission && !surge_admission {
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
                surge_admission,
            ));
        }
        let surge_contracts = rows.iter().filter(|row| row.7).count();
        let core_rows: Vec<_> = rows.iter().filter(|row| !row.7).cloned().collect();
        let rolling_history_ready = rows.iter().filter(|row| row.4.is_some()).count();
        let mut selected: BTreeSet<String> = strategy
            .symbols
            .iter()
            .filter(|symbol| eligible.contains(*symbol))
            .cloned()
            .collect();
        let mut by_liquidity = core_rows.clone();
        by_liquidity.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (symbol, _, _, _, _, _, _, _) in by_liquidity
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
        for (symbol, _, _, _, _, _, _, _) in
            by_movement.iter().take(strategy.universe.top_mover_names)
        {
            selected.insert(symbol.clone());
        }
        // Liquidity and mover rankings overlap heavily in trending markets.
        // Fill the remainder from executable, liquid contracts so max_symbols
        // is the active target rather than an accidental upper bound.
        for (symbol, _, _, _, _, _, _, _) in &by_liquidity {
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
            .filter(|(symbol, _, _, _, _, _, _, _)| selected_symbols.contains(symbol.as_str()))
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
                admission: if row.7 {
                    "short_term_surge"
                } else {
                    "core_liquidity"
                },
            })
            .collect();
        Ok(UniverseDiscovery {
            as_of_ms: now_ms,
            symbols,
            eligible_contracts: eligible.len(),
            liquid_contracts: core_rows.len(),
            surge_contracts,
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
        serde_json::json!({
            "last_success_ms":self.last_success_ms,
            "last_error":self.last_error,
            "paper_only":true,
            "transport":"websocket_primary",
            "stream":stream,
            "bootstrap":{
                "pending_requests":self.candle_bootstrap_pending,
                "last_error":self.candle_bootstrap_last_error,
            },
            "telemetry":telemetry,
            "average_latency_ms":average_latency_ms
        })
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

    async fn bootstrap_missing_klines(&mut self, strategy: &StrategyConfig, now: i64) {
        let mut symbols = strategy.symbols.clone();
        symbols.sort_by_key(|symbol| match symbol.as_str() {
            "BTCUSDT" => 0,
            "ETHUSDT" => 1,
            "BNBUSDT" => 2,
            "SOLUSDT" => 3,
            _ => 4,
        });
        let intervals = [
            (
                "15m",
                900_000,
                self.config
                    .research_backfill_15m_bars
                    .max(self.config.candle_limit),
            ),
            ("1h", 3_600_000, self.config.research_backfill_1h_bars),
            ("5m", 300_000, self.config.research_backfill_5m_bars),
            ("1m", 60_000, self.config.research_backfill_1m_bars),
        ];
        let mut requests = Vec::new();
        // Warm the trading-critical 15m series for every symbol before the
        // additional research horizons. A cold restart therefore resumes
        // candidate evaluation quickly while 1h/5m/1m context fills in behind it.
        for (interval, interval_ms, limit) in intervals {
            for symbol in &symbols {
                let key = (symbol.clone(), interval.to_string());
                if self.candle_cache.contains_key(&key)
                    || self
                        .candle_bootstrap_retry_after
                        .get(&key)
                        .is_some_and(|retry_after| now < *retry_after)
                {
                    continue;
                }
                if requests.len() < MAX_KLINE_BOOTSTRAP_REQUESTS_PER_FRAME {
                    requests.push((symbol.clone(), interval, interval_ms, limit));
                }
            }
        }
        self.candle_bootstrap_pending = strategy
            .symbols
            .iter()
            .flat_map(|symbol| ["15m", "1h", "5m", "1m"].map(move |interval| (symbol, interval)))
            .filter(|(symbol, interval)| {
                !self
                    .candle_cache
                    .contains_key(&(String::from(symbol.as_str()), String::from(*interval)))
            })
            .count();
        if requests.is_empty() {
            return;
        }

        let bases = self.futures_bases();
        let source = &*self;
        let results = stream::iter(requests)
            .map(|(symbol, interval, interval_ms, limit)| {
                let bases = bases.clone();
                async move {
                    let result = match tokio::time::timeout(
                        KLINE_BOOTSTRAP_TIMEOUT,
                        source.klines_at_interval(
                            &bases,
                            "/fapi/v1/klines",
                            &symbol,
                            MarketKind::Perpetual,
                            interval,
                            interval_ms,
                            limit,
                            now,
                        ),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow!(
                            "timed out after {}ms",
                            KLINE_BOOTSTRAP_TIMEOUT.as_millis()
                        )),
                    };
                    ((symbol, interval.to_string()), result)
                }
            })
            .buffer_unordered(KLINE_BOOTSTRAP_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        self.candle_bootstrap_last_error = None;
        for (key, result) in results {
            match result {
                Ok(series) => {
                    self.candle_bootstrap_retry_after.remove(&key);
                    self.candle_cache.insert(key, series);
                }
                Err(error) => {
                    if error.to_string().contains("timed out") {
                        // The health snapshot already exposes the latest
                        // pending bootstrap error. Dynamic newcomers are
                        // retried automatically, so repeated timeouts do not
                        // need one journal line per retry.
                        tracing::debug!(symbol = %key.0, interval = %key.1, error = %error, "kline history bootstrap retry timed out");
                    } else {
                        tracing::warn!(symbol = %key.0, interval = %key.1, error = %error, "kline history bootstrap failed");
                    }
                    self.candle_bootstrap_retry_after
                        .insert(key.clone(), now + KLINE_BOOTSTRAP_RETRY_DELAY_MS);
                    self.candle_bootstrap_last_error =
                        Some(format!("{} {}: {error}", key.0, key.1));
                }
            }
        }
        self.candle_bootstrap_pending = strategy
            .symbols
            .iter()
            .flat_map(|symbol| ["15m", "1h", "5m", "1m"].map(move |interval| (symbol, interval)))
            .filter(|(symbol, interval)| {
                !self
                    .candle_cache
                    .contains_key(&(String::from(symbol.as_str()), String::from(*interval)))
            })
            .count();
    }

    async fn streamed_klines(
        &mut self,
        symbol: &str,
        interval: &str,
        limit: usize,
        now: i64,
    ) -> Result<CandleSeries> {
        let key = (symbol.to_string(), interval.to_string());
        if !self.candle_cache.contains_key(&key) {
            return Err(anyhow!("kline history is still warming"));
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
        // Also recover missed terminal websocket updates by wall clock. This
        // covers quiet symbols that have not emitted the first update of the
        // next interval yet after a reconnect.
        heal_elapsed_candle_closes(&mut series.values, now);
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

    async fn open_interest_history(&self, symbol: &str, now: i64) -> Result<OpenInterestSeries> {
        let suffix = format!("/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=24");
        let values = self.get_from_bases(&self.futures_bases(), &suffix).await?;
        let rows = values
            .as_array()
            .ok_or_else(|| anyhow!("open-interest response is not an array"))?;
        let mut points = Vec::with_capacity(rows.len());
        for row in rows {
            let timestamp_ms = row
                .get("timestamp")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow!("open-interest timestamp is missing"))?;
            let value_usd = row
                .get("sumOpenInterestValue")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("open-interest value is missing"))?
                .parse()?;
            points.push(OpenInterestPoint {
                timestamp_ms,
                value_usd,
            });
        }
        Ok(OpenInterestSeries {
            interval_ms: 300_000,
            meta: Self::meta(
                now,
                OI_REFRESH_MS + 60_000,
                "binance_open_interest",
                DataQuality::Complete,
            ),
            values: points,
        })
    }

    async fn refresh_open_interest(&mut self, strategy: &StrategyConfig, now: i64) {
        let symbols: Vec<_> = strategy
            .symbols
            .iter()
            .filter(|symbol| {
                let stale = self
                    .open_interest_cache
                    .get(*symbol)
                    .is_none_or(|series| now - series.meta.received_ms >= OI_REFRESH_MS);
                let retry_ready = self
                    .open_interest_retry_after
                    .get(*symbol)
                    .is_none_or(|retry_after| now >= *retry_after);
                stale && retry_ready
            })
            .take(MAX_OI_REQUESTS_PER_FRAME)
            .cloned()
            .collect();
        let source = &*self;
        let results = stream::iter(symbols)
            .map(|symbol| async move {
                let result = tokio::time::timeout(
                    KLINE_BOOTSTRAP_TIMEOUT,
                    source.open_interest_history(&symbol, now),
                )
                .await
                .map_err(|_| anyhow!("open-interest request timed out"))
                .and_then(|value| value);
                (symbol, result)
            })
            .buffer_unordered(MAX_OI_REQUESTS_PER_FRAME)
            .collect::<Vec<_>>()
            .await;
        for (symbol, result) in results {
            match result {
                Ok(series) => {
                    self.open_interest_cache.insert(symbol.clone(), series);
                    self.open_interest_retry_after.remove(&symbol);
                }
                Err(error) => {
                    tracing::debug!(symbol = %symbol, error = %error, "open-interest history is warming");
                    self.open_interest_retry_after
                        .insert(symbol, now + KLINE_BOOTSTRAP_RETRY_DELAY_MS);
                }
            }
        }
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
        self.bootstrap_missing_klines(strategy, now).await;
        self.refresh_open_interest(strategy, now).await;
        for symbol in &strategy.symbols {
            if !self
                .candle_cache
                .contains_key(&(symbol.clone(), "15m".to_string()))
            {
                continue;
            }
            let perpetual = match self
                .streamed_klines(symbol, "15m", self.config.candle_limit, now)
                .await
            {
                Ok(series) => series,
                Err(error) => {
                    warnings.push(format!("perpetual klines {symbol}: {error}"));
                    continue;
                }
            };
            let price = perpetual.values.last().map(|bar| bar.close).unwrap_or(0.0);
            let hourly_perpetual = if self
                .candle_cache
                .contains_key(&(symbol.clone(), "1h".to_string()))
            {
                match self
                    .streamed_klines(symbol, "1h", self.config.research_backfill_1h_bars, now)
                    .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("hourly perpetual klines {symbol}: {error}"));
                        None
                    }
                }
            } else {
                None
            };
            let fast_perpetual = if self
                .candle_cache
                .contains_key(&(symbol.clone(), "5m".to_string()))
            {
                match self
                    .streamed_klines(symbol, "5m", self.config.research_backfill_5m_bars, now)
                    .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("fast perpetual klines {symbol}: {error}"));
                        None
                    }
                }
            } else {
                None
            };
            let micro_perpetual = if self
                .candle_cache
                .contains_key(&(symbol.clone(), "1m".to_string()))
            {
                self.streamed_klines(symbol, "1m", self.config.research_backfill_1m_bars, now)
                    .await
                    .ok()
            } else {
                self.stream_observation_klines(symbol, "1m", 60_000, now)
            };
            let book = self
                .stream
                .as_ref()
                .and_then(|stream| stream.book(symbol, now));
            let warming = self
                .stream
                .as_ref()
                .is_some_and(|stream| stream.symbol_is_warming(symbol, now));
            if !warming && book.as_ref().is_none_or(|value| !value.meta.usable_at(now)) {
                warnings.push(format!("websocket depth stale or missing {symbol}"));
            }
            let microstructure = self
                .stream
                .as_ref()
                .and_then(|stream| stream.microstructure(symbol, now));
            instruments.insert(
                symbol.clone(),
                InstrumentFrame {
                    symbol: symbol.clone(),
                    price,
                    perpetual,
                    hourly_perpetual,
                    fast_perpetual,
                    micro_perpetual,
                    open_interest: self.open_interest_cache.get(symbol).cloned(),
                    book,
                    microstructure,
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

    fn ticker(quote_volume_24h: f64, change_24h: f64, return_15m: Option<f64>) -> StreamTicker {
        StreamTicker {
            received_ms: 1,
            price: 1.0 + change_24h,
            open_24h: 1.0,
            quote_volume_24h,
            return_1m: None,
            return_5m: None,
            return_15m,
        }
    }

    #[test]
    fn short_term_surge_admission_accepts_either_early_or_established_momentum() {
        let universe = greed_strategy::UniverseConfig::default();
        assert!(is_surge_admission(
            &ticker(2_100_000.0, 0.15, Some(0.02)),
            &universe
        ));
        assert!(!is_surge_admission(
            &ticker(1_900_000.0, 0.15, Some(0.02)),
            &universe
        ));
        assert!(is_surge_admission(
            &ticker(2_100_000.0, 0.07, Some(0.02)),
            &universe
        ));
        assert!(is_surge_admission(
            &ticker(2_100_000.0, 0.15, Some(0.01)),
            &universe
        ));
        assert!(is_surge_admission(
            &ticker(2_100_000.0, 0.15, None),
            &universe
        ));
        assert!(!is_surge_admission(
            &ticker(2_100_000.0, 0.07, Some(0.01)),
            &universe
        ));
    }

    #[test]
    fn elapsed_candles_are_closed_after_a_missed_websocket_terminal_update() {
        let mut values = vec![
            Candle {
                open_ms: 0,
                close_ms: 899_999,
                open: 1.0,
                high: 2.0,
                low: 1.0,
                close: 2.0,
                quote_volume: 100.0,
                taker_buy_quote: Some(60.0),
                closed: false,
            },
            Candle {
                open_ms: 900_000,
                close_ms: 1_799_999,
                open: 2.0,
                high: 2.0,
                low: 1.5,
                close: 1.8,
                quote_volume: 50.0,
                taker_buy_quote: Some(20.0),
                closed: false,
            },
        ];

        heal_elapsed_candle_closes(&mut values, 1_000_000);

        assert!(values[0].closed);
        assert!(!values[1].closed);
    }

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
