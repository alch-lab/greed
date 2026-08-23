use crate::config::RuntimeConfig;
use anyhow::{anyhow, Result};
use greed_kernel::{
    AccountFrame, AssetClass, BookState, Candle, CandleSeries, DataQuality, DerivativesState,
    ExternalState, InstrumentFrame, MarketFrame, MarketKind, ObservationMeta, PriceLevel,
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
    endpoints: BTreeMap<String, EndpointTelemetry>,
}

pub struct BinancePaperSource {
    config: RuntimeConfig,
    http: Client,
    oi_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    last_success_ms: Option<i64>,
    last_error: Option<String>,
    telemetry: Mutex<ApiTelemetry>,
    discovery_prices: BTreeMap<String, VecDeque<(i64, f64)>>,
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
    pub return_1h_pct: Option<f64>,
}

#[derive(Debug, serde::Deserialize)]
struct SlowContextFile {
    as_of_ms: i64,
    #[serde(default)]
    symbols: BTreeMap<String, SlowSymbolContext>,
}

#[derive(Debug, serde::Deserialize)]
struct SlowSymbolContext {
    etf_daily_flow_usd: Option<f64>,
    etf_rolling_5d_flow_usd: Option<f64>,
    cme_basis_pct: Option<f64>,
}
impl BinancePaperSource {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let mut builder = Client::builder()
            .user_agent("greed-paper/0.1")
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
        })
    }
    pub async fn discover_altcoins(
        &mut self,
        strategy: &StrategyConfig,
        now_ms: i64,
    ) -> Result<UniverseDiscovery> {
        if !strategy.universe.dynamic_enabled {
            return Ok(UniverseDiscovery {
                as_of_ms: now_ms,
                symbols: strategy.altcoins.clone(),
                eligible_contracts: strategy.altcoins.len(),
                liquid_contracts: strategy.altcoins.len(),
                rolling_history_ready: 0,
                leaders: Vec::new(),
            });
        }
        let bases = self.futures_bases();
        let (exchange, tickers) = tokio::try_join!(
            self.get_from_bases(&bases, "/fapi/v1/exchangeInfo"),
            self.get_from_bases(&bases, "/fapi/v1/ticker/24hr")
        )?;
        let eligible: BTreeSet<String> = exchange["symbols"]
            .as_array()
            .ok_or_else(|| anyhow!("exchangeInfo symbols is not an array"))?
            .iter()
            .filter(|value| {
                value["status"] == "TRADING"
                    && value["contractType"] == "PERPETUAL"
                    && value["quoteAsset"] == "USDT"
                    && value
                        .get("underlyingType")
                        .and_then(Value::as_str)
                        .is_none_or(|kind| kind == "COIN")
            })
            .filter_map(|value| value["symbol"].as_str().map(str::to_owned))
            .filter(|symbol| !strategy.majors.contains(symbol))
            .collect();
        let mut rows = Vec::new();
        for ticker in tickers
            .as_array()
            .ok_or_else(|| anyhow!("24hr ticker response is not an array"))?
        {
            let Some(symbol) = ticker["symbol"].as_str() else {
                continue;
            };
            if !eligible.contains(symbol) {
                continue;
            }
            let parse = |key: &str| {
                ticker[key]
                    .as_str()
                    .and_then(|value| value.parse::<f64>().ok())
            };
            let (Some(price), Some(quote_volume), Some(change_24h_pct)) = (
                parse("lastPrice"),
                parse("quoteVolume"),
                parse("priceChangePercent"),
            ) else {
                continue;
            };
            if price <= 0.0 || quote_volume < strategy.universe.min_24h_quote_volume_usd {
                continue;
            }
            let history = self.discovery_prices.entry(symbol.into()).or_default();
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
                symbol.to_owned(),
                quote_volume,
                change_24h_pct / 100.0,
                return_1h,
            ));
        }
        let rolling_history_ready = rows
            .iter()
            .filter(|(_, _, _, return_1h)| return_1h.is_some())
            .count();
        let mut selected: BTreeSet<String> = strategy
            .altcoins
            .iter()
            .filter(|symbol| eligible.contains(*symbol))
            .cloned()
            .collect();
        let mut by_liquidity = rows.clone();
        by_liquidity.sort_by(|a, b| b.1.total_cmp(&a.1));
        for (symbol, _, _, _) in by_liquidity
            .iter()
            .take(strategy.universe.top_liquidity_names)
        {
            selected.insert(symbol.clone());
        }
        let mut by_movement = rows.clone();
        by_movement.sort_by(|a, b| {
            let score = |row: &(String, f64, f64, Option<f64>)| {
                row.3.unwrap_or(row.2).abs().max(row.2.abs() * 0.35)
            };
            score(b).total_cmp(&score(a))
        });
        for (symbol, _, _, _) in by_movement.iter().take(strategy.universe.top_mover_names) {
            selected.insert(symbol.clone());
        }
        let selected_scores: BTreeMap<_, _> = rows
            .iter()
            .map(|(symbol, volume, change, rolling)| {
                (
                    symbol,
                    rolling.unwrap_or(*change).abs().max(change.abs() * 0.35)
                        + volume.ln_1p() * 1e-6,
                )
            })
            .collect();
        let mut symbols: Vec<_> = selected.into_iter().collect();
        symbols.sort_by(|a, b| {
            selected_scores
                .get(b)
                .copied()
                .unwrap_or(0.0)
                .total_cmp(&selected_scores.get(a).copied().unwrap_or(0.0))
        });
        symbols.truncate(strategy.universe.max_altcoins);
        symbols.sort();
        let selected_symbols: BTreeSet<_> = symbols.iter().map(String::as_str).collect();
        let leaders = by_movement
            .into_iter()
            .filter(|(symbol, _, _, _)| selected_symbols.contains(symbol.as_str()))
            .take(20)
            .map(
                |(symbol, quote_volume, change_24h, return_1h)| UniverseLeader {
                    symbol,
                    quote_volume_24h_usd: quote_volume,
                    change_24h_pct: change_24h * 100.0,
                    return_1h_pct: return_1h.map(|value| value * 100.0),
                },
            )
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
        serde_json::json!({"last_success_ms":self.last_success_ms,"last_error":self.last_error,"paper_only":true,"telemetry":telemetry,"average_latency_ms":average_latency_ms})
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
    fn spot_bases(&self) -> Vec<String> {
        std::iter::once(self.config.binance_spot_base.clone())
            .chain(self.config.binance_spot_fallbacks.clone())
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
    async fn klines(
        &self,
        bases: &[String],
        path: &str,
        symbol: &str,
        market: MarketKind,
        now: i64,
    ) -> Result<CandleSeries> {
        let suffix = format!(
            "{path}?symbol={symbol}&interval=15m&limit={}",
            self.config.candle_limit
        );
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
            interval_ms: 900_000,
            meta: Self::meta(now, 120_000, "binance_klines", DataQuality::Complete),
            values: bars,
        })
    }
    async fn book(&self, symbol: &str, now: i64) -> Result<BookState> {
        let suffix = format!("/fapi/v1/depth?symbol={symbol}&limit=20");
        let value = self.get_from_bases(&self.futures_bases(), &suffix).await?;
        let parse = |name: &str| -> Result<Vec<(f64, f64)>> {
            value[name]
                .as_array()
                .ok_or_else(|| anyhow!("missing {name}"))?
                .iter()
                .map(|row| {
                    let row = row.as_array().ok_or_else(|| anyhow!("invalid book row"))?;
                    Ok((
                        row[0].as_str().unwrap_or("0").parse()?,
                        row[1].as_str().unwrap_or("0").parse()?,
                    ))
                })
                .collect()
        };
        let bids = parse("bids")?;
        let asks = parse("asks")?;
        let bid = bids.first().map(|v| v.0).unwrap_or(0.0);
        let ask = asks.first().map(|v| v.0).unwrap_or(0.0);
        let bid_depth = bids.iter().map(|(p, q)| p * q).sum();
        let ask_depth = asks.iter().map(|(p, q)| p * q).sum();
        Ok(BookState {
            meta: Self::meta(now, 30_000, "binance_depth", DataQuality::Complete),
            bid,
            ask,
            bid_depth_usd: bid_depth,
            ask_depth_usd: ask_depth,
            expected_buy_slippage_bps: sweep_slippage(&asks, 300.0, ask),
            expected_sell_slippage_bps: sweep_slippage(&bids, 300.0, bid),
            bids: bids
                .iter()
                .map(|(price, quantity)| PriceLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(price, quantity)| PriceLevel {
                    price: *price,
                    quantity: *quantity,
                })
                .collect(),
        })
    }
    async fn derivatives(
        &mut self,
        symbol: &str,
        price: f64,
        now: i64,
    ) -> Result<DerivativesState> {
        let bases = self.futures_bases();
        let oi_value = self
            .get_from_bases(&bases, &format!("/fapi/v1/openInterest?symbol={symbol}"))
            .await?;
        let premium = self
            .get_from_bases(&bases, &format!("/fapi/v1/premiumIndex?symbol={symbol}"))
            .await?;
        let oi_units: f64 = oi_value["openInterest"].as_str().unwrap_or("0").parse()?;
        let oi = oi_units * price;
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
            meta: Self::meta(now, 120_000, "binance_derivatives", DataQuality::Complete),
            open_interest_usd: Some(oi),
            open_interest_change_pct: change,
            funding_rate: premium["lastFundingRate"]
                .as_str()
                .and_then(|v| v.parse().ok()),
            basis_pct: premium["markPrice"]
                .as_str()
                .and_then(|v| v.parse::<f64>().ok())
                .zip(
                    premium["indexPrice"]
                        .as_str()
                        .and_then(|v| v.parse::<f64>().ok()),
                )
                .map(|(mark, index)| mark / index - 1.0),
            long_liquidations_usd: None,
            short_liquidations_usd: None,
        })
    }
    async fn coinbase_external(
        &self,
        symbol: &str,
        binance_spot: f64,
        now: i64,
    ) -> Result<ExternalState> {
        let asset = symbol.trim_end_matches("USDT");
        let asset_url = format!("{}/products/{asset}-USD/ticker", self.config.coinbase_base);
        let usdt_url = format!("{}/products/USDT-USD/ticker", self.config.coinbase_base);
        let asset_value = self.get(asset_url).await?;
        let usdt_value = self.get(usdt_url).await;
        let usd: f64 = asset_value["price"].as_str().unwrap_or("0").parse()?;
        let raw = usd / binance_spot - 1.0;
        let (true_premium, quality) = match usdt_value {
            Ok(value) => {
                let usdt: f64 = value["price"].as_str().unwrap_or("0").parse()?;
                (
                    Some(usd / (binance_spot * usdt) - 1.0),
                    DataQuality::Complete,
                )
            }
            Err(_) => (None, DataQuality::Partial),
        };
        let slow = self.read_slow_context(symbol, now);
        Ok(ExternalState {
            meta: Self::meta(now, 90_000, "coinbase_exchange", quality),
            coinbase_raw_premium_pct: Some(raw),
            coinbase_true_premium_pct: true_premium,
            etf_daily_flow_usd: slow.as_ref().and_then(|value| value.etf_daily_flow_usd),
            etf_rolling_5d_flow_usd: slow
                .as_ref()
                .and_then(|value| value.etf_rolling_5d_flow_usd),
            cme_basis_pct: slow.as_ref().and_then(|value| value.cme_basis_pct),
        })
    }
    fn read_slow_context(&self, symbol: &str, now: i64) -> Option<SlowSymbolContext> {
        let path = self.config.slow_context_path.as_deref()?;
        let text = std::fs::read_to_string(path).ok()?;
        let mut context: SlowContextFile = serde_json::from_str(&text).ok()?;
        // Confirmed ETF/CME context is slow, but an old value must not live
        // forever.  Forty-eight hours covers weekends without hiding outages.
        if now - context.as_of_ms > 48 * 3_600_000 || context.as_of_ms > now + 60_000 {
            return None;
        }
        context.symbols.remove(symbol)
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
        let futures_bases = self.futures_bases();
        let spot_bases = self.spot_bases();
        let mut warnings = Vec::new();
        for symbol in strategy.majors.iter().chain(strategy.altcoins.iter()) {
            let major = strategy.majors.contains(symbol);
            let perpetual = match self
                .klines(
                    &futures_bases,
                    "/fapi/v1/klines",
                    symbol,
                    MarketKind::Perpetual,
                    now,
                )
                .await
            {
                Ok(series) => series,
                Err(error) if major => {
                    warnings.push(format!("perpetual klines {symbol}: {error}"));
                    CandleSeries {
                        venue: "binance".into(),
                        market: MarketKind::Perpetual,
                        interval_ms: 900_000,
                        meta: Self::meta(now, 0, "binance_klines", DataQuality::Missing),
                        values: vec![],
                    }
                }
                Err(error) => {
                    warnings.push(format!("perpetual klines {symbol}: {error}"));
                    continue;
                }
            };
            let price = perpetual.values.last().map(|bar| bar.close).unwrap_or(0.0);
            let (spot, book, derivatives, external) = if major {
                let spot = match self
                    .klines(&spot_bases, "/api/v3/klines", symbol, MarketKind::Spot, now)
                    .await
                {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("spot klines {symbol}: {error}"));
                        None
                    }
                };
                let spot_price = spot
                    .as_ref()
                    .and_then(|series| series.values.last())
                    .map(|bar| bar.close)
                    .unwrap_or(price);
                let book = match self.book(symbol, now).await {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("depth {symbol}: {error}"));
                        None
                    }
                };
                let derivatives = if price > 0.0 {
                    match self.derivatives(symbol, price, now).await {
                        Ok(value) => Some(value),
                        Err(error) => {
                            warnings.push(format!("derivatives {symbol}: {error}"));
                            None
                        }
                    }
                } else {
                    None
                };
                let external = if spot_price > 0.0 {
                    match self.coinbase_external(symbol, spot_price, now).await {
                        Ok(value) => Some(value),
                        Err(error) => {
                            warnings.push(format!("external {symbol}: {error}"));
                            None
                        }
                    }
                } else {
                    None
                };
                (spot, book, derivatives, external)
            } else {
                let book = match self.book(symbol, now).await {
                    Ok(value) => Some(value),
                    Err(error) => {
                        warnings.push(format!("depth {symbol}: {error}"));
                        None
                    }
                };
                let derivatives = if price > 0.0 {
                    match self.derivatives(symbol, price, now).await {
                        Ok(value) => Some(value),
                        Err(error) => {
                            warnings.push(format!("derivatives {symbol}: {error}"));
                            None
                        }
                    }
                } else {
                    None
                };
                (None, book, derivatives, None)
            };
            instruments.insert(
                symbol.clone(),
                InstrumentFrame {
                    symbol: symbol.clone(),
                    asset_class: if major {
                        AssetClass::Major
                    } else {
                        AssetClass::Altcoin
                    },
                    price,
                    spot,
                    perpetual,
                    book,
                    derivatives,
                    external,
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

fn sweep_slippage(levels: &[(f64, f64)], notional: f64, reference: f64) -> Option<f64> {
    if reference <= 0.0 {
        return None;
    }
    let mut remaining = notional;
    let mut qty = 0.0;
    let mut cost = 0.0;
    for (price, available) in levels {
        let level_notional = price * available;
        let take = remaining.min(level_notional);
        qty += take / price;
        cost += take;
        remaining -= take;
        if remaining <= f64::EPSILON {
            break;
        }
    }
    if remaining > f64::EPSILON || qty <= 0.0 {
        return None;
    }
    let vwap = cost / qty;
    Some((vwap / reference - 1.0).abs() * 10_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_tracks_failures_rate_limits_and_latency() {
        let source = BinancePaperSource::new(RuntimeConfig::default()).unwrap();
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
