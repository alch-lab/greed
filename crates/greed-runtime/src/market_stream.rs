use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use greed_kernel::{
    BookState, Candle, DataQuality, MicrostructureState, ObservationMeta, PriceLevel,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

const STREAM_TTL_MS: i64 = 15_000;
const CANDLE_CACHE_LIMIT: usize = 200;
const TICKER_HISTORY_MS: i64 = 20 * 60_000;

#[derive(Debug, Clone)]
pub struct StreamTicker {
    pub received_ms: i64,
    pub price: f64,
    pub open_24h: f64,
    pub quote_volume_24h: f64,
    pub return_1m: Option<f64>,
    pub return_5m: Option<f64>,
    pub return_15m: Option<f64>,
}

impl StreamTicker {
    pub fn change_24h(&self) -> f64 {
        self.price / self.open_24h.max(f64::EPSILON) - 1.0
    }
}

#[derive(Debug, Clone)]
pub struct StreamMark {
    pub event_ms: i64,
    pub received_ms: i64,
    pub mark_price: f64,
    pub index_price: f64,
    pub funding_rate: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StreamTelemetry {
    pub radar_connected: bool,
    pub market_connected: bool,
    pub public_connected: bool,
    pub last_radar_message_ms: Option<i64>,
    pub last_market_message_ms: Option<i64>,
    pub last_public_message_ms: Option<i64>,
    pub radar_messages: u64,
    pub market_messages: u64,
    pub public_messages: u64,
    pub reconnects: u64,
    pub parse_errors: u64,
    pub last_error: Option<String>,
    pub subscribed_symbols: usize,
    pub micro_candle_symbols: usize,
}

#[derive(Default)]
struct StreamState {
    tickers: BTreeMap<String, StreamTicker>,
    ticker_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    candles: BTreeMap<(String, String), VecDeque<Candle>>,
    candle_update_ms: BTreeMap<(String, String), i64>,
    books: BTreeMap<String, BookState>,
    marks: BTreeMap<String, StreamMark>,
    trades: BTreeMap<String, VecDeque<(i64, bool, f64)>>,
    liquidations: BTreeMap<String, VecDeque<(i64, bool, f64)>>,
    telemetry: StreamTelemetry,
}

#[derive(Clone)]
pub struct MarketStreamHub {
    state: Arc<RwLock<StreamState>>,
    symbols: watch::Sender<Vec<String>>,
}

impl MarketStreamHub {
    pub fn start(base_url: String) -> Self {
        let state = Arc::new(RwLock::new(StreamState::default()));
        let (symbols, receiver) = watch::channel(Vec::new());
        tokio::spawn(run_supervisor(base_url, Arc::clone(&state), receiver));
        Self { state, symbols }
    }

    pub fn set_symbols(&self, symbols: &[String]) {
        let mut normalized: Vec<_> = symbols.iter().map(|value| value.to_uppercase()).collect();
        normalized.sort();
        normalized.dedup();
        if *self.symbols.borrow() != normalized {
            let _ = self.symbols.send(normalized);
        }
    }

    pub fn tickers(&self) -> BTreeMap<String, StreamTicker> {
        self.state
            .read()
            .expect("stream state poisoned")
            .tickers
            .clone()
    }

    pub fn candles(&self, symbol: &str, interval: &str) -> (Vec<Candle>, Option<i64>) {
        let state = self.state.read().expect("stream state poisoned");
        let key = (symbol.to_uppercase(), interval.into());
        (
            state
                .candles
                .get(&key)
                .map(|values| values.iter().cloned().collect())
                .unwrap_or_default(),
            state.candle_update_ms.get(&key).copied(),
        )
    }

    pub fn book(&self, symbol: &str, now_ms: i64) -> Option<BookState> {
        self.state
            .read()
            .expect("stream state poisoned")
            .books
            .get(&symbol.to_uppercase())
            .cloned()
            .map(|mut book| {
                if now_ms > book.meta.expires_ms {
                    book.meta.quality = DataQuality::Stale;
                }
                book
            })
    }

    pub fn mark(&self, symbol: &str) -> Option<StreamMark> {
        self.state
            .read()
            .expect("stream state poisoned")
            .marks
            .get(&symbol.to_uppercase())
            .cloned()
    }

    pub fn microstructure(&self, symbol: &str, now_ms: i64) -> Option<MicrostructureState> {
        let symbol = symbol.to_uppercase();
        let state = self.state.read().expect("stream state poisoned");
        let trades = state.trades.get(&symbol)?;
        let mut buy = 0.0;
        let mut sell = 0.0;
        for (_, is_buy, notional) in trades.iter().filter(|(ts, _, _)| *ts >= now_ms - 60_000) {
            if *is_buy {
                buy += notional
            } else {
                sell += notional
            }
        }
        let mut long_liq = 0.0;
        let mut short_liq = 0.0;
        if let Some(values) = state.liquidations.get(&symbol) {
            for (_, is_long, notional) in values.iter().filter(|(ts, _, _)| *ts >= now_ms - 60_000)
            {
                if *is_long {
                    long_liq += notional
                } else {
                    short_liq += notional
                }
            }
        }
        let latest = trades.back().map(|value| value.0).unwrap_or_default();
        Some(MicrostructureState {
            meta: ObservationMeta {
                event_ms: latest,
                received_ms: latest,
                expires_ms: latest + STREAM_TTL_MS,
                source: "binance_ws_agg_trade_force_order".into(),
                quality: if now_ms - latest <= STREAM_TTL_MS {
                    DataQuality::Complete
                } else {
                    DataQuality::Stale
                },
            },
            buy_notional_60s: buy,
            sell_notional_60s: sell,
            long_liquidations_60s: long_liq,
            short_liquidations_60s: short_liq,
        })
    }

    pub fn telemetry(&self) -> StreamTelemetry {
        let state = self.state.read().expect("stream state poisoned");
        let mut telemetry = state.telemetry.clone();
        telemetry.micro_candle_symbols = state
            .candles
            .iter()
            .filter(|((_, interval), values)| interval == "1m" && !values.is_empty())
            .count();
        telemetry
    }
}

async fn run_supervisor(
    base_url: String,
    state: Arc<RwLock<StreamState>>,
    mut symbols: watch::Receiver<Vec<String>>,
) {
    let radar_url = format!(
        "{}/stream?streams=!ticker@arr",
        base_url.trim_end_matches('/')
    );
    tokio::spawn(run_connection(
        StreamRoute::Radar,
        radar_url,
        Arc::clone(&state),
    ));
    loop {
        let active = symbols.borrow().clone();
        if active.is_empty() {
            if symbols.changed().await.is_err() {
                return;
            }
            continue;
        }
        {
            let mut state = state.write().expect("stream state poisoned");
            state.telemetry.subscribed_symbols = active.len();
            state.telemetry.market_connected = false;
            state.telemetry.public_connected = false;
        }
        let market_url = market_url(&base_url, &active);
        let public_url = public_url(&base_url, &active);
        let mut market = tokio::spawn(run_connection(
            StreamRoute::Market,
            market_url,
            Arc::clone(&state),
        ));
        let mut public = tokio::spawn(run_connection(
            StreamRoute::Public,
            public_url,
            Arc::clone(&state),
        ));
        tokio::select! {
            changed = symbols.changed() => {
                market.abort();
                public.abort();
                if changed.is_err() { return; }
            }
            _ = &mut market => {
                public.abort();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ = &mut public => {
                market.abort();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum StreamRoute {
    Radar,
    Market,
    Public,
}

async fn run_connection(route: StreamRoute, url: String, state: Arc<RwLock<StreamState>>) {
    let mut backoff = 1u64;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
            Err(_) => {
                set_connected(
                    &state,
                    route,
                    false,
                    Some("websocket connect timed out after 10 seconds".into()),
                );
                warn!(url=%url, "Binance market websocket connection timed out");
            }
            Ok(Ok((stream, _))) => {
                set_connected(&state, route, true, None);
                backoff = 1;
                let (mut writer, mut reader) = stream.split();
                while let Some(message) = reader.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            match serde_json::from_str::<Value>(&text)
                                .map_err(anyhow::Error::from)
                                .and_then(|value| handle_payload(&state, route, value, now_ms))
                            {
                                Ok(()) => record_message(&state, route, now_ms),
                                Err(error) => record_parse_error(&state, error.to_string()),
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if writer.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
                set_connected(&state, route, false, Some("websocket disconnected".into()));
            }
            Ok(Err(error)) => {
                set_connected(&state, route, false, Some(error.to_string()));
                warn!(error=%error, "Binance market websocket connection failed");
            }
        }
        {
            let mut state = state.write().expect("stream state poisoned");
            state.telemetry.reconnects += 1;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

fn market_url(base: &str, symbols: &[String]) -> String {
    let mut streams = Vec::new();
    for symbol in symbols {
        let symbol = symbol.to_lowercase();
        streams.push(format!("{symbol}@kline_15m"));
        streams.push(format!("{symbol}@kline_5m"));
        streams.push(format!("{symbol}@kline_1m"));
        streams.push(format!("{symbol}@markPrice@1s"));
        streams.push(format!("{symbol}@aggTrade"));
        streams.push(format!("{symbol}@forceOrder"));
    }
    format!(
        "{}/stream?streams={}",
        base.trim_end_matches('/'),
        streams.join("/")
    )
}

fn public_url(base: &str, symbols: &[String]) -> String {
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@depth20@1000ms", symbol.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    format!("{}/stream?streams={streams}", base.trim_end_matches('/'))
}

fn handle_payload(
    state: &Arc<RwLock<StreamState>>,
    _route: StreamRoute,
    value: Value,
    received_ms: i64,
) -> Result<()> {
    let payload = value.get("data").unwrap_or(&value);
    if let Some(values) = payload.as_array() {
        for value in values {
            if value
                .get("st")
                .and_then(Value::as_i64)
                .is_some_and(|kind| kind != 1)
            {
                continue;
            }
            if value.get("e").and_then(Value::as_str) == Some("24hrTicker") {
                update_ticker(state, value, received_ms)?;
            }
        }
        return Ok(());
    }
    if payload
        .get("st")
        .and_then(Value::as_i64)
        .is_some_and(|kind| kind != 1)
    {
        return Ok(());
    }
    match payload.get("e").and_then(Value::as_str) {
        Some("kline") => update_kline(state, payload, received_ms),
        Some("markPriceUpdate") => update_mark(state, payload, received_ms),
        Some("aggTrade") => update_trade(state, payload, received_ms),
        Some("forceOrder") => update_liquidation(state, payload, received_ms),
        Some("24hrTicker") => update_ticker(state, payload, received_ms),
        _ if (payload.get("b").is_some() || payload.get("bids").is_some())
            && (payload.get("a").is_some() || payload.get("asks").is_some()) =>
        {
            update_depth(state, payload, received_ms)
        }
        _ => Ok(()),
    }
}

fn update_trade(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let symbol = string(value, "s")?.to_uppercase();
    let notional = number(value, "p")? * number(value, "q")?;
    let is_taker_buy = !value.get("m").and_then(Value::as_bool).unwrap_or(false);
    let event_ms = value
        .get("T")
        .and_then(Value::as_i64)
        .unwrap_or(received_ms);
    let mut state = state.write().expect("stream state poisoned");
    let values = state.trades.entry(symbol).or_default();
    values.push_back((event_ms, is_taker_buy, notional));
    while values
        .front()
        .is_some_and(|(ts, _, _)| *ts < received_ms - 120_000)
    {
        values.pop_front();
    }
    Ok(())
}

fn update_liquidation(
    state: &Arc<RwLock<StreamState>>,
    value: &Value,
    received_ms: i64,
) -> Result<()> {
    let order = value
        .get("o")
        .ok_or_else(|| anyhow!("missing force order payload"))?;
    let symbol = string(order, "s")?.to_uppercase();
    let price = optional_number(order, "ap")
        .filter(|v| *v > 0.0)
        .unwrap_or(number(order, "p")?);
    let quantity = optional_number(order, "z")
        .filter(|v| *v > 0.0)
        .unwrap_or(number(order, "q")?);
    let is_long_liquidation = string(order, "S")? == "SELL";
    let event_ms = order
        .get("T")
        .and_then(Value::as_i64)
        .unwrap_or(received_ms);
    let mut state = state.write().expect("stream state poisoned");
    let values = state.liquidations.entry(symbol).or_default();
    values.push_back((event_ms, is_long_liquidation, price * quantity));
    while values
        .front()
        .is_some_and(|(ts, _, _)| *ts < received_ms - 120_000)
    {
        values.pop_front();
    }
    Ok(())
}

fn update_ticker(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let symbol = string(value, "s")?.to_uppercase();
    let price = number(value, "c")?;
    let open = number(value, "o")?;
    let quote_volume = number(value, "q")?;
    if price <= 0.0 || open <= 0.0 {
        return Err(anyhow!("invalid ticker price for {symbol}"));
    }
    let mut state = state.write().expect("stream state poisoned");
    let history = state.ticker_history.entry(symbol.clone()).or_default();
    while history
        .front()
        .is_some_and(|(ts, _)| *ts < received_ms - TICKER_HISTORY_MS)
    {
        history.pop_front();
    }
    let prior_return = |age_ms: i64| {
        history
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= received_ms - age_ms)
            .map(|(_, prior)| price / prior - 1.0)
    };
    let return_1m = prior_return(60_000);
    let return_5m = prior_return(5 * 60_000);
    let return_15m = prior_return(15 * 60_000);
    history.push_back((received_ms, price));
    state.tickers.insert(
        symbol,
        StreamTicker {
            received_ms,
            price,
            open_24h: open,
            quote_volume_24h: quote_volume,
            return_1m,
            return_5m,
            return_15m,
        },
    );
    Ok(())
}

fn update_kline(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let kline = value
        .get("k")
        .ok_or_else(|| anyhow!("missing kline payload"))?;
    let symbol = string(kline, "s")?.to_uppercase();
    let interval = string(kline, "i")?.to_string();
    let candle = Candle {
        open_ms: integer(kline, "t")?,
        close_ms: integer(kline, "T")?,
        open: number(kline, "o")?,
        high: number(kline, "h")?,
        low: number(kline, "l")?,
        close: number(kline, "c")?,
        quote_volume: number(kline, "q")?,
        taker_buy_quote: optional_number(kline, "Q"),
        closed: kline.get("x").and_then(Value::as_bool).unwrap_or(false),
    };
    let mut state = state.write().expect("stream state poisoned");
    let key = (symbol, interval);
    state.candle_update_ms.insert(key.clone(), received_ms);
    let values = state.candles.entry(key).or_default();
    if values
        .back()
        .is_some_and(|value| value.open_ms == candle.open_ms)
    {
        values.pop_back();
    }
    values.push_back(candle);
    while values.len() > CANDLE_CACHE_LIMIT {
        values.pop_front();
    }
    Ok(())
}

fn update_mark(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let symbol = string(value, "s")?.to_uppercase();
    state.write().expect("stream state poisoned").marks.insert(
        symbol,
        StreamMark {
            event_ms: value
                .get("E")
                .and_then(Value::as_i64)
                .unwrap_or(received_ms),
            received_ms,
            mark_price: number(value, "p")?,
            index_price: number(value, "i")?,
            funding_rate: optional_number(value, "r"),
        },
    );
    Ok(())
}

fn update_depth(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let symbol = string(value, "s")?.to_uppercase();
    let parse = |short: &str, long: &str| -> Result<Vec<(f64, f64)>> {
        value
            .get(short)
            .or_else(|| value.get(long))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("missing depth {short}"))?
            .iter()
            .map(|row| {
                let row = row.as_array().ok_or_else(|| anyhow!("invalid depth row"))?;
                Ok((parse_value(&row[0])?, parse_value(&row[1])?))
            })
            .collect()
    };
    let bids = parse("b", "bids")?;
    let asks = parse("a", "asks")?;
    let bid = bids.first().map(|value| value.0).unwrap_or_default();
    let ask = asks.first().map(|value| value.0).unwrap_or_default();
    let event_ms = value
        .get("E")
        .and_then(Value::as_i64)
        .unwrap_or(received_ms);
    let book = BookState {
        meta: ObservationMeta {
            event_ms,
            received_ms,
            expires_ms: received_ms + STREAM_TTL_MS,
            source: "binance_ws_depth20".into(),
            quality: DataQuality::Complete,
        },
        bid,
        ask,
        bid_depth_usd: bids.iter().map(|(price, quantity)| price * quantity).sum(),
        ask_depth_usd: asks.iter().map(|(price, quantity)| price * quantity).sum(),
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
    };
    state
        .write()
        .expect("stream state poisoned")
        .books
        .insert(symbol, book);
    Ok(())
}

fn set_connected(
    state: &Arc<RwLock<StreamState>>,
    route: StreamRoute,
    connected: bool,
    error: Option<String>,
) {
    let mut state = state.write().expect("stream state poisoned");
    match route {
        StreamRoute::Radar => state.telemetry.radar_connected = connected,
        StreamRoute::Market => state.telemetry.market_connected = connected,
        StreamRoute::Public => state.telemetry.public_connected = connected,
    }
    if let Some(error) = error {
        state.telemetry.last_error = Some(error);
    }
}

fn record_message(state: &Arc<RwLock<StreamState>>, route: StreamRoute, now_ms: i64) {
    let mut state = state.write().expect("stream state poisoned");
    match route {
        StreamRoute::Radar => {
            state.telemetry.radar_messages += 1;
            state.telemetry.last_radar_message_ms = Some(now_ms);
        }
        StreamRoute::Market => {
            state.telemetry.market_messages += 1;
            state.telemetry.last_market_message_ms = Some(now_ms);
        }
        StreamRoute::Public => {
            state.telemetry.public_messages += 1;
            state.telemetry.last_public_message_ms = Some(now_ms);
        }
    }
}

fn record_parse_error(state: &Arc<RwLock<StreamState>>, error: String) {
    let mut state = state.write().expect("stream state poisoned");
    state.telemetry.parse_errors += 1;
    state.telemetry.last_error = Some(error);
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string field {key}"))
}

fn integer(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing integer field {key}"))
}

fn number(value: &Value, key: &str) -> Result<f64> {
    value
        .get(key)
        .map(parse_value)
        .transpose()?
        .with_context(|| format!("missing number field {key}"))
}

fn optional_number(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|value| parse_value(value).ok())
}

fn parse_value(value: &Value) -> Result<f64> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value.as_f64())
        .ok_or_else(|| anyhow!("invalid numeric value"))
}

fn sweep_slippage(levels: &[(f64, f64)], notional: f64, reference: f64) -> Option<f64> {
    if reference <= 0.0 {
        return None;
    }
    let mut remaining = notional;
    let mut quantity = 0.0;
    let mut cost = 0.0;
    for (price, available) in levels {
        let take = remaining.min(price * available);
        quantity += take / price;
        cost += take;
        remaining -= take;
        if remaining <= f64::EPSILON {
            break;
        }
    }
    if remaining > f64::EPSILON || quantity <= 0.0 {
        return None;
    }
    Some((cost / quantity / reference - 1.0).abs() * 10_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_combined_ticker_kline_and_depth_messages() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        handle_payload(
            &state,
            StreamRoute::Market,
            serde_json::json!({"stream":"!ticker@arr","data":[{"e":"24hrTicker","E":10,"s":"YBUSDT","c":"2","o":"1","q":"30000000","st":1},{"e":"24hrTicker","s":"BTCUSD_PERP","c":"2","o":"1","q":"1","st":2}]}),
            10,
        )
        .unwrap();
        handle_payload(
            &state,
            StreamRoute::Market,
            serde_json::json!({"data":{"e":"kline","s":"YBUSDT","k":{"t":1,"T":2,"s":"YBUSDT","i":"5m","o":"1","c":"2","h":"2","l":"1","q":"100","Q":"60","x":true}}}),
            10,
        )
        .unwrap();
        handle_payload(
            &state,
            StreamRoute::Public,
            serde_json::json!({"data":{"E":10,"s":"YBUSDT","b":[["1.9","100"]],"a":[["2.1","100"]]}}),
            10,
        )
        .unwrap();
        let locked = state.read().unwrap();
        assert_eq!(locked.tickers.len(), 1);
        assert_eq!(locked.candles.len(), 1);
        assert_eq!(locked.books["YBUSDT"].bid, 1.9);
    }

    #[test]
    fn stream_urls_use_split_binance_routes() {
        let symbols = vec!["BTCUSDT".to_string(), "YBUSDT".to_string()];
        let market = market_url("wss://fstream.binance.com", &symbols);
        let public = public_url("wss://fstream.binance.com", &symbols);
        assert!(!market.contains("!ticker@arr"));
        assert!(market.contains("/stream?streams="));
        assert!(market.contains("ybusdt@kline_5m"));
        assert!(market.contains("ybusdt@kline_1m"));
        assert!(market.contains("btcusdt@kline_5m"));
        assert!(market.contains("btcusdt@aggTrade"));
        assert!(market.contains("btcusdt@forceOrder"));
        assert!(public.contains("/stream?streams="));
    }

    #[test]
    fn ticker_radar_keeps_short_interval_returns() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        update_ticker(
            &state,
            &serde_json::json!({"s":"YBUSDT","c":"1.0","o":"0.9","q":"30000000"}),
            1_000,
        )
        .unwrap();
        update_ticker(
            &state,
            &serde_json::json!({"s":"YBUSDT","c":"1.1","o":"0.9","q":"31000000"}),
            61_000,
        )
        .unwrap();
        let ticker = state.read().unwrap().tickers["YBUSDT"].clone();
        assert!((ticker.return_1m.unwrap() - 0.10).abs() < 1e-9);
        assert!(ticker.return_5m.is_none());
    }
}
