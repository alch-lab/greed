use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use greed_kernel::{
    BookState, Candle, DataQuality, MicrostructureState, ObservationMeta, PriceLevel,
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

const STREAM_TTL_MS: i64 = 15_000;
const CANDLE_CACHE_LIMIT: usize = 200;
const TICKER_HISTORY_MS: i64 = 20 * 60_000;
const BOOK_FLOW_HISTORY_MS: i64 = 2 * 60_000;

#[derive(Debug, Clone)]
struct BookFlowObservation {
    event_ms: i64,
    raw_ofi_usd: f64,
    visible_top_usd: f64,
    mid: f64,
}

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
    /// Symbols with enough recent depth changes to calculate a 10-second
    /// snapshot OFI observation.
    pub book_flow_ready_symbols: usize,
}

#[derive(Default)]
struct StreamState {
    tickers: BTreeMap<String, StreamTicker>,
    ticker_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    candles: BTreeMap<(String, String), VecDeque<Candle>>,
    candle_update_ms: BTreeMap<(String, String), i64>,
    books: BTreeMap<String, BookState>,
    book_flow: BTreeMap<String, VecDeque<BookFlowObservation>>,
    trades: BTreeMap<String, VecDeque<(i64, bool, f64)>>,
    desired_symbols: BTreeSet<String>,
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
            let mut state = self.state.write().expect("stream state poisoned");
            state.telemetry.subscribed_symbols = normalized.len();
            state.desired_symbols = normalized.iter().cloned().collect();
            drop(state);
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
        let latest = trades.back().map(|value| value.0).unwrap_or_default();
        let flow_10s = state
            .book_flow
            .get(&symbol)
            .and_then(|values| aggregate_book_flow(values, now_ms, 10_000));
        let flow_60s = state
            .book_flow
            .get(&symbol)
            .and_then(|values| aggregate_book_flow(values, now_ms, 60_000));
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
            long_liquidations_60s: 0.0,
            short_liquidations_60s: 0.0,
            snapshot_ofi_10s: flow_10s.as_ref().map(|value| value.normalized_ofi),
            snapshot_ofi_60s: flow_60s.as_ref().map(|value| value.normalized_ofi),
            mid_return_bps_10s: flow_10s.as_ref().map(|value| value.mid_return_bps),
            mid_return_bps_60s: flow_60s.as_ref().map(|value| value.mid_return_bps),
            price_impact_bps_per_ofi_10s: flow_10s.as_ref().and_then(|value| {
                (value.normalized_ofi.abs() > 1e-9)
                    .then_some(value.mid_return_bps.abs() / value.normalized_ofi.abs())
            }),
            book_updates_10s: flow_10s.as_ref().map_or(0, |value| value.updates),
            book_updates_60s: flow_60s.as_ref().map_or(0, |value| value.updates),
        })
    }

    pub fn telemetry(&self) -> StreamTelemetry {
        let state = self.state.read().expect("stream state poisoned");
        let mut telemetry = state.telemetry.clone();
        telemetry.micro_candle_symbols = state
            .candles
            .iter()
            .filter(|((symbol, interval), values)| {
                state.desired_symbols.contains(symbol) && interval == "1m" && !values.is_empty()
            })
            .count();
        let now_ms = chrono::Utc::now().timestamp_millis();
        telemetry.book_flow_ready_symbols = state
            .book_flow
            .iter()
            .filter(|(symbol, values)| {
                state.desired_symbols.contains(*symbol)
                    && aggregate_book_flow(values, now_ms, 10_000).is_some()
            })
            .count();
        telemetry
    }
}

async fn run_supervisor(
    base_url: String,
    state: Arc<RwLock<StreamState>>,
    symbols: watch::Receiver<Vec<String>>,
) {
    let radar_url = format!(
        "{}/market/stream?streams=!ticker@arr",
        base_url.trim_end_matches('/')
    );
    tokio::spawn(run_connection(
        StreamRoute::Radar,
        radar_url,
        Arc::clone(&state),
    ));
    tokio::spawn(run_dynamic_connection(
        StreamRoute::Market,
        route_url(&base_url, StreamRoute::Market),
        Arc::clone(&state),
        symbols.clone(),
    ));
    run_dynamic_connection(
        StreamRoute::Public,
        route_url(&base_url, StreamRoute::Public),
        state,
        symbols,
    )
    .await;
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

async fn run_dynamic_connection(
    route: StreamRoute,
    url: String,
    state: Arc<RwLock<StreamState>>,
    mut symbols: watch::Receiver<Vec<String>>,
) {
    let mut backoff = 1u64;
    let mut request_id = 1u64;
    loop {
        if symbols.borrow().is_empty() {
            if symbols.changed().await.is_err() {
                return;
            }
            continue;
        }
        match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
            Err(_) => {
                set_connected(
                    &state,
                    route,
                    false,
                    Some("websocket connect timed out after 10 seconds".into()),
                );
                warn!(url=%url, "Binance dynamic websocket connection timed out");
            }
            Ok(Err(error)) => {
                set_connected(&state, route, false, Some(error.to_string()));
                warn!(error=%error, url=%url, "Binance dynamic websocket connection failed");
            }
            Ok(Ok((stream, _))) => {
                set_connected(&state, route, true, None);
                backoff = 1;
                let (mut writer, mut reader) = stream.split();
                let desired = route_streams(route, &symbols.borrow());
                if send_subscription_change(
                    &mut writer,
                    "SUBSCRIBE",
                    desired.iter().cloned().collect(),
                    request_id,
                )
                .await
                .is_err()
                {
                    set_connected(
                        &state,
                        route,
                        false,
                        Some("initial websocket subscription failed".into()),
                    );
                } else {
                    request_id += 1;
                    let mut active = desired;
                    loop {
                        tokio::select! {
                            changed = symbols.changed() => {
                                if changed.is_err() { return; }
                                let desired = route_streams(route, &symbols.borrow());
                                let additions: Vec<_> = desired.difference(&active).cloned().collect();
                                let removals: Vec<_> = active.difference(&desired).cloned().collect();
                                if !additions.is_empty() {
                                    if send_subscription_change(
                                        &mut writer,
                                        "SUBSCRIBE",
                                        additions,
                                        request_id,
                                    ).await.is_err() { break; }
                                    request_id += 1;
                                }
                                if !removals.is_empty() {
                                    if send_subscription_change(
                                        &mut writer,
                                        "UNSUBSCRIBE",
                                        removals,
                                        request_id,
                                    ).await.is_err() { break; }
                                    request_id += 1;
                                }
                                active = desired;
                            }
                            message = reader.next() => {
                                match message {
                                    Some(Ok(Message::Text(text))) => {
                                        let now_ms = chrono::Utc::now().timestamp_millis();
                                        let parsed = serde_json::from_str::<Value>(&text);
                                        if parsed.as_ref().is_ok_and(|value| value.get("code").is_some()) {
                                            record_parse_error(
                                                &state,
                                                format!("Binance websocket subscription rejected: {}", parsed.expect("checked above")),
                                            );
                                            break;
                                        }
                                        match parsed.map_err(anyhow::Error::from).and_then(|value| {
                                            handle_payload(&state, route, value, now_ms)
                                        }) {
                                            Ok(()) => record_message(&state, route, now_ms),
                                            Err(error) => record_parse_error(&state, error.to_string()),
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if writer.send(Message::Pong(payload)).await.is_err() { break; }
                                    }
                                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                                    _ => {}
                                }
                            }
                        }
                    }
                    set_connected(&state, route, false, Some("websocket disconnected".into()));
                }
            }
        }
        state
            .write()
            .expect("stream state poisoned")
            .telemetry
            .reconnects += 1;
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn send_subscription_change<S>(
    writer: &mut S,
    method: &str,
    params: Vec<String>,
    id: u64,
) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    if params.is_empty() {
        return Ok(());
    }
    writer
        .send(Message::Text(
            serde_json::json!({"method":method,"params":params,"id":id})
                .to_string()
                .into(),
        ))
        .await
        .context("send Binance websocket subscription change")
}

fn route_url(base: &str, route: StreamRoute) -> String {
    let path = match route {
        StreamRoute::Market => "market/ws",
        StreamRoute::Public => "public/ws",
        StreamRoute::Radar => "market/ws",
    };
    format!("{}/{path}", base.trim_end_matches('/'))
}

fn route_streams(route: StreamRoute, symbols: &[String]) -> BTreeSet<String> {
    match route {
        StreamRoute::Market => market_streams(symbols),
        StreamRoute::Public => public_streams(symbols),
        StreamRoute::Radar => BTreeSet::from(["!ticker@arr".into()]),
    }
}

fn market_streams(symbols: &[String]) -> BTreeSet<String> {
    let mut streams = BTreeSet::new();
    for symbol in symbols {
        let symbol = symbol.to_lowercase();
        streams.insert(format!("{symbol}@kline_15m"));
        streams.insert(format!("{symbol}@kline_5m"));
        streams.insert(format!("{symbol}@kline_1m"));
        streams.insert(format!("{symbol}@aggTrade"));
    }
    streams
}

fn public_streams(symbols: &[String]) -> BTreeSet<String> {
    symbols
        .iter()
        .map(|symbol| format!("{}@depth20@500ms", symbol.to_lowercase()))
        .collect()
}

fn handle_payload(
    state: &Arc<RwLock<StreamState>>,
    _route: StreamRoute,
    value: Value,
    received_ms: i64,
) -> Result<()> {
    if value.get("code").is_some() {
        return Err(anyhow!("Binance websocket control error: {value}"));
    }
    if value.get("id").is_some() && value.get("result").is_some() {
        return Ok(());
    }
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
        Some("aggTrade") => update_trade(state, payload, received_ms),
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
    let mut state = state.write().expect("stream state poisoned");
    if let Some(previous) = state.books.get(&symbol) {
        if let Some(observation) = book_flow_observation(previous, &book) {
            let values = state.book_flow.entry(symbol.clone()).or_default();
            values.push_back(observation);
            while values
                .front()
                .is_some_and(|value| value.event_ms < received_ms - BOOK_FLOW_HISTORY_MS)
            {
                values.pop_front();
            }
        }
    }
    state.books.insert(symbol, book);
    Ok(())
}

fn book_flow_observation(previous: &BookState, current: &BookState) -> Option<BookFlowObservation> {
    if previous.bid <= 0.0
        || previous.ask <= 0.0
        || current.bid <= 0.0
        || current.ask <= 0.0
        || previous.bids.is_empty()
        || previous.asks.is_empty()
        || current.bids.is_empty()
        || current.asks.is_empty()
    {
        return None;
    }
    let previous_bid = previous.bids.first()?;
    let current_bid = current.bids.first()?;
    let previous_ask = previous.asks.first()?;
    let current_ask = current.asks.first()?;
    let previous_bid_usd = previous_bid.price * previous_bid.quantity;
    let current_bid_usd = current_bid.price * current_bid.quantity;
    let previous_ask_usd = previous_ask.price * previous_ask.quantity;
    let current_ask_usd = current_ask.price * current_ask.quantity;

    let bid_flow = if current_bid.price > previous_bid.price {
        current_bid_usd
    } else if current_bid.price == previous_bid.price {
        current_bid_usd - previous_bid_usd
    } else {
        -previous_bid_usd
    };
    let ask_flow = if current_ask.price < previous_ask.price {
        -current_ask_usd
    } else if current_ask.price == previous_ask.price {
        previous_ask_usd - current_ask_usd
    } else {
        previous_ask_usd
    };
    Some(BookFlowObservation {
        event_ms: current.meta.event_ms,
        raw_ofi_usd: bid_flow + ask_flow,
        visible_top_usd: ((previous_bid_usd
            + current_bid_usd
            + previous_ask_usd
            + current_ask_usd)
            / 4.0)
            .max(f64::EPSILON),
        mid: (current.bid + current.ask) / 2.0,
    })
}

struct BookFlowAggregate {
    normalized_ofi: f64,
    mid_return_bps: f64,
    updates: u32,
}

fn aggregate_book_flow(
    values: &VecDeque<BookFlowObservation>,
    now_ms: i64,
    window_ms: i64,
) -> Option<BookFlowAggregate> {
    let selected: Vec<_> = values
        .iter()
        .filter(|value| value.event_ms >= now_ms - window_ms && value.event_ms <= now_ms + 1_000)
        .collect();
    if selected.len() < 2 {
        return None;
    }
    let raw_ofi = selected.iter().map(|value| value.raw_ofi_usd).sum::<f64>();
    let average_visible = selected
        .iter()
        .map(|value| value.visible_top_usd)
        .sum::<f64>()
        / selected.len() as f64;
    let first_mid = selected.first()?.mid;
    let last_mid = selected.last()?.mid;
    if first_mid <= 0.0 || average_visible <= 0.0 {
        return None;
    }
    Some(BookFlowAggregate {
        normalized_ofi: raw_ofi / average_visible,
        mid_return_bps: (last_mid / first_mid - 1.0) * 10_000.0,
        updates: u32::try_from(selected.len()).unwrap_or(u32::MAX),
    })
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
    } else if connected
        && state.telemetry.radar_connected
        && state.telemetry.market_connected
        && state.telemetry.public_connected
    {
        state.telemetry.last_error = None;
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

    fn test_book(
        event_ms: i64,
        bid: f64,
        bid_quantity: f64,
        ask: f64,
        ask_quantity: f64,
    ) -> BookState {
        BookState {
            meta: ObservationMeta {
                event_ms,
                received_ms: event_ms,
                expires_ms: event_ms + STREAM_TTL_MS,
                source: "test".into(),
                quality: DataQuality::Complete,
            },
            bid,
            ask,
            bid_depth_usd: bid * bid_quantity,
            ask_depth_usd: ask * ask_quantity,
            expected_buy_slippage_bps: None,
            expected_sell_slippage_bps: None,
            bids: vec![PriceLevel {
                price: bid,
                quantity: bid_quantity,
            }],
            asks: vec![PriceLevel {
                price: ask,
                quantity: ask_quantity,
            }],
        }
    }

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
    fn dynamic_streams_use_split_binance_routes() {
        let symbols = vec!["BTCUSDT".to_string(), "YBUSDT".to_string()];
        let market = market_streams(&symbols);
        let public = public_streams(&symbols);
        assert_eq!(
            route_url("wss://fstream.binance.com", StreamRoute::Market),
            "wss://fstream.binance.com/market/ws"
        );
        assert_eq!(
            route_url("wss://fstream.binance.com", StreamRoute::Public),
            "wss://fstream.binance.com/public/ws"
        );
        assert!(!market.contains("!ticker@arr"));
        assert!(market.contains("ybusdt@kline_5m"));
        assert!(market.contains("ybusdt@kline_1m"));
        assert!(market.contains("btcusdt@kline_5m"));
        assert!(market.contains("btcusdt@aggTrade"));
        assert!(!market.contains("btcusdt@forceOrder"));
        assert!(public.contains("btcusdt@depth20@500ms"));
    }

    #[test]
    fn subscription_changes_only_touch_changed_symbols() {
        let before = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let after = vec!["BTCUSDT".to_string(), "SOLUSDT".to_string()];
        let active = market_streams(&before);
        let desired = market_streams(&after);
        let additions: Vec<_> = desired.difference(&active).cloned().collect();
        let removals: Vec<_> = active.difference(&desired).cloned().collect();
        assert_eq!(additions.len(), 4);
        assert!(additions.iter().all(|value| value.starts_with("solusdt@")));
        assert_eq!(removals.len(), 4);
        assert!(removals.iter().all(|value| value.starts_with("ethusdt@")));
    }

    #[tokio::test]
    async fn dynamic_subscription_change_keeps_the_same_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(RwLock::new(StreamState::default()));
        let (sender, receiver) = watch::channel(vec!["BTCUSDT".to_string()]);
        let task = tokio::spawn(run_dynamic_connection(
            StreamRoute::Market,
            format!("ws://{address}/market/ws"),
            Arc::clone(&state),
            receiver,
        ));
        let (socket, _) = listener.accept().await.unwrap();
        let mut server = tokio_tungstenite::accept_async(socket).await.unwrap();
        let read_control = |message: Message| match message {
            Message::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
            other => panic!("unexpected websocket message: {other:?}"),
        };
        let initial = read_control(
            tokio::time::timeout(Duration::from_secs(1), server.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        assert_eq!(initial["method"], "SUBSCRIBE");
        assert_eq!(initial["params"].as_array().unwrap().len(), 4);

        sender
            .send(vec!["BTCUSDT".to_string(), "SOLUSDT".to_string()])
            .unwrap();
        let addition = read_control(
            tokio::time::timeout(Duration::from_secs(1), server.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        assert_eq!(addition["method"], "SUBSCRIBE");
        assert!(addition["params"]
            .as_array()
            .unwrap()
            .iter()
            .all(|value| value.as_str().unwrap().starts_with("solusdt@")));
        assert!(state.read().unwrap().telemetry.market_connected);
        assert_eq!(state.read().unwrap().telemetry.reconnects, 0);
        task.abort();
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

    #[test]
    fn snapshot_ofi_is_positive_when_bid_queue_grows() {
        let previous = test_book(1_000, 99.0, 10.0, 101.0, 10.0);
        let current = test_book(1_500, 99.0, 20.0, 101.0, 10.0);
        let observation = book_flow_observation(&previous, &current).unwrap();
        assert!(observation.raw_ofi_usd > 0.0);
    }

    #[test]
    fn snapshot_ofi_is_negative_when_ask_queue_grows() {
        let previous = test_book(1_000, 99.0, 10.0, 101.0, 10.0);
        let current = test_book(1_500, 99.0, 10.0, 101.0, 20.0);
        let observation = book_flow_observation(&previous, &current).unwrap();
        assert!(observation.raw_ofi_usd < 0.0);
    }

    #[test]
    fn aggregates_snapshot_ofi_and_price_response() {
        let values = VecDeque::from([
            BookFlowObservation {
                event_ms: 1_000,
                raw_ofi_usd: 1_000.0,
                visible_top_usd: 10_000.0,
                mid: 100.0,
            },
            BookFlowObservation {
                event_ms: 1_500,
                raw_ofi_usd: 2_000.0,
                visible_top_usd: 10_000.0,
                mid: 100.1,
            },
        ]);
        let aggregate = aggregate_book_flow(&values, 1_500, 10_000).unwrap();
        assert!((aggregate.normalized_ofi - 0.3).abs() < 1e-9);
        assert!((aggregate.mid_return_bps - 10.0).abs() < 1e-9);
        assert_eq!(aggregate.updates, 2);
    }
}
