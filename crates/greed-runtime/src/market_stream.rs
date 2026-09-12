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
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

const STREAM_TTL_MS: i64 = 15_000;
const CANDLE_CACHE_LIMIT: usize = 200;
const TICKER_HISTORY_MS: i64 = 20 * 60_000;
const BOOK_FLOW_HISTORY_MS: i64 = 2 * 60_000;
// Binance sends a protocol ping every three minutes. A 45-second watchdog
// treated quiet, low-volume shards as dead and created its own reconnect
// storm. Five minutes still detects a wedged reader while allowing the
// exchange heartbeat to prove the socket is alive.
const STREAM_IDLE_TIMEOUT_SECS: u64 = 5 * 60;
// Keep independent failure domains even though every route fits within
// Binance's per-connection stream ceiling. A peer reset must warm only half
// the universe instead of simultaneously removing every executable book.
const MARKET_STREAM_SHARDS: usize = 2;
const TRADE_STREAM_SHARDS: usize = 2;
// Depth snapshots are substantially larger than trades or candles. Keep four
// small failure domains so a peer reset affects roughly one quarter of the
// universe without duplicating every high-volume depth stream.
const PUBLIC_STREAM_SHARDS: usize = 4;
const SYMBOL_WARMUP_MS: i64 = 10_000;
const CLIENT_HEARTBEAT_SECS: u64 = 30;

#[derive(Debug, Clone)]
struct BookFlowObservation {
    received_ms: i64,
    raw_ofi_usd: f64,
    visible_top_usd: f64,
    mid: f64,
}

#[derive(Debug, Clone)]
struct LiquidationObservation {
    event_ms: i64,
    received_ms: i64,
    is_long_liquidation: bool,
    notional_usd: f64,
    pressure_depth_usd: f64,
    mid: f64,
    book_received_ms: i64,
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
    pub market_shards_connected: usize,
    pub market_shards_total: usize,
    pub trade_connected: bool,
    pub trade_shards_connected: usize,
    pub trade_shards_total: usize,
    pub public_connected: bool,
    pub public_shards_connected: usize,
    pub public_shards_total: usize,
    pub last_radar_message_ms: Option<i64>,
    pub last_market_message_ms: Option<i64>,
    pub last_trade_message_ms: Option<i64>,
    pub last_public_message_ms: Option<i64>,
    pub radar_messages: u64,
    pub market_messages: u64,
    pub trade_messages: u64,
    pub public_messages: u64,
    pub reconnects: u64,
    pub radar_reconnects: u64,
    pub market_reconnects: u64,
    pub trade_reconnects: u64,
    pub public_reconnects: u64,
    pub last_disconnect_ms: Option<i64>,
    pub last_disconnect_reason: Option<String>,
    pub parse_errors: u64,
    pub last_error: Option<String>,
    pub subscribed_symbols: usize,
    pub micro_candle_symbols: usize,
    /// Symbols with enough recent depth changes to calculate a 10-second
    /// snapshot OFI observation.
    pub book_flow_ready_symbols: usize,
    /// Symbols with a fresh executable top-20 order-book snapshot.
    pub book_ready_symbols: usize,
}

#[derive(Default)]
struct StreamState {
    tickers: BTreeMap<String, StreamTicker>,
    ticker_history: BTreeMap<String, VecDeque<(i64, f64)>>,
    candles: BTreeMap<(String, String), VecDeque<Candle>>,
    candle_update_ms: BTreeMap<(String, String), i64>,
    books: BTreeMap<String, BookState>,
    // Partial depth snapshots carry a final update id. With redundant depth
    // subscriptions both sockets deliver the same update; retain the id so
    // the duplicate refreshes freshness without manufacturing a second OFI
    // observation.
    book_update_ids: BTreeMap<String, u64>,
    book_flow: BTreeMap<String, VecDeque<BookFlowObservation>>,
    trades: BTreeMap<String, VecDeque<(i64, bool, f64)>>,
    /// Binance liquidation stream snapshots. Binance publishes at most the
    /// largest forced order per symbol in each 1s window, so these values are
    /// useful event context but are not complete liquidation volume.
    liquidations: BTreeMap<String, VecDeque<LiquidationObservation>>,
    desired_symbols: BTreeSet<String>,
    symbol_admitted_ms: BTreeMap<String, i64>,
    market_connections: [bool; MARKET_STREAM_SHARDS],
    trade_connections: [bool; TRADE_STREAM_SHARDS],
    public_connections: [bool; PUBLIC_STREAM_SHARDS],
    telemetry: StreamTelemetry,
}

#[derive(Clone)]
pub struct MarketStreamHub {
    state: Arc<RwLock<StreamState>>,
    symbols: watch::Sender<Vec<String>>,
}

impl MarketStreamHub {
    pub fn start(base_url: String) -> Self {
        let mut initial_state = StreamState::default();
        initial_state.telemetry.market_shards_total = MARKET_STREAM_SHARDS;
        initial_state.telemetry.trade_shards_total = TRADE_STREAM_SHARDS;
        initial_state.telemetry.public_shards_total = PUBLIC_STREAM_SHARDS;
        let state = Arc::new(RwLock::new(initial_state));
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
            let now_ms = chrono::Utc::now().timestamp_millis();
            for symbol in &normalized {
                state
                    .symbol_admitted_ms
                    .entry(symbol.clone())
                    .or_insert(now_ms);
            }
            state
                .symbol_admitted_ms
                .retain(|symbol, _| normalized.binary_search(symbol).is_ok());
            state.telemetry.subscribed_symbols = normalized.len();
            state.desired_symbols = normalized.iter().cloned().collect();
            refresh_public_connection_health(&mut state);
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

    pub fn symbol_is_warming(&self, symbol: &str, now_ms: i64) -> bool {
        self.state
            .read()
            .expect("stream state poisoned")
            .symbol_admitted_ms
            .get(&symbol.to_uppercase())
            .is_some_and(|admitted_ms| now_ms - admitted_ms < SYMBOL_WARMUP_MS)
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
        let mut long_liquidations = 0.0;
        let mut short_liquidations = 0.0;
        if let Some(values) = state.liquidations.get(&symbol) {
            for value in values
                .iter()
                .filter(|value| value.received_ms >= now_ms - 60_000)
            {
                if value.is_long_liquidation {
                    long_liquidations += value.notional_usd;
                } else {
                    short_liquidations += value.notional_usd;
                }
            }
        }
        let liquidation_3s = state.liquidations.get(&symbol).and_then(|values| {
            aggregate_liquidation_window(
                values,
                state.book_flow.get(&symbol),
                state.books.get(&symbol),
                3_000,
            )
        });
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
                source: "binance_ws_agg_trade_force_order_snapshot".into(),
                quality: if now_ms - latest <= STREAM_TTL_MS {
                    DataQuality::Complete
                } else {
                    DataQuality::Stale
                },
            },
            buy_notional_60s: buy,
            sell_notional_60s: sell,
            long_liquidations_60s: long_liquidations,
            short_liquidations_60s: short_liquidations,
            long_liquidations_3s: liquidation_3s.as_ref().map(|value| value.long_notional_usd),
            short_liquidations_3s: liquidation_3s
                .as_ref()
                .map(|value| value.short_notional_usd),
            liquidation_dominance_3s: liquidation_3s.as_ref().map(|value| value.dominance),
            liquidation_depth_ratio_3s: liquidation_3s.as_ref().map(|value| value.depth_ratio),
            liquidation_aligned_return_bps_3s: liquidation_3s
                .as_ref()
                .and_then(|value| value.aligned_return_bps),
            liquidation_reversal_bps: liquidation_3s.as_ref().and_then(|value| value.reversal_bps),
            liquidation_event_ms: liquidation_3s.as_ref().map(|value| value.event_ms),
            liquidation_received_ms: liquidation_3s.as_ref().map(|value| value.received_ms),
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

    /// Whether a subscribed symbol has a fresh liquidation burst that already
    /// clears the event-strength gates. Slow REST cache maintenance yields to
    /// these plausible setups, without starving on every tiny forced order.
    pub fn has_recent_liquidation_setup(
        &self,
        now_ms: i64,
        window_ms: i64,
        max_age_ms: i64,
        min_dominance: f64,
        min_depth_ratio: f64,
    ) -> bool {
        let state = self.state.read().expect("stream state poisoned");
        state.desired_symbols.iter().any(|symbol| {
            let Some(values) = state.liquidations.get(symbol) else {
                return false;
            };
            let Some(event) = values.back() else {
                return false;
            };
            if event.received_ms > now_ms + 1_000
                || now_ms.saturating_sub(event.received_ms) > max_age_ms
            {
                return false;
            }
            aggregate_liquidation_window(
                values,
                state.book_flow.get(symbol),
                state.books.get(symbol),
                window_ms,
            )
            .is_some_and(|value| {
                value.dominance >= min_dominance && value.depth_ratio >= min_depth_ratio
            })
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
        telemetry.book_ready_symbols = state
            .books
            .iter()
            .filter(|(symbol, book)| {
                state.desired_symbols.contains(*symbol) && book.meta.usable_at(now_ms)
            })
            .count();
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
    for shard in 0..MARKET_STREAM_SHARDS {
        tokio::spawn(run_dynamic_connection(
            StreamRoute::Market(shard),
            route_url(&base_url, StreamRoute::Market(shard)),
            Arc::clone(&state),
            symbols.clone(),
        ));
    }
    for shard in 0..TRADE_STREAM_SHARDS {
        tokio::spawn(run_dynamic_connection(
            StreamRoute::Trade(shard),
            route_url(&base_url, StreamRoute::Trade(shard)),
            Arc::clone(&state),
            symbols.clone(),
        ));
    }
    for shard in 0..PUBLIC_STREAM_SHARDS {
        tokio::spawn(run_dynamic_connection(
            StreamRoute::Public(shard),
            route_url(&base_url, StreamRoute::Public(shard)),
            Arc::clone(&state),
            symbols.clone(),
        ));
    }
    std::future::pending::<()>().await;
}

#[derive(Clone, Copy, Debug)]
enum StreamRoute {
    Radar,
    Market(usize),
    Trade(usize),
    Public(usize),
}

async fn run_connection(route: StreamRoute, url: String, state: Arc<RwLock<StreamState>>) {
    let mut backoff = 1u64;
    loop {
        let mut stable_connection = false;
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
                let connected_at = Instant::now();
                let (mut writer, mut reader) = stream.split();
                let disconnect_reason = loop {
                    let message = match tokio::time::timeout(
                        Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
                        reader.next(),
                    )
                    .await
                    {
                        Err(_) => break "websocket receive idle timeout".to_string(),
                        Ok(message) => message,
                    };
                    match message {
                        Some(message) => match message {
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
                                    break "pong write failed".to_string();
                                }
                            }
                            Ok(Message::Close(frame)) => {
                                break format!("server close: {frame:?}");
                            }
                            Err(error) => {
                                break format!("websocket read failed: {error}");
                            }
                            _ => {}
                        },
                        None => break "websocket stream ended".to_string(),
                    }
                };
                stable_connection = connected_at.elapsed() >= Duration::from_secs(60);
                let detail = format!("{route:?} {disconnect_reason}");
                set_connected(&state, route, false, Some(detail.clone()));
                log_disconnect(route, connected_at.elapsed(), &detail, false);
            }
            Ok(Err(error)) => {
                set_connected(&state, route, false, Some(error.to_string()));
                warn!(error=%error, "Binance market websocket connection failed");
            }
        }
        record_reconnect(&state, route);
        tokio::time::sleep(reconnect_delay(backoff, route)).await;
        backoff = if stable_connection {
            1
        } else {
            (backoff * 2).min(30)
        };
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
        if route_streams(route, &symbols.borrow()).is_empty() {
            // A small dynamic universe can leave a deterministic shard with
            // no assigned symbols. It is healthy without a socket and should
            // not reconnect forever merely to hold an empty subscription.
            set_connected(&state, route, true, None);
            if symbols.changed().await.is_err() {
                return;
            }
            continue;
        }
        let mut stable_connection = false;
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
                mark_route_recovering(
                    &state,
                    route,
                    &symbols.borrow(),
                    chrono::Utc::now().timestamp_millis(),
                );
                let connected_at = Instant::now();
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
                    let idle_deadline =
                        tokio::time::sleep(Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS));
                    tokio::pin!(idle_deadline);
                    let heartbeat = tokio::time::sleep(Duration::from_secs(CLIENT_HEARTBEAT_SECS));
                    tokio::pin!(heartbeat);
                    let disconnect_reason = loop {
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
                                    ).await.is_err() { break "subscribe write failed".to_string(); }
                                    request_id += 1;
                                }
                                if !removals.is_empty() {
                                    if send_subscription_change(
                                        &mut writer,
                                        "UNSUBSCRIBE",
                                        removals,
                                        request_id,
                                    ).await.is_err() { break "unsubscribe write failed".to_string(); }
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
                                            break "subscription rejected by Binance".to_string();
                                        }
                                        match parsed.map_err(anyhow::Error::from).and_then(|value| {
                                            handle_payload(&state, route, value, now_ms)
                                        }) {
                                            Ok(()) => record_message(&state, route, now_ms),
                                            Err(error) => record_parse_error(&state, error.to_string()),
                                        }
                                        idle_deadline.as_mut().reset(
                                            tokio::time::Instant::now()
                                                + Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
                                        );
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if writer.send(Message::Pong(payload)).await.is_err() { break "pong write failed".to_string(); }
                                        idle_deadline.as_mut().reset(
                                            tokio::time::Instant::now()
                                                + Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
                                        );
                                    }
                                    Some(Ok(Message::Close(frame))) => break format!("server close: {frame:?}"),
                                    Some(Err(error)) => break format!("websocket read failed: {error}"),
                                    None => break "websocket stream ended".to_string(),
                                    _ => {}
                                }
                            }
                            _ = &mut idle_deadline => {
                                break "websocket receive idle timeout".to_string();
                            }
                            _ = &mut heartbeat => {
                                if writer.send(Message::Ping(Vec::new().into())).await.is_err() {
                                    break "heartbeat write failed".to_string();
                                }
                                heartbeat.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + Duration::from_secs(CLIENT_HEARTBEAT_SECS),
                                );
                            }
                        }
                    };
                    stable_connection = connected_at.elapsed() >= Duration::from_secs(60);
                    let detail = format!("{route:?} {disconnect_reason}");
                    set_connected(&state, route, false, Some(detail.clone()));
                    log_disconnect(route, connected_at.elapsed(), &detail, true);
                }
            }
        }
        record_reconnect(&state, route);
        tokio::time::sleep(reconnect_delay(backoff, route)).await;
        backoff = if stable_connection {
            1
        } else {
            (backoff * 2).min(30)
        };
    }
}

/// A reconnect invalidates only the short windows owned by that route. Never
/// join pre-disconnect force orders, trades, or depth snapshots to fresh data;
/// doing so can manufacture a liquidation event that never existed.
fn mark_route_recovering(
    state: &Arc<RwLock<StreamState>>,
    route: StreamRoute,
    symbols: &[String],
    _now_ms: i64,
) {
    let mut state = state.write().expect("stream state poisoned");
    match route {
        StreamRoute::Public(shard) => {
            // OFI cannot bridge a disconnected route. Keep the last book only
            // as a short-lived executable snapshot; its TTL will expire
            // naturally and the first new snapshot will refresh it.
            for symbol in symbols {
                if symbol_shard(
                    &format!("{}@depth20@500ms", symbol.to_lowercase()),
                    PUBLIC_STREAM_SHARDS,
                ) == shard
                {
                    state.book_flow.remove(&symbol.to_uppercase());
                }
            }
        }
        StreamRoute::Trade(shard) => {
            for symbol in symbols {
                let normalized = symbol.to_uppercase();
                if symbol_shard(&symbol.to_lowercase(), TRADE_STREAM_SHARDS) == shard {
                    state.trades.remove(&normalized);
                    state.liquidations.remove(&normalized);
                }
            }
        }
        StreamRoute::Market(_) | StreamRoute::Radar => {}
    }
}

fn log_disconnect(route: StreamRoute, connected_for: Duration, detail: &str, dynamic: bool) {
    let seconds = connected_for.as_secs();
    let transient_peer_close = detail.contains("Connection reset")
        || detail.contains("connection without sending TLS close_notify")
        || detail.contains("peer closed connection");
    if transient_peer_close && seconds >= 15 {
        // Peer resets are common on long-lived public market-data sockets.
        // Recovery is represented in telemetry; keeping every successful
        // auto-recovery at WARN/INFO floods journald without an operator
        // action to take.
        debug!(
            route=?route,
            connected_seconds=seconds,
            "Binance websocket peer reset; reconnecting"
        );
    } else if dynamic {
        warn!(
            route=?route,
            connected_seconds=seconds,
            reason=%detail,
            "Binance dynamic websocket disconnected"
        );
    } else {
        warn!(
            route=?route,
            connected_seconds=seconds,
            reason=%detail,
            "Binance websocket disconnected"
        );
    }
}

fn reconnect_delay(backoff_seconds: u64, route: StreamRoute) -> Duration {
    let slot = match route {
        StreamRoute::Radar => 0,
        StreamRoute::Market(shard) => 1 + shard,
        StreamRoute::Trade(shard) => 1 + MARKET_STREAM_SHARDS + shard,
        StreamRoute::Public(shard) => 1 + MARKET_STREAM_SHARDS + TRADE_STREAM_SHARDS + shard,
    };
    Duration::from_millis(backoff_seconds * 1_000 + slot as u64 * 125)
}

fn record_reconnect(state: &Arc<RwLock<StreamState>>, route: StreamRoute) {
    let mut state = state.write().expect("stream state poisoned");
    state.telemetry.reconnects += 1;
    match route {
        StreamRoute::Radar => state.telemetry.radar_reconnects += 1,
        StreamRoute::Market(_) => state.telemetry.market_reconnects += 1,
        StreamRoute::Trade(_) => state.telemetry.trade_reconnects += 1,
        StreamRoute::Public(_) => state.telemetry.public_reconnects += 1,
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
        // These connections send live SUBSCRIBE/UNSUBSCRIBE control frames.
        // Binance documents `/stream` for request-based combined streams;
        // `/ws/{name}` is the raw single-stream form.
        StreamRoute::Market(_) => "market/stream",
        StreamRoute::Trade(_) => "market/stream",
        StreamRoute::Public(_) => "public/stream",
        StreamRoute::Radar => "market/stream",
    };
    format!("{}/{path}", base.trim_end_matches('/'))
}

fn route_streams(route: StreamRoute, symbols: &[String]) -> BTreeSet<String> {
    match route {
        StreamRoute::Market(shard) => market_streams_shard(symbols, shard),
        StreamRoute::Trade(shard) => trade_streams_shard(symbols, shard),
        StreamRoute::Public(shard) => public_streams_shard(symbols, shard),
        StreamRoute::Radar => BTreeSet::from(["!ticker@arr".into()]),
    }
}

fn market_streams(symbols: &[String]) -> BTreeSet<String> {
    let mut streams = BTreeSet::new();
    for symbol in symbols {
        let symbol = symbol.to_lowercase();
        streams.insert(format!("{symbol}@kline_15m"));
        streams.insert(format!("{symbol}@kline_1h"));
        streams.insert(format!("{symbol}@kline_5m"));
        streams.insert(format!("{symbol}@kline_1m"));
    }
    streams
}

fn market_streams_shard(symbols: &[String], shard: usize) -> BTreeSet<String> {
    market_streams(symbols)
        .into_iter()
        // Keep all kline intervals for one symbol on the same connection. If
        // one shard is recovering, only that shard's coins warm up instead of
        // leaving every coin with an incomplete interval set.
        .filter(|stream| {
            symbol_shard(
                stream.split('@').next().unwrap_or(stream),
                MARKET_STREAM_SHARDS,
            ) == shard
        })
        .collect()
}

fn trade_streams(symbols: &[String]) -> BTreeSet<String> {
    symbols
        .iter()
        .flat_map(|symbol| {
            let symbol = symbol.to_lowercase();
            [format!("{symbol}@aggTrade"), format!("{symbol}@forceOrder")]
        })
        .collect()
}

fn trade_streams_shard(symbols: &[String], shard: usize) -> BTreeSet<String> {
    trade_streams(symbols)
        .into_iter()
        .filter(|stream| symbol_shard(stream, TRADE_STREAM_SHARDS) == shard)
        .collect()
}

fn symbol_shard(symbol: &str, shard_count: usize) -> usize {
    let hash = symbol.bytes().fold(2_166_136_261_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
    });
    hash as usize % shard_count.max(1)
}

fn public_streams(symbols: &[String]) -> BTreeSet<String> {
    symbols
        .iter()
        .map(|symbol| format!("{}@depth20@500ms", symbol.to_lowercase()))
        .collect()
}

fn public_streams_shard(symbols: &[String], shard: usize) -> BTreeSet<String> {
    public_streams(symbols)
        .into_iter()
        .filter(|stream| symbol_shard(stream, PUBLIC_STREAM_SHARDS) == shard)
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

fn update_liquidation(
    state: &Arc<RwLock<StreamState>>,
    value: &Value,
    received_ms: i64,
) -> Result<()> {
    let order = value
        .get("o")
        .ok_or_else(|| anyhow!("missing force-order payload"))?;
    let symbol = string(order, "s")?.to_uppercase();
    let side = string(order, "S")?;
    let price = optional_number(order, "ap")
        .filter(|value| *value > 0.0)
        .or_else(|| optional_number(order, "p"))
        .unwrap_or_default();
    let quantity = optional_number(order, "z")
        .filter(|value| *value > 0.0)
        .or_else(|| optional_number(order, "q"))
        .unwrap_or_default();
    if price <= 0.0 || quantity <= 0.0 {
        return Err(anyhow!("invalid force-order notional for {symbol}"));
    }
    // A SELL forced order closes a long; BUY closes a short.
    let is_long_liquidation = side == "SELL";
    let event_ms = value
        .get("E")
        .and_then(Value::as_i64)
        .unwrap_or(received_ms);
    let mut state = state.write().expect("stream state poisoned");
    let book = state.books.get(&symbol);
    let (pressure_depth_usd, mid, book_received_ms) = book.map_or((0.0, 0.0, 0), |book| {
        (
            if is_long_liquidation {
                book.bid_depth_usd
            } else {
                book.ask_depth_usd
            },
            (book.bid + book.ask) * 0.5,
            book.meta.received_ms,
        )
    });
    let values = state.liquidations.entry(symbol).or_default();
    values.push_back(LiquidationObservation {
        event_ms,
        received_ms,
        is_long_liquidation,
        notional_usd: price * quantity,
        pressure_depth_usd,
        mid,
        book_received_ms,
    });
    while values
        .front()
        .is_some_and(|value| value.received_ms < received_ms - 120_000)
    {
        values.pop_front();
    }
    Ok(())
}

fn update_trade(state: &Arc<RwLock<StreamState>>, value: &Value, received_ms: i64) -> Result<()> {
    let symbol = string(value, "s")?.to_uppercase();
    let notional = number(value, "p")? * number(value, "q")?;
    let is_taker_buy = !value.get("m").and_then(Value::as_bool).unwrap_or(false);
    let mut state = state.write().expect("stream state poisoned");
    let values = state.trades.entry(symbol).or_default();
    // This cache drives causal freshness and rolling flow at the local
    // process. Exchange event clocks can be slightly ahead or behind and must
    // not make a newly observed trade look future-dated or stale.
    values.push_back((received_ms, is_taker_buy, notional));
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
    // A reconnect can miss the final `x=true` update for a candle. Once a
    // newer interval arrives, the older candle is definitively closed even if
    // Binance's terminal update was lost. Heal that state here so strategies
    // do not permanently discard otherwise complete history.
    for previous in values.iter_mut().filter(|value| {
        !value.closed && value.open_ms < candle.open_ms && value.close_ms < candle.open_ms
    }) {
        previous.closed = true;
    }
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
    let update_id = value.get("u").and_then(Value::as_u64);
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
    if let Some(update_id) = update_id {
        match state.book_update_ids.get(&symbol).copied() {
            Some(previous_id) if update_id < previous_id => return Ok(()),
            Some(previous_id) if update_id == previous_id => {
                // The redundant route delivered the same exchange snapshot.
                // Refresh receipt time, but do not count it as order flow.
                state.books.insert(symbol, book);
                return Ok(());
            }
            _ => {
                state.book_update_ids.insert(symbol.clone(), update_id);
            }
        }
    }
    if let Some(previous) = state.books.get(&symbol) {
        let contiguous = book
            .meta
            .received_ms
            .saturating_sub(previous.meta.received_ms)
            <= STREAM_TTL_MS;
        if contiguous {
            if let Some(observation) = book_flow_observation(previous, &book) {
                let values = state.book_flow.entry(symbol.clone()).or_default();
                values.push_back(observation);
                while values
                    .front()
                    .is_some_and(|value| value.received_ms < received_ms - BOOK_FLOW_HISTORY_MS)
                {
                    values.pop_front();
                }
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
        // Window local order-book changes by arrival time. Exchange event
        // time can differ from the runtime clock enough to make every OFI
        // observation appear old even though depth messages are current.
        received_ms: current.meta.received_ms,
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

struct LiquidationWindowAggregate {
    event_ms: i64,
    received_ms: i64,
    long_notional_usd: f64,
    short_notional_usd: f64,
    dominance: f64,
    depth_ratio: f64,
    aligned_return_bps: Option<f64>,
    reversal_bps: Option<f64>,
}

fn aggregate_liquidation_window(
    values: &VecDeque<LiquidationObservation>,
    book_flow: Option<&VecDeque<BookFlowObservation>>,
    current_book: Option<&BookState>,
    window_ms: i64,
) -> Option<LiquidationWindowAggregate> {
    let latest = values.back()?;
    let start_ms = latest.received_ms.saturating_sub(window_ms);
    let selected: Vec<_> = values
        .iter()
        .filter(|value| value.received_ms >= start_ms && value.received_ms <= latest.received_ms)
        .collect();
    let long_notional_usd = selected
        .iter()
        .filter(|value| value.is_long_liquidation)
        .map(|value| value.notional_usd)
        .sum::<f64>();
    let short_notional_usd = selected
        .iter()
        .filter(|value| !value.is_long_liquidation)
        .map(|value| value.notional_usd)
        .sum::<f64>();
    let total = long_notional_usd + short_notional_usd;
    if total <= f64::EPSILON {
        return None;
    }
    let long_dominant = long_notional_usd >= short_notional_usd;
    let directional_notional = long_notional_usd.max(short_notional_usd);
    let pressure_depth_usd = selected
        .iter()
        .rev()
        .find(|value| {
            value.is_long_liquidation == long_dominant
                && value.pressure_depth_usd > 0.0
                && value
                    .received_ms
                    .saturating_sub(value.book_received_ms)
                    .abs()
                    <= 2_000
        })
        .map(|value| value.pressure_depth_usd)
        .unwrap_or_default();
    let start_mid = book_flow.and_then(|observations| {
        observations
            .iter()
            .filter(|value| {
                value.received_ms <= start_ms && start_ms.saturating_sub(value.received_ms) <= 1_500
            })
            .next_back()
            .or_else(|| {
                observations.iter().find(|value| {
                    value.received_ms >= start_ms
                        && value.received_ms.saturating_sub(start_ms) <= 1_500
                })
            })
            .map(|value| value.mid)
    });
    let end_mid = (latest.mid > 0.0
        && latest
            .received_ms
            .saturating_sub(latest.book_received_ms)
            .abs()
            <= 2_000)
        .then_some(latest.mid)
        .or_else(|| {
            book_flow.and_then(|observations| {
                observations
                    .iter()
                    .filter(|value| value.received_ms <= latest.received_ms)
                    .next_back()
                    .map(|value| value.mid)
            })
        });
    let aligned_return_bps = start_mid.zip(end_mid).and_then(|(start, end)| {
        (start > 0.0 && end > 0.0)
            .then_some((if long_dominant { -1.0 } else { 1.0 }) * (end / start - 1.0) * 10_000.0)
    });
    let current_mid = current_book.and_then(|book| {
        (book.bid > 0.0
            && book.ask > book.bid
            && book.meta.received_ms >= latest.received_ms
            && book.meta.received_ms.saturating_sub(latest.received_ms) <= 30_000)
            .then_some((book.bid + book.ask) * 0.5)
    });
    let reversal_bps = current_mid.and_then(|current| {
        let mut observed = vec![latest.mid];
        if let Some(values) = book_flow {
            observed.extend(
                values
                    .iter()
                    .filter(|value| {
                        value.received_ms >= latest.received_ms
                            && current_book
                                .is_some_and(|book| value.received_ms <= book.meta.received_ms)
                    })
                    .map(|value| value.mid),
            );
        }
        observed.retain(|value| *value > 0.0 && value.is_finite());
        if observed.is_empty() {
            return None;
        }
        let value = if long_dominant {
            let low = observed.iter().copied().fold(f64::INFINITY, f64::min);
            (current / low - 1.0) * 10_000.0
        } else {
            let high = observed.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            (high / current - 1.0) * 10_000.0
        };
        value.is_finite().then_some(value.max(0.0))
    });
    Some(LiquidationWindowAggregate {
        event_ms: latest.event_ms,
        received_ms: latest.received_ms,
        long_notional_usd,
        short_notional_usd,
        dominance: directional_notional / total,
        // Missing event-time depth is not evidence of an infinitely strong
        // liquidation. Score it as unavailable/zero so the strategy blocks.
        depth_ratio: if pressure_depth_usd > f64::EPSILON {
            directional_notional / pressure_depth_usd
        } else {
            0.0
        },
        aligned_return_bps,
        reversal_bps,
    })
}

fn aggregate_book_flow(
    values: &VecDeque<BookFlowObservation>,
    now_ms: i64,
    window_ms: i64,
) -> Option<BookFlowAggregate> {
    let selected: Vec<_> = values
        .iter()
        .filter(|value| {
            value.received_ms >= now_ms - window_ms && value.received_ms <= now_ms + 1_000
        })
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
        StreamRoute::Market(shard) => {
            if let Some(value) = state.market_connections.get_mut(shard) {
                *value = connected;
            }
            state.telemetry.market_shards_total = MARKET_STREAM_SHARDS;
            state.telemetry.market_shards_connected = state
                .market_connections
                .iter()
                .filter(|value| **value)
                .count();
            state.telemetry.market_connected =
                state.telemetry.market_shards_connected == MARKET_STREAM_SHARDS;
        }
        StreamRoute::Trade(shard) => {
            if let Some(value) = state.trade_connections.get_mut(shard) {
                *value = connected;
            }
            state.telemetry.trade_shards_total = TRADE_STREAM_SHARDS;
            state.telemetry.trade_shards_connected = state
                .trade_connections
                .iter()
                .filter(|value| **value)
                .count();
            state.telemetry.trade_connected =
                state.telemetry.trade_shards_connected == TRADE_STREAM_SHARDS;
        }
        StreamRoute::Public(shard) => {
            if let Some(value) = state.public_connections.get_mut(shard) {
                *value = connected;
            }
            state.telemetry.public_shards_total = PUBLIC_STREAM_SHARDS;
            state.telemetry.public_shards_connected = state
                .public_connections
                .iter()
                .filter(|value| **value)
                .count();
            state.telemetry.public_connected =
                state.telemetry.public_shards_connected == PUBLIC_STREAM_SHARDS;
        }
    }
    if let Some(error) = error {
        state.telemetry.last_disconnect_ms = Some(chrono::Utc::now().timestamp_millis());
        state.telemetry.last_disconnect_reason = Some(error.clone());
        state.telemetry.last_error = Some(error);
    } else if connected
        && state.telemetry.radar_connected
        && state.telemetry.market_connected
        && state.telemetry.trade_connected
        && state.telemetry.public_connected
    {
        state.telemetry.last_error = None;
    }
}

fn refresh_public_connection_health(state: &mut StreamState) {
    state.telemetry.public_connected =
        state.telemetry.public_shards_connected == PUBLIC_STREAM_SHARDS;
}

fn record_message(state: &Arc<RwLock<StreamState>>, route: StreamRoute, now_ms: i64) {
    let mut state = state.write().expect("stream state poisoned");
    match route {
        StreamRoute::Radar => {
            state.telemetry.radar_messages += 1;
            state.telemetry.last_radar_message_ms = Some(now_ms);
        }
        StreamRoute::Market(_) => {
            state.telemetry.market_messages += 1;
            state.telemetry.last_market_message_ms = Some(now_ms);
        }
        StreamRoute::Trade(_) => {
            state.telemetry.trade_messages += 1;
            state.telemetry.last_trade_message_ms = Some(now_ms);
        }
        StreamRoute::Public(_) => {
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
            StreamRoute::Market(0),
            serde_json::json!({"stream":"!ticker@arr","data":[{"e":"24hrTicker","E":10,"s":"YBUSDT","c":"2","o":"1","q":"30000000","st":1},{"e":"24hrTicker","s":"BTCUSD_PERP","c":"2","o":"1","q":"1","st":2}]}),
            10,
        )
        .unwrap();
        handle_payload(
            &state,
            StreamRoute::Market(0),
            serde_json::json!({"data":{"e":"kline","s":"YBUSDT","k":{"t":1,"T":2,"s":"YBUSDT","i":"5m","o":"1","c":"2","h":"2","l":"1","q":"100","Q":"60","x":true}}}),
            10,
        )
        .unwrap();
        handle_payload(
            &state,
            StreamRoute::Public(0),
            serde_json::json!({"data":{"E":10,"s":"YBUSDT","b":[["1.9","100"]],"a":[["2.1","100"]]}}),
            10,
        )
        .unwrap();
        handle_payload(
            &state,
            StreamRoute::Trade(0),
            serde_json::json!({"data":{"e":"forceOrder","E":10,"o":{"s":"YBUSDT","S":"SELL","p":"2.0","ap":"2.1","q":"10","z":"8"}}}),
            10,
        )
        .unwrap();
        let locked = state.read().unwrap();
        assert_eq!(locked.tickers.len(), 1);
        assert_eq!(locked.candles.len(), 1);
        assert_eq!(locked.books["YBUSDT"].bid, 1.9);
        assert_eq!(
            locked.liquidations["YBUSDT"].back().unwrap().notional_usd,
            16.8
        );
    }

    #[test]
    fn newer_kline_heals_a_missed_terminal_close_update() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        let kline = |open_ms: i64, close_ms: i64, closed: bool| {
            serde_json::json!({"k":{
                "t":open_ms,"T":close_ms,"s":"YBUSDT","i":"15m",
                "o":"1","c":"2","h":"2","l":"1","q":"100","Q":"60","x":closed
            }})
        };
        update_kline(&state, &kline(0, 899_999, false), 899_000).unwrap();
        update_kline(&state, &kline(900_000, 1_799_999, false), 900_100).unwrap();

        let locked = state.read().unwrap();
        let values = &locked.candles[&("YBUSDT".to_string(), "15m".to_string())];
        assert!(values[0].closed);
        assert!(!values[1].closed);
    }

    #[test]
    fn dynamic_streams_use_split_binance_routes() {
        let symbols = vec!["BTCUSDT".to_string(), "YBUSDT".to_string()];
        let market = market_streams(&symbols);
        let public = public_streams(&symbols);
        assert_eq!(
            route_url("wss://fstream.binance.com", StreamRoute::Market(0)),
            "wss://fstream.binance.com/market/stream"
        );
        assert_eq!(
            route_url("wss://fstream.binance.com", StreamRoute::Public(0)),
            "wss://fstream.binance.com/public/stream"
        );
        assert_eq!(
            route_url("wss://fstream.binance.com", StreamRoute::Trade(0)),
            "wss://fstream.binance.com/market/stream"
        );
        assert!(!market.contains("!ticker@arr"));
        assert!(market.contains("ybusdt@kline_5m"));
        assert!(market.contains("ybusdt@kline_1m"));
        assert!(market.contains("btcusdt@kline_5m"));
        assert!(!market.contains("btcusdt@aggTrade"));
        assert!(trade_streams(&symbols).contains("btcusdt@aggTrade"));
        assert!(trade_streams(&symbols).contains("btcusdt@forceOrder"));
        assert!(!market.contains("btcusdt@forceOrder"));
        assert!(public.contains("btcusdt@depth20@500ms"));
    }

    #[test]
    fn trade_stream_shards_are_disjoint_and_complete() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "YBUSDT".to_string(),
        ];
        let expected = trade_streams(&symbols);
        let mut combined = BTreeSet::new();
        for shard in 0..TRADE_STREAM_SHARDS {
            let streams = trade_streams_shard(&symbols, shard);
            assert!(combined.is_disjoint(&streams));
            combined.extend(streams);
        }
        assert_eq!(combined, expected);
    }

    #[test]
    fn market_stream_shards_are_disjoint_and_complete() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "YBUSDT".to_string(),
        ];
        let expected = market_streams(&symbols);
        let mut combined = BTreeSet::new();
        for shard in 0..MARKET_STREAM_SHARDS {
            let streams = market_streams_shard(&symbols, shard);
            assert!(combined.is_disjoint(&streams));
            combined.extend(streams);
        }
        assert_eq!(combined, expected);
        for symbol in &symbols {
            let prefix = symbol.to_lowercase();
            let owning_shards = (0..MARKET_STREAM_SHARDS)
                .filter(|shard| {
                    market_streams_shard(&symbols, *shard)
                        .iter()
                        .any(|stream| stream.starts_with(&prefix))
                })
                .count();
            assert_eq!(owning_shards, 1);
        }
    }

    #[test]
    fn public_stream_shards_are_disjoint_and_complete() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "YBUSDT".to_string(),
        ];
        let expected = public_streams(&symbols);
        let mut combined = BTreeSet::new();
        for shard in 0..PUBLIC_STREAM_SHARDS {
            let streams = public_streams_shard(&symbols, shard);
            assert!(combined.is_disjoint(&streams));
            combined.extend(streams);
        }
        assert_eq!(combined, expected);
    }

    #[test]
    fn reconnects_are_staggered_across_routes() {
        let delays = [
            StreamRoute::Radar,
            StreamRoute::Market(0),
            StreamRoute::Trade(0),
            StreamRoute::Public(0),
        ]
        .map(|route| reconnect_delay(1, route));
        assert!(delays.windows(2).all(|pair| pair[0] < pair[1]));
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
        let shard = symbol_shard("btcusdt", MARKET_STREAM_SHARDS);
        let added_symbol = ["ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
            .into_iter()
            .find(|symbol| symbol_shard(&symbol.to_lowercase(), MARKET_STREAM_SHARDS) == shard)
            .expect("test symbols should include one in BTC's market shard");
        let (sender, receiver) = watch::channel(vec!["BTCUSDT".to_string()]);
        let task = tokio::spawn(run_dynamic_connection(
            StreamRoute::Market(shard),
            format!("ws://{address}/market/stream"),
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
            .send(vec!["BTCUSDT".to_string(), added_symbol.to_string()])
            .unwrap();
        let addition = read_control(
            tokio::time::timeout(Duration::from_secs(1), server.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
        );
        assert_eq!(addition["method"], "SUBSCRIBE");
        assert!(addition["params"].as_array().unwrap().iter().all(|value| {
            value
                .as_str()
                .unwrap()
                .starts_with(&added_symbol.to_lowercase())
        }));
        assert_eq!(state.read().unwrap().telemetry.market_shards_connected, 1);
        assert!(!state.read().unwrap().telemetry.market_connected);
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
    fn public_reconnect_clears_only_the_affected_flow_window() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let shard = symbol_shard("btcusdt@depth20@500ms", PUBLIC_STREAM_SHARDS);
        let state = Arc::new(RwLock::new(StreamState::default()));
        {
            let mut inner = state.write().unwrap();
            for symbol in &symbols {
                inner.symbol_admitted_ms.insert(symbol.clone(), 1_000);
                inner.book_flow.insert(
                    symbol.clone(),
                    VecDeque::from([BookFlowObservation {
                        received_ms: 1_000,
                        raw_ofi_usd: 1.0,
                        visible_top_usd: 10.0,
                        mid: 1.0,
                    }]),
                );
            }
        }

        mark_route_recovering(&state, StreamRoute::Public(shard), &symbols, 2_000);

        let inner = state.read().unwrap();
        for symbol in &symbols {
            assert_eq!(inner.symbol_admitted_ms[symbol], 1_000);
            let affected = symbol_shard(
                &format!("{}@depth20@500ms", symbol.to_lowercase()),
                PUBLIC_STREAM_SHARDS,
            ) == shard;
            assert_eq!(inner.book_flow.contains_key(symbol), !affected);
        }
    }

    #[test]
    fn one_public_shard_failure_is_reported_without_affecting_other_routes() {
        let symbols = [
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let mut state = StreamState {
            desired_symbols: symbols.iter().cloned().collect(),
            ..StreamState::default()
        };
        state.public_connections.fill(true);
        for failed in 0..PUBLIC_STREAM_SHARDS {
            state.public_connections[failed] = false;
            refresh_public_connection_health(&mut state);
            assert!(!state.telemetry.public_connected);
            state.public_connections[failed] = true;
        }
    }

    #[test]
    fn redundant_depth_updates_do_not_double_count_book_flow() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        let depth = |update_id: u64, bid_qty: &str, ask_qty: &str| {
            serde_json::json!({
                "s":"YBUSDT", "E":1_000 + update_id as i64, "u":update_id,
                "b":[["1.0",bid_qty]], "a":[["1.1",ask_qty]]
            })
        };
        update_depth(&state, &depth(10, "100", "100"), 1_000).unwrap();
        update_depth(&state, &depth(11, "110", "90"), 1_500).unwrap();
        update_depth(&state, &depth(11, "110", "90"), 1_501).unwrap();
        assert_eq!(state.read().unwrap().book_flow["YBUSDT"].len(), 1);
    }

    #[test]
    fn depth_flow_does_not_bridge_a_stale_reconnect_gap() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        let depth = |update_id: u64, event_ms: i64| {
            serde_json::json!({
                "s":"YBUSDT", "E":event_ms, "u":update_id,
                "b":[["1.0","100"]], "a":[["1.1","100"]]
            })
        };
        update_depth(&state, &depth(10, 1_000), 1_000).unwrap();
        update_depth(&state, &depth(20, 31_000), 31_000).unwrap();
        assert!(state
            .read()
            .unwrap()
            .book_flow
            .get("YBUSDT")
            .is_none_or(VecDeque::is_empty));
    }

    #[test]
    fn trade_reconnect_drops_only_affected_short_windows() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
            "XRPUSDT".to_string(),
        ];
        let shard = symbol_shard("btcusdt", TRADE_STREAM_SHARDS);
        let state = Arc::new(RwLock::new(StreamState::default()));
        {
            let mut inner = state.write().unwrap();
            for symbol in &symbols {
                inner
                    .trades
                    .insert(symbol.clone(), VecDeque::from([(1_000, true, 10.0)]));
                inner.liquidations.insert(
                    symbol.clone(),
                    VecDeque::from([LiquidationObservation {
                        event_ms: 1_000,
                        received_ms: 1_001,
                        is_long_liquidation: true,
                        notional_usd: 100.0,
                        pressure_depth_usd: 200.0,
                        mid: 1.0,
                        book_received_ms: 1_000,
                    }]),
                );
            }
        }

        mark_route_recovering(&state, StreamRoute::Trade(shard), &symbols, 2_000);

        let inner = state.read().unwrap();
        for symbol in &symbols {
            let affected = symbol_shard(&symbol.to_lowercase(), TRADE_STREAM_SHARDS) == shard;
            assert_eq!(inner.trades.contains_key(symbol), !affected);
            assert_eq!(inner.liquidations.contains_key(symbol), !affected);
        }
    }

    #[test]
    fn recent_liquidation_preempts_slow_frame_maintenance() {
        let state = Arc::new(RwLock::new(StreamState::default()));
        {
            let mut inner = state.write().unwrap();
            inner.desired_symbols.insert("ALTUSDT".into());
            inner.liquidations.insert(
                "ALTUSDT".into(),
                VecDeque::from([LiquidationObservation {
                    event_ms: 9_900,
                    received_ms: 10_000,
                    is_long_liquidation: true,
                    notional_usd: 1_000.0,
                    pressure_depth_usd: 500.0,
                    mid: 1.0,
                    book_received_ms: 10_000,
                }]),
            );
        }
        let (symbols, _) = watch::channel(Vec::new());
        let hub = MarketStreamHub { state, symbols };
        assert!(hub.has_recent_liquidation_setup(15_000, 3_000, 12_000, 0.80, 1.20));
        assert!(!hub.has_recent_liquidation_setup(22_001, 3_000, 12_000, 0.80, 1.20));
    }

    #[test]
    fn snapshot_ofi_is_positive_when_bid_queue_grows() {
        let previous = test_book(1_000, 99.0, 10.0, 101.0, 10.0);
        let mut current = test_book(1_500, 99.0, 20.0, 101.0, 10.0);
        current.meta.event_ms = 500;
        let observation = book_flow_observation(&previous, &current).unwrap();
        assert!(observation.raw_ofi_usd > 0.0);
        assert_eq!(observation.received_ms, 1_500);
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
                received_ms: 1_000,
                raw_ofi_usd: 1_000.0,
                visible_top_usd: 10_000.0,
                mid: 100.0,
            },
            BookFlowObservation {
                received_ms: 1_500,
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

    #[test]
    fn liquidation_window_uses_directional_depth_and_aligned_price_move() {
        let books = VecDeque::from([
            BookFlowObservation {
                received_ms: 7_000,
                raw_ofi_usd: 0.0,
                visible_top_usd: 10_000.0,
                mid: 100.0,
            },
            BookFlowObservation {
                received_ms: 10_000,
                raw_ofi_usd: -1_000.0,
                visible_top_usd: 10_000.0,
                mid: 99.9,
            },
        ]);
        let liquidations = VecDeque::from([
            LiquidationObservation {
                event_ms: 8_000,
                received_ms: 8_000,
                is_long_liquidation: true,
                notional_usd: 6_000.0,
                pressure_depth_usd: 10_000.0,
                mid: 99.96,
                book_received_ms: 8_000,
            },
            LiquidationObservation {
                event_ms: 10_000,
                received_ms: 10_000,
                is_long_liquidation: true,
                notional_usd: 3_000.0,
                pressure_depth_usd: 10_000.0,
                mid: 99.9,
                book_received_ms: 10_000,
            },
            LiquidationObservation {
                event_ms: 10_000,
                received_ms: 10_000,
                is_long_liquidation: false,
                notional_usd: 1_000.0,
                pressure_depth_usd: 10_000.0,
                mid: 99.9,
                book_received_ms: 10_000,
            },
        ]);
        let current = test_book(11_000, 99.99, 10.0, 100.01, 10.0);
        let aggregate =
            aggregate_liquidation_window(&liquidations, Some(&books), Some(&current), 3_000)
                .unwrap();
        assert_eq!(aggregate.long_notional_usd, 9_000.0);
        assert_eq!(aggregate.short_notional_usd, 1_000.0);
        assert_eq!(aggregate.received_ms, 10_000);
        assert!((aggregate.dominance - 0.9).abs() < 1e-9);
        assert!((aggregate.depth_ratio - 0.9).abs() < 1e-9);
        assert!((aggregate.aligned_return_bps.unwrap() - 10.0).abs() < 1e-9);
        assert!((aggregate.reversal_bps.unwrap() - 10.01001001).abs() < 1e-6);
    }
}
