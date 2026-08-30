use anyhow::{Context, Result};
use greed_kernel::{Candle, CandleSeries, MarketFrame, MarketKind};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

const SNAPSHOT_INTERVAL_MS: i64 = 60_000;
const JOURNAL_MAX_BYTES: u64 = 256 * 1024 * 1024;
const JOURNAL_ROTATIONS: usize = 4;
const FORWARD_HORIZONS_MS: [i64; 6] = [10_000, 30_000, 60_000, 180_000, 300_000, 900_000];

#[derive(Default)]
pub struct SampleRecorder {
    seen_candles: BTreeSet<String>,
    last_snapshot_ms: BTreeMap<String, i64>,
}

impl SampleRecorder {
    pub fn record(&mut self, journal: &Journal, frame: &MarketFrame) -> Result<()> {
        for instrument in frame.instruments.values() {
            for series in std::iter::once(&instrument.perpetual)
                .chain(instrument.fast_perpetual.iter())
                .chain(instrument.micro_perpetual.iter())
            {
                // The REST bootstrap can contain 1,200 bars per interval. Those
                // bars are inputs, not observations made by this run. Persist
                // only the newest closed bar and then append each newly closed
                // bar once as the websocket advances.
                if let Some(bar) = series.values.iter().rev().find(|bar| bar.closed) {
                    let market = match series.market {
                        MarketKind::Perpetual => "perpetual",
                    };
                    let key = format!(
                        "{}:{market}:{}:{}",
                        instrument.symbol, series.interval_ms, bar.close_ms
                    );
                    if self.seen_candles.insert(key) {
                        journal.append(
                            "market_candle",
                            serde_json::json!({"symbol":instrument.symbol,"venue":series.venue,"market":series.market,"interval_ms":series.interval_ms,"bar":bar,"meta":series.meta}),
                        )?;
                    }
                }
            }
            let last_snapshot = self
                .last_snapshot_ms
                .get(&instrument.symbol)
                .copied()
                .unwrap_or_default();
            if frame.as_of_ms - last_snapshot >= SNAPSHOT_INTERVAL_MS {
                journal.append(
                    "market_snapshot",
                    serde_json::json!({
                        "as_of_ms":frame.as_of_ms,
                        "symbol":instrument.symbol,
                        "price":instrument.price,
                        "last_5m":instrument.fast_perpetual.as_ref().and_then(|series|series.values.last()),
                        "last_1m":instrument.micro_perpetual.as_ref().and_then(|series|series.values.last()),
                        "book":instrument.book,
                        "microstructure":instrument.microstructure
                    }),
                )?;
                self.last_snapshot_ms
                    .insert(instrument.symbol.clone(), frame.as_of_ms);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PendingResearchSample {
    sample_ms: i64,
    reference_price: f64,
    min_price: f64,
    max_price: f64,
    next_horizon: usize,
    forward: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchStatus {
    pub enabled: bool,
    pub path: String,
    pub snapshot_seconds: u64,
    pub symbols_observed: usize,
    pub snapshots_written: u64,
    pub candles_written: u64,
    pub labels_written: u64,
    pub level_maps_written: u64,
    pub pending_labels: usize,
    pub last_write_ms: Option<i64>,
    pub current_file_bytes: u64,
    pub capacity_bytes: u64,
}

/// Compact, bounded market-data recorder used for offline alpha research.
/// It is intentionally independent from the verbose decision journal: a large
/// graph evaluation can no longer rotate away the market observations needed
/// for a later walk-forward experiment.
pub struct ResearchRecorder {
    path: String,
    snapshot_interval_ms: i64,
    level_map_interval_ms: i64,
    backfill_bars: BTreeMap<i64, usize>,
    capacity_bytes: u64,
    initialized_symbols: BTreeSet<String>,
    seen_candles: BTreeSet<String>,
    last_snapshot_ms: BTreeMap<String, i64>,
    last_level_map_ms: BTreeMap<String, i64>,
    pending: BTreeMap<String, VecDeque<PendingResearchSample>>,
    snapshots_written: u64,
    candles_written: u64,
    labels_written: u64,
    level_maps_written: u64,
    last_write_ms: Option<i64>,
    last_health_ms: i64,
}

impl ResearchRecorder {
    pub fn new(
        path: String,
        snapshot_seconds: u64,
        level_map_seconds: u64,
        backfill_bars: BTreeMap<i64, usize>,
        capacity_bytes: u64,
    ) -> Self {
        Self {
            path,
            snapshot_interval_ms: snapshot_seconds as i64 * 1_000,
            level_map_interval_ms: level_map_seconds as i64 * 1_000,
            backfill_bars,
            capacity_bytes,
            initialized_symbols: BTreeSet::new(),
            seen_candles: BTreeSet::new(),
            last_snapshot_ms: BTreeMap::new(),
            last_level_map_ms: BTreeMap::new(),
            pending: BTreeMap::new(),
            snapshots_written: 0,
            candles_written: 0,
            labels_written: 0,
            level_maps_written: 0,
            last_write_ms: None,
            last_health_ms: 0,
        }
    }

    pub fn record(&mut self, journal: &Journal, frame: &MarketFrame) -> Result<()> {
        let mut records = Vec::new();
        for instrument in frame.instruments.values() {
            let first_observation = self.initialized_symbols.insert(instrument.symbol.clone());
            for series in std::iter::once(&instrument.perpetual)
                .chain(instrument.hourly_perpetual.iter())
                .chain(instrument.fast_perpetual.iter())
                .chain(instrument.micro_perpetual.iter())
            {
                let closed: Vec<_> = series.values.iter().filter(|bar| bar.closed).collect();
                let bars: Vec<_> = if first_observation {
                    let limit = self
                        .backfill_bars
                        .get(&series.interval_ms)
                        .copied()
                        .unwrap_or(120);
                    closed
                        .into_iter()
                        .rev()
                        .take(limit)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect()
                } else {
                    closed.into_iter().rev().take(1).collect()
                };
                for bar in bars {
                    let key = format!(
                        "{}:{}:{}",
                        instrument.symbol, series.interval_ms, bar.close_ms
                    );
                    if self.seen_candles.insert(key) {
                        records.push((
                            "research_candle".to_string(),
                            serde_json::json!({
                                "symbol":instrument.symbol,
                                "venue":series.venue,
                                "interval_ms":series.interval_ms,
                                "bootstrap":first_observation,
                                "bar":bar,
                                "quality":series.meta.quality,
                            }),
                        ));
                        self.candles_written += 1;
                    }
                }
            }

            let last_level_map = self
                .last_level_map_ms
                .get(&instrument.symbol)
                .copied()
                .unwrap_or_default();
            if frame.as_of_ms - last_level_map >= self.level_map_interval_ms {
                if let Some(map) = liquidity_level_map(
                    instrument.price,
                    &instrument.perpetual,
                    instrument.hourly_perpetual.as_ref(),
                    frame.as_of_ms,
                ) {
                    records.push((
                        "research_liquidity_map".to_string(),
                        serde_json::json!({"symbol":instrument.symbol,"as_of_ms":frame.as_of_ms,"map":map}),
                    ));
                    self.level_maps_written += 1;
                }
                self.last_level_map_ms
                    .insert(instrument.symbol.clone(), frame.as_of_ms);
            }

            let last_snapshot = self
                .last_snapshot_ms
                .get(&instrument.symbol)
                .copied()
                .unwrap_or_default();
            if frame.as_of_ms - last_snapshot < self.snapshot_interval_ms {
                continue;
            }

            let price = instrument.price;
            let queue = self.pending.entry(instrument.symbol.clone()).or_default();
            for sample in queue.iter_mut() {
                sample.min_price = sample.min_price.min(price);
                sample.max_price = sample.max_price.max(price);
                let elapsed_ms = frame.as_of_ms - sample.sample_ms;
                while sample.next_horizon < FORWARD_HORIZONS_MS.len()
                    && elapsed_ms >= FORWARD_HORIZONS_MS[sample.next_horizon]
                {
                    let horizon_ms = FORWARD_HORIZONS_MS[sample.next_horizon];
                    sample.forward.push(serde_json::json!({
                        "horizon_ms":horizon_ms,
                        "observed_ms":frame.as_of_ms,
                        "return_bps":(price/sample.reference_price-1.0)*10_000.0,
                    }));
                    sample.next_horizon += 1;
                }
            }
            while queue
                .front()
                .is_some_and(|sample| sample.next_horizon == FORWARD_HORIZONS_MS.len())
            {
                let sample = queue.pop_front().expect("front checked");
                records.push((
                    "research_forward_label".to_string(),
                    serde_json::json!({
                        "symbol":instrument.symbol,
                        "sample_ms":sample.sample_ms,
                        "reference_price":sample.reference_price,
                        "forward":sample.forward,
                        "max_favorable_bps":(sample.max_price/sample.reference_price-1.0)*10_000.0,
                        "max_adverse_bps":(sample.min_price/sample.reference_price-1.0)*10_000.0,
                    }),
                ));
                self.labels_written += 1;
            }

            let book = instrument.book.as_ref().map(|book| {
                serde_json::json!({
                    "bid":book.bid,
                    "ask":book.ask,
                    "spread_bps":if book.bid > 0.0 {(book.ask/book.bid-1.0)*10_000.0} else {0.0},
                    "bid_depth_usd":book.bid_depth_usd,
                    "ask_depth_usd":book.ask_depth_usd,
                    "expected_buy_slippage_bps":book.expected_buy_slippage_bps,
                    "expected_sell_slippage_bps":book.expected_sell_slippage_bps,
                    "event_age_ms":frame.as_of_ms-book.meta.event_ms,
                    "quality":book.meta.quality,
                })
            });
            let flow = instrument.microstructure.as_ref().map(|micro| {
                serde_json::json!({
                    "buy_notional_60s":micro.buy_notional_60s,
                    "sell_notional_60s":micro.sell_notional_60s,
                    "trade_imbalance_60s":micro.trade_imbalance(),
                    "long_liquidations_60s":micro.long_liquidations_60s,
                    "short_liquidations_60s":micro.short_liquidations_60s,
                    "snapshot_ofi_10s":micro.snapshot_ofi_10s,
                    "snapshot_ofi_60s":micro.snapshot_ofi_60s,
                    "mid_return_bps_10s":micro.mid_return_bps_10s,
                    "mid_return_bps_60s":micro.mid_return_bps_60s,
                    "price_impact_bps_per_ofi_10s":micro.price_impact_bps_per_ofi_10s,
                    "book_updates_10s":micro.book_updates_10s,
                    "book_updates_60s":micro.book_updates_60s,
                    "event_age_ms":frame.as_of_ms-micro.meta.event_ms,
                    "quality":micro.meta.quality,
                })
            });
            records.push((
                "research_snapshot".to_string(),
                serde_json::json!({
                    "as_of_ms":frame.as_of_ms,
                    "symbol":instrument.symbol,
                    "price":price,
                    "book":book,
                    "flow":flow,
                }),
            ));
            queue.push_back(PendingResearchSample {
                sample_ms: frame.as_of_ms,
                reference_price: price,
                min_price: price,
                max_price: price,
                next_horizon: 0,
                forward: Vec::with_capacity(FORWARD_HORIZONS_MS.len()),
            });
            self.last_snapshot_ms
                .insert(instrument.symbol.clone(), frame.as_of_ms);
            self.snapshots_written += 1;
        }
        if !records.is_empty() {
            journal.append_many(records)?;
            self.last_write_ms = Some(frame.as_of_ms);
        }
        Ok(())
    }

    pub fn record_health(&mut self, journal: &Journal, now_ms: i64, health: Value) -> Result<()> {
        if now_ms - self.last_health_ms >= 60_000 {
            journal.append("research_data_health", health)?;
            self.last_health_ms = now_ms;
            self.last_write_ms = Some(now_ms);
        }
        Ok(())
    }

    pub fn status(&self, journal: &Journal) -> ResearchStatus {
        ResearchStatus {
            enabled: true,
            path: self.path.clone(),
            snapshot_seconds: (self.snapshot_interval_ms / 1_000) as u64,
            symbols_observed: self.initialized_symbols.len(),
            snapshots_written: self.snapshots_written,
            candles_written: self.candles_written,
            labels_written: self.labels_written,
            level_maps_written: self.level_maps_written,
            pending_labels: self.pending.values().map(VecDeque::len).sum(),
            last_write_ms: self.last_write_ms,
            current_file_bytes: journal.current_bytes(),
            capacity_bytes: self.capacity_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct LevelPoint {
    price: f64,
    ts_ms: i64,
    quote_volume: f64,
    timeframe: &'static str,
    origin: &'static str,
}

fn liquidity_level_map(
    price: f64,
    structural_15m: &CandleSeries,
    structural_1h: Option<&CandleSeries>,
    now_ms: i64,
) -> Option<Value> {
    let atr = average_true_range(&structural_15m.values, 20)?;
    if price <= 0.0 || atr <= 0.0 {
        return None;
    }
    let mut points = Vec::new();
    collect_pivots(&structural_15m.values, "15m", 480, &mut points);
    if let Some(hourly) = structural_1h {
        collect_pivots(&hourly.values, "1h", 720, &mut points);
    }
    if points.is_empty() {
        return None;
    }
    let tolerance = (atr * 0.25).max(price * 0.0015);
    let average_volume =
        points.iter().map(|point| point.quote_volume).sum::<f64>() / points.len() as f64;

    let describe = |point: &LevelPoint| {
        let nearby: Vec<_> = points
            .iter()
            .filter(|other| (other.price - point.price).abs() <= tolerance)
            .collect();
        let touches = nearby.len();
        let last_touch_ms = nearby
            .iter()
            .map(|other| other.ts_ms)
            .max()
            .unwrap_or(point.ts_ms);
        let one_hour_touches = nearby
            .iter()
            .filter(|other| other.timeframe == "1h")
            .count();
        let mean_level = nearby.iter().map(|other| other.price).sum::<f64>() / touches as f64;
        let volume_ratio = if average_volume > 0.0 {
            nearby.iter().map(|other| other.quote_volume).sum::<f64>()
                / touches as f64
                / average_volume
        } else {
            1.0
        };
        let age_hours = ((now_ms - last_touch_ms).max(0) as f64 / 3_600_000.0).max(0.0);
        let score = touches as f64
            + one_hour_touches as f64 * 0.75
            + volume_ratio.max(0.01).ln_1p()
            + (-age_hours / (24.0 * 14.0)).exp();
        serde_json::json!({
            "price":mean_level,
            "distance_bps":(mean_level/price-1.0)*10_000.0,
            "distance_atr":(mean_level-price).abs()/atr,
            "touches":touches,
            "one_hour_touches":one_hour_touches,
            "last_touch_ms":last_touch_ms,
            "age_hours":age_hours,
            "volume_ratio":volume_ratio,
            "score":score,
            "origins":nearby.iter().map(|other|other.origin).collect::<BTreeSet<_>>(),
        })
    };

    let nearest = |above: bool| {
        points
            .iter()
            .filter(|point| {
                if above {
                    point.price > price
                } else {
                    point.price < price
                }
            })
            .min_by(|left, right| {
                (left.price - price)
                    .abs()
                    .total_cmp(&(right.price - price).abs())
            })
            .map(describe)
    };
    let support = nearest(false);
    let resistance = nearest(true);
    let approaching_support = support
        .as_ref()
        .and_then(|value| value["distance_atr"].as_f64())
        .is_some_and(|distance| distance <= 0.5);
    let approaching_resistance = resistance
        .as_ref()
        .and_then(|value| value["distance_atr"].as_f64())
        .is_some_and(|distance| distance <= 0.5);
    Some(serde_json::json!({
        "price":price,
        "atr_15m":atr,
        "level_tolerance":tolerance,
        "support":support,
        "resistance":resistance,
        "approaching_support":approaching_support,
        "approaching_resistance":approaching_resistance,
        "action":"observe_only",
    }))
}

fn average_true_range(values: &[Candle], periods: usize) -> Option<f64> {
    let closed: Vec<_> = values.iter().filter(|bar| bar.closed).collect();
    if closed.len() < periods + 1 {
        return None;
    }
    let start = closed.len() - periods;
    let total = (start..closed.len())
        .map(|index| {
            let bar = closed[index];
            let previous_close = closed[index - 1].close;
            (bar.high - bar.low)
                .max((bar.high - previous_close).abs())
                .max((bar.low - previous_close).abs())
        })
        .sum::<f64>();
    Some(total / periods as f64)
}

fn collect_pivots(
    values: &[Candle],
    timeframe: &'static str,
    max_bars: usize,
    output: &mut Vec<LevelPoint>,
) {
    let closed: Vec<_> = values
        .iter()
        .filter(|bar| bar.closed)
        .rev()
        .take(max_bars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if closed.len() < 5 {
        return;
    }
    for index in 2..closed.len() - 2 {
        let bar = closed[index];
        let window = &closed[index - 2..=index + 2];
        if window.iter().all(|other| bar.high >= other.high) {
            output.push(LevelPoint {
                price: bar.high,
                ts_ms: bar.close_ms,
                quote_volume: bar.quote_volume,
                timeframe,
                origin: "pivot_high",
            });
        }
        if window.iter().all(|other| bar.low <= other.low) {
            output.push(LevelPoint {
                price: bar.low,
                ts_ms: bar.close_ms,
                quote_volume: bar.quote_volume,
                timeframe,
                origin: "pivot_low",
            });
        }
    }
}

pub struct Journal {
    path: PathBuf,
    file: Mutex<JournalFile>,
    max_bytes: u64,
    rotations: usize,
}

struct JournalFile {
    file: File,
    bytes: u64,
}
impl Journal {
    pub fn new(path: &str) -> Result<Self> {
        Self::with_limits(path, JOURNAL_MAX_BYTES, JOURNAL_ROTATIONS)
    }

    pub fn bounded(path: &str, max_bytes: u64, rotations: usize) -> Result<Self> {
        Self::with_limits(path, max_bytes, rotations)
    }

    fn with_limits(path: &str, max_bytes: u64, rotations: usize) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open journal {path}"))?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path: path.into(),
            file: Mutex::new(JournalFile { file, bytes }),
            max_bytes,
            rotations,
        })
    }
    pub fn append(&self, kind: &str, payload: Value) -> Result<()> {
        self.append_many([(kind.to_string(), payload)])
    }

    pub fn append_many<I>(&self, records: I) -> Result<()>
    where
        I: IntoIterator<Item = (String, Value)>,
    {
        let mut state = self.file.lock().expect("journal mutex poisoned");
        for (kind, payload) in records {
            let value = serde_json::json!({"recorded_ms":chrono::Utc::now().timestamp_millis(),"kind":kind,"payload":payload});
            let mut line = serde_json::to_vec(&value)?;
            line.push(b'\n');
            if state.bytes > 0 && state.bytes + line.len() as u64 > self.max_bytes {
                state.file.flush()?;
                rotate_files(&self.path, self.rotations)?;
                state.file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?;
                state.bytes = 0;
            }
            state.file.write_all(&line)?;
            state.bytes += line.len() as u64;
        }
        state.file.flush()?;
        Ok(())
    }

    pub fn current_bytes(&self) -> u64 {
        self.file.lock().expect("journal mutex poisoned").bytes
    }
}

fn rotate_files(path: &Path, rotations: usize) -> Result<()> {
    if rotations == 0 {
        std::fs::remove_file(path).ok();
        return Ok(());
    }
    for index in (1..=rotations).rev() {
        let source = if index == 1 {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}.{}", path.display(), index - 1))
        };
        let destination = PathBuf::from(format!("{}.{}", path.display(), index));
        if source.exists() {
            if destination.exists() {
                std::fs::remove_file(&destination)?;
            }
            std::fs::rename(source, destination)?;
        }
    }
    Ok(())
}

pub struct StatusWriter {
    path: PathBuf,
}
impl StatusWriter {
    pub fn new(path: &str) -> Self {
        Self { path: path.into() }
    }
    pub fn write<T: Serialize>(&self, value: &T) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(value)?;
        std::fs::write(&temp, data)?;
        std::fs::rename(temp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_rotates_before_exceeding_its_bound() {
        let directory = std::env::temp_dir().join(format!(
            "greed-journal-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("events.jsonl");
        let journal = Journal::with_limits(path.to_str().unwrap(), 180, 2).unwrap();
        for index in 0..10 {
            journal
                .append("test", serde_json::json!({"index":index,"value":"payload"}))
                .unwrap();
        }
        assert!(path.exists());
        assert!(PathBuf::from(format!("{}.1", path.display())).exists());
        assert!(PathBuf::from(format!("{}.2", path.display())).exists());
        assert!(std::fs::metadata(&path).unwrap().len() <= 180);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn liquidity_map_finds_a_repeated_overhead_pivot_cluster() {
        let values = (0..32)
            .map(|index| {
                let pivot = matches!(index, 6 | 14 | 22);
                Candle {
                    open_ms: index * 900_000,
                    close_ms: (index + 1) * 900_000 - 1,
                    open: 101.0,
                    high: if pivot { 110.0 } else { 103.0 },
                    low: 99.0,
                    close: 101.0,
                    quote_volume: if pivot { 2_000_000.0 } else { 500_000.0 },
                    taker_buy_quote: Some(250_000.0),
                    closed: true,
                }
            })
            .collect();
        let series = CandleSeries {
            venue: "binance".into(),
            market: MarketKind::Perpetual,
            interval_ms: 900_000,
            meta: greed_kernel::ObservationMeta {
                event_ms: 32 * 900_000,
                received_ms: 32 * 900_000,
                expires_ms: 33 * 900_000,
                source: "test".into(),
                quality: greed_kernel::DataQuality::Complete,
            },
            values,
        };
        let map = liquidity_level_map(109.0, &series, None, 32 * 900_000).unwrap();
        let resistance = &map["resistance"];
        assert!((resistance["price"].as_f64().unwrap() - 110.0).abs() < 1e-9);
        assert!(resistance["touches"].as_u64().unwrap() >= 3);
        assert_eq!(map["action"], "observe_only");
    }
}
