//! TRDR 风格的上游市场地图。
//!
//! 这一层不直接下单。它把现货/永续订单簿、滚动主动成交 Delta 与 OI
//! 归一成四类上下文信号，供足迹/力竭模型在价格进入真实挂单区域后确认入场。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde_json::{json, Value as Json};
use tcore::{
    BookSnapshot, Ctx, Event, Exchange, Side, Signal, SignalKind, SignalPlugin, Timestamp,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket_ms: i64,
    pub delta_window_ms: i64,
    pub trend_window_ms: i64,
    pub book_fresh_ms: i64,
    pub book_bands: Vec<f64>,
    pub min_zone_depth_usd: f64,
    pub green_ratio: f64,
    pub yellow_ratio: f64,
    pub red_ratio: f64,
    pub blue_ratio: f64,
    pub zone_bin_pct: f64,
    pub min_book_sources: usize,
    pub min_spot_sources: usize,
    pub min_perp_sources: usize,
    pub delta_share_levels: [f64; 4],
    pub footprint_bin_usd: f64,
    pub footprint_imbalance_share: f64,
    pub trend_return_pct: f64,
    pub trend_efficiency: f64,
    pub oi_change_threshold: f64,
}

impl Config {
    pub fn from_params(p: &Json) -> Self {
        let f = |key: &str, default: f64| p.get(key).and_then(Json::as_f64).unwrap_or(default);
        let i = |key: &str, default: i64| p.get(key).and_then(Json::as_i64).unwrap_or(default);
        let u = |key: &str, default: usize| {
            p.get(key)
                .and_then(Json::as_u64)
                .map(|v| v as usize)
                .unwrap_or(default)
        };
        let levels = p
            .get("delta_share_levels")
            .and_then(Json::as_array)
            .filter(|xs| xs.len() == 4)
            .map(|xs| {
                [
                    xs[0].as_f64().unwrap_or(0.04),
                    xs[1].as_f64().unwrap_or(0.08),
                    xs[2].as_f64().unwrap_or(0.12),
                    xs[3].as_f64().unwrap_or(0.16),
                ]
            })
            .unwrap_or([0.04, 0.08, 0.12, 0.16]);
        let mut bands = p
            .get("book_bands_pct")
            .and_then(Json::as_array)
            .map(|xs| {
                xs.iter()
                    .filter_map(Json::as_f64)
                    .filter(|v| *v > 0.0 && *v <= 0.10)
                    .collect::<Vec<_>>()
            })
            .filter(|xs| !xs.is_empty())
            .unwrap_or_else(|| vec![0.01, 0.025, 0.05]);
        bands.sort_by(f64::total_cmp);
        bands.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
        Self {
            bucket_ms: i("bucket_ms", 10_000).max(1_000),
            delta_window_ms: i("delta_window_ms", 30 * 60_000).max(60_000),
            trend_window_ms: i("trend_window_ms", 30 * 60_000).max(60_000),
            book_fresh_ms: i("book_fresh_ms", 20_000).max(1_000),
            book_bands: bands,
            min_zone_depth_usd: f("min_zone_depth_usd", 1_000_000.0).max(0.0),
            green_ratio: f("green_ratio", 1.8).max(1.01),
            yellow_ratio: f("yellow_ratio", 2.5).max(1.01),
            red_ratio: f("red_ratio", 3.5).max(1.01),
            blue_ratio: f("blue_ratio", 5.0).max(1.01),
            zone_bin_pct: f("zone_bin_pct", 0.001).clamp(0.0001, 0.01),
            min_book_sources: u("min_book_sources", 4).clamp(1, 6),
            min_spot_sources: u("min_spot_sources", 2).clamp(1, 3),
            min_perp_sources: u("min_perp_sources", 2).clamp(1, 3),
            delta_share_levels: levels,
            footprint_bin_usd: f("footprint_bin_usd", 10.0).clamp(0.5, 1_000.0),
            footprint_imbalance_share: f("footprint_imbalance_share", 0.20).clamp(0.05, 0.95),
            trend_return_pct: f("trend_return_pct", 0.012).clamp(0.001, 0.10),
            trend_efficiency: f("trend_efficiency", 0.45).clamp(0.05, 1.0),
            oi_change_threshold: f("oi_change_threshold", 0.001).clamp(0.00001, 0.10),
        }
    }
}

#[derive(Debug, Clone)]
struct BookState {
    ts_ms: i64,
    book: BookSnapshot,
}

#[derive(Debug, Clone, Default)]
struct FlowBucket {
    open: f64,
    close: f64,
    volume: f64,
    delta: f64,
    spot_volume: f64,
    spot_delta: f64,
    perp_volume: f64,
    perp_delta: f64,
    source_volume: HashMap<Exchange, f64>,
    source_delta: HashMap<Exchange, f64>,
    clusters: BTreeMap<i64, PriceCluster>,
    long_liquidation_usd: f64,
    short_liquidation_usd: f64,
    liquidation_sources: HashSet<Exchange>,
    long_liquidation_by_source: HashMap<Exchange, f64>,
    short_liquidation_by_source: HashMap<Exchange, f64>,
}

#[derive(Debug, Clone, Default)]
struct PriceCluster {
    volume_usd: f64,
    delta_usd: f64,
    trades: u64,
}

impl FlowBucket {
    fn add(&mut self, exchange: Exchange, price: f64, notional: f64, signed: f64, bin_usd: f64) {
        if self.open <= 0.0 {
            self.open = price;
        }
        self.close = price;
        self.volume += notional;
        self.delta += signed;
        *self.source_volume.entry(exchange).or_default() += notional;
        *self.source_delta.entry(exchange).or_default() += signed;
        let cluster = self
            .clusters
            .entry((price / bin_usd).floor() as i64)
            .or_default();
        cluster.volume_usd += notional;
        cluster.delta_usd += signed;
        cluster.trades += 1;
        match exchange {
            Exchange::BinanceSpot | Exchange::BybitSpot | Exchange::OkxSpot => {
                self.spot_volume += notional;
                self.spot_delta += signed;
            }
            _ => {
                self.perp_volume += notional;
                self.perp_delta += signed;
            }
        }
    }

    fn add_liquidation(&mut self, exchange: Exchange, side: Side, notional: f64) {
        self.liquidation_sources.insert(exchange);
        match side {
            Side::Sell => {
                self.long_liquidation_usd += notional;
                *self.long_liquidation_by_source.entry(exchange).or_default() += notional;
            }
            Side::Buy => {
                self.short_liquidation_usd += notional;
                *self
                    .short_liquidation_by_source
                    .entry(exchange)
                    .or_default() += notional;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BandMetric {
    band_pct: f64,
    bid_usd: f64,
    ask_usd: f64,
    ratio: f64,
    side: Option<Side>,
    grade: &'static str,
    grade_rank: u8,
    complete: bool,
    source_count: usize,
}

#[derive(Debug, Clone)]
struct BookZone {
    ts_ms: i64,
    side: Side,
    grade: &'static str,
    grade_rank: u8,
    band_pct: f64,
    ratio: f64,
    bid_usd: f64,
    ask_usd: f64,
    zone_low: f64,
    zone_high: f64,
    zone_center: f64,
    distance_pct: f64,
    wall_usd: f64,
    persistence_ms: i64,
    spot_ratio: Option<f64>,
    perp_ratio: Option<f64>,
    spot_perp_confluence: bool,
    spot_source_count: usize,
    perp_source_count: usize,
    coverage: Vec<Json>,
    bands: Vec<Json>,
}

#[derive(Debug, Clone)]
struct ZoneTracker {
    side: Side,
    center: f64,
    since_ms: i64,
}

#[derive(Debug, Clone)]
struct FlowState {
    ts_ms: i64,
    delta_usd: f64,
    volume_usd: f64,
    delta_share: f64,
    spot_delta_usd: f64,
    spot_volume_usd: f64,
    perp_delta_usd: f64,
    perp_volume_usd: f64,
    tier: u8,
    tier_name: &'static str,
    direction: &'static str,
    trade_source_count: usize,
    spot_source_count: usize,
    perp_source_count: usize,
    source_coverage_complete: bool,
    trade_sources: Vec<&'static str>,
    source_stats: Vec<Json>,
    footprint_price: Option<f64>,
    footprint_delta_usd: f64,
    footprint_volume_usd: f64,
    footprint_delta_share: f64,
    footprint_direction: &'static str,
    stacked_imbalance: usize,
    long_liquidation_usd: f64,
    short_liquidation_usd: f64,
    liquidation_sources: Vec<&'static str>,
    liquidation_stats: Vec<Json>,
    oi_change_pct: Option<f64>,
    oi_quadrant: &'static str,
    regime: &'static str,
    trend_return_pct: f64,
    trend_efficiency: f64,
}

pub struct TrdrMarketMap {
    cfg: Config,
    books: HashMap<Exchange, BookState>,
    flows: BTreeMap<i64, FlowBucket>,
    oi_by_exchange: HashMap<Exchange, f64>,
    oi_history: VecDeque<(i64, f64)>,
    last_flow_bucket: i64,
    latest_zone: Option<BookZone>,
    latest_coverage: Vec<Json>,
    latest_bands: Vec<Json>,
    zone_tracker: Option<ZoneTracker>,
    latest_flow: Option<FlowState>,
    eval: Option<Json>,
}

impl TrdrMarketMap {
    pub fn from_params(p: &Json) -> Self {
        Self::new(Config::from_params(p))
    }

    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            books: HashMap::new(),
            flows: BTreeMap::new(),
            oi_by_exchange: HashMap::new(),
            oi_history: VecDeque::new(),
            last_flow_bucket: i64::MIN,
            latest_zone: None,
            latest_coverage: Vec::new(),
            latest_bands: Vec::new(),
            zone_tracker: None,
            latest_flow: None,
            eval: None,
        }
    }

    fn grade(&self, ratio: f64) -> (&'static str, u8) {
        if ratio >= self.cfg.blue_ratio {
            ("blue", 4)
        } else if ratio >= self.cfg.red_ratio {
            ("red", 3)
        } else if ratio >= self.cfg.yellow_ratio {
            ("yellow", 2)
        } else if ratio >= self.cfg.green_ratio {
            ("green", 1)
        } else {
            ("none", 0)
        }
    }

    fn fresh_books(&self, now_ms: i64) -> Vec<BookState> {
        self.books
            .values()
            .filter(|b| now_ms - b.ts_ms <= self.cfg.book_fresh_ms)
            .cloned()
            .collect()
    }

    fn metric_for_books(&self, books: &[BookState], band: f64) -> BandMetric {
        let covered = books
            .iter()
            .filter(|b| {
                let (down, up) = Self::coverage_values(&b.book);
                down >= band * 0.98 && up >= band * 0.98
            })
            .collect::<Vec<_>>();
        let bid = covered
            .iter()
            .map(|b| b.book.bid_qty_within(band))
            .sum::<f64>();
        let ask = covered
            .iter()
            .map(|b| b.book.ask_qty_within(band))
            .sum::<f64>();
        let (side, ratio) = if bid > ask && ask > 0.0 {
            (Some(Side::Buy), bid / ask)
        } else if ask > bid && bid > 0.0 {
            (Some(Side::Sell), ask / bid)
        } else {
            (None, 1.0)
        };
        let dominant = bid.max(ask);
        let source_count = covered.len();
        let complete = source_count >= self.cfg.min_book_sources;
        let (grade, grade_rank) = if complete && dominant >= self.cfg.min_zone_depth_usd {
            self.grade(ratio)
        } else {
            ("none", 0)
        };
        BandMetric {
            band_pct: band,
            bid_usd: bid,
            ask_usd: ask,
            ratio,
            side,
            grade,
            grade_rank,
            complete,
            source_count,
        }
    }

    fn one_exchange_ratio(
        &self,
        exchange: Exchange,
        band: f64,
        now_ms: i64,
    ) -> Option<(Side, f64, u8)> {
        let state = self.books.get(&exchange)?;
        if now_ms - state.ts_ms > self.cfg.book_fresh_ms {
            return None;
        }
        let (down, up) = Self::coverage_values(&state.book);
        if down < band * 0.98 || up < band * 0.98 {
            return None;
        }
        let bid = state.book.bid_qty_within(band);
        let ask = state.book.ask_qty_within(band);
        let (side, ratio) = if bid > ask && ask > 0.0 {
            (Side::Buy, bid / ask)
        } else if ask > bid && bid > 0.0 {
            (Side::Sell, ask / bid)
        } else {
            return None;
        };
        let (_, grade_rank) = self.grade(ratio);
        Some((side, ratio, grade_rank))
    }

    fn coverage(&self, book: &BookSnapshot, now_ms: i64) -> Json {
        let (down, up) = Self::coverage_values(book);
        let bands = self
            .cfg
            .book_bands
            .iter()
            .map(|band| {
                json!({
                    "band_pct":band,
                    "bid_usd":book.bid_qty_within(*band),
                    "ask_usd":book.ask_qty_within(*band),
                    "depth_complete":down >= *band * 0.98 && up >= *band * 0.98
                })
            })
            .collect::<Vec<_>>();
        json!({
            "exchange":book.exchange.as_str(),
            "snapshot_ts_ms":book.ts.as_millis(),
            "age_ms":(now_ms - book.ts.as_millis()).max(0),
            "levels_bid":book.bids.len(),
            "levels_ask":book.asks.len(),
            "coverage_down_pct":down,
            "coverage_up_pct":up,
            "bands":bands
        })
    }

    fn coverage_values(book: &BookSnapshot) -> (f64, f64) {
        let mid = book.mid_price().map(|p| p.to_f64()).unwrap_or(0.0);
        let min_bid = book.bids.last().map(|x| x.0.to_f64()).unwrap_or(mid);
        let max_ask = book.asks.last().map(|x| x.0.to_f64()).unwrap_or(mid);
        if mid > 0.0 {
            (1.0 - min_bid / mid, max_ask / mid - 1.0)
        } else {
            (0.0, 0.0)
        }
    }

    fn build_zone(&mut self, now_ms: i64) -> Option<BookZone> {
        let books = self.fresh_books(now_ms);
        if books.is_empty() {
            self.zone_tracker = None;
            self.latest_coverage.clear();
            self.latest_bands.clear();
            return None;
        }
        let metrics = self
            .cfg
            .book_bands
            .iter()
            .map(|band| self.metric_for_books(&books, *band))
            .collect::<Vec<_>>();
        self.latest_coverage = books
            .iter()
            .map(|b| self.coverage(&b.book, now_ms))
            .collect();
        self.latest_bands = metrics
            .iter()
            .map(|m| {
                json!({
                    "band_pct":m.band_pct, "bid_usd":m.bid_usd, "ask_usd":m.ask_usd,
                    "ratio":m.ratio, "side":m.side.map(side_name), "grade":m.grade,
                    "complete":m.complete, "source_count":m.source_count,
                    "required_sources":self.cfg.min_book_sources
                })
            })
            .collect();
        let best = metrics
            .iter()
            .filter(|m| m.grade_rank > 0 && m.side.is_some())
            .max_by(|a, b| {
                a.grade_rank
                    .cmp(&b.grade_rank)
                    .then_with(|| b.band_pct.total_cmp(&a.band_pct))
            })?
            .clone();
        let side = best.side?;
        let mids = books
            .iter()
            .filter_map(|b| b.book.mid_price().map(|p| p.to_f64()))
            .collect::<Vec<_>>();
        let reference_mid = mids.iter().sum::<f64>() / mids.len() as f64;
        let bin_width = (reference_mid * self.cfg.zone_bin_pct).max(0.01);
        let mut bins = BTreeMap::<i64, f64>::new();
        for state in books.iter().filter(|state| {
            let (down, up) = Self::coverage_values(&state.book);
            down >= best.band_pct * 0.98 && up >= best.band_pct * 0.98
        }) {
            let levels = match side {
                Side::Buy => &state.book.bids,
                Side::Sell => &state.book.asks,
            };
            for (price, qty) in levels {
                let px = price.to_f64();
                let within = match side {
                    Side::Buy => px <= reference_mid && px >= reference_mid * (1.0 - best.band_pct),
                    Side::Sell => {
                        px >= reference_mid && px <= reference_mid * (1.0 + best.band_pct)
                    }
                };
                if within {
                    let key = (px / bin_width).floor() as i64;
                    *bins.entry(key).or_default() += px * qty.to_f64();
                }
            }
        }
        let (best_bin, wall_usd) = bins
            .into_iter()
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .unwrap_or(((reference_mid / bin_width).floor() as i64, 0.0));
        let zone_low = best_bin as f64 * bin_width;
        let zone_high = zone_low + bin_width;
        let center = (zone_low + zone_high) / 2.0;
        let same_zone = self.zone_tracker.as_ref().is_some_and(|z| {
            z.side == side && z.center > 0.0 && (center / z.center - 1.0).abs() <= 0.005
        });
        if !same_zone {
            self.zone_tracker = Some(ZoneTracker {
                side,
                center,
                since_ms: now_ms,
            });
        }
        let persistence_ms = self
            .zone_tracker
            .as_ref()
            .map(|z| now_ms - z.since_ms)
            .unwrap_or(0);
        let spot_metrics = [
            Exchange::BinanceSpot,
            Exchange::BybitSpot,
            Exchange::OkxSpot,
        ]
        .into_iter()
        .filter_map(|ex| self.one_exchange_ratio(ex, best.band_pct, now_ms))
        .filter(|x| x.0 == side && x.2 > 0)
        .collect::<Vec<_>>();
        let perp_metrics = [
            Exchange::BinanceFutures,
            Exchange::BybitFutures,
            Exchange::OkxFutures,
        ]
        .into_iter()
        .filter_map(|ex| self.one_exchange_ratio(ex, best.band_pct, now_ms))
        .filter(|x| x.0 == side && x.2 > 0)
        .collect::<Vec<_>>();
        let spot_source_count = spot_metrics.len();
        let perp_source_count = perp_metrics.len();
        let confluence = spot_source_count >= self.cfg.min_spot_sources
            && perp_source_count >= self.cfg.min_perp_sources;
        let average_ratio = |xs: &[(Side, f64, u8)]| {
            (!xs.is_empty()).then(|| xs.iter().map(|x| x.1).sum::<f64>() / xs.len() as f64)
        };
        Some(BookZone {
            ts_ms: now_ms,
            side,
            grade: best.grade,
            grade_rank: best.grade_rank,
            band_pct: best.band_pct,
            ratio: best.ratio,
            bid_usd: best.bid_usd,
            ask_usd: best.ask_usd,
            zone_low,
            zone_high,
            zone_center: center,
            distance_pct: center / reference_mid - 1.0,
            wall_usd,
            persistence_ms,
            spot_ratio: average_ratio(&spot_metrics),
            perp_ratio: average_ratio(&perp_metrics),
            spot_perp_confluence: confluence,
            spot_source_count,
            perp_source_count,
            coverage: self.latest_coverage.clone(),
            bands: self.latest_bands.clone(),
        })
    }

    fn prune_flows(&mut self, now_ms: i64) {
        let keep_ms = self.cfg.delta_window_ms.max(self.cfg.trend_window_ms) + self.cfg.bucket_ms;
        self.flows.retain(|start, _| now_ms - *start <= keep_ms);
    }

    fn delta_tier(&self, delta: f64, volume: f64) -> (u8, &'static str) {
        let abs = delta.abs();
        let share = if volume > 0.0 { abs / volume } else { 0.0 };
        // 已接入三家现货+永续后直接使用作者的全市场美元档位，不再按猜测覆盖率缩放。
        let static_rank = if abs >= 3_500_000_000.0 {
            4
        } else if abs >= 3_000_000_000.0 {
            3
        } else if abs >= 2_000_000_000.0 {
            2
        } else if abs >= 1_000_000_000.0 {
            1
        } else {
            0
        };
        let share_rank = self
            .cfg
            .delta_share_levels
            .iter()
            .position(|v| share < *v)
            .map(|i| i as u8)
            .unwrap_or(4);
        let rank = static_rank.max(share_rank);
        (
            rank,
            ["none", "watch", "entry", "high", "extreme"][rank as usize],
        )
    }

    fn total_oi(&self) -> Option<f64> {
        (!self.oi_by_exchange.is_empty()).then(|| self.oi_by_exchange.values().sum())
    }

    fn build_flow_state(&self, now_ms: i64) -> FlowState {
        let delta_from = now_ms - self.cfg.delta_window_ms;
        let trend_from = now_ms - self.cfg.trend_window_ms;
        let mut volume = 0.0;
        let mut delta = 0.0;
        let mut spot_volume = 0.0;
        let mut spot_delta = 0.0;
        let mut perp_volume = 0.0;
        let mut perp_delta = 0.0;
        let mut sources = HashSet::new();
        let mut source_volume = HashMap::<Exchange, f64>::new();
        let mut source_delta = HashMap::<Exchange, f64>::new();
        let mut clusters = BTreeMap::<i64, PriceCluster>::new();
        let mut long_liquidation_usd = 0.0;
        let mut short_liquidation_usd = 0.0;
        let mut liquidation_sources = HashSet::new();
        let mut long_liquidation_by_source = HashMap::<Exchange, f64>::new();
        let mut short_liquidation_by_source = HashMap::<Exchange, f64>::new();
        let mut trend = Vec::new();
        for (start, b) in &self.flows {
            if *start >= delta_from {
                volume += b.volume;
                delta += b.delta;
                spot_volume += b.spot_volume;
                spot_delta += b.spot_delta;
                perp_volume += b.perp_volume;
                perp_delta += b.perp_delta;
                sources.extend(
                    b.source_volume
                        .iter()
                        .filter(|(_, volume)| **volume > 0.0)
                        .map(|(exchange, _)| *exchange),
                );
                for (exchange, value) in &b.source_volume {
                    *source_volume.entry(*exchange).or_default() += value;
                }
                for (exchange, value) in &b.source_delta {
                    *source_delta.entry(*exchange).or_default() += value;
                }
                for (price_bin, cluster) in &b.clusters {
                    let target = clusters.entry(*price_bin).or_default();
                    target.volume_usd += cluster.volume_usd;
                    target.delta_usd += cluster.delta_usd;
                    target.trades += cluster.trades;
                }
                long_liquidation_usd += b.long_liquidation_usd;
                short_liquidation_usd += b.short_liquidation_usd;
                liquidation_sources.extend(b.liquidation_sources.iter().copied());
                for (exchange, value) in &b.long_liquidation_by_source {
                    *long_liquidation_by_source.entry(*exchange).or_default() += value;
                }
                for (exchange, value) in &b.short_liquidation_by_source {
                    *short_liquidation_by_source.entry(*exchange).or_default() += value;
                }
            }
            if *start >= trend_from && b.open > 0.0 {
                trend.push(b);
            }
        }
        let delta_share = if volume > 0.0 { delta / volume } else { 0.0 };
        let (tier, tier_name) = self.delta_tier(delta, volume);
        let footprint = clusters
            .iter()
            .max_by(|a, b| a.1.delta_usd.abs().total_cmp(&b.1.delta_usd.abs()));
        let (footprint_price, footprint_delta_usd, footprint_volume_usd) = footprint
            .map(|(bin, cluster)| {
                (
                    Some((*bin as f64 + 0.5) * self.cfg.footprint_bin_usd),
                    cluster.delta_usd,
                    cluster.volume_usd,
                )
            })
            .unwrap_or((None, 0.0, 0.0));
        let footprint_delta_share = if footprint_volume_usd > 0.0 {
            footprint_delta_usd / footprint_volume_usd
        } else {
            0.0
        };
        let footprint_sign = footprint_delta_usd.signum();
        let mut stacked_imbalance = 0usize;
        let mut current_stack = 0usize;
        let mut previous_bin = None;
        for (bin, cluster) in &clusters {
            let share = if cluster.volume_usd > 0.0 {
                cluster.delta_usd / cluster.volume_usd
            } else {
                0.0
            };
            let adjacent = previous_bin.is_some_and(|last| *bin == last + 1);
            if share.abs() >= self.cfg.footprint_imbalance_share && share.signum() == footprint_sign
            {
                current_stack = if adjacent { current_stack + 1 } else { 1 };
                stacked_imbalance = stacked_imbalance.max(current_stack);
            } else {
                current_stack = 0;
            }
            previous_bin = Some(*bin);
        }
        let spot_source_count = sources.iter().filter(|x| is_spot(**x)).count();
        let perp_source_count = sources.iter().filter(|x| is_perp(**x)).count();
        let source_coverage_complete = spot_source_count >= self.cfg.min_spot_sources
            && perp_source_count >= self.cfg.min_perp_sources;
        let mut trade_sources = sources.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        trade_sources.sort_unstable();
        let source_stats = trade_sources
            .iter()
            .map(|name| {
                let exchange = sources
                    .iter()
                    .find(|exchange| exchange.as_str() == *name)
                    .copied()
                    .expect("trade source exists");
                let volume = source_volume.get(&exchange).copied().unwrap_or(0.0);
                let delta = source_delta.get(&exchange).copied().unwrap_or(0.0);
                json!({
                    "exchange":name,
                    "volume_usd":volume,
                    "delta_usd":delta,
                    "delta_share":if volume > 0.0 { delta / volume } else { 0.0 }
                })
            })
            .collect::<Vec<_>>();
        let mut liquidation_sources = liquidation_sources
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<_>>();
        liquidation_sources.sort_unstable();
        let liquidation_stats = [
            Exchange::BinanceFutures,
            Exchange::BybitFutures,
            Exchange::OkxFutures,
        ]
            .into_iter()
            .filter(|exchange| liquidation_sources.contains(&exchange.as_str()))
            .map(|exchange| {
                json!({
                    "exchange":exchange.as_str(),
                    "long_liquidation_usd":long_liquidation_by_source.get(&exchange).copied().unwrap_or(0.0),
                    "short_liquidation_usd":short_liquidation_by_source.get(&exchange).copied().unwrap_or(0.0)
                })
            })
            .collect::<Vec<_>>();
        let first = trend.first().map(|b| b.open).unwrap_or(0.0);
        let last = trend.last().map(|b| b.close).unwrap_or(first);
        let trend_return = if first > 0.0 { last / first - 1.0 } else { 0.0 };
        let mut path = 0.0;
        let mut prev = first;
        for b in &trend {
            path += (b.close - prev).abs();
            prev = b.close;
        }
        let efficiency = if path > 0.0 {
            (last - first).abs() / path
        } else {
            0.0
        };
        let aligned = trend_return.signum() == delta.signum();
        let regime = if aligned
            && trend_return >= self.cfg.trend_return_pct
            && efficiency >= self.cfg.trend_efficiency
        {
            "trend_up"
        } else if aligned
            && trend_return <= -self.cfg.trend_return_pct
            && efficiency >= self.cfg.trend_efficiency
        {
            "trend_down"
        } else {
            "range"
        };
        let oi_change = match (self.oi_history.front(), self.oi_history.back()) {
            (Some((_, first)), Some((_, last))) if *first > 0.0 => Some(last / first - 1.0),
            _ => None,
        };
        let oi_quadrant = match oi_change {
            Some(v) if v >= self.cfg.oi_change_threshold && delta > 0.0 => "new_longs",
            Some(v) if v >= self.cfg.oi_change_threshold && delta < 0.0 => "new_shorts",
            Some(v) if v <= -self.cfg.oi_change_threshold && delta > 0.0 => "short_cover",
            Some(v) if v <= -self.cfg.oi_change_threshold && delta < 0.0 => "long_liquidation",
            _ => "neutral",
        };
        FlowState {
            ts_ms: now_ms,
            delta_usd: delta,
            volume_usd: volume,
            delta_share,
            spot_delta_usd: spot_delta,
            spot_volume_usd: spot_volume,
            perp_delta_usd: perp_delta,
            perp_volume_usd: perp_volume,
            tier,
            tier_name,
            direction: if delta > 0.0 {
                "buy"
            } else if delta < 0.0 {
                "sell"
            } else {
                "neutral"
            },
            trade_source_count: sources.len(),
            spot_source_count,
            perp_source_count,
            source_coverage_complete,
            trade_sources,
            source_stats,
            footprint_price,
            footprint_delta_usd,
            footprint_volume_usd,
            footprint_delta_share,
            footprint_direction: if footprint_delta_usd > 0.0 {
                "buy"
            } else if footprint_delta_usd < 0.0 {
                "sell"
            } else {
                "neutral"
            },
            stacked_imbalance,
            long_liquidation_usd,
            short_liquidation_usd,
            liquidation_sources,
            liquidation_stats,
            oi_change_pct: oi_change,
            oi_quadrant,
            regime,
            trend_return_pct: trend_return,
            trend_efficiency: efficiency,
        }
    }

    fn zone_payload(zone: Option<&BookZone>) -> Json {
        match zone {
            Some(z) => json!({
                "active":true, "ts_ms":z.ts_ms, "side":side_name(z.side),
                "grade":z.grade, "grade_rank":z.grade_rank, "band_pct":z.band_pct,
                "ratio":z.ratio, "bid_usd":z.bid_usd, "ask_usd":z.ask_usd,
                "zone_low":z.zone_low, "zone_high":z.zone_high, "zone_center":z.zone_center,
                "distance_pct":z.distance_pct, "wall_usd":z.wall_usd,
                "persistence_ms":z.persistence_ms, "spot_ratio":z.spot_ratio,
                "perp_ratio":z.perp_ratio, "spot_perp_confluence":z.spot_perp_confluence,
                "spot_source_count":z.spot_source_count,"perp_source_count":z.perp_source_count,
                "coverage":z.coverage, "bands":z.bands
            }),
            None => json!({"active":false}),
        }
    }

    fn flow_payload(flow: Option<&FlowState>) -> Json {
        match flow {
            Some(f) => json!({
                "ts_ms":f.ts_ms, "window_ms":null, "delta_usd":f.delta_usd,
                "volume_usd":f.volume_usd, "delta_share":f.delta_share,
                "spot_delta_usd":f.spot_delta_usd, "spot_volume_usd":f.spot_volume_usd,
                "perp_delta_usd":f.perp_delta_usd, "perp_volume_usd":f.perp_volume_usd,
                "tier":f.tier, "tier_name":f.tier_name, "direction":f.direction,
                "trade_source_count":f.trade_source_count,
                "spot_source_count":f.spot_source_count,"perp_source_count":f.perp_source_count,
                "source_coverage_complete":f.source_coverage_complete,"trade_sources":f.trade_sources,
                "source_stats":f.source_stats,
                "footprint_price":f.footprint_price,"footprint_delta_usd":f.footprint_delta_usd,
                "footprint_volume_usd":f.footprint_volume_usd,
                "footprint_delta_share":f.footprint_delta_share,
                "footprint_direction":f.footprint_direction,"stacked_imbalance":f.stacked_imbalance,
                "long_liquidation_usd":f.long_liquidation_usd,
                "short_liquidation_usd":f.short_liquidation_usd,
                "liquidation_source_count":f.liquidation_sources.len(),
                "liquidation_sources":f.liquidation_sources,
                "liquidation_stats":f.liquidation_stats,
                "oi_change_pct":f.oi_change_pct, "oi_quadrant":f.oi_quadrant,
                "regime":f.regime, "trend_return_pct":f.trend_return_pct,
                "trend_efficiency":f.trend_efficiency
            }),
            None => json!({"tier":0,"tier_name":"none","direction":"neutral","regime":"warming"}),
        }
    }

    fn update_eval(&mut self, ts_ms: i64) {
        let mut zone = Self::zone_payload(self.latest_zone.as_ref());
        if self.latest_zone.is_none() {
            zone["coverage"] = json!(self.latest_coverage);
            zone["bands"] = json!(self.latest_bands);
        }
        let mut flow = Self::flow_payload(self.latest_flow.as_ref());
        if let Some(obj) = flow.as_object_mut() {
            obj.insert("window_ms".into(), json!(self.cfg.delta_window_ms));
        }
        let regime = self
            .latest_flow
            .as_ref()
            .map(|f| f.regime)
            .unwrap_or("warming");
        let reason = match (self.latest_zone.as_ref(), self.latest_flow.as_ref()) {
            (Some(z), Some(f)) => format!(
                "{} {} 色带，30m Delta {}，市场 {}",
                side_cn(z.side),
                z.grade,
                f.tier_name,
                f.regime
            ),
            (Some(z), None) => format!("{} {} 色带，Delta 预热中", side_cn(z.side), z.grade),
            (None, Some(f)) => {
                format!("暂无有效色带，30m Delta {}，市场 {}", f.tier_name, f.regime)
            }
            (None, None) => "等待现货/永续订单簿与 30m Delta 预热".to_string(),
        };
        let trade_sources = self
            .latest_flow
            .as_ref()
            .map(|f| f.trade_sources.clone())
            .unwrap_or_default();
        self.eval = Some(json!({
            "ts_ms":ts_ms.div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms,
            "decision":"market_map", "reason":reason, "regime":regime,
            "zone":zone, "flow":flow,
            "book_sources":self.fresh_books(ts_ms).iter().map(|x| x.book.exchange.as_str()).collect::<Vec<_>>(),
            "trade_sources":trade_sources,
            "limitations":["TRDR 专有热图由三所公开逐笔和订单簿等价重建，不依赖 TRDR 私有接口"]
        }));
    }

    fn zone_signal(&self, ts: Timestamp) -> Signal {
        Signal::new(
            SignalKind::ObiZone,
            ts,
            "TrdrMarketMap",
            Self::zone_payload(self.latest_zone.as_ref()),
        )
    }

    fn flow_signals(&self, ts: Timestamp) -> Vec<Signal> {
        let Some(f) = self.latest_flow.as_ref() else {
            return vec![];
        };
        vec![
            Signal::new(
                SignalKind::DeltaTier,
                ts,
                "TrdrMarketMap",
                Self::flow_payload(Some(f)),
            ),
            Signal::new(
                SignalKind::OiQuadrant,
                ts,
                "TrdrMarketMap",
                json!({"ts_ms":f.ts_ms,"quadrant":f.oi_quadrant,"oi_change_pct":f.oi_change_pct,"delta_usd":f.delta_usd}),
            ),
            Signal::new(
                SignalKind::TrendRegime,
                ts,
                "TrdrMarketMap",
                json!({
                    "ts_ms":f.ts_ms,"regime":f.regime,
                    "blocked_side":match f.regime { "trend_up"=>"sell", "trend_down"=>"buy", _=>"none" },
                    "return_pct":f.trend_return_pct,"efficiency":f.trend_efficiency
                }),
            ),
        ]
    }
}

impl SignalPlugin for TrdrMarketMap {
    fn name(&self) -> &'static str {
        "TrdrMarketMap"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Book(book) => {
                let ts_ms = book.ts.as_millis();
                self.books.insert(
                    book.exchange,
                    BookState {
                        ts_ms,
                        book: book.clone(),
                    },
                );
                self.latest_zone = self.build_zone(ts_ms);
                self.update_eval(ts_ms);
                vec![self.zone_signal(book.ts)]
            }
            Event::Oi(oi) => {
                let ts_ms = oi.ts.as_millis();
                self.oi_by_exchange.insert(oi.exchange, oi.oi_usd);
                if let Some(total) = self.total_oi() {
                    self.oi_history.push_back((ts_ms, total));
                    while self
                        .oi_history
                        .front()
                        .is_some_and(|(ts, _)| ts_ms - *ts > self.cfg.delta_window_ms)
                    {
                        self.oi_history.pop_front();
                    }
                }
                self.latest_flow = Some(self.build_flow_state(ts_ms));
                self.update_eval(ts_ms);
                self.flow_signals(oi.ts)
            }
            Event::Trade(trade) => {
                let ts_ms = trade.ts.as_millis();
                let start = ts_ms.div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms;
                self.flows.entry(start).or_default().add(
                    trade.exchange,
                    trade.price.to_f64(),
                    trade.notional(),
                    trade.signed_notional(),
                    self.cfg.footprint_bin_usd,
                );
                self.prune_flows(ts_ms);
                if start == self.last_flow_bucket {
                    return vec![];
                }
                self.last_flow_bucket = start;
                self.latest_flow = Some(self.build_flow_state(ts_ms));
                self.update_eval(ts_ms);
                self.flow_signals(trade.ts)
            }
            Event::Liquidation(tick) => {
                let ts_ms = tick.ts.as_millis();
                let start = ts_ms.div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms;
                self.flows.entry(start).or_default().add_liquidation(
                    tick.exchange,
                    tick.side,
                    tick.notional(),
                );
                self.prune_flows(ts_ms);
                self.latest_flow = Some(self.build_flow_state(ts_ms));
                self.update_eval(ts_ms);
                self.flow_signals(tick.ts)
            }
            Event::Funding(_) | Event::Timer(_) => vec![],
        }
    }

    fn eval_note(&self) -> Option<Json> {
        self.eval.clone()
    }
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn is_spot(exchange: Exchange) -> bool {
    matches!(
        exchange,
        Exchange::BinanceSpot | Exchange::BybitSpot | Exchange::OkxSpot
    )
}

fn is_perp(exchange: Exchange) -> bool {
    matches!(
        exchange,
        Exchange::BinanceFutures | Exchange::BybitFutures | Exchange::OkxFutures
    )
}

fn side_cn(side: Side) -> &'static str {
    match side {
        Side::Buy => "下方支撑",
        Side::Sell => "上方压力",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::{LiquidationTick, Price, Qty, Symbol, Trade};

    fn book(exchange: Exchange, ts_ms: i64, bid_qty: f64, ask_qty: f64) -> BookSnapshot {
        BookSnapshot {
            ts: Timestamp::from_millis(ts_ms),
            exchange,
            symbol: Symbol::new("BTCUSDT"),
            bids: vec![
                (Price::from_f64(99.9), Qty::from_f64(bid_qty)),
                (Price::from_f64(99.0), Qty::from_f64(bid_qty)),
            ],
            asks: vec![
                (Price::from_f64(100.1), Qty::from_f64(ask_qty)),
                (Price::from_f64(101.0), Qty::from_f64(ask_qty)),
            ],
        }
    }

    #[test]
    fn aggregates_spot_and_perp_into_blue_support() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.min_zone_depth_usd = 1.0;
        cfg.min_book_sources = 2;
        cfg.min_spot_sources = 1;
        cfg.min_perp_sources = 1;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        map.on_event(
            &Event::Book(book(Exchange::BinanceFutures, 1_000, 6.0, 1.0)),
            &ctx,
        );
        let out = map.on_event(
            &Event::Book(book(Exchange::BinanceSpot, 2_000, 6.0, 1.0)),
            &ctx,
        );
        assert_eq!(out[0].kind, SignalKind::ObiZone);
        assert_eq!(out[0].payload["side"], "buy");
        assert_eq!(out[0].payload["grade"], "blue");
        assert_eq!(out[0].payload["spot_perp_confluence"], true);
        assert!(out[0].payload["coverage"][0]["bands"].is_array());
        assert!(out[0].payload["coverage"][0]["age_ms"].is_number());
    }

    #[test]
    fn emits_delta_and_regime_signals_on_bucket_change() {
        let mut cfg = Config::from_params(&json!({"delta_share_levels":[0.01,0.02,0.03,0.04]}));
        cfg.trend_return_pct = 0.005;
        cfg.trend_efficiency = 0.2;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        let mut last = vec![];
        for (ts, px) in [(1_000, 100.0), (11_000, 101.0), (21_000, 102.0)] {
            let trade = Trade {
                ts: Timestamp::from_millis(ts),
                exchange: Exchange::BinanceFutures,
                symbol: Symbol::new("BTCUSDT"),
                price: Price::from_f64(px),
                qty: Qty::from_f64(100.0),
                is_buyer_maker: false,
            };
            last = map.on_event(&Event::Trade(trade), &ctx);
            assert!(last.iter().any(|s| s.kind == SignalKind::DeltaTier));
        }
        assert_eq!(map.latest_flow.as_ref().unwrap().regime, "trend_up");
        let delta = last
            .iter()
            .find(|signal| signal.kind == SignalKind::DeltaTier)
            .unwrap();
        assert_eq!(
            delta.payload["source_stats"][0]["exchange"],
            "binance_futures"
        );
        assert!(
            delta.payload["source_stats"][0]["volume_usd"]
                .as_f64()
                .unwrap()
                > 0.0
        );
    }

    #[test]
    fn liquidation_payload_keeps_per_exchange_attribution() {
        let mut map = TrdrMarketMap::new(Config::from_params(&json!({})));
        let tick = LiquidationTick {
            ts: Timestamp::from_millis(1_000),
            exchange: Exchange::BybitFutures,
            symbol: Symbol::new("BTCUSDT"),
            side: Side::Sell,
            price: Price::from_f64(100.0),
            qty: Qty::from_f64(2.0),
        };
        let out = map.on_event(&Event::Liquidation(tick), &Ctx::default());
        let delta = out
            .iter()
            .find(|signal| signal.kind == SignalKind::DeltaTier)
            .unwrap();
        assert_eq!(
            delta.payload["liquidation_stats"][0]["exchange"],
            "bybit_futures"
        );
        assert_eq!(
            delta.payload["liquidation_stats"][0]["long_liquidation_usd"],
            200.0
        );
    }
}
