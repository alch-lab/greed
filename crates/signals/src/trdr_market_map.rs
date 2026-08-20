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
    pub trend_slow_window_ms: i64,
    pub trend_anchor_window_ms: i64,
    pub book_fresh_ms: i64,
    pub book_bands: Vec<f64>,
    pub min_zone_depth_usd: f64,
    pub green_ratio: f64,
    pub yellow_ratio: f64,
    pub red_ratio: f64,
    pub blue_ratio: f64,
    pub zone_bin_pct: f64,
    /// 单一交易所在同一局部价格簇中支持主导方向的最低买卖比。
    pub source_wall_ratio: f64,
    pub min_book_sources: usize,
    pub min_spot_sources: usize,
    pub min_perp_sources: usize,
    pub delta_share_levels: [f64; 4],
    pub footprint_bin_usd: f64,
    pub footprint_imbalance_share: f64,
    pub trend_return_pct: f64,
    pub trend_efficiency: f64,
    pub trend_slow_return_pct: f64,
    pub trend_slow_efficiency: f64,
    pub trend_anchor_return_pct: f64,
    pub trend_anchor_efficiency: f64,
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
            trend_slow_window_ms: i("trend_slow_window_ms", 2 * 60 * 60_000).max(5 * 60_000),
            trend_anchor_window_ms: i("trend_anchor_window_ms", 6 * 60 * 60_000).max(30 * 60_000),
            book_fresh_ms: i("book_fresh_ms", 20_000).max(1_000),
            book_bands: bands,
            min_zone_depth_usd: f("min_zone_depth_usd", 1_000_000.0).max(0.0),
            green_ratio: f("green_ratio", 1.8).max(1.01),
            yellow_ratio: f("yellow_ratio", 2.5).max(1.01),
            red_ratio: f("red_ratio", 3.5).max(1.01),
            blue_ratio: f("blue_ratio", 5.0).max(1.01),
            zone_bin_pct: f("zone_bin_pct", 0.001).clamp(0.0001, 0.01),
            source_wall_ratio: f("source_wall_ratio", 1.20).clamp(1.01, 10.0),
            min_book_sources: u("min_book_sources", 4).clamp(1, 6),
            min_spot_sources: u("min_spot_sources", 2).clamp(1, 3),
            min_perp_sources: u("min_perp_sources", 2).clamp(1, 3),
            delta_share_levels: levels,
            footprint_bin_usd: f("footprint_bin_usd", 10.0).clamp(0.5, 1_000.0),
            footprint_imbalance_share: f("footprint_imbalance_share", 0.20).clamp(0.05, 0.95),
            trend_return_pct: f("trend_return_pct", 0.012).clamp(0.001, 0.10),
            trend_efficiency: f("trend_efficiency", 0.45).clamp(0.05, 1.0),
            trend_slow_return_pct: f("trend_slow_return_pct", 0.005).clamp(0.001, 0.10),
            trend_slow_efficiency: f("trend_slow_efficiency", 0.10).clamp(0.01, 1.0),
            trend_anchor_return_pct: f("trend_anchor_return_pct", 0.010).clamp(0.002, 0.20),
            trend_anchor_efficiency: f("trend_anchor_efficiency", 0.08).clamp(0.01, 1.0),
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
    reference_open: f64,
    reference_close: f64,
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
        if exchange == Exchange::BinanceFutures {
            if self.reference_open <= 0.0 {
                self.reference_open = price;
            }
            self.reference_close = price;
        }
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
    distance_bin: usize,
    source_wall_ratio: f64,
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
    trend_slow_return_pct: f64,
    trend_slow_efficiency: f64,
    trend_slow_window_ms: i64,
    trend_anchor_return_pct: f64,
    trend_anchor_efficiency: f64,
    trend_anchor_window_ms: i64,
    mr_allowed: bool,
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
    /// 当前完整价格带内，每个来源、每个镜像距离档的原始派生值。
    /// 即使没有达到色带门槛也保留，供后续离线重放和阈值优化。
    latest_local_bins: Json,
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
            latest_local_bins: json!({}),
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
        let mut books = self
            .books
            .values()
            .filter(|b| now_ms - b.ts_ms <= self.cfg.book_fresh_ms)
            .cloned()
            .collect::<Vec<_>>();
        books.sort_by_key(|state| state.book.exchange.as_str());
        books
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
        let grade = if complete && dominant >= self.cfg.min_zone_depth_usd {
            self.grade(ratio).0
        } else {
            "none"
        };
        BandMetric {
            band_pct: band,
            bid_usd: bid,
            ask_usd: ask,
            ratio,
            side,
            grade,
            complete,
            source_count,
        }
    }

    fn distance_bin_notional(
        book: &BookSnapshot,
        band: f64,
        distance_bin: usize,
        bin_pct: f64,
    ) -> (f64, f64) {
        let Some(mid) = book.mid_price().map(|p| p.to_f64()) else {
            return (0.0, 0.0);
        };
        if mid <= 0.0 {
            return (0.0, 0.0);
        }
        let lower = distance_bin as f64 * bin_pct;
        let upper = ((distance_bin + 1) as f64 * bin_pct).min(band);
        let in_bin = |distance: f64| {
            distance >= lower && (distance < upper || (upper >= band && distance <= upper * 1.001))
        };
        let bid = book
            .bids
            .iter()
            .filter_map(|(price, qty)| {
                let px = price.to_f64();
                let distance = 1.0 - px / mid;
                (distance >= 0.0 && in_bin(distance)).then(|| px * qty.to_f64())
            })
            .sum();
        let ask = book
            .asks
            .iter()
            .filter_map(|(price, qty)| {
                let px = price.to_f64();
                let distance = px / mid - 1.0;
                (distance >= 0.0 && in_bin(distance)).then(|| px * qty.to_f64())
            })
            .sum();
        (bid, ask)
    }

    fn one_exchange_bin_ratio(
        &self,
        exchange: Exchange,
        band: f64,
        distance_bin: usize,
        now_ms: i64,
    ) -> Option<(Side, f64)> {
        let state = self.books.get(&exchange)?;
        if now_ms - state.ts_ms > self.cfg.book_fresh_ms {
            return None;
        }
        let (down, up) = Self::coverage_values(&state.book);
        if down < band * 0.98 || up < band * 0.98 {
            return None;
        }
        let (bid, ask) =
            Self::distance_bin_notional(&state.book, band, distance_bin, self.cfg.zone_bin_pct);
        let (side, ratio) = if bid > ask && ask > 0.0 {
            (Side::Buy, bid / ask)
        } else if ask > bid && bid > 0.0 {
            (Side::Sell, ask / bid)
        } else {
            return None;
        };
        (ratio >= self.cfg.source_wall_ratio).then_some((side, ratio))
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
            self.latest_local_bins = json!({});
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
        // 先选择覆盖完整的最窄价格带，再在带内按“距各自中间价的局部价格簇”寻找墙体。
        // 旧实现先要求整片盘口达到 1.8x/2.5x，真实多市场深度会相互稀释，导致区域
        // 永远无法成立。局部镜像买卖比更接近热图对单个流动性墙的定义。
        let Some(selected_band) = metrics.iter().find(|m| m.complete).map(|m| m.band_pct) else {
            self.zone_tracker = None;
            self.latest_local_bins = json!({
                "ready":false,
                "reason":"no_complete_band",
                "required_sources":self.cfg.min_book_sources,
                "source_count":books.len(),
            });
            return None;
        };
        let covered = books
            .iter()
            .filter(|state| {
                let (down, up) = Self::coverage_values(&state.book);
                down >= selected_band * 0.98 && up >= selected_band * 0.98
            })
            .collect::<Vec<_>>();
        let mids = books
            .iter()
            .filter_map(|b| b.book.mid_price().map(|p| p.to_f64()))
            .collect::<Vec<_>>();
        let reference_mid = mids.iter().sum::<f64>() / mids.len() as f64;
        let n_bins = (selected_band / self.cfg.zone_bin_pct).ceil().max(1.0) as usize;
        let source_bins = covered
            .iter()
            .map(|state| {
                let bins = (0..n_bins)
                    .map(|distance_bin| {
                        let (bid, ask) = Self::distance_bin_notional(
                            &state.book,
                            selected_band,
                            distance_bin,
                            self.cfg.zone_bin_pct,
                        );
                        let (side, ratio) = if bid > ask && ask > 0.0 {
                            (Some("buy"), Some(bid / ask))
                        } else if ask > bid && bid > 0.0 {
                            (Some("sell"), Some(ask / bid))
                        } else {
                            (None, None)
                        };
                        json!({
                            "distance_bin":distance_bin,
                            "from_pct":distance_bin as f64 * self.cfg.zone_bin_pct,
                            "to_pct":((distance_bin + 1) as f64 * self.cfg.zone_bin_pct).min(selected_band),
                            "bid_usd":bid, "ask_usd":ask, "side":side, "ratio":ratio,
                            "meets_source_wall":ratio.is_some_and(|value| value >= self.cfg.source_wall_ratio),
                        })
                    })
                    .collect::<Vec<_>>();
                json!({"exchange":state.book.exchange.as_str(), "bins":bins})
            })
            .collect::<Vec<_>>();
        let aggregate_metrics = (0..n_bins)
            .map(|distance_bin| {
                let (bid, ask) = covered.iter().fold((0.0, 0.0), |(bid, ask), state| {
                    let (local_bid, local_ask) = Self::distance_bin_notional(
                        &state.book,
                        selected_band,
                        distance_bin,
                        self.cfg.zone_bin_pct,
                    );
                    (bid + local_bid, ask + local_ask)
                });
                let (side, ratio, wall_usd) = if bid >= ask && ask > 0.0 {
                    (Some(Side::Buy), Some(bid / ask), bid)
                } else if ask > bid && bid > 0.0 {
                    (Some(Side::Sell), Some(ask / bid), ask)
                } else {
                    (None, None, bid.max(ask))
                };
                let (grade, grade_rank) =
                    ratio.map(|value| self.grade(value)).unwrap_or(("none", 0));
                (
                    distance_bin,
                    side,
                    ratio,
                    bid,
                    ask,
                    wall_usd,
                    grade,
                    grade_rank,
                )
            })
            .collect::<Vec<_>>();
        let aggregate_bins = aggregate_metrics
            .iter()
            .map(|(distance_bin, side, ratio, bid, ask, wall, grade, _)| json!({
                "distance_bin":distance_bin,
                "from_pct":*distance_bin as f64 * self.cfg.zone_bin_pct,
                "to_pct":((*distance_bin + 1) as f64 * self.cfg.zone_bin_pct).min(selected_band),
                "bid_usd":bid, "ask_usd":ask, "side":side.map(side_name),
                "ratio":ratio, "wall_usd":wall, "grade":grade,
                "meets_depth":*wall >= self.cfg.min_zone_depth_usd,
            }))
            .collect::<Vec<_>>();
        self.latest_local_bins = json!({
            "ready":true,
            "selected_band_pct":selected_band,
            "bin_pct":self.cfg.zone_bin_pct,
            "reference_mid":reference_mid,
            "required_sources":self.cfg.min_book_sources,
            "source_wall_ratio":self.cfg.source_wall_ratio,
            "min_zone_depth_usd":self.cfg.min_zone_depth_usd,
            "aggregate":aggregate_bins,
            "sources":source_bins,
        });
        let candidate = aggregate_metrics
            .into_iter()
            .filter_map(
                |(distance_bin, side, ratio, bid, ask, wall_usd, grade, grade_rank)| {
                    let (Some(side), Some(ratio)) = (side, ratio) else {
                        return None;
                    };
                    (grade_rank > 0 && wall_usd >= self.cfg.min_zone_depth_usd).then_some((
                        distance_bin,
                        side,
                        ratio,
                        bid,
                        ask,
                        wall_usd,
                        grade,
                        grade_rank,
                    ))
                },
            )
            .max_by(|a, b| {
                a.7.cmp(&b.7)
                    .then_with(|| a.2.total_cmp(&b.2))
                    .then_with(|| a.5.total_cmp(&b.5))
            });
        let Some(candidate) = candidate else {
            // 区域中断必须清空持续性计时，不能让消失后重新出现的墙继承旧时长。
            self.zone_tracker = None;
            return None;
        };
        let (distance_bin, side, ratio, bid_usd, ask_usd, wall_usd, grade, grade_rank) = candidate;
        let lower_distance = distance_bin as f64 * self.cfg.zone_bin_pct;
        let upper_distance = ((distance_bin + 1) as f64 * self.cfg.zone_bin_pct).min(selected_band);
        let (zone_low, zone_high) = match side {
            Side::Buy => (
                reference_mid * (1.0 - upper_distance),
                reference_mid * (1.0 - lower_distance),
            ),
            Side::Sell => (
                reference_mid * (1.0 + lower_distance),
                reference_mid * (1.0 + upper_distance),
            ),
        };
        let center = (zone_low + zone_high) / 2.0;
        let same_zone = self.zone_tracker.as_ref().is_some_and(|z| {
            let tolerance = (self.cfg.zone_bin_pct * 1.5).max(0.0005);
            z.side == side && z.center > 0.0 && (center / z.center - 1.0).abs() <= tolerance
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
        .filter_map(|ex| self.one_exchange_bin_ratio(ex, selected_band, distance_bin, now_ms))
        .filter(|x| x.0 == side)
        .collect::<Vec<_>>();
        let perp_metrics = [
            Exchange::BinanceFutures,
            Exchange::BybitFutures,
            Exchange::OkxFutures,
        ]
        .into_iter()
        .filter_map(|ex| self.one_exchange_bin_ratio(ex, selected_band, distance_bin, now_ms))
        .filter(|x| x.0 == side)
        .collect::<Vec<_>>();
        let spot_source_count = spot_metrics.len();
        let perp_source_count = perp_metrics.len();
        let confluence = spot_source_count >= self.cfg.min_spot_sources
            && perp_source_count >= self.cfg.min_perp_sources;
        let average_ratio = |xs: &[(Side, f64)]| {
            (!xs.is_empty()).then(|| xs.iter().map(|x| x.1).sum::<f64>() / xs.len() as f64)
        };
        Some(BookZone {
            ts_ms: now_ms,
            side,
            grade,
            grade_rank,
            band_pct: selected_band,
            ratio,
            bid_usd,
            ask_usd,
            zone_low,
            zone_high,
            zone_center: center,
            distance_pct: center / reference_mid - 1.0,
            wall_usd,
            distance_bin,
            source_wall_ratio: self.cfg.source_wall_ratio,
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
        let keep_ms = self
            .cfg
            .delta_window_ms
            .max(self.cfg.trend_window_ms)
            .max(self.cfg.trend_slow_window_ms)
            .max(self.cfg.trend_anchor_window_ms)
            + self.cfg.bucket_ms;
        self.flows.retain(|start, _| now_ms - *start <= keep_ms);
    }

    /// Price regime uses a single executable reference market, sampled to one-minute
    /// closes.  Mixing the last arriving trade from six venues makes the path length
    /// mostly measure venue basis/arrival jitter and incorrectly labels trends as range.
    fn reference_trend_stats<'a>(
        buckets: impl Iterator<Item = (&'a i64, &'a FlowBucket)>,
    ) -> (f64, f64) {
        let mut minutes = BTreeMap::<i64, (f64, f64)>::new();
        for (ts, bucket) in buckets.filter(|(_, bucket)| bucket.reference_open > 0.0) {
            let minute = ts.div_euclid(60_000) * 60_000;
            let entry = minutes
                .entry(minute)
                .or_insert((bucket.reference_open, bucket.reference_close));
            entry.1 = bucket.reference_close;
        }
        let mut values = minutes.into_values();
        let Some((first, first_close)) = values.next() else {
            return (0.0, 0.0);
        };
        let mut last = first_close;
        let mut previous = first_close;
        let mut path = (first_close - first).abs();
        for (_, close) in values {
            path += (close - previous).abs();
            previous = close;
            last = close;
        }
        let trend_return = if first > 0.0 { last / first - 1.0 } else { 0.0 };
        let efficiency = if path > 0.0 {
            (last - first).abs() / path
        } else {
            0.0
        };
        (trend_return, efficiency)
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
        let trend_slow_from = now_ms - self.cfg.trend_slow_window_ms;
        let trend_anchor_from = now_ms - self.cfg.trend_anchor_window_ms;
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
        let (trend_return, efficiency) =
            Self::reference_trend_stats(self.flows.range(trend_from..));
        let (slow_return, slow_efficiency) =
            Self::reference_trend_stats(self.flows.range(trend_slow_from..));
        let (anchor_return, anchor_efficiency) =
            Self::reference_trend_stats(self.flows.range(trend_anchor_from..));
        let aligned = trend_return.signum() == delta.signum();
        let fast_up = aligned
            && trend_return >= self.cfg.trend_return_pct
            && efficiency >= self.cfg.trend_efficiency;
        let fast_down = aligned
            && trend_return <= -self.cfg.trend_return_pct
            && efficiency >= self.cfg.trend_efficiency;
        // 慢趋势只看价格本身。上涨中的卖方 Delta 往往代表回调或吸收，不能因此把
        // 两小时上涨误标成 range，再允许均值回归模型逆势摸顶。
        let slow_up = slow_return >= self.cfg.trend_slow_return_pct
            && slow_efficiency >= self.cfg.trend_slow_efficiency;
        let slow_down = slow_return <= -self.cfg.trend_slow_return_pct
            && slow_efficiency >= self.cfg.trend_slow_efficiency;
        let anchor_up = anchor_return >= self.cfg.trend_anchor_return_pct
            && anchor_efficiency >= self.cfg.trend_anchor_efficiency;
        let anchor_down = anchor_return <= -self.cfg.trend_anchor_return_pct
            && anchor_efficiency >= self.cfg.trend_anchor_efficiency;
        let directional_conflict = slow_up && anchor_down || slow_down && anchor_up;
        let regime = if directional_conflict {
            "transition"
        } else if slow_up || anchor_up {
            "trend_up"
        } else if slow_down || anchor_down {
            "trend_down"
        } else if fast_up {
            "trend_up"
        } else if fast_down {
            "trend_down"
        } else {
            "range"
        };
        // MR is a range sleeve.  A trend on either horizon, or disagreement between
        // horizons, delegates the account to continuation/pullback instead of fading it.
        let mr_allowed = !(slow_up || slow_down || anchor_up || anchor_down);
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
            trend_slow_return_pct: slow_return,
            trend_slow_efficiency: slow_efficiency,
            trend_slow_window_ms: self.cfg.trend_slow_window_ms,
            trend_anchor_return_pct: anchor_return,
            trend_anchor_efficiency: anchor_efficiency,
            trend_anchor_window_ms: self.cfg.trend_anchor_window_ms,
            mr_allowed,
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
                "distance_bin":z.distance_bin, "ratio_basis":"local_mirrored_bin",
                "source_wall_ratio":z.source_wall_ratio,
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
                "trend_efficiency":f.trend_efficiency,
                "trend_slow_return_pct":f.trend_slow_return_pct,
                "trend_slow_efficiency":f.trend_slow_efficiency,
                "trend_slow_window_ms":f.trend_slow_window_ms,
                "trend_anchor_return_pct":f.trend_anchor_return_pct,
                "trend_anchor_efficiency":f.trend_anchor_efficiency,
                "trend_anchor_window_ms":f.trend_anchor_window_ms,
                "mr_allowed":f.mr_allowed
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
        zone["local_bins"] = self.latest_local_bins.clone();
        let mut flow = Self::flow_payload(self.latest_flow.as_ref());
        if let Some(obj) = flow.as_object_mut() {
            obj.insert("window_ms".into(), json!(self.cfg.delta_window_ms));
            obj.insert(
                "trend_slow_window_ms".into(),
                json!(self.cfg.trend_slow_window_ms),
            );
            obj.insert(
                "trend_anchor_window_ms".into(),
                json!(self.cfg.trend_anchor_window_ms),
            );
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
        let mut payload = Self::zone_payload(self.latest_zone.as_ref());
        payload["local_bins"] = self.latest_local_bins.clone();
        Signal::new(SignalKind::ObiZone, ts, "TrdrMarketMap", payload)
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
                    "return_pct":f.trend_return_pct,"efficiency":f.trend_efficiency,
                    "slow_return_pct":f.trend_slow_return_pct,
                    "slow_efficiency":f.trend_slow_efficiency,
                    "slow_window_ms":self.cfg.trend_slow_window_ms,
                    "anchor_return_pct":f.trend_anchor_return_pct,
                    "anchor_efficiency":f.trend_anchor_efficiency,
                    "anchor_window_ms":self.cfg.trend_anchor_window_ms,
                    "mr_allowed":f.mr_allowed
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

    fn locally_imbalanced_book(exchange: Exchange, ts_ms: i64) -> BookSnapshot {
        BookSnapshot {
            ts: Timestamp::from_millis(ts_ms),
            exchange,
            symbol: Symbol::new("BTCUSDT"),
            // 整片盘口接近平衡，但最靠近现价的 0.1% 价格簇存在明显买墙。
            bids: vec![
                (Price::from_f64(99.95), Qty::from_f64(7.0)),
                (Price::from_f64(99.80), Qty::from_f64(1.0)),
            ],
            asks: vec![
                (Price::from_f64(100.05), Qty::from_f64(1.0)),
                (Price::from_f64(100.20), Qty::from_f64(6.0)),
            ],
        }
    }

    fn locally_balanced_book(exchange: Exchange, ts_ms: i64) -> BookSnapshot {
        BookSnapshot {
            ts: Timestamp::from_millis(ts_ms),
            exchange,
            symbol: Symbol::new("BTCUSDT"),
            bids: vec![
                (Price::from_f64(99.95), Qty::from_f64(1.0)),
                (Price::from_f64(99.80), Qty::from_f64(1.0)),
            ],
            asks: vec![
                (Price::from_f64(100.05), Qty::from_f64(1.0)),
                (Price::from_f64(100.20), Qty::from_f64(1.0)),
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
    fn local_wall_survives_whole_band_dilution() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.book_bands = vec![0.002];
        cfg.min_zone_depth_usd = 1.0;
        cfg.min_book_sources = 2;
        cfg.min_spot_sources = 1;
        cfg.min_perp_sources = 1;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        let futures = locally_imbalanced_book(Exchange::BinanceFutures, 1_000);
        let spot = locally_imbalanced_book(Exchange::BinanceSpot, 2_000);
        let broad_bid = futures.bid_qty_within(0.002) + spot.bid_qty_within(0.002);
        let broad_ask = futures.ask_qty_within(0.002) + spot.ask_qty_within(0.002);
        assert!(broad_bid / broad_ask < 1.8);
        map.on_event(&Event::Book(futures), &ctx);
        let out = map.on_event(&Event::Book(spot), &ctx);
        assert_eq!(out[0].payload["active"], true);
        assert_eq!(out[0].payload["side"], "buy");
        assert_eq!(out[0].payload["grade"], "blue");
        assert_eq!(out[0].payload["ratio_basis"], "local_mirrored_bin");
        assert_eq!(out[0].payload["spot_perp_confluence"], true);
    }

    #[test]
    fn disappearing_wall_resets_persistence() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.book_bands = vec![0.002];
        cfg.min_zone_depth_usd = 1.0;
        cfg.min_book_sources = 2;
        cfg.min_spot_sources = 1;
        cfg.min_perp_sources = 1;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        map.on_event(
            &Event::Book(locally_imbalanced_book(Exchange::BinanceFutures, 1_000)),
            &ctx,
        );
        map.on_event(
            &Event::Book(locally_imbalanced_book(Exchange::BinanceSpot, 2_000)),
            &ctx,
        );
        assert!(map.latest_zone.is_some());
        map.on_event(
            &Event::Book(locally_balanced_book(Exchange::BinanceFutures, 10_000)),
            &ctx,
        );
        map.on_event(
            &Event::Book(locally_balanced_book(Exchange::BinanceSpot, 11_000)),
            &ctx,
        );
        assert!(map.latest_zone.is_none());
        assert!(map.zone_tracker.is_none());
        map.on_event(
            &Event::Book(locally_imbalanced_book(Exchange::BinanceFutures, 20_000)),
            &ctx,
        );
        let out = map.on_event(
            &Event::Book(locally_imbalanced_book(Exchange::BinanceSpot, 21_000)),
            &ctx,
        );
        assert!(out[0].payload["persistence_ms"].as_i64().unwrap() <= 1_000);
    }

    #[test]
    fn logs_all_local_bins_even_without_an_active_zone() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.book_bands = vec![0.002];
        cfg.min_zone_depth_usd = 1_000_000_000.0; // 故意让候选无法成为有效色带
        cfg.min_book_sources = 2;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        map.on_event(
            &Event::Book(locally_balanced_book(Exchange::BinanceFutures, 1_000)),
            &ctx,
        );
        let out = map.on_event(
            &Event::Book(locally_balanced_book(Exchange::BinanceSpot, 2_000)),
            &ctx,
        );
        let zone = &out[0].payload;
        assert_eq!(zone["active"], false);
        assert_eq!(zone["local_bins"]["ready"], true);
        assert_eq!(zone["local_bins"]["sources"].as_array().unwrap().len(), 2);
        assert_eq!(zone["local_bins"]["aggregate"].as_array().unwrap().len(), 2);
        assert!(zone["local_bins"]["aggregate"][0]["bid_usd"].is_number());
        assert!(zone["local_bins"]["sources"][0]["bins"][0]["ask_usd"].is_number());
    }

    #[test]
    fn emits_delta_and_regime_signals_on_bucket_change() {
        let mut cfg = Config::from_params(&json!({"delta_share_levels":[0.01,0.02,0.03,0.04]}));
        cfg.trend_return_pct = 0.005;
        cfg.trend_efficiency = 0.2;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        let mut last = vec![];
        for (ts, px) in [(1_000, 100.0), (61_000, 101.0), (121_000, 102.0)] {
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
    fn slow_price_trend_is_not_erased_by_opposite_delta() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.trend_return_pct = 0.10; // disable the fast detector in this fixture
        cfg.trend_slow_return_pct = 0.005;
        cfg.trend_slow_efficiency = 0.10;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        for (ts, px) in [(1_000, 100.0), (61_000, 100.4), (121_000, 100.8)] {
            let trade = Trade {
                ts: Timestamp::from_millis(ts),
                exchange: Exchange::BinanceFutures,
                symbol: Symbol::new("BTCUSDT"),
                price: Price::from_f64(px),
                qty: Qty::from_f64(100.0),
                is_buyer_maker: true, // aggressive sells: Delta points down while price rises
            };
            map.on_event(&Event::Trade(trade), &ctx);
        }
        let flow = map.latest_flow.as_ref().unwrap();
        assert!(flow.delta_usd < 0.0);
        assert_eq!(flow.regime, "trend_up");
        assert!(flow.trend_slow_return_pct >= 0.005);
        assert!(!flow.mr_allowed);
    }

    #[test]
    fn venue_basis_noise_does_not_erase_reference_trend() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.trend_return_pct = 0.10;
        cfg.trend_slow_return_pct = 0.005;
        cfg.trend_slow_efficiency = 0.10;
        let mut map = TrdrMarketMap::new(cfg);
        let ctx = Ctx::default();
        for minute in 0..4 {
            let base_ts = minute * 60_000 + 1_000;
            let reference = 100.0 + minute as f64 * 0.4;
            for (offset, exchange, price) in [
                (0, Exchange::BinanceFutures, reference),
                (10_000, Exchange::BybitFutures, reference * 1.003),
                (20_000, Exchange::OkxFutures, reference * 0.997),
                (30_000, Exchange::BinanceSpot, reference * 1.002),
            ] {
                map.on_event(
                    &Event::Trade(Trade {
                        ts: Timestamp::from_millis(base_ts + offset),
                        exchange,
                        symbol: Symbol::new("BTCUSDT"),
                        price: Price::from_f64(price),
                        qty: Qty::from_f64(1.0),
                        is_buyer_maker: false,
                    }),
                    &ctx,
                );
            }
        }
        let flow = map.latest_flow.as_ref().unwrap();
        assert_eq!(flow.regime, "trend_up");
        assert!(flow.trend_slow_efficiency > 0.9);
        assert!(!flow.mr_allowed);
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
