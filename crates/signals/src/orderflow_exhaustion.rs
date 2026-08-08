use std::collections::VecDeque;

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Exchange, Side, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket_ms: i64,
    pub baseline_buckets: usize,
    pub location_buckets: usize,
    pub min_baseline_buckets: usize,
    pub classic_volume_ratio: f64,
    pub strong_volume_ratio: f64,
    pub min_delta_share: f64,
    pub strong_delta_share: f64,
    pub confirm_delta_share: f64,
    pub max_efficiency: f64,
    pub min_sweep_pct: f64,
    pub min_vwap_deviation_pct: f64,
    pub strong_confirm_buckets: usize,
    pub weak_confirm_buckets: usize,
    pub cluster_cooldown_buckets: usize,
    pub cooldown_rearm_volume_ratio: f64,
    pub cooldown_rearm_price_pct: f64,
    pub stop_buffer_pct: f64,
    pub execution_min_context_score: u8,
    pub execution_require_source_coverage: bool,
    pub execution_require_footprint: bool,
    pub allow_local_location: bool,
    pub execution_require_absorption: bool,
    pub execution_require_location_in_range: bool,
    pub trend_aligned_context_discount: u8,
    pub block_countertrend: bool,
    pub min_trdr_grade_rank: u8,
    pub min_trdr_delta_tier: u8,
    pub max_zone_distance_pct: f64,
    pub trdr_max_age_ms: i64,
    pub min_zone_persistence_ms: i64,
    pub require_spot_perp_confluence: bool,
    pub require_source_coverage: bool,
    pub min_stacked_imbalance: usize,
    pub min_liquidation_usd: f64,
    /// 渐进吸收观察窗口；只产出 shadow observation，永不直接下单。
    pub cumulative_window_buckets: usize,
    pub cumulative_min_delta_share: f64,
    pub cumulative_max_return_pct: f64,
    pub cumulative_max_efficiency: f64,
    pub cumulative_cooldown_buckets: usize,
}

impl Config {
    pub fn from_params(p: &Json) -> Self {
        let f = |key: &str, default: f64| p.get(key).and_then(Json::as_f64).unwrap_or(default);
        let u = |key: &str, default: usize| {
            p.get(key)
                .and_then(Json::as_u64)
                .map(|v| v as usize)
                .unwrap_or(default)
        };
        let b = |key: &str, default: bool| p.get(key).and_then(Json::as_bool).unwrap_or(default);
        Self {
            bucket_ms: p
                .get("bucket_ms")
                .and_then(Json::as_i64)
                .unwrap_or(10_000)
                .max(1_000),
            baseline_buckets: u("baseline_buckets", 360).max(30),
            location_buckets: u("location_buckets", 540).max(30),
            min_baseline_buckets: u("min_baseline_buckets", 120).max(20),
            classic_volume_ratio: f("classic_volume_ratio", 1.60).max(1.0),
            strong_volume_ratio: f("strong_volume_ratio", 2.20).max(1.0),
            min_delta_share: f("min_delta_share", 0.15).clamp(0.01, 0.95),
            strong_delta_share: f("strong_delta_share", 0.35).clamp(0.01, 0.95),
            confirm_delta_share: f("confirm_delta_share", 0.08).clamp(0.01, 0.95),
            max_efficiency: f("max_efficiency", 0.45).clamp(0.01, 1.0),
            min_sweep_pct: f("min_sweep_pct", 0.00035).max(0.0),
            min_vwap_deviation_pct: f("min_vwap_deviation_pct", 0.0030).max(0.0),
            strong_confirm_buckets: u("strong_confirm_buckets", 18).max(1),
            weak_confirm_buckets: u("weak_confirm_buckets", 30).max(1),
            cluster_cooldown_buckets: u("cluster_cooldown_buckets", 60).max(1),
            cooldown_rearm_volume_ratio: f("cooldown_rearm_volume_ratio", 50.0).max(2.0),
            cooldown_rearm_price_pct: f("cooldown_rearm_price_pct", 0.005).clamp(0.001, 0.05),
            stop_buffer_pct: f("stop_buffer_pct", 0.00035).max(0.0),
            execution_min_context_score: u("execution_min_context_score", 5).min(8) as u8,
            execution_require_source_coverage: b("execution_require_source_coverage", true),
            execution_require_footprint: b("execution_require_footprint", true),
            allow_local_location: b("allow_local_location", true),
            execution_require_absorption: b("execution_require_absorption", true),
            execution_require_location_in_range: b("execution_require_location_in_range", true),
            trend_aligned_context_discount: u("trend_aligned_context_discount", 1).min(4) as u8,
            block_countertrend: b("block_countertrend", true),
            min_trdr_grade_rank: u("min_trdr_grade_rank", 1).clamp(1, 4) as u8,
            min_trdr_delta_tier: u("min_trdr_delta_tier", 1).min(4) as u8,
            max_zone_distance_pct: f("max_zone_distance_pct", 0.004).clamp(0.0005, 0.05),
            trdr_max_age_ms: p
                .get("trdr_max_age_ms")
                .and_then(Json::as_i64)
                .unwrap_or(30_000)
                .max(1_000),
            min_zone_persistence_ms: p
                .get("min_zone_persistence_ms")
                .and_then(Json::as_i64)
                .unwrap_or(30_000)
                .max(0),
            require_spot_perp_confluence: b("require_spot_perp_confluence", true),
            require_source_coverage: b("require_source_coverage", true),
            min_stacked_imbalance: u("min_stacked_imbalance", 2).clamp(1, 10),
            min_liquidation_usd: f("min_liquidation_usd", 1_000_000.0).max(0.0),
            cumulative_window_buckets: u("cumulative_window_buckets", 180).max(6),
            cumulative_min_delta_share: f("cumulative_min_delta_share", 0.15).clamp(0.01, 0.95),
            cumulative_max_return_pct: f("cumulative_max_return_pct", 0.006).clamp(0.0001, 0.10),
            cumulative_max_efficiency: f("cumulative_max_efficiency", 0.45).clamp(0.01, 1.0),
            cumulative_cooldown_buckets: u("cumulative_cooldown_buckets", 180).max(1),
        }
    }
}

#[derive(Debug, Clone)]
struct Bucket {
    start_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    delta: f64,
}

impl Bucket {
    fn new(start_ms: i64, price: f64) -> Self {
        Self {
            start_ms,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0.0,
            delta: 0.0,
        }
    }

    fn add(&mut self, price: f64, notional: f64, signed_notional: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += notional;
        self.delta += signed_notional;
    }
}

#[derive(Debug, Clone)]
struct Setup {
    event_id: i64,
    side: Side,
    strength: &'static str,
    event_price: f64,
    entry_price: f64,
    stop_anchor: f64,
    event_volume: f64,
    event_volume_ratio: f64,
    event_delta_share: f64,
    event_efficiency: f64,
    location_confirmed: bool,
    absorption_confirmed: bool,
    trend_aligned: bool,
    context_score: u8,
    context_reasons: Vec<String>,
    context_fused: bool,
    trdr_zone_grade: String,
    trdr_zone_band_pct: Option<f64>,
    trdr_zone_ratio: Option<f64>,
    trdr_zone_wall_usd: Option<f64>,
    trdr_zone_distance_bin: Option<u64>,
    trdr_source_wall_ratio: Option<f64>,
    trdr_zone_distance_pct: Option<f64>,
    trdr_zone_low: Option<f64>,
    trdr_zone_high: Option<f64>,
    trdr_spot_perp_confluence: bool,
    trdr_delta_tier: u8,
    trdr_delta_usd: Option<f64>,
    trdr_delta_share: Option<f64>,
    trdr_spot_volume_usd: Option<f64>,
    trdr_perp_volume_usd: Option<f64>,
    trdr_source_stats: Vec<Json>,
    trdr_liquidation_stats: Vec<Json>,
    trdr_oi_quadrant: String,
    trdr_regime: String,
    trend_blocked: bool,
    production_ready: bool,
    trdr_zone_persistence_ms: i64,
    trdr_source_coverage_complete: bool,
    trdr_footprint_matches: bool,
    trdr_footprint_price: Option<f64>,
    trdr_footprint_delta_usd: Option<f64>,
    trdr_stacked_imbalance: usize,
    trdr_long_liquidation_usd: f64,
    trdr_short_liquidation_usd: f64,
    expires_at: i64,
}

#[derive(Debug, Clone)]
struct TrdrContext {
    zone_matches: bool,
    zone_grade: String,
    zone_band_pct: Option<f64>,
    zone_ratio: Option<f64>,
    zone_wall_usd: Option<f64>,
    zone_distance_bin: Option<u64>,
    source_wall_ratio: Option<f64>,
    zone_distance_pct: Option<f64>,
    zone_low: Option<f64>,
    zone_high: Option<f64>,
    spot_perp_confluence: bool,
    delta_matches: bool,
    delta_tier: u8,
    delta_usd: Option<f64>,
    delta_share: Option<f64>,
    spot_volume_usd: Option<f64>,
    perp_volume_usd: Option<f64>,
    source_stats: Vec<Json>,
    liquidation_stats: Vec<Json>,
    oi_quadrant: String,
    regime: String,
    trend_blocked: bool,
    production_ready: bool,
    zone_persistence_ms: i64,
    source_coverage_complete: bool,
    footprint_matches: bool,
    footprint_price: Option<f64>,
    footprint_delta_usd: Option<f64>,
    stacked_imbalance: usize,
    long_liquidation_usd: f64,
    short_liquidation_usd: f64,
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn side_slot(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn add_reason(reasons: &mut Vec<String>, reason: &str) -> bool {
    if reasons.iter().any(|value| value == reason) {
        false
    } else {
        reasons.push(reason.to_string());
        true
    }
}

fn evidence_score(reasons: &[String]) -> u8 {
    let has = |reason: &str| reasons.iter().any(|value| value == reason);
    let location = has("trdr_zone") || has("local_location");
    [
        location,
        has("absorption"),
        has("trdr_delta"),
        has("footprint_cluster"),
        has("oi"),
        has("spot_perp_confluence"),
        has("liquidation"),
    ]
    .into_iter()
    .filter(|value| *value)
    .count() as u8
}

fn fuse_setup_context(
    setup: &mut Setup,
    trdr: &TrdrContext,
    oi_support: bool,
    cfg: &Config,
    now_ms: i64,
) {
    let mut added = false;
    if trdr.zone_matches {
        setup.location_confirmed = true;
        added |= add_reason(&mut setup.context_reasons, "trdr_zone");
        if let (Some(low), Some(high)) = (trdr.zone_low, trdr.zone_high) {
            setup.entry_price = (low + high) / 2.0;
            setup.stop_anchor = match setup.side {
                Side::Buy => setup.stop_anchor.min(low * (1.0 - cfg.stop_buffer_pct)),
                Side::Sell => setup.stop_anchor.max(high * (1.0 + cfg.stop_buffer_pct)),
            };
        }
        setup.trdr_zone_grade = trdr.zone_grade.clone();
        setup.trdr_zone_band_pct = trdr.zone_band_pct;
        setup.trdr_zone_ratio = trdr.zone_ratio;
        setup.trdr_zone_wall_usd = trdr.zone_wall_usd;
        setup.trdr_zone_distance_bin = trdr.zone_distance_bin;
        setup.trdr_source_wall_ratio = trdr.source_wall_ratio;
        setup.trdr_zone_distance_pct = trdr.zone_distance_pct;
        setup.trdr_zone_low = trdr.zone_low;
        setup.trdr_zone_high = trdr.zone_high;
        setup.trdr_zone_persistence_ms = trdr.zone_persistence_ms;
    }
    if trdr.delta_matches {
        added |= add_reason(&mut setup.context_reasons, "trdr_delta");
    }
    if trdr.footprint_matches {
        added |= add_reason(&mut setup.context_reasons, "footprint_cluster");
    }
    if oi_support {
        added |= add_reason(&mut setup.context_reasons, "oi");
    }
    if trdr.spot_perp_confluence {
        added |= add_reason(&mut setup.context_reasons, "spot_perp_confluence");
    }
    let liquidation_support = match setup.side {
        Side::Buy => trdr.long_liquidation_usd >= cfg.min_liquidation_usd,
        Side::Sell => trdr.short_liquidation_usd >= cfg.min_liquidation_usd,
    };
    if liquidation_support {
        added |= add_reason(&mut setup.context_reasons, "liquidation");
    }
    setup.context_fused |= added && now_ms > setup.event_id;
    setup.context_score = evidence_score(&setup.context_reasons);
    setup.trdr_spot_perp_confluence |= trdr.spot_perp_confluence;
    setup.trdr_delta_tier = setup.trdr_delta_tier.max(trdr.delta_tier);
    if trdr.delta_matches {
        setup.trdr_delta_usd = trdr.delta_usd;
        setup.trdr_delta_share = trdr.delta_share;
    }
    setup.trdr_spot_volume_usd = trdr.spot_volume_usd.or(setup.trdr_spot_volume_usd);
    setup.trdr_perp_volume_usd = trdr.perp_volume_usd.or(setup.trdr_perp_volume_usd);
    if !trdr.source_stats.is_empty() {
        setup.trdr_source_stats = trdr.source_stats.clone();
    }
    if !trdr.liquidation_stats.is_empty() {
        setup.trdr_liquidation_stats = trdr.liquidation_stats.clone();
    }
    if trdr.oi_quadrant != "neutral" {
        setup.trdr_oi_quadrant = trdr.oi_quadrant.clone();
    }
    setup.trdr_source_coverage_complete |= trdr.source_coverage_complete;
    setup.trdr_footprint_matches |= trdr.footprint_matches;
    if trdr.footprint_matches {
        setup.trdr_footprint_price = trdr.footprint_price;
        setup.trdr_footprint_delta_usd = trdr.footprint_delta_usd;
        setup.trdr_stacked_imbalance = setup.trdr_stacked_imbalance.max(trdr.stacked_imbalance);
    }
    setup.trdr_long_liquidation_usd = setup
        .trdr_long_liquidation_usd
        .max(trdr.long_liquidation_usd);
    setup.trdr_short_liquidation_usd = setup
        .trdr_short_liquidation_usd
        .max(trdr.short_liquidation_usd);
    setup.production_ready = setup.location_confirmed
        && setup
            .context_reasons
            .iter()
            .any(|value| value == "trdr_delta")
        && setup.trdr_footprint_matches
        && (!cfg.require_source_coverage || setup.trdr_source_coverage_complete);
}

fn trdr_context(ctx: &Ctx, side: Side, cfg: &Config, now_ms: i64) -> TrdrContext {
    let zone = ctx.latest_of(SignalKind::ObiZone).map(|s| &s.payload);
    let zone_fresh = zone
        .and_then(|z| z.get("ts_ms").and_then(Json::as_i64))
        .is_some_and(|ts| now_ms - ts <= cfg.trdr_max_age_ms);
    let zone_grade_rank = zone
        .and_then(|z| z.get("grade_rank").and_then(Json::as_u64))
        .unwrap_or(0) as u8;
    let zone_distance = zone.and_then(|z| z.get("distance_pct").and_then(Json::as_f64));
    let zone_persistence_ms = zone
        .and_then(|z| z.get("persistence_ms").and_then(Json::as_i64))
        .unwrap_or(0);
    let spot_perp_confluence = zone
        .and_then(|z| z.get("spot_perp_confluence").and_then(Json::as_bool))
        .unwrap_or(false);
    let zone_matches = zone_fresh
        && zone.and_then(|z| z.get("active").and_then(Json::as_bool)) == Some(true)
        && zone.and_then(|z| z.get("side").and_then(Json::as_str)) == Some(side_name(side))
        && zone_grade_rank >= cfg.min_trdr_grade_rank
        && zone_distance.is_some_and(|v| v.abs() <= cfg.max_zone_distance_pct)
        && zone_persistence_ms >= cfg.min_zone_persistence_ms
        && (!cfg.require_spot_perp_confluence || spot_perp_confluence);

    let delta = ctx.latest_of(SignalKind::DeltaTier).map(|s| &s.payload);
    let expected_pressure = side_name(side.opposite());
    let delta_tier = delta
        .and_then(|d| d.get("tier").and_then(Json::as_u64))
        .unwrap_or(0) as u8;
    let delta_fresh = delta
        .and_then(|d| d.get("ts_ms").and_then(Json::as_i64))
        .is_some_and(|ts| now_ms - ts <= cfg.trdr_max_age_ms.max(cfg.bucket_ms * 2));
    let delta_matches = delta_fresh
        && delta_tier >= cfg.min_trdr_delta_tier
        && delta.and_then(|d| d.get("direction").and_then(Json::as_str)) == Some(expected_pressure);
    let source_coverage_complete = delta
        .and_then(|d| d.get("source_coverage_complete").and_then(Json::as_bool))
        .unwrap_or(false);
    let footprint_direction =
        delta.and_then(|d| d.get("footprint_direction").and_then(Json::as_str));
    let stacked_imbalance = delta
        .and_then(|d| d.get("stacked_imbalance").and_then(Json::as_u64))
        .unwrap_or(0) as usize;
    let footprint_matches = delta_fresh
        && footprint_direction == Some(expected_pressure)
        && stacked_imbalance >= cfg.min_stacked_imbalance;
    let production_ready = zone_matches
        && delta_matches
        && footprint_matches
        && (!cfg.require_source_coverage || source_coverage_complete);

    let oi = ctx.latest_of(SignalKind::OiQuadrant).map(|s| &s.payload);
    let regime = ctx
        .latest_of(SignalKind::TrendRegime)
        .and_then(|s| s.payload.get("regime").and_then(Json::as_str))
        .unwrap_or("warming")
        .to_string();
    let blocked_side = ctx
        .latest_of(SignalKind::TrendRegime)
        .and_then(|s| s.payload.get("blocked_side").and_then(Json::as_str));

    TrdrContext {
        zone_matches,
        zone_grade: zone
            .and_then(|z| z.get("grade").and_then(Json::as_str))
            .unwrap_or("none")
            .to_string(),
        zone_band_pct: zone.and_then(|z| z.get("band_pct").and_then(Json::as_f64)),
        zone_ratio: zone.and_then(|z| z.get("ratio").and_then(Json::as_f64)),
        zone_wall_usd: zone.and_then(|z| z.get("wall_usd").and_then(Json::as_f64)),
        zone_distance_bin: zone.and_then(|z| z.get("distance_bin").and_then(Json::as_u64)),
        source_wall_ratio: zone.and_then(|z| z.get("source_wall_ratio").and_then(Json::as_f64)),
        zone_distance_pct: zone_distance,
        zone_low: zone.and_then(|z| z.get("zone_low").and_then(Json::as_f64)),
        zone_high: zone.and_then(|z| z.get("zone_high").and_then(Json::as_f64)),
        spot_perp_confluence,
        delta_matches,
        delta_tier,
        delta_usd: delta.and_then(|d| d.get("delta_usd").and_then(Json::as_f64)),
        delta_share: delta.and_then(|d| d.get("delta_share").and_then(Json::as_f64)),
        spot_volume_usd: delta.and_then(|d| d.get("spot_volume_usd").and_then(Json::as_f64)),
        perp_volume_usd: delta.and_then(|d| d.get("perp_volume_usd").and_then(Json::as_f64)),
        source_stats: delta
            .and_then(|d| d.get("source_stats").and_then(Json::as_array))
            .cloned()
            .unwrap_or_default(),
        liquidation_stats: delta
            .and_then(|d| d.get("liquidation_stats").and_then(Json::as_array))
            .cloned()
            .unwrap_or_default(),
        oi_quadrant: oi
            .and_then(|o| o.get("quadrant").and_then(Json::as_str))
            .unwrap_or("neutral")
            .to_string(),
        regime,
        trend_blocked: cfg.block_countertrend && blocked_side == Some(side_name(side)),
        production_ready,
        zone_persistence_ms,
        source_coverage_complete,
        footprint_matches,
        footprint_price: delta.and_then(|d| d.get("footprint_price").and_then(Json::as_f64)),
        footprint_delta_usd: delta
            .and_then(|d| d.get("footprint_delta_usd").and_then(Json::as_f64)),
        stacked_imbalance,
        long_liquidation_usd: delta
            .and_then(|d| d.get("long_liquidation_usd").and_then(Json::as_f64))
            .unwrap_or(0.0),
        short_liquidation_usd: delta
            .and_then(|d| d.get("short_liquidation_usd").and_then(Json::as_f64))
            .unwrap_or(0.0),
    }
}

/// 10 秒放量事件 -> 3/5 分钟 Delta 反转 -> context 分层。
///
/// `classic/strength/high_frequency/balanced/quality` 是同一事件链的质量层级，
/// 不是五个会重复下单的策略。每次确认只输出最高通过层级，并用事件簇冷却去重。
pub struct OrderFlowExhaustion {
    cfg: Config,
    current: Option<Bucket>,
    history: VecDeque<Bucket>,
    setup: Option<Setup>,
    cooldown_until: [i64; 2],
    cooldown_price: [Option<f64>; 2],
    cumulative_cooldown_until: i64,
    session_day: i64,
    session_notional: f64,
    session_pv: f64,
    latest_oi: Option<f64>,
    previous_oi: Option<f64>,
    book_imbalance: Option<f64>,
    eval: Option<Json>,
}

impl OrderFlowExhaustion {
    pub fn from_params(p: &Json) -> Self {
        Self::new(Config::from_params(p))
    }

    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            current: None,
            history: VecDeque::new(),
            setup: None,
            cooldown_until: [0; 2],
            cooldown_price: [None; 2],
            cumulative_cooldown_until: 0,
            session_day: i64::MIN,
            session_notional: 0.0,
            session_pv: 0.0,
            latest_oi: None,
            previous_oi: None,
            book_imbalance: None,
            eval: None,
        }
    }

    fn rearm_triggered(&self, side: Side, price: f64, volume_ratio: f64) -> bool {
        let slot = side_slot(side);
        let new_price_region = self.cooldown_price[slot].is_some_and(|anchor| {
            anchor > 0.0 && (price / anchor - 1.0).abs() >= self.cfg.cooldown_rearm_price_pct
        });
        volume_ratio >= self.cfg.cooldown_rearm_volume_ratio || new_price_region
    }

    fn cooldown_blocks(&self, side: Side, now_ms: i64, price: f64, volume_ratio: f64) -> bool {
        let slot = side_slot(side);
        now_ms < self.cooldown_until[slot] && !self.rearm_triggered(side, price, volume_ratio)
    }

    fn median_volume(&self) -> f64 {
        let mut xs: Vec<f64> = self
            .history
            .iter()
            .rev()
            .take(self.cfg.baseline_buckets)
            .map(|b| b.volume)
            .filter(|v| *v > 0.0)
            .collect();
        if xs.is_empty() {
            return 0.0;
        }
        xs.sort_by(f64::total_cmp);
        xs[xs.len() / 2]
    }

    /// 在较长窗口中识别“Delta 持续单边，但价格推进有限且路径反复”的渐进吸收。
    /// 返回的特征每个 10 秒桶都会写入 eval；active 只代表值得跟踪，不参与交易。
    fn cumulative_absorption(&self, current: &Bucket) -> Json {
        let needed = self.cfg.cumulative_window_buckets;
        let mut window = self
            .history
            .iter()
            .rev()
            .take(needed.saturating_sub(1))
            .collect::<Vec<_>>();
        window.reverse();
        if window.len() + 1 < needed {
            return json!({
                "ready":false,
                "active":false,
                "observed_buckets":window.len() + 1,
                "required_buckets":needed,
                "window_ms":needed as i64 * self.cfg.bucket_ms,
                "reason":"渐进吸收观察窗口预热中",
            });
        }

        let open = window
            .first()
            .map(|bucket| bucket.open)
            .unwrap_or(current.open);
        let close = current.close;
        let mut volume = current.volume;
        let mut delta = current.delta;
        let mut path = 0.0;
        let mut previous_close = open;
        for bucket in &window {
            volume += bucket.volume;
            delta += bucket.delta;
            path += (bucket.close - previous_close).abs();
            previous_close = bucket.close;
        }
        path += (current.close - previous_close).abs();
        let delta_share = if volume > 0.0 { delta / volume } else { 0.0 };
        let return_pct = if open > 0.0 { close / open - 1.0 } else { 0.0 };
        let efficiency = if path > 0.0 {
            (close - open).abs() / path
        } else {
            0.0
        };
        let pressure = if delta_share >= self.cfg.cumulative_min_delta_share {
            Some(Side::Buy)
        } else if delta_share <= -self.cfg.cumulative_min_delta_share {
            Some(Side::Sell)
        } else {
            None
        };
        let fade_side = pressure.map(Side::opposite);
        let limited_return = return_pct.abs() <= self.cfg.cumulative_max_return_pct;
        let inefficient_path = efficiency <= self.cfg.cumulative_max_efficiency;
        let active = pressure.is_some() && limited_return && inefficient_path;
        let reason = if pressure.is_none() {
            "滚动 Delta 尚未达到观察门槛"
        } else if !limited_return {
            "价格已随 Delta 明显推进，不属于吸收"
        } else if !inefficient_path {
            "价格路径仍偏单边，不属于反复承接"
        } else {
            "持续单边 Delta 未换来等比例价格推进，记录渐进吸收观察事件"
        };
        json!({
            "ready":true,
            "active":active,
            "emitted":false,
            "window_buckets":needed,
            "window_ms":needed as i64 * self.cfg.bucket_ms,
            "open":open,
            "close":close,
            "volume_usd":volume,
            "delta_usd":delta,
            "delta_share":delta_share,
            "return_pct":return_pct,
            "efficiency":efficiency,
            "pressure_side":pressure.map(side_name),
            "fade_side":fade_side.map(side_name),
            "min_delta_share":self.cfg.cumulative_min_delta_share,
            "max_return_pct":self.cfg.cumulative_max_return_pct,
            "max_efficiency":self.cfg.cumulative_max_efficiency,
            "reason":reason,
        })
    }

    fn cumulative_signal(&self, ts: Timestamp, observation: &Json) -> Option<Signal> {
        let side = observation.get("fade_side")?.as_str()?;
        Some(Signal::new(
            SignalKind::Other,
            ts,
            "OrderFlowExhaustion",
            json!({
                "model":"orderflow_exhaustion_v6",
                "stage":"observation",
                "profile":"cumulative_absorption",
                "event_id":ts.as_millis(),
                "strength":if observation.get("delta_share").and_then(Json::as_f64).unwrap_or(0.0).abs() >= self.cfg.strong_delta_share { "strong" } else { "watch" },
                "side":side,
                "price":observation.get("close").and_then(Json::as_f64),
                "zone":observation.get("close").and_then(Json::as_f64),
                "location_confirmed":false,
                "context_score":0,
                "observation":observation,
            }),
        ))
    }

    fn emit_cumulative_if_due(
        &mut self,
        ts: Timestamp,
        bucket_start_ms: i64,
        cumulative: &mut Json,
        out: &mut Vec<Signal>,
    ) {
        if cumulative.get("active").and_then(Json::as_bool) == Some(true)
            && bucket_start_ms >= self.cumulative_cooldown_until
        {
            if let Some(observation) = self.cumulative_signal(ts, cumulative) {
                out.push(observation);
                cumulative["emitted"] = json!(true);
                self.cumulative_cooldown_until = bucket_start_ms
                    + self.cfg.cumulative_cooldown_buckets as i64 * self.cfg.bucket_ms;
            }
        }
    }

    fn trade_gate(&self, setup: &Setup) -> (bool, Vec<&'static str>) {
        let mut blockers = Vec::new();
        let min_context_score = if setup.trend_aligned {
            self.cfg
                .execution_min_context_score
                .saturating_sub(self.cfg.trend_aligned_context_discount)
        } else {
            self.cfg.execution_min_context_score
        };
        if setup.context_score < min_context_score {
            blockers.push("context_score");
        }
        if self.cfg.execution_require_absorption && !setup.absorption_confirmed {
            blockers.push("absorption");
        }
        if self.cfg.execution_require_location_in_range
            && setup.trdr_regime == "range"
            && !setup.location_confirmed
        {
            blockers.push("range_location");
        }
        if self.cfg.execution_require_source_coverage && !setup.trdr_source_coverage_complete {
            blockers.push("source_coverage");
        }
        if self.cfg.execution_require_footprint && !setup.trdr_footprint_matches {
            blockers.push("footprint");
        }
        (blockers.is_empty(), blockers)
    }

    fn profile(&self, setup: &Setup) -> &'static str {
        if self.trade_gate(setup).0 {
            "verified_context"
        } else {
            "research_context"
        }
    }

    fn signal(&self, ts: Timestamp, setup: &Setup, price: f64, delta_share: f64) -> Signal {
        let profile = self.profile(setup);
        let (trade_eligible, gate_blockers) = self.trade_gate(setup);
        let effective_min_context_score = if setup.trend_aligned {
            self.cfg
                .execution_min_context_score
                .saturating_sub(self.cfg.trend_aligned_context_discount)
        } else {
            self.cfg.execution_min_context_score
        };
        let trdr = json!({
            "zone_grade":setup.trdr_zone_grade,
            "zone_band_pct":setup.trdr_zone_band_pct,
            "zone_ratio":setup.trdr_zone_ratio,
            "zone_wall_usd":setup.trdr_zone_wall_usd,
            "zone_distance_bin":setup.trdr_zone_distance_bin,
            "source_wall_ratio":setup.trdr_source_wall_ratio,
            "ratio_basis":"local_mirrored_bin",
            "zone_distance_pct":setup.trdr_zone_distance_pct,
            "zone_low":setup.trdr_zone_low,
            "zone_high":setup.trdr_zone_high,
            "spot_perp_confluence":setup.trdr_spot_perp_confluence,
            "delta_tier":setup.trdr_delta_tier,
            "delta_usd":setup.trdr_delta_usd,
            "delta_share":setup.trdr_delta_share,
            "spot_volume_usd":setup.trdr_spot_volume_usd,
            "perp_volume_usd":setup.trdr_perp_volume_usd,
            "source_stats":setup.trdr_source_stats,
            "liquidation_stats":setup.trdr_liquidation_stats,
            "oi_quadrant":setup.trdr_oi_quadrant,
            "regime":setup.trdr_regime,
            "trend_blocked":setup.trend_blocked,
            "production_ready":setup.production_ready,
            "zone_persistence_ms":setup.trdr_zone_persistence_ms,
            "source_coverage_complete":setup.trdr_source_coverage_complete,
            "footprint_matches":setup.trdr_footprint_matches,
            "footprint_price":setup.trdr_footprint_price,
            "footprint_delta_usd":setup.trdr_footprint_delta_usd,
            "stacked_imbalance":setup.trdr_stacked_imbalance,
            "long_liquidation_usd":setup.trdr_long_liquidation_usd,
            "short_liquidation_usd":setup.trdr_short_liquidation_usd
        });
        Signal::new(
            SignalKind::Other,
            ts,
            "OrderFlowExhaustion",
            json!({
                "model":"orderflow_exhaustion_v13",
                "stage":"confirmed",
                "profile":profile,
                "event_id":setup.event_id,
                "strength":setup.strength,
                "side":match setup.side { Side::Buy => "buy", Side::Sell => "sell" },
                "price":price,
                "zone":setup.entry_price,
                "stop_anchor":setup.stop_anchor,
                "volume_usd":setup.event_volume,
                "volume_ratio":setup.event_volume_ratio,
                "event_delta_share":setup.event_delta_share,
                "confirm_delta_share":delta_share,
                "efficiency":setup.event_efficiency,
                "location_confirmed":setup.location_confirmed,
                "absorption_confirmed":setup.absorption_confirmed,
                "trend_aligned":setup.trend_aligned,
                "context_score":setup.context_score,
                "context_reasons":setup.context_reasons,
                "context_fused":setup.context_fused,
                "trade_eligible":trade_eligible,
                "estimated_roundtrip_fee_bps":6.0,
                "trade_gate":{
                    "min_context_score":effective_min_context_score,
                    "base_min_context_score":self.cfg.execution_min_context_score,
                    "trend_aligned_discount":self.cfg.trend_aligned_context_discount,
                    "source_coverage_required":self.cfg.execution_require_source_coverage,
                    "footprint_required":self.cfg.execution_require_footprint,
                    "absorption_required":self.cfg.execution_require_absorption,
                    "range_location_required":self.cfg.execution_require_location_in_range,
                    "blockers":gate_blockers
                },
                "book_imbalance":self.book_imbalance,
                "trdr":trdr
            }),
        )
    }

    fn evaluate(&mut self, b: &Bucket, ctx: &Ctx) -> Vec<Signal> {
        let ts = Timestamp::from_millis(b.start_ms + self.cfg.bucket_ms);
        let observed = self.history.iter().filter(|x| x.volume > 0.0).count();
        let mut cumulative = self.cumulative_absorption(b);
        if observed < self.cfg.min_baseline_buckets {
            self.eval = Some(json!({
                "ts_ms":ts.as_millis(), "decision":"warmup", "reason":"真实 10 秒订单流基线积累中",
                "buckets":observed, "required":self.cfg.min_baseline_buckets,
                "bucket_ms":self.cfg.bucket_ms,
                "cumulative_absorption":cumulative
            }));
            return vec![];
        }

        let baseline = self.median_volume();
        let volume_ratio = if baseline > 0.0 {
            b.volume / baseline
        } else {
            0.0
        };
        let delta_share = if b.volume > 0.0 {
            b.delta / b.volume
        } else {
            0.0
        };
        let range = (b.high - b.low).max(b.close * 1e-7);
        let efficiency = (b.close - b.open).abs() / range;
        let prior: Vec<&Bucket> = self
            .history
            .iter()
            .rev()
            .take(self.cfg.location_buckets)
            .collect();
        let prior_high = prior
            .iter()
            .map(|x| x.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let prior_low = prior.iter().map(|x| x.low).fold(f64::INFINITY, f64::min);
        let swept_high =
            b.high >= prior_high * (1.0 + self.cfg.min_sweep_pct) && b.close < prior_high;
        let swept_low = b.low <= prior_low * (1.0 - self.cfg.min_sweep_pct) && b.close > prior_low;
        let vwap = if self.session_notional > 0.0 {
            self.session_pv / self.session_notional
        } else {
            b.close
        };
        let vwap_dev = b.close / vwap - 1.0;
        let oi_change = match (self.latest_oi, self.previous_oi) {
            (Some(a), Some(z)) if z > 0.0 => Some(a / z - 1.0),
            _ => None,
        };

        let mut decision = "none";
        let mut reason = "等待自适应放量事件";
        let mut out = Vec::new();

        if let Some(mut setup) = self.setup.take() {
            if b.start_ms > setup.expires_at {
                decision = "expired";
                reason = "放量事件在确认窗口内未出现 Delta 反转";
            } else {
                let trdr = trdr_context(ctx, setup.side, &self.cfg, ts.as_millis());
                let oi_support = trdr.oi_quadrant != "neutral"
                    || oi_change.is_some_and(|value| value.abs() >= 0.0005);
                fuse_setup_context(&mut setup, &trdr, oi_support, &self.cfg, ts.as_millis());
                let reverse_delta = match setup.side {
                    Side::Buy => delta_share >= self.cfg.confirm_delta_share,
                    Side::Sell => delta_share <= -self.cfg.confirm_delta_share,
                };
                let reverse_price = match setup.side {
                    Side::Buy => b.close > b.open && b.close > setup.event_price,
                    Side::Sell => b.close < b.open && b.close < setup.event_price,
                };
                if reverse_delta && reverse_price {
                    let profile = self.profile(&setup);
                    decision = "confirmed";
                    reason = if profile == "verified_context" {
                        "反转与全市场执行门槛全部通过"
                    } else {
                        "反转已确认，但只记录研究样本，不允许下单"
                    };
                    out.push(self.signal(ts, &setup, b.close, delta_share));
                    let slot = side_slot(setup.side);
                    self.cooldown_until[slot] =
                        b.start_ms + self.cfg.cluster_cooldown_buckets as i64 * self.cfg.bucket_ms;
                    self.cooldown_price[slot] = Some(setup.event_price);
                } else {
                    self.setup = Some(setup);
                }
            }
        }

        let pressure_side = if delta_share >= self.cfg.min_delta_share {
            Some(Side::Buy)
        } else if delta_share <= -self.cfg.min_delta_share {
            Some(Side::Sell)
        } else {
            None
        };
        let exhausted_side = pressure_side.map(Side::opposite);
        let no_result = efficiency <= self.cfg.max_efficiency
            || pressure_side == Some(Side::Buy) && b.close <= b.open
            || pressure_side == Some(Side::Sell) && b.close >= b.open;
        let volume_event =
            volume_ratio >= self.cfg.classic_volume_ratio && pressure_side.is_some() && no_result;

        if volume_event && decision == "none" {
            let side = exhausted_side.expect("volume event has pressure side");
            let slot = side_slot(side);
            let cooldown_blocks = self.cooldown_blocks(side, b.start_ms, b.close, volume_ratio);
            let exceptional = self.rearm_triggered(side, b.close, volume_ratio);
            let replace_pending = self.setup.as_ref().is_some_and(|pending| {
                exceptional
                    && (pending.side != side
                        || volume_ratio >= self.cfg.cooldown_rearm_volume_ratio)
            });
            let can_arm = self.setup.is_none() || replace_pending;
            if !can_arm {
                // Existing confirmation window remains authoritative unless a genuinely
                // exceptional/opposite event starts a new market episode.
            } else if cooldown_blocks {
                decision = "deduplicated";
                reason = "同方向同价区事件仍在冷却，已去重";
            } else {
                let trdr = trdr_context(ctx, side, &self.cfg, ts.as_millis());
                if trdr.trend_blocked {
                    decision = "trend_blocked";
                    reason = "TRDR 判定为同向单边，禁止逆势抄底/摸顶";
                    self.emit_cumulative_if_due(ts, b.start_ms, &mut cumulative, &mut out);
                    self.eval = Some(json!({
                        "ts_ms":ts.as_millis(), "decision":decision, "reason":reason,
                        "price":b.close, "bucket_ms":self.cfg.bucket_ms,
                        "volume_ratio":volume_ratio, "delta_share":delta_share,
                        "trdr_regime":trdr.regime, "trdr_zone_grade":trdr.zone_grade,
                        "trdr_delta_tier":trdr.delta_tier,
                        "trend_blocked":true,
                        "funnel":{"volume":true,"pressure":true,"stalled":no_result,
                            "pending_confirmation":false,"confirmed":false},
                        "cumulative_absorption":cumulative
                    }));
                    return out;
                }
                let local_location = match side {
                    Side::Buy => swept_low || vwap_dev <= -self.cfg.min_vwap_deviation_pct,
                    Side::Sell => swept_high || vwap_dev >= self.cfg.min_vwap_deviation_pct,
                };
                let location =
                    trdr.zone_matches || (self.cfg.allow_local_location && local_location);
                let oi_support =
                    trdr.oi_quadrant != "neutral" || oi_change.is_some_and(|v| v.abs() >= 0.0005);
                let stalled = efficiency <= self.cfg.max_efficiency * 0.65;
                let mut context_score = 0u8;
                let mut context_reasons = Vec::new();
                if location {
                    context_score += 1;
                    context_reasons.push(if trdr.zone_matches {
                        "trdr_zone".to_string()
                    } else {
                        "local_location".to_string()
                    });
                }
                if stalled {
                    context_score += 1;
                    context_reasons.push("absorption".to_string());
                }
                if trdr.delta_matches {
                    context_score += 1;
                    context_reasons.push("trdr_delta".to_string());
                }
                if trdr.footprint_matches {
                    context_score += 1;
                    context_reasons.push("footprint_cluster".to_string());
                }
                if oi_support {
                    context_score += 1;
                    context_reasons.push("oi".to_string());
                }
                if trdr.spot_perp_confluence {
                    context_score += 1;
                    context_reasons.push("spot_perp_confluence".to_string());
                }
                // local_location 已在 location 中计过一次，不能把同一证据重复加分。
                let liquidation_support = match side {
                    Side::Buy => trdr.long_liquidation_usd >= self.cfg.min_liquidation_usd,
                    Side::Sell => trdr.short_liquidation_usd >= self.cfg.min_liquidation_usd,
                };
                if liquidation_support {
                    context_score += 1;
                    context_reasons.push("liquidation".to_string());
                }
                let strong = volume_ratio >= self.cfg.strong_volume_ratio
                    || delta_share.abs() >= self.cfg.strong_delta_share;
                let confirm_buckets = if strong {
                    self.cfg.strong_confirm_buckets
                } else {
                    self.cfg.weak_confirm_buckets
                };
                let stop_anchor = match side {
                    Side::Buy => {
                        b.low.min(trdr.zone_low.unwrap_or(b.low)) * (1.0 - self.cfg.stop_buffer_pct)
                    }
                    Side::Sell => {
                        b.high.max(trdr.zone_high.unwrap_or(b.high))
                            * (1.0 + self.cfg.stop_buffer_pct)
                    }
                };
                self.setup = Some(Setup {
                    event_id: b.start_ms,
                    side,
                    strength: if strong { "strong" } else { "weak" },
                    event_price: if trdr.zone_matches {
                        match (trdr.zone_low, trdr.zone_high) {
                            (Some(lo), Some(hi)) => (lo + hi) / 2.0,
                            _ => b.close,
                        }
                    } else {
                        b.close
                    },
                    entry_price: if trdr.zone_matches {
                        match (trdr.zone_low, trdr.zone_high) {
                            (Some(lo), Some(hi)) => (lo + hi) / 2.0,
                            _ => b.close,
                        }
                    } else {
                        b.close
                    },
                    stop_anchor,
                    event_volume: b.volume,
                    event_volume_ratio: volume_ratio,
                    event_delta_share: delta_share,
                    event_efficiency: efficiency,
                    location_confirmed: location,
                    absorption_confirmed: stalled,
                    trend_aligned: matches!(
                        (side, trdr.regime.as_str()),
                        (Side::Buy, "trend_up") | (Side::Sell, "trend_down")
                    ),
                    context_score,
                    context_reasons,
                    context_fused: false,
                    trdr_zone_grade: trdr.zone_grade,
                    trdr_zone_band_pct: trdr.zone_band_pct,
                    trdr_zone_ratio: trdr.zone_ratio,
                    trdr_zone_wall_usd: trdr.zone_wall_usd,
                    trdr_zone_distance_bin: trdr.zone_distance_bin,
                    trdr_source_wall_ratio: trdr.source_wall_ratio,
                    trdr_zone_distance_pct: trdr.zone_distance_pct,
                    trdr_zone_low: trdr.zone_low,
                    trdr_zone_high: trdr.zone_high,
                    trdr_spot_perp_confluence: trdr.spot_perp_confluence,
                    trdr_delta_tier: trdr.delta_tier,
                    trdr_delta_usd: trdr.delta_usd,
                    trdr_delta_share: trdr.delta_share,
                    trdr_spot_volume_usd: trdr.spot_volume_usd,
                    trdr_perp_volume_usd: trdr.perp_volume_usd,
                    trdr_source_stats: trdr.source_stats,
                    trdr_liquidation_stats: trdr.liquidation_stats,
                    trdr_oi_quadrant: trdr.oi_quadrant,
                    trdr_regime: trdr.regime,
                    trend_blocked: trdr.trend_blocked,
                    production_ready: trdr.production_ready,
                    trdr_zone_persistence_ms: trdr.zone_persistence_ms,
                    trdr_source_coverage_complete: trdr.source_coverage_complete,
                    trdr_footprint_matches: trdr.footprint_matches,
                    trdr_footprint_price: trdr.footprint_price,
                    trdr_footprint_delta_usd: trdr.footprint_delta_usd,
                    trdr_stacked_imbalance: trdr.stacked_imbalance,
                    trdr_long_liquidation_usd: trdr.long_liquidation_usd,
                    trdr_short_liquidation_usd: trdr.short_liquidation_usd,
                    expires_at: b.start_ms + confirm_buckets as i64 * self.cfg.bucket_ms,
                });
                decision = "volume_event";
                reason = if exceptional && b.start_ms < self.cooldown_until[slot] {
                    "异常放量或新价区事件，已突破同方向冷却并等待反转"
                } else if strong {
                    "强放量事件，等待 3 分钟内 Delta 反转"
                } else {
                    "弱放量事件，等待 5 分钟内 Delta 反转"
                };
            }
        }

        let pending = self.setup.as_ref().map(|s| {
            json!({
                "event_id":s.event_id,
                "side":match s.side { Side::Buy => "buy", Side::Sell => "sell" },
                "strength":s.strength,
                "event_price":s.event_price,
                "entry_price":s.entry_price,
                "expires_at_ms":s.expires_at,
                "context_score":s.context_score,
                "context_fused":s.context_fused,
                "location_confirmed":s.location_confirmed,
                "absorption_confirmed":s.absorption_confirmed,
                "trend_aligned":s.trend_aligned,
                "context_reasons":s.context_reasons,
                "volume_ratio":s.event_volume_ratio,
                "event_delta_share":s.event_delta_share,
                "trdr_zone_grade":s.trdr_zone_grade,
                "trdr_zone_band_pct":s.trdr_zone_band_pct,
                "trdr_zone_ratio":s.trdr_zone_ratio,
                "trdr_zone_wall_usd":s.trdr_zone_wall_usd,
                "trdr_zone_distance_bin":s.trdr_zone_distance_bin,
                "trdr_source_wall_ratio":s.trdr_source_wall_ratio,
                "trdr_zone_distance_pct":s.trdr_zone_distance_pct,
                "trdr_spot_perp_confluence":s.trdr_spot_perp_confluence,
                "trdr_delta_tier":s.trdr_delta_tier,
                "trdr_delta_usd":s.trdr_delta_usd,
                "trdr_delta_share":s.trdr_delta_share,
                "trdr_spot_volume_usd":s.trdr_spot_volume_usd,
                "trdr_perp_volume_usd":s.trdr_perp_volume_usd,
                "trdr_source_stats":s.trdr_source_stats,
                "trdr_liquidation_stats":s.trdr_liquidation_stats,
                "trdr_oi_quadrant":s.trdr_oi_quadrant,
                "trdr_regime":s.trdr_regime,
                "trend_blocked":s.trend_blocked,
                "production_ready":s.production_ready,
                "trdr_zone_persistence_ms":s.trdr_zone_persistence_ms,
                "trdr_source_coverage_complete":s.trdr_source_coverage_complete,
                "trdr_footprint_matches":s.trdr_footprint_matches,
                "trdr_footprint_price":s.trdr_footprint_price,
                "trdr_footprint_delta_usd":s.trdr_footprint_delta_usd,
                "trdr_stacked_imbalance":s.trdr_stacked_imbalance,
                "trdr_long_liquidation_usd":s.trdr_long_liquidation_usd,
                "trdr_short_liquidation_usd":s.trdr_short_liquidation_usd
            })
        });
        self.emit_cumulative_if_due(ts, b.start_ms, &mut cumulative, &mut out);
        let confirmed_signal = out
            .iter()
            .find(|signal| signal.payload.get("stage").and_then(Json::as_str) == Some("confirmed"))
            .map(|signal| signal.payload.clone());
        let confirmed_profile = confirmed_signal
            .as_ref()
            .and_then(|payload| payload.get("profile"))
            .cloned();
        let trade_confirmed = confirmed_signal
            .as_ref()
            .and_then(|payload| payload.get("trade_eligible"))
            .and_then(Json::as_bool)
            == Some(true);
        self.eval = Some(json!({
            "ts_ms":ts.as_millis(), "price":b.close, "decision":decision, "reason":reason,
            "bucket_ms":self.cfg.bucket_ms, "volume_usd":b.volume,
            "volume_rate_usd_s":b.volume / (self.cfg.bucket_ms as f64 / 1000.0),
            "baseline_volume_usd":baseline, "volume_ratio":volume_ratio,
            "delta_usd":b.delta, "delta_share":delta_share, "efficiency":efficiency,
            "vwap":vwap, "vwap_deviation_pct":vwap_dev,
            "swept_high":swept_high, "swept_low":swept_low,
            "prior_high":prior_high, "prior_low":prior_low,
            "oi_change_pct":oi_change, "book_imbalance":self.book_imbalance,
            "funnel":{
                "volume":volume_ratio >= self.cfg.classic_volume_ratio,
                "pressure":pressure_side.is_some(),
                "stalled":no_result,
                "pending_confirmation":self.setup.is_some(),
                "confirmed":trade_confirmed
            },
            "pending":pending, "profile":confirmed_profile,
            "confirmation":confirmed_signal,
            "cumulative_absorption":cumulative
        }));
        out
    }
}

impl SignalPlugin for OrderFlowExhaustion {
    fn name(&self) -> &'static str {
        "OrderFlowExhaustion"
    }

    fn on_event(&mut self, ev: &Event, ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Oi(oi) => {
                self.previous_oi = self.latest_oi;
                self.latest_oi = Some(oi.oi_usd);
                vec![]
            }
            Event::Book(book) => {
                if book.exchange != Exchange::BinanceFutures {
                    return vec![];
                }
                let bid = book.bid_qty_within(0.002);
                let ask = book.ask_qty_within(0.002);
                self.book_imbalance = (bid + ask > 0.0).then_some((bid - ask) / (bid + ask));
                vec![]
            }
            Event::Trade(t) => {
                // 现货逐笔成交只供 TrdrMarketMap 做跨市场 Delta 确认，不能推进
                // 合约执行信号的桶，否则 context feed 会产生无法下单的幽灵信号。
                if t.exchange != Exchange::BinanceFutures {
                    return vec![];
                }
                let ms = t.ts.as_millis();
                let day = ms.div_euclid(86_400_000);
                if day != self.session_day {
                    self.session_day = day;
                    self.session_notional = 0.0;
                    self.session_pv = 0.0;
                }
                let notional = t.notional();
                self.session_notional += notional;
                self.session_pv += t.price.to_f64() * notional;
                let start = ms.div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms;
                let mut signals = Vec::new();
                if self.current.as_ref().is_some_and(|b| b.start_ms != start) {
                    let completed = self.current.take().expect("current bucket");
                    signals = self.evaluate(&completed, ctx);
                    self.history.push_back(completed);
                    while self.history.len()
                        > self
                            .cfg
                            .location_buckets
                            .max(self.cfg.baseline_buckets)
                            .max(self.cfg.cumulative_window_buckets)
                    {
                        self.history.pop_front();
                    }
                }
                let b = self
                    .current
                    .get_or_insert_with(|| Bucket::new(start, t.price.to_f64()));
                b.add(t.price.to_f64(), notional, t.signed_notional());
                signals
            }
            Event::Funding(_) | Event::Liquidation(_) | Event::Timer(_) => vec![],
        }
    }

    fn eval_note(&self) -> Option<Json> {
        self.eval.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::{Exchange, Price, Qty, Symbol, Trade};

    fn trade(ms: i64, price: f64, buy: bool, qty: f64) -> Event {
        Event::Trade(Trade {
            ts: Timestamp::from_millis(ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker: !buy,
        })
    }

    fn test_signal() -> OrderFlowExhaustion {
        let mut cfg = Config::from_params(&json!({}));
        cfg.bucket_ms = 1_000;
        cfg.min_baseline_buckets = 5;
        cfg.baseline_buckets = 5;
        cfg.location_buckets = 5;
        cfg.classic_volume_ratio = 1.5;
        cfg.strong_volume_ratio = 3.0;
        cfg.strong_confirm_buckets = 3;
        cfg.weak_confirm_buckets = 5;
        cfg.cluster_cooldown_buckets = 10;
        OrderFlowExhaustion::new(cfg)
    }

    #[test]
    fn cumulative_absorption_emits_observation_only_signal() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.bucket_ms = 1_000;
        cfg.min_baseline_buckets = 5;
        cfg.baseline_buckets = 5;
        cfg.location_buckets = 5;
        cfg.cumulative_window_buckets = 3;
        cfg.cumulative_min_delta_share = 0.20;
        cfg.cumulative_max_return_pct = 0.01;
        cfg.cumulative_max_efficiency = 0.60;
        let mut signal = OrderFlowExhaustion::new(cfg);
        for i in 0..5 {
            let close = if i % 2 == 0 { 100.1 } else { 99.9 };
            signal.history.push_back(Bucket {
                start_ms: i * 1_000,
                open: 100.0,
                high: 100.2,
                low: 99.8,
                close,
                volume: 100.0,
                delta: -30.0,
            });
        }
        let current = Bucket {
            start_ms: 6_000,
            open: 99.9,
            high: 100.1,
            low: 99.8,
            close: 100.0,
            volume: 100.0,
            delta: -30.0,
        };
        let out = signal.evaluate(&current, &Ctx::default());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["stage"], "observation");
        assert_eq!(out[0].payload["profile"], "cumulative_absorption");
        assert_eq!(out[0].payload["side"], "buy");
        assert_eq!(signal.eval.as_ref().unwrap()["decision"], "none");
        assert_eq!(signal.eval.as_ref().unwrap()["funnel"]["confirmed"], false);
        assert_eq!(
            signal.eval.as_ref().unwrap()["cumulative_absorption"]["active"],
            true
        );
        assert_eq!(
            signal.eval.as_ref().unwrap()["cumulative_absorption"]["emitted"],
            true
        );
    }

    fn setup(volume_ratio: f64, location_confirmed: bool, context_score: u8) -> Setup {
        Setup {
            event_id: 1,
            side: Side::Sell,
            strength: "strong",
            event_price: 100.0,
            entry_price: 100.0,
            stop_anchor: 101.0,
            event_volume: 1_000.0,
            event_volume_ratio: volume_ratio,
            event_delta_share: 0.5,
            event_efficiency: 0.1,
            location_confirmed,
            absorption_confirmed: true,
            trend_aligned: false,
            context_score,
            context_reasons: vec![],
            context_fused: false,
            trdr_zone_grade: "yellow".into(),
            trdr_zone_band_pct: Some(0.025),
            trdr_zone_ratio: Some(2.5),
            trdr_zone_wall_usd: Some(2_000_000.0),
            trdr_zone_distance_bin: Some(1),
            trdr_source_wall_ratio: Some(1.2),
            trdr_zone_distance_pct: Some(0.001),
            trdr_zone_low: Some(99.5),
            trdr_zone_high: Some(100.5),
            trdr_spot_perp_confluence: true,
            trdr_delta_tier: 2,
            trdr_delta_usd: Some(-1_000_000.0),
            trdr_delta_share: Some(-0.10),
            trdr_spot_volume_usd: Some(10_000_000.0),
            trdr_perp_volume_usd: Some(20_000_000.0),
            trdr_source_stats: vec![],
            trdr_liquidation_stats: vec![],
            trdr_oi_quadrant: "price_down_oi_up".into(),
            trdr_regime: "range".into(),
            trend_blocked: false,
            production_ready: true,
            trdr_zone_persistence_ms: 60_000,
            trdr_source_coverage_complete: true,
            trdr_footprint_matches: true,
            trdr_footprint_price: Some(100.0),
            trdr_footprint_delta_usd: Some(-500_000.0),
            trdr_stacked_imbalance: 3,
            trdr_long_liquidation_usd: 2_000_000.0,
            trdr_short_liquidation_usd: 0.0,
            expires_at: 10,
        }
    }

    #[test]
    fn spot_trades_are_context_only() {
        let mut s = test_signal();
        let mut ev = trade(1_000, 100.0, true, 1.0);
        if let Event::Trade(t) = &mut ev {
            t.exchange = Exchange::BinanceSpot;
        }
        assert!(s.on_event(&ev, &Ctx::default()).is_empty());
        assert!(s.current.is_none());
    }

    #[test]
    fn profile_is_a_single_verified_or_research_gate() {
        let s = test_signal();
        assert_eq!(s.profile(&setup(2.2, true, 5)), "verified_context");
        assert_eq!(s.profile(&setup(2.2, true, 4)), "research_context");
    }

    #[test]
    fn execution_requires_absorption_and_range_location() {
        let s = test_signal();
        let mut no_absorption = setup(2.2, true, 5);
        no_absorption.absorption_confirmed = false;
        assert!(s.trade_gate(&no_absorption).1.contains(&"absorption"));

        let no_location = setup(2.2, false, 5);
        assert!(s.trade_gate(&no_location).1.contains(&"range_location"));
    }

    #[test]
    fn trend_aligned_setup_gets_one_point_discount() {
        let s = test_signal();
        let mut aligned = setup(2.2, false, 4);
        aligned.side = Side::Buy;
        aligned.trdr_regime = "trend_up".into();
        aligned.trend_aligned = true;
        assert_eq!(s.profile(&aligned), "verified_context");
    }

    #[test]
    fn cooldown_is_directional_and_exceptional_events_rearm_it() {
        let mut signal = test_signal();
        signal.cfg.cooldown_rearm_volume_ratio = 50.0;
        signal.cfg.cooldown_rearm_price_pct = 0.005;
        signal.cooldown_until[side_slot(Side::Sell)] = 20_000;
        signal.cooldown_price[side_slot(Side::Sell)] = Some(100.0);

        assert!(signal.cooldown_blocks(Side::Sell, 10_000, 100.1, 10.0));
        assert!(!signal.cooldown_blocks(Side::Buy, 10_000, 100.1, 10.0));
        assert!(!signal.cooldown_blocks(Side::Sell, 10_000, 100.1, 50.0));
        assert!(!signal.cooldown_blocks(Side::Sell, 10_000, 100.51, 10.0));
    }

    #[test]
    fn pending_event_can_fuse_later_trdr_context() {
        let signal = test_signal();
        let mut ctx = Ctx::default();
        ctx.set_latest(Signal::new(
            SignalKind::ObiZone,
            Timestamp::from_millis(40_000),
            "test",
            json!({
                "active":true,"ts_ms":40_000,"side":"sell","grade":"red",
                "grade_rank":3,"distance_pct":0.001,"persistence_ms":40_000,
                "spot_perp_confluence":true,"zone_low":99.8,"zone_high":100.2,
                "ratio":4.0,"wall_usd":40_000_000.0
            }),
        ));
        ctx.set_latest(Signal::new(
            SignalKind::DeltaTier,
            Timestamp::from_millis(40_000),
            "test",
            json!({
                "ts_ms":40_000,"tier":3,"direction":"buy",
                "source_coverage_complete":true,"footprint_direction":"buy",
                "stacked_imbalance":4,"spot_delta_usd":1.0,"perp_delta_usd":2.0
            }),
        ));
        ctx.set_latest(Signal::new(
            SignalKind::OiQuadrant,
            Timestamp::from_millis(40_000),
            "test",
            json!({"quadrant":"new_longs"}),
        ));
        ctx.set_latest(Signal::new(
            SignalKind::TrendRegime,
            Timestamp::from_millis(40_000),
            "test",
            json!({"regime":"range","blocked_side":"none"}),
        ));

        let mut pending = setup(60.0, false, 2);
        pending.event_id = 10_000;
        pending.context_reasons = vec!["absorption".into(), "oi".into()];
        let trdr = trdr_context(&ctx, Side::Sell, &signal.cfg, 40_000);
        fuse_setup_context(&mut pending, &trdr, true, &signal.cfg, 40_000);

        assert!(pending.context_fused);
        assert!(pending.location_confirmed);
        assert!(pending.trdr_footprint_matches);
        assert!(pending.trdr_source_coverage_complete);
        assert_eq!(pending.trdr_zone_grade, "red");
        assert!(pending.context_score >= 6);
    }

    #[test]
    fn emits_one_confirmed_profile_and_deduplicates_cluster() {
        let mut s = test_signal();
        let ctx = Ctx::default();
        for i in 0..6 {
            s.on_event(&trade(i * 1_000, 100.0, true, 1.0), &ctx);
        }
        s.on_event(&trade(6_000, 100.20, true, 8.0), &ctx);
        s.on_event(&trade(6_500, 100.02, true, 2.0), &ctx);
        let event = s.on_event(&trade(7_000, 100.03, false, 1.0), &ctx);
        assert!(event.is_empty());
        assert_eq!(s.eval.as_ref().unwrap()["decision"], "volume_event");

        s.on_event(&trade(7_100, 100.00, false, 2.0), &ctx);
        s.on_event(&trade(7_500, 99.90, false, 3.0), &ctx);
        let confirmed = s.on_event(&trade(8_000, 99.91, true, 1.0), &ctx);
        assert_eq!(confirmed.len(), 1, "{confirmed:?} {:?}", s.eval);
        assert_eq!(confirmed[0].payload["stage"], "confirmed");
        assert!(confirmed[0].payload["profile"].is_string());

        s.on_event(&trade(8_100, 100.30, true, 10.0), &ctx);
        s.on_event(&trade(8_500, 99.80, true, 5.0), &ctx);
        let duplicate = s.on_event(&trade(9_000, 100.0, false, 1.0), &ctx);
        assert!(duplicate.is_empty());
        assert_eq!(s.eval.as_ref().unwrap()["decision"], "deduplicated");
    }

    #[test]
    fn expires_unconfirmed_event() {
        let mut s = test_signal();
        let ctx = Ctx::default();
        for i in 0..6 {
            s.on_event(&trade(i * 1_000, 100.0, true, 1.0), &ctx);
        }
        s.on_event(&trade(6_000, 100.20, true, 8.0), &ctx);
        s.on_event(&trade(6_500, 100.02, true, 2.0), &ctx);
        s.on_event(&trade(7_000, 100.01, true, 1.0), &ctx);
        for second in 7..=10 {
            s.on_event(&trade(second * 1_000 + 100, 100.0, true, 1.0), &ctx);
        }
        s.on_event(&trade(11_100, 100.0, true, 1.0), &ctx);
        assert_eq!(s.eval.as_ref().unwrap()["decision"], "expired");
    }
}
