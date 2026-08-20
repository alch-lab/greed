//! 稀缺强趋势覆盖层。
//!
//! 这一层不把普通 2h 波动当趋势。只有价格位移、路径效率、六路主动成交方向、
//! 现货/永续 Delta 与 OI 象限同时支持时才发出一次延续信号。趋势恢复中性至少
//! 一段时间后才允许重新武装，避免同一行情反复追价。

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Exchange, Side, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    bucket_ms: i64,
    warmup_ms: i64,
    min_return_pct: f64,
    max_return_pct: f64,
    min_efficiency: f64,
    min_delta_tier: u8,
    reset_neutral_ms: i64,
    reentry_cooldown_ms: i64,
    require_source_coverage: bool,
    require_spot_perp_delta: bool,
    require_oi_support: bool,
}

impl Config {
    pub fn from_params(params: &Json) -> Self {
        let f = |key: &str, default: f64| params.get(key).and_then(Json::as_f64).unwrap_or(default);
        let i = |key: &str, default: i64| params.get(key).and_then(Json::as_i64).unwrap_or(default);
        let u = |key: &str, default: u8| {
            params
                .get(key)
                .and_then(Json::as_u64)
                .map(|value| value as u8)
                .unwrap_or(default)
        };
        let b =
            |key: &str, default: bool| params.get(key).and_then(Json::as_bool).unwrap_or(default);
        Self {
            bucket_ms: i("bucket_ms", 10_000).max(1_000),
            warmup_ms: i("warmup_ms", 2 * 60 * 60_000).max(30 * 60_000),
            min_return_pct: f("min_return_pct", 0.008).clamp(0.002, 0.05),
            max_return_pct: f("max_return_pct", 0.030).clamp(0.005, 0.20),
            min_efficiency: f("min_efficiency", 0.15).clamp(0.02, 1.0),
            min_delta_tier: u("min_delta_tier", 1).min(4),
            reset_neutral_ms: i("reset_neutral_ms", 15 * 60_000).max(60_000),
            reentry_cooldown_ms: i("reentry_cooldown_ms", 30 * 60_000).max(60_000),
            require_source_coverage: b("require_source_coverage", true),
            require_spot_perp_delta: b("require_spot_perp_delta", true),
            require_oi_support: b("require_oi_support", true),
        }
    }
}

pub struct TrendContinuation {
    cfg: Config,
    started_at_ms: Option<i64>,
    last_bucket: Option<i64>,
    fired_side: Option<Side>,
    last_fired_ms: Option<i64>,
    neutral_since_ms: Option<i64>,
    eval: Option<Json>,
}

impl TrendContinuation {
    pub fn from_params(params: &Json) -> Self {
        Self {
            cfg: Config::from_params(params),
            started_at_ms: None,
            last_bucket: None,
            fired_side: None,
            last_fired_ms: None,
            neutral_since_ms: None,
            eval: None,
        }
    }

    fn evaluate(&mut self, ts: Timestamp, price: f64, ctx: &Ctx) -> Vec<Signal> {
        let now_ms = ts.as_millis();
        let started_at = *self.started_at_ms.get_or_insert(now_ms);
        let observed_ms = now_ms - started_at;
        let trend = ctx
            .latest_of(SignalKind::TrendRegime)
            .map(|signal| &signal.payload);
        let delta = ctx
            .latest_of(SignalKind::DeltaTier)
            .map(|signal| &signal.payload);
        let slow_return = trend
            .and_then(|value| value.get("slow_return_pct"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let slow_efficiency = trend
            .and_then(|value| value.get("slow_efficiency"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let regime = trend
            .and_then(|value| value.get("regime"))
            .and_then(Json::as_str)
            .unwrap_or("warming");
        let side = if slow_return >= self.cfg.min_return_pct
            && slow_return <= self.cfg.max_return_pct
            && slow_efficiency >= self.cfg.min_efficiency
            && regime == "trend_up"
        {
            Some(Side::Buy)
        } else if slow_return <= -self.cfg.min_return_pct
            && slow_return >= -self.cfg.max_return_pct
            && slow_efficiency >= self.cfg.min_efficiency
            && regime == "trend_down"
        {
            Some(Side::Sell)
        } else {
            None
        };

        if side.is_none() {
            let neutral_since = *self.neutral_since_ms.get_or_insert(now_ms);
            if now_ms - neutral_since >= self.cfg.reset_neutral_ms {
                self.fired_side = None;
            }
        } else {
            self.neutral_since_ms = None;
            if self.fired_side.is_some() && self.fired_side != side {
                self.fired_side = None;
            }
        }

        let expected = side.map(side_name);
        let delta_tier = delta
            .and_then(|value| value.get("tier"))
            .and_then(Json::as_u64)
            .unwrap_or(0) as u8;
        let delta_direction = delta
            .and_then(|value| value.get("direction"))
            .and_then(Json::as_str);
        let source_coverage = delta
            .and_then(|value| value.get("source_coverage_complete"))
            .and_then(Json::as_bool)
            .unwrap_or(false);
        let spot_delta = delta
            .and_then(|value| value.get("spot_delta_usd"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let perp_delta = delta
            .and_then(|value| value.get("perp_delta_usd"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let oi_quadrant = delta
            .and_then(|value| value.get("oi_quadrant"))
            .and_then(Json::as_str)
            .unwrap_or("neutral");
        let sign = side.map_or(0.0, |value| if value == Side::Buy { 1.0 } else { -1.0 });
        let price_ready = side.is_some();
        let delta_ready = expected == delta_direction && delta_tier >= self.cfg.min_delta_tier;
        let coverage_ready = !self.cfg.require_source_coverage || source_coverage;
        let cross_market_ready =
            !self.cfg.require_spot_perp_delta || spot_delta * sign > 0.0 && perp_delta * sign > 0.0;
        let oi_ready = !self.cfg.require_oi_support
            || matches!(
                (side, oi_quadrant),
                (
                    Some(Side::Buy),
                    "new_longs" | "short_cover" | "short_covering"
                ) | (Some(Side::Sell), "new_shorts" | "long_liquidation")
            );
        let warmed = observed_ms >= self.cfg.warmup_ms;
        let flat = ctx.position.is_none();
        let reentry_ready = self
            .last_fired_ms
            .is_none_or(|last| now_ms - last >= self.cfg.reentry_cooldown_ms);
        let already_fired = side.is_some() && self.fired_side == side && !reentry_ready;
        let ready = warmed
            && flat
            && reentry_ready
            && price_ready
            && delta_ready
            && coverage_ready
            && cross_market_ready
            && oi_ready
            && !already_fired;

        let blockers = [
            (!warmed).then_some("warmup"),
            (!flat).then_some("position_open"),
            (!reentry_ready).then_some("reentry_cooldown"),
            (!price_ready).then_some("strong_price_trend"),
            (!delta_ready).then_some("delta_alignment"),
            (!coverage_ready).then_some("source_coverage"),
            (!cross_market_ready).then_some("spot_perp_delta"),
            (!oi_ready).then_some("oi_support"),
            already_fired.then_some("episode_deduplicated"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        self.eval = Some(json!({
            "ts_ms":now_ms,
            "decision":if ready { "confirmed" } else if !warmed { "warmup" } else { "waiting" },
            "reason":if ready { "强趋势与全市场资金流同向，允许趋势延续入场" } else { "等待强趋势覆盖层全部通过" },
            "price":price,
            "side":side.map(side_name),
            "slow_return_pct":slow_return,
            "slow_efficiency":slow_efficiency,
            "regime":regime,
            "max_return_pct":self.cfg.max_return_pct,
            "delta_tier":delta_tier,
            "delta_direction":delta_direction,
            "source_coverage_complete":source_coverage,
            "spot_delta_usd":spot_delta,
            "perp_delta_usd":perp_delta,
            "oi_quadrant":oi_quadrant,
            "warmed":warmed,
            "already_fired":already_fired,
            "flat":flat,
            "reentry_ready":reentry_ready,
            "reentry_cooldown_ms":self.cfg.reentry_cooldown_ms,
            "trade_eligible":ready,
            "blockers":blockers,
            "funnel":{
                "strong_trend":price_ready,
                "delta_alignment":delta_ready,
                "cross_market":coverage_ready && cross_market_ready,
                "oi_support":oi_ready,
                "confirmed":ready,
            }
        }));
        if !ready {
            return vec![];
        }
        let side = side.expect("ready trend side");
        self.fired_side = Some(side);
        self.last_fired_ms = Some(now_ms);
        vec![Signal::new(
            SignalKind::Other,
            ts,
            "TrendContinuation",
            json!({
                "model":"strong_trend_continuation_v1",
                "stage":"confirmed",
                "profile":"strong_trend",
                "event_id":now_ms,
                "strength":"strong",
                "side":side_name(side),
                "price":price,
                "zone":price,
                "stop_anchor":match side { Side::Buy => price * 0.9975, Side::Sell => price * 1.0025 },
                "trade_eligible":true,
                "estimated_roundtrip_fee_bps":8.0,
                "slow_return_pct":slow_return,
                "slow_efficiency":slow_efficiency,
                "delta_tier":delta_tier,
                "delta_direction":delta_direction,
                "spot_delta_usd":spot_delta,
                "perp_delta_usd":perp_delta,
                "oi_quadrant":oi_quadrant,
                "source_coverage_complete":source_coverage,
            }),
        )]
    }
}

impl SignalPlugin for TrendContinuation {
    fn name(&self) -> &'static str {
        "TrendContinuation"
    }

    fn on_event(&mut self, event: &Event, ctx: &Ctx) -> Vec<Signal> {
        let Event::Trade(trade) = event else {
            return vec![];
        };
        if trade.exchange != Exchange::BinanceFutures {
            return vec![];
        }
        let bucket = trade.ts.as_millis().div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms;
        if self.last_bucket == Some(bucket) {
            return vec![];
        }
        self.last_bucket = Some(bucket);
        self.evaluate(trade.ts, trade.price.to_f64(), ctx)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::{Price, Qty, Symbol, Trade};

    fn trade(ts_ms: i64, price: f64) -> Event {
        Event::Trade(Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(1.0),
            is_buyer_maker: false,
        })
    }

    #[test]
    fn emits_once_when_every_strong_trend_gate_passes() {
        let mut plugin = TrendContinuation::from_params(&json!({"warmup_ms":1800000}));
        let mut ctx = Ctx::default();
        ctx.set_latest(Signal::new(
            SignalKind::TrendRegime,
            Timestamp::from_millis(2_000_000),
            "test",
            json!({"regime":"trend_up","slow_return_pct":0.009,"slow_efficiency":0.20}),
        ));
        ctx.set_latest(Signal::new(
            SignalKind::DeltaTier,
            Timestamp::from_millis(2_000_000),
            "test",
            json!({"tier":3,"direction":"buy","source_coverage_complete":true,
                "spot_delta_usd":1.0,"perp_delta_usd":2.0,"oi_quadrant":"new_longs"}),
        ));
        assert!(plugin.on_event(&trade(0, 100.0), &ctx).is_empty());
        let emitted = plugin.on_event(&trade(2_000_000, 101.0), &ctx);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].payload["side"], "buy");
        assert!(plugin.on_event(&trade(2_010_000, 101.1), &ctx).is_empty());
        let reentry = plugin.on_event(&trade(4_000_000, 102.0), &ctx);
        assert_eq!(reentry.len(), 1);
    }
}
