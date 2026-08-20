//! Flow-confirmed tactical trend pullback.
//!
//! This is deliberately not a generic VWAP dip buyer.  A signal requires a
//! directional two-hour regime, a measurable pullback from the rolling 30-minute
//! extreme, a price reclaim, and aligned spot/perpetual aggressive flow.

use std::collections::VecDeque;

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Exchange, Side, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    bucket_ms: i64,
    warmup_ms: i64,
    price_window_ms: i64,
    range_window_ms: i64,
    min_return_pct: f64,
    min_efficiency: f64,
    min_pullback_pct: f64,
    reclaim_pct: f64,
    min_delta_tier: u8,
    cooldown_ms: i64,
    require_source_coverage: bool,
    require_spot_perp_delta: bool,
}

impl Config {
    pub fn from_params(params: &Json) -> Self {
        let f = |key: &str, default: f64| params.get(key).and_then(Json::as_f64).unwrap_or(default);
        let i = |key: &str, default: i64| params.get(key).and_then(Json::as_i64).unwrap_or(default);
        let u = |key: &str, default: u8| {
            params
                .get(key)
                .and_then(Json::as_u64)
                .map(|v| v as u8)
                .unwrap_or(default)
        };
        let b =
            |key: &str, default: bool| params.get(key).and_then(Json::as_bool).unwrap_or(default);
        Self {
            bucket_ms: i("bucket_ms", 10_000).max(1_000),
            warmup_ms: i("warmup_ms", 2 * 60 * 60_000).max(30 * 60_000),
            price_window_ms: i("price_window_ms", 30 * 60_000).max(5 * 60_000),
            range_window_ms: i("range_window_ms", 60 * 60_000).max(10 * 60_000),
            min_return_pct: f("min_return_pct", 0.005).clamp(0.001, 0.05),
            min_efficiency: f("min_efficiency", 0.10).clamp(0.01, 1.0),
            min_pullback_pct: f("min_pullback_pct", 0.002).clamp(0.0005, 0.02),
            reclaim_pct: f("reclaim_pct", 0.0006).clamp(0.0002, 0.01),
            min_delta_tier: u("min_delta_tier", 1).min(4),
            cooldown_ms: i("cooldown_ms", 30 * 60_000).max(60_000),
            require_source_coverage: b("require_source_coverage", true),
            require_spot_perp_delta: b("require_spot_perp_delta", true),
        }
    }
}

pub struct TacticalPullback {
    cfg: Config,
    started_at_ms: Option<i64>,
    last_bucket: Option<i64>,
    prices: VecDeque<(i64, f64)>,
    trend_side: Option<Side>,
    pullback_extreme: Option<f64>,
    cooldown_until_ms: i64,
    range_observation_cooldown_until_ms: i64,
    eval: Option<Json>,
}

impl TacticalPullback {
    pub fn from_params(params: &Json) -> Self {
        Self {
            cfg: Config::from_params(params),
            started_at_ms: None,
            last_bucket: None,
            prices: VecDeque::new(),
            trend_side: None,
            pullback_extreme: None,
            cooldown_until_ms: 0,
            range_observation_cooldown_until_ms: 0,
            eval: None,
        }
    }

    fn evaluate(&mut self, ts: Timestamp, price: f64, ctx: &Ctx) -> Vec<Signal> {
        let now_ms = ts.as_millis();
        let started_at = *self.started_at_ms.get_or_insert(now_ms);
        while self
            .prices
            .front()
            .is_some_and(|(stamp, _)| *stamp < now_ms - self.cfg.range_window_ms)
        {
            self.prices.pop_front();
        }
        let recent = self
            .prices
            .iter()
            .filter(|(stamp, _)| *stamp >= now_ms - self.cfg.price_window_ms)
            .map(|(_, value)| *value)
            .collect::<Vec<_>>();
        let recent_high = recent.iter().copied().reduce(f64::max).unwrap_or(price);
        let recent_low = recent.iter().copied().reduce(f64::min).unwrap_or(price);
        let range_high = self
            .prices
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::max)
            .unwrap_or(price);
        let range_low = self
            .prices
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::min)
            .unwrap_or(price);

        let trend = ctx
            .latest_of(SignalKind::TrendRegime)
            .map(|signal| &signal.payload);
        let delta = ctx
            .latest_of(SignalKind::DeltaTier)
            .map(|signal| &signal.payload);
        let slow_return = trend
            .and_then(|v| v.get("slow_return_pct"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let slow_efficiency = trend
            .and_then(|v| v.get("slow_efficiency"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let regime = trend
            .and_then(|value| value.get("regime"))
            .and_then(Json::as_str)
            .unwrap_or("warming");
        let side = if slow_return >= self.cfg.min_return_pct
            && slow_efficiency >= self.cfg.min_efficiency
            && regime == "trend_up"
        {
            Some(Side::Buy)
        } else if slow_return <= -self.cfg.min_return_pct
            && slow_efficiency >= self.cfg.min_efficiency
            && regime == "trend_down"
        {
            Some(Side::Sell)
        } else {
            None
        };
        if side != self.trend_side {
            self.trend_side = side;
            self.pullback_extreme = None;
        }

        let mut pulled_back = false;
        let mut reclaimed = false;
        if let Some(value) = side {
            let extreme = self.pullback_extreme.get_or_insert(price);
            match value {
                Side::Buy => {
                    *extreme = extreme.min(price);
                    pulled_back = price <= recent_high * (1.0 - self.cfg.min_pullback_pct);
                    reclaimed = pulled_back && price >= *extreme * (1.0 + self.cfg.reclaim_pct);
                }
                Side::Sell => {
                    *extreme = extreme.max(price);
                    pulled_back = price >= recent_low * (1.0 + self.cfg.min_pullback_pct);
                    reclaimed = pulled_back && price <= *extreme * (1.0 - self.cfg.reclaim_pct);
                }
            }
        }

        let expected = side.map(side_name);
        let delta_direction = delta
            .and_then(|v| v.get("direction"))
            .and_then(Json::as_str);
        let delta_tier = delta
            .and_then(|v| v.get("tier"))
            .and_then(Json::as_u64)
            .unwrap_or(0) as u8;
        let coverage = delta
            .and_then(|v| v.get("source_coverage_complete"))
            .and_then(Json::as_bool)
            .unwrap_or(false);
        let spot_delta = delta
            .and_then(|v| v.get("spot_delta_usd"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let perp_delta = delta
            .and_then(|v| v.get("perp_delta_usd"))
            .and_then(Json::as_f64)
            .unwrap_or(0.0);
        let sign = side.map_or(0.0, |value| if value == Side::Buy { 1.0 } else { -1.0 });
        let delta_ready = expected == delta_direction && delta_tier >= self.cfg.min_delta_tier;
        let coverage_ready = !self.cfg.require_source_coverage || coverage;
        let cross_market_ready =
            !self.cfg.require_spot_perp_delta || spot_delta * sign > 0.0 && perp_delta * sign > 0.0;
        let warmed = now_ms - started_at >= self.cfg.warmup_ms
            && self
                .prices
                .front()
                .is_some_and(|(stamp, _)| now_ms - *stamp >= self.cfg.price_window_ms);
        let cooling_down = now_ms < self.cooldown_until_ms;
        let flat = ctx.position.is_none();
        let ready = warmed
            && flat
            && side.is_some()
            && pulled_back
            && reclaimed
            && delta_ready
            && coverage_ready
            && cross_market_ready
            && !cooling_down;
        let range_width_pct = if range_low > 0.0 {
            range_high / range_low - 1.0
        } else {
            0.0
        };
        let range_side =
            if side.is_none() && range_width_pct >= 0.003 && price <= range_low * 1.0003 {
                Some(Side::Buy)
            } else if side.is_none() && range_width_pct >= 0.003 && price >= range_high * 0.9997 {
                Some(Side::Sell)
            } else {
                None
            };
        let range_edge = range_side.is_some();

        let blockers = [
            (!warmed).then_some("warmup"),
            (!flat).then_some("position_open"),
            (side.is_none()).then_some("trend_regime"),
            (!pulled_back).then_some("pullback_depth"),
            (!reclaimed).then_some("price_reclaim"),
            (!delta_ready).then_some("delta_alignment"),
            (!coverage_ready).then_some("source_coverage"),
            (!cross_market_ready).then_some("spot_perp_delta"),
            cooling_down.then_some("cooldown"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        self.eval = Some(json!({
            "ts_ms":now_ms,
            "decision":if ready { "confirmed" } else if !warmed { "warmup" } else { "waiting" },
            "reason":if ready { "趋势回踩后价格回收，现货与永续主动流同向" } else { "等待趋势回踩通道逐级通过" },
            "price":price,
            "side":side.map(side_name),
            "slow_return_pct":slow_return,
            "slow_efficiency":slow_efficiency,
            "regime":regime,
            "recent_high":recent_high,
            "recent_low":recent_low,
            "pullback_extreme":self.pullback_extreme,
            "pullback_pct":match side { Some(Side::Buy) => recent_high / price - 1.0, Some(Side::Sell) => price / recent_low - 1.0, None => 0.0 },
            "required_pullback_pct":self.cfg.min_pullback_pct,
            "reclaim_pct":self.cfg.reclaim_pct,
            "delta_tier":delta_tier,
            "delta_direction":delta_direction,
            "source_coverage_complete":coverage,
            "spot_delta_usd":spot_delta,
            "perp_delta_usd":perp_delta,
            "cooldown_until_ms":self.cooldown_until_ms,
            "flat":flat,
            "warmed":warmed,
            "trade_eligible":ready,
            "blockers":blockers,
            "range_observation":{"active":range_edge,"side":range_side.map(side_name),"width_pct":range_width_pct,"trade_enabled":false,"reason":"长期与当前样本均未证明扣费后优势，仅记录候选"},
            "funnel":{"trend_regime":side.is_some(),"pullback_depth":pulled_back,"price_reclaim":reclaimed,"cross_market_flow":delta_ready && coverage_ready && cross_market_ready,"confirmed":ready}
        }));
        self.prices.push_back((now_ms, price));
        let mut out = Vec::new();
        if let Some(range_side) =
            range_side.filter(|_| now_ms >= self.range_observation_cooldown_until_ms)
        {
            self.range_observation_cooldown_until_ms = now_ms + self.cfg.cooldown_ms;
            out.push(Signal::new(SignalKind::Other, ts, "TacticalPullback", json!({
                "model":"range_edge_observation_v1","stage":"observation","profile":"range_edge_observation",
                "event_id":now_ms,"strength":"research","side":side_name(range_side),"price":price,"zone":price,
                "stop_anchor":match range_side { Side::Buy => price * 0.9975, Side::Sell => price * 1.0025 },
                "estimated_roundtrip_fee_bps":8.0,
                "observation":{"range_width_pct":range_width_pct,"range_high":range_high,"range_low":range_low,
                    "source_coverage_complete":coverage,"delta_tier":delta_tier,"delta_direction":delta_direction}
            })));
        }
        if ready {
            let side = side.expect("ready tactical side");
            self.cooldown_until_ms = now_ms + self.cfg.cooldown_ms;
            self.pullback_extreme = Some(price);
            out.push(Signal::new(SignalKind::Other, ts, "TacticalPullback", json!({
            "model":"flow_confirmed_trend_pullback_v1","stage":"confirmed","profile":"tactical_pullback",
                "event_id":now_ms,"strength":"balanced","side":side_name(side),"price":price,"zone":price,
                "stop_anchor":match side { Side::Buy => price * 0.9975, Side::Sell => price * 1.0025 },
                "trade_eligible":true,"estimated_roundtrip_fee_bps":8.0,"slow_return_pct":slow_return,
                "slow_efficiency":slow_efficiency,"pullback_pct":match side { Side::Buy => recent_high / price - 1.0, Side::Sell => price / recent_low - 1.0 },
                "reclaim_pct":self.cfg.reclaim_pct,"delta_tier":delta_tier,"delta_direction":delta_direction,
                "spot_delta_usd":spot_delta,"perp_delta_usd":perp_delta,"source_coverage_complete":coverage
            })));
        }
        out
    }
}

impl SignalPlugin for TacticalPullback {
    fn name(&self) -> &'static str {
        "TacticalPullback"
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
    fn confirms_only_after_pullback_reclaim_and_cross_market_flow() {
        let mut plugin = TacticalPullback::from_params(
            &json!({"warmup_ms":1800000,"price_window_ms":1800000,"min_pullback_pct":0.002,"reclaim_pct":0.0006}),
        );
        let mut ctx = Ctx::default();
        ctx.set_latest(Signal::new(
            SignalKind::TrendRegime,
            Timestamp::from_millis(0),
            "test",
            json!({"regime":"trend_up","slow_return_pct":0.006,"slow_efficiency":0.20}),
        ));
        ctx.set_latest(Signal::new(SignalKind::DeltaTier, Timestamp::from_millis(0), "test", json!({"tier":2,"direction":"buy","source_coverage_complete":true,"spot_delta_usd":1.0,"perp_delta_usd":2.0})));
        assert!(plugin.on_event(&trade(0, 100.0), &ctx).is_empty());
        assert!(plugin.on_event(&trade(10_000, 100.0), &ctx).is_empty());
        assert!(plugin.on_event(&trade(1_800_000, 99.7), &ctx).is_empty());
        let signal = plugin.on_event(&trade(1_810_000, 99.77), &ctx);
        assert_eq!(signal.len(), 1);
        assert_eq!(signal[0].payload["side"], "buy");
    }
}
