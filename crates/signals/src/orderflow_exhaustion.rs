use std::collections::VecDeque;

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Side, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket_ms: i64,
    pub baseline_buckets: usize,
    pub location_buckets: usize,
    pub min_baseline_buckets: usize,
    pub classic_volume_ratio: f64,
    pub strong_volume_ratio: f64,
    pub quality_volume_ratio: f64,
    pub balanced_max_volume_ratio: f64,
    pub min_delta_share: f64,
    pub strong_delta_share: f64,
    pub confirm_delta_share: f64,
    pub max_efficiency: f64,
    pub min_sweep_pct: f64,
    pub min_vwap_deviation_pct: f64,
    pub strong_confirm_buckets: usize,
    pub weak_confirm_buckets: usize,
    pub cluster_cooldown_buckets: usize,
    pub stop_buffer_pct: f64,
    pub balanced_context_score: u8,
    pub quality_context_score: u8,
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
            quality_volume_ratio: f("quality_volume_ratio", 3.00).max(1.0),
            balanced_max_volume_ratio: f("balanced_max_volume_ratio", 2.50).max(1.0),
            min_delta_share: f("min_delta_share", 0.15).clamp(0.01, 0.95),
            strong_delta_share: f("strong_delta_share", 0.35).clamp(0.01, 0.95),
            confirm_delta_share: f("confirm_delta_share", 0.08).clamp(0.01, 0.95),
            max_efficiency: f("max_efficiency", 0.45).clamp(0.01, 1.0),
            min_sweep_pct: f("min_sweep_pct", 0.00035).max(0.0),
            min_vwap_deviation_pct: f("min_vwap_deviation_pct", 0.0030).max(0.0),
            strong_confirm_buckets: u("strong_confirm_buckets", 18).max(1),
            weak_confirm_buckets: u("weak_confirm_buckets", 30).max(1),
            cluster_cooldown_buckets: u("cluster_cooldown_buckets", 60).max(1),
            stop_buffer_pct: f("stop_buffer_pct", 0.00035).max(0.0),
            balanced_context_score: u("balanced_context_score", 2).min(4) as u8,
            quality_context_score: u("quality_context_score", 3).min(4) as u8,
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
    stop_anchor: f64,
    event_volume: f64,
    event_volume_ratio: f64,
    event_delta_share: f64,
    event_efficiency: f64,
    location_confirmed: bool,
    context_score: u8,
    context_reasons: Vec<&'static str>,
    expires_at: i64,
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
    cooldown_until: i64,
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
            cooldown_until: 0,
            session_day: i64::MIN,
            session_notional: 0.0,
            session_pv: 0.0,
            latest_oi: None,
            previous_oi: None,
            book_imbalance: None,
            eval: None,
        }
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

    fn profile(&self, setup: &Setup) -> &'static str {
        if setup.strength == "strong"
            && setup.event_volume_ratio >= self.cfg.quality_volume_ratio
            && setup.location_confirmed
            && setup.context_score >= self.cfg.quality_context_score
        {
            "quality"
        } else if setup.context_score >= self.cfg.balanced_context_score
            && setup.location_confirmed
            && setup.event_volume_ratio < self.cfg.balanced_max_volume_ratio
        {
            "balanced"
        } else if setup.context_score > 0 {
            "high_frequency"
        } else if setup.strength == "strong" {
            "strength"
        } else {
            "classic"
        }
    }

    fn signal(&self, ts: Timestamp, setup: &Setup, price: f64, delta_share: f64) -> Signal {
        let profile = self.profile(setup);
        Signal::new(
            SignalKind::Other,
            ts,
            "OrderFlowExhaustion",
            json!({
                "model":"orderflow_exhaustion_v3",
                "stage":"confirmed",
                "profile":profile,
                "event_id":setup.event_id,
                "strength":setup.strength,
                "side":match setup.side { Side::Buy => "buy", Side::Sell => "sell" },
                "price":price,
                "zone":setup.event_price,
                "stop_anchor":setup.stop_anchor,
                "volume_usd":setup.event_volume,
                "volume_ratio":setup.event_volume_ratio,
                "event_delta_share":setup.event_delta_share,
                "confirm_delta_share":delta_share,
                "efficiency":setup.event_efficiency,
                "location_confirmed":setup.location_confirmed,
                "context_score":setup.context_score,
                "context_reasons":setup.context_reasons,
                "book_imbalance":self.book_imbalance
            }),
        )
    }

    fn evaluate(&mut self, b: &Bucket) -> Vec<Signal> {
        let ts = Timestamp::from_millis(b.start_ms + self.cfg.bucket_ms);
        let observed = self.history.iter().filter(|x| x.volume > 0.0).count();
        if observed < self.cfg.min_baseline_buckets {
            self.eval = Some(json!({
                "ts_ms":ts.as_millis(), "decision":"warmup", "reason":"真实 10 秒订单流基线积累中",
                "buckets":observed, "required":self.cfg.min_baseline_buckets,
                "bucket_ms":self.cfg.bucket_ms
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

        if let Some(setup) = self.setup.clone() {
            if b.start_ms > setup.expires_at {
                decision = "expired";
                reason = "放量事件在确认窗口内未出现 Delta 反转";
                self.setup = None;
            } else {
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
                    reason = match profile {
                        "quality" => "Delta 反转确认，强放量与多因子 context 同时成立",
                        "balanced" => "Delta 反转确认，context 达到均衡执行标准",
                        "high_frequency" => "Delta 反转确认，仅通过宽松 context",
                        "strength" => "强放量已确认，但 context 不足",
                        _ => "经典放量力竭得到 Delta 反转确认",
                    };
                    out.push(self.signal(ts, &setup, b.close, delta_share));
                    self.setup = None;
                    self.cooldown_until =
                        b.start_ms + self.cfg.cluster_cooldown_buckets as i64 * self.cfg.bucket_ms;
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

        if self.setup.is_none() && volume_event && decision == "none" {
            if b.start_ms < self.cooldown_until {
                decision = "deduplicated";
                reason = "同一放量事件簇仍在冷却，已去重";
            } else {
                let side = exhausted_side.expect("volume event has pressure side");
                let location = match side {
                    Side::Buy => swept_low || vwap_dev <= -self.cfg.min_vwap_deviation_pct,
                    Side::Sell => swept_high || vwap_dev >= self.cfg.min_vwap_deviation_pct,
                };
                let book_support = self.book_imbalance.is_some_and(|imbalance| match side {
                    Side::Buy => imbalance > 0.10,
                    Side::Sell => imbalance < -0.10,
                });
                let oi_support = oi_change.is_some_and(|v| v.abs() >= 0.0005);
                let stalled = efficiency <= self.cfg.max_efficiency * 0.65;
                let mut context_score = 0u8;
                let mut context_reasons = Vec::new();
                if location {
                    context_score += 1;
                    context_reasons.push("location");
                }
                if stalled {
                    context_score += 1;
                    context_reasons.push("absorption");
                }
                if book_support {
                    context_score += 1;
                    context_reasons.push("book");
                }
                if oi_support {
                    context_score += 1;
                    context_reasons.push("oi");
                }
                let strong = volume_ratio >= self.cfg.strong_volume_ratio
                    || delta_share.abs() >= self.cfg.strong_delta_share;
                let confirm_buckets = if strong {
                    self.cfg.strong_confirm_buckets
                } else {
                    self.cfg.weak_confirm_buckets
                };
                let stop_anchor = match side {
                    Side::Buy => b.low * (1.0 - self.cfg.stop_buffer_pct),
                    Side::Sell => b.high * (1.0 + self.cfg.stop_buffer_pct),
                };
                self.setup = Some(Setup {
                    event_id: b.start_ms,
                    side,
                    strength: if strong { "strong" } else { "weak" },
                    event_price: b.close,
                    stop_anchor,
                    event_volume: b.volume,
                    event_volume_ratio: volume_ratio,
                    event_delta_share: delta_share,
                    event_efficiency: efficiency,
                    location_confirmed: location,
                    context_score,
                    context_reasons,
                    expires_at: b.start_ms + confirm_buckets as i64 * self.cfg.bucket_ms,
                });
                decision = "volume_event";
                reason = if strong {
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
                "expires_at_ms":s.expires_at,
            "context_score":s.context_score,
            "location_confirmed":s.location_confirmed,
                "context_reasons":s.context_reasons,
                "volume_ratio":s.event_volume_ratio,
                "event_delta_share":s.event_delta_share
            })
        });
        let confirmed_profile = out.first().and_then(|s| s.payload.get("profile")).cloned();
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
                "confirmed":!out.is_empty()
            },
            "pending":pending, "profile":confirmed_profile
        }));
        out
    }
}

impl SignalPlugin for OrderFlowExhaustion {
    fn name(&self) -> &'static str {
        "OrderFlowExhaustion"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Oi(oi) => {
                self.previous_oi = self.latest_oi;
                self.latest_oi = Some(oi.oi_usd);
                vec![]
            }
            Event::Book(book) => {
                let bid = book.bid_qty_within(0.002);
                let ask = book.ask_qty_within(0.002);
                self.book_imbalance = (bid + ask > 0.0).then_some((bid - ask) / (bid + ask));
                vec![]
            }
            Event::Trade(t) => {
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
                    signals = self.evaluate(&completed);
                    self.history.push_back(completed);
                    while self.history.len()
                        > self.cfg.location_buckets.max(self.cfg.baseline_buckets)
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
            Event::Funding(_) | Event::Timer(_) => vec![],
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

    fn setup(volume_ratio: f64, location_confirmed: bool, context_score: u8) -> Setup {
        Setup {
            event_id: 1,
            side: Side::Sell,
            strength: "strong",
            event_price: 100.0,
            stop_anchor: 101.0,
            event_volume: 1_000.0,
            event_volume_ratio: volume_ratio,
            event_delta_share: 0.5,
            event_efficiency: 0.1,
            location_confirmed,
            context_score,
            context_reasons: vec![],
            expires_at: 10,
        }
    }

    #[test]
    fn profile_keeps_middle_volume_band_observational() {
        let s = test_signal();
        assert_eq!(s.profile(&setup(2.2, true, 3)), "balanced");
        assert_eq!(s.profile(&setup(2.7, true, 3)), "high_frequency");
        assert_eq!(s.profile(&setup(3.2, true, 3)), "quality");
        assert_eq!(s.profile(&setup(2.2, false, 3)), "high_frequency");
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
