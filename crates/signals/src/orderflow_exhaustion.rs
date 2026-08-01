use std::collections::VecDeque;

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Side, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket_ms: i64,
    pub baseline_buckets: usize,
    pub location_buckets: usize,
    pub min_baseline_buckets: usize,
    pub min_volume_ratio: f64,
    pub min_delta_share: f64,
    pub confirm_delta_share: f64,
    pub max_efficiency: f64,
    pub min_sweep_pct: f64,
    pub min_vwap_deviation_pct: f64,
    pub require_sweep: bool,
    pub confirm_buckets: usize,
    pub second_entry_buckets: usize,
    pub cooldown_buckets: usize,
    pub stop_buffer_pct: f64,
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
            baseline_buckets: u("baseline_buckets", 60).max(10),
            location_buckets: u("location_buckets", 90).max(10),
            min_baseline_buckets: u("min_baseline_buckets", 20).max(5),
            min_volume_ratio: f("min_volume_ratio", 1.8),
            min_delta_share: f("min_delta_share", 0.22),
            confirm_delta_share: f("confirm_delta_share", 0.10),
            max_efficiency: f("max_efficiency", 0.32),
            min_sweep_pct: f("min_sweep_pct", 0.00015),
            min_vwap_deviation_pct: f("min_vwap_deviation_pct", 0.0025),
            require_sweep: p
                .get("require_sweep")
                .and_then(Json::as_bool)
                .unwrap_or(true),
            confirm_buckets: u("confirm_buckets", 6),
            second_entry_buckets: u("second_entry_buckets", 18),
            cooldown_buckets: u("cooldown_buckets", 12),
            stop_buffer_pct: f("stop_buffer_pct", 0.00035),
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
    side: Side,
    zone: f64,
    stop_anchor: f64,
    expires_at: i64,
    confirmed: bool,
}

/// 作者模型的量化实现：不把“反转砖”当作力竭本身，而是在原始成交上衡量
/// effort/result，并把入场拆成极值、确认、二次失败三类。
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

    fn evaluate(&mut self, b: &Bucket) -> Vec<Signal> {
        let ts = Timestamp::from_millis(b.start_ms + self.cfg.bucket_ms);
        let observed = self.history.iter().filter(|b| b.volume > 0.0).count();
        if observed < self.cfg.min_baseline_buckets {
            self.eval = Some(
                json!({"ts_ms":ts.as_millis(),"decision":"warmup","reason":"真实订单流基线积累中","buckets":observed,"required":self.cfg.min_baseline_buckets}),
            );
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

        let pressure_side = if delta_share >= self.cfg.min_delta_share {
            Some(Side::Buy)
        } else if delta_share <= -self.cfg.min_delta_share {
            Some(Side::Sell)
        } else {
            None
        };
        let no_result = efficiency <= self.cfg.max_efficiency
            || pressure_side == Some(Side::Buy) && b.close <= b.open
            || pressure_side == Some(Side::Sell) && b.close >= b.open;
        let vwap_high = !self.cfg.require_sweep && vwap_dev >= self.cfg.min_vwap_deviation_pct;
        let vwap_low = !self.cfg.require_sweep && vwap_dev <= -self.cfg.min_vwap_deviation_pct;
        let location_side = if swept_high || vwap_high {
            Some(Side::Sell)
        } else if swept_low || vwap_low {
            Some(Side::Buy)
        } else {
            None
        };
        let exhausted_side = pressure_side.map(Side::opposite);
        let absorption = volume_ratio >= self.cfg.min_volume_ratio
            && no_result
            && exhausted_side.is_some()
            && exhausted_side == location_side;

        let mut decision = "none";
        let mut reason = "等待位置、放量和价格停滞同时成立";
        let mut out = Vec::new();
        if let Some(setup) = self.setup.clone() {
            if b.start_ms > setup.expires_at {
                self.setup = None;
            } else {
                let reverse_delta = match setup.side {
                    Side::Buy => delta_share >= self.cfg.confirm_delta_share,
                    Side::Sell => delta_share <= -self.cfg.confirm_delta_share,
                };
                let reverse_price = match setup.side {
                    Side::Buy => b.close > b.open,
                    Side::Sell => b.close < b.open,
                };
                if !setup.confirmed && reverse_delta && reverse_price {
                    decision = "confirmed";
                    reason = "Delta 已翻转且价格开始离开力竭区";
                    out.push(self.signal(
                        ts,
                        "confirmed",
                        &setup,
                        b.close,
                        volume_ratio,
                        delta_share,
                        efficiency,
                        vwap_dev,
                        oi_change,
                    ));
                    self.setup = Some(Setup {
                        confirmed: true,
                        expires_at: b.start_ms
                            + self.cfg.second_entry_buckets as i64 * self.cfg.bucket_ms,
                        ..setup
                    });
                    self.cooldown_until =
                        b.start_ms + self.cfg.cooldown_buckets as i64 * self.cfg.bucket_ms;
                } else if setup.confirmed
                    && absorption
                    && exhausted_side == Some(setup.side)
                    && (b.close / setup.zone - 1.0).abs() <= 0.003
                {
                    decision = "second";
                    reason = "同一区域二次进攻失败";
                    out.push(self.signal(
                        ts,
                        "second",
                        &setup,
                        b.close,
                        volume_ratio,
                        delta_share,
                        efficiency,
                        vwap_dev,
                        oi_change,
                    ));
                    self.setup = None;
                    self.cooldown_until =
                        b.start_ms + self.cfg.cooldown_buckets as i64 * self.cfg.bucket_ms;
                }
            }
        }

        if absorption
            && b.start_ms >= self.cooldown_until
            && self.setup.as_ref().is_none_or(|s| s.confirmed)
        {
            let side = exhausted_side.expect("absorption has side");
            let stop_anchor = if side == Side::Buy {
                b.low * (1.0 - self.cfg.stop_buffer_pct)
            } else {
                b.high * (1.0 + self.cfg.stop_buffer_pct)
            };
            let setup = Setup {
                side,
                zone: b.close,
                stop_anchor,
                expires_at: b.start_ms + self.cfg.confirm_buckets as i64 * self.cfg.bucket_ms,
                confirmed: false,
            };
            decision = "extreme";
            reason = "关键位置出现主动成交力竭";
            out.push(self.signal(
                ts,
                "extreme",
                &setup,
                b.close,
                volume_ratio,
                delta_share,
                efficiency,
                vwap_dev,
                oi_change,
            ));
            self.setup = Some(setup);
            self.cooldown_until =
                b.start_ms + self.cfg.cooldown_buckets as i64 * self.cfg.bucket_ms;
        }

        self.eval = Some(json!({
            "ts_ms": ts.as_millis(), "price": b.close, "decision": decision, "reason": reason,
            "volume_usd": b.volume, "volume_ratio": volume_ratio, "delta_usd": b.delta,
            "delta_share": delta_share, "efficiency": efficiency, "vwap": vwap,
            "vwap_deviation_pct": vwap_dev, "swept_high": swept_high, "swept_low": swept_low,
            "require_sweep": self.cfg.require_sweep,
            "prior_high": prior_high, "prior_low": prior_low, "oi_change_pct": oi_change,
            "book_imbalance": self.book_imbalance
        }));
        out
    }

    fn signal(
        &self,
        ts: Timestamp,
        stage: &str,
        setup: &Setup,
        price: f64,
        volume_ratio: f64,
        delta_share: f64,
        efficiency: f64,
        vwap_dev: f64,
        oi_change: Option<f64>,
    ) -> Signal {
        Signal::new(
            SignalKind::Other,
            ts,
            "OrderFlowExhaustion",
            json!({
                "model":"orderflow_exhaustion_v2", "stage":stage,
                "side":match setup.side { Side::Buy => "buy", Side::Sell => "sell" },
                "price":price, "zone":setup.zone, "stop_anchor":setup.stop_anchor,
                "volume_ratio":volume_ratio, "delta_share":delta_share,
                "efficiency":efficiency, "vwap_deviation_pct":vwap_dev,
                "oi_change_pct":oi_change, "book_imbalance":self.book_imbalance
            }),
        )
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

    #[test]
    fn detects_high_sweep_buy_absorption() {
        let mut cfg = Config::from_params(&json!({}));
        cfg.bucket_ms = 1_000;
        cfg.min_baseline_buckets = 5;
        cfg.baseline_buckets = 5;
        cfg.location_buckets = 5;
        cfg.min_volume_ratio = 1.5;
        cfg.min_vwap_deviation_pct = 9.0;
        let mut s = OrderFlowExhaustion::new(cfg);
        let ctx = Ctx::default();
        for i in 0..6 {
            s.on_event(&trade(i * 1_000, 100.0 + i as f64 * 0.01, true, 1.0), &ctx);
        }
        s.on_event(&trade(6_000, 100.20, true, 5.0), &ctx);
        s.on_event(&trade(6_500, 100.02, true, 5.0), &ctx);
        let out = s.on_event(&trade(7_000, 100.01, false, 1.0), &ctx);
        assert!(
            out.iter().any(|x| x.payload["stage"] == "extreme"),
            "{out:?} {:?}",
            s.eval
        );
    }
}
