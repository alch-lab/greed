//! 日内过度延伸后的确认式均值回归。
//!
//! 这条腿不依赖盘口色带。它先要求 30 分钟价格、路径效率和 Binance 永续
//! 主动成交 Delta 同向过度延伸，再等待分钟级价格与 Delta 同时反转。这样既补上
//! “没有稳定挂单墙、但行情已经走过头”的机会，也避免直接逆势摸顶抄底。

use std::collections::VecDeque;

use serde_json::{json, Value as Json};
use tcore::{Ctx, Event, Exchange, Signal, SignalKind, SignalPlugin, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    pub bucket_ms: i64,
    pub window_buckets: usize,
    pub extension_return_pct: f64,
    pub min_efficiency: f64,
    pub min_delta_share: f64,
    pub confirm_delta_share: f64,
    pub confirmation_buckets: usize,
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
                .unwrap_or(60_000)
                .max(10_000),
            window_buckets: u("window_buckets", 30).max(10),
            extension_return_pct: f("extension_return_pct", 0.005).clamp(0.001, 0.05),
            min_efficiency: f("min_efficiency", 0.35).clamp(0.05, 1.0),
            min_delta_share: f("min_delta_share", 0.08).clamp(0.0, 0.95),
            confirm_delta_share: f("confirm_delta_share", 0.03).clamp(0.0, 0.95),
            confirmation_buckets: u("confirmation_buckets", 5).max(1),
            cooldown_buckets: u("cooldown_buckets", 60).max(1),
            stop_buffer_pct: f("stop_buffer_pct", 0.0005).clamp(0.0, 0.01),
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

    fn add(&mut self, price: f64, volume: f64, delta: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += volume;
        self.delta += delta;
    }
}

#[derive(Debug, Clone)]
struct Setup {
    /// +1 表示向上过度延伸、等待做空；-1 表示向下延伸、等待做多。
    extension_side: i8,
    event_id: i64,
    expires_at_ms: i64,
    extreme: f64,
    confirm_volume: f64,
    confirm_delta: f64,
    extension_return: f64,
    extension_efficiency: f64,
    extension_delta_share: f64,
}

pub struct IntradayExtensionReversion {
    cfg: Config,
    current: Option<Bucket>,
    history: VecDeque<Bucket>,
    setup: Option<Setup>,
    last_confirmed_ms: i64,
    eval: Option<Json>,
}

impl IntradayExtensionReversion {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            current: None,
            history: VecDeque::new(),
            setup: None,
            last_confirmed_ms: i64::MIN / 2,
            eval: None,
        }
    }

    pub fn from_params(p: &Json) -> Self {
        Self::new(Config::from_params(p))
    }

    fn reset_on_gap(&mut self, next_start_ms: i64) {
        let Some(last) = self.history.back() else {
            return;
        };
        if next_start_ms - last.start_ms > self.cfg.bucket_ms * 2 {
            self.history.clear();
            self.setup = None;
        }
    }

    fn evaluate(&mut self, b: &Bucket) -> Vec<Signal> {
        self.reset_on_gap(b.start_ms);
        let ts = Timestamp::from_millis(b.start_ms + self.cfg.bucket_ms);
        let required = self.cfg.window_buckets.saturating_sub(1);
        if self.history.len() < required {
            self.eval = Some(json!({
                "ts_ms": ts.as_millis(), "decision":"warmup",
                "reason":"日内过度延伸窗口预热中", "observed":self.history.len() + 1,
                "required":self.cfg.window_buckets
            }));
            return vec![];
        }

        let window = self
            .history
            .iter()
            .rev()
            .take(required)
            .rev()
            .chain(std::iter::once(b))
            .collect::<Vec<_>>();
        let first = window.first().expect("non-empty extension window");
        let return_pct = b.close / first.open - 1.0;
        let path = window
            .windows(2)
            .map(|w| (w[1].close - w[0].close).abs())
            .sum::<f64>();
        let efficiency = if path > 0.0 {
            (b.close - first.open).abs() / path
        } else {
            0.0
        };
        let volume = window.iter().map(|x| x.volume).sum::<f64>();
        let delta = window.iter().map(|x| x.delta).sum::<f64>();
        let delta_share = if volume > 0.0 { delta / volume } else { 0.0 };

        let mut confirmed = None;
        let mut expired = false;
        if let Some(setup) = self.setup.as_mut() {
            if ts.as_millis() > setup.expires_at_ms {
                expired = true;
            } else {
                setup.confirm_volume += b.volume;
                setup.confirm_delta += b.delta;
                setup.extreme = if setup.extension_side > 0 {
                    setup.extreme.max(b.high)
                } else {
                    setup.extreme.min(b.low)
                };
                let confirm_share = if setup.confirm_volume > 0.0 {
                    setup.confirm_delta / setup.confirm_volume
                } else {
                    0.0
                };
                let previous = self.history.back().expect("history is warm");
                let price_reversed = if setup.extension_side > 0 {
                    b.close < previous.low
                } else {
                    b.close > previous.high
                };
                let delta_reversed =
                    -(setup.extension_side as f64) * confirm_share >= self.cfg.confirm_delta_share;
                if b.start_ms > setup.event_id && price_reversed && delta_reversed {
                    confirmed = Some((setup.clone(), confirm_share));
                }
            }
        }
        if expired {
            self.setup = None;
        }

        if let Some((setup, confirm_share)) = confirmed {
            self.setup = None;
            self.last_confirmed_ms = ts.as_millis();
            let side = if setup.extension_side > 0 {
                "sell"
            } else {
                "buy"
            };
            let stop_anchor = if setup.extension_side > 0 {
                setup.extreme * (1.0 + self.cfg.stop_buffer_pct)
            } else {
                setup.extreme * (1.0 - self.cfg.stop_buffer_pct)
            };
            self.eval = Some(json!({
                "ts_ms":ts.as_millis(), "decision":"confirmed",
                "reason":"30m 过度延伸后价格与 Delta 同步反转",
                "profile":"intraday_reversion", "side":side, "price":b.close,
                "event_id":setup.event_id, "return_pct":setup.extension_return,
                "efficiency":setup.extension_efficiency,
                "delta_share":setup.extension_delta_share,
                "confirm_delta_share":confirm_share, "stop_anchor":stop_anchor
            }));
            return vec![Signal::new(
                SignalKind::Other,
                ts,
                "IntradayExtensionReversion",
                json!({
                    "stage":"confirmed", "profile":"intraday_reversion", "side":side,
                    "price":b.close, "stop_anchor":stop_anchor, "event_id":setup.event_id,
                    "force_market":true, "return_pct":setup.extension_return,
                    "efficiency":setup.extension_efficiency,
                    "delta_share":setup.extension_delta_share,
                    "confirm_delta_share":confirm_share
                }),
            )];
        }

        let extension_side = if return_pct >= self.cfg.extension_return_pct
            && efficiency >= self.cfg.min_efficiency
            && delta_share >= self.cfg.min_delta_share
        {
            1
        } else if return_pct <= -self.cfg.extension_return_pct
            && efficiency >= self.cfg.min_efficiency
            && delta_share <= -self.cfg.min_delta_share
        {
            -1
        } else {
            0
        };
        let cooldown_ms = self.cfg.cooldown_buckets as i64 * self.cfg.bucket_ms;
        if extension_side != 0
            && self.setup.is_none()
            && ts.as_millis() - self.last_confirmed_ms >= cooldown_ms
        {
            self.setup = Some(Setup {
                extension_side,
                // 与 10 秒 OrderFlowExhaustion 的整秒 event_id 错开，避免影子归因碰撞。
                event_id: b.start_ms + 1,
                expires_at_ms: ts.as_millis()
                    + self.cfg.confirmation_buckets as i64 * self.cfg.bucket_ms,
                extreme: if extension_side > 0 { b.high } else { b.low },
                confirm_volume: 0.0,
                confirm_delta: 0.0,
                extension_return: return_pct,
                extension_efficiency: efficiency,
                extension_delta_share: delta_share,
            });
        }

        let pending = self.setup.as_ref().map(|s| {
            json!({
                "event_id":s.event_id,
                "side":if s.extension_side > 0 { "sell" } else { "buy" },
                "expires_at_ms":s.expires_at_ms,
                "extreme":s.extreme
            })
        });
        self.eval = Some(json!({
            "ts_ms":ts.as_millis(),
            "decision":if pending.is_some() { "waiting_reversal" } else { "none" },
            "reason":if pending.is_some() { "已过度延伸，等待分钟价格与 Delta 反转" } else { "等待 30m 过度延伸" },
            "return_pct":return_pct, "efficiency":efficiency,
            "delta_usd":delta, "delta_share":delta_share, "pending":pending,
            "thresholds":{
                "return_pct":self.cfg.extension_return_pct,
                "min_efficiency":self.cfg.min_efficiency,
                "min_delta_share":self.cfg.min_delta_share,
                "confirm_delta_share":self.cfg.confirm_delta_share
            }
        }));
        vec![]
    }
}

impl SignalPlugin for IntradayExtensionReversion {
    fn name(&self) -> &'static str {
        "IntradayExtensionReversion"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        let Event::Trade(t) = ev else { return vec![] };
        if t.exchange != Exchange::BinanceFutures {
            return vec![];
        }
        let start = t.ts.as_millis().div_euclid(self.cfg.bucket_ms) * self.cfg.bucket_ms;
        let mut signals = Vec::new();
        if self.current.as_ref().is_some_and(|b| b.start_ms != start) {
            let completed = self.current.take().expect("current extension bucket");
            signals = self.evaluate(&completed);
            self.history.push_back(completed);
            while self.history.len() > self.cfg.window_buckets {
                self.history.pop_front();
            }
        }
        let b = self
            .current
            .get_or_insert_with(|| Bucket::new(start, t.price.to_f64()));
        b.add(t.price.to_f64(), t.notional(), t.signed_notional());
        signals
    }

    fn eval_note(&self) -> Option<Json> {
        self.eval.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::{Price, Qty, Symbol, Trade};

    fn trade(minute: i64, second: i64, price: f64, buy: bool, qty: f64) -> Event {
        Event::Trade(Trade {
            ts: Timestamp::from_millis(minute * 60_000 + second * 1_000),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker: !buy,
        })
    }

    #[test]
    fn extension_needs_price_and_delta_reversal() {
        let cfg = Config::from_params(&json!({
            "window_buckets":10, "extension_return_pct":0.005,
            "min_efficiency":0.2, "min_delta_share":0.05,
            "confirm_delta_share":0.02, "confirmation_buckets":3
        }));
        let mut s = IntradayExtensionReversion::new(cfg);
        let ctx = Ctx::default();
        for minute in 0..10 {
            let price = 100.0 + minute as f64 * 0.08;
            s.on_event(&trade(minute, 0, price, true, 10.0), &ctx);
            s.on_event(&trade(minute, 50, price + 0.04, true, 10.0), &ctx);
        }
        // 结算延伸桶并建立 setup。
        assert!(s
            .on_event(&trade(10, 0, 100.80, true, 10.0), &ctx)
            .is_empty());
        // 反向成交但价格尚未跌破上一分钟低点，不能确认。
        s.on_event(&trade(10, 50, 100.79, false, 100.0), &ctx);
        assert!(s
            .on_event(&trade(11, 0, 100.78, false, 100.0), &ctx)
            .is_empty());
        // 跌破上一桶低点且确认期 Delta 转负，生成做空信号。
        s.on_event(&trade(11, 50, 100.60, false, 100.0), &ctx);
        let out = s.on_event(&trade(12, 0, 100.59, false, 10.0), &ctx);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["side"], "sell");
        assert_eq!(out[0].payload["profile"], "intraday_reversion");
    }
}
