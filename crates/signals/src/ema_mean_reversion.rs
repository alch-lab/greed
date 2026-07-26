//! EMA 均值回归信号：价格偏离 EMA 时产生回归信号。
//!
//! 按固定时间间隔采样最新价，计算 EMA，
//! 当 price/ema - 1 > threshold 时产生 overbought 信号（预期回归下跌）
//! 当 price/ema - 1 < -threshold 时产生 oversold 信号（预期回归上涨）

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct EmaMeanReversion {
    bar_ms: i64,
    ema_period: usize,
    deviation_threshold: f64,

    last_bar_ts: Option<i64>,
    ema: Option<f64>,
    prices: Vec<f64>,
    last_deviation: f64,
    last_signal_dir: Option<String>, // 避免同一bar重复信号
}

impl EmaMeanReversion {
    pub fn new(bar_ms: i64, ema_period: usize, deviation_threshold: f64) -> Self {
        Self {
            bar_ms,
            ema_period,
            deviation_threshold,
            last_bar_ts: None,
            ema: None,
            prices: Vec::new(),
            last_deviation: 0.0,
            last_signal_dir: None,
        }
    }

    fn update_ema(prev: f64, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        price * k + prev * (1.0 - k)
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();

        let need_bar = match self.last_bar_ts {
            None => true,
            Some(last) => ts - last >= self.bar_ms,
        };

        if !need_bar {
            return Vec::new();
        }

        self.last_bar_ts = Some(ts);

        // 启动期：用 SMA 初始化
        if self.prices.len() < self.ema_period {
            self.prices.push(price);
            if self.prices.len() == self.ema_period {
                let sum: f64 = self.prices.iter().sum();
                self.ema = Some(sum / self.prices.len() as f64);
            }
            return Vec::new();
        }

        let prev_ema = self.ema.unwrap();
        let new_ema = Self::update_ema(prev_ema, price, self.ema_period);
        self.ema = Some(new_ema);

        let deviation = (price - new_ema) / new_ema;
        self.last_deviation = deviation;

        let mut sigs = Vec::new();

        // 偏离超过正阈值 → overbought（预期回归下跌，做空）
        if deviation > self.deviation_threshold {
            if self.last_signal_dir.as_ref() != Some(&"overbought".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
                    serde_json::json!({
                        "regime": "overbought",
                        "price": price,
                        "ema": new_ema,
                        "deviation": deviation,
                    }),
                ));
                self.last_signal_dir = Some("overbought".to_string());
            }
        }
        // 偏离超过负阈值 → oversold（预期回归上涨，做多）
        else if deviation < -self.deviation_threshold {
            if self.last_signal_dir.as_ref() != Some(&"oversold".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
                    serde_json::json!({
                        "regime": "oversold",
                        "price": price,
                        "ema": new_ema,
                        "deviation": deviation,
                    }),
                ));
                self.last_signal_dir = Some("oversold".to_string());
            }
        }
        // 回到区间内 → 重置信号方向记忆（允许下次再次触发）
        else {
            self.last_signal_dir = None;
        }

        sigs
    }
}

impl SignalPlugin for EmaMeanReversion {
    fn name(&self) -> &'static str {
        "EmaMeanReversion"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl EmaMeanReversion {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("bar_ms", 300_000.0) as i64,      // 默认 5 分钟
            g("ema_period", 20.0) as usize,
            g("deviation_threshold", 0.015),     // 默认 1.5%
        )
    }
}
