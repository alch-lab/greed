//! EMA 交叉信号：双均线趋势跟踪。
//!
//! 按固定时间间隔（如 1 分钟）采样最新价，计算快速/慢速 EMA，
//! 金叉 → CrossUp 信号，死叉 → CrossDown 信号。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

/// EMA 交叉信号插件。
pub struct EmaCross {
    /// 采样间隔（毫秒），默认 1 分钟
    bar_ms: i64,
    /// 快速 EMA 周期（ bars ）
    fast_period: usize,
    /// 慢速 EMA 周期（ bars ）
    slow_period: usize,

    // ---- 内部状态 ----
    last_bar_ts: Option<i64>,
    last_price: Option<f64>,
    fast_ema: Option<f64>,
    slow_ema: Option<f64>,
    prices: Vec<f64>, // 用于启动期 SMA
}

impl EmaCross {
    pub fn new(bar_ms: i64, fast_period: usize, slow_period: usize) -> Self {
        Self {
            bar_ms,
            fast_period,
            slow_period,
            last_bar_ts: None,
            last_price: None,
            fast_ema: None,
            slow_ema: None,
            prices: Vec::new(),
        }
    }

    fn update_ema(prev: Option<f64>, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        match prev {
            Some(p) => price * k + p * (1.0 - k),
            None => price,
        }
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();
        self.last_price = Some(price);

        // 检查是否需要新 bar
        let need_bar = match self.last_bar_ts {
            None => true,
            Some(last) => ts - last >= self.bar_ms,
        };

        if !need_bar {
            return Vec::new();
        }

        self.last_bar_ts = Some(ts);

        // 启动期：用 SMA 初始化
        if self.prices.len() < self.slow_period {
            self.prices.push(price);
            if self.prices.len() == self.slow_period {
                let sum: f64 = self.prices.iter().sum();
                self.fast_ema = Some(sum / self.prices.len() as f64);
                self.slow_ema = self.fast_ema;
            }
            return Vec::new();
        }

        let prev_fast = self.fast_ema.unwrap();
        let prev_slow = self.slow_ema.unwrap();

        self.fast_ema = Some(Self::update_ema(self.fast_ema, price, self.fast_period));
        self.slow_ema = Some(Self::update_ema(self.slow_ema, price, self.slow_period));

        let new_fast = self.fast_ema.unwrap();
        let new_slow = self.slow_ema.unwrap();

        let mut sigs = Vec::new();
        if prev_fast <= prev_slow && new_fast > new_slow {
            sigs.push(Signal::new(
                SignalKind::TrendRegime,
                t.ts,
                self.name(),
                serde_json::json!({"direction": "up", "fast": new_fast, "slow": new_slow, "price": price}),
            ));
        } else if prev_fast >= prev_slow && new_fast < new_slow {
            sigs.push(Signal::new(
                SignalKind::TrendRegime,
                t.ts,
                self.name(),
                serde_json::json!({"direction": "down", "fast": new_fast, "slow": new_slow, "price": price}),
            ));
        }
        sigs
    }
}

impl SignalPlugin for EmaCross {
    fn name(&self) -> &'static str {
        "EmaCross"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl EmaCross {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("bar_ms", 60_000.0) as i64,
            g("fast_period", 20.0) as usize,
            g("slow_period", 50.0) as usize,
        )
    }
}
