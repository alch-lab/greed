//! 双时间框架顺势均值回归
//!
//! 大趋势向上 → 只在 5 分钟超卖时做多（逢低做多）
//! 大趋势向下 → 只在 5 分钟超买时做空（逢高做空）
//! 大趋势不确定 → 双向都做

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct DualTfMeanReversion {
    fast_bar_ms: i64,
    slow_bar_ms: i64,
    fast_ema_p: usize,
    slow_ema_p: usize,
    deviation_threshold: f64,

    // 可选过滤器（路径 A）：ATR 爆发抑制 + RSI 动量守卫；0 = 关闭
    atr_period: usize,
    atr_avg_period: usize,
    atr_max_mult: f64,
    rsi_period: usize,
    rsi_no_long_above: f64,
    rsi_no_short_below: f64,

    // 快速 bar（5分钟）
    fast_cur_idx: Option<i64>,
    fast_open: f64, fast_high: f64, fast_low: f64, fast_close: f64,
    fast_bars: Vec<f64>, // close prices for fast EMA
    fast_ema: f64,

    // ATR/RSI 状态（基于已收快速 bar）
    prev_fast_close: Option<f64>,
    atr: Option<f64>,          // Wilder ATR
    atr_hist: Vec<f64>,        // ATR 样本（算近期平均）
    rsi_avg_gain: Option<f64>,
    rsi_avg_loss: Option<f64>,
    rsi_count: usize,

    // 慢速 bar（1小时）
    slow_cur_idx: Option<i64>,
    slow_open: f64, slow_high: f64, slow_low: f64, slow_close: f64,
    slow_bars: Vec<f64>,
    slow_ema: f64,

    last_signal_dir: Option<String>,
}

impl DualTfMeanReversion {
    pub fn new(fast_bar_ms: i64, slow_bar_ms: i64, fast_ema_p: usize, slow_ema_p: usize, deviation: f64) -> Self {
        Self {
            fast_bar_ms, slow_bar_ms, fast_ema_p, slow_ema_p, deviation_threshold: deviation,
            atr_period: 0, atr_avg_period: 20, atr_max_mult: 1.3,
            rsi_period: 0, rsi_no_long_above: 70.0, rsi_no_short_below: 30.0,
            fast_cur_idx: None, fast_open: 0.0, fast_high: 0.0, fast_low: 0.0, fast_close: 0.0,
            fast_bars: Vec::new(), fast_ema: 0.0,
            prev_fast_close: None, atr: None, atr_hist: Vec::new(),
            rsi_avg_gain: None, rsi_avg_loss: None, rsi_count: 0,
            slow_cur_idx: None, slow_open: 0.0, slow_high: 0.0, slow_low: 0.0, slow_close: 0.0,
            slow_bars: Vec::new(), slow_ema: 0.0,
            last_signal_dir: None,
        }
    }

    /// 快速 bar 收盘时更新 ATR（Wilder）与 RSI（Wilder）。
    fn update_volatility(&mut self, high: f64, low: f64, close: f64) {
        if let Some(prev) = self.prev_fast_close {
            let tr = (high - low)
                .max((high - prev).abs())
                .max((low - prev).abs());
            if self.atr_period > 0 {
                let p = self.atr_period as f64;
                let atr = match self.atr {
                    Some(a) => (a * (p - 1.0) + tr) / p,
                    None => tr,
                };
                self.atr = Some(atr);
                self.atr_hist.push(atr);
                if self.atr_hist.len() > self.atr_avg_period * 2 {
                    self.atr_hist.remove(0);
                }
            }
            if self.rsi_period > 0 {
                let chg = close - prev;
                let gain = chg.max(0.0);
                let loss = (-chg).max(0.0);
                let p = self.rsi_period as f64;
                match (self.rsi_avg_gain, self.rsi_avg_loss) {
                    (Some(g), Some(l)) => {
                        self.rsi_avg_gain = Some((g * (p - 1.0) + gain) / p);
                        self.rsi_avg_loss = Some((l * (p - 1.0) + loss) / p);
                    }
                    _ => {
                        self.rsi_avg_gain = Some(gain);
                        self.rsi_avg_loss = Some(loss);
                    }
                }
                self.rsi_count += 1;
            }
        }
        self.prev_fast_close = Some(close);
    }

    fn rsi(&self) -> Option<f64> {
        if self.rsi_period == 0 || self.rsi_count < self.rsi_period {
            return None;
        }
        let g = self.rsi_avg_gain?;
        let l = self.rsi_avg_loss?;
        if l < 1e-12 {
            return Some(100.0);
        }
        let rs = g / l;
        Some(100.0 - 100.0 / (1.0 + rs))
    }

    /// ATR 爆发抑制：当前 ATR > atr_max_mult × 近期平均 → true（不交易）
    fn atr_burst(&self) -> bool {
        if self.atr_period == 0 {
            return false;
        }
        let Some(atr) = self.atr else { return false };
        if self.atr_hist.len() < self.atr_avg_period {
            return false; // 样本不足不过滤
        }
        let start = self.atr_hist.len() - self.atr_avg_period;
        let avg: f64 =
            self.atr_hist[start..].iter().sum::<f64>() / self.atr_avg_period as f64;
        avg > 0.0 && atr > avg * self.atr_max_mult
    }

    fn update_ema(prev: f64, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        price * k + prev * (1.0 - k)
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();

        // 更新快速 bar
        let fast_idx = ts / self.fast_bar_ms * self.fast_bar_ms;
        match self.fast_cur_idx {
            None => {
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price; self.fast_high = price; self.fast_low = price; self.fast_close = price;
            }
            Some(cur) if cur == fast_idx => {
                self.fast_high = self.fast_high.max(price);
                self.fast_low = self.fast_low.min(price);
                self.fast_close = price;
            }
            Some(_) => {
                // 关闭快速 bar
                if self.fast_bars.is_empty() {
                    self.fast_ema = self.fast_close;
                } else {
                    self.fast_ema = Self::update_ema(self.fast_ema, self.fast_close, self.fast_ema_p);
                }
                self.fast_bars.push(self.fast_close);
                self.update_volatility(self.fast_high, self.fast_low, self.fast_close);
                if self.fast_bars.len() > self.fast_ema_p * 2 {
                    self.fast_bars.remove(0);
                }
                // 启动新 bar
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price; self.fast_high = price; self.fast_low = price; self.fast_close = price;
            }
        }

        // 更新慢速 bar
        let slow_idx = ts / self.slow_bar_ms * self.slow_bar_ms;
        match self.slow_cur_idx {
            None => {
                self.slow_cur_idx = Some(slow_idx);
                self.slow_open = price; self.slow_high = price; self.slow_low = price; self.slow_close = price;
            }
            Some(cur) if cur == slow_idx => {
                self.slow_high = self.slow_high.max(price);
                self.slow_low = self.slow_low.min(price);
                self.slow_close = price;
            }
            Some(_) => {
                if self.slow_bars.is_empty() {
                    self.slow_ema = self.slow_close;
                } else {
                    self.slow_ema = Self::update_ema(self.slow_ema, self.slow_close, self.slow_ema_p);
                }
                self.slow_bars.push(self.slow_close);
                if self.slow_bars.len() > self.slow_ema_p * 2 {
                    self.slow_bars.remove(0);
                }
                self.slow_cur_idx = Some(slow_idx);
                self.slow_open = price; self.slow_high = price; self.slow_low = price; self.slow_close = price;
            }
        }

        // 只在快速 bar 关闭时检查信号
        if self.fast_bars.len() < self.fast_ema_p || self.slow_bars.len() < self.slow_ema_p {
            return Vec::new();
        }

        let deviation = (self.fast_close - self.fast_ema) / self.fast_ema;
        let trend = if self.slow_close > self.slow_ema { 1 } else if self.slow_close < self.slow_ema { -1 } else { 0 };

        let mut sigs = Vec::new();

        // 路径 A 过滤：ATR 爆发期不交易（趋势爆发时均值回归容易被碾压）
        if self.atr_burst() {
            return sigs;
        }
        let rsi = self.rsi();

        // 大趋势向上 → 只做 oversold（做多）
        // 大趋势向下 → 只做 overbought（做空）
        // 不确定 → 双向

        if deviation > self.deviation_threshold {
            // RSI 动量守卫：RSI < rsi_no_short_below 时不做空（跌深不追空）
            let rsi_block = rsi.map(|r| r < self.rsi_no_short_below).unwrap_or(false);
            if !rsi_block && trend <= 0 && self.last_signal_dir.as_ref() != Some(&"overbought".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime, t.ts, self.name(),
                    serde_json::json!({"regime": "overbought", "price": self.fast_close, "ema": self.fast_ema, "trend": trend}),
                ));
                self.last_signal_dir = Some("overbought".to_string());
            }
        } else if deviation < -self.deviation_threshold {
            // RSI 动量守卫：RSI > rsi_no_long_above 时不做多（涨高不追多）
            let rsi_block = rsi.map(|r| r > self.rsi_no_long_above).unwrap_or(false);
            if !rsi_block && trend >= 0 && self.last_signal_dir.as_ref() != Some(&"oversold".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime, t.ts, self.name(),
                    serde_json::json!({"regime": "oversold", "price": self.fast_close, "ema": self.fast_ema, "trend": trend}),
                ));
                self.last_signal_dir = Some("oversold".to_string());
            }
        } else {
            self.last_signal_dir = None;
        }

        sigs
    }
}

impl SignalPlugin for DualTfMeanReversion {
    fn name(&self) -> &'static str { "DualTfMeanReversion" }
    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev { Event::Trade(t) => self.on_trade(t), _ => Vec::new() }
    }
}

impl DualTfMeanReversion {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        let mut s = Self::new(
            g("fast_bar_ms", 300_000.0) as i64,
            g("slow_bar_ms", 3_600_000.0) as i64,
            g("fast_ema_p", 20.0) as usize,
            g("slow_ema_p", 20.0) as usize,
            g("deviation_threshold", 0.015),
        );
        // 可选过滤（路径 A）：不配则保持 v1 原行为
        s.atr_period = g("atr_period", 0.0) as usize;
        s.atr_avg_period = g("atr_avg_period", 20.0) as usize;
        s.atr_max_mult = g("atr_max_mult", 1.3);
        s.rsi_period = g("rsi_period", 0.0) as usize;
        s.rsi_no_long_above = g("rsi_no_long_above", 70.0);
        s.rsi_no_short_below = g("rsi_no_short_below", 30.0);
        s
    }
}
