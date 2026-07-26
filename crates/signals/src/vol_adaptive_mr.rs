//! 波动率自适应双时间框架均值回归（VolAdaptiveMr）
//!
//! 与 DualTfMeanReversion 的差异：
//! - 偏离度用 ATR 归一化（z = (close − EMA) / ATR），替代固定百分比阈值。
//!   固定阈值在低波动月信号泛滥、在高波动趋势月完全无信号；z-score 在所有
//!   波动率环境下语义一致（"价格偏离均值 N 倍噪声"），跨年份可迁移。
//! - 止损锚由扳机按 ATR 设定（见 MrAtrFollow），仓位随噪声自动反比缩放。
//!
//! 方向过滤与 v1 一致：大趋势向上只做超卖（逢低做多），向下只做超买，持平双向。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct VolAdaptiveMr {
    fast_bar_ms: i64,
    slow_bar_ms: i64,
    fast_ema_p: usize,
    slow_ema_p: usize,
    atr_p: usize,
    z_entry: f64,
    /// ATR 下限（占价格比例）：防止死市里 ATR≈0 导致 z 爆炸、在噪声中频繁交易
    min_atr_pct: f64,

    // 快速 bar（默认 5 分钟）
    fast_cur_idx: Option<i64>,
    fast_open: f64,
    fast_high: f64,
    fast_low: f64,
    fast_close: f64,
    fast_prev_close: f64, // 上一根已收 bar 的 close（算 TR 用）
    fast_bars: usize,     // 已收 bar 数
    fast_ema: f64,
    atr: f64,

    // 慢速 bar（默认 1 小时）
    slow_cur_idx: Option<i64>,
    slow_close: f64,
    slow_bars: usize,
    slow_ema: f64,

    last_signal_dir: Option<&'static str>,
}

impl VolAdaptiveMr {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fast_bar_ms: i64,
        slow_bar_ms: i64,
        fast_ema_p: usize,
        slow_ema_p: usize,
        atr_p: usize,
        z_entry: f64,
        min_atr_pct: f64,
    ) -> Self {
        Self {
            fast_bar_ms,
            slow_bar_ms,
            fast_ema_p,
            slow_ema_p,
            atr_p,
            z_entry,
            min_atr_pct,
            fast_cur_idx: None,
            fast_open: 0.0,
            fast_high: 0.0,
            fast_low: 0.0,
            fast_close: 0.0,
            fast_prev_close: 0.0,
            fast_bars: 0,
            fast_ema: 0.0,
            atr: 0.0,
            slow_cur_idx: None,
            slow_close: 0.0,
            slow_bars: 0,
            slow_ema: 0.0,
            last_signal_dir: None,
        }
    }

    fn update_ema(prev: f64, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        price * k + prev * (1.0 - k)
    }

    /// Wilder 平滑 ATR。
    fn update_atr(prev: f64, tr: f64, period: usize) -> f64 {
        (prev * (period as f64 - 1.0) + tr) / period as f64
    }

    /// 关闭当前快速 bar，更新 EMA/ATR，返回 (z, atr_used)。
    fn close_fast_bar(&mut self) {
        let tr = if self.fast_bars == 0 {
            self.fast_high - self.fast_low
        } else {
            let hl = self.fast_high - self.fast_low;
            let hc = (self.fast_high - self.fast_prev_close).abs();
            let lc = (self.fast_low - self.fast_prev_close).abs();
            hl.max(hc).max(lc)
        };
        if self.fast_bars == 0 {
            self.fast_ema = self.fast_close;
            self.atr = tr;
        } else {
            self.fast_ema = Self::update_ema(self.fast_ema, self.fast_close, self.fast_ema_p);
            self.atr = Self::update_atr(self.atr, tr, self.atr_p);
        }
        self.fast_prev_close = self.fast_close;
        self.fast_bars += 1;
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();

        // ---- 快速 bar 聚合 ----
        let fast_idx = ts / self.fast_bar_ms * self.fast_bar_ms;
        let mut closed_fast = false;
        match self.fast_cur_idx {
            None => {
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price;
                self.fast_high = price;
                self.fast_low = price;
                self.fast_close = price;
            }
            Some(cur) if cur == fast_idx => {
                self.fast_high = self.fast_high.max(price);
                self.fast_low = self.fast_low.min(price);
                self.fast_close = price;
            }
            Some(_) => {
                self.close_fast_bar();
                closed_fast = true;
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price;
                self.fast_high = price;
                self.fast_low = price;
                self.fast_close = price;
            }
        }

        // ---- 慢速 bar 聚合 ----
        let slow_idx = ts / self.slow_bar_ms * self.slow_bar_ms;
        match self.slow_cur_idx {
            None => {
                self.slow_cur_idx = Some(slow_idx);
                self.slow_close = price;
            }
            Some(cur) if cur == slow_idx => {
                self.slow_close = price;
            }
            Some(_) => {
                if self.slow_bars == 0 {
                    self.slow_ema = self.slow_close;
                } else {
                    self.slow_ema = Self::update_ema(self.slow_ema, self.slow_close, self.slow_ema_p);
                }
                self.slow_bars += 1;
                self.slow_cur_idx = Some(slow_idx);
                self.slow_close = price;
            }
        }

        // ---- 只在快速 bar 收盘时评估 ----
        if !closed_fast {
            return Vec::new();
        }
        let warm = self.atr_p.max(self.fast_ema_p);
        if self.fast_bars < warm || self.slow_bars < self.slow_ema_p {
            return Vec::new();
        }

        let atr_floor = self.fast_close * self.min_atr_pct;
        let atr_used = self.atr.max(atr_floor);
        if atr_used <= 0.0 || self.fast_ema <= 0.0 {
            return Vec::new();
        }
        let z = (self.fast_close - self.fast_ema) / atr_used;
        let trend = if self.slow_close > self.slow_ema {
            1
        } else if self.slow_close < self.slow_ema {
            -1
        } else {
            0
        };

        let mut sigs = Vec::new();
        if z >= self.z_entry {
            // 超买：大趋势向下或持平时做空
            if trend <= 0 && self.last_signal_dir != Some("overbought") {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
                    serde_json::json!({
                        "regime": "overbought", "price": self.fast_close,
                        "ema": self.fast_ema, "atr": atr_used, "trend": trend, "z": z,
                    }),
                ));
                self.last_signal_dir = Some("overbought");
            }
        } else if z <= -self.z_entry {
            // 超卖：大趋势向上或持平时做多
            if trend >= 0 && self.last_signal_dir != Some("oversold") {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
                    serde_json::json!({
                        "regime": "oversold", "price": self.fast_close,
                        "ema": self.fast_ema, "atr": atr_used, "trend": trend, "z": z,
                    }),
                ));
                self.last_signal_dir = Some("oversold");
            }
        } else {
            self.last_signal_dir = None;
        }
        sigs
    }
}

impl SignalPlugin for VolAdaptiveMr {
    fn name(&self) -> &'static str {
        "VolAdaptiveMr"
    }
    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl VolAdaptiveMr {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("fast_bar_ms", 300_000.0) as i64,
            g("slow_bar_ms", 3_600_000.0) as i64,
            g("fast_ema_p", 20.0) as usize,
            g("slow_ema_p", 20.0) as usize,
            g("atr_p", 14.0) as usize,
            g("z_entry", 2.0),
            g("min_atr_pct", 0.0005),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

    fn trade(ts_ms: i64, price: f64) -> tcore::Trade {
        tcore::Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(0.01),
            is_buyer_maker: false,
        }
    }

    /// 构造一段时间序列：前 3 小时在 66920 横盘（慢速 EMA 偏低），
    /// 第 4 小时上台阶到 67000（trend=+1），随后小幅急跌到 66960：
    /// 相对 5m EMA 偏离足够深（z < −z_entry）但仍在慢速 EMA 上方，应发出 oversold。
    #[test]
    fn fires_oversold_on_deep_dip() {
        let mut s = VolAdaptiveMr::new(60_000, 3_600_000, 5, 3, 5, 2.0, 0.0001);
        // 前 3 小时：66920 ± 3 窄幅
        for i in 0..180 {
            let px = 66920.0 + ((i % 7) as f64) - 3.0;
            s.on_trade(&trade(i * 60_000 + 59_999, px));
        }
        // 第 4 小时：上台阶到 67000 ± 3
        for i in 180..240 {
            let px = 67000.0 + ((i % 7) as f64) - 3.0;
            s.on_trade(&trade(i * 60_000 + 59_999, px));
        }
        // 小幅急跌 3 根 bar：66960 / 66940 / 66930（仍在慢速 EMA 上方）
        let mut fired = false;
        for (k, px) in [66960.0, 66940.0, 66930.0].iter().enumerate() {
            let base = 240i64 + k as i64;
            for sig in s.on_trade(&trade(base * 60_000 + 59_999, *px)) {
                if sig.payload.get("regime").unwrap() == "oversold" {
                    fired = true;
                    assert!(sig.payload.get("atr").unwrap().as_f64().unwrap() > 0.0);
                }
            }
        }
        assert!(fired, "顺势回调应发出 oversold 信号");
    }

    /// 逆势深跌（价格跌破慢速 EMA）不应发出 oversold。
    #[test]
    fn counter_trend_dip_blocked() {
        let mut s = VolAdaptiveMr::new(60_000, 3_600_000, 5, 3, 5, 2.0, 0.0001);
        for i in 0..240 {
            let px = 67000.0 + ((i % 7) as f64) - 3.0;
            s.on_trade(&trade(i * 60_000 + 59_999, px));
        }
        // 大跌：连续 3 根每根 −150，跌破慢速 EMA → trend=−1，做多被拦
        for k in 0..3 {
            let base = 240i64 + k;
            let px = 67000.0 - (k as f64 + 1.0) * 150.0;
            for sig in s.on_trade(&trade(base * 60_000 + 59_999, px)) {
                assert_ne!(sig.payload.get("regime").unwrap(), "oversold");
            }
        }
    }

    /// 横盘小幅波动永不触发（z 达不到阈值）。
    #[test]
    fn sideways_never_fires() {
        let mut s = VolAdaptiveMr::new(60_000, 3_600_000, 5, 3, 5, 2.5, 0.0001);
        for i in 0..300 {
            let px = 67000.0 + ((i % 5) as f64) * 2.0 - 4.0;
            assert!(s.on_trade(&trade(i * 60_000 + 59_999, px)).is_empty());
        }
    }
}
