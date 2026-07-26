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
    fast_vol: f64,             // 当前 bar 成交额（USD，评估记录用）
    fast_bars: Vec<f64>, // close prices for fast EMA
    fast_ema: f64,
    /// 最近一根已关闭 bar 的 (open, high, low, close, vol)——near-miss 分析用
    closed_bar: Option<(f64, f64, f64, f64, f64)>,
    /// 已关闭 bar 的成交额基线（量比计算；跳过 0 = 预热合成段）
    vol_hist: Vec<f64>,

    // ATR/RSI 状态（基于已收快速 bar）
    prev_fast_close: Option<f64>,
    atr: Option<f64>,          // Wilder ATR
    atr_hist: Vec<f64>,        // ATR 样本（算近期平均）
    rsi_avg_gain: Option<f64>,
    rsi_avg_loss: Option<f64>,
    rsi_count: usize,

    // 记录用指标：ATR(14)/RSI(14) 始终计算（可观测性特征快照），
    // 与上面的过滤用 ATR/RSI 完全独立，不改变任何信号/过滤行为
    rec_atr: Option<f64>,
    rec_atr_hist: Vec<f64>,
    rec_rsi_avg_gain: Option<f64>,
    rec_rsi_avg_loss: Option<f64>,
    rec_rsi_count: usize,

    // 慢速 bar（1小时）
    slow_cur_idx: Option<i64>,
    slow_open: f64, slow_high: f64, slow_low: f64, slow_close: f64,
    slow_bars: Vec<f64>,
    slow_ema: f64,
    /// 慢线 EMA 历史（趋势斜率，cap 10）
    slow_ema_hist: Vec<f64>,

    last_signal_dir: Option<String>,
    /// 最近一次评估说明（可观测性，供控制面展示）
    last_eval: Option<serde_json::Value>,
}

impl DualTfMeanReversion {
    pub fn new(fast_bar_ms: i64, slow_bar_ms: i64, fast_ema_p: usize, slow_ema_p: usize, deviation: f64) -> Self {
        Self {
            fast_bar_ms, slow_bar_ms, fast_ema_p, slow_ema_p, deviation_threshold: deviation,
            atr_period: 0, atr_avg_period: 20, atr_max_mult: 1.3,
            rsi_period: 0, rsi_no_long_above: 70.0, rsi_no_short_below: 30.0,
            fast_cur_idx: None, fast_open: 0.0, fast_high: 0.0, fast_low: 0.0, fast_close: 0.0,
            fast_vol: 0.0,
            fast_bars: Vec::new(), fast_ema: 0.0,
            closed_bar: None, vol_hist: Vec::new(),
            prev_fast_close: None, atr: None, atr_hist: Vec::new(),
            rsi_avg_gain: None, rsi_avg_loss: None, rsi_count: 0,
            rec_atr: None, rec_atr_hist: Vec::new(),
            rec_rsi_avg_gain: None, rec_rsi_avg_loss: None, rec_rsi_count: 0,
            slow_cur_idx: None, slow_open: 0.0, slow_high: 0.0, slow_low: 0.0, slow_close: 0.0,
            slow_bars: Vec::new(), slow_ema: 0.0,
            slow_ema_hist: Vec::new(),
            last_signal_dir: None,
            last_eval: None,
        }
    }

    /// 快速 bar 收盘时更新 ATR（Wilder）与 RSI（Wilder）。
    fn update_volatility(&mut self, high: f64, low: f64, close: f64) {
        if let Some(prev) = self.prev_fast_close {
            let tr = (high - low)
                .max((high - prev).abs())
                .max((low - prev).abs());
            // 记录用 ATR(14)：始终维护（特征快照，不过滤）
            const REC_P: f64 = 14.0;
            let rec = match self.rec_atr {
                Some(a) => (a * (REC_P - 1.0) + tr) / REC_P,
                None => tr,
            };
            self.rec_atr = Some(rec);
            self.rec_atr_hist.push(rec);
            if self.rec_atr_hist.len() > 40 {
                self.rec_atr_hist.remove(0);
            }
            // 记录用 RSI(14)：始终维护（特征快照，不过滤）
            {
                let chg = close - prev;
                let gain = chg.max(0.0);
                let loss = (-chg).max(0.0);
                match (self.rec_rsi_avg_gain, self.rec_rsi_avg_loss) {
                    (Some(g), Some(l)) => {
                        self.rec_rsi_avg_gain = Some((g * (REC_P - 1.0) + gain) / REC_P);
                        self.rec_rsi_avg_loss = Some((l * (REC_P - 1.0) + loss) / REC_P);
                    }
                    _ => {
                        self.rec_rsi_avg_gain = Some(gain);
                        self.rec_rsi_avg_loss = Some(loss);
                    }
                }
                self.rec_rsi_count += 1;
            }
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

    /// 记录用 RSI(14)（攒满 14 根后有效）
    fn rec_rsi(&self) -> Option<f64> {
        if self.rec_rsi_count < 14 {
            return None;
        }
        let g = self.rec_rsi_avg_gain?;
        let l = self.rec_rsi_avg_loss?;
        if l < 1e-12 {
            return Some(100.0);
        }
        Some(100.0 - 100.0 / (1.0 + g / l))
    }

    /// ATR 倍数：当前 ATR(14) / 近 20 根均值（波动环境快照）
    fn rec_atr_mult(&self) -> Option<f64> {
        let atr = self.rec_atr?;
        if self.rec_atr_hist.len() < 21 {
            return None;
        }
        let start = self.rec_atr_hist.len() - 21;
        let avg: f64 = self.rec_atr_hist[start..self.rec_atr_hist.len() - 1]
            .iter()
            .sum::<f64>()
            / 20.0;
        if avg > 0.0 { Some(atr / avg) } else { None }
    }

    /// 量比：最近关闭 bar 成交额 / 近 20 根均值（跳过预热零量段）
    fn vol_mult(&self) -> Option<f64> {
        let (_, _, _, _, v) = self.closed_bar?;
        if v <= 0.0 || self.vol_hist.len() < 11 {
            return None;
        }
        let n = self.vol_hist.len();
        let start = n.saturating_sub(21);
        let base: Vec<f64> = self.vol_hist[start..n - 1].to_vec();
        if base.is_empty() {
            return None;
        }
        let avg: f64 = base.iter().sum::<f64>() / base.len() as f64;
        if avg > 0.0 { Some(v / avg) } else { None }
    }

    /// 慢线 EMA 斜率（最近 5 根 1h 的变化率 %，趋势强度）
    fn slow_slope_pct(&self) -> Option<f64> {
        let n = self.slow_ema_hist.len();
        if n < 6 {
            return None;
        }
        let old = self.slow_ema_hist[n - 6];
        if old.abs() < 1e-12 {
            return None;
        }
        Some((self.slow_ema_hist[n - 1] - old) / old * 100.0)
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

    fn on_trade(&mut self, t: &tcore::Trade, ctx: &Ctx) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();
        let notional = t.qty.to_f64() * price;

        // 更新快速 bar
        let mut fast_closed = false;
        let fast_idx = ts / self.fast_bar_ms * self.fast_bar_ms;
        match self.fast_cur_idx {
            None => {
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price; self.fast_high = price; self.fast_low = price; self.fast_close = price;
                self.fast_vol = notional;
            }
            Some(cur) if cur == fast_idx => {
                self.fast_high = self.fast_high.max(price);
                self.fast_low = self.fast_low.min(price);
                self.fast_close = price;
                self.fast_vol += notional;
            }
            Some(_) => {
                // 关闭快速 bar
                fast_closed = true;
                if self.fast_bars.is_empty() {
                    self.fast_ema = self.fast_close;
                } else {
                    self.fast_ema = Self::update_ema(self.fast_ema, self.fast_close, self.fast_ema_p);
                }
                self.fast_bars.push(self.fast_close);
                self.update_volatility(self.fast_high, self.fast_low, self.fast_close);
                // 暂存已关闭 bar 快照（near-miss/量比分析），0 量（预热合成段）不进基线
                self.closed_bar = Some((self.fast_open, self.fast_high, self.fast_low, self.fast_close, self.fast_vol));
                if self.fast_vol > 0.0 {
                    self.vol_hist.push(self.fast_vol);
                    if self.vol_hist.len() > 40 {
                        self.vol_hist.remove(0);
                    }
                }
                if self.fast_bars.len() > self.fast_ema_p * 2 {
                    self.fast_bars.remove(0);
                }
                // 启动新 bar
                self.fast_cur_idx = Some(fast_idx);
                self.fast_open = price; self.fast_high = price; self.fast_low = price; self.fast_close = price;
                self.fast_vol = notional;
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
                self.slow_ema_hist.push(self.slow_ema);
                if self.slow_ema_hist.len() > 10 {
                    self.slow_ema_hist.remove(0);
                }
                if self.slow_bars.len() > self.slow_ema_p * 2 {
                    self.slow_bars.remove(0);
                }
                self.slow_cur_idx = Some(slow_idx);
                self.slow_open = price; self.slow_high = price; self.slow_low = price; self.slow_close = price;
            }
        }

        // 信号检查（快慢线攒满 EMA 周期后才评估）
        if self.fast_bars.len() < self.fast_ema_p || self.slow_bars.len() < self.slow_ema_p {
            if fast_closed {
                self.last_eval = Some(serde_json::json!({
                    "ts_ms": ts,
                    "price": self.fast_close,
                    "decision": "warmup",
                    "reason": format!(
                        "预热中：快线 {}/{} 根，慢线 {}/{} 根（攒满后才评估信号）",
                        self.fast_bars.len(), self.fast_ema_p,
                        self.slow_bars.len(), self.slow_ema_p
                    ),
                }));
            }
            return Vec::new();
        }

        let deviation = (self.fast_close - self.fast_ema) / self.fast_ema;
        let trend = if self.slow_close > self.slow_ema { 1 } else if self.slow_close < self.slow_ema { -1 } else { 0 };
        let trend_text = match trend { 1 => "向上", -1 => "向下", _ => "持平" };
        let dev_pct = deviation * 100.0;
        let thr_pct = self.deviation_threshold * 100.0;

        let mut sigs = Vec::new();
        // 本次评估的结论与人话解释（控制面「为什么下单/不下单」展示用）
        let mut decision = "none";
        let mut reason = format!(
            "5m 偏离 {:+.2}%，未达 ±{:.1}% 阈值，观望（再跌 {:.2}% 触发做多 / 再涨 {:.2}% 触发做空）",
            dev_pct, thr_pct, dev_pct + thr_pct, thr_pct - dev_pct
        );

        // 路径 A 过滤：ATR 爆发期不交易（趋势爆发时均值回归容易被碾压）
        if self.atr_burst() {
            reason = format!("5m 偏离 {:+.2}%，但 ATR 爆发抑制中（波动骤增），暂停交易", dev_pct);
        } else {
            let rsi = self.rsi();

            // 大趋势向上 → 只做 oversold（做多）
            // 大趋势向下 → 只做 overbought（做空）
            // 不确定 → 双向
            if deviation > self.deviation_threshold {
                // RSI 动量守卫：RSI < rsi_no_short_below 时不做空（跌深不追空）
                let rsi_block = rsi.map(|r| r < self.rsi_no_short_below).unwrap_or(false);
                if rsi_block {
                    reason = format!("5m 超买 {:+.2}%，但 RSI 动量守卫拦截（跌深不追空）", dev_pct);
                } else if trend > 0 {
                    reason = format!("5m 超买 {:+.2}%，但 1h 趋势{}，趋势过滤放弃做空", dev_pct, trend_text);
                } else if self.last_signal_dir.as_deref() == Some("overbought") {
                    decision = "cooldown";
                    reason = format!("5m 超买 {:+.2}% + 1h 趋势{}，同向信号冷却中（等待偏离复位）", dev_pct, trend_text);
                } else {
                    decision = "short";
                    reason = format!("5m 超买 {:+.2}% + 1h 趋势{} → 逢高做空", dev_pct, trend_text);
                    sigs.push(Signal::new(
                        SignalKind::TrendRegime, t.ts, self.name(),
                        serde_json::json!({"regime": "overbought", "price": self.fast_close, "ema": self.fast_ema, "trend": trend}),
                    ));
                    self.last_signal_dir = Some("overbought".to_string());
                }
            } else if deviation < -self.deviation_threshold {
                // RSI 动量守卫：RSI > rsi_no_long_above 时不做多（涨高不追多）
                let rsi_block = rsi.map(|r| r > self.rsi_no_long_above).unwrap_or(false);
                if rsi_block {
                    reason = format!("5m 超卖 {:+.2}%，但 RSI 动量守卫拦截（涨高不追多）", dev_pct);
                } else if trend < 0 {
                    reason = format!("5m 超卖 {:+.2}%，但 1h 趋势{}，趋势过滤放弃做多", dev_pct, trend_text);
                } else if self.last_signal_dir.as_deref() == Some("oversold") {
                    decision = "cooldown";
                    reason = format!("5m 超卖 {:+.2}% + 1h 趋势{}，同向信号冷却中（等待偏离复位）", dev_pct, trend_text);
                } else {
                    decision = "long";
                    reason = format!("5m 超卖 {:+.2}% + 1h 趋势{} → 逢低做多", dev_pct, trend_text);
                    sigs.push(Signal::new(
                        SignalKind::TrendRegime, t.ts, self.name(),
                        serde_json::json!({"regime": "oversold", "price": self.fast_close, "ema": self.fast_ema, "trend": trend}),
                    ));
                    self.last_signal_dir = Some("oversold".to_string());
                }
            } else {
                self.last_signal_dir = None;
            }
        }

        // near-miss：没出信号但盘中曾触及触发线（阈值敏感性分析的关键数据）
        if sigs.is_empty() {
            if let Some((_, h, l, _, _)) = self.closed_bar {
                let min_dev = (l - self.fast_ema) / self.fast_ema * 100.0;
                let max_dev = (h - self.fast_ema) / self.fast_ema * 100.0;
                if min_dev <= -thr_pct {
                    reason.push_str(&format!("；盘中最低触及 {:+.2}% 后收回（near-miss）", min_dev));
                } else if max_dev >= thr_pct {
                    reason.push_str(&format!("；盘中最高触及 {:+.2}% 后回落（near-miss）", max_dev));
                }
            }
        }

        // 记录评估说明：快线收盘必记；盘中触发信号也记（信号更重要，后写覆盖收盘快照）
        if fast_closed || !sigs.is_empty() {
            self.last_eval = Some(serde_json::json!({
                "ts_ms": ts,
                "price": self.fast_close,
                "fast_ema": self.fast_ema,
                "slow_close": self.slow_close,
                "slow_ema": self.slow_ema,
                "deviation_pct": dev_pct,
                "threshold_pct": thr_pct,
                "trend": trend_text,
                "decision": decision,
                "reason": reason,
                // ---- 特征快照（优化分析用；null = 预热段数据不足）----
                "slow_dev_pct": (self.slow_close - self.slow_ema) / self.slow_ema * 100.0,
                "slope_5h_pct": self.slow_slope_pct(),
                "atr_pct": self.rec_atr.map(|a| a / self.fast_close * 100.0),
                "atr_mult": self.rec_atr_mult(),
                "rsi": self.rec_rsi(),
                "vol_mult": self.vol_mult(),
                "bar_min_dev_pct": self.closed_bar.map(|(_, _, l, _, _)| (l - self.fast_ema) / self.fast_ema * 100.0),
                "bar_max_dev_pct": self.closed_bar.map(|(_, h, _, _, _)| (h - self.fast_ema) / self.fast_ema * 100.0),
                "session": ctx.flag("session"),
                "funding_rate": ctx.flag("funding_rate").and_then(|s| s.parse::<f64>().ok()),
            }));
        }

        sigs
    }
}

impl SignalPlugin for DualTfMeanReversion {
    fn name(&self) -> &'static str { "DualTfMeanReversion" }
    fn on_event(&mut self, ev: &Event, ctx: &Ctx) -> Vec<Signal> {
        match ev { Event::Trade(t) => self.on_trade(t, ctx), _ => Vec::new() }
    }
    fn eval_note(&self) -> Option<serde_json::Value> {
        self.last_eval.clone()
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

// ============================================================================
// Tests
// ============================================================================

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

    fn feed(s: &mut DualTfMeanReversion, n: i64) {
        let ctx = Ctx::default();
        for i in 0..n {
            // 每个 5m bar 一笔成交，价格小幅波动（不触发信号）
            let price = 67000.0 + ((i % 8) as f64 - 4.0) * 5.0;
            let ev = Event::Trade(trade(i * 300_000, price));
            s.on_event(&ev, &ctx);
        }
    }

    /// 预热期：eval_note 报告 warmup 进度（快/慢线攒线数）
    #[test]
    fn eval_note_reports_warmup_progress() {
        let mut s = DualTfMeanReversion::new(300_000, 3_600_000, 20, 20, 0.015);
        feed(&mut s, 5);
        let note = s.eval_note().expect("预热期也应有评估说明");
        assert_eq!(note["decision"], "warmup");
        assert!(note["reason"].as_str().unwrap().contains("预热中"));
    }

    /// 攒满后：每次快线收盘产出评估说明，无信号时 decision=none 且解释观望原因
    #[test]
    fn eval_note_explains_no_signal() {
        let mut s = DualTfMeanReversion::new(300_000, 3_600_000, 20, 20, 0.015);
        feed(&mut s, 260); // 260 根 5m ≈ 21.7h，慢线也攒满 20 根
        let note = s.eval_note().expect("攒满后应有评估说明");
        assert_eq!(note["decision"], "none");
        let reason = note["reason"].as_str().unwrap();
        assert!(reason.contains("观望"), "reason={}", reason);
        assert!(reason.contains("触发做多"), "应提示距触发距离: {}", reason);
        assert!(note["deviation_pct"].as_f64().unwrap().abs() < 1.5);
        assert!(note["fast_ema"].as_f64().unwrap() > 0.0);
        assert!(note["trend"].as_str().is_some());
    }

    /// 超卖 + 趋势向上：eval_note 给出做多决策与人话原因
    #[test]
    fn eval_note_explains_long_signal() {
        let mut s = DualTfMeanReversion::new(300_000, 3_600_000, 20, 20, 0.015);
        // 慢线稳步上行（趋势向上）：每小时 +300，EMA 滞后足以扛住急跌
        let ctx = Ctx::default();
        let mut i: i64 = 0;
        for h in 0..22 {
            for m in 0..12 {
                let price = 60000.0 + h as f64 * 300.0 + (m % 3) as f64;
                let ev = Event::Trade(trade(i * 300_000, price));
                s.on_event(&ev, &ctx);
                i += 1;
            }
        }
        // 急砸 2%：一根 5m 内直接打到超卖区
        let base = 60000.0 + 21.0 * 300.0;
        let ev = Event::Trade(trade(i * 300_000 + 1, base * 0.98));
        let sigs = s.on_event(&ev, &ctx);
        assert!(!sigs.is_empty(), "超卖 + 趋势向上应出做多信号");
        let note = s.eval_note().unwrap();
        assert_eq!(note["decision"], "long");
        assert!(note["reason"].as_str().unwrap().contains("逢低做多"));
    }
}
