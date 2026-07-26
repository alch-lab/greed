//!  regime 切换组合信号（路径 B：震荡做均值回归，趋势做 EMA 跟随）
//!
//! 用 1h ADX(14) 划分市场状态：
//! - ADX ≤ adx_range_max（震荡）：与 `DualTfMeanReversion` 相同的 5m 偏离
//!   均值回归信号（overbought/oversold，TP=EMA）。
//! - ADX ≥ adx_trend_min（趋势）：1h EMA 快/慢金叉死叉发 trend_long/trend_short。
//! - 两者之间为死区：不开新仓。
//!
//! ADX 未就绪（启动期）按震荡处理，与 v1 行为一致。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Regime {
    Range,
    Dead,
    Trend,
}

pub struct RegimeSwitchMr {
    // 均值回归腿（与 DualTfMeanReversion 相同）
    fast_bar_ms: i64,
    slow_bar_ms: i64,
    fast_ema_p: usize,
    slow_ema_p: usize,
    deviation_threshold: f64,
    // 趋势腿
    trend_fast_p: usize,
    trend_slow_p: usize,
    // ADX
    adx_period: usize,
    adx_range_max: f64,
    adx_trend_min: f64,

    // 快速 bar（5分钟）
    fast_cur_idx: Option<i64>,
    fast_high: f64,
    fast_low: f64,
    fast_close: f64,
    fast_bars: Vec<f64>,
    fast_ema: f64,

    // 慢速 bar（1小时）
    slow_cur_idx: Option<i64>,
    slow_high: f64,
    slow_low: f64,
    slow_close: f64,
    slow_bars: Vec<f64>,
    slow_ema: f64,
    /// 已收 1h bar 总数（warmup 判定；slow_bars 有截断不能用于此）
    slow_count: usize,

    // 趋势腿 EMA（1h 收盘价）
    trend_fast_ema: Option<f64>,
    trend_slow_ema: Option<f64>,
    last_trend_dir: i8, // 最近趋势信号方向，防重复

    // ADX 状态（Wilder，1h bar）
    prev_slow_h: Option<f64>,
    prev_slow_l: Option<f64>,
    prev_slow_c: Option<f64>,
    sm_tr: Option<f64>,
    sm_plus_dm: Option<f64>,
    sm_minus_dm: Option<f64>,
    adx: Option<f64>,
    adx_count: usize,

    last_signal_dir: Option<String>,
}

impl RegimeSwitchMr {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fast_bar_ms: i64,
        slow_bar_ms: i64,
        fast_ema_p: usize,
        slow_ema_p: usize,
        deviation: f64,
        trend_fast_p: usize,
        trend_slow_p: usize,
        adx_period: usize,
        adx_range_max: f64,
        adx_trend_min: f64,
    ) -> Self {
        Self {
            fast_bar_ms,
            slow_bar_ms,
            fast_ema_p,
            slow_ema_p,
            deviation_threshold: deviation,
            trend_fast_p,
            trend_slow_p,
            adx_period,
            adx_range_max,
            adx_trend_min,
            fast_cur_idx: None,
            fast_high: 0.0,
            fast_low: 0.0,
            fast_close: 0.0,
            fast_bars: Vec::new(),
            fast_ema: 0.0,
            slow_cur_idx: None,
            slow_high: 0.0,
            slow_low: 0.0,
            slow_close: 0.0,
            slow_bars: Vec::new(),
            slow_ema: 0.0,
            slow_count: 0,
            trend_fast_ema: None,
            trend_slow_ema: None,
            last_trend_dir: 0,
            prev_slow_h: None,
            prev_slow_l: None,
            prev_slow_c: None,
            sm_tr: None,
            sm_plus_dm: None,
            sm_minus_dm: None,
            adx: None,
            adx_count: 0,
            last_signal_dir: None,
        }
    }

    fn update_ema(prev: f64, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        price * k + prev * (1.0 - k)
    }

    fn regime(&self) -> Regime {
        match self.adx {
            // 启动期 ADX 未就绪：按震荡处理（与 v1 一致）
            None => Regime::Range,
            Some(a) if a >= self.adx_trend_min => Regime::Trend,
            Some(a) if a <= self.adx_range_max => Regime::Range,
            _ => Regime::Dead,
        }
    }

    /// 1h bar 收盘：更新慢速 EMA / 趋势 EMA / ADX，可能产出趋势入场信号。
    fn on_slow_close(&mut self, ts: tcore::types::Timestamp) -> Vec<Signal> {
        let h = self.slow_high;
        let l = self.slow_low;
        let c = self.slow_close;

        // 慢速 EMA（MR 腿的大趋势方向）
        if self.slow_bars.is_empty() {
            self.slow_ema = c;
        } else {
            self.slow_ema = Self::update_ema(self.slow_ema, c, self.slow_ema_p);
        }
        self.slow_bars.push(c);
        if self.slow_bars.len() > self.slow_ema_p * 2 {
            self.slow_bars.remove(0);
        }
        self.slow_count += 1;

        // 趋势腿 EMA
        self.trend_fast_ema = Some(match self.trend_fast_ema {
            Some(e) => Self::update_ema(e, c, self.trend_fast_p),
            None => c,
        });
        self.trend_slow_ema = Some(match self.trend_slow_ema {
            Some(e) => Self::update_ema(e, c, self.trend_slow_p),
            None => c,
        });

        // ADX（Wilder）
        let mut sigs = Vec::new();
        if let (Some(ph), Some(pl), Some(pc)) = (self.prev_slow_h, self.prev_slow_l, self.prev_slow_c)
        {
            let tr = (h - l).max((h - pc).abs()).max((l - pc).abs());
            let up = h - ph;
            let dn = pl - l;
            let plus_dm = if up > dn && up > 0.0 { up } else { 0.0 };
            let minus_dm = if dn > up && dn > 0.0 { dn } else { 0.0 };
            let p = self.adx_period as f64;
            let wilder = |prev: Option<f64>, x: f64| match prev {
                Some(v) => v - v / p + x,
                None => x,
            };
            self.sm_tr = Some(wilder(self.sm_tr, tr));
            self.sm_plus_dm = Some(wilder(self.sm_plus_dm, plus_dm));
            self.sm_minus_dm = Some(wilder(self.sm_minus_dm, minus_dm));
            self.adx_count += 1;

            if self.adx_count > self.adx_period {
                let sm_tr = self.sm_tr.unwrap();
                if sm_tr > 1e-12 {
                    let plus_di = 100.0 * self.sm_plus_dm.unwrap() / sm_tr;
                    let minus_di = 100.0 * self.sm_minus_dm.unwrap() / sm_tr;
                    let di_sum = plus_di + minus_di;
                    if di_sum > 1e-12 {
                        let dx = 100.0 * (plus_di - minus_di).abs() / di_sum;
                        self.adx = Some(match self.adx {
                            Some(a) => (a * (p - 1.0) + dx) / p,
                            None => dx,
                        });
                    }
                }
            }
        }
        self.prev_slow_h = Some(h);
        self.prev_slow_l = Some(l);
        self.prev_slow_c = Some(c);

        // 趋势腿：金叉/死叉（仅趋势状态发信号）
        if self.regime() == Regime::Trend && self.slow_count >= self.trend_slow_p {
            let fe = self.trend_fast_ema.unwrap();
            let se = self.trend_slow_ema.unwrap();
            let dir = if fe > se {
                1i8
            } else if fe < se {
                -1i8
            } else {
                0i8
            };
            if dir != 0 && dir != self.last_trend_dir {
                let regime = if dir == 1 { "trend_long" } else { "trend_short" };
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    ts,
                    self.name(),
                    serde_json::json!({
                        "regime": regime,
                        "price": c,
                        "ema": fe,
                        "adx": self.adx.unwrap_or(0.0),
                    }),
                ));
                self.last_trend_dir = dir;
            }
        }

        sigs
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();
        let mut sigs = Vec::new();

        // 更新快速 bar
        let fast_idx = ts / self.fast_bar_ms * self.fast_bar_ms;
        match self.fast_cur_idx {
            None => {
                self.fast_cur_idx = Some(fast_idx);
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
                if self.fast_bars.is_empty() {
                    self.fast_ema = self.fast_close;
                } else {
                    self.fast_ema =
                        Self::update_ema(self.fast_ema, self.fast_close, self.fast_ema_p);
                }
                self.fast_bars.push(self.fast_close);
                if self.fast_bars.len() > self.fast_ema_p * 2 {
                    self.fast_bars.remove(0);
                }
                self.fast_cur_idx = Some(fast_idx);
                self.fast_high = price;
                self.fast_low = price;
                self.fast_close = price;
            }
        }

        // 更新慢速 bar（收盘时可能产出趋势信号）
        let slow_idx = ts / self.slow_bar_ms * self.slow_bar_ms;
        match self.slow_cur_idx {
            None => {
                self.slow_cur_idx = Some(slow_idx);
                self.slow_high = price;
                self.slow_low = price;
                self.slow_close = price;
            }
            Some(cur) if cur == slow_idx => {
                self.slow_high = self.slow_high.max(price);
                self.slow_low = self.slow_low.min(price);
                self.slow_close = price;
            }
            Some(_) => {
                sigs.extend(self.on_slow_close(t.ts));
                self.slow_cur_idx = Some(slow_idx);
                self.slow_high = price;
                self.slow_low = price;
                self.slow_close = price;
            }
        }

        // MR 腿：仅震荡状态发偏离信号
        if self.fast_bars.len() < self.fast_ema_p
            || self.slow_bars.len() < self.slow_ema_p
            || self.regime() != Regime::Range
        {
            return sigs;
        }

        let deviation = (self.fast_close - self.fast_ema) / self.fast_ema;
        let trend = if self.slow_close > self.slow_ema {
            1
        } else if self.slow_close < self.slow_ema {
            -1
        } else {
            0
        };

        if deviation > self.deviation_threshold {
            if trend <= 0 && self.last_signal_dir.as_ref() != Some(&"overbought".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
                    serde_json::json!({"regime": "overbought", "price": self.fast_close, "ema": self.fast_ema, "trend": trend}),
                ));
                self.last_signal_dir = Some("overbought".to_string());
            }
        } else if deviation < -self.deviation_threshold {
            if trend >= 0 && self.last_signal_dir.as_ref() != Some(&"oversold".to_string()) {
                sigs.push(Signal::new(
                    SignalKind::TrendRegime,
                    t.ts,
                    self.name(),
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

impl SignalPlugin for RegimeSwitchMr {
    fn name(&self) -> &'static str {
        "RegimeSwitchMr"
    }
    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl RegimeSwitchMr {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("fast_bar_ms", 300_000.0) as i64,
            g("slow_bar_ms", 3_600_000.0) as i64,
            g("fast_ema_p", 20.0) as usize,
            g("slow_ema_p", 20.0) as usize,
            g("deviation_threshold", 0.015),
            g("trend_fast_p", 20.0) as usize,
            g("trend_slow_p", 50.0) as usize,
            g("adx_period", 14.0) as usize,
            g("adx_range_max", 20.0),
            g("adx_trend_min", 25.0),
        )
    }
}
