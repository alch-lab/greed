//! EMA 均值回归信号 v2：加入 ATR 波动率过滤。
//!
//! 只在低波动时段交易（ATR < 1.3×近期平均），避开趋势爆发。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct EmaMeanReversionV2 {
    bar_ms: i64,
    ema_period: usize,
    deviation_threshold: f64,
    atr_period: usize,
    atr_mult: f64, // ATR 超过 avg_ATR × 此倍数 = 高波动，不交易

    // 当前 bar
    cur_bar_idx: Option<i64>,
    cur_high: f64,
    cur_low: f64,
    cur_close: f64,

    bars: Vec<Bar>,
    last_signal_dir: Option<String>,
}

#[derive(Clone, Debug)]
struct Bar {
    close: f64,
    ema: f64,
    atr: f64,
}

impl EmaMeanReversionV2 {
    pub fn new(bar_ms: i64, ema_period: usize, deviation_threshold: f64, atr_period: usize, atr_mult: f64) -> Self {
        Self {
            bar_ms,
            ema_period,
            deviation_threshold,
            atr_period,
            atr_mult,
            cur_bar_idx: None,
            cur_high: 0.0,
            cur_low: 0.0,
            cur_close: 0.0,
            bars: Vec::new(),
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
        let bar_idx = ts / self.bar_ms * self.bar_ms;

        match self.cur_bar_idx {
            None => {
                self.cur_bar_idx = Some(bar_idx);
                self.cur_high = price;
                self.cur_low = price;
                self.cur_close = price;
                Vec::new()
            }
            Some(cur_idx) if cur_idx == bar_idx => {
                self.cur_high = self.cur_high.max(price);
                self.cur_low = self.cur_low.min(price);
                self.cur_close = price;
                Vec::new()
            }
            Some(_) => {
                let close = self.cur_close;
                let ema = if let Some(last) = self.bars.last() {
                    Self::update_ema(last.ema, close, self.ema_period)
                } else {
                    close
                };

                // 计算 TR
                let tr = if let Some(last) = self.bars.last() {
                    let tr1 = self.cur_high - self.cur_low;
                    let tr2 = (self.cur_high - last.close).abs();
                    let tr3 = (self.cur_low - last.close).abs();
                    tr1.max(tr2).max(tr3)
                } else {
                    self.cur_high - self.cur_low
                };

                // 计算 ATR
                let atr = if let Some(last) = self.bars.last() {
                    let k = 2.0 / (self.atr_period as f64 + 1.0);
                    tr * k + last.atr * (1.0 - k)
                } else {
                    tr
                };

                let new_bar = Bar { close, ema, atr };

                self.bars.push(new_bar);

                // 计算信号
                let mut sigs = Vec::new();
                if self.bars.len() >= self.ema_period.max(self.atr_period) {
                    let last = self.bars.last().unwrap();
                    let deviation = (last.close - last.ema) / last.ema;

                    // 波动率过滤：计算近期平均 ATR
                    let start = self.bars.len().saturating_sub(self.atr_period);
                    let avg_atr: f64 = self.bars[start..].iter().map(|b| b.atr).sum::<f64>()
                        / self.atr_period as f64;
                    let is_high_vol = last.atr > avg_atr * self.atr_mult;

                    if !is_high_vol {
                        if deviation > self.deviation_threshold {
                            if self.last_signal_dir.as_ref() != Some(&"overbought".to_string()) {
                                sigs.push(Signal::new(
                                    SignalKind::TrendRegime,
                                    t.ts,
                                    self.name(),
                                    serde_json::json!({
                                        "regime": "overbought",
                                        "price": last.close,
                                        "ema": last.ema,
                                        "deviation": deviation,
                                    }),
                                ));
                                self.last_signal_dir = Some("overbought".to_string());
                            }
                        } else if deviation < -self.deviation_threshold {
                            if self.last_signal_dir.as_ref() != Some(&"oversold".to_string()) {
                                sigs.push(Signal::new(
                                    SignalKind::TrendRegime,
                                    t.ts,
                                    self.name(),
                                    serde_json::json!({
                                        "regime": "oversold",
                                        "price": last.close,
                                        "ema": last.ema,
                                        "deviation": deviation,
                                    }),
                                ));
                                self.last_signal_dir = Some("oversold".to_string());
                            }
                        } else {
                            self.last_signal_dir = None;
                        }
                    }
                }

                // 启动新 bar
                self.cur_bar_idx = Some(bar_idx);
                self.cur_high = price;
                self.cur_low = price;
                self.cur_close = price;

                sigs
            }
        }
    }
}

impl SignalPlugin for EmaMeanReversionV2 {
    fn name(&self) -> &'static str {
        "EmaMeanReversionV2"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl EmaMeanReversionV2 {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("bar_ms", 300_000.0) as i64,       // 5 分钟
            g("ema_period", 20.0) as usize,
            g("deviation_threshold", 0.015),
            g("atr_period", 10.0) as usize,
            g("atr_mult", 1.3),
        )
    }
}
