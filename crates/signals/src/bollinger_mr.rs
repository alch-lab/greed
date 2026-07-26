//! 布林带均值回归信号
//!
//! 维护 OHLC bar，计算 EMA + 布林带。
//! 价格触及上轨 → Overbought 信号
//! 价格触及下轨 → Oversold 信号

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct BollingerMeanReversion {
    bar_ms: i64,
    ema_period: usize,
    bb_mult: f64,

    cur_bar_idx: Option<i64>,
    cur_close: f64,

    bars: Vec<Bar>,
}

#[derive(Clone, Debug)]
struct Bar {
    close: f64,
    ema: f64,
}

impl BollingerMeanReversion {
    pub fn new(bar_ms: i64, ema_period: usize, bb_mult: f64) -> Self {
        Self {
            bar_ms,
            ema_period,
            bb_mult,
            cur_bar_idx: None,
            cur_close: 0.0,
            bars: Vec::new(),
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
                // 第一个 bar
                self.cur_bar_idx = Some(bar_idx);
                self.cur_close = price;
                Vec::new()
            }
            Some(cur_idx) if cur_idx == bar_idx => {
                // 同一 bar，更新
                self.cur_close = price;
                Vec::new()
            }
            Some(_) => {
                // 新 bar 开始，关闭旧 bar
                let close = self.cur_close;
                let ema = if let Some(last) = self.bars.last() {
                    Self::update_ema(last.ema, close, self.ema_period)
                } else {
                    close
                };

                let new_bar = Bar { close, ema };

                // 计算标准差（基于最近 ema_period 个已关闭 bar + 当前 bar）
                let mut sigs = Vec::new();
                if self.bars.len() + 1 >= self.ema_period {
                    let start = self.bars.len().saturating_sub(self.ema_period - 1);
                    let sum_sq: f64 = self.bars[start..].iter()
                        .map(|b| (b.close - b.ema).powi(2))
                        .sum::<f64>()
                        + (new_bar.close - new_bar.ema).powi(2);
                    let std = (sum_sq / self.ema_period as f64).sqrt();

                    let upper = new_bar.ema + self.bb_mult * std;
                    let lower = new_bar.ema - self.bb_mult * std;

                    if new_bar.close > upper {
                        sigs.push(Signal::new(
                            SignalKind::TrendRegime,
                            t.ts,
                            self.name(),
                            serde_json::json!({
                                "regime": "overbought",
                                "price": new_bar.close,
                                "ema": new_bar.ema,
                                "std": std,
                            }),
                        ));
                    } else if new_bar.close < lower {
                        sigs.push(Signal::new(
                            SignalKind::TrendRegime,
                            t.ts,
                            self.name(),
                            serde_json::json!({
                                "regime": "oversold",
                                "price": new_bar.close,
                                "ema": new_bar.ema,
                                "std": std,
                            }),
                        ));
                    }
                }

                self.bars.push(new_bar);

                // 启动新 bar
                self.cur_bar_idx = Some(bar_idx);
                self.cur_close = price;

                sigs
            }
        }
    }
}

impl SignalPlugin for BollingerMeanReversion {
    fn name(&self) -> &'static str {
        "BollingerMeanReversion"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl BollingerMeanReversion {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("bar_ms", 3_600_000.0) as i64,
            g("ema_period", 20.0) as usize,
            g("bb_mult", 2.0),
        )
    }
}
