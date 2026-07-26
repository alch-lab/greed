//! 挤兑完成检测器（F3，独立入场信号）
//!
//! 与 v1 的"价格/EMA 位移"不同源：本信号看**持仓出清**。
//!
//! 多头挤兑做多（做空镜像）：
//! 1. 60m 价格跌幅 ≥ price_drop_pct（默认 2%）——有级别的急跌
//! 2. 60m OI 降幅 ≥ oi_flush_pct（默认 2%）——多头被强平出清
//! 3. 现价自窗口低点回升 ≥ rebound_pct（默认 0.3%）——跌势停止
//! 4. 近 15m OI 降幅 < oi_calm_pct（默认 0.5%）——出清已结束
//! → 发 f3_long（止损锚 = 窗口低点外 0.3%，TP 参考 = 5m EMA20）
//!
//! 空头挤压做空：60m 涨 ≥2% + OI 降 ≥2%（空头被挤压出清）+ 自高点回落。
//!
//! 每个方向一次性触发：条件消失后才允许再次触发（触发器另有冷却）。

use std::collections::VecDeque;
use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

struct Bar {
    high: f64,
    low: f64,
    close: f64,
}

pub struct SqueezeCompletion {
    bar_ms: i64,
    ema_p: usize,
    /// 急跌/急涨窗口（bar 数，默认 12 = 60m）
    window_bars: usize,
    price_drop_pct: f64,
    oi_flush_pct: f64,
    rebound_pct: f64,
    /// OI 平静窗口（毫秒，默认 15m）
    oi_calm_ms: i64,
    oi_calm_pct: f64,

    // 当前 bar
    cur_idx: Option<i64>,
    cur_high: f64,
    cur_low: f64,
    cur_close: f64,
    bars: VecDeque<Bar>,
    ema: Option<f64>,

    // OI 环形缓冲 (ts_ms, oi_usd)
    oi_ticks: VecDeque<(i64, f64)>,

    // 一次性触发锁（条件消失后复位）
    fired_long: bool,
    fired_short: bool,
}

impl SqueezeCompletion {
    pub fn new(
        bar_ms: i64,
        ema_p: usize,
        window_bars: usize,
        price_drop_pct: f64,
        oi_flush_pct: f64,
        rebound_pct: f64,
        oi_calm_ms: i64,
        oi_calm_pct: f64,
    ) -> Self {
        Self {
            bar_ms,
            ema_p,
            window_bars,
            price_drop_pct,
            oi_flush_pct,
            rebound_pct,
            oi_calm_ms,
            oi_calm_pct,
            cur_idx: None,
            cur_high: 0.0,
            cur_low: 0.0,
            cur_close: 0.0,
            bars: VecDeque::new(),
            ema: None,
            oi_ticks: VecDeque::new(),
            fired_long: false,
            fired_short: false,
        }
    }

    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("bar_ms", 300_000.0) as i64,
            g("ema_p", 20.0) as usize,
            g("window_bars", 12.0) as usize,
            g("price_drop_pct", 0.02),
            g("oi_flush_pct", 0.02),
            g("rebound_pct", 0.003),
            g("oi_calm_ms", 900_000.0) as i64,
            g("oi_calm_pct", 0.005),
        )
    }

    fn update_ema(prev: f64, price: f64, period: usize) -> f64 {
        let k = 2.0 / (period as f64 + 1.0);
        price * k + prev * (1.0 - k)
    }

    fn on_oi(&mut self, ts: i64, oi_usd: f64) {
        self.oi_ticks.push_back((ts, oi_usd));
        // 缓冲保留 90m 足够覆盖 60m 窗口与 15m 平静窗
        while let Some(&(t0, _)) = self.oi_ticks.front() {
            if t0 < ts - 5_400_000 {
                self.oi_ticks.pop_front();
            } else {
                break;
            }
        }
    }

    /// ts 时刻往前 window_ms 的 OI 变化率（取不晚于起点的最近样本为基准）。
    fn oi_chg(&self, ts: i64, window_ms: i64) -> Option<f64> {
        let base_ts = ts - window_ms;
        let mut base: Option<f64> = None;
        let mut last: Option<f64> = None;
        for &(t, v) in self.oi_ticks.iter() {
            if t <= base_ts {
                base = Some(v);
            }
            last = Some(v);
        }
        match (base, last) {
            (Some(b), Some(l)) if b > 0.0 => Some((l - b) / b),
            _ => None,
        }
    }

    /// bar 收盘：评估挤兑条件。
    fn on_bar_close(&mut self, ts: tcore::types::Timestamp) -> Vec<Signal> {
        // 维护 EMA 与窗口
        self.ema = Some(match self.ema {
            Some(e) => Self::update_ema(e, self.cur_close, self.ema_p),
            None => self.cur_close,
        });
        self.bars.push_back(Bar {
            high: self.cur_high,
            low: self.cur_low,
            close: self.cur_close,
        });
        while self.bars.len() > self.window_bars + 1 {
            self.bars.pop_front();
        }
        if self.bars.len() < self.window_bars + 1 {
            return Vec::new();
        }
        let Some(ema) = self.ema else { return Vec::new() };

        // 窗口统计（不含刚收盘的这根的起点：用 window_bars+1 根，首尾对比）
        let first = &self.bars[0];
        let cur = self.cur_close;
        let win_low = self.bars.iter().map(|b| b.low).fold(f64::INFINITY, f64::min);
        let win_high = self
            .bars
            .iter()
            .map(|b| b.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if first.close <= 0.0 {
            return Vec::new();
        }
        let px_chg = (cur - first.close) / first.close;
        let ts_ms = ts.as_millis();
        let window_ms = self.window_bars as i64 * self.bar_ms;
        let Some(oi_chg_win) = self.oi_chg(ts_ms, window_ms) else {
            return Vec::new();
        };
        let Some(oi_chg_calm) = self.oi_chg(ts_ms, self.oi_calm_ms) else {
            return Vec::new();
        };

        // 多头挤兑完成：急跌 + OI 出清 + 止跌回升 + OI 转稳
        let long_setup = px_chg <= -self.price_drop_pct
            && oi_chg_win <= -self.oi_flush_pct
            && cur >= win_low * (1.0 + self.rebound_pct)
            && oi_chg_calm >= -self.oi_calm_pct;
        // 空头挤压完成：急涨 + OI 出清（空头被挤）+ 冲高回落 + OI 转稳
        let short_setup = px_chg >= self.price_drop_pct
            && oi_chg_win <= -self.oi_flush_pct
            && cur <= win_high * (1.0 - self.rebound_pct)
            && oi_chg_calm >= -self.oi_calm_pct;

        let mut sigs = Vec::new();
        if long_setup && !self.fired_long {
            sigs.push(Signal::new(
                SignalKind::Other,
                ts,
                self.name(),
                serde_json::json!({
                    "regime": "f3_long",
                    "price": cur,
                    "ema": ema,
                    "sl": win_low * (1.0 - self.rebound_pct),
                    "px_chg": px_chg,
                    "oi_chg": oi_chg_win,
                }),
            ));
            self.fired_long = true;
        } else if !long_setup {
            self.fired_long = false;
        }
        if short_setup && !self.fired_short {
            sigs.push(Signal::new(
                SignalKind::Other,
                ts,
                self.name(),
                serde_json::json!({
                    "regime": "f3_short",
                    "price": cur,
                    "ema": ema,
                    "sl": win_high * (1.0 + self.rebound_pct),
                    "px_chg": px_chg,
                    "oi_chg": oi_chg_win,
                }),
            ));
            self.fired_short = true;
        } else if !short_setup {
            self.fired_short = false;
        }
        sigs
    }
}

impl SignalPlugin for SqueezeCompletion {
    fn name(&self) -> &'static str {
        "SqueezeCompletion"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Oi(o) => {
                self.on_oi(o.ts.as_millis(), o.oi_usd);
                Vec::new()
            }
            Event::Trade(t) => {
                let ts = t.ts.as_millis();
                let price = t.price.to_f64();
                let idx = ts / self.bar_ms * self.bar_ms;
                match self.cur_idx {
                    None => {
                        self.cur_idx = Some(idx);
                        self.cur_high = price;
                        self.cur_low = price;
                        self.cur_close = price;
                        Vec::new()
                    }
                    Some(cur) if cur == idx => {
                        self.cur_high = self.cur_high.max(price);
                        self.cur_low = self.cur_low.min(price);
                        self.cur_close = price;
                        Vec::new()
                    }
                    Some(_) => {
                        let sigs = self.on_bar_close(t.ts);
                        self.cur_idx = Some(idx);
                        self.cur_high = price;
                        self.cur_low = price;
                        self.cur_close = price;
                        sigs
                    }
                }
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
    use tcore::{OiTick, Trade};

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }
    fn trade(ts_ms: i64, price: f64) -> Event {
        Event::Trade(Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: sym(),
            price: Price::from_f64(price),
            qty: Qty::from_f64(0.1),
            is_buyer_maker: false,
        })
    }
    fn oi(ts_ms: i64, usd: f64) -> Event {
        Event::Oi(OiTick {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: sym(),
            oi_usd: usd,
        })
    }

    /// 构造多头挤兑：60m 跌 3.3% + OI 降 2.3% 后走平 + 回升 0.5%，应发 f3_long。
    #[test]
    fn long_squeeze_fires() {
        let mut s = SqueezeCompletion::from_params(&serde_json::json!({}));
        let bar = 300_000i64;
        // OI：0..=10 时刻从 100 降到 97.5（60m 降 2.3%），之后走平（出清结束）
        for i in 0..=12 {
            let v = if i <= 10 { 100.0 - i as f64 * 0.25 } else { 97.5 };
            s.on_event(&oi(i * bar, v), &Ctx::default());
        }
        // 价格：13 根 bar，先 12 根阴跌（累计 -2.5%），第 13 根回升 0.5%
        let mut px = 100_000.0;
        for i in 0..12 {
            px *= 1.0 - 0.033 / 12.0;
            // 每根 bar 内两笔（形成 high/low），收盘价单调降
            s.on_event(&trade(i * bar + 1, px * 1.0005), &Ctx::default());
            s.on_event(&trade(i * bar + 2, px), &Ctx::default());
        }
        let rebound = px * 1.005;
        s.on_event(&trade(12 * bar + 1, rebound), &Ctx::default());
        // 第 13 根 bar 收盘（下一根的第一笔触发结算）→ 应触发
        let sigs = s.on_event(&trade(13 * bar + 1, rebound), &Ctx::default());
        assert!(
            sigs.iter()
                .any(|g| g.payload.get("regime").unwrap() == "f3_long"),
            "应触发 f3_long，实际 {:?}",
            sigs.len()
        );
        // 一次性：同条件不重复发
        let sigs2 = s.on_event(&trade(14 * bar + 1, rebound), &Ctx::default());
        assert!(sigs2.is_empty());
    }

    /// 只有价格下跌、OI 没出清：不发信号。
    #[test]
    fn no_flush_no_signal() {
        let mut s = SqueezeCompletion::from_params(&serde_json::json!({}));
        let bar = 300_000i64;
        for i in 0..=12 {
            s.on_event(&oi(i * bar, 100.0), &Ctx::default()); // OI 不动
        }
        let mut px = 100_000.0;
        for i in 0..12 {
            px *= 1.0 - 0.025 / 12.0;
            s.on_event(&trade(i * bar + 1, px * 1.0005), &Ctx::default());
            s.on_event(&trade(i * bar + 2, px), &Ctx::default());
        }
        let sigs = s.on_event(&trade(12 * bar + 1, px * 1.005), &Ctx::default());
        assert!(sigs.is_empty());
    }
}
