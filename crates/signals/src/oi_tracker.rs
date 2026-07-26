//! OI 变化追踪信号（F2 的数据腿）。
//!
//! 消费 `Event::Oi`（5m 粒度），维护窗口环形缓冲，
//! 每个 OI 刻度发出一条 OiQuadrant 信号，载荷为窗口 OI 变化率：
//! `oi_chg = (now - window_start) / window_start`。
//! 供 `OiConfirmFilter` 做"是否已去杠杆"的裁决；不直接产生交易方向。

use std::collections::VecDeque;
use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

pub struct OiTracker {
    /// 回看窗口（毫秒），默认 30 分钟
    window_ms: i64,
    /// (ts_ms, oi_usd)
    ticks: VecDeque<(i64, f64)>,
}

impl OiTracker {
    pub fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            ticks: VecDeque::new(),
        }
    }

    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(g("window_ms", 1_800_000.0) as i64)
    }
}

impl SignalPlugin for OiTracker {
    fn name(&self) -> &'static str {
        "OiTracker"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        let Event::Oi(o) = ev else {
            return Vec::new();
        };
        let ts = o.ts.as_millis();
        self.ticks.push_back((ts, o.oi_usd));
        while let Some(&(t0, _)) = self.ticks.front() {
            if t0 < ts - self.window_ms {
                self.ticks.pop_front();
            } else {
                break;
            }
        }
        // 窗口内最老样本作为基准；不足窗口一半时不发信号（启动期）
        let Some(&(t0, oi0)) = self.ticks.front() else {
            return Vec::new();
        };
        if ts - t0 < self.window_ms / 2 || oi0 <= 0.0 {
            return Vec::new();
        }
        let chg = (o.oi_usd - oi0) / oi0;
        vec![Signal::new(
            SignalKind::OiQuadrant,
            o.ts,
            self.name(),
            serde_json::json!({"oi_chg": chg, "oi_usd": o.oi_usd, "window_ms": self.window_ms}),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Symbol, Timestamp};
    use tcore::OiTick;

    fn oi(ts_ms: i64, usd: f64) -> Event {
        Event::Oi(OiTick {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            oi_usd: usd,
        })
    }

    #[test]
    fn emits_window_change_after_warmup() {
        let mut tr = OiTracker::new(1_800_000); // 30m
        // 启动期（不足半窗）无信号
        assert!(tr.on_event(&oi(0, 100.0), &Ctx::default()).is_empty());
        assert!(tr.on_event(&oi(600_000, 100.0), &Ctx::default()).is_empty());
        // 满半窗后发信号：OI 降 5%
        let sigs = tr.on_event(&oi(1_000_000, 95.0), &Ctx::default());
        assert_eq!(sigs.len(), 1);
        let chg = sigs[0].payload.get("oi_chg").unwrap().as_f64().unwrap();
        assert!((chg - (-0.05)).abs() < 1e-9);
        // 窗口滑动：基准更新
        let sigs = tr.on_event(&oi(2_000_000, 90.0), &Ctx::default());
        let chg = sigs[0].payload.get("oi_chg").unwrap().as_f64().unwrap();
        // 基准 = 600_000 时刻的 100（0 时刻样本已滑出 30m 窗口）
        assert!((chg - (-0.10)).abs() < 1e-9);
    }
}
