//! 大单流速率信号：AGGR 听觉信号的量化替代。
//!
//! 规则（手册 2.5）：`large_trade_rate = 过去 window_secs 内 ≥ size_large_usd 的笔数/秒`，
//! 速率 ≥ surge_mult × 基线 → FlowSurge 预警（"听到声音切过去"）。
//!
//! 基线估计：窗口内大单计数的指数衰减均值（tau=10min），避开瞬时尖峰的滚动典型值；
//! 冷启动基线未定时不发信号（避免开局误报）。

use std::collections::VecDeque;
use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

/// 大单流速率信号插件。
pub struct LargeTradeFlow {
    /// 大单笔名义额阈值（USD）
    pub size_large_usd: f64,
    /// 速率窗口（秒）
    pub window_secs: u32,
    /// 激增倍数（当前速率 ≥ surge_mult × 基线速率）
    pub surge_mult: f64,
    /// 基线衰减常数（毫秒；默认 10min）
    tau_ms: f64,
    /// 窗口内大单时间戳（毫秒）
    hits: VecDeque<i64>,
    /// 基线：指数衰减的大单计数率（笔/秒）；None = 未初始化
    baseline: Option<f64>,
    /// 上次基线更新时间
    last_ts: Option<i64>,
    /// 上次发信号时间（防抖：一个窗口最多一次）
    last_fired: Option<i64>,
    /// 首个大单时间（冷启动热身用）
    first_ts: Option<i64>,
}

impl LargeTradeFlow {
    pub fn new(size_large_usd: f64, window_secs: u32, surge_mult: f64) -> Self {
        Self {
            size_large_usd,
            window_secs,
            surge_mult,
            tau_ms: 600_000.0,
            hits: VecDeque::new(),
            baseline: None,
            last_ts: None,
            last_fired: None,
            first_ts: None,
        }
    }

    /// 当前窗口速率（笔/秒）。
    pub fn rate(&self) -> f64 {
        self.hits.len() as f64 / self.window_secs as f64
    }
    /// 当前基线速率（None = 冷启动未定）。
    pub fn baseline(&self) -> Option<f64> {
        self.baseline
    }

    fn advance_baseline(&mut self, ts_ms: i64) {
        if let Some(last) = self.last_ts {
            let dt = (ts_ms - last).max(0) as f64;
            let decay = (-dt / self.tau_ms).exp();
            // 基线向"当前命中密度"缓慢回归：b' = b·decay + (窗口速率)·(1−decay)
            let b = self.baseline.unwrap_or(0.0) * decay + self.rate() * (1.0 - decay);
            self.baseline = Some(b);
        }
        self.last_ts = Some(ts_ms);
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Option<Signal> {
        let ts = t.ts.as_millis();
        // 只统计大单
        if t.notional() >= self.size_large_usd {
            self.hits.push_back(ts);
            if self.first_ts.is_none() {
                self.first_ts = Some(ts);
            }
        }
        // 滑出窗口的剔除
        let cutoff = ts - self.window_secs as i64 * 1000;
        while self.hits.front().is_some_and(|&h| h < cutoff) {
            self.hits.pop_front();
        }
        self.advance_baseline(ts);

        let base = self.baseline?;
        // 冷启动保护：热身未满一个 tau（10min）不发——基线未建立时恒定流会被误判激增
        let warm_ok = self
            .first_ts
            .is_some_and(|f0| ts - f0 >= self.tau_ms as i64);
        if !warm_ok || self.hits.len() < 3 || base <= 1e-9 {
            return None;
        }
        let rate = self.rate();
        if rate < self.surge_mult * base {
            return None;
        }
        // 防抖：一个窗口内只发一次
        if self
            .last_fired
            .is_some_and(|f| ts - f < self.window_secs as i64 * 1000)
        {
            return None;
        }
        self.last_fired = Some(ts);
        Some(Signal::new(
            SignalKind::FlowSurge,
            t.ts,
            self.name(),
            serde_json::json!({
                "rate": rate,
                "baseline": base,
                "mult": rate / base,
                "window_secs": self.window_secs,
                "size_large_usd": self.size_large_usd,
            }),
        ))
    }
}

impl SignalPlugin for LargeTradeFlow {
    fn name(&self) -> &'static str {
        "LargeTradeFlow"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t).into_iter().collect(),
            _ => Vec::new(),
        }
    }
}

impl LargeTradeFlow {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("size_large_usd", 200_000.0),
            g("window_secs", 10.0) as u32,
            g("surge_mult", 5.0),
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

    fn tr(ts_ms: i64, px: f64, qty: f64) -> tcore::Trade {
        tcore::Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(px),
            qty: Qty::from_f64(qty),
            is_buyer_maker: false,
        }
    }

    #[test]
    fn surge_fires_on_rate_spike() {
        // 基线阶段：稀疏大单（10s 窗口 1-2 笔）
        let mut f = LargeTradeFlow::new(200_000.0, 10, 5.0);
        let ctx = Ctx::default();
        // 12 分钟基线期（> tau=10min），每分钟 3 笔大单（速率 0.05/s 量级）
        for m in 0..12 {
            for k in 0..3 {
                let ts = (m * 60 + k * 20) * 1000;
                f.on_event(&Event::Trade(tr(ts, 67000.0, 10.0)), &ctx); // 67万 > 20万
            }
        }
        // 突发：10s 内 10 笔大单（速率 1/s，远超基线）
        let mut fired = 0;
        for k in 0..10 {
            let ts = 720_000 + k * 900;
            let sigs = f.on_event(&Event::Trade(tr(ts, 67000.0, 10.0)), &ctx);
            fired += sigs.len();
            for s in &sigs {
                assert_eq!(s.kind, SignalKind::FlowSurge);
                assert!(s.payload["mult"].as_f64().unwrap() >= 5.0);
            }
        }
        assert!(fired >= 1, "应至少触发一次 FlowSurge");
    }

    #[test]
    fn no_surge_on_steady_flow() {
        let mut f = LargeTradeFlow::new(200_000.0, 10, 5.0);
        let ctx = Ctx::default();
        // 持续均匀大单流：速率恒定，无激增
        for i in 0..600 {
            let sigs = f.on_event(&Event::Trade(tr(i * 1000, 67000.0, 10.0)), &ctx);
            // 热身期后稳定流不应触发
            if i > 60 {
                assert!(sigs.is_empty(), "稳定流不应触发（i={i}）");
            }
        }
    }

    #[test]
    fn small_trades_ignored() {
        let mut f = LargeTradeFlow::new(200_000.0, 10, 5.0);
        let ctx = Ctx::default();
        // 全是小单：永不触发
        for i in 0..1000 {
            let sigs = f.on_event(&Event::Trade(tr(i * 100, 67000.0, 0.1)), &ctx);
            assert!(sigs.is_empty());
        }
        assert_eq!(f.rate(), 0.0);
    }

    #[test]
    fn debounce_one_per_window() {
        let mut f = LargeTradeFlow::new(200_000.0, 10, 3.0);
        let ctx = Ctx::default();
        // 基线（12 分钟 > tau）
        for m in 0..12 {
            for k in 0..3 {
                f.on_event(
                    &Event::Trade(tr((m * 60 + k * 20) * 1000, 67000.0, 10.0)),
                    &ctx,
                );
            }
        }
        // 突发 20 笔：一个窗口内最多发一次
        let mut fired = 0;
        for k in 0..20 {
            fired += f
                .on_event(&Event::Trade(tr(720_000 + k * 400, 67000.0, 10.0)), &ctx)
                .len();
        }
        assert_eq!(fired, 1, "窗口内防抖，只发一次，实际 {fired}");
    }
}
