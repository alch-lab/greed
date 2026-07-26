//! 总 Delta 阈值信号（PR-9）。
//!
//! 30m 聚合 Delta（USD 名义额，taker 买为正）为最小单元：
//! ```text
//! AccDelta(t) = Σ_{i∈[t−W,t]} AggDelta_30m(i)      窗口 W 默认 24h
//! ```
//! - **档位**：|AccDelta| ≥ t1(1B) WATCH / t2(2B) ENTRY_OK / t3(3B) HIGH_Q / t4(3.5B) EXTREME，
//!   档位变化时发 `DeltaTier` 信号。
//! - **趋势状态机**：回看 N(14) 天的日度多空极值，
//!   ratio = maxAcc_short / max(maxAcc_long, ε)；
//!   > r_trend(2.0) → TREND_DOWN，< 1/r_trend → TREND_UP，其余 RANGE；
//!   状态迁移时发 `TrendRegime` 信号（payload.state）。
//! - **连续击穿（单边熔断）**：同向 AccDelta 连续 K(3) 次破 t2 且价格未反转 r_rev(1.5%)，
//!   发 `Other` 信号（payload.circuit = "one_sided"）。

use std::collections::VecDeque;
use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

const BUCKET_MS: i64 = 30 * 60_000; // 30m 聚合单元
const DAY_MS: i64 = 86_400_000;

/// 档位枚举（由低到高）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Tier {
    None,
    Watch,
    EntryOk,
    HighQ,
    Extreme,
}

impl Tier {
    fn as_str(self) -> &'static str {
        match self {
            Tier::None => "NONE",
            Tier::Watch => "WATCH",
            Tier::EntryOk => "ENTRY_OK",
            Tier::HighQ => "HIGH_Q",
            Tier::Extreme => "EXTREME",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrendState {
    Range,
    Up,
    Down,
}

impl TrendState {
    fn as_str(self) -> &'static str {
        match self {
            TrendState::Range => "range",
            TrendState::Up => "trend_up",
            TrendState::Down => "trend_down",
        }
    }
}

/// 日度极值记录。
#[derive(Debug, Clone, Copy)]
struct DayExtremes {
    max_long: f64,  // AccDelta 正极值（USD）
    max_short: f64, // AccDelta 负极值绝对值（USD）
}

pub struct AggDeltaTier {
    /// 阈值档位（USD，如 1e9 = 1B）
    t1: f64,
    t2: f64,
    t3: f64,
    t4: f64,
    /// AccDelta 窗口（小时）
    window_hours: u32,
    /// 趋势不对称阈值
    r_trend: f64,
    /// 趋势回看天数
    trend_days: u32,
    /// 连续击穿次数阈值
    k_break: u32,
    /// 反转判定幅度
    r_rev: f64,

    // ---- 内部状态 ----
    /// 当前 30m 桶
    cur_bucket: Option<i64>,
    cur_delta: f64,
    /// 窗口内已收桶 (bucket_start, delta_usd)
    buckets: VecDeque<(i64, f64)>,
    acc_delta: f64,
    /// 日度极值
    cur_day: Option<i64>,
    cur_day_long: f64,
    cur_day_short: f64,
    days: VecDeque<DayExtremes>,
    /// 当前档位 / 趋势状态
    tier: Tier,
    trend: TrendState,
    /// 连续击穿跟踪（方向、次数、首次击穿价）
    break_side: i8,
    break_count: u32,
    break_price: f64,
    circuit_fired: bool,
}

impl AggDeltaTier {
    pub fn new(
        t1: f64,
        t2: f64,
        t3: f64,
        t4: f64,
        window_hours: u32,
        r_trend: f64,
    ) -> Self {
        Self {
            t1,
            t2,
            t3,
            t4,
            window_hours,
            r_trend,
            trend_days: 14,
            k_break: 3,
            r_rev: 0.015,
            cur_bucket: None,
            cur_delta: 0.0,
            buckets: VecDeque::new(),
            acc_delta: 0.0,
            cur_day: None,
            cur_day_long: 0.0,
            cur_day_short: 0.0,
            days: VecDeque::new(),
            tier: Tier::None,
            trend: TrendState::Range,
            break_side: 0,
            break_count: 0,
            break_price: 0.0,
            circuit_fired: false,
        }
    }

    fn tier_of(&self, acc: f64) -> Tier {
        let a = acc.abs();
        if a >= self.t4 {
            Tier::Extreme
        } else if a >= self.t3 {
            Tier::HighQ
        } else if a >= self.t2 {
            Tier::EntryOk
        } else if a >= self.t1 {
            Tier::Watch
        } else {
            Tier::None
        }
    }

    /// 关闭当前桶并更新 AccDelta、日极值、趋势状态机。
    /// 返回 (档位变化, 趋势变化)。
    fn close_bucket(&mut self) -> (bool, bool) {
        self.buckets.push_back((self.cur_bucket.unwrap_or(0), self.cur_delta));
        self.acc_delta += self.cur_delta;
        self.cur_delta = 0.0;

        // 窗口修剪
        let window_ms = self.window_hours as i64 * 3_600_000;
        let cutoff = self.cur_bucket.unwrap_or(0) - window_ms;
        while let Some(&(ts, d)) = self.buckets.front() {
            if ts < cutoff {
                self.buckets.pop_front();
                self.acc_delta -= d;
            } else {
                break;
            }
        }

        // 日极值
        let day = self.cur_bucket.unwrap_or(0) / DAY_MS;
        if self.cur_day != Some(day) {
            if self.cur_day.is_some() {
                self.days.push_back(DayExtremes {
                    max_long: self.cur_day_long,
                    max_short: self.cur_day_short,
                });
                while self.days.len() > self.trend_days as usize {
                    self.days.pop_front();
                }
            }
            self.cur_day = Some(day);
            self.cur_day_long = 0.0;
            self.cur_day_short = 0.0;
        }
        if self.acc_delta > self.cur_day_long {
            self.cur_day_long = self.acc_delta;
        }
        if -self.acc_delta > self.cur_day_short {
            self.cur_day_short = -self.acc_delta;
        }

        // 趋势状态机（含今日在内的极值）
        let mut max_long = self.cur_day_long;
        let mut max_short = self.cur_day_short;
        for d in &self.days {
            max_long = max_long.max(d.max_long);
            max_short = max_short.max(d.max_short);
        }
        let ratio = max_short / max_long.max(1.0);
        let new_trend = if ratio > self.r_trend {
            TrendState::Down
        } else if ratio < 1.0 / self.r_trend {
            TrendState::Up
        } else {
            TrendState::Range
        };
        let trend_changed = new_trend != self.trend;
        self.trend = new_trend;

        // 档位
        let new_tier = self.tier_of(self.acc_delta);
        let tier_changed = new_tier != self.tier;
        self.tier = new_tier;

        (tier_changed, trend_changed)
    }

    /// 连续击穿检测：同向破 t2 累计 K 次且价格未反转 r_rev → 熔断信号。
    fn check_circuit(&mut self, price: f64) -> bool {
        if self.circuit_fired || self.tier < Tier::EntryOk {
            // 档位回落到 t2 以下时重置连击
            if self.tier < Tier::EntryOk && self.break_count > 0 {
                self.break_count = 0;
                self.break_side = 0;
            }
            return false;
        }
        let side: i8 = if self.acc_delta > 0.0 { 1 } else { -1 };
        // 价格反转判定：从首次击穿价反向运行 r_rev
        let reversed = if self.break_side != 0 && self.break_price > 0.0 {
            let ret = (price - self.break_price) / self.break_price;
            (self.break_side == 1 && ret <= -self.r_rev)
                || (self.break_side == -1 && ret >= self.r_rev)
        } else {
            false
        };
        if side != self.break_side || reversed {
            self.break_side = side;
            self.break_count = 1;
            self.break_price = price;
        } else {
            self.break_count += 1;
        }
        if self.break_count >= self.k_break {
            self.circuit_fired = true;
            return true;
        }
        false
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Vec<Signal> {
        let ts = t.ts.as_millis();
        let price = t.price.to_f64();
        // taker 买为正：is_buyer_maker=true 表示买方是 maker → taker 为卖方 → 负
        let signed = if t.is_buyer_maker { -1.0 } else { 1.0 } * t.qty.to_f64() * price;

        let bucket = ts / BUCKET_MS * BUCKET_MS;
        let mut sigs = Vec::new();

        if self.cur_bucket != Some(bucket) {
            if self.cur_bucket.is_some() {
                let (tier_changed, trend_changed) = self.close_bucket();
                if tier_changed {
                    sigs.push(Signal::new(
                        SignalKind::DeltaTier,
                        t.ts,
                        self.name(),
                        serde_json::json!({
                            "tier": self.tier.as_str(),
                            "acc_delta_usd": self.acc_delta,
                            "side": if self.acc_delta > 0.0 { "long" } else { "short" },
                        }),
                    ));
                }
                if trend_changed {
                    sigs.push(Signal::new(
                        SignalKind::TrendRegime,
                        t.ts,
                        self.name(),
                        serde_json::json!({
                            "state": self.trend.as_str(),
                            "acc_delta_usd": self.acc_delta,
                        }),
                    ));
                }
                if self.check_circuit(price) {
                    sigs.push(Signal::new(
                        SignalKind::Other,
                        t.ts,
                        self.name(),
                        serde_json::json!({
                            "circuit": "one_sided",
                            "side": if self.break_side == 1 { "long" } else { "short" },
                            "breaks": self.break_count,
                        }),
                    ));
                }
            }
            self.cur_bucket = Some(bucket);
        }
        self.cur_delta += signed;
        sigs
    }
}

impl SignalPlugin for AggDeltaTier {
    fn name(&self) -> &'static str {
        "AggDeltaTier"
    }
    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t),
            _ => Vec::new(),
        }
    }
}

impl AggDeltaTier {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("t1", 1.0e9),
            g("t2", 2.0e9),
            g("t3", 3.0e9),
            g("t4", 3.5e9),
            g("window_hours", 24.0) as u32,
            g("r_trend", 2.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

    fn trade(ts_ms: i64, price: f64, qty: f64, buyer_maker: bool) -> tcore::Trade {
        tcore::Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker: buyer_maker,
        }
    }

    /// 构造连续 taker 买（buyer_maker=false）推动 AccDelta 上穿档位。
    #[test]
    fn tier_escalates_on_buy_pressure() {
        // t1/t2 缩小以便测试（100M / 200M USD）
        let mut s = AggDeltaTier::new(1.0e8, 2.0e8, 3.0e8, 3.5e8, 24, 2.0);
        let mut tiers = Vec::new();
        // 每个 30m 桶 taker 买 1.5e8 USD（> t1，两桶后 > t2）
        for b in 0..3 {
            let base = b * BUCKET_MS;
            // 本桶内成交
            for sig_ts in [base + 1_000, base + 2_000] {
                for sig in s.on_trade(&trade(sig_ts, 67_000.0, 1_000.0, false)) {
                    if let Some(t) = sig.payload.get("tier") {
                        tiers.push(t.as_str().unwrap().to_string());
                    }
                }
            }
            // 下一桶首笔触发收桶
            for sig in s.on_trade(&trade(base + BUCKET_MS + 500, 67_000.0, 1.0, false)) {
                if let Some(t) = sig.payload.get("tier") {
                    tiers.push(t.as_str().unwrap().to_string());
                }
            }
        }
        assert!(tiers.contains(&"WATCH".to_string()), "应出现 WATCH: {:?}", tiers);
        assert!(tiers.contains(&"ENTRY_OK".to_string()), "应出现 ENTRY_OK: {:?}", tiers);
    }

    /// 卖方持续压制 → ratio > r_trend → TREND_DOWN。
    #[test]
    fn trend_down_on_short_dominance() {
        let mut s = AggDeltaTier::new(1.0e8, 2.0e8, 3.0e8, 3.5e8, 24, 2.0);
        let mut states = Vec::new();
        // 10 个桶持续 taker 卖（buyer_maker=true），每桶 -2e8
        for b in 0..10 {
            let base = b * BUCKET_MS;
            s.on_trade(&trade(base + 1_000, 67_000.0, 1_500.0, true));
            for sig in s.on_trade(&trade(base + BUCKET_MS + 500, 67_000.0, 1.0, true)) {
                if let Some(st) = sig.payload.get("state") {
                    states.push(st.as_str().unwrap().to_string());
                }
            }
        }
        assert!(
            states.contains(&"trend_down".to_string()),
            "空方主导应迁移到 trend_down: {:?}",
            states
        );
    }

    /// 窗口修剪：旧桶滑出窗口后 AccDelta 回落、档位降级。
    #[test]
    fn window_prune_deescalates_tier() {
        let mut s = AggDeltaTier::new(1.0e8, 2.0e8, 3.0e8, 3.5e8, 1, 2.0); // 窗口 1h = 2 桶
        // 桶0：大买单 +3e8（HIGH_Q）
        s.on_trade(&trade(1_000, 67_000.0, 2_000.0, false));
        let mut tiers = Vec::new();
        // 桶1、2、3、4 空过（每桶首笔触发收桶；桶0 在桶3 收盘时滑出 1h 窗口）
        for b in 1..=4i64 {
            for sig in s.on_trade(&trade(b * BUCKET_MS + 500, 67_000.0, 1.0, false)) {
                if let Some(t) = sig.payload.get("tier") {
                    tiers.push(t.as_str().unwrap().to_string());
                }
            }
        }
        // 窗口 1h：桶0 滑出后 AccDelta ≈ 0 → 档位回落 NONE
        assert_eq!(tiers.last().unwrap(), "NONE", "旧桶滑出后应降级: {:?}", tiers);
    }
}
