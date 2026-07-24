//! 扳机插件：力竭反转三条件
//!
//! 数学本质：三条件 = "lambda 衰减 + 净流变号"的可计算形式——
//! 放量（vol surge）+ 不推进（价格停滞）+ Delta 反转（净流变号），
//! 即 Kyle's lambda 在吸收点趋向零的检测。
//!
//! 三条件（做多；做空镜像）：
//! 1. **放量快速**（必要）：V_now = 链均量/链时长 ≥ vol_mult_exh × V_base，且当前砖 duration ≤ dur_max
//! 2. **力竭不推进**（确认）：|实体|/range ≤ prog_ratio（小实体/长影线）
//! 3. **Delta 反转**（扳机）：当前砖 delta% 翻向且 |delta%| ≥ delta_flip_pct
//!
//! 基线估计器：滚动 lookback 块砖的**中位**速率（剔除 >中位×3 的大砖），按时段系数缩放。
//! 时段系数从 `ctx.flags["session"]` 读取（asia/europe/us/weekend）。
//!
//! 入场约束：**优先 maker 限价**（limit_price = 反转砖收盘价）；
//! 止损锚 = 针尖外 sl_min（有墙放墙外留待 OBI 接入）。
//!
//! 无效性基线：本扳机只负责产生意图；胜率是否超越 0.605 几何基线
//! 由 PR-7 报告的 ineffectiveness_gate 判定——**该判定是验收的硬门槛**。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use std::collections::VecDeque;
use tcore::plugin::{Ctx, OrderIntent, Signal, SignalKind, TriggerPlugin};
use tcore::types::{Price, Qty, Side, Symbol};

/// 占位扳机：从不触发。用于装配链路打通与测试。
pub struct NoopTrigger;

impl TriggerPlugin for NoopTrigger {
    fn name(&self) -> &'static str {
        "NoopTrigger"
    }
    fn should_fire(&self, _signals: &[Signal], _ctx: &Ctx) -> Option<OrderIntent> {
        None
    }
}

pub fn build_noop(_p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    Ok(Box::new(NoopTrigger))
}

// ============================================================================
// 基线估计器（剔除大砖 + 时段系数）
// ============================================================================

/// 砖速率的滚动基线：最近 lookback 块砖的速率中位数，剔除 >中位×3 的大砖后重估。
#[derive(Debug)]
struct RateBaseline {
    /// 最近砖的速率（USD/s）缓冲
    rates: VecDeque<f64>,
    lookback: usize,
}

impl RateBaseline {
    fn new(lookback: usize) -> Self {
        Self {
            rates: VecDeque::with_capacity(lookback + 1),
            lookback,
        }
    }

    fn push(&mut self, rate: f64) {
        if rate.is_finite() && rate > 0.0 {
            self.rates.push_back(rate);
            if self.rates.len() > self.lookback {
                self.rates.pop_front();
            }
        }
    }

    /// 剔除大砖后的典型速率（None = 样本不足）。
    ///
    /// 估计口径：先取全体中位 `med`，只保留 `r ≤ med×2.0` 的砖，再取保留集的中位。
    /// ×2.0（而非 ×3.0）是因为 3 连放量链会把混合中位推高 ~3×，×3 阈值剔不干净——
    /// ×2.0 确保基线锚定"平静时段"的典型速率，这正是手册 7.2 "剔除大砖"的本意。
    fn median_rate(&self) -> Option<f64> {
        if self.rates.len() < 20 {
            return None; // 冷启动样本不足
        }
        let mut v: Vec<f64> = self.rates.iter().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = v[v.len() / 2];
        let mut kept: Vec<f64> = v.into_iter().filter(|&r| r <= med * 2.0).collect();
        if kept.is_empty() {
            return Some(med);
        }
        kept.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some(kept[kept.len() / 2])
    }
}

// ============================================================================
// 力竭反转扳机
// ============================================================================

/// 三条件力竭反转扳机。
///
/// 消费 `BrickClosed` 信号，内部维护链状态与速率基线；
/// 在反转砖收盘时评估三条件，满足则产出限价 `OrderIntent`。
pub struct ExhaustionReversal {
    /// 放量倍数（×时段系数）
    pub vol_mult_exh: f64,
    /// Delta 反转阈值（%）
    pub delta_flip_pct: f64,
    /// Delta 反转阈值上限(%,超过视为恐慌，非力竭)
    pub delta_flip_max_pct: f64,
    /// 快速上限（毫秒/砖）
    pub dur_max_ms: u64,
    /// 力竭不推进比（|实体|/range 上限）
    pub prog_ratio: f64,
    /// 最小链长（≥3 连砖才评估力竭）
    pub min_chain: u32,
    /// 基线回溯砖数
    pub lookback: usize,
    /// 止损锚：针尖外缓冲（美元）
    pub sl_buffer_usd: f64,
    /// 时段系数（asia/europe/us/weekend）
    pub session_scaler: std::collections::HashMap<String, f64>,
    /// 冷却：扣扳机后 N 块砖内不再扣（0=关闭）
    pub cooldown_bricks: u32,

    // ---- 内部状态 ----
    baseline: RateBaseline,
    /// 当前同向链的累计（量 USD、时长 ms、砖数）
    chain_vol: f64,
    chain_dur: i64,
    chain_n: u32,
    /// 上一块砖的 delta%（判"翻向"用）
    prev_delta_pct: Option<f64>,
    /// 上一砖方向（链方向）
    prev_dir: i8,
    /// 冷却剩余砖数
    cooldown_left: u32,
}

impl ExhaustionReversal {
    fn session_of(ctx: &Ctx) -> &str {
        ctx.flag("session").unwrap_or("europe")
    }

    fn scaler(&self, session: &str) -> f64 {
        self.session_scaler.get(session).copied().unwrap_or(1.0)
    }

    /// 从砖载荷提取字段。
    fn f(p: &Json, k: &str) -> f64 {
        p.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0)
    }

    /// 评估一块砖（在 BrickClosed 信号到达时调用）。
    /// 返回 Some(intent) 表示扣扳机。
    fn on_brick(&mut self, sig: &Signal, ctx: &Ctx, symbol: &Symbol) -> Option<OrderIntent> {
        let p = &sig.payload;
        let dir = Self::f(p, "dir") as i8;
        let chain_index = Self::f(p, "chain_index") as u32;
        let is_reversal = p
            .get("is_reversal")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let vol = Self::f(p, "volume"); // 币
        let dur_ms = Self::f(p, "duration_ms").max(1.0) as i64;
        let close = Self::f(p, "close");
        let body = Self::f(p, "body_usd");
        let wick_up = Self::f(p, "wick_up_usd");
        let wick_dn = Self::f(p, "wick_down_usd");
        let range = body + wick_up + wick_dn;
        let (brick_high, brick_low) = if dir == 1 {
            (close + wick_up, close - body - wick_dn)
        } else {
            (close + body + wick_up, close - wick_dn)
        };
        let delta = Self::f(p, "delta"); // 币
        let delta_pct = if vol > 1e-9 { delta / vol * 100.0 } else { 0.0 };
        let close_px = Price::from_f64(close);

        // ---- 全量砖日志（反事实诊断用；评估日志之外，每块砖都打一行）----
        let body = Self::f(p, "body_usd");
        let wick_up = Self::f(p, "wick_up_usd");
        let wick_dn = Self::f(p, "wick_down_usd");
        let (brick_high, brick_low) = if dir == 1 {
            (close + wick_up, close - body - wick_dn)
        } else {
            (close + body + wick_up, close - wick_dn)
        };
        tracing::debug!(
            dir,
            chain_index,
            is_reversal,
            high = brick_high,
            low = brick_low,
            close,
            volume = vol,
            "renko 砖"
        );

        // ---- 基线维护（所有砖都进基线，估计器内部剔除大砖）----
        let brick_rate = if dur_ms > 0 {
            vol * close / (dur_ms as f64 / 1000.0)
        } else {
            0.0
        };
        self.baseline.push(brick_rate);
        let base = self.baseline.median_rate();

        // ---- 三条件评估（只在反转砖上扣扳机；评估用「被反转链」的旧累计）----
        // 语义：力竭的对象是**进入反转砖之前的那条同向链**——因此先在旧累计上评估，
        // 评估完再把反转砖记为新链第 1 块（见下方链状态维护）。
        let session = Self::session_of(ctx);
        let scaler = self.scaler(session);
        let mut fire = false;
        let mut fired_chain_n = 0u32; // 触发时被反转链的真实链长
        if is_reversal && self.chain_n >= self.min_chain && self.cooldown_left == 0 {
            // 只看"≥N 连后的反转"
            if let (Some(base), Some(prev_dp)) = (base, self.prev_delta_pct) {
                // 条件1：放量快速——被反转链的均速率 ≥ vol_mult × 基线×时段系数；
                // 且当前反转砖 duration ≤ dur_max（快速收口）
                let chain_rate = if self.chain_dur > 0 {
                    self.chain_vol / (self.chain_dur as f64 / 1000.0)
                } else {
                    0.0
                };
                let c1 = chain_rate >= self.vol_mult_exh * scaler * base
                    && dur_ms <= self.dur_max_ms as i64;
                // 条件2：力竭不推进——小实体/长影线
                let c2 = range > 1e-9 && body / range <= self.prog_ratio;
                // 条件3：Delta 翻向——前砖与链方向同号，当前砖反号且 |delta%| 达阈
                let c3 = prev_dp.signum() == -(dir as f64) // 前砖与"链原方向"同向（dir 已翻）
                    && delta_pct.signum() == dir as f64
                    && delta_pct.abs() >= self.delta_flip_pct
                    && delta_pct.abs() <= self.delta_flip_max_pct;
                fire = c1 && c2 && c3;
                if fire {
                    fired_chain_n = self.chain_n
                } else {
                    tracing::debug!(
                        chain_rate,
                        base,
                        need = self.vol_mult_exh * scaler * base,
                        dur_ms,
                        body_ratio = if range > 1e-9 { body / range } else { -1.0 },
                        delta_pct,
                        prev_dp,
                        chain_n = self.chain_n,
                        dir,
                        high = brick_high,
                        low = brick_low,
                        close,
                        c1,
                        c2,
                        c3,
                        "exhaustion 评估（未触发）"
                    );
                }
            }
        }

        // ---- 链状态维护（评估之后推进）----
        if chain_index == 1 {
            // 新链（首砖/反转砖）：重置累计为当前砖自身
            self.chain_vol = vol * close; // 近似 USD
            self.chain_dur = dur_ms;
            self.chain_n = 1;
        } else {
            self.chain_vol += vol * close;
            self.chain_dur += dur_ms;
            self.chain_n = chain_index;
        }
        self.prev_delta_pct = Some(delta_pct);
        self.prev_dir = dir;

        if !fire {
            return None;
        }
        self.cooldown_left = self.cooldown_bricks;
        tracing::info!(dir, close, "ExhaustionReversal 扣扳机");

        // ---- 产出限价意图（7.5-① maker 优先）----
        // 方向：反转砖方向 = 入场方向（跌砖反转 → 做多；涨砖反转 → 做空）
        let side = if dir == 1 { Side::Buy } else { Side::Sell };
        // 止损锚：针尖外 buffer（做多=砖 low − buffer；做空=砖 high + buffer）
        let sl = if dir == 1 {
            Price::from_f64(Self::f(p, "low") - self.sl_buffer_usd)
        } else {
            Price::from_f64(Self::f(p, "high") + self.sl_buffer_usd)
        };
        Some(OrderIntent {
            symbol: symbol.clone(),
            side,
            qty: Qty::ZERO,              // 仓位由引擎按 risk_pct/止损距离计算
            limit_price: Some(close_px), // maker 限价：反转砖收盘价
            stop_price: sl,
            tp1_price: None, // TP 由出场插件/色带决定
            reason: format!(
                "exhaustion_rev(c{}链 vol×{:.1} base {:.0} body/range {:.2} Δ% {:.1} session {})",
                fired_chain_n,
                self.vol_mult_exh,
                base.unwrap_or(0.0),
                if range > 1e-9 { body / range } else { 0.0 },
                delta_pct,
                session
            ),
            ts: sig.ts,
        })
    }
}

impl TriggerPlugin for ExhaustionReversal {
    fn name(&self) -> &'static str {
        "ExhaustionReversal"
    }

    fn should_fire(&self, _signals: &[Signal], _ctx: &Ctx) -> Option<OrderIntent> {
        None
    }

    /// 有状态扳机路径：逐块消费 BrickClosed 信号，维护链/基线，产出意图。
    fn on_signals(
        &mut self,
        signals: &[Signal],
        ctx: &Ctx,
        symbol: &Symbol,
    ) -> Option<OrderIntent> {
        for sig in signals {
            if sig.kind == SignalKind::BrickClosed {
                if let Some(intent) = self.on_brick(sig, ctx, symbol) {
                    return Some(intent);
                }
            }
        }
        None
    }
}

pub fn build_exhaustion(p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    let mut scaler = std::collections::HashMap::new();
    if let Some(obj) = p.get("session_scaler").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(x) = v.as_f64() {
                scaler.insert(k.clone(), x);
            }
        }
    } else {
        scaler.insert("asia".into(), 1.0);
        scaler.insert("europe".into(), 1.0);
        scaler.insert("us".into(), 0.8);
        scaler.insert("weekend".into(), 0.6);
    }
    let _ = g("t_ren_ticks", 200.0) * g("tick_usd", 0.5); // 砖尺寸预留（TP/波段换算）
    Ok(Box::new(ExhaustionReversal {
        vol_mult_exh: g("vol_mult_exh", 3.0),
        delta_flip_pct: g("delta_flip_pct", 8.0),
        dur_max_ms: g("dur_max_ms", 30_000.0) as u64,
        prog_ratio: g("prog_ratio", 0.4),
        min_chain: g("min_chain", 3.0) as u32,
        lookback: g("lookback", 200.0) as usize,
        sl_buffer_usd: g("sl_buffer_usd", 150.0),
        session_scaler: scaler,
        baseline: RateBaseline::new(g("lookback", 200.0) as usize),
        chain_vol: 0.0,
        chain_dur: 0,
        chain_n: 0,
        prev_delta_pct: None,
        prev_dir: 0,
        delta_flip_max_pct: 50.0,
        cooldown_bricks: 5,
        cooldown_left: 0,
    }))
}

// ============================================================================
// Tests（验收：黄金样本 + 三条件开关）
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::plugin::SignalKind;
    use tcore::types::Timestamp;

    #[allow(clippy::too_many_arguments)] // 测试辅助：砖字段多，保持显式更清晰
    pub(crate) fn brick_sig(
        idx: u64,
        dir: i8,
        chain_index: u32,
        is_reversal: bool,
        volume: f64,
        dur_ms: i64,
        close: f64,
        delta: f64,
        body: f64,
        wick_up: f64,
        wick_down: f64,
    ) -> Signal {
        Signal::new(
            SignalKind::BrickClosed,
            Timestamp::from_millis(idx as i64 * 1000),
            "RenkoBricks",
            serde_json::json!({
                "idx": idx, "dir": dir, "is_reversal": is_reversal, "chain_index": chain_index,
                "open": close - dir as f64 * body, "close": close,
                "high": close.max(close - dir as f64 * body) + wick_up,
                "low": close.min(close - dir as f64 * body) - wick_down,
                "body_usd": body, "wick_up_usd": wick_up, "wick_down_usd": wick_down,
                "duration_ms": dur_ms, "trades": 100, "volume": volume,
                "delta": delta, "net_notional": delta * close,
            }),
        )
    }

    fn ctx_with_session(s: &str) -> Ctx {
        let mut c = Ctx::default();
        c.flags.insert("session".into(), s.into());
        c
    }

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }

    /// 构造一个"基线已建立 + 3 连放量链 + 力竭反转砖"的场景，验证三条件齐备时触发。
    #[test]
    fn golden_exhaustion_fires() {
        let mut trg = ExhaustionReversal {
            vol_mult_exh: 3.0,
            delta_flip_pct: 8.0,
            dur_max_ms: 30_000,
            prog_ratio: 0.4,
            min_chain: 3,
            lookback: 200,
            sl_buffer_usd: 150.0,
            session_scaler: [("europe".into(), 1.0)].into_iter().collect(),
            baseline: RateBaseline::new(200),
            chain_vol: 0.0,
            chain_dur: 0,
            chain_n: 0,
            prev_delta_pct: None,
            prev_dir: 0,
        };
        let ctx = ctx_with_session("europe");

        // 1) 基线期：50 块慢速砖（速率 ~67 USD/s，远低于后面的链）
        for i in 0..50 {
            let s = brick_sig(
                i,
                if i % 2 == 0 { 1 } else { -1 },
                1,
                i > 0,
                0.1,
                100_000,
                67000.0,
                0.0,
                100.0,
                0.0,
                0.0,
            );
            assert!(trg.on_signals(&[s], &ctx, &sym()).is_none());
        }
        // 2) 3 连放量上涨链：每块 5 币 × 67000 / 20s ≈ 16,750 USD/s ≫ 3× 基线
        //    delta 同向为正（追高的买方）
        let base_idx = 50;
        for k in 1..=3u32 {
            let s = brick_sig(
                base_idx + k as u64,
                1,
                k,
                false,
                5.0,
                20_000,
                67000.0 + k as f64 * 100.0,
                4.0,
                100.0,
                0.0,
                0.0,
            );
            assert!(trg.on_signals(&[s], &ctx, &sym()).is_none(), "链中不应触发");
        }
        // 3) 反转砖：跌砖、小实体（body 30 < 0.4×(30+60)）+ 长上影、delta 翻负且 |−12%|≥8
        //    chain_index=1（新链第 1 块），is_reversal=true
        let rev = brick_sig(
            base_idx + 4,
            -1,
            1,
            true,
            4.0,
            15_000,
            67238.0,
            -4.0 * 0.12,
            30.0,
            60.0,
            0.0,
        );
        // 手动把 chain_n 顶到 3（反转砖的链评估用的是"被反转的链"）
        trg.chain_n = 3;
        trg.chain_vol = 15.0 * 67100.0;
        trg.chain_dur = 60_000;
        let intent = trg.on_signals(std::slice::from_ref(&rev), &ctx, &sym());
        assert!(intent.is_some(), "三条件齐备 + min_chain 应触发");
        let it = intent.unwrap();
        assert_eq!(it.side, Side::Sell, "涨链力竭反转 → 做空");
        assert!(it.limit_price.is_some(), "maker 限价入场（7.5-①）");
        assert!((it.limit_price.unwrap().to_f64() - 67238.0).abs() < 1e-6);
        // 止损 = 砖 high + buffer
        assert!(it.stop_price.to_f64() > 67238.0);
        assert!(it.reason.contains("exhaustion_rev"));
    }

    /// 三条件缺一不可：分别破坏每一条件，断言不触发。
    #[test]
    fn each_condition_necessary() {
        let mk = |vol_mult, delta_flip, prog| ExhaustionReversal {
            vol_mult_exh: vol_mult,
            delta_flip_pct: delta_flip,
            dur_max_ms: 30_000,
            prog_ratio: prog,
            min_chain: 3,
            lookback: 200,
            sl_buffer_usd: 150.0,
            session_scaler: [("europe".into(), 1.0)].into_iter().collect(),
            baseline: RateBaseline::new(200),
            chain_vol: 0.0,
            chain_dur: 0,
            chain_n: 3,
            prev_delta_pct: Some(10.0), // 前砖与链同向
            prev_dir: 1,
        };
        let ctx = ctx_with_session("europe");
        // 标准反转砖（三条件都满足的形态）
        let good = || brick_sig(99, -1, 1, true, 4.0, 15_000, 67238.0, -0.5, 30.0, 60.0, 0.0);

        // A) vol_mult 提到 1000 → 条件1（放量）不满足
        let mut t = mk(1000.0, 8.0, 0.4);
        for _ in 0..50 {
            t.baseline.push(100.0);
        }
        t.chain_vol = 15.0 * 67100.0;
        t.chain_dur = 60_000;
        assert!(
            t.on_signals(&[good()], &ctx, &sym()).is_none(),
            "条件1 破坏"
        );

        // B) prog_ratio 收到 0.01 → 条件2（力竭不推进）不满足（body/range=30/90=0.33）
        let mut t = mk(3.0, 8.0, 0.01);
        for _ in 0..50 {
            t.baseline.push(100.0);
        }
        t.chain_vol = 15.0 * 67100.0;
        t.chain_dur = 60_000;
        assert!(
            t.on_signals(&[good()], &ctx, &sym()).is_none(),
            "条件2 破坏"
        );

        // C) delta_flip 提到 50 → 条件3（Delta 反转）不满足（|−12.5%|<50）
        let mut t = mk(3.0, 50.0, 0.4);
        for _ in 0..50 {
            t.baseline.push(100.0);
        }
        t.chain_vol = 15.0 * 67100.0;
        t.chain_dur = 60_000;
        assert!(
            t.on_signals(&[good()], &ctx, &sym()).is_none(),
            "条件3 破坏"
        );

        // D) 链长不足（min_chain=5 但只有 3）
        let mut t = mk(3.0, 8.0, 0.4);
        t.min_chain = 5;
        for _ in 0..50 {
            t.baseline.push(100.0);
        }
        t.chain_vol = 15.0 * 67100.0;
        t.chain_dur = 60_000;
        assert!(
            t.on_signals(&[good()], &ctx, &sym()).is_none(),
            "min_chain 破坏"
        );
    }

    /// 横盘慢速不触发（手册验收条款）：低量慢砖的反转不扣扳机。
    #[test]
    fn sideways_slow_no_fire() {
        let mut trg = ExhaustionReversal {
            vol_mult_exh: 3.0,
            delta_flip_pct: 8.0,
            dur_max_ms: 30_000,
            prog_ratio: 0.4,
            min_chain: 3,
            lookback: 200,
            sl_buffer_usd: 150.0,
            session_scaler: [("europe".into(), 1.0)].into_iter().collect(),
            baseline: RateBaseline::new(200),
            chain_vol: 0.0,
            chain_dur: 0,
            chain_n: 0,
            prev_delta_pct: None,
            prev_dir: 0,
        };
        let ctx = ctx_with_session("europe");
        // 慢速交替砖（速率低、量小），反转砖也不该触发（条件1 不过）
        for i in 0..60 {
            let dir = if i % 2 == 0 { 1 } else { -1 };
            let s = brick_sig(
                i, dir, 1, true, 0.05, 120_000, 67000.0, -0.002, 30.0, 40.0, 30.0,
            );
            assert!(
                trg.on_signals(&[s], &ctx, &sym()).is_none(),
                "横盘慢速不触发"
            );
        }
    }

    /// 时段系数影响（测试条款）：weekend 系数 0.6 → 同量更容易过条件1。
    #[test]
    fn session_scaler_effect() {
        let mut scaler = std::collections::HashMap::new();
        scaler.insert("europe".to_string(), 1.0);
        scaler.insert("weekend".to_string(), 0.6);
        let mk = |sc: std::collections::HashMap<String, f64>| ExhaustionReversal {
            vol_mult_exh: 3.0,
            delta_flip_pct: 8.0,
            dur_max_ms: 30_000,
            prog_ratio: 0.4,
            min_chain: 3,
            lookback: 200,
            sl_buffer_usd: 150.0,
            session_scaler: sc,
            baseline: RateBaseline::new(200),
            chain_vol: 2.0 * 67100.0, // 链速率 2237 ≈ 基线×2.2（3.0×1.0=3000 不过、3.0×0.6=1800 过）
            chain_dur: 60_000,
            chain_n: 3,
            prev_delta_pct: Some(9.0),
            prev_dir: 1,
        };
        let rev = brick_sig(9, -1, 1, true, 4.0, 15_000, 67238.0, -0.4, 30.0, 50.0, 0.0);
        // europe（系数1.0）：阈值 3.0×base，链速率不足 → 不触发
        let mut t = mk(scaler.clone());
        for _ in 0..50 {
            t.baseline.push(1000.0);
        }
        let ctx_e = ctx_with_session("europe");
        assert!(t
            .on_signals(std::slice::from_ref(&rev), &ctx_e, &sym())
            .is_none());
        // weekend（系数0.6）：阈值 1.8×base，同量 → 触发
        let mut t2 = mk(scaler);
        for _ in 0..50 {
            t2.baseline.push(1000.0);
        }
        let ctx_w = ctx_with_session("weekend");
        let it = t2.on_signals(std::slice::from_ref(&rev), &ctx_w, &sym());
        assert!(it.is_some(), "weekend 降阈后应触发");
        assert!(it.unwrap().reason.contains("weekend"));
    }
}

// ============================================================================
// 端到端集成测试（验收的黄金样本：合成序列直驱）
// ============================================================================

#[cfg(test)]
mod e2e_tests {
    use super::*;

    /// 教科书黄金样本（验收）：
    /// 平静基线 → 4 连放量上涨链 → 针尖力竭反转（小实体/长上影/Delta 翻负），
    /// 断言在反转砖上扣扳机；横盘慢速对照不触发。
    #[test]
    fn golden_sample_fires_and_sideways_not() {
        let mk = || ExhaustionReversal {
            vol_mult_exh: 3.0,
            delta_flip_pct: 8.0,
            dur_max_ms: 30_000,
            prog_ratio: 0.4,
            min_chain: 3,
            lookback: 200,
            sl_buffer_usd: 150.0,
            session_scaler: [("europe".into(), 1.0)].into_iter().collect(),
            baseline: RateBaseline::new(200),
            chain_vol: 0.0,
            chain_dur: 0,
            chain_n: 0,
            prev_delta_pct: None,
            prev_dir: 0,
        };
        let ctx = {
            let mut c = Ctx::default();
            c.flags.insert("session".into(), "europe".into());
            c
        };
        let sym = Symbol::new("BTCUSDT");

        // 平静基线：60 块慢速砖（速率 ~146 USD/s）
        let mut t = mk();
        for i in 0..60 {
            let s = crate::triggers::tests::brick_sig(
                i,
                if i % 2 == 0 { 1 } else { -1 },
                1,
                i > 0,
                0.02,
                9_000,
                67000.0,
                0.0,
                62.0,
                0.0,
                0.0,
            );
            assert!(t.on_signals(&[s], &ctx, &sym).is_none(), "基线不触发");
        }
        // 4 连放量链（每砖 17s/35币 → 速率 ~139,000 ≫ 3×146）
        let base_idx = 60;
        for k in 1..=4u32 {
            let s = crate::triggers::tests::brick_sig(
                base_idx + k as u64,
                1,
                k,
                false,
                35.0,
                17_000,
                67000.0 + k as f64 * 100.0,
                35.0,
                100.0,
                0.0,
                0.0,
            );
            assert!(t.on_signals(&[s], &ctx, &sym).is_none(), "链中不触发");
        }
        // 力竭反转砖：跌、16s、body 62/全 range 62+40 → body/range=0.61 超 prog...
        // 满足条件2 需要 body/range ≤ 0.4 → 用 body 30、wick_up 70
        let rev = crate::triggers::tests::brick_sig(
            base_idx + 5,
            -1,
            1,
            true,
            30.0,
            16_000,
            67400.0,
            -27.0,
            30.0,
            70.0,
            0.0,
        );
        let intent = t.on_signals(std::slice::from_ref(&rev), &ctx, &sym);
        assert!(intent.is_some(), "黄金样本应触发");
        let it = intent.unwrap();
        assert_eq!(it.side, Side::Sell);
        assert!(it.limit_price.is_some());
        assert!(it.reason.contains("exhaustion_rev"));

        // 横盘慢速对照：同样形态但量速率低（0.5 币/砖/分钟级）
        let mut t2 = mk();
        for i in 0..60 {
            let s = crate::triggers::tests::brick_sig(
                i,
                if i % 2 == 0 { 1 } else { -1 },
                1,
                i > 0,
                0.02,
                9_000,
                67000.0,
                0.0,
                62.0,
                0.0,
                0.0,
            );
            t2.on_signals(&[s], &ctx, &sym);
        }
        for k in 1..=4u32 {
            let s = crate::triggers::tests::brick_sig(
                100 + k as u64,
                1,
                k,
                false,
                0.05,
                17_000,
                67000.0 + k as f64 * 100.0,
                0.05,
                100.0,
                0.0,
                0.0,
            );
            t2.on_signals(&[s], &ctx, &sym);
        }
        let rev_slow = crate::triggers::tests::brick_sig(
            105, -1, 1, true, 0.04, 16_000, 67400.0, -0.004, 30.0, 70.0, 0.0,
        );
        assert!(
            t2.on_signals(std::slice::from_ref(&rev_slow), &ctx, &sym)
                .is_none(),
            "横盘慢速不触发（手册验收）"
        );
    }
}
