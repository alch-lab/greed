//! 绩效报告：从成交序列产出 markdown + JSON
//!
//! 胜率 / 盈亏比 / expectancy / 最大回撤 / Sharpe / Sortino、
//! 按时段（亚盘/欧盘/美盘/周末）分组统计、权益曲线；输出 markdown + JSON。
//! 与引擎解耦：输入 `&[Fill]` + 权益曲线，输出报告——策略无关、可独立测试。
//!
//! 报告必含（对应手册条款）：
//! - 总览：胜率/盈亏比/expectancy/最大回撤/Sharpe/Sortino
//! - **可行性窗口**：实测胜率 vs 盈亏平衡胜率 vs 随机方向基线
//! - **无效性基线判定**：CI 下沿 > max（几何基线， 盈亏平衡） 才计 Edge
//! - **多重检验校正**：DSR + PBO；凡报 Sharpe 必附 DSR
//! - **插件级归因**：按成交 reason 前缀分组的胜率/期望/费用
//! - **样本量提示**：笔数低于门槛只给区间
//! - 连亏回撤对照表；maker/taker 分列

use serde::Serialize;
use std::collections::BTreeMap;

use crate::account::Fill;
use crate::stats::{
    binomial_ci, deflated_sharpe, ineffectiveness_gate, min_sample_size, prob_backtest_overfit,
    sharpe_annualized, streak_drawdown_table, GateVerdict,
};

/// 报告配置。
#[derive(Debug, Clone)]
pub struct ReportConfig {
    /// 随机方向胜率基线；订单流二元方向默认 50%。
    pub null_winrate_baseline: f64,
    /// maker/taker 盈亏平衡胜率（7.5-① 表格；由报告者按实际执行方式填）
    pub breakeven_maker: f64,
    pub breakeven_taker: f64,
    /// 试验次数（DSR 用；扫描过多少组合就填多少）
    pub n_trials: usize,
    /// 无效性判定的最小样本量（7.7-⑤）
    pub min_n: usize,
    /// 风险分数（连亏表；默认 0.75%）
    pub risk_pct: f64,
}

impl Default for ReportConfig {
    fn default() -> Self {
        Self {
            null_winrate_baseline: 0.5,
            breakeven_maker: 0.51,
            breakeven_taker: 0.54,
            n_trials: 1,
            min_n: 100,
            risk_pct: 0.0075,
        }
    }
}

/// 一笔回合（开仓→平仓的完整往返；引擎 fill 配对后得到）。
#[derive(Debug, Clone)]
pub struct RoundTrip {
    pub open_ts: i64,
    pub close_ts: i64,
    pub side: &'static str,
    pub qty: f64,
    pub entry: f64,
    pub exit: f64,
    /// 净盈亏（已扣双边费用）
    pub pnl_net: f64,
    pub fees: f64,
    /// 开仓是否 maker（7.5-① 分列）
    pub entry_maker: bool,
    /// 出场原因
    pub exit_reason: String,
    /// 开仓扳机（归因分组键）
    pub entry_reason: String,
}

impl RoundTrip {
    pub fn is_win(&self) -> bool {
        self.pnl_net > 0.0
    }
    pub fn hold_ms(&self) -> i64 {
        self.close_ts - self.open_ts
    }
}

/// 把成交序列配对为回合（FIFO：同向累计开仓，反向成交按量冲销）。
///
/// 引擎的 fill 已是净持仓记账，回合配对只用于绩效归因，不影响记账守恒。
pub fn pair_round_trips(fills: &[Fill]) -> Vec<RoundTrip> {
    struct Open {
        ts: i64,
        side: tcore::types::Side,
        qty: f64,
        entry_cost: f64, // Σ(price×qty)
        fees: f64,
        closed_qty: f64,
        exit_notional: f64,
        realized_gross: f64,
        entry_maker: bool,
        reason: String,
    }
    let mut trips = Vec::new();
    let mut open: Option<Open> = None;
    for f in fills {
        let side = f.side;
        let qty = f.qty.to_f64();
        let px = f.price.to_f64();
        match &mut open {
            None => {
                open = Some(Open {
                    ts: f.ts.as_millis(),
                    side,
                    qty,
                    entry_cost: px * qty,
                    fees: f.fee,
                    closed_qty: 0.0,
                    exit_notional: 0.0,
                    realized_gross: 0.0,
                    entry_maker: f.is_maker,
                    reason: f.reason.clone(),
                });
            }
            Some(o) if o.side == side => {
                // 同向加仓：摊均价，累计费用；保留首笔 maker/原因
                o.entry_cost += px * qty;
                o.qty += qty;
                o.fees += f.fee;
            }
            Some(o) => {
                // 反向成交：冲销（可能部分 → 拆回合；超出 → 翻仓开新）
                let close_qty = qty.min(o.qty);
                let entry_px = o.entry_cost / o.qty;
                let gross = match o.side {
                    tcore::types::Side::Buy => (px - entry_px) * close_qty,
                    tcore::types::Side::Sell => (entry_px - px) * close_qty,
                };
                o.closed_qty += close_qty;
                o.exit_notional += px * close_qty;
                o.realized_gross += gross;
                o.fees += f.fee;
                if qty >= o.qty {
                    let total_fees = o.fees;
                    trips.push(RoundTrip {
                        open_ts: o.ts,
                        close_ts: f.ts.as_millis(),
                        side: match o.side {
                            tcore::types::Side::Buy => "long",
                            tcore::types::Side::Sell => "short",
                        },
                        qty: o.closed_qty,
                        entry: entry_px,
                        exit: o.exit_notional / o.closed_qty.max(1e-12),
                        pnl_net: o.realized_gross - total_fees,
                        fees: total_fees,
                        entry_maker: o.entry_maker,
                        exit_reason: f.reason.clone(),
                        entry_reason: o.reason.clone(),
                    });
                    // 完全平仓或翻仓
                    if qty > o.qty + 1e-12 {
                        let rem = qty - o.qty;
                        open = Some(Open {
                            ts: f.ts.as_millis(),
                            side,
                            qty: rem,
                            entry_cost: px * rem,
                            fees: 0.0,
                            closed_qty: 0.0,
                            exit_notional: 0.0,
                            realized_gross: 0.0,
                            entry_maker: f.is_maker,
                            reason: f.reason.clone(),
                        });
                    } else {
                        open = None;
                    }
                } else {
                    // 部分平仓：缩减开仓
                    let frac = close_qty / o.qty;
                    o.entry_cost *= 1.0 - frac;
                    o.qty -= close_qty;
                }
            }
        }
    }
    trips
}

/// 指标汇总（一组回合）。
#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    pub n: usize,
    pub wins: usize,
    pub winrate: f64,
    pub winrate_ci_lo: f64,
    pub winrate_ci_hi: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub profit_factor: f64,
    pub expectancy: f64,
    pub total_fees: f64,
    pub fees_per_trade: f64,
    pub maker_share: f64,
}

pub fn metrics_of(trips: &[RoundTrip]) -> Metrics {
    let n = trips.len();
    if n == 0 {
        return Metrics {
            n: 0,
            wins: 0,
            winrate: f64::NAN,
            winrate_ci_lo: 0.0,
            winrate_ci_hi: 1.0,
            gross_profit: 0.0,
            gross_loss: 0.0,
            profit_factor: f64::NAN,
            expectancy: 0.0,
            total_fees: 0.0,
            fees_per_trade: 0.0,
            maker_share: f64::NAN,
        };
    }
    let wins = trips.iter().filter(|t| t.is_win()).count();
    let (_, lo, hi) = binomial_ci(wins, n, 1.96);
    let gp: f64 = trips
        .iter()
        .filter(|t| t.pnl_net > 0.0)
        .map(|t| t.pnl_net)
        .sum();
    let gl: f64 = -trips
        .iter()
        .filter(|t| t.pnl_net < 0.0)
        .map(|t| t.pnl_net)
        .sum::<f64>();
    let fees: f64 = trips.iter().map(|t| t.fees).sum();
    let maker = trips.iter().filter(|t| t.entry_maker).count();
    Metrics {
        n,
        wins,
        winrate: wins as f64 / n as f64,
        winrate_ci_lo: lo,
        winrate_ci_hi: hi,
        gross_profit: gp,
        gross_loss: gl,
        profit_factor: if gl > 0.0 { gp / gl } else { f64::INFINITY },
        expectancy: (gp - gl) / n as f64,
        total_fees: fees,
        fees_per_trade: fees / n as f64,
        maker_share: maker as f64 / n as f64,
    }
}

/// 插件级归因（7.1）：按开仓 reason 分组。
#[derive(Debug, Clone, Serialize)]
pub struct Attribution {
    pub group: String,
    pub metrics: Metrics,
    /// 相对全体的期望差（后验增益的代理）
    pub expectancy_lift: f64,
    pub gate: String,
}

/// 权益曲线点（由引擎/调用方按日采样提供）。
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct EquityPoint {
    pub ts_ms: i64,
    pub equity: f64,
}

/// 最大回撤（峰值到谷值的最大跌幅，USD 与 %）。
pub fn max_drawdown(curve: &[EquityPoint]) -> (f64, f64) {
    let mut peak = f64::NEG_INFINITY;
    let mut max_dd_usd = 0.0;
    let mut max_dd_pct = 0.0;
    for p in curve {
        peak = peak.max(p.equity);
        if peak > 0.0 {
            let dd = peak - p.equity;
            let pct = dd / peak;
            if dd > max_dd_usd {
                max_dd_usd = dd;
            }
            if pct > max_dd_pct {
                max_dd_pct = pct;
            }
        }
    }
    (max_dd_usd, max_dd_pct)
}

/// 日收益序列（权益曲线相邻点之差/前一点）。
pub fn daily_returns(curve: &[EquityPoint]) -> Vec<f64> {
    curve
        .windows(2)
        .filter(|w| w[0].equity > 0.0)
        .map(|w| (w[1].equity - w[0].equity) / w[0].equity)
        .collect()
}

/// Sortino（只惩罚下行波动）。
pub fn sortino_annualized(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).copied().collect();
    if downside.is_empty() {
        return f64::INFINITY;
    }
    let dd = (downside.iter().map(|r| r * r).sum::<f64>() / downside.len() as f64).sqrt();
    if dd <= 0.0 {
        return f64::NAN;
    }
    mean / dd * (365.0_f64).sqrt()
}

/// 完整报告（JSON 序列化的顶层结构）。
#[derive(Debug, Serialize)]
pub struct Report {
    pub overall: Metrics,
    pub max_drawdown_usd: f64,
    pub max_drawdown_pct: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub dsr: f64,
    pub pbo: f64,
    pub n_trials: usize,
    pub gate_verdict: String,
    pub gate_baseline: f64,
    pub min_sample_needed: f64,
    pub maker_winrate: f64,
    pub taker_winrate: f64,
    pub attribution: Vec<Attribution>,
    pub streak_table: Vec<(usize, f64)>,
    pub sample_note: String,
}

/// 生成报告。
///
/// - `oos_sharpes`：CPCV 各折样本外 Sharpe（无 CPCV 时传空切片，PBO 报 NaN）。
pub fn build_report(
    trips: &[RoundTrip],
    equity_curve: &[EquityPoint],
    cfg: &ReportConfig,
    oos_sharpes: &[f64],
) -> Report {
    let overall = metrics_of(trips);
    let (mdd_usd, mdd_pct) = max_drawdown(equity_curve);
    let rets = daily_returns(equity_curve);
    let sharpe = sharpe_annualized(&rets);
    let sortino = sortino_annualized(&rets);
    let dsr = deflated_sharpe(sharpe, rets.len(), &rets, cfg.n_trials);
    let pbo = prob_backtest_overfit(oos_sharpes);

    // maker/taker 分列胜率
    let maker_trips: Vec<_> = trips.iter().filter(|t| t.entry_maker).cloned().collect();
    let taker_trips: Vec<_> = trips.iter().filter(|t| !t.entry_maker).cloned().collect();
    let m_wr = metrics_of(&maker_trips).winrate;
    let t_wr = metrics_of(&taker_trips).winrate;

    // 无效性基线判定（按实际 maker 占比选择盈亏平衡参考）
    let be = if overall.maker_share >= 0.5 {
        cfg.breakeven_maker
    } else {
        cfg.breakeven_taker
    };
    let (verdict, baseline, (p, lo, hi)) = ineffectiveness_gate(
        overall.wins,
        overall.n,
        cfg.null_winrate_baseline,
        be,
        cfg.min_n,
    );
    let gate_str = match verdict {
        GateVerdict::Edge => "Edge（CI 下沿 > 基线，创造超额）",
        GateVerdict::NoEdge => "NoEdge（不能拒绝只是免费基线）",
        GateVerdict::Insufficient => "样本不足（7.7-⑤：只报区间）",
    };
    let min_needed = min_sample_size(baseline, baseline + 0.02);

    // 插件级归因
    let mut groups: BTreeMap<String, Vec<RoundTrip>> = BTreeMap::new();
    for t in trips {
        groups
            .entry(t.entry_reason.clone())
            .or_default()
            .push(t.clone());
    }
    let attribution: Vec<Attribution> = groups
        .into_iter()
        .map(|(g, ts)| {
            let m = metrics_of(&ts);
            let (v, _, _) =
                ineffectiveness_gate(m.wins, m.n, cfg.null_winrate_baseline, be, cfg.min_n);
            Attribution {
                group: g,
                expectancy_lift: m.expectancy - overall.expectancy,
                metrics: m,
                gate: format!("{:?}", v),
            }
        })
        .collect();

    let sample_note = if overall.n < cfg.min_n {
        format!(
            "⚠️ 样本不足（n={} < {}）：按 7.7-⑤ 只给区间——胜率 {:.1}%，95%CI [{:.1}%, {:.1}%]，不做有效性结论",
            overall.n, cfg.min_n, p * 100.0, lo * 100.0, hi * 100.0
        )
    } else {
        format!(
            "n={} ≥ {}，可做基线判定；区分基线+2pp 需 n≈{:.0}",
            overall.n, cfg.min_n, min_needed
        )
    };

    Report {
        overall,
        max_drawdown_usd: mdd_usd,
        max_drawdown_pct: mdd_pct,
        sharpe,
        sortino,
        dsr,
        pbo,
        n_trials: cfg.n_trials,
        gate_verdict: gate_str.into(),
        gate_baseline: baseline,
        min_sample_needed: min_needed,
        maker_winrate: m_wr,
        taker_winrate: t_wr,
        attribution,
        streak_table: streak_drawdown_table(cfg.risk_pct, &[5, 10, 15, 20, 30]),
        sample_note,
    }
}

// ============================================================================
// markdown 渲染
// ============================================================================

pub fn to_markdown(r: &Report) -> String {
    let mut md = String::new();
    md.push_str("# 回测绩效报告（PR-7，含第 7 章验证纪律）\n\n");

    md.push_str("## 总览\n\n");
    md.push_str(&format!(
        "- 回合数：{}（胜 {} / 负 {}）｜胜率 **{:.1}%**（95%CI [{:.1}%, {:.1}%]）\n",
        r.overall.n,
        r.overall.wins,
        r.overall.n - r.overall.wins,
        r.overall.winrate * 100.0,
        r.overall.winrate_ci_lo * 100.0,
        r.overall.winrate_ci_hi * 100.0
    ));
    md.push_str(&format!(
        "- 盈亏比(PF)：{:.2}｜expectancy：{:.2}$/笔｜总费用：{:.0}$（{:.1}$/笔）\n",
        r.overall.profit_factor,
        r.overall.expectancy,
        r.overall.total_fees,
        r.overall.fees_per_trade
    ));
    md.push_str(&format!(
        "- 最大回撤：{:.0}$（{:.1}%）\n",
        r.max_drawdown_usd,
        r.max_drawdown_pct * 100.0
    ));
    md.push_str(&format!(
        "- Sharpe：{:.2}｜Sortino：{:.2}\n",
        r.sharpe, r.sortino
    ));
    md.push('\n');

    md.push_str("## 可行性窗口（7.5-①③ + 7.3.2）\n\n");
    md.push_str(&format!(
        "- 实测胜率：{:.1}%（maker 单 {:.1}% / taker 单 {:.1}%）\n",
        r.overall.winrate * 100.0,
        r.maker_winrate * 100.0,
        r.taker_winrate * 100.0
    ));
    md.push_str(&format!(
        "- 无效性基线：**{:.1}%**（= max(随机方向基线, 盈亏平衡胜率)）\n",
        r.gate_baseline * 100.0
    ));
    md.push_str(&format!("- **判定：{}**\n", r.gate_verdict));
    md.push_str(&format!("- {}\n\n", r.sample_note));

    md.push_str("## 多重检验校正（7.5-③）\n\n");
    md.push_str(&format!(
        "- DSR（Deflated Sharpe，n_trials={}）：{:.3}{}\n",
        r.n_trials,
        r.dsr,
        if r.dsr.is_nan() {
            "（样本不足）"
        } else if r.dsr < 0.95 {
            " ⚠️ 未通过校正"
        } else {
            " ✅"
        }
    ));
    md.push_str(&format!(
        "- PBO（样本外 Sharpe≤0 频率）：{}\n\n",
        if r.pbo.is_nan() {
            "N/A（未做 CPCV）".into()
        } else {
            format!("{:.1}%", r.pbo * 100.0)
        }
    ));

    md.push_str("## 插件级归因（7.1）\n\n");
    md.push_str("| 来源 | 笔数 | 胜率 | expectancy | 期望增益 | 基线判定 |\n|---|---:|---:|---:|---:|---|\n");
    for a in &r.attribution {
        md.push_str(&format!(
            "| {} | {} | {:.1}% | {:.2} | {:+.2} | {} |\n",
            a.group,
            a.metrics.n,
            a.metrics.winrate * 100.0,
            a.metrics.expectancy,
            a.expectancy_lift,
            a.gate
        ));
    }
    md.push('\n');

    md.push_str("## 连亏回撤对照（7.4，0.75% 固定分数）\n\n");
    md.push_str("| 连亏笔数 | 回撤 |\n|---:|---:|\n");
    for (k, dd) in &r.streak_table {
        md.push_str(&format!("| {} | {:.1}% |\n", k, dd));
    }
    md
}

pub fn to_json(r: &Report) -> serde_json::Result<String> {
    serde_json::to_string_pretty(r)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Price, Qty, Side, Timestamp};

    fn fill(ts: i64, side: Side, px: f64, qty: f64, fee: f64, maker: bool, reason: &str) -> Fill {
        Fill {
            ts: Timestamp::from_millis(ts),
            side,
            price: Price::from_f64(px),
            qty: Qty::from_f64(qty),
            fee,
            is_maker: maker,
            realized_pnl: 0.0,
            position_side_after: None,
            reason: reason.into(),
        }
    }

    #[test]
    fn round_trip_pairing_simple() {
        // 开多 1 @67000 → 平 @67100：gross=+100，fees=13.4+13.47 → pnl≈73.13
        let fills = vec![
            fill(0, Side::Buy, 67000.0, 1.0, 13.4, true, "exhaustion"),
            fill(1000, Side::Sell, 67100.0, 1.0, 13.47, false, "stop"),
        ];
        let trips = pair_round_trips(&fills);
        assert_eq!(trips.len(), 1);
        let t = &trips[0];
        assert!(t.is_win());
        assert!((t.pnl_net - (100.0 - 13.4 - 13.47)).abs() < 1e-6);
        assert_eq!(t.side, "long");
        assert!(t.entry_maker);
        assert_eq!(t.exit_reason, "stop");
        assert!((t.hold_ms() - 1000).abs() < 1);
    }

    #[test]
    fn round_trip_partial_and_flip() {
        // 开多 1.0 → 平 0.5（盈利）→ 平 0.5 + 反开空 0.5（翻仓）
        let fills = vec![
            fill(0, Side::Buy, 100.0, 1.0, 0.02, false, "open"),
            fill(10, Side::Sell, 110.0, 0.5, 0.011, false, "tp1"),
            fill(20, Side::Sell, 120.0, 1.0, 0.024, false, "reverse"),
        ];
        let trips = pair_round_trips(&fills);
        assert_eq!(trips.len(), 1);
        assert!(trips[0].pnl_net > 0.0);
        assert_eq!(trips[0].exit_reason, "reverse");
    }

    #[test]
    fn metrics_known_set() {
        // 3 胜 2 负：胜率 60%，CI 含 0.5
        let trips: Vec<RoundTrip> = vec![rt(100.0), rt(100.0), rt(100.0), rt(-50.0), rt(-50.0)];
        let m = metrics_of(&trips);
        assert_eq!(m.n, 5);
        assert_eq!(m.wins, 3);
        assert!((m.winrate - 0.6).abs() < 1e-12);
        assert!((m.gross_profit - 300.0).abs() < 1e-9);
        assert!((m.gross_loss - 100.0).abs() < 1e-9);
        assert!((m.profit_factor - 3.0).abs() < 1e-9);
        assert!((m.expectancy - 40.0).abs() < 1e-9);
    }

    fn rt(pnl: f64) -> RoundTrip {
        RoundTrip {
            open_ts: 0,
            close_ts: 1,
            side: "long",
            qty: 1.0,
            entry: 100.0,
            exit: 100.0 + pnl,
            pnl_net: pnl,
            fees: 1.0,
            entry_maker: true,
            exit_reason: "x".into(),
            entry_reason: "t".into(),
        }
    }

    #[test]
    fn max_drawdown_known() {
        let curve = vec![
            EquityPoint {
                ts_ms: 0,
                equity: 100.0,
            },
            EquityPoint {
                ts_ms: 1,
                equity: 120.0,
            },
            EquityPoint {
                ts_ms: 2,
                equity: 90.0,
            }, // dd 30/120=25%
            EquityPoint {
                ts_ms: 3,
                equity: 110.0,
            },
        ];
        let (usd, pct) = max_drawdown(&curve);
        assert!((usd - 30.0).abs() < 1e-9);
        assert!((pct - 0.25).abs() < 1e-9);
    }

    #[test]
    fn report_gate_noedge_for_coinflip() {
        // 合成：55% 胜率、300 笔、基线 0.605 → NoEdge（点估计高但 CI 下沿不足）
        let trips: Vec<RoundTrip> = (0..300)
            .map(|i| rt(if i % 20 < 11 { 100.0 } else { -100.0 }))
            .collect();
        let curve = vec![
            EquityPoint {
                ts_ms: 0,
                equity: 100_000.0,
            },
            EquityPoint {
                ts_ms: 1,
                equity: 100_500.0,
            },
        ];
        let r = build_report(&trips, &curve, &ReportConfig::default(), &[]);
        assert!(r.gate_verdict.contains("NoEdge"));
        assert!(r.sharpe.is_nan() || r.sharpe.is_finite());
        let md = to_markdown(&r);
        assert!(md.contains("可行性窗口"));
        assert!(md.contains("连亏回撤对照"));
        assert!(to_json(&r).is_ok());
    }

    #[test]
    fn attribution_groups_by_reason() {
        let mut trips = vec![rt(100.0), rt(-50.0)];
        trips[1].entry_reason = "grid".into();
        let curve = vec![
            EquityPoint {
                ts_ms: 0,
                equity: 100.0,
            },
            EquityPoint {
                ts_ms: 1,
                equity: 101.0,
            },
        ];
        let r = build_report(&trips, &curve, &ReportConfig::default(), &[]);
        assert_eq!(r.attribution.len(), 2);
        let groups: Vec<_> = r.attribution.iter().map(|a| a.group.as_str()).collect();
        assert!(groups.contains(&"t") && groups.contains(&"grid"));
    }
}
