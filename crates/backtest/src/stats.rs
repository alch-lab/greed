//! 统计工具层：手册第 7 章验证纪律的可计算实现。
//!
//! 内容：
//! - [`binomial_ci`]：胜率 Wilson 置信区间（样本量表述）；
//! - [`min_sample_size`]：区分两胜率所需最小样本量；
//! - [`ineffectiveness_gate`]：无效性基线判定式（验收的核心判定）；
//! - [`sharpe_annualized`] / [`deflated_sharpe`]：Sharpe 与 Deflated Sharpe Ratio（多重检验校正）；
//! - [`prob_backtest_overfit`]：CPCV/PBO 回测过拟合概率；
//! - [`streak_drawdown_table`]：连亏回撤对照表（7.4，0.75% 固定分数）。
//!
//! 全部为纯函数，单元测试用已知答案的合成序列

// ============================================================================
// 二项推断
// ============================================================================

/// 胜率的 Wilson score 置信区间（小样本稳健，优于正态近似）。
///
/// 返回 (点估计, 下沿, 上沿)。n=0 时返回 (NaN, 0, 1)。
pub fn binomial_ci(wins: usize, n: usize, z: f64) -> (f64, f64, f64) {
    if n == 0 {
        return (f64::NAN, 0.0, 1.0);
    }
    let nf = n as f64;
    let p = wins as f64 / nf;
    let z2 = z * z;
    let denom = 1.0 + z2 / nf;
    let center = (p + z2 / (2.0 * nf)) / denom;
    let half = (z / denom) * ((p * (1.0 - p) / nf) + z2 / (4.0 * nf * nf)).sqrt();
    (p, (center - half).max(0.0), (center + half).min(1.0))
}

/// 区分胜率 p1 与基线 p0 所需的最小样本量（**单侧** α=0.05，power=80%，基线方差近似）。
///
/// 基线检验本质是单侧的（"胜率 > 基线"），公式：n ≈ (z_{1−α}+z_{1−β})²·p0(1−p0) / (p1−p0)²。
/// 与手册 7.5-③ 的口径一致（53% vs 50% ≈ 1700；52% vs 50% ≈ 3900）。
pub fn min_sample_size(p0: f64, p1: f64) -> f64 {
    let z_a = 1.644_853_626_951_47; // 单侧 5%
    let z_b = 0.841_621_233_572_914; // 80% power
    let z = z_a + z_b;
    z * z * p0 * (1.0 - p0) / ((p1 - p0) * (p1 - p0))
}

/// 无效性基线判定
///
/// 信号子集有效 ⟺ 净胜率 95% CI **下沿** > max(几何自然反转率, 盈亏平衡胜率)。
/// 返回 (是否有效, 基线, CI 下沿)。样本不足（n < `min_n`）时一律判"样本不足"
/// 由上层决定表述
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GateVerdict {
    /// CI 下沿 > 基线：信号创造超额
    Edge,
    /// CI 下沿 ≤ 基线：不能拒绝"只是免费基线"
    NoEdge,
    /// n < min_n：只能报区间
    Insufficient,
}

pub fn ineffectiveness_gate(
    wins: usize,
    n: usize,
    geometric_baseline: f64,
    breakeven_winrate: f64,
    min_n: usize,
) -> (GateVerdict, f64, (f64, f64, f64)) {
    let baseline = geometric_baseline.max(breakeven_winrate);
    let (p, lo, hi) = binomial_ci(wins, n, 1.96);
    if n < min_n {
        return (GateVerdict::Insufficient, baseline, (p, lo, hi));
    }
    let verdict = if lo > baseline {
        GateVerdict::Edge
    } else {
        GateVerdict::NoEdge
    };
    (verdict, baseline, (p, lo, hi))
}

// ============================================================================
// Sharpe / DSR / PBO
// ============================================================================

/// 年化 Sharpe（日收益序列，无风险利率取 0）。样本 <2 或方差 0 返回 NaN。
pub fn sharpe_annualized(daily_returns: &[f64]) -> f64 {
    let n = daily_returns.len();
    if n < 2 {
        return f64::NAN;
    }
    let mean = daily_returns.iter().sum::<f64>() / n as f64;
    let var = daily_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / (n - 1) as f64;
    if var <= 0.0 {
        return f64::NAN;
    }
    mean / var.sqrt() * (365.0_f64).sqrt()
}

/// 序列的偏度与超额峰度（DSR 用）。
fn skew_kurt(x: &[f64]) -> (f64, f64) {
    let n = x.len() as f64;
    let mean = x.iter().sum::<f64>() / n;
    let m2 = x.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    if m2 <= 0.0 {
        return (0.0, 0.0);
    }
    let m3 = x.iter().map(|v| (v - mean).powi(3)).sum::<f64>() / n;
    let m4 = x.iter().map(|v| (v - mean).powi(4)).sum::<f64>() / n;
    (m3 / m2.powf(1.5), m4 / (m2 * m2) - 3.0)
}

/// 标准正态 CDF（Abramowitz–Stegun 7.1.26，|ε|<1.5e-7）。
pub fn norm_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.231_641_9 * x.abs());
    let poly = t
        * (0.319_381_530
            + t * (-0.356_563_782
                + t * (1.781_477_937 + t * (-1.821_255_978 + t * 1.330_274_429))));
    let pdf = (-x * x / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt();
    let cdf = 1.0 - pdf * poly;
    if x >= 0.0 {
        cdf
    } else {
        1.0 - cdf
    }
}

/// Deflated Sharpe Ratio（Bailey & López de Prado 2014）。
///
/// 校正"试出来的 Sharpe"：给定观测 Sharpe `sr`（年化）、样本期数 `t`（交易日数）、
/// 日收益的偏度/峰度、以及**试验次数** `n_trials`，给出 SR>0 的 deflate 后概率。
/// 返回 [0,1] 的概率；越高越好，<0.95 视为未通过多重检验校正。
///
/// 期望最大噪声 Sharpe（n_trials 次独立试验）：
/// E[max] ≈ √(V) · ((1−γ)·Φ⁻¹(1−1/N) + γ·Φ⁻¹(1−1/(N·e)))，γ=欧拉常数。
pub fn deflated_sharpe(sr: f64, t: usize, daily_returns: &[f64], n_trials: usize) -> f64 {
    if t < 2 || daily_returns.len() < 2 || sr.is_nan() {
        return f64::NAN;
    }
    let n = n_trials.max(1) as f64;
    let (skew, kurt) = skew_kurt(daily_returns);
    let tf = t as f64;
    // SR 估计量的方差（Bailey-LdP 式 4）：V[SR̂] = (1 − γ3·SR + (γ4−1)/4·SR²) / (T−1)
    let sr_daily = sr / (365.0_f64).sqrt();
    let var_sr_raw =
        (1.0 - skew * sr_daily + (kurt - 1.0) / 4.0 * sr_daily * sr_daily) / (tf - 1.0);
    // 方差估计可因高 Sharpe/厚尾而变负——按小正数夹紧（等价"标准误≈0 → 极强信号"）。
    let var_sr = var_sr_raw.max(1e-12);
    // 期望最大噪声 SR（年化）
    let gamma = 0.577_215_664_901_532_9;
    let inv = |p: f64| norm_ppf(p);
    let e_max_daily = var_sr.sqrt()
        * ((1.0 - gamma) * inv(1.0 - 1.0 / n) + gamma * inv(1.0 - 1.0 / (n * std::f64::consts::E)));
    let e_max = e_max_daily * (365.0_f64).sqrt();
    // P(SR 真实 > 0)：以观测 SR 与 E[max] 之差按标准误标准化
    let se = var_sr.sqrt() * (365.0_f64).sqrt();
    norm_cdf((sr - e_max) / se)
}

/// 标准正态 PPF（Acklam 有理逼近，|ε|<1.15e-9）。
pub fn norm_ppf(p: f64) -> f64 {
    let a = [
        -3.969_683_028_665_38e1,
        2.209_460_984_245_2e2,
        -2.759_285_104_469_69e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_72e1,
        2.506_628_277_459_239,
    ];
    let b = [
        -5.447_609_879_822_41e1,
        1.615_858_368_580_41e2,
        -1.556_989_798_598_87e2,
        6.680_131_188_771_97e1,
        -1.328_068_155_288_57e1,
    ];
    let c = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    let d = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let plow = 0.024_25;
    let phigh = 1.0 - plow;
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if p < plow {
        let q = (-2.0 * p.ln()).sqrt();
        (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    } else if p <= phigh {
        let q = p - 0.5;
        let r = q * q;
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    }
}

/// PBO（Probability of Backtest Overfitting，CPCV 简化实现）。
///
/// 输入：k 折样本外 Sharpe 序列（每折一个数值；由上层按 CPCV 分组产生）。
/// 返回样本外 Sharpe ≤ 0 的经验频率——即"回测过拟合概率"的实证估计。
pub fn prob_backtest_overfit(oos_sharpes: &[f64]) -> f64 {
    if oos_sharpes.is_empty() {
        return f64::NAN;
    }
    let neg = oos_sharpes
        .iter()
        .filter(|&&s| s <= 0.0 || s.is_nan())
        .count();
    neg as f64 / oos_sharpes.len() as f64
}

// ============================================================================
// 连亏回撤对照表
// ============================================================================

/// 0.75% 固定分数下，连亏 k 笔的权益剩余比例 = (1−r)^k。
/// 返回 [(k, 回撤%) ] 对照表（手册 7.4 数值）。
pub fn streak_drawdown_table(risk_pct: f64, ks: &[usize]) -> Vec<(usize, f64)> {
    ks.iter()
        .map(|&k| {
            let remain = (1.0 - risk_pct).powi(k as i32);
            (k, (1.0 - remain) * 100.0)
        })
        .collect()
}

// ============================================================================
// Tests（已知答案合成序列）
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wilson_ci_known_values() {
        // 50/100：点估计 0.5，95% CI ≈ [0.4038, 0.5962]（教科书值）
        let (p, lo, hi) = binomial_ci(50, 100, 1.96);
        assert!((p - 0.5).abs() < 1e-12);
        assert!((lo - 0.4038).abs() < 5e-3);
        assert!((hi - 0.5962).abs() < 5e-3);
        // n=0 安全
        let (p0, lo0, hi0) = binomial_ci(0, 0, 1.96);
        assert!(p0.is_nan() && lo0 == 0.0 && hi0 == 1.0);
    }

    #[test]
    fn min_sample_size_matches_manual() {
        // 手册 7.5-③：53% vs 50% ≈ 1700；52% vs 50% ≈ 3900
        let n53 = min_sample_size(0.50, 0.53);
        let n52 = min_sample_size(0.50, 0.52);
        assert!((n53 - 1700.0).abs() < 200.0, "n53={n53}");
        assert!((n52 - 3900.0).abs() < 500.0, "n52={n52}");
        // 62% vs 60.5%（7.3 实证门槛）：同口径 ≈ 6600（基线方差更小）
        let n62 = min_sample_size(0.605, 0.62);
        assert!((n62 - 6600.0).abs() < 1200.0, "n62={n62}");
    }

    #[test]
    fn gate_verdicts() {
        // 700 胜 / 1000 笔 = 70%，CI 下沿 ~0.671 > 0.605 → Edge
        let (v, base, _) = ineffectiveness_gate(700, 1000, 0.605, 0.54, 100);
        assert_eq!(v, GateVerdict::Edge);
        assert!((base - 0.605).abs() < 1e-12);
        // 610/1000 = 61%，下沿 ~0.58 < 0.605 → NoEdge（即使点估计>0.5）
        let (v2, _, _) = ineffectiveness_gate(610, 1000, 0.605, 0.54, 100);
        assert_eq!(v2, GateVerdict::NoEdge);
        // 样本不足
        let (v3, _, _) = ineffectiveness_gate(15, 20, 0.605, 0.54, 100);
        assert_eq!(v3, GateVerdict::Insufficient);
        // 盈亏平衡更高时取更高者
        let (_, base2, _) = ineffectiveness_gate(610, 1000, 0.50, 0.54, 100);
        assert!((base2 - 0.54).abs() < 1e-12);
    }

    #[test]
    fn sharpe_known_series() {
        // 恒定正收益 → NaN（方差 0），引擎层应处理
        assert!(sharpe_annualized(&[0.01, 0.01, 0.01]).is_nan());
        // 对称正负 → Sharpe≈0
        let r: Vec<f64> = (0..100)
            .map(|i| if i % 2 == 0 { 0.01 } else { -0.01 })
            .collect();
        assert!(sharpe_annualized(&r).abs() < 0.2);
        // 稳定正漂移 → 显著正 Sharpe
        let up: Vec<f64> = (0..252).map(|i| 0.002 + (i % 5) as f64 * 0.0001).collect();
        assert!(sharpe_annualized(&up) > 5.0);
    }

    #[test]
    fn norm_cdf_ppf_roundtrip() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-7);
        assert!((norm_cdf(1.96) - 0.975).abs() < 1e-4);
        assert!((norm_cdf(-1.645) - 0.05).abs() < 1e-3);
        for p in [0.01, 0.1, 0.5, 0.9, 0.99] {
            assert!((norm_cdf(norm_ppf(p)) - p).abs() < 1e-6);
        }
    }

    #[test]
    fn dsr_behavior() {
        // 强趋势 + 多试验 → DSR 应仍高；同一序列试验越多 DSR 越低
        let strong: Vec<f64> = (0..500)
            .map(|i| 0.004 + (i % 3) as f64 * 0.0004 - 0.0004)
            .collect();
        let sr = sharpe_annualized(&strong);
        assert!(sr > 2.0, "sr={sr}");
        let dsr1 = deflated_sharpe(sr, 500, &strong, 1);
        let dsr100 = deflated_sharpe(sr, 500, &strong, 100);
        assert!(dsr1 > 0.99, "dsr1={dsr1}");
        assert!(
            dsr100 <= dsr1 + 1e-12,
            "试验越多 DSR 不升: {dsr100} <= {dsr1}"
        );
        assert!(dsr100 > 0.9, "强信号即使 100 次试验仍应高: {dsr100}");
        // 中等 Sharpe（≈1.8，类实盘有效但非超神）：100 次试验应把 DSR 拉到 <0.95
        // （这才是校正的意义——Sharpe 1.8 试 100 次得出的不可信）
        let mut seed = 12345u64;
        let mut rand = || {
            // xorshift：确定性伪随机
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % 2000) as f64 / 1000.0 - 1.0
        };
        let mid: Vec<f64> = (0..500).map(|_| 0.0005 + 0.006 * rand()).collect();
        let sr_mid = sharpe_annualized(&mid);
        assert!(sr_mid > 0.5 && sr_mid < 4.0, "sr_mid={sr_mid}");
        let dsr_mid1 = deflated_sharpe(sr_mid, 500, &mid, 1);
        let dsr_mid100 = deflated_sharpe(sr_mid, 500, &mid, 100);
        assert!(
            dsr_mid100 < dsr_mid1 && dsr_mid100 < 0.95,
            "中等信号多试验应拉低: dsr1={dsr_mid1} dsr100={dsr_mid100}"
        );
        // 噪声序列 Sharpe≈0 → DSR 应 < 0.95（多试验下）
        let noise: Vec<f64> = (0..500)
            .map(|i| ((i * 7919) % 7) as f64 * 0.001 - 0.003)
            .collect();
        let sr_n = sharpe_annualized(&noise);
        let dsr_n = deflated_sharpe(sr_n, 500, &noise, 50);
        assert!(dsr_n < 0.95, "dsr_noise={dsr_n} (sr={sr_n})");
    }

    #[test]
    fn pbo_known() {
        assert_eq!(prob_backtest_overfit(&[1.0, 0.5, -0.2, 0.3]), 0.25);
        assert_eq!(prob_backtest_overfit(&[-1.0, -0.5]), 1.0);
        assert!(prob_backtest_overfit(&[]).is_nan());
    }

    #[test]
    fn streak_table_matches_manual_7_4() {
        // 手册 7.4：0.75% 固定分数，连亏 5/10/15/20/30 → 3.7/7.3/10.7/14.0/20.2%
        let t = streak_drawdown_table(0.0075, &[5, 10, 15, 20, 30]);
        let expect = [3.7, 7.3, 10.7, 14.0, 20.2];
        for ((_, dd), e) in t.iter().zip(expect) {
            assert!((dd - e).abs() < 0.15, "dd={dd} expect={e}");
        }
    }
}
