//! signals：信号引擎（纯函数，可独立测试；回测与实盘共用）。
//!
//! - `renko`      → （Trend Reversal 200-124 反转砖）
//! - `exhaustion` → （力竭反转三条件，含 AGGR 大单速率）
//! - `obi`        → （色带 OBI，需采集数据）
//! - `agg_delta` / `oi_regime` → PR-9（总Delta阈值 / OI 四象限）

pub mod agg_delta;
pub mod exhaustion;
pub mod large_trade_flow;
pub mod obi;
pub mod oi_regime;
pub mod renko;

#[cfg(test)]
mod tests {
    #[test]
    fn signals_smoke() {
        // 冒烟测试：crate 可链接、模块可加载。
        assert_eq!(1 + 1, 2);
    }
}
