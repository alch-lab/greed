//! 力竭反转：三条件入场扳机的信号侧。
//!
//! 1) 放量快速（必要）：砖速率 ≥ vol_mult_exh × 基线，且 duration ≤ dur_max
//! 2) 力竭不推进：小实体/长影线 或 P 型 footprint（努力≠结果）
//! 3) Delta 反转：delta% 翻向且 |delta%| ≥ delta_flip
//!
//! 另含 AGGR 替代：大单流速率预警（≥ size_large 的笔数/秒）。
