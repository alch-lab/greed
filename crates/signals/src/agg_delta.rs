//! 总 Delta 阈值
//!
//! 滚动累加全网主动买卖差 AccDelta，按 T1–T4 档位给出反转枯竭信号；
//! 并用多空极值不对称性驱动趋势状态机（RANGE / TREND_UP / TREND_DOWN）。
