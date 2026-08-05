//! 原始订单流信号。
//!
//! 生产策略只保留一条可解释链路：位置（扫流动性/VWAP 偏离）→ 主动成交
//! 力竭（大量 effort、很少 result）→ Delta 反转确认。旧的 EMA/MR/Renko
//! 近似模型已退出装配入口，避免同名概念对应不同实现。

pub mod intraday_extension_reversion;
pub mod orderflow_exhaustion;
pub mod trdr_market_map;
