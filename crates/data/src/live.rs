//! 实时采集：三所 WebSocket 采集器 daemon。
//!
//! 规划：
//! - Binance / Bybit / OKX 的 trades + depth 快照(5s) + OI(60s) 订阅
//! - 断线重连、序列号 gap 检测、重快照
//! - 按天分片写入数据湖，schema 与历史回测一致
