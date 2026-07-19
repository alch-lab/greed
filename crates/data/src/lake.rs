//! 本地数据湖：历史数据的写入与按时间范围流式读取。
//!
//! 规划接口：
//! ```ignore
//! pub fn ingest_aggtrades(symbol, month) -> Result<IngestStats>;   
//! pub fn stream_trades(symbol, from, to) -> impl Iterator<Item = Trade>;
//! ```

/// 数据湖根目录占位常量（默认 `data/lake`，可被配置覆盖）。
pub const DEFAULT_LAKE_DIR: &str = "data/lake";
