//! 实时采集 daemon
//!
//! 设计（币安优先；Bybit/OKX 后续适配）：
//! - **trades**：合约/现货 aggTrade WebSocket 订阅；断线重连（指数退避）后
//!   用 REST `aggTrades?fromId=` 回补断档期，按 agg_trade_id 去重。
//! - **订单簿**：REST `depth?limit=1000` 周期轮询（默认 5s），按 `±band_pct`
//!   过滤档位后落盘。v1 不走 WS diff depth（对齐复杂），REST 快照简单可靠。
//! - **OI**：REST `openInterest` 周期轮询（默认 60s）。
//! - **落盘**：内存缓冲，达到阈值或跨天切分写 `{date}.part-{ms}.parquet`；
//!   SIGINT/SIGTERM 优雅退出时全量 flush。
//!
//! 用法（CLI）：`greed collect --config config/base.toml [--dry-run]`。

pub mod binance_ws;
pub mod binlog;
pub mod book_poller;
pub mod config;
pub mod oi_poller;
pub mod shard_writer;
pub mod supervisor;

pub use config::CollectorConfig;
pub use shard_writer::{now_ms, utc_date_of, LiveEvent, ShardWriter};
pub use supervisor::run_collector;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CollectError {
    #[error("HTTP 错误: {0}")]
    Http(#[from] reqwest::Error),
    #[error("WebSocket 错误: {0}")]
    Ws(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("JSON 解析错误: {0}")]
    Json(#[from] serde_json::Error),
    #[error("数据湖错误: {0}")]
    Lake(Box<crate::lake::LakeError>),
    #[error("数据格式错误: {0}")]
    Data(String),
}

// 大错误类型装箱（保持 Result 瘦小，避免 result_large_err）。
impl From<tokio_tungstenite::tungstenite::Error> for CollectError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        CollectError::Ws(Box::new(e))
    }
}

impl From<crate::lake::LakeError> for CollectError {
    fn from(e: crate::lake::LakeError) -> Self {
        CollectError::Lake(Box::new(e))
    }
}
