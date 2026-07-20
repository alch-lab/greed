//! data：数据层。
//!
//! 落地历史数据导入：
//! - [`binance_dump`]：Binance 公共数据 dump 下载（aggTrades zip → CSV）。
//! - [`normalize`]：CSV → `tcore::Trade` 归一化（定点价格/数量）。
//! - [`lake`]：Parquet 数据湖的写入与按时间范围读取。
//! - [`schema`]：Trade 表的 Parquet schema 与列投影。
//!

pub mod binance_dump;
pub mod lake;
pub mod live;
pub mod normalize;
pub mod schema;

pub use binance_dump::{ingest_day, IngestStats, Market};
pub use lake::{Lake, LakeError, DEFAULT_LAKE_DIR};
pub use normalize::{parse_aggtrades_csv, NormalizedTrade};

#[cfg(test)]
mod tests {
    #[test]
    fn data_smoke() {
        // 冒烟测试：crate 可链接、模块可加载。
        assert_eq!(2 + 2, 4);
    }
}
