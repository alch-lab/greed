//! 数据湖 Parquet schema 与列定义
//!
//! 设计要点：
//! - **价格/数量存定点 i64**（×1e8，与 `core::types` 的 `Price`/`Qty` 一致），读写零精度损失。
//! - 交易所存 `&str`（与 `Exchange::as_str()` 对应），避免枚举编码复杂度。
//! - 读取侧以**列投影**方式解码为 `Vec<Trade>`，列顺序必须与 [`TRADE_COLUMNS`] 一致。
//! - **book 表**的 bids/asks 存 JSON 文本列（`[[price_raw, qty_raw], ...]` 定点对），
//!   配合 zstd 压缩平衡体积与读写简单性（嵌套 list<struct> 留待后续优化）。

use parquet2::metadata::SchemaDescriptor;
use parquet2::schema::types::{ParquetType, PhysicalType};
use tcore::event::Trade;
use tcore::types::Timestamp;

/// Trade 表的列名（读写两侧共用，保证投影顺序一致）。
pub const TRADE_COLUMNS: [&str; 7] = [
    "ts_ms",
    "exchange",
    "symbol",
    "price_raw",
    "qty_raw",
    "is_buyer_maker",
    "agg_trade_id",
];

/// 构造 Trade 表的 Parquet schema。
///
/// 列全部为 required（非空）：
/// - `ts_ms`          Int64   成交时间（UTC 毫秒）
/// - `exchange`       Binary  交易所（"binance_futures" 等）
/// - `symbol`         Binary  交易对（"BTCUSDT"）
/// - `price_raw`      Int64   定点价格（×1e8）
/// - `qty_raw`        Int64   定点数量（×1e8）
/// - `is_buyer_maker` Boolean 买方是否 maker
/// - `agg_trade_id`   Int64   聚合成交 id（去重/审计用）
pub fn trade_schema() -> SchemaDescriptor {
    use PhysicalType as P;
    let fields = vec![
        ParquetType::from_physical("ts_ms".into(), P::Int64),
        ParquetType::from_physical("exchange".into(), P::ByteArray),
        ParquetType::from_physical("symbol".into(), P::ByteArray),
        ParquetType::from_physical("price_raw".into(), P::Int64),
        ParquetType::from_physical("qty_raw".into(), P::Int64),
        ParquetType::from_physical("is_buyer_maker".into(), P::Boolean),
        ParquetType::from_physical("agg_trade_id".into(), P::Int64),
    ];
    SchemaDescriptor::new("trades".to_string(), fields)
}

// ============================================================================
// book 表（PR-11）：订单簿快照
// ============================================================================

/// book 表列名。
pub const BOOK_COLUMNS: [&str; 6] = [
    "ts_ms",
    "exchange",
    "symbol",
    "bids_json",
    "asks_json",
    "last_update_id",
];

/// 订单簿快照表 schema。
///
/// - `ts_ms`          Int64   快照时间（UTC 毫秒，本地接收时刻）
/// - `exchange`       Binary  交易所
/// - `symbol`         Binary  交易对
/// - `bids_json`      Binary  bid 档位 JSON：`[[price_raw, qty_raw], ...]`（价格降序）
/// - `asks_json`      Binary  ask 档位 JSON（价格升序）
/// - `last_update_id` Int64   交易所侧更新 id（审计/连续性检查）
pub fn book_schema() -> SchemaDescriptor {
    use PhysicalType as P;
    let fields = vec![
        ParquetType::from_physical("ts_ms".into(), P::Int64),
        ParquetType::from_physical("exchange".into(), P::ByteArray),
        ParquetType::from_physical("symbol".into(), P::ByteArray),
        ParquetType::from_physical("bids_json".into(), P::ByteArray),
        ParquetType::from_physical("asks_json".into(), P::ByteArray),
        ParquetType::from_physical("last_update_id".into(), P::Int64),
    ];
    SchemaDescriptor::new("book".to_string(), fields)
}

/// book 表的一行（落盘前）。
#[derive(Debug, Clone)]
pub struct BookRow {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub bids_json: Vec<u8>,
    pub asks_json: Vec<u8>,
    pub last_update_id: i64,
}

// ============================================================================
// oi 表：持仓量
// ============================================================================

/// oi 表列名。
pub const OI_COLUMNS: [&str; 4] = ["ts_ms", "exchange", "symbol", "oi_raw"];

/// 持仓量表 schema：`oi_raw` 为定点 i64（×1e8，单位币）。
pub fn oi_schema() -> SchemaDescriptor {
    use PhysicalType as P;
    let fields = vec![
        ParquetType::from_physical("ts_ms".into(), P::Int64),
        ParquetType::from_physical("exchange".into(), P::ByteArray),
        ParquetType::from_physical("symbol".into(), P::ByteArray),
        ParquetType::from_physical("oi_raw".into(), P::Int64),
    ];
    SchemaDescriptor::new("oi".to_string(), fields)
}

/// oi 表的一行。
#[derive(Debug, Clone)]
pub struct OiRow {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub oi_raw: i64,
}

/// 一列解码后的中间表示（从 Parquet 读出、尚未组装为 Trade）。
#[derive(Debug, Default)]
pub struct TradeColumns {
    pub ts_ms: Vec<i64>,
    pub exchange: Vec<Vec<u8>>,
    pub symbol: Vec<Vec<u8>>,
    pub price_raw: Vec<i64>,
    pub qty_raw: Vec<i64>,
    pub is_buyer_maker: Vec<bool>,
    pub agg_trade_id: Vec<i64>,
}

impl TradeColumns {
    pub fn len(&self) -> usize {
        self.ts_ms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts_ms.is_empty()
    }

    /// 把列数据组装为 `Vec<Trade>`。
    pub fn into_trades(self) -> Result<Vec<Trade>, String> {
        let n = self.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let exchange = tcore::types::Exchange::parse(
                std::str::from_utf8(&self.exchange[i]).map_err(|e| e.to_string())?,
            )
            .ok_or_else(|| format!("未知交易所: {:?}", self.exchange[i]))?;
            let symbol = tcore::types::Symbol::new(
                std::str::from_utf8(&self.symbol[i]).map_err(|e| e.to_string())?,
            );
            out.push(Trade {
                ts: Timestamp::from_millis(self.ts_ms[i]),
                exchange,
                symbol,
                price: tcore::types::Price::from_raw(self.price_raw[i]),
                qty: tcore::types::Qty::from_raw(self.qty_raw[i]),
                is_buyer_maker: self.is_buyer_maker[i],
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_seven_columns() {
        let s = trade_schema();
        assert_eq!(s.columns().len(), 7);
    }

    #[test]
    fn columns_roundtrip_to_trades() {
        let cols = TradeColumns {
            ts_ms: vec![1000, 2000],
            exchange: vec![b"binance_futures".to_vec(), b"binance_spot".to_vec()],
            symbol: vec![b"BTCUSDT".to_vec(), b"BTCUSDT".to_vec()],
            price_raw: vec![tcore::types::Price::from_f64(100.5).raw(), 200_000],
            qty_raw: vec![tcore::types::Qty::from_f64(0.25).raw(), 100_000_000],
            is_buyer_maker: vec![true, false],
            agg_trade_id: vec![1, 2],
        };
        let trades = cols.into_trades().unwrap();
        assert_eq!(trades.len(), 2);
        assert!((trades[0].price.to_f64() - 100.5).abs() < 1e-6);
        assert!((trades[0].qty.to_f64() - 0.25).abs() < 1e-6);
        assert!(trades[0].is_buyer_maker);
        assert_eq!(trades[1].exchange, tcore::types::Exchange::BinanceSpot);
        assert!(!trades[1].is_buyer_maker);
    }

    #[test]
    fn book_schema_columns() {
        assert_eq!(book_schema().columns().len(), 6);
        assert_eq!(oi_schema().columns().len(), 4);
    }
}
