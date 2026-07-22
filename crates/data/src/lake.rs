//! 本地数据湖：Trade 记录的 Parquet 写入与读取
//!
//! 布局：`{lake_dir}/trades/{exchange}/{symbol}/{yyyy-mm-dd}.parquet`，按天分片。
//! - 价格/数量存定点 i64（与 core 一致，零精度损失）。
//! - 压缩用 Snappy（列存 + 快速回放）。
//! - 读取侧提供按时间范围的有序收集 [`read_range`]。

use parquet2::compression::CompressionOptions;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, Page};
use parquet2::statistics::{serialize_statistics, PrimitiveStatistics, Statistics};
use parquet2::types::NativeType;
use parquet2::write::{
    Compressor, DynIter, DynStreamingIterator, FileWriter, Version, WriteOptions,
};
use std::fs::{self, File};
use std::io::BufWriter;
use std::matches;
use std::path::{Path, PathBuf};
use tcore::event::Trade;
use tcore::types::{Exchange, Symbol, Timestamp};
use thiserror::Error;

use crate::normalize::NormalizedTrade;
use crate::schema::{book_schema, oi_schema, trade_schema, BookRow, OiRow, TradeColumns};

#[derive(Debug, Error)]
pub enum LakeError {
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet 错误: {0}")]
    Parquet(#[from] parquet2::error::Error),
    #[error("数据错误: {0}")]
    Data(String),
}

/// 数据湖根目录默认。
pub const DEFAULT_LAKE_DIR: &str = "data/lake";

/// 数据湖句柄。
#[derive(Debug, Clone)]
pub struct Lake {
    root: PathBuf,
}

impl Lake {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Lake { root: root.into() }
    }
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 某交易所/交易对/日期（yyyy-mm-dd）的分片路径。
    pub fn shard_path(&self, exchange: Exchange, symbol: &Symbol, date: &str) -> PathBuf {
        self.dir(exchange, symbol).join(format!("{}.parquet", date))
    }

    /// 某交易所/交易对的目录。
    pub fn dir(&self, exchange: Exchange, symbol: &Symbol) -> PathBuf {
        self.root
            .join("trades")
            .join(exchange.as_str())
            .join(symbol.as_str())
    }

    /// 订单簿快照目录：`{root}/book/{exchange}/{symbol}/`
    pub fn book_dir(&self, exchange: Exchange, symbol: &Symbol) -> PathBuf {
        self.root
            .join("book")
            .join(exchange.as_str())
            .join(symbol.as_str())
    }

    /// OI 目录：`{root}/oi/{exchange}/symbol}/`
    pub fn oi_dir(&self, exchange: Exchange, symbol: &Symbol) -> PathBuf {
        self.root
            .join("oi")
            .join(exchange.as_str())
            .join(symbol.as_str())
    }
}

// ============================================================================
// 写入
// ============================================================================

type PqResult<T> = Result<T, parquet2::error::Error>;

/// 普通 Vec<T>（无空值）→ DataPage（Plain 编码，全 required 无 validity）。
fn vec_to_page<T: NativeType>(
    values: &[T],
    options: &WriteOptions,
    descriptor: &Descriptor,
) -> PqResult<Page> {
    let mut buffer = vec![];
    for v in values {
        buffer.extend_from_slice(v.to_le_bytes().as_ref());
    }
    let statistics = if options.write_statistics && !values.is_empty() {
        let s = &PrimitiveStatistics {
            primitive_type: descriptor.primitive_type.clone(),
            null_count: Some(0),
            distinct_count: None,
            max_value: values.iter().max_by(|a, b| a.ord(b)).copied(),
            min_value: values.iter().min_by(|a, b| a.ord(b)).copied(),
        } as &dyn Statistics;
        Some(serialize_statistics(s))
    } else {
        None
    };
    let header = DataPageHeaderV1 {
        num_values: values.len() as i32,
        encoding: parquet2::encoding::Encoding::Plain.into(),
        definition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
        repetition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
        statistics,
    };
    Ok(Page::Data(DataPage::new(
        DataPageHeader::V1(header),
        buffer,
        descriptor.clone(),
        Some(values.len()),
    )))
}

/// 布尔列 → DataPage（Plain 布尔，1 字节/值）。
fn bool_to_page(values: &[bool], descriptor: &Descriptor) -> PqResult<Page> {
    let buffer: Vec<u8> = values.iter().map(|b| u8::from(*b)).collect();
    Ok(Page::Data(DataPage::new(
        DataPageHeader::V1(DataPageHeaderV1 {
            num_values: values.len() as i32,
            encoding: parquet2::encoding::Encoding::Plain.into(),
            definition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
            repetition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
            statistics: None,
        }),
        buffer,
        descriptor.clone(),
        Some(values.len()),
    )))
}

/// 二进制列 → DataPage（Plain：4 字节小端长度 + 字节）。
fn binary_to_page(values: &[Vec<u8>], descriptor: &Descriptor) -> PqResult<Page> {
    let mut buffer = vec![];
    for v in values {
        buffer.extend_from_slice(&(v.len() as u32).to_le_bytes());
        buffer.extend_from_slice(v);
    }
    Ok(Page::Data(DataPage::new(
        DataPageHeader::V1(DataPageHeaderV1 {
            num_values: values.len() as i32,
            encoding: parquet2::encoding::Encoding::Plain.into(),
            definition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
            repetition_level_encoding: parquet2::encoding::Encoding::Rle.into(),
            statistics: None,
        }),
        buffer,
        descriptor.clone(),
        Some(values.len()),
    )))
}

/// 把一个分片的记录写入 Parquet 文件，返回写入行数。
pub fn write_shard(
    path: &Path,
    records: &[NormalizedTrade],
    compression: CompressionOptions,
) -> Result<usize, LakeError> {
    if records.is_empty() {
        return Ok(0);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    // 拆列
    let mut ts_ms = Vec::with_capacity(records.len());
    let mut exchange = Vec::with_capacity(records.len());
    let mut symbol = Vec::with_capacity(records.len());
    let mut price_raw = Vec::with_capacity(records.len());
    let mut qty_raw = Vec::with_capacity(records.len());
    let mut is_buyer_maker = Vec::with_capacity(records.len());
    let mut agg_id = Vec::with_capacity(records.len());
    for r in records {
        ts_ms.push(r.trade.ts.as_millis());
        exchange.push(r.trade.exchange.as_str().as_bytes().to_vec());
        symbol.push(r.trade.symbol.as_str().as_bytes().to_vec());
        price_raw.push(r.trade.price.raw());
        qty_raw.push(r.trade.qty.raw());
        is_buyer_maker.push(r.trade.is_buyer_maker);
        agg_id.push(r.agg_trade_id);
    }

    let schema = trade_schema();
    // 统一关闭统计：bool/binary 列未生成统计，避免与 options 不一致。
    // 后续如需谓词下推，可为数值列单独开启。
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };
    let cols = schema.columns().to_vec();

    // 每列一个压缩 page 迭代器
    let make_col = |page: Page| {
        DynStreamingIterator::new(Compressor::new_from_vec(
            DynIter::new(std::iter::once(Ok::<Page, parquet2::error::Error>(page))),
            compression,
            vec![],
        ))
    };
    let column_iters = vec![
        Ok(make_col(vec_to_page(
            &ts_ms,
            &options,
            &cols[0].descriptor,
        )?)),
        Ok(make_col(binary_to_page(&exchange, &cols[1].descriptor)?)),
        Ok(make_col(binary_to_page(&symbol, &cols[2].descriptor)?)),
        Ok(make_col(vec_to_page(
            &price_raw,
            &options,
            &cols[3].descriptor,
        )?)),
        Ok(make_col(vec_to_page(
            &qty_raw,
            &options,
            &cols[4].descriptor,
        )?)),
        Ok(make_col(bool_to_page(
            &is_buyer_maker,
            &cols[5].descriptor,
        )?)),
        Ok(make_col(vec_to_page(
            &agg_id,
            &options,
            &cols[6].descriptor,
        )?)),
    ];
    let row_group = DynIter::new(column_iters.into_iter());

    let file = BufWriter::new(File::create(path)?);
    let mut writer = FileWriter::new(file, schema, options, Some("greed-lake".into()));
    writer.write(row_group)?;
    writer.end(None)?;
    Ok(records.len())
}

// ============================================================================
// book / oi 表写入
// ============================================================================

/// 通用列式写出：一组 DataPage 按 schema 写为单 row group 文件。
fn write_columnar(
    path: &Path,
    schema: parquet2::metadata::SchemaDescriptor,
    pages: Vec<Page>,
    compression: CompressionOptions,
) -> Result<(), LakeError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };
    let make_col = |page: Page| {
        DynStreamingIterator::new(Compressor::new_from_vec(
            DynIter::new(std::iter::once(Ok::<Page, parquet2::error::Error>(page))),
            compression,
            vec![],
        ))
    };
    let column_iters: Vec<_> = pages.into_iter().map(|p| Ok(make_col(p))).collect();
    let row_group = DynIter::new(column_iters.into_iter());
    let file = BufWriter::new(File::create(path)?);
    let mut writer = FileWriter::new(file, schema, options, Some("greed-lake".into()));
    writer.write(row_group)?;
    writer.end(None)?;
    Ok(())
}

/// 写入订单簿快照分片，返回行数。
pub fn write_book_shard(
    path: &Path,
    rows: &[BookRow],
    compression: CompressionOptions,
) -> Result<usize, LakeError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };
    let schema = book_schema();
    let cols = schema.columns().to_vec();
    let ts_ms: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
    let exchange: Vec<Vec<u8>> = rows
        .iter()
        .map(|r| r.exchange.clone().into_bytes())
        .collect();
    let symbol: Vec<Vec<u8>> = rows.iter().map(|r| r.symbol.clone().into_bytes()).collect();
    let bids: Vec<Vec<u8>> = rows.iter().map(|r| r.bids_json.clone()).collect();
    let asks: Vec<Vec<u8>> = rows.iter().map(|r| r.asks_json.clone()).collect();
    let uid: Vec<i64> = rows.iter().map(|r| r.last_update_id).collect();
    let pages = vec![
        vec_to_page(&ts_ms, &options, &cols[0].descriptor)?,
        binary_to_page(&exchange, &cols[1].descriptor)?,
        binary_to_page(&symbol, &cols[2].descriptor)?,
        binary_to_page(&bids, &cols[3].descriptor)?,
        binary_to_page(&asks, &cols[4].descriptor)?,
        vec_to_page(&uid, &options, &cols[5].descriptor)?,
    ];
    write_columnar(path, schema, pages, compression)?;
    Ok(rows.len())
}

/// 写入 OI 分片，返回行数。
pub fn write_oi_shard(
    path: &Path,
    rows: &[OiRow],
    compression: CompressionOptions,
) -> Result<usize, LakeError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V2,
    };
    let schema = oi_schema();
    let cols = schema.columns().to_vec();
    let ts_ms: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
    let exchange: Vec<Vec<u8>> = rows
        .iter()
        .map(|r| r.exchange.clone().into_bytes())
        .collect();
    let symbol: Vec<Vec<u8>> = rows.iter().map(|r| r.symbol.clone().into_bytes()).collect();
    let oi: Vec<i64> = rows.iter().map(|r| r.oi_raw).collect();
    let pages = vec![
        vec_to_page(&ts_ms, &options, &cols[0].descriptor)?,
        binary_to_page(&exchange, &cols[1].descriptor)?,
        binary_to_page(&symbol, &cols[2].descriptor)?,
        vec_to_page(&oi, &options, &cols[3].descriptor)?,
    ];
    write_columnar(path, schema, pages, compression)?;
    Ok(rows.len())
}

// ============================================================================
// 读取
// ============================================================================

use parquet2::read::{decompress, get_page_iterator, read_metadata};

/// 读取单个 Parquet 分片为列数据。
pub fn read_shard(path: &Path) -> Result<TradeColumns, LakeError> {
    let mut file = File::open(path)?;
    let metadata = read_metadata(&mut file)?;
    let schema = metadata.schema().clone();
    let mut cols = TradeColumns::default();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().to_string();
        for row_group in metadata.row_groups.iter() {
            let col_chunk = row_group
                .columns()
                .get(col_idx)
                .ok_or_else(|| LakeError::Data(format!("缺列 {}", name)))?;
            read_column_into(&mut file, col_chunk, &name, &mut cols)?;
        }
    }
    Ok(cols)
}

fn read_column_into(
    file: &mut File,
    col_chunk: &parquet2::metadata::ColumnChunkMetaData,
    name: &str,
    cols: &mut TradeColumns,
) -> Result<(), LakeError> {
    let pages = get_page_iterator(col_chunk, file, None, vec![], usize::MAX)?;
    let mut scratch = Vec::new();
    for page in pages {
        let compressed = page?;
        let page = decompress(compressed, &mut scratch)?;
        // 只处理数据页（本 schema 无字典页）
        let data_page = match page {
            Page::Data(dp) => dp,
            Page::Dict(_) => continue,
        };
        let buffer = data_page.buffer();
        match name {
            "ts_ms" => push_i64(buffer, &mut cols.ts_ms),
            "price_raw" => push_i64(buffer, &mut cols.price_raw),
            "qty_raw" => push_i64(buffer, &mut cols.qty_raw),
            "agg_trade_id" => push_i64(buffer, &mut cols.agg_trade_id),
            "is_buyer_maker" => push_bool(buffer, &mut cols.is_buyer_maker),
            "exchange" => push_binary(buffer, &mut cols.exchange),
            "symbol" => push_binary(buffer, &mut cols.symbol),
            _ => {}
        }
    }
    Ok(())
}

fn push_i64(buffer: &[u8], out: &mut Vec<i64>) {
    for chunk in buffer.chunks_exact(8) {
        out.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
}
fn push_bool(buffer: &[u8], out: &mut Vec<bool>) {
    for b in buffer {
        out.push(*b != 0);
    }
}
fn push_binary(buffer: &[u8], out: &mut Vec<Vec<u8>>) {
    let mut i = 0;
    while i + 4 <= buffer.len() {
        let len = u32::from_le_bytes(buffer[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + len > buffer.len() {
            break;
        }
        out.push(buffer[i..i + len].to_vec());
        i += len;
    }
}

/// 读取指定时间范围内的所有 Trade（跨分片，按时间升序）。
pub fn read_range(
    lake: &Lake,
    exchange: Exchange,
    symbol: &Symbol,
    from: Timestamp,
    to: Timestamp,
) -> Result<Vec<Trade>, LakeError> {
    let dir = lake.dir(exchange, symbol);
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut entries: Vec<PathBuf> = fs::read_dir(&dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            matches!(
                p.extension().and_then(|s| s.to_str()),
                Some("parquet") | Some("binlog")
            )
        })
        .collect();
    entries.sort();

    let mut trades: Vec<Trade> = Vec::new();
    for path in entries {
        if path.extension().and_then(|s| s.to_str()) == Some("binlog") {
            for row in crate::live::binlog::read_trade_log(&path)? {
                let t = row.into_trade().map_err(LakeError::Data)?;
                if t.ts >= from && t.ts < to {
                    trades.push(t);
                }
            }
        } else {
            let cols = read_shard(&path)?;
            for t in cols.into_trades().map_err(LakeError::Data)? {
                if t.ts >= from && t.ts < to {
                    trades.push(t);
                }
            }
        }
    }

    trades.sort_by_key(|t| t.ts);
    Ok(trades)
}

/// 读取单个订单簿分片为行（book 行数少，直接行式返回）。
pub fn read_book_shard(path: &Path) -> Result<Vec<BookRow>, LakeError> {
    if path.extension().and_then(|s| s.to_str()) == Some("binlog") {
        return crate::live::binlog::read_book_log(path);
    }

    let mut file = File::open(path)?;
    let metadata = read_metadata(&mut file)?;
    let schema = metadata.schema().clone();

    let mut ts_ms: Vec<i64> = Vec::new();
    let mut exchange: Vec<Vec<u8>> = Vec::new();
    let mut symbol: Vec<Vec<u8>> = Vec::new();
    let mut bids: Vec<Vec<u8>> = Vec::new();
    let mut asks: Vec<Vec<u8>> = Vec::new();
    let mut uid: Vec<i64> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().to_string();
        for row_group in metadata.row_groups.iter() {
            let col_chunk = row_group
                .columns()
                .get(col_idx)
                .ok_or_else(|| LakeError::Data(format!("缺列 {}", name)))?;
            let pages = get_page_iterator(col_chunk, &mut file, None, vec![], usize::MAX)?;
            let mut scratch = Vec::new();
            for page in pages {
                let compressed = page?;
                let page = decompress(compressed, &mut scratch)?;
                let data_page = match page {
                    Page::Data(dp) => dp,
                    Page::Dict(_) => continue,
                };
                let buffer = data_page.buffer();
                match name.as_str() {
                    "ts_ms" => push_i64(buffer, &mut ts_ms),
                    "exchange" => push_binary(buffer, &mut exchange),
                    "symbol" => push_binary(buffer, &mut symbol),
                    "bids_json" => push_binary(buffer, &mut bids),
                    "asks_json" => push_binary(buffer, &mut asks),
                    "last_update_id" => push_i64(buffer, &mut uid),
                    _ => {}
                }
            }
        }
    }

    let n = ts_ms.len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(BookRow {
            ts_ms: ts_ms[i],
            exchange: String::from_utf8(exchange[i].clone())
                .map_err(|e| LakeError::Data(format!("exchange 非 UTF-8: {}", e)))?,
            symbol: String::from_utf8(symbol[i].clone())
                .map_err(|e| LakeError::Data(format!("symbol 非 UTF-8: {}", e)))?,
            bids_json: bids[i].clone(),
            asks_json: asks[i].clone(),
            last_update_id: uid[i],
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Price, Qty};

    fn rec(ts_ms: i64, price: f64, qty: f64, ibm: bool, id: i64) -> NormalizedTrade {
        NormalizedTrade {
            trade: Trade {
                ts: Timestamp::from_millis(ts_ms),
                exchange: Exchange::BinanceFutures,
                symbol: Symbol::new("BTCUSDT"),
                price: Price::from_f64(price),
                qty: Qty::from_f64(qty),
                is_buyer_maker: ibm,
            },
            agg_trade_id: id,
        }
    }

    #[test]
    fn write_then_read_roundtrip() {
        let dir = std::env::temp_dir().join(format!("lake_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let lake = Lake::new(&dir);
        let path = lake.shard_path(
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT"),
            "2024-01-01",
        );
        let records = vec![
            rec(1000, 42313.9, 0.046, true, 111),
            rec(2000, 42314.0, 0.005, false, 112),
            rec(3000, 42320.5, 1.25, true, 113),
        ];
        let n = write_shard(&path, &records, CompressionOptions::Uncompressed).unwrap();
        assert_eq!(n, 3);

        let cols = read_shard(&path).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols.ts_ms, vec![1000, 2000, 3000]);
        assert_eq!(cols.agg_trade_id, vec![111, 112, 113]);
        let trades = cols.into_trades().unwrap();
        assert!((trades[0].price.to_f64() - 42313.9).abs() < 1e-6);
        assert!((trades[2].qty.to_f64() - 1.25).abs() < 1e-6);
        assert!(trades[0].is_buyer_maker);
        assert!(!trades[1].is_buyer_maker);

        // Snappy 压缩也能读
        let path2 = lake.shard_path(
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT"),
            "2024-01-02",
        );
        write_shard(&path2, &records, CompressionOptions::Snappy).unwrap();
        let cols2 = read_shard(&path2).unwrap();
        assert_eq!(cols2.len(), 3);

        // read_range 过滤（跨两个分片）
        let ranged = read_range(
            &lake,
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT"),
            Timestamp::from_millis(1500),
            Timestamp::from_millis(3000),
        )
        .unwrap();
        assert_eq!(ranged.len(), 2); // 两个分片各命中 ts=2000 那条
        assert!(ranged
            .iter()
            .all(|t| (t.price.to_f64() - 42314.0).abs() < 1e-6));

        let _ = fs::remove_dir_all(&dir);
    }
}
