//! Append-only binlog 读取（读取侧；live 落盘的配套）。
//!
//! 文件布局：`[u32 len][bincode payload][u32 crc32]` × N，纯追加。
//! 读取遇「长度非法 / 尾部不足 / CRC 不匹配」时返回有效前缀（崩溃最多丢最后半条）。

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use tcore::event::Trade;
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

use crate::lake::LakeError;
use crate::normalize::NormalizedTrade;
use crate::schema::{BookRow, OiRow};

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// trades binlog 行（定点 i64，零精度损失）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogTrade {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub price_raw: i64,
    pub qty_raw: i64,
    pub is_buyer_maker: bool,
    pub agg_trade_id: i64,
}

impl From<&NormalizedTrade> for BinlogTrade {
    fn from(r: &NormalizedTrade) -> Self {
        Self {
            ts_ms: r.trade.ts.as_millis(),
            exchange: r.trade.exchange.as_str().to_string(),
            symbol: r.trade.symbol.as_str().to_string(),
            price_raw: r.trade.price.raw(),
            qty_raw: r.trade.qty.raw(),
            is_buyer_maker: r.trade.is_buyer_maker,
            agg_trade_id: r.agg_trade_id,
        }
    }
}

impl BinlogTrade {
    pub fn into_trade(self) -> Result<Trade, String> {
        let exchange = Exchange::parse(&self.exchange)
            .ok_or_else(|| format!("未知交易所: {}", self.exchange))?;
        Ok(Trade {
            ts: Timestamp::from_millis(self.ts_ms),
            exchange,
            symbol: Symbol::new(&self.symbol),
            price: Price::from_raw(self.price_raw),
            qty: Qty::from_raw(self.qty_raw),
            is_buyer_maker: self.is_buyer_maker,
        })
    }
}

/// book binlog 行。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogBook {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub bids_json: Vec<u8>,
    pub asks_json: Vec<u8>,
    pub last_update_id: i64,
}

/// oi binlog 行。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogOi {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub oi_raw: i64,
}

/// 某一日期的 binlog 路径：`{dir}/{yyyy-mm-dd}.binlog`。
pub fn day_path(dir: PathBuf, date: &str) -> PathBuf {
    dir.join(format!("{}.binlog", date))
}

/// 读取一个 binlog 文件为记录向量；坏尾/撕裂尾按有效日志结束处理。
pub fn read_log<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, LakeError> {
    let bytes = fs::read(path)?;
    let mut out = Vec::new();
    let mut cur = 0usize;
    while cur + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap()) as usize;
        if len == 0 || len > MAX_RECORD_BYTES {
            break;
        }
        let payload_start = cur + 4;
        let payload_end = payload_start.saturating_add(len);
        let crc_end = payload_end.saturating_add(4);
        if crc_end > bytes.len() {
            break;
        }
        let payload = &bytes[payload_start..payload_end];
        let want = u32::from_le_bytes(bytes[payload_end..crc_end].try_into().unwrap());
        if crc32fast::hash(payload) != want {
            break;
        }
        out.push(
            bincode::deserialize(payload)
                .map_err(|e| LakeError::Data(format!("bincode 反序列化失败: {e}")))?,
        );
        cur = crc_end;
    }
    Ok(out)
}

/// 读取 trades binlog。
pub fn read_trade_log(path: &Path) -> Result<Vec<BinlogTrade>, LakeError> {
    read_log(path)
}

/// 读取 book binlog（还原为 BookRow）。
pub fn read_book_log(path: &Path) -> Result<Vec<BookRow>, LakeError> {
    let rows: Vec<BinlogBook> = read_log(path)?;
    Ok(rows
        .into_iter()
        .map(|r| BookRow {
            ts_ms: r.ts_ms,
            exchange: r.exchange,
            symbol: r.symbol,
            bids_json: r.bids_json,
            asks_json: r.asks_json,
            last_update_id: r.last_update_id,
        })
        .collect())
}

/// 读取 oi binlog。
pub fn read_oi_log(path: &Path) -> Result<Vec<OiRow>, LakeError> {
    let rows: Vec<BinlogOi> = read_log(path)?;
    Ok(rows
        .into_iter()
        .map(|r| OiRow {
            ts_ms: r.ts_ms,
            exchange: r.exchange,
            symbol: r.symbol,
            oi_raw: r.oi_raw,
        })
        .collect())
}

/// 把记录追加写入 binlog（不存在则创建）。
fn append_log<T: Serialize>(path: &Path, records: &[T]) -> Result<usize, LakeError> {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    // 如果是空文件，从头开始写；否则追加到末尾。
    let start = file.seek(SeekFrom::End(0))?;
    let mut written = 0usize;

    let mut buf = Vec::new();
    for rec in records {
        buf.clear();
        bincode::serialize_into(&mut buf, rec)
            .map_err(|e| LakeError::Data(format!("bincode 序列化失败: {e}")))?;
        let len = buf.len() as u32;
        let crc = crc32fast::hash(&buf);
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&buf)?;
        file.write_all(&crc.to_le_bytes())?;
        written += 1;
    }

    file.flush()?;

    // 简单校验：若之前是空文件，至少保证写入了一些字节；否则信任 append。
    if start == 0 && written == 0 && !records.is_empty() {
        return Err(LakeError::Data("binlog 写入失败：未写入任何记录".into()));
    }
    Ok(written)
}

/// 追加 trades 到 binlog。
pub fn append_trade_log(path: &Path, records: &[NormalizedTrade]) -> Result<usize, LakeError> {
    let binlogs: Vec<BinlogTrade> = records.iter().map(BinlogTrade::from).collect();
    append_log(path, &binlogs)
}

/// 追加 book 行到 binlog。
pub fn append_book_log(path: &Path, records: &[BookRow]) -> Result<usize, LakeError> {
    let binlogs: Vec<BinlogBook> = records
        .iter()
        .map(|r| BinlogBook {
            ts_ms: r.ts_ms,
            exchange: r.exchange.clone(),
            symbol: r.symbol.clone(),
            bids_json: r.bids_json.clone(),
            asks_json: r.asks_json.clone(),
            last_update_id: r.last_update_id,
        })
        .collect();
    append_log(path, &binlogs)
}

/// 追加 oi 行到 binlog。
pub fn append_oi_log(path: &Path, records: &[OiRow]) -> Result<usize, LakeError> {
    let binlogs: Vec<BinlogOi> = records
        .iter()
        .map(|r| BinlogOi {
            ts_ms: r.ts_ms,
            exchange: r.exchange.clone(),
            symbol: r.symbol.clone(),
            oi_raw: r.oi_raw,
        })
        .collect();
    append_log(path, &binlogs)
}
