//! Append-only binlog 存储
//!
//! 文件布局：`[u32 len][bincode payload][u32 crc32]` × N，纯追加。
//! 读取遇「长度非法 / 尾部不足 / CRC 不匹配」时，返回有效前缀；崩溃最多丢最后半条。

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tcore::event::Trade;
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

use crate::lake::LakeError;
use crate::normalize::NormalizedTrade;
use crate::schema::{BookRow, OiRow};

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogBook {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub bids_json: Vec<u8>,
    pub asks_json: Vec<u8>,
    pub last_update_id: i64,
}

impl From<&BookRow> for BinlogBook {
    fn from(r: &BookRow) -> Self {
        Self {
            ts_ms: r.ts_ms,
            exchange: r.exchange.clone(),
            symbol: r.symbol.clone(),
            bids_json: r.bids_json.clone(),
            asks_json: r.asks_json.clone(),
            last_update_id: r.last_update_id,
        }
    }
}

impl From<BinlogBook> for BookRow {
    fn from(r: BinlogBook) -> Self {
        Self {
            ts_ms: r.ts_ms,
            exchange: r.exchange,
            symbol: r.symbol,
            bids_json: r.bids_json,
            asks_json: r.asks_json,
            last_update_id: r.last_update_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogOi {
    pub ts_ms: i64,
    pub exchange: String,
    pub symbol: String,
    pub oi_raw: i64,
}

impl From<&OiRow> for BinlogOi {
    fn from(r: &OiRow) -> Self {
        Self {
            ts_ms: r.ts_ms,
            exchange: r.exchange.clone(),
            symbol: r.symbol.clone(),
            oi_raw: r.oi_raw,
        }
    }
}

impl From<BinlogOi> for OiRow {
    fn from(r: BinlogOi) -> Self {
        Self {
            ts_ms: r.ts_ms,
            exchange: r.exchange,
            symbol: r.symbol,
            oi_raw: r.oi_raw,
        }
    }
}

pub fn day_path(dir: PathBuf, date: &str) -> PathBuf {
    dir.join(format!("{}.binlog", date))
}

struct BinlogAppender {
    w: BufWriter<File>,
}

impl BinlogAppender {
    fn open(path: &Path) -> Result<Self, LakeError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            w: BufWriter::new(file),
        })
    }

    fn append<T: Serialize>(&mut self, rec: &T) -> Result<(), LakeError> {
        let payload = bincode::serialize(rec)
            .map_err(|e| LakeError::Data(format!("bincode 序列化失败: {e}")))?;
        if payload.is_empty() || payload.len() > MAX_RECORD_BYTES {
            return Err(LakeError::Data(format!(
                "binlog payload 长度非法: {}",
                payload.len()
            )));
        }
        let len = payload.len() as u32;
        let crc = crc32fast::hash(&payload);
        self.w.write_all(&len.to_le_bytes())?;
        self.w.write_all(&payload)?;
        self.w.write_all(&crc.to_le_bytes())?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), LakeError> {
        self.w.flush()?;
        self.w.get_ref().sync_data()?;
        Ok(())
    }
}

pub fn append_trade_log(path: &Path, records: &[NormalizedTrade]) -> Result<usize, LakeError> {
    if records.is_empty() {
        return Ok(0);
    }
    let mut app = BinlogAppender::open(path)?;
    for r in records {
        app.append(&BinlogTrade::from(r))?;
    }
    app.flush()?;
    Ok(records.len())
}

pub fn append_book_log(path: &Path, rows: &[BookRow]) -> Result<usize, LakeError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut app = BinlogAppender::open(path)?;
    for r in rows {
        app.append(&BinlogBook::from(r))?;
    }
    app.flush()?;
    Ok(rows.len())
}

pub fn append_oi_log(path: &Path, rows: &[OiRow]) -> Result<usize, LakeError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut app = BinlogAppender::open(path)?;
    for r in rows {
        app.append(&BinlogOi::from(r))?;
    }
    app.flush()?;
    Ok(rows.len())
}

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

pub fn read_trade_log(path: &Path) -> Result<Vec<BinlogTrade>, LakeError> {
    read_log(path)
}

pub fn read_book_log(path: &Path) -> Result<Vec<BookRow>, LakeError> {
    let rows: Vec<BinlogBook> = read_log(path)?;
    Ok(rows.into_iter().map(BookRow::from).collect())
}

pub fn read_oi_log(path: &Path) -> Result<Vec<OiRow>, LakeError> {
    let rows: Vec<BinlogOi> = read_log(path)?;
    Ok(rows.into_iter().map(OiRow::from).collect())
}
