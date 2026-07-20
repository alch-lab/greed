//! Binance 公共数据 dump 下载与入库
//!
//! 数据源：`https://data.binance.vision/data/{market}/daily/aggTrades/{symbol}/{symbol}-aggTrades-{yyyy}-{mm}-{dd}.zip`
//! - `market`：`futures/um`（USDT 永续）或 `spot`。
//! - 文件为 zip 压缩的单个 CSV。
//!
//! 本模块提供：单日下载（含 zip 解压）→ 解析 → 写数据湖分片的完整管线。

use std::io::Cursor;
use std::path::Path;
use tcore::types::{Exchange, Symbol};
use thiserror::Error;
use tracing::{info, warn};

use crate::lake::{write_shard, Lake, LakeError};
use crate::normalize::{parse_aggtrades_csv, NormalizeError};
use parquet2::compression::CompressionOptions;

#[derive(Debug, Error)]
pub enum DumpError {
    #[error("HTTP 错误: {0}")]
    Http(String),
    #[error("解压失败: {0}")]
    Zip(String),
    #[error(transparent)]
    Normalize(#[from] NormalizeError),
    #[error(transparent)]
    Lake(#[from] LakeError),
}

/// Binance dump 的市场类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Market {
    /// USDT 永续合约（futures/um）
    UsdtPerp,
    /// 现货
    Spot,
}

impl Market {
    /// URL 路径段。
    pub fn url_path(self) -> &'static str {
        match self {
            Market::UsdtPerp => "futures/um",
            Market::Spot => "spot",
        }
    }
    /// 对应的交易所枚举。
    pub fn exchange(self) -> Exchange {
        match self {
            Market::UsdtPerp => Exchange::BinanceFutures,
            Market::Spot => Exchange::BinanceSpot,
        }
    }
}

const BASE: &str = "https://data.binance.vision/data";

/// 构造某日 aggTrades 的下载 URL。
pub fn daily_url(market: Market, symbol: &str, date: &str) -> String {
    format!(
        "{}/{}/daily/aggTrades/{}/{}-aggTrades-{}.zip",
        BASE,
        market.url_path(),
        symbol,
        symbol,
        date
    )
}

/// 入库统计。
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestStats {
    pub rows: usize,
    pub bytes: usize,
}

/// 下载某一天的 aggTrades 并写入数据湖。
///
/// `date` 格式 `yyyy-mm-dd`。返回写入行数；若该日无数据（404）返回 `Ok(None)`。
pub async fn ingest_day(
    client: &reqwest::Client,
    lake: &Lake,
    market: Market,
    symbol: &str,
    date: &str,
) -> Result<Option<IngestStats>, DumpError> {
    let url = daily_url(market, symbol, date);
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| DumpError::Http(format!("请求失败 {}: {}", url, e)))?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        warn!(%url, "该日无数据(404)");
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(DumpError::Http(format!(
            "HTTP {} for {}",
            resp.status(),
            url
        )));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| DumpError::Http(e.to_string()))?;
    let nbytes = bytes.len();

    // 解压 zip 中的第一个 CSV
    let csv_bytes = unzip_first(&bytes)?;
    let records = parse_aggtrades_csv(
        Cursor::new(csv_bytes),
        market.exchange(),
        &Symbol::new(symbol),
    )?;
    let rows = records.len();

    let path = lake.shard_path(market.exchange(), &Symbol::new(symbol), date);
    write_shard(Path::new(&path), &records, CompressionOptions::Snappy)?;
    info!(%date, rows, "已写入分片");
    Ok(Some(IngestStats {
        rows,
        bytes: nbytes,
    }))
}

/// 从 zip 字节中解压第一个文件。
fn unzip_first(bytes: &[u8]) -> Result<Vec<u8>, DumpError> {
    let reader = Cursor::new(bytes);
    let mut zip = zip::ZipArchive::new(reader)
        .map_err(|e| DumpError::Zip(format!("打开 zip 失败: {}", e)))?;
    if zip.is_empty() {
        return Err(DumpError::Zip("zip 为空".into()));
    }
    let mut file = zip
        .by_index(0)
        .map_err(|e| DumpError::Zip(format!("读取 zip 条目失败: {}", e)))?;
    let mut out = Vec::with_capacity(file.size() as usize);
    use std::io::Read;
    file.read_to_end(&mut out)
        .map_err(|e| DumpError::Zip(format!("解压读取失败: {}", e)))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_daily_url() {
        let u = daily_url(Market::UsdtPerp, "BTCUSDT", "2024-01-01");
        assert_eq!(
            u,
            "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-01.zip"
        );
        let s = daily_url(Market::Spot, "BTCUSDT", "2024-01-01");
        assert!(s.contains("/spot/daily/aggTrades/"));
    }

    #[test]
    fn unzip_roundtrip_with_local_zip() {
        // 用一个内存中构造的 zip 验证解压
        use std::io::Write;
        let mut buf = Cursor::new(vec![]);
        {
            let mut zw = zip::ZipWriter::new(&mut buf);
            let opts: zip::write::FileOptions<()> = zip::write::FileOptions::default();
            zw.start_file("a.csv", opts).unwrap();
            zw.write_all(b"hello,world\n").unwrap();
            zw.finish().unwrap();
        }
        let out = unzip_first(&buf.into_inner()).unwrap();
        assert_eq!(out, b"hello,world\n");
    }
}
