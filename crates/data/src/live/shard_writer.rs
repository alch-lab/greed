//! 采集落盘：内存缓冲 → 按天分片 Parquet。
//!
//! - 三类事件（trades / book / oi）各自缓冲，达到阈值或 flush tick 时写盘。
//! - **跨天切分**：flush 时按事件 UTC 日期分组，分别写 `{date}.part-{ms}.parquet`；
//!   读取端（`read_range` / `read_book_shard`）扫描目录全部 `*.parquet`，天然兼容。
//! - trades 用 Snappy（与历史导入一致）；book/oi 用 zstd level 3。
//! - 优雅退出时 `flush_all`，保证缓冲区落盘。

use std::collections::BTreeMap;
use std::path::PathBuf;
use tcore::types::{Exchange, Symbol};
use tracing::{debug, info};

use super::binlog::{append_book_log, append_oi_log, append_trade_log, day_path};
use super::CollectError;
use crate::lake::Lake;
use crate::normalize::NormalizedTrade;
use crate::schema::{BookRow, OiRow};

/// 采集事件（channel 消息）。
#[derive(Debug)]
pub enum LiveEvent {
    Trade(NormalizedTrade),
    Book(BookRow),
    Oi(OiRow),
}

/// 当前 UTC 毫秒。
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// ts_ms → UTC 日期字符串（yyyy-mm-dd）。
pub fn utc_date_of(ts_ms: i64) -> String {
    let dt = chrono::DateTime::from_timestamp_millis(ts_ms).unwrap_or_default();
    dt.format("%Y-%m-%d").to_string()
}

/// 落盘写入器。
pub struct ShardWriter {
    lake: Lake,
    trades_buf: Vec<NormalizedTrade>,
    book_buf: Vec<BookRow>,
    oi_buf: Vec<OiRow>,
    trades_threshold: usize,
    book_threshold: usize,
    /// 累计写入行数（统计/日志）
    pub written_trades: usize,
    pub written_book: usize,
    pub written_oi: usize,
    /// 上次 flush 时
    last_flush_ms: i64,
    /// 缓存最长停留时间
    max_buffer_ms: i64,
}

impl ShardWriter {
    pub fn new(
        lake_dir: &str,
        trades_threshold: usize,
        book_threshold: usize,
        max_buffer_ms: i64,
    ) -> Self {
        Self {
            lake: Lake::new(lake_dir),
            trades_buf: Vec::new(),
            book_buf: Vec::new(),
            oi_buf: Vec::new(),
            trades_threshold,
            book_threshold,
            written_trades: 0,
            written_book: 0,
            written_oi: 0,
            last_flush_ms: now_ms(),
            max_buffer_ms,
        }
    }

    pub fn push(&mut self, ev: LiveEvent) {
        match ev {
            LiveEvent::Trade(t) => self.trades_buf.push(t),
            LiveEvent::Book(b) => self.book_buf.push(b),
            LiveEvent::Oi(o) => self.oi_buf.push(o),
        }
    }

    /// flush 判定（tick 周期调用）：
    /// 1. trades/book 达到行数阈值，或
    /// 2. 距上次 flush 超过 `max_buffer_ms`（默认 5 分钟）。
    pub fn maybe_flush(&mut self) -> Result<(), CollectError> {
        let threshold_hit = self.trades_buf.len() >= self.trades_threshold
            || self.book_buf.len() >= self.book_threshold;
        let stale = now_ms() - self.last_flush_ms >= self.max_buffer_ms;
        if threshold_hit || stale {
            self.flush_all()?;
        }
        Ok(())
    }

    /// 全量 flush（优雅退出 / 跨天安全点）。
    pub fn flush_all(&mut self) -> Result<(), CollectError> {
        self.last_flush_ms = now_ms();
        self.flush_trades()?;
        self.flush_book()?;
        self.flush_oi()?;
        Ok(())
    }

    /// 按事件 UTC 日期分组（保持组内顺序）。
    fn group_by_date<T, F>(rows: &[T], key: F) -> BTreeMap<String, Vec<T>>
    where
        T: Clone,
        F: Fn(&T) -> i64,
    {
        let mut map: BTreeMap<String, Vec<T>> = BTreeMap::new();
        for r in rows {
            map.entry(utc_date_of(key(r))).or_default().push(r.clone());
        }
        map
    }

    /// 当日 binlog 路径：`{dir}/{date}.binlog`
    fn part_path(dir: PathBuf, date: &str) -> PathBuf {
        day_path(dir, date)
    }

    fn flush_trades(&mut self) -> Result<(), CollectError> {
        if self.trades_buf.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.trades_buf);
        let groups = Self::group_by_date(&rows, |r| r.trade.ts.as_millis());
        let mut total = 0;
        for (date, group) in groups {
            // 同一分片内可能混有合约/现货：再按 (exchange, symbol) 细分
            let mut by_stream: BTreeMap<(String, String), Vec<NormalizedTrade>> = BTreeMap::new();
            for r in group {
                by_stream
                    .entry((
                        r.trade.exchange.as_str().to_string(),
                        r.trade.symbol.as_str().to_string(),
                    ))
                    .or_default()
                    .push(r);
            }
            for ((ex, sym), recs) in by_stream {
                let exchange = Exchange::parse(&ex)
                    .ok_or_else(|| CollectError::Data(format!("未知交易所 {ex}")))?;
                let dir = self.lake.dir(exchange, &Symbol::new(&sym));
                let path = Self::part_path(dir, &date);
                let n = append_trade_log(&path, &recs)?;
                total += n;
                debug!(%date, %ex, rows = n, path = %path.display(), "trades flush");
            }
        }
        self.written_trades += total;
        info!(rows = total, "trades 分片落盘");
        Ok(())
    }

    fn flush_book(&mut self) -> Result<(), CollectError> {
        if self.book_buf.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.book_buf);
        let groups = Self::group_by_date(&rows, |r| r.ts_ms);
        let mut total = 0;
        for (date, group) in groups {
            let first = &group[0];
            let exchange = Exchange::parse(&first.exchange)
                .ok_or_else(|| CollectError::Data(format!("未知交易所 {}", first.exchange)))?;
            let dir = self.lake.book_dir(exchange, &Symbol::new(&first.symbol));
            let path = Self::part_path(dir, &date);
            let n = append_book_log(&path, &group)?;
            total += n;
            debug!(%date, rows = n, "book flush");
        }
        self.written_book += total;
        info!(rows = total, "book 分片落盘");
        Ok(())
    }

    fn flush_oi(&mut self) -> Result<(), CollectError> {
        if self.oi_buf.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.oi_buf);
        let groups = Self::group_by_date(&rows, |r| r.ts_ms);
        let mut total = 0;
        for (date, group) in groups {
            let first = &group[0];
            let exchange = Exchange::parse(&first.exchange)
                .ok_or_else(|| CollectError::Data(format!("未知交易所 {}", first.exchange)))?;
            let dir = self.lake.oi_dir(exchange, &Symbol::new(&first.symbol));
            let path = Self::part_path(dir, &date);
            let n = append_oi_log(&path, &group)?;
            total += n;
        }
        self.written_oi += total;
        info!(rows = total, "oi 分片落盘");
        Ok(())
    }

    /// 当前缓冲量（dry-run/统计用）。
    pub fn buffered(&self) -> (usize, usize, usize) {
        (
            self.trades_buf.len(),
            self.book_buf.len(),
            self.oi_buf.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Price, Qty, Timestamp};
    use tcore::Trade;

    fn trade_rec(ts_ms: i64, exchange: Exchange, id: i64) -> NormalizedTrade {
        NormalizedTrade {
            trade: Trade {
                ts: Timestamp::from_millis(ts_ms),
                exchange,
                symbol: Symbol::new("BTCUSDT"),
                price: Price::from_f64(67000.0),
                qty: Qty::from_f64(0.01),
                is_buyer_maker: false,
            },
            agg_trade_id: id,
        }
    }

    #[test]
    fn utc_date_of_ms() {
        assert_eq!(utc_date_of(1704067200000), "2024-01-01");
        assert_eq!(utc_date_of(1704153599999), "2024-01-01");
        assert_eq!(utc_date_of(1704153600000), "2024-01-02");
    }

    #[test]
    fn flush_splits_by_date_and_stream() {
        let dir = std::env::temp_dir().join(format!("greed-test-{}", now_ms()));
        let mut w = ShardWriter::new(dir.to_str().unwrap(), 1000, 1000, 300_000);
        // 跨两天 + 两个市场
        w.push(LiveEvent::Trade(trade_rec(
            1704067200000,
            Exchange::BinanceFutures,
            1,
        )));
        w.push(LiveEvent::Trade(trade_rec(
            1704153600000,
            Exchange::BinanceFutures,
            2,
        )));
        w.push(LiveEvent::Trade(trade_rec(
            1704067200000,
            Exchange::BinanceSpot,
            1,
        )));
        w.flush_all().unwrap();
        assert_eq!(w.written_trades, 3);
        let perp_dir =
            Lake::new(dir.to_str().unwrap()).dir(Exchange::BinanceFutures, &Symbol::new("BTCUSDT"));
        let spot_dir =
            Lake::new(dir.to_str().unwrap()).dir(Exchange::BinanceSpot, &Symbol::new("BTCUSDT"));
        let perp_files: Vec<_> = std::fs::read_dir(&perp_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        let spot_files: Vec<_> = std::fs::read_dir(&spot_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(perp_files.len(), 2); // 两天两个 part
        assert_eq!(spot_files.len(), 1);
        // 读回验证
        let all = crate::lake::read_range(
            &Lake::new(dir.to_str().unwrap()),
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT"),
            Timestamp::from_millis(0),
            Timestamp::from_millis(i64::MAX),
        )
        .unwrap();
        assert_eq!(all.len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn book_and_oi_roundtrip() {
        let dir = std::env::temp_dir().join(format!("greed-test-book-{}", now_ms()));
        let mut w = ShardWriter::new(dir.to_str().unwrap(), 1000, 1000, 300_000);
        w.push(LiveEvent::Book(BookRow {
            ts_ms: 1704067200000,
            exchange: "binance_futures".into(),
            symbol: "BTCUSDT".into(),
            bids_json: serde_json::to_vec(&vec![(670001000000000i64, 100000000i64)]).unwrap(),
            asks_json: serde_json::to_vec(&vec![(670010000000000i64, 200000000i64)]).unwrap(),
            last_update_id: 42,
        }));
        w.push(LiveEvent::Oi(OiRow {
            ts_ms: 1704067200000,
            exchange: "binance_futures".into(),
            symbol: "BTCUSDT".into(),
            oi_raw: 1_064_866_400_000_000,
        }));
        w.flush_all().unwrap();
        assert_eq!(w.written_book, 1);
        assert_eq!(w.written_oi, 1);
        // 读回 book
        let book_dir = Lake::new(dir.to_str().unwrap())
            .book_dir(Exchange::BinanceFutures, &Symbol::new("BTCUSDT"));
        let part = std::fs::read_dir(&book_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .next()
            .unwrap()
            .path();
        let rows = crate::lake::read_book_shard(&part).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_update_id, 42);
        let bids: Vec<(i64, i64)> = serde_json::from_slice(&rows[0].bids_json).unwrap();
        assert_eq!(bids[0].0, 670001000000000);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn threshold_flush_keeps_buffer_below() {
        let dir = std::env::temp_dir().join(format!("greed-test-th-{}", now_ms()));
        let mut w = ShardWriter::new(dir.to_str().unwrap(), 2, 1000, 300_000);
        for i in 0..3 {
            w.push(LiveEvent::Trade(trade_rec(
                1704067200000 + i,
                Exchange::BinanceFutures,
                i,
            )));
            w.maybe_flush().unwrap();
        }
        // 前 2 条触发一次 flush；第 3 条留在缓冲
        assert_eq!(w.written_trades, 2);
        assert_eq!(w.buffered().0, 1);
        w.flush_all().unwrap();
        assert_eq!(w.written_trades, 3);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stale_buffer_flushes_on_time() {
        // v1.2：未达阈值但缓冲超时也强制落盘（防小时级数据滞留）
        let dir = std::env::temp_dir().join(format!("trader-test-stale-{}", now_ms()));
        let mut w = ShardWriter::new(dir.to_str().unwrap(), 1_000_000, 1_000_000, 300_000);
        w.push(LiveEvent::Trade(trade_rec(
            1704067200000,
            Exchange::BinanceFutures,
            1,
        )));
        w.maybe_flush().unwrap();
        assert_eq!(w.written_trades, 0); // 未达阈值、未超时 → 不落盘
                                         // 模拟缓冲已停留 301 秒
        w.last_flush_ms -= 301_000;
        w.maybe_flush().unwrap();
        assert_eq!(w.written_trades, 1); // 超时强制落盘
        std::fs::remove_dir_all(&dir).ok();
    }
}
