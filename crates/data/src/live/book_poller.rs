//! Binance 合约订单簿快照轮询
//!
//! `GET /fapi/v1/depth?symbol={sym}&limit=1000` 周期抓取：
//! ```json
//! {"lastUpdateId":1027024,"bids":[["67000.10","1.234"],...],"asks":[["67001.00","0.5"],...]}
//! ```
//!
//! - 档位按 `±band_pct`（距中间价）过滤后，序列化为 JSON `[[price_raw,qty_raw],...]`
//!   定点对落盘（无损、可读、配合 zstd 体积可控）。
//! - v1 用 REST 轮询而非 WS diff depth：快照自洽无对齐问题，断线无 gap 风险，
//!   代价是 5s 粒度（色带回测足够）。后续需要 tick 级再升级 diff 订阅。
//! - 限频：limit=1000 权重 10，2400/min 限额，5s 一次（120 权重/min）余量充足。

use serde::Deserialize;
use tcore::types::{Exchange, Price, Qty, Symbol};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::shard_writer::{now_ms, LiveEvent};
use super::CollectError;
use crate::schema::BookRow;

#[derive(Debug, Deserialize)]
struct DepthResponse {
    #[serde(rename = "lastUpdateId")]
    last_update_id: i64,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

fn parse_level(s: &[String; 2]) -> Result<(Price, Qty), CollectError> {
    let p: f64 = s[0]
        .parse()
        .map_err(|_| CollectError::Data(format!("档位价格非法: {:?}", s[0])))?;
    let q: f64 = s[1]
        .parse()
        .map_err(|_| CollectError::Data(format!("档位数量非法: {:?}", s[1])))?;
    Ok((Price::from_f64(p), Qty::from_f64(q)))
}

/// 把整份 depth 响应解析并过滤为落盘行。
///
/// 过滤规则：mid = (best_bid + best_ask) / 2；
/// 保留 bid.price ≥ mid×(1−band%) 且 ask.price ≤ mid×(1+band%)。
pub fn depth_to_row(
    text: &str,
    ts_ms: i64,
    exchange: Exchange,
    symbol: &Symbol,
    band_pct: f64,
) -> Result<BookRow, CollectError> {
    let resp: DepthResponse = serde_json::from_str(text)?;
    let mut bids = Vec::with_capacity(resp.bids.len());
    for l in &resp.bids {
        bids.push(parse_level(l)?);
    }
    let mut asks = Vec::with_capacity(resp.asks.len());
    for l in &resp.asks {
        asks.push(parse_level(l)?);
    }
    if bids.is_empty() || asks.is_empty() {
        return Err(CollectError::Data("订单簿一侧为空".into()));
    }

    let mid_raw = (bids[0].0.raw() + asks[0].0.raw()) / 2;
    let band = band_pct / 100.0;
    let bid_floor = (mid_raw as f64 * (1.0 - band)) as i64;
    let ask_ceil = (mid_raw as f64 * (1.0 + band)) as i64;
    let bids: Vec<(i64, i64)> = bids
        .iter()
        .filter(|(p, _)| p.raw() >= bid_floor)
        .map(|(p, q)| (p.raw(), q.raw()))
        .collect();
    let asks: Vec<(i64, i64)> = asks
        .iter()
        .filter(|(p, _)| p.raw() <= ask_ceil)
        .map(|(p, q)| (p.raw(), q.raw()))
        .collect();

    Ok(BookRow {
        ts_ms,
        exchange: exchange.as_str().to_string(),
        symbol: symbol.as_str().to_string(),
        bids_json: serde_json::to_vec(&bids)?,
        asks_json: serde_json::to_vec(&asks)?,
        last_update_id: resp.last_update_id,
    })
}

/// 运行订单簿轮询循环（合约）。
pub async fn run_book_poller(
    symbol: Symbol,
    interval_ms: u64,
    band_pct: f64,
    limit: u32,
    tx: mpsc::Sender<LiveEvent>,
) {
    let client = reqwest::Client::builder()
        .user_agent("greed-collect/0.1")
        .build()
        .expect("reqwest client");
    let url = format!(
        "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={}",
        symbol.as_str(),
        limit
    );
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let resp = client.get(&url).send().await;
        let text = match resp {
            Ok(r) => match r.error_for_status() {
                Ok(r) => match r.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(error = %e, "depth 读取 body 失败");
                        continue;
                    }
                },
                Err(e) => {
                    warn!(error = %e, "depth HTTP 状态错误");
                    continue;
                }
            },
            Err(e) => {
                warn!(error = %e, "depth 请求失败");
                continue;
            }
        };
        match depth_to_row(&text, now_ms(), Exchange::BinanceFutures, &symbol, band_pct) {
            Ok(row) => {
                debug!(ts_ms = row.ts_ms, "book snapshot");
                if tx.send(LiveEvent::Book(row)).await.is_err() {
                    return;
                }
            }
            Err(e) => warn!(error = %e, "depth 解析失败（跳过）"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }

    #[test]
    fn parses_and_bands_depth() {
        // mid = 100.0；±6% → bid ≥ 94，ask ≤ 106
        let text = r#"{
            "lastUpdateId": 42,
            "bids": [["100.0","1.0"],["99.0","2.0"],["93.9","5.0"]],
            "asks": [["100.0","1.5"],["105.9","2.5"],["106.1","9.9"]]
        }"#;
        let row = depth_to_row(text, 1000, Exchange::BinanceFutures, &sym(), 6.0).unwrap();
        assert_eq!(row.last_update_id, 42);
        assert_eq!(row.exchange, "binance_futures");
        let bids: Vec<(i64, i64)> = serde_json::from_slice(&row.bids_json).unwrap();
        let asks: Vec<(i64, i64)> = serde_json::from_slice(&row.asks_json).unwrap();
        // 93.9（低于 94）被过滤；106.1（高于 106）被过滤
        assert_eq!(bids.len(), 2);
        assert_eq!(asks.len(), 2);
        assert_eq!(bids[0].0, Price::from_f64(100.0).raw());
        assert_eq!(asks[1].0, Price::from_f64(105.9).raw());
        assert_eq!(bids[1].1, Qty::from_f64(2.0).raw());
    }

    #[test]
    fn narrow_band_keeps_top_only() {
        let text = r#"{
            "lastUpdateId": 1,
            "bids": [["100.0","1.0"],["99.5","2.0"]],
            "asks": [["100.0","1.0"],["100.5","2.0"]]
        }"#;
        // mid=100，±0.4% → bid≥99.6，ask≤100.4
        let row = depth_to_row(text, 0, Exchange::BinanceFutures, &sym(), 0.4).unwrap();
        let bids: Vec<(i64, i64)> = serde_json::from_slice(&row.bids_json).unwrap();
        let asks: Vec<(i64, i64)> = serde_json::from_slice(&row.asks_json).unwrap();
        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);
    }

    #[test]
    fn empty_side_is_error() {
        let text = r#"{"lastUpdateId":1,"bids":[],"asks":[["100.0","1.0"]]}"#;
        assert!(depth_to_row(text, 0, Exchange::BinanceFutures, &sym(), 6.0).is_err());
    }
}
