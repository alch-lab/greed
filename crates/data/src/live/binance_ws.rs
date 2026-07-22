//! Binance aggTrade 实时采集：WebSocket 订阅 + REST 断档回补。
//!
//! 消息格式（combined stream）：
//! ```json
//! {"stream":"btcusdt@aggTrade","data":{
//!   "e":"aggTrade","E":1704067200040,"s":"BTCUSDT","a":5933014,
//!   "p":"67000.10","q":"0.500","f":100,"l":105,"T":1704067200038,"m":true}}
//! ```
//! - `a` 聚合成交 id（单调递增，回补与去重的锚）
//! - `T` 成交时间（UTC 毫秒）；`m` = is_buyer_maker
//!
//! 端点：
//! - 合约：`wss://fstream.binance.com/market/stream?streams={sym}@aggTrade`
//! - 现货：`wss://stream.binance.com:9443/stream?streams={sym}@aggTrade`
//!
//! 可靠性：
//! - 断线指数退避重连（1s → 30s 封顶）；
//! - 重连后先 REST `aggTrades?fromId=last+1&limit=1000` 分页回补断档期，
//!   再恢复 WS 消费；WS 与回补的重叠段按 id 去重；
//! - 90s 无消息视为假死，主动重连。

use futures_util::StreamExt;
use serde::Deserialize;
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
use tcore::Trade;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use super::shard_writer::LiveEvent;
use super::CollectError;
use crate::normalize::NormalizedTrade;

/// 市场类型（决定 WS/REST 端点与交易所标记）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMarket {
    UsdtPerp,
    Spot,
}

impl BinanceMarket {
    pub fn exchange(self) -> Exchange {
        match self {
            BinanceMarket::UsdtPerp => Exchange::BinanceFutures,
            BinanceMarket::Spot => Exchange::BinanceSpot,
        }
    }
    fn ws_url(self, symbol_lower: &str) -> String {
        match self {
            BinanceMarket::UsdtPerp => format!(
                "wss://fstream.binance.com/market/stream?streams={}@aggTrade",
                symbol_lower
            ),
            BinanceMarket::Spot => format!(
                "wss://stream.binance.com:9443/stream?streams={}@aggTrade",
                symbol_lower
            ),
        }
    }
    fn rest_base(self) -> &'static str {
        match self {
            BinanceMarket::UsdtPerp => "https://fapi.binance.com",
            BinanceMarket::Spot => "https://api.binance.com",
        }
    }
}

// ============================================================================
// 消息解析（纯函数，可测）
// ============================================================================

#[derive(Debug, Deserialize)]
struct AggTradeData {
    /// 聚合成交 id
    #[serde(rename = "a")]
    agg_id: i64,
    /// 价格（字符串，保精度）
    #[serde(rename = "p")]
    price: String,
    /// 数量（币）
    #[serde(rename = "q")]
    qty: String,
    /// 成交时间（UTC 毫秒）
    #[serde(rename = "T")]
    trade_time: i64,
    /// 买方是否 maker
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Debug, Deserialize)]
struct CombinedMsg {
    data: AggTradeData,
}

/// 解析一条 aggTrade 载荷为归一化记录。
fn parse_aggtrade(
    d: &AggTradeData,
    exchange: Exchange,
    symbol: &Symbol,
) -> Result<NormalizedTrade, CollectError> {
    let price: f64 = d
        .price
        .parse()
        .map_err(|_| CollectError::Data(format!("价格非法: {:?}", d.price)))?;
    let qty: f64 = d
        .qty
        .parse()
        .map_err(|_| CollectError::Data(format!("数量非法: {:?}", d.qty)))?;
    Ok(NormalizedTrade {
        trade: Trade {
            ts: Timestamp::from_millis(d.trade_time),
            exchange,
            symbol: symbol.clone(),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker: d.is_buyer_maker,
        },
        agg_trade_id: d.agg_id,
    })
}

/// 解析 combined-stream WS 文本消息。非 aggTrade JSON 返回 None（容噪）。
fn parse_ws_text(
    text: &str,
    exchange: Exchange,
    symbol: &Symbol,
) -> Result<Option<NormalizedTrade>, CollectError> {
    let msg: CombinedMsg = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => return Ok(None), // 订阅确认等控制帧
    };
    Ok(Some(parse_aggtrade(&msg.data, exchange, symbol)?))
}

/// 解析 REST `aggTrades` 响应（数组，无外层包装）。
fn parse_rest_aggtrades(
    text: &str,
    exchange: Exchange,
    symbol: &Symbol,
) -> Result<Vec<NormalizedTrade>, CollectError> {
    let rows: Vec<AggTradeData> = serde_json::from_str(text)?;
    rows.iter()
        .map(|d| parse_aggtrade(d, exchange, symbol))
        .collect()
}

// ============================================================================
// 缺口追踪：去重 + 回补锚点
// ============================================================================

/// 追踪最近收到的 agg_trade_id，过滤 WS/回补重叠段的重复记录。
#[derive(Debug, Default)]
pub struct GapTracker {
    last_id: Option<i64>,
}

impl GapTracker {
    /// 记录一条成交；重复（id ≤ last）返回 false。
    pub fn accept(&mut self, agg_id: i64) -> bool {
        match self.last_id {
            Some(last) if agg_id <= last => false,
            _ => {
                self.last_id = Some(agg_id);
                true
            }
        }
    }
    /// 回补起点（last + 1）；尚无任何记录时 None（冷启动不回补）。
    pub fn backfill_from(&self) -> Option<i64> {
        self.last_id.map(|id| id + 1)
    }
}

// ============================================================================
// 采集任务
// ============================================================================

/// 运行一个市场的 aggTrade 采集循环（永不返回，除非 channel 关闭）。
///
/// 断线自动重连并回补；所有错误只记日志，不中断 daemon。
pub async fn run_aggtrade_collector(
    market: BinanceMarket,
    symbol: Symbol,
    tx: mpsc::Sender<LiveEvent>,
) {
    let exchange = market.exchange();
    let symbol_lower = symbol.as_str().to_lowercase();
    let client = reqwest::Client::builder()
        .user_agent("greed-collect/0.1")
        .build()
        .expect("reqwest client");
    let mut tracker = GapTracker::default();
    let mut backoff_ms = 1_000u64;

    loop {
        // 1) 回补断档期（冷启动 last_id=None → 跳过，直接从 WS 实时开始）
        if let Some(from_id) = tracker.backfill_from() {
            match backfill(&client, market, &symbol, from_id, &mut tracker, &tx).await {
                Ok(n) if n > 0 => info!(?market, rows = n, "断档回补完成"),
                Ok(_) => {}
                Err(e) => warn!(?market, error = %e, "回补失败（继续重连）"),
            }
        }

        // 2) 建立 WS
        let url = market.ws_url(&symbol_lower);
        info!(?market, %url, "连接 aggTrade WS");
        let ws = match connect_async(&url).await {
            Ok((w, _)) => w,
            Err(e) => {
                warn!(?market, error = %e, backoff_ms, "WS 连接失败，退避重试");
                sleep(backoff_ms).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
                continue;
            }
        };
        backoff_ms = 1_000;

        // 3) 消费循环（90s 无消息视为假死）
        let mut ws = ws;
        loop {
            let next = tokio::time::timeout(std::time::Duration::from_secs(90), ws.next()).await;
            let msg = match next {
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    warn!(?market, error = %e, "WS 读错误，重连");
                    break;
                }
                Ok(None) => {
                    warn!(?market, "WS 对端关闭，重连");
                    break;
                }
                Err(_) => {
                    warn!(?market, "WS 90s 无消息，主动重连");
                    break;
                }
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Ping(_) | Message::Pong(_) | Message::Binary(_) => continue,
                Message::Close(_) => break,
                _ => continue,
            };
            match parse_ws_text(text.as_str(), exchange, &symbol) {
                Ok(Some(rec)) => {
                    let id = rec.agg_trade_id;
                    if tracker.accept(id) && tx.send(LiveEvent::Trade(rec)).await.is_err() {
                        info!(?market, "下游已关闭，采集任务退出");
                        return;
                    }
                    debug!(?market, agg_id = id, "trade");
                }
                Ok(None) => {}
                Err(e) => warn!(?market, error = %e, "消息解析失败（跳过）"),
            }
        }
    }
}

async fn sleep(ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

/// REST 分页回补：`GET {base}/api/v3/aggTrades?symbol=&fromId=&limit=1000`（现货）
/// 或 `/fapi/v1/aggTrades`（合约）。返回回补行数。
async fn backfill(
    client: &reqwest::Client,
    market: BinanceMarket,
    symbol: &Symbol,
    from_id: i64,
    tracker: &mut GapTracker,
    tx: &mpsc::Sender<LiveEvent>,
) -> Result<usize, CollectError> {
    let path = match market {
        BinanceMarket::UsdtPerp => "/fapi/v1/aggTrades",
        BinanceMarket::Spot => "/api/v3/aggTrades",
    };
    let mut from = from_id;
    let mut total = 0usize;
    loop {
        let url = format!(
            "{}{}?symbol={}&fromId={}&limit=1000",
            market.rest_base(),
            path,
            symbol.as_str(),
            from
        );
        let text = client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let rows = parse_rest_aggtrades(&text, market.exchange(), symbol)?;
        let n = rows.len();
        if n == 0 {
            break;
        }
        for rec in rows {
            let id = rec.agg_trade_id;
            if tracker.accept(id) {
                total += 1;
                if tx.send(LiveEvent::Trade(rec)).await.is_err() {
                    return Ok(total);
                }
            }
            from = id + 1;
        }
        if n < 1000 {
            break; // 已追到最新
        }
        // 限频保护：合约 2400 权重/min，aggTrades 权重 20，保守 sleep
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }

    #[test]
    fn parses_combined_ws_message() {
        let text = r#"{"stream":"btcusdt@aggTrade","data":{
            "e":"aggTrade","E":1704067200040,"s":"BTCUSDT","a":5933014,
            "p":"67000.10","q":"0.500","f":100,"l":105,"T":1704067200038,"m":true}}"#;
        let rec = parse_ws_text(text, Exchange::BinanceFutures, &sym())
            .unwrap()
            .unwrap();
        assert_eq!(rec.agg_trade_id, 5933014);
        assert!((rec.trade.price.to_f64() - 67000.10).abs() < 1e-6);
        assert!((rec.trade.qty.to_f64() - 0.5).abs() < 1e-9);
        assert_eq!(rec.trade.ts.as_millis(), 1704067200038);
        assert!(rec.trade.is_buyer_maker);
        assert_eq!(rec.trade.exchange, Exchange::BinanceFutures);
    }

    #[test]
    fn ws_noise_returns_none() {
        // 订阅确认帧
        let text = r#"{"result":null,"id":1}"#;
        assert!(parse_ws_text(text, Exchange::BinanceFutures, &sym())
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_rest_array() {
        let text = r#"[
            {"a":100,"p":"67000.0","q":"0.1","f":1,"l":1,"T":1704067200000,"m":false},
            {"a":101,"p":"67001.0","q":"0.2","f":2,"l":2,"T":1704067200100,"m":true}
        ]"#;
        let rows = parse_rest_aggtrades(text, Exchange::BinanceFutures, &sym()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].agg_trade_id, 100);
        assert!(!rows[0].trade.is_buyer_maker);
        assert!(rows[1].trade.is_buyer_maker);
    }

    #[test]
    fn gap_tracker_dedup_and_backfill_anchor() {
        let mut t = GapTracker::default();
        assert_eq!(t.backfill_from(), None); // 冷启动
        assert!(t.accept(100));
        assert!(t.accept(101));
        assert!(!t.accept(100)); // 重复（WS 与回补重叠）
        assert!(!t.accept(99)); // 乱序旧记录
        assert!(t.accept(102));
        assert_eq!(t.backfill_from(), Some(103));
    }

    #[test]
    fn ws_urls() {
        assert!(BinanceMarket::UsdtPerp
            .ws_url("btcusdt")
            .contains("fstream.binance.com/market/stream?streams="));
        assert!(BinanceMarket::Spot
            .ws_url("btcusdt")
            .contains("stream.binance.com:9443"));
    }
}
