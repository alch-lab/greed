//! 实时行情：aggTrade WebSocket 订阅（testnet / 主网通用，端点由配置决定）。
//!
//! 与采集器（data::live::binance_ws，主网专用、落盘导向）不同，本模块面向
//! 交易引擎：单一流、可配置端点、断线指数退避重连 + 90s 假死看门狗。
//! 短线波段策略逐笔回补断档期意义不大（策略基于 5m/1h K 线），重连即可，
//! 不回补——缺失的几笔成交不会改变 K 线形态。
//!
//! ⚠️ 主网端点：币安于 2026-04-23 永久退役旧版合约 WS 路径
//! `wss://fstream.binance.com/ws/...`（握手成功但永远零数据），
//! 市场数据须走 [`MAINNET_WS`]（`wss://fstream.binance.com/market`，
//! 单流模式 `{base}/ws/{symbol}@aggTrade`）。testnet 旧路径仍可用。
//!
//! 消息格式（单流裸订阅 `{base}/ws/{symbol}@aggTrade`）：
//! ```json
//! {"e":"aggTrade","E":...,"s":"BTCUSDT","a":5933014,
//!  "p":"67000.10","q":"0.500","f":100,"l":105,"T":...,"m":true}
//! ```

use futures_util::StreamExt;
use serde::Deserialize;
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
use tcore::{BookSnapshot, Event, OiTick, Trade};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

/// 主网公共行情 WS（无需密钥，只读）。
/// paper/dry 模式的行情源：testnet 成交流稀疏、价格陈旧且偏离真实市场，
/// 信号与回测（主网历史数据）保持同一价格环境；订单仍由 testnet 撮合。
/// 注意 `/market` 前缀：旧版 `/ws/...` 裸路径已于 2026-04-23 退役。
pub const MAINNET_WS: &str = "wss://fstream.binance.com/market";

#[derive(Debug, Deserialize)]
struct RawAggTrade {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

/// 轮询主网公共 depth 与 OI，为订单流信号补充位置上下文。
/// 失败只跳过当前快照，不影响逐笔成交主链路。
pub async fn run_context_feed(
    rest_base: &str,
    symbol: &str,
    client: reqwest::Client,
    tx: mpsc::Sender<Event>,
) {
    let sym = Symbol::new(symbol);
    let depth_url = format!(
        "{}/fapi/v1/depth?symbol={}&limit=100",
        rest_base.trim_end_matches('/'),
        symbol
    );
    let oi_url = format!(
        "{}/fapi/v1/openInterest?symbol={}",
        rest_base.trim_end_matches('/'),
        symbol
    );
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        let (depth, oi) = tokio::join!(client.get(&depth_url).send(), client.get(&oi_url).send());
        let now = Timestamp::from_millis(chrono::Utc::now().timestamp_millis());
        let mut mid = None;
        if let Ok(resp) = depth {
            if let Ok(text) = resp.text().await {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    let parse = |key: &str| -> Vec<(Price, Qty)> {
                        v.get(key)
                            .and_then(|x| x.as_array())
                            .into_iter()
                            .flatten()
                            .filter_map(|row| {
                                let a = row.as_array()?;
                                Some((
                                    Price::from_f64(a.first()?.as_str()?.parse().ok()?),
                                    Qty::from_f64(a.get(1)?.as_str()?.parse().ok()?),
                                ))
                            })
                            .collect()
                    };
                    let book = BookSnapshot {
                        ts: now,
                        exchange: Exchange::BinanceFutures,
                        symbol: sym.clone(),
                        bids: parse("bids"),
                        asks: parse("asks"),
                    };
                    mid = book.mid_price().map(Price::to_f64);
                    if !book.bids.is_empty()
                        && !book.asks.is_empty()
                        && tx.send(Event::Book(book)).await.is_err()
                    {
                        return;
                    }
                }
            }
        }
        if let (Ok(resp), Some(price)) = (oi, mid) {
            if let Ok(text) = resp.text().await {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(qty) = v
                        .get("openInterest")
                        .and_then(|x| x.as_str())
                        .and_then(|x| x.parse::<f64>().ok())
                    {
                        let event = Event::Oi(OiTick {
                            ts: now,
                            exchange: Exchange::BinanceFutures,
                            symbol: sym.clone(),
                            oi_usd: qty * price,
                        });
                        if tx.send(event).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// 解析一条 WS 文本为 Trade；非 aggTrade 帧（订阅确认/ping）返回 None。
fn parse_trade(text: &str, exchange: Exchange, symbol: &Symbol) -> Option<Trade> {
    let raw: RawAggTrade = serde_json::from_str(text).ok()?;
    Some(Trade {
        ts: Timestamp::from_millis(raw.trade_time),
        exchange,
        symbol: symbol.clone(),
        price: Price::from_f64(raw.price.parse().ok()?),
        qty: Qty::from_f64(raw.qty.parse().ok()?),
        is_buyer_maker: raw.is_buyer_maker,
    })
}

/// 持续订阅 aggTrade 并推入通道；永不返回（断线自动重连）。
pub async fn run_trade_feed(
    ws_base: &str,
    symbol: &str,
    exchange: Exchange,
    tx: mpsc::Sender<Trade>,
) {
    let url = format!(
        "{}/ws/{}@aggTrade",
        ws_base.trim_end_matches('/'),
        symbol.to_lowercase()
    );
    let sym = Symbol::new(symbol);
    let mut backoff_secs = 1u64;
    loop {
        info!(url = %url, "连接行情 WS");
        match connect_async(&url).await {
            Ok((mut ws, _)) => {
                backoff_secs = 1;
                loop {
                    // 90s 无消息视为假死
                    let next =
                        tokio::time::timeout(std::time::Duration::from_secs(90), ws.next()).await;
                    let msg = match next {
                        Ok(Some(Ok(Message::Text(t)))) => t,
                        Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
                        Ok(Some(Ok(_))) => continue,
                        Ok(Some(Err(e))) => {
                            warn!(error = %e, "行情 WS 错误，重连");
                            break;
                        }
                        Ok(None) => {
                            warn!("行情 WS 对端关闭，重连");
                            break;
                        }
                        Err(_) => {
                            warn!("行情 WS 90s 无消息（假死），重连");
                            break;
                        }
                    };
                    if let Some(trade) = parse_trade(&msg, exchange, &sym) {
                        if tx.send(trade).await.is_err() {
                            info!("行情通道已关闭，feed 退出");
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, backoff_secs, "行情 WS 连接失败");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aggtrade_frame() {
        let text = r#"{"e":"aggTrade","E":1704067200040,"s":"BTCUSDT","a":5933014,
            "p":"67000.10","q":"0.500","f":100,"l":105,"T":1704067200038,"m":true}"#;
        let t = parse_trade(text, Exchange::BinanceFutures, &Symbol::new("BTCUSDT")).unwrap();
        assert!((t.price.to_f64() - 67000.10).abs() < 1e-9);
        assert!((t.qty.to_f64() - 0.5).abs() < 1e-9);
        assert!(t.is_buyer_maker);
        assert_eq!(t.ts.as_millis(), 1704067200038);
    }

    #[test]
    fn ignores_control_frames() {
        assert!(parse_trade(
            r#"{"result":null,"id":1}"#,
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT")
        )
        .is_none());
    }
}
