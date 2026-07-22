//! Binance 合约 OI（持仓量）轮询
//!
//! `GET /fapi/v1/openInterest?symbol={sym}`：
//! ```json
//! {"openInterest":"10648.664","symbol":"BTCUSDT","time":1704067200000}
//! ```
//! OI 单位为**币**（BTC），存定点 i64（×1e8）。USD 名义额在读侧按价格换算。

use serde::Deserialize;
use tcore::types::{Exchange, Symbol};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::shard_writer::LiveEvent;
use super::CollectError;
use crate::schema::OiRow;

#[derive(Debug, Deserialize)]
struct OiResponse {
    #[serde(rename = "openInterest")]
    open_interest: String,
    time: i64,
}

/// 解析 OI 响应为落盘行。
pub fn parse_oi(text: &str, exchange: Exchange, symbol: &Symbol) -> Result<OiRow, CollectError> {
    let resp: OiResponse = serde_json::from_str(text)?;
    let oi: f64 = resp
        .open_interest
        .parse()
        .map_err(|_| CollectError::Data(format!("OI 非法: {:?}", resp.open_interest)))?;
    Ok(OiRow {
        ts_ms: resp.time,
        exchange: exchange.as_str().to_string(),
        symbol: symbol.as_str().to_string(),
        oi_raw: tcore::types::Qty::from_f64(oi).raw(),
    })
}

/// 运行 OI 轮询循环（合约）。
pub async fn run_oi_poller(symbol: Symbol, interval_ms: u64, tx: mpsc::Sender<LiveEvent>) {
    let client = reqwest::Client::builder()
        .user_agent("greed-collect/0.1")
        .build()
        .expect("reqwest client");
    let url = format!(
        "https://fapi.binance.com/fapi/v1/openInterest?symbol={}",
        symbol.as_str()
    );
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let text = match client.get(&url).send().await {
            Ok(r) => match r.error_for_status() {
                Ok(r) => match r.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(error = %e, "OI 读取 body 失败");
                        continue;
                    }
                },
                Err(e) => {
                    warn!(error = %e, "OI HTTP 状态错误");
                    continue;
                }
            },
            Err(e) => {
                warn!(error = %e, "OI 请求失败");
                continue;
            }
        };
        match parse_oi(&text, Exchange::BinanceFutures, &symbol) {
            Ok(row) => {
                debug!(ts_ms = row.ts_ms, "oi tick");
                if tx.send(LiveEvent::Oi(row)).await.is_err() {
                    return;
                }
            }
            Err(e) => warn!(error = %e, "OI 解析失败（跳过）"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_oi_response() {
        let text = r#"{"openInterest":"10648.664","symbol":"BTCUSDT","time":1704067200000}"#;
        let row = parse_oi(text, Exchange::BinanceFutures, &Symbol::new("BTCUSDT")).unwrap();
        assert_eq!(row.ts_ms, 1704067200000);
        assert_eq!(row.exchange, "binance_futures");
        assert!((row.oi_raw as f64 / 1e8 - 10648.664).abs() < 1e-6);
    }

    #[test]
    fn bad_oi_is_error() {
        let text = r#"{"openInterest":"abc","symbol":"BTCUSDT","time":0}"#;
        assert!(parse_oi(text, Exchange::BinanceFutures, &Symbol::new("BTCUSDT")).is_err());
    }
}
