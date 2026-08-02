//! 非执行交易所的公共 TRDR 行情源。
//!
//! Bybit/OKX 只提供现货、永续成交和深度上下文；所有订单仍由 Binance broker 执行。

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tcore::{
    BookSnapshot, Event, Exchange, LiquidationTick, OiTick, Price, Qty, Side, Symbol, Timestamp,
    Trade,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

pub const BYBIT_SPOT_WS: &str = "wss://stream.bybit.com/v5/public/spot";
pub const BYBIT_LINEAR_WS: &str = "wss://stream.bybit.com/v5/public/linear";
pub const OKX_PUBLIC_WS: &str = "wss://ws.okx.com:8443/ws/v5/public";
pub const OKX_BUSINESS_WS: &str = "wss://ws.okx.com:8443/ws/v5/business";
pub const BINANCE_FORCE_WS: &str = "wss://fstream.binance.com/market";
/// OKX 规格接口短暂不可用时的保守回退值；正常启动会动态获取 `ctVal`。
pub const OKX_BTC_SWAP_CT_VAL: f64 = 0.01;

pub async fn fetch_okx_swap_ct_val(client: &reqwest::Client) -> Option<f64> {
    let text = client
        .get("https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=BTC-USDT-SWAP")
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value
        .get("data")?
        .as_array()?
        .first()?
        .get("ctVal")?
        .as_str()?
        .parse::<f64>()
        .ok()
        .filter(|v| *v > 0.0)
}

fn reconnect_delay(backoff: &mut u64) -> std::time::Duration {
    let out = std::time::Duration::from_secs(*backoff);
    *backoff = (*backoff * 2).min(30);
    out
}

fn parse_bybit_trades(text: &str, exchange: Exchange, symbol: &Symbol) -> Vec<Trade> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    v.get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|x| {
            let side = x.get("S")?.as_str()?;
            Some(Trade {
                ts: Timestamp::from_millis(x.get("T")?.as_i64()?),
                exchange,
                symbol: symbol.clone(),
                price: Price::from_f64(x.get("p")?.as_str()?.parse().ok()?),
                qty: Qty::from_f64(x.get("v")?.as_str()?.parse().ok()?),
                is_buyer_maker: side.eq_ignore_ascii_case("Sell"),
            })
        })
        .collect()
}

fn parse_okx_trades(
    text: &str,
    exchange: Exchange,
    symbol: &Symbol,
    qty_multiplier: f64,
) -> Vec<Trade> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    v.get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|x| {
            let side = x.get("side")?.as_str()?;
            Some(Trade {
                ts: Timestamp::from_millis(x.get("ts")?.as_str()?.parse().ok()?),
                exchange,
                symbol: symbol.clone(),
                price: Price::from_f64(x.get("px")?.as_str()?.parse().ok()?),
                qty: Qty::from_f64(x.get("sz")?.as_str()?.parse::<f64>().ok()? * qty_multiplier),
                is_buyer_maker: side.eq_ignore_ascii_case("sell"),
            })
        })
        .collect()
}

pub async fn run_bybit_trade_feed(
    ws_url: &str,
    symbol: &str,
    exchange: Exchange,
    tx: mpsc::Sender<Event>,
) {
    let topic = format!("publicTrade.{symbol}");
    let subscribe = json!({"op":"subscribe","args":[topic]}).to_string();
    let sym = Symbol::new(symbol);
    let mut backoff = 1;
    loop {
        info!(
            url = ws_url,
            exchange = exchange.as_str(),
            "连接 Bybit 公共成交"
        );
        match connect_async(ws_url).await {
            Ok((mut ws, _)) => {
                backoff = 1;
                if ws.send(Message::Text(subscribe.clone())).await.is_err() {
                    continue;
                }
                while let Some(message) = ws.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            for trade in parse_bybit_trades(&text, exchange, &sym) {
                                if tx.send(Event::Trade(trade)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, exchange = exchange.as_str(), "Bybit WS 中断");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, exchange = exchange.as_str(), "Bybit WS 连接失败"),
        }
        tokio::time::sleep(reconnect_delay(&mut backoff)).await;
    }
}

pub async fn run_okx_trade_feed(
    inst_id: &str,
    exchange: Exchange,
    qty_multiplier: f64,
    tx: mpsc::Sender<Event>,
) {
    let subscribe = json!({
        "op":"subscribe",
        "args":[{"channel":"trades-all","instId":inst_id}]
    })
    .to_string();
    let sym = Symbol::new("BTCUSDT");
    let mut backoff = 1;
    loop {
        info!(inst_id, exchange = exchange.as_str(), "连接 OKX 公共成交");
        // `trades-all` is published on OKX's business websocket, not the
        // public websocket used by books and tickers.
        match connect_async(OKX_BUSINESS_WS).await {
            Ok((mut ws, _)) => {
                backoff = 1;
                if ws.send(Message::Text(subscribe.clone())).await.is_err() {
                    continue;
                }
                while let Some(message) = ws.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            for trade in parse_okx_trades(&text, exchange, &sym, qty_multiplier) {
                                if tx.send(Event::Trade(trade)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, exchange = exchange.as_str(), "OKX WS 中断");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, exchange = exchange.as_str(), "OKX WS 连接失败"),
        }
        tokio::time::sleep(reconnect_delay(&mut backoff)).await;
    }
}

fn parse_binance_liquidation(text: &str, symbol: &Symbol) -> Option<LiquidationTick> {
    let v: Value = serde_json::from_str(text).ok()?;
    let o = v.get("o")?;
    let price = o
        .get("ap")
        .and_then(Value::as_str)
        .filter(|x| *x != "0")
        .or_else(|| o.get("p").and_then(Value::as_str))?
        .parse()
        .ok()?;
    let qty = o
        .get("z")
        .and_then(Value::as_str)
        .filter(|x| *x != "0")
        .or_else(|| o.get("q").and_then(Value::as_str))?
        .parse()
        .ok()?;
    Some(LiquidationTick {
        ts: Timestamp::from_millis(o.get("T")?.as_i64()?),
        exchange: Exchange::BinanceFutures,
        symbol: symbol.clone(),
        side: if o.get("S")?.as_str()? == "BUY" {
            Side::Buy
        } else {
            Side::Sell
        },
        price: Price::from_f64(price),
        qty: Qty::from_f64(qty),
    })
}

pub async fn run_binance_liquidation_feed(symbol: &str, tx: mpsc::Sender<Event>) {
    let url = format!(
        "{}/ws/{}@forceOrder",
        BINANCE_FORCE_WS,
        symbol.to_lowercase()
    );
    let sym = Symbol::new(symbol);
    let mut backoff = 1;
    loop {
        info!(url, "连接 Binance 强平流");
        match connect_async(&url).await {
            Ok((mut ws, _)) => {
                backoff = 1;
                while let Some(message) = ws.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            if let Some(tick) = parse_binance_liquidation(&text, &sym) {
                                if tx.send(Event::Liquidation(tick)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, "Binance 强平流中断");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "Binance 强平流连接失败"),
        }
        tokio::time::sleep(reconnect_delay(&mut backoff)).await;
    }
}

fn parse_bybit_liquidations(text: &str, symbol: &Symbol) -> Vec<LiquidationTick> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    v.get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|x| {
            // Bybit's S is the liquidated position side: Buy means a long
            // position was liquidated. Normalize to the forced order side.
            let forced_side = match x.get("S")?.as_str()? {
                "Buy" => Side::Sell,
                "Sell" => Side::Buy,
                _ => return None,
            };
            Some(LiquidationTick {
                ts: Timestamp::from_millis(x.get("T")?.as_i64()?),
                exchange: Exchange::BybitFutures,
                symbol: symbol.clone(),
                side: forced_side,
                price: Price::from_f64(x.get("p")?.as_str()?.parse().ok()?),
                qty: Qty::from_f64(x.get("v")?.as_str()?.parse().ok()?),
            })
        })
        .collect()
}

pub async fn run_bybit_liquidation_feed(symbol: &str, tx: mpsc::Sender<Event>) {
    let subscribe =
        json!({"op":"subscribe","args":[format!("allLiquidation.{symbol}")]}).to_string();
    let sym = Symbol::new(symbol);
    let mut backoff = 1;
    loop {
        info!(symbol, "连接 Bybit 全量强平流");
        match connect_async(BYBIT_LINEAR_WS).await {
            Ok((mut ws, _)) => {
                backoff = 1;
                if ws.send(Message::Text(subscribe.clone())).await.is_err() {
                    continue;
                }
                while let Some(message) = ws.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            for tick in parse_bybit_liquidations(&text, &sym) {
                                if tx.send(Event::Liquidation(tick)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, "Bybit 强平流中断");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "Bybit 强平流连接失败"),
        }
        tokio::time::sleep(reconnect_delay(&mut backoff)).await;
    }
}

fn parse_okx_liquidations(
    text: &str,
    symbol: &Symbol,
    contract_value: f64,
) -> Vec<LiquidationTick> {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return vec![];
    };
    v.get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|row| {
            row.get("details")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|x| {
            let forced_side = match x.get("posSide")?.as_str()? {
                "long" => Side::Sell,
                "short" => Side::Buy,
                _ => return None,
            };
            Some(LiquidationTick {
                ts: Timestamp::from_millis(x.get("ts")?.as_str()?.parse().ok()?),
                exchange: Exchange::OkxFutures,
                symbol: symbol.clone(),
                side: forced_side,
                price: Price::from_f64(x.get("bkPx")?.as_str()?.parse().ok()?),
                qty: Qty::from_f64(x.get("sz")?.as_str()?.parse::<f64>().ok()? * contract_value),
            })
        })
        .collect()
}

pub async fn run_okx_liquidation_feed(contract_value: f64, tx: mpsc::Sender<Event>) {
    let subscribe = json!({
        "op":"subscribe",
        "args":[{"channel":"liquidation-orders","instType":"SWAP","instFamily":"BTC-USDT"}]
    })
    .to_string();
    let sym = Symbol::new("BTCUSDT");
    let mut backoff = 1;
    loop {
        info!("连接 OKX 强平流");
        match connect_async(OKX_PUBLIC_WS).await {
            Ok((mut ws, _)) => {
                backoff = 1;
                if ws.send(Message::Text(subscribe.clone())).await.is_err() {
                    continue;
                }
                loop {
                    match tokio::time::timeout(std::time::Duration::from_secs(20), ws.next()).await
                    {
                        Ok(Some(Ok(Message::Text(text)))) if text.as_str() == "pong" => {}
                        Ok(Some(Ok(Message::Text(text)))) => {
                            for tick in parse_okx_liquidations(&text, &sym, contract_value) {
                                if tx.send(Event::Liquidation(tick)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            let _ = ws.send(Message::Pong(data)).await;
                        }
                        Ok(Some(Ok(_))) => {}
                        Ok(Some(Err(e))) => {
                            warn!(error = %e, "OKX 强平流中断");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {
                            if ws.send(Message::Text("ping".into())).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "OKX 强平流连接失败"),
        }
        tokio::time::sleep(reconnect_delay(&mut backoff)).await;
    }
}

fn parse_levels(v: &Value, key: &str, qty_multiplier: f64) -> Vec<(Price, Qty)> {
    v.get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let row = row.as_array()?;
            Some((
                Price::from_f64(row.first()?.as_str()?.parse().ok()?),
                Qty::from_f64(row.get(1)?.as_str()?.parse::<f64>().ok()? * qty_multiplier),
            ))
        })
        .collect()
}

fn parse_bybit_book(text: &str, exchange: Exchange, symbol: &Symbol) -> Option<BookSnapshot> {
    let v: Value = serde_json::from_str(text).ok()?;
    let result = v.get("result")?;
    let book = BookSnapshot {
        ts: Timestamp::from_millis(
            result
                .get("ts")
                .and_then(Value::as_i64)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
        ),
        exchange,
        symbol: symbol.clone(),
        bids: parse_levels(result, "b", 1.0),
        asks: parse_levels(result, "a", 1.0),
    };
    (!book.bids.is_empty() && !book.asks.is_empty()).then_some(book)
}

fn parse_okx_book(
    text: &str,
    exchange: Exchange,
    symbol: &Symbol,
    qty_multiplier: f64,
) -> Option<BookSnapshot> {
    let v: Value = serde_json::from_str(text).ok()?;
    let result = v.get("data")?.as_array()?.first()?;
    let book = BookSnapshot {
        ts: Timestamp::from_millis(result.get("ts")?.as_str()?.parse().ok()?),
        exchange,
        symbol: symbol.clone(),
        bids: parse_levels(result, "bids", qty_multiplier),
        asks: parse_levels(result, "asks", qty_multiplier),
    };
    (!book.bids.is_empty() && !book.asks.is_empty()).then_some(book)
}

fn parse_bybit_oi(text: &str, price: f64, symbol: &Symbol) -> Option<OiTick> {
    let v: Value = serde_json::from_str(text).ok()?;
    let x = v.get("result")?.get("list")?.as_array()?.first()?;
    Some(OiTick {
        ts: Timestamp::from_millis(x.get("timestamp")?.as_str()?.parse().ok()?),
        exchange: Exchange::BybitFutures,
        symbol: symbol.clone(),
        oi_usd: x.get("openInterest")?.as_str()?.parse::<f64>().ok()? * price,
    })
}

fn parse_okx_oi(text: &str, price: f64, symbol: &Symbol, contract_value: f64) -> Option<OiTick> {
    let v: Value = serde_json::from_str(text).ok()?;
    let x = v.get("data")?.as_array()?.first()?;
    let oi_btc = x
        .get("oiCcy")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| {
            x.get("oi")
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
                .map(|v| v * contract_value)
        })?;
    Some(OiTick {
        ts: Timestamp::from_millis(x.get("ts")?.as_str()?.parse().ok()?),
        exchange: Exchange::OkxFutures,
        symbol: symbol.clone(),
        oi_usd: oi_btc * price,
    })
}

/// 每 5 秒抓取 Bybit/OKX 全深度快照与永续 OI。
pub async fn run_external_context_feed(
    symbol: &str,
    okx_contract_value: f64,
    client: reqwest::Client,
    tx: mpsc::Sender<Event>,
) {
    let sym = Symbol::new(symbol);
    let urls = [
        format!("https://api.bybit.com/v5/market/orderbook?category=spot&symbol={symbol}&limit=1000"),
        format!("https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit=1000"),
        "https://www.okx.com/api/v5/market/books-full?instId=BTC-USDT&sz=5000".into(),
        "https://www.okx.com/api/v5/market/books-full?instId=BTC-USDT-SWAP&sz=5000".into(),
        format!("https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=5min&limit=1"),
        "https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP".into(),
    ];
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        let (bs, bf, os, of, boi, ooi) = tokio::join!(
            client.get(&urls[0]).send(),
            client.get(&urls[1]).send(),
            client.get(&urls[2]).send(),
            client.get(&urls[3]).send(),
            client.get(&urls[4]).send(),
            client.get(&urls[5]).send()
        );
        let mut bybit_mid = None;
        let mut okx_mid = None;
        for (resp, exchange, multiplier) in [
            (bs, Exchange::BybitSpot, 1.0),
            (bf, Exchange::BybitFutures, 1.0),
            (os, Exchange::OkxSpot, 1.0),
            (of, Exchange::OkxFutures, okx_contract_value),
        ] {
            let Ok(resp) = resp else { continue };
            let Ok(text) = resp.text().await else {
                continue;
            };
            let book = match exchange {
                Exchange::BybitSpot | Exchange::BybitFutures => {
                    parse_bybit_book(&text, exchange, &sym)
                }
                _ => parse_okx_book(&text, exchange, &sym, multiplier),
            };
            let Some(book) = book else { continue };
            let mid = book.mid_price().map(Price::to_f64);
            match exchange {
                Exchange::BybitFutures => bybit_mid = mid,
                Exchange::OkxFutures => okx_mid = mid,
                _ => {}
            }
            if tx.send(Event::Book(book)).await.is_err() {
                return;
            }
        }
        if let (Ok(resp), Some(mid)) = (boi, bybit_mid) {
            if let Ok(text) = resp.text().await {
                if let Some(oi) = parse_bybit_oi(&text, mid, &sym) {
                    if tx.send(Event::Oi(oi)).await.is_err() {
                        return;
                    }
                }
            }
        }
        if let (Ok(resp), Some(mid)) = (ooi, okx_mid) {
            if let Ok(text) = resp.text().await {
                if let Some(oi) = parse_okx_oi(&text, mid, &sym, okx_contract_value) {
                    if tx.send(Event::Oi(oi)).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bybit_and_okx_public_trades() {
        let sym = Symbol::new("BTCUSDT");
        let bybit = r#"{"data":[{"T":1,"S":"Sell","v":"0.2","p":"60000"}]}"#;
        let okx = r#"{"data":[{"ts":"2","side":"buy","sz":"3","px":"60010"}]}"#;
        let a = parse_bybit_trades(bybit, Exchange::BybitFutures, &sym);
        let b = parse_okx_trades(okx, Exchange::OkxFutures, &sym, 0.01);
        assert_eq!(a[0].taker_side(), Side::Sell);
        assert!((b[0].qty.to_f64() - 0.03).abs() < 1e-9);
    }

    #[test]
    fn parses_binance_force_order() {
        let text = r#"{"o":{"S":"SELL","q":"1","p":"60000","ap":"59990","z":"0.5","T":7}}"#;
        let x = parse_binance_liquidation(text, &Symbol::new("BTCUSDT")).unwrap();
        assert_eq!(x.side, Side::Sell);
        assert!((x.notional() - 29_995.0).abs() < 1e-6);
    }

    #[test]
    fn parses_bybit_and_okx_liquidations_to_forced_side() {
        let sym = Symbol::new("BTCUSDT");
        let bybit = r#"{"data":[{"T":7,"S":"Buy","v":"2","p":"60000"}]}"#;
        let okx =
            r#"{"data":[{"details":[{"ts":"8","posSide":"short","sz":"3","bkPx":"60010"}]}]}"#;
        let a = parse_bybit_liquidations(bybit, &sym);
        let b = parse_okx_liquidations(okx, &sym, 0.01);
        assert_eq!(a[0].side, Side::Sell);
        assert_eq!(b[0].side, Side::Buy);
        assert!((b[0].qty.to_f64() - 0.03).abs() < 1e-9);
    }
}
