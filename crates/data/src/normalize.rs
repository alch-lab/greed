//! Binance aggTrades CSV → `tcore::Trade` 归一化
//!
//! Binance 公共数据 dump 的 aggTrades CSV 列格式（含表头）：
//! ```text
//! agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
//! 1965151407,42313.9,0.046,4426785111,4426785111,1704067200038,true
//! ```
//!
//! 解析要点：
//! - `price`/`quantity` 用小数字符串解析后转**定点 i64**（`Price::from_f64`/`Qty::from_f64`）。
//! - `transact_time` 为 UTC 毫秒（注意：极早期部分数据是微秒，见 [`normalize_ts`]）。
//! - `is_buyer_maker` ∈ {"true","false"}。

use std::io::Read;
use tcore::event::Trade;
use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NormalizeError {
    #[error("CSV 解析失败: {0}")]
    Csv(#[from] csv::Error),
    #[error("字段缺失或格式错误 行{row}: {msg}")]
    BadRow { row: usize, msg: String },
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
}

/// 一条归一化后的记录（含 agg_trade_id，供落盘审计/去重）。
#[derive(Debug, Clone)]
pub struct NormalizedTrade {
    pub trade: Trade,
    pub agg_trade_id: i64,
}

/// Binance 早期 aggTrades 的时间戳曾是微秒（约 2019 年前），后改毫秒。
/// 用数量级判断：> 1e14 视为微秒，换算为毫秒；否则按毫秒处理。
fn normalize_ts(ts: i64) -> i64 {
    if ts > 100_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

/// 从读取器解析 aggTrades CSV 为记录向量。
///
/// `exchange`/`symbol` 由调用方指定（同一文件属于同一交易所同一交易对）。
pub fn parse_aggtrades_csv<R: Read>(
    reader: R,
    exchange: Exchange,
    symbol: &Symbol,
) -> Result<Vec<NormalizedTrade>, NormalizeError> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true) // 容忍末尾字段缺失/多余空白行
        .from_reader(reader);

    let mut out = Vec::new();
    for (idx, rec) in rdr.records().enumerate() {
        let rec = rec?;
        let row = idx + 2; // 表头占第 1 行，数据从第 2 行起
        let get = |i: usize, name: &str| -> Result<&str, NormalizeError> {
            rec.get(i).ok_or_else(|| NormalizeError::BadRow {
                row,
                msg: format!("缺列 {}", name),
            })
        };

        let agg_trade_id: i64 =
            get(0, "agg_trade_id")?
                .parse()
                .map_err(|_| NormalizeError::BadRow {
                    row,
                    msg: "agg_trade_id 非整数".into(),
                })?;
        let price: f64 = get(1, "price")?
            .parse()
            .map_err(|_| NormalizeError::BadRow {
                row,
                msg: "price 非数值".into(),
            })?;
        let qty: f64 = get(2, "quantity")?
            .parse()
            .map_err(|_| NormalizeError::BadRow {
                row,
                msg: "quantity 非数值".into(),
            })?;
        let ts_raw: i64 = get(5, "transact_time")?
            .parse()
            .map_err(|_| NormalizeError::BadRow {
                row,
                msg: "transact_time 非整数".into(),
            })?;
        let is_buyer_maker = match get(6, "is_buyer_maker")? {
            "true" | "True" | "1" => true,
            "false" | "False" | "0" => false,
            other => {
                return Err(NormalizeError::BadRow {
                    row,
                    msg: format!("is_buyer_maker 非法值: {}", other),
                })
            }
        };

        out.push(NormalizedTrade {
            trade: Trade {
                ts: Timestamp::from_millis(normalize_ts(ts_raw)),
                exchange,
                symbol: symbol.clone(),
                price: Price::from_f64(price),
                qty: Qty::from_f64(qty),
                is_buyer_maker,
            },
            agg_trade_id,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::Side;

    const CSV: &str =
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
1965151407,42313.9,0.046,4426785111,4426785111,1704067200038,true\n\
1965151409,42314.0,0.005,4426785114,4426785114,1704067203142,false\n";

    #[test]
    fn parses_real_format() {
        let recs = parse_aggtrades_csv(
            CSV.as_bytes(),
            Exchange::BinanceFutures,
            &Symbol::new("BTCUSDT"),
        )
        .unwrap();
        assert_eq!(recs.len(), 2);
        let t0 = &recs[0];
        assert_eq!(t0.agg_trade_id, 1965151407);
        assert!((t0.trade.price.to_f64() - 42313.9).abs() < 1e-6);
        assert!((t0.trade.qty.to_f64() - 0.046).abs() < 1e-6);
        assert_eq!(t0.trade.ts.as_millis(), 1704067200038);
        assert!(t0.trade.is_buyer_maker);
        assert_eq!(t0.trade.taker_side(), Side::Sell); // is_buyer_maker → 卖方主动
        assert!(!recs[1].trade.is_buyer_maker);
        assert_eq!(recs[1].trade.taker_side(), Side::Buy);
    }

    #[test]
    fn microsecond_ts_normalized() {
        let csv = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
1,100.0,1.0,1,1,1577836800000000,false\n"; // 微秒（2020-01-01）
        let recs = parse_aggtrades_csv(
            csv.as_bytes(),
            Exchange::BinanceSpot,
            &Symbol::new("BTCUSDT"),
        )
        .unwrap();
        assert_eq!(recs[0].trade.ts.as_millis(), 1577836800000);
    }

    #[test]
    fn rejects_bad_row() {
        let csv = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
1,not_a_number,1.0,1,1,1577836800000,false\n";
        let r = parse_aggtrades_csv(
            csv.as_bytes(),
            Exchange::BinanceSpot,
            &Symbol::new("BTCUSDT"),
        );
        assert!(r.is_err());
    }
}
