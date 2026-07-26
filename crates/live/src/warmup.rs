//! 启动预热：用历史 K 线重建信号状态，免去 20 小时实时攒线。
//!
//! 策略信号（如 DualTfMeanReversion）靠逐笔成交自聚合 5m/1h K 线并维护
//! EMA/ATR/RSI。进程重启后若只靠实时 WS，1h EMA20 需要 20 小时才能出信号，
//! 调参实验完全不可行。
//!
//! 本模块在引擎启动时从币安公共接口 `GET /fapi/v1/klines`（免签名）拉取
//! 最近 100 根 1h + 300 根 5m K 线，每根合成「开→高→低→收」四笔虚拟成交，
//! 按时间序喂给 [`LiveEngine::warmup_trade`]（只喂信号，不触发交易），
//! 几秒完成预热后立即具备出信号能力。
//!
//! 注意：预热只重建信号内部状态；EMA 用 100 根收敛已足够（20 周期），
//! ATR/RSI（Wilder，默认关闭的守卫用）用 300 根 5m 亦有充分历史。

use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};
use tcore::Trade;
use tracing::info;

use crate::engine::LiveEngine;
use crate::rest::RestError;

/// 一根 K 线（只取需要的字段；币安返回数组格式）。
#[derive(Debug, Clone, Copy)]
pub struct Kline {
    pub open_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// 解析 klines 响应：[[openTime, open, high, low, close, ...], ...]
pub fn parse_klines(text: &str) -> Result<Vec<Kline>, RestError> {
    let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(text)?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let parse_f = |i: usize| -> Option<f64> { r.get(i)?.as_str()?.parse().ok() };
        let (o, h, l, c) = match (parse_f(1), parse_f(2), parse_f(3), parse_f(4)) {
            (Some(o), Some(h), Some(l), Some(c)) => (o, h, l, c),
            _ => return Err(RestError::Data(format!("kline 字段非法: {:?}", r))),
        };
        let ts = r
            .first()
            .and_then(|v| v.as_i64())
            .ok_or_else(|| RestError::Data("kline openTime 缺失".into()))?;
        out.push(Kline {
            open_time_ms: ts,
            open: o,
            high: h,
            low: l,
            close: c,
        });
    }
    Ok(out)
}

/// 拉取 K 线（公共接口）。
pub async fn fetch_klines(
    http: &reqwest::Client,
    rest_base: &str,
    symbol: &str,
    interval: &str,
    limit: u32,
) -> Result<Vec<Kline>, RestError> {
    let url = format!(
        "{}/fapi/v1/klines?symbol={}&interval={}&limit={}",
        rest_base.trim_end_matches('/'),
        symbol,
        interval,
        limit
    );
    let text = http
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?
        .text()
        .await?;
    parse_klines(&text)
}

/// 一根 K 线 → 四笔虚拟成交（开→高→低→收），时间戳放在 bar 内
/// （open+0/1/2/3 秒），保证落入正确的 bar 桶且顺序确定。
/// qty=0：合成逐笔不携带成交量，避免污染信号插件的量比基线
/// （策略层会跳过 0 量 bar，量比在实时段积累后生效）。
fn kline_to_trades(k: &Kline, exchange: Exchange, symbol: &Symbol) -> Vec<Trade> {
    let mk = |offset_ms: i64, price: f64| Trade {
        ts: Timestamp::from_millis(k.open_time_ms + offset_ms),
        exchange,
        symbol: symbol.clone(),
        price: Price::from_f64(price),
        qty: Qty::from_f64(0.0),
        is_buyer_maker: false,
    };
    vec![
        mk(0, k.open),
        mk(1000, k.high),
        mk(2000, k.low),
        mk(3000, k.close),
    ]
}

/// 执行预热：拉 1h×100 + 5m×300，合并排序后喂引擎。返回喂入的虚拟成交笔数。
///
/// `rest_base` 应与行情源一致（testnet 模式用 testnet；dry 模式建议主网公共数据）。
pub async fn warmup_engine(
    engine: &mut LiveEngine,
    http: &reqwest::Client,
    rest_base: &str,
    symbol: &str,
) -> Result<usize, RestError> {
    let sym = Symbol::new(symbol);
    let exchange = Exchange::BinanceFutures;

    let slow = fetch_klines(http, rest_base, symbol, "1h", 100).await?;
    let fast = fetch_klines(http, rest_base, symbol, "5m", 300).await?;

    let mut trades: Vec<Trade> = Vec::with_capacity((slow.len() + fast.len()) * 4);
    for k in slow.iter().chain(fast.iter()) {
        trades.extend(kline_to_trades(k, exchange, &sym));
    }
    trades.sort_by_key(|t| t.ts);

    let n = trades.len();
    for t in &trades {
        engine.warmup_trade(t);
    }
    info!(
        slow_bars = slow.len(),
        fast_bars = fast.len(),
        synthetic_trades = n,
        "信号预热完成（历史 K 线）"
    );
    Ok(n)
}

/// dry 模式预热数据源：主网公共 klines（与主网 WS 行情一致，数据稠密）。
pub const MAINNET_FAPI: &str = "https://fapi.binance.com";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_klines() {
        let text = r#"[
            [1704067200000,"67000.00","67100.00","66900.00","67050.00","123.4",1704070799999,"8270000",5321,"60.0","4000000","0"],
            [1704070800000,"67050.00","67200.00","67000.00","67180.00","100.0",1704074399999,"6700000",4100,"50.0","3350000","0"]
        ]"#;
        let ks = parse_klines(text).unwrap();
        assert_eq!(ks.len(), 2);
        assert_eq!(ks[0].open_time_ms, 1704067200000);
        assert!((ks[0].high - 67100.0).abs() < 1e-9);
        assert!((ks[1].close - 67180.0).abs() < 1e-9);
    }

    #[test]
    fn kline_makes_four_ordered_trades_in_bar() {
        let k = Kline {
            open_time_ms: 3_600_000,
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 105.0,
        };
        let ts = kline_to_trades(&k, Exchange::BinanceFutures, &Symbol::new("BTCUSDT"));
        assert_eq!(ts.len(), 4);
        assert!((ts[0].price.to_f64() - 100.0).abs() < 1e-9);
        assert!((ts[3].price.to_f64() - 105.0).abs() < 1e-9);
        // 全部落在同一 1h bar 桶内
        for t in &ts {
            assert_eq!(t.ts.as_millis() / 3_600_000, 1);
        }
    }
}
