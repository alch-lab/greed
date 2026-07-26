//! 币安 USDⓈ-M 合约签名 REST 客户端（testnet / 主网共用，端点由配置决定）。
//!
//! - 签名：`HMAC-SHA256(secret, query_string)`，hex 后追加 `&signature=`，
//!   请求头带 `X-MBX-APIKEY`。
//! - 时间戳：启动时与 `/fapi/v1/time` 对时，之后本地时钟 + 偏移。
//! - 参数均为数字/静态 ASCII，不做 URL 编码。
//!
//! 安全约定：secret 只从环境变量进入本结构，不落盘、不打日志。

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum RestError {
    #[error("HTTP 错误: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON 解析错误: {0}")]
    Json(#[from] serde_json::Error),
    #[error("币安返回错误 {code}: {msg}")]
    Binance { code: i64, msg: String },
    #[error("数据格式错误: {0}")]
    Data(String),
}

/// 交易对精度约束（来自 exchangeInfo）。
#[derive(Debug, Clone, Copy)]
pub struct SymbolFilters {
    /// 价格最小变动（PRICE_FILTER tickSize）
    pub tick_size: f64,
    /// 数量最小步长（LOT_SIZE stepSize）
    pub step_size: f64,
    /// 最小名义价值（MIN_NOTIONAL/NOTIONAL）
    pub min_notional: f64,
}

/// 向下取整到步长的整数倍（币安要求 price/qty 对齐 tick/step）。
pub fn floor_to_step(x: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return x;
    }
    (x / step).floor() * step
}

/// 把 f64 格式化成币安接受的十进制字符串（按步长推小数位，去尾零）。
pub fn fmt_step(x: f64, step: f64) -> String {
    let decimals = if step >= 1.0 {
        0
    } else {
        (-step.log10()).ceil().max(0.0) as usize
    };
    let s = format!("{:.*}", decimals, x);
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

/// 一笔账户成交（GET /fapi/v1/userTrades）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserTrade {
    pub order_id: i64,
    #[serde(rename = "id")]
    pub trade_id: i64,
    pub side: String, // "BUY" / "SELL"
    pub price: String,
    pub qty: String,
    /// 成交额（USDT）
    pub quote_qty: String,
    /// 手续费（USDT，本系统只用 USDT 永续）
    pub commission: String,
    pub maker: bool,
    pub time: i64,
}

pub struct RestClient {
    http: reqwest::Client,
    base: String,
    api_key: String,
    api_secret: String,
    /// server_time - local_time（毫秒），签名时间戳用
    time_offset_ms: i64,
}

impl RestClient {
    pub fn new(http: reqwest::Client, base: &str, api_key: String, api_secret: String) -> Self {
        RestClient {
            http,
            base: base.trim_end_matches('/').to_string(),
            api_key,
            api_secret,
            time_offset_ms: 0,
        }
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn ts(&self) -> i64 {
        Self::now_ms() + self.time_offset_ms
    }

    /// HMAC-SHA256 签名（hex）。
    pub fn sign(&self, query: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC 接受任意长度密钥");
        mac.update(query.as_bytes());
        let out = mac.finalize().into_bytes();
        out.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// 无签名 GET 并解析 JSON。
    async fn get_json(&self, url: &str) -> Result<serde_json::Value, RestError> {
        let text = self.http.get(url).send().await?.text().await?;
        Ok(serde_json::from_str(&text)?)
    }

    /// 与服务器对时（漂移 >1s 时签名会被拒）。
    pub async fn sync_time(&mut self) -> Result<(), RestError> {
        let v = self.get_json(&format!("{}/fapi/v1/time", self.base)).await?;
        let server = v["serverTime"]
            .as_i64()
            .ok_or_else(|| RestError::Data("serverTime 缺失".into()))?;
        self.time_offset_ms = server - Self::now_ms();
        tracing::info!(offset_ms = self.time_offset_ms, "币安服务器对时完成");
        Ok(())
    }

    /// 签名请求统一入口。params 不含 timestamp/signature。
    async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<serde_json::Value, RestError> {
        let mut qs: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        qs.push("recvWindow=5000".into());
        qs.push(format!("timestamp={}", self.ts()));
        let query = qs.join("&");
        let sig = self.sign(&query);
        let url = format!("{}{}?{}&signature={}", self.base, path, query, sig);
        let resp = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        let v: serde_json::Value =
            serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text.clone()));
        if !status.is_success() {
            let code = v["code"].as_i64().unwrap_or(status.as_u16() as i64);
            let msg = v["msg"].as_str().unwrap_or(&text).to_string();
            return Err(RestError::Binance { code, msg });
        }
        Ok(v)
    }

    /// 交易对精度约束。
    pub async fn symbol_filters(&self, symbol: &str) -> Result<SymbolFilters, RestError> {
        let v = self
            .get_json(&format!(
                "{}/fapi/v1/exchangeInfo?symbol={}",
                self.base, symbol
            ))
            .await?;
        let sym = v["symbols"]
            .as_array()
            .and_then(|a| a.first())
            .ok_or_else(|| RestError::Data(format!("exchangeInfo 无 {}", symbol)))?;
        let mut f = SymbolFilters {
            tick_size: 0.1,
            step_size: 0.001,
            min_notional: 100.0,
        };
        for flt in sym["filters"].as_array().cloned().unwrap_or_default() {
            match flt["filterType"].as_str().unwrap_or("") {
                "PRICE_FILTER" => {
                    f.tick_size = flt["tickSize"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(f.tick_size)
                }
                "LOT_SIZE" => {
                    f.step_size = flt["stepSize"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(f.step_size)
                }
                "MIN_NOTIONAL" | "NOTIONAL" => {
                    f.min_notional = flt["notional"]
                        .as_str()
                        .or_else(|| flt["minNotional"].as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(f.min_notional)
                }
                _ => {}
            }
        }
        Ok(f)
    }

    /// 设置杠杆（启动时调用一次）。
    pub async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<(), RestError> {
        self.signed(
            reqwest::Method::POST,
            "/fapi/v1/leverage",
            &[
                ("symbol", symbol.to_string()),
                ("leverage", leverage.to_string()),
            ],
        )
        .await?;
        Ok(())
    }

    /// 设逐仓（已是对应模式时币安返回 -4059，视为成功）。
    pub async fn set_margin_isolated(&self, symbol: &str) -> Result<(), RestError> {
        match self
            .signed(
                reqwest::Method::POST,
                "/fapi/v1/marginType",
                &[
                    ("symbol", symbol.to_string()),
                    ("marginType", "ISOLATED".into()),
                ],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(RestError::Binance { code, .. }) if code == -4059 => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// USDT 钱包余额（GET /fapi/v2/balance，取 asset=USDT 的 balance）。
    pub async fn wallet_balance_usdt(&self) -> Result<f64, RestError> {
        let v = self.signed(reqwest::Method::GET, "/fapi/v2/balance", &[]).await?;
        v.as_array()
            .and_then(|a| {
                a.iter()
                    .find(|b| b["asset"].as_str() == Some("USDT"))
                    .and_then(|b| b["balance"].as_str())
                    .and_then(|s| s.parse().ok())
            })
            .ok_or_else(|| RestError::Data("balance 响应无 USDT".into()))
    }

    /// 当前持仓数量（带符号：多正空负；0 = 空仓）。GET /fapi/v2/positionRisk。
    pub async fn position_amt(&self, symbol: &str) -> Result<f64, RestError> {
        let v = self
            .signed(
                reqwest::Method::GET,
                "/fapi/v2/positionRisk",
                &[("symbol", symbol.to_string())],
            )
            .await?;
        v.as_array()
            .and_then(|a| a.first())
            .and_then(|p| p["positionAmt"].as_str())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Data("positionRisk 响应异常".into()))
    }

    /// 撤销本交易对全部挂单。
    pub async fn cancel_all_open_orders(&self, symbol: &str) -> Result<(), RestError> {
        self.signed(
            reqwest::Method::DELETE,
            "/fapi/v1/allOpenOrders",
            &[("symbol", symbol.to_string())],
        )
        .await?;
        Ok(())
    }

    /// 下单。返回 orderId。
    ///
    /// - `kind`："MARKET" / "LIMIT" / "STOP_MARKET"
    /// - LIMIT 需 `price`；STOP_MARKET 需 `stop_price`；止损平仓用 `reduce_only`。
    pub async fn place_order(
        &self,
        symbol: &str,
        side: &str, // "BUY" / "SELL"
        kind: &str,
        qty: f64,
        price: Option<f64>,
        stop_price: Option<f64>,
        reduce_only: bool,
        filters: &SymbolFilters,
    ) -> Result<i64, RestError> {
        let qty_s = fmt_step(floor_to_step(qty, filters.step_size), filters.step_size);
        let mut params: Vec<(&str, String)> = vec![
            ("symbol", symbol.to_string()),
            ("side", side.to_string()),
            ("type", kind.to_string()),
            ("quantity", qty_s),
        ];
        if kind == "LIMIT" {
            let p = price.ok_or_else(|| RestError::Data("LIMIT 缺 price".into()))?;
            params.push(("timeInForce", "GTC".into()));
            params.push((
                "price",
                fmt_step(floor_to_step(p, filters.tick_size), filters.tick_size),
            ));
        }
        if kind == "STOP_MARKET" {
            let sp = stop_price.ok_or_else(|| RestError::Data("STOP_MARKET 缺 stopPrice".into()))?;
            params.push((
                "stopPrice",
                fmt_step(floor_to_step(sp, filters.tick_size), filters.tick_size),
            ));
        }
        if reduce_only {
            params.push(("reduceOnly", "true".into()));
        }
        let v = self.signed(reqwest::Method::POST, "/fapi/v1/order", &params).await?;
        v["orderId"]
            .as_i64()
            .ok_or_else(|| RestError::Data(format!("order 响应无 orderId: {}", v)))
    }

    /// 账户成交流水（fill 检测数据源），from_id 之后（含）的成交，升序。
    pub async fn user_trades(
        &self,
        symbol: &str,
        from_id: i64,
    ) -> Result<Vec<UserTrade>, RestError> {
        let mut params = vec![
            ("symbol", symbol.to_string()),
            ("limit", "1000".to_string()),
        ];
        if from_id > 0 {
            params.push(("fromId", from_id.to_string()));
        }
        let v = self
            .signed(reqwest::Method::GET, "/fapi/v1/userTrades", &params)
            .await?;
        Ok(serde_json::from_value(v)?)
    }

    /// 标记价格 + 当期资金费率（GET /fapi/v1/premiumIndex）。
    pub async fn premium_index(&self, symbol: &str) -> Result<(f64, f64), RestError> {
        let v = self
            .get_json(&format!(
                "{}/fapi/v1/premiumIndex?symbol={}",
                self.base, symbol
            ))
            .await?;
        let mark = v["markPrice"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Data("markPrice 缺失".into()))?;
        let funding = v["lastFundingRate"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        Ok((mark, funding))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 币安官方文档签名示例（spot 文档，HMAC 算法与合约一致）。
    #[test]
    fn hmac_matches_binance_doc_vector() {
        let c = RestClient::new(
            reqwest::Client::new(),
            "https://testnet.binancefuture.com",
            "key".into(),
            "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j".into(),
        );
        let sig = c.sign("symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559");
        assert_eq!(
            sig,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn floor_and_fmt_step() {
        assert!((floor_to_step(0.0129, 0.001) - 0.012).abs() < 1e-12);
        assert!((floor_to_step(67000.05, 0.1) - 67000.0).abs() < 1e-9);
        assert_eq!(fmt_step(0.012, 0.001), "0.012");
        assert_eq!(fmt_step(67000.0, 0.1), "67000");
        assert_eq!(fmt_step(67000.1, 0.1), "67000.1");
    }
}
