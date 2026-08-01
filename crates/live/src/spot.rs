//! 币安现货 REST 客户端（carry 的现货腿；支持 Spot Testnet/主网）。

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::rest::{floor_to_step, fmt_step, RestError};

type HmacSha256 = Hmac<Sha256>;

pub struct SpotRestClient {
    http: reqwest::Client,
    base: String,
    api_key: String,
    api_secret: String,
    time_offset_ms: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct SpotExecution {
    pub qty: f64,
    pub quote_qty: f64,
    pub fee_usdt: f64,
}

impl SpotRestClient {
    pub fn new(http: reqwest::Client, base: &str, api_key: String, api_secret: String) -> Self {
        Self {
            http,
            base: base.trim_end_matches('/').into(),
            api_key,
            api_secret,
            time_offset_ms: 0,
        }
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn sign(&self, query: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC 接受任意长度密钥");
        mac.update(query.as_bytes());
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }

    async fn public_get(&self, path: &str) -> Result<serde_json::Value, RestError> {
        let text = self
            .http
            .get(format!("{}{}", self.base, path))
            .send()
            .await?
            .text()
            .await?;
        Ok(serde_json::from_str(&text)?)
    }

    async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<serde_json::Value, RestError> {
        let mut fields: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        fields.push("recvWindow=5000".into());
        fields.push(format!(
            "timestamp={}",
            Self::now_ms() + self.time_offset_ms
        ));
        let query = fields.join("&");
        let url = format!(
            "{}{}?{}&signature={}",
            self.base,
            path,
            query,
            self.sign(&query)
        );
        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        let value: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::String(text.clone()));
        if !status.is_success() {
            return Err(RestError::Binance {
                code: value["code"].as_i64().unwrap_or(status.as_u16() as i64),
                msg: value["msg"].as_str().unwrap_or(&text).into(),
            });
        }
        Ok(value)
    }

    pub async fn sync_time(&mut self) -> Result<(), RestError> {
        let value = self.public_get("/api/v3/time").await?;
        let server = value["serverTime"]
            .as_i64()
            .ok_or_else(|| RestError::Data("spot serverTime 缺失".into()))?;
        self.time_offset_ms = server - Self::now_ms();
        Ok(())
    }

    pub async fn free_balance(&self, asset: &str) -> Result<f64, RestError> {
        let value = self
            .signed(reqwest::Method::GET, "/api/v3/account", &[])
            .await?;
        value["balances"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["asset"] == asset))
            .and_then(|row| row["free"].as_str())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Data(format!("spot account 无 {} 余额", asset)))
    }

    /// 一次账户查询返回现货 USDT 与基础币总余额（free + locked）。
    pub async fn equity_balances(&self, base_asset: &str) -> Result<(f64, f64), RestError> {
        let value = self
            .signed(reqwest::Method::GET, "/api/v3/account", &[])
            .await?;
        let total = |asset: &str| -> Option<f64> {
            let row = value["balances"]
                .as_array()?
                .iter()
                .find(|row| row["asset"].as_str() == Some(asset))?;
            let free = row["free"].as_str()?.parse::<f64>().ok()?;
            let locked = row["locked"].as_str()?.parse::<f64>().ok()?;
            Some(free + locked)
        };
        let usdt =
            total("USDT").ok_or_else(|| RestError::Data("spot account 无 USDT 余额".into()))?;
        let base = total(base_asset)
            .ok_or_else(|| RestError::Data(format!("spot account 无 {} 余额", base_asset)))?;
        Ok((usdt, base))
    }

    pub async fn lot_step(&self, symbol: &str) -> Result<f64, RestError> {
        let value = self
            .public_get(&format!("/api/v3/exchangeInfo?symbol={}", symbol))
            .await?;
        value["symbols"][0]["filters"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["filterType"] == "LOT_SIZE"))
            .and_then(|row| row["stepSize"].as_str())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Data("spot LOT_SIZE 缺失".into()))
    }

    /// 市价调整现货 BTC 数量，返回实际累计成交额与成交量。
    pub async fn market_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        step: f64,
    ) -> Result<SpotExecution, RestError> {
        let qty = floor_to_step(qty, step);
        let value = self
            .signed(
                reqwest::Method::POST,
                "/api/v3/order",
                &[
                    ("symbol", symbol.into()),
                    ("side", side.into()),
                    ("type", "MARKET".into()),
                    ("quantity", fmt_step(qty, step)),
                    ("newOrderRespType", "FULL".into()),
                ],
            )
            .await?;
        let executed = value["executedQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let quote = value["cummulativeQuoteQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let avg = if executed > 0.0 {
            quote / executed
        } else {
            0.0
        };
        let fee_usdt = value["fills"]
            .as_array()
            .map(|fills| {
                fills
                    .iter()
                    .map(|fill| {
                        let fee = fill["commission"]
                            .as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        if fill["commissionAsset"] == "BTC" {
                            fee * avg
                        } else {
                            fee
                        }
                    })
                    .sum()
            })
            .unwrap_or(0.0);
        Ok(SpotExecution {
            qty: executed,
            quote_qty: quote,
            fee_usdt,
        })
    }
}
