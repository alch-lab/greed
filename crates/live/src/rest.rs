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
use std::collections::HashSet;
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
    /// 市价类订单数量步长（MARKET_LOT_SIZE）。
    pub market_step_size: f64,
    /// 价格允许的最大小数位（exchangeInfo.pricePrecision）。
    pub price_precision: usize,
    /// 数量允许的最大小数位（exchangeInfo.quantityPrecision）。
    pub quantity_precision: usize,
    /// 普通订单最大数量（LOT_SIZE maxQty）。
    pub max_qty: f64,
    /// 市价类订单最大数量（MARKET_LOT_SIZE maxQty）。
    pub market_max_qty: f64,
    /// 最小名义价值（MIN_NOTIONAL/NOTIONAL）
    pub min_notional: f64,
    /// 相对参考价允许的最高/最低限价倍数（PERCENT_PRICE）。
    pub multiplier_up: f64,
    pub multiplier_down: f64,
}

/// Binance `positionRisk` 返回的交易所侧实时仓位估值。
///
/// 前端与风险判断应优先使用这里的 markPrice/unRealizedProfit，不能用最近一根
/// 已收盘 K 线代替，否则在高波动山寨币上会产生显著滞后。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionRisk {
    pub position_amt: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_profit: f64,
}

/// 下单前从实际执行端点测得的可成交性。价格信号可以来自主网，但 paper/live
/// 是否允许下单必须以真正承接订单的盘口和成交流为准。
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct LiquiditySnapshot {
    pub measured_at_ms: i64,
    pub bid: f64,
    pub ask: f64,
    pub spread_bps: f64,
    pub bid_depth_usd: f64,
    pub ask_depth_usd: f64,
    pub entry_impact_bps: Option<f64>,
    pub exit_impact_bps: Option<f64>,
    pub recent_trade_count: usize,
    pub unique_trade_prices: usize,
    pub last_trade_age_ms: i64,
}

fn parse_level(value: &serde_json::Value) -> Option<(f64, f64)> {
    Some((
        value.get(0)?.as_str()?.parse().ok()?,
        value.get(1)?.as_str()?.parse().ok()?,
    ))
}

fn sweep_impact_bps(levels: &[serde_json::Value], notional: f64, reference: f64) -> Option<f64> {
    if notional <= 0.0 || reference <= 0.0 {
        return None;
    }
    let mut remaining = notional;
    let mut base_qty = 0.0;
    let mut spent = 0.0;
    for level in levels {
        let (price, qty) = parse_level(level)?;
        let available = price * qty;
        let used = remaining.min(available);
        base_qty += used / price;
        spent += used;
        remaining -= used;
        if remaining <= 1e-8 {
            let average = spent / base_qty;
            return Some((average / reference - 1.0).abs() * 10_000.0);
        }
    }
    None
}

/// 向下取整到步长的整数倍（币安要求 price/qty 对齐 tick/step）。
pub fn floor_to_step(x: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return x;
    }
    ((x / step) + 1e-10).floor() * step
}

/// 向上取整到步长的整数倍。保护性止损按方向选择更保守的 tick。
pub fn ceil_to_step(x: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return x;
    }
    ((x / step) - 1e-10).ceil() * step
}

/// 把 f64 格式化成币安接受的十进制字符串（按步长推小数位，去尾零）。
pub fn fmt_step(x: f64, step: f64) -> String {
    let decimals = step_decimal_places(step);
    let s = format!("{:.*}", decimals, x);
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

fn step_decimal_places(step: f64) -> usize {
    (0..=15)
        .find(|decimals| {
            let scaled = step * 10f64.powi(*decimals as i32);
            (scaled - scaled.round()).abs() < 1e-9
        })
        .unwrap_or(15)
}

/// 同时遵守步长与交易对最大小数位。部分山寨币的 MARKET_LOT_SIZE 与
/// LOT_SIZE 不同，单靠 stepSize 推导小数位会触发 Binance -1111。
pub fn fmt_step_with_precision(x: f64, step: f64, precision: usize) -> String {
    let precision_step = 10f64.powi(-(precision.min(15) as i32));
    let effective_step = step.max(precision_step);
    let aligned = floor_to_step(x, effective_step);
    let step_decimals = step_decimal_places(effective_step);
    let decimals = step_decimals.min(precision);
    let s = format!("{:.*}", decimals, aligned + effective_step * 1e-9);
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

fn quantity_string(kind: &str, qty: f64, filters: &SymbolFilters) -> String {
    let step = if kind == "MARKET" || kind == "STOP_MARKET" {
        filters.market_step_size
    } else {
        filters.step_size
    };
    fmt_step_with_precision(qty, step, filters.quantity_precision)
}

fn parse_symbol_filters(
    value: &serde_json::Value,
    symbol: &str,
) -> Result<SymbolFilters, RestError> {
    let sym = value["symbols"]
        .as_array()
        .and_then(|symbols| {
            symbols
                .iter()
                .find(|item| item["symbol"].as_str() == Some(symbol))
        })
        .ok_or_else(|| RestError::Data(format!("exchangeInfo 无 {symbol}")))?;
    let mut filters = SymbolFilters {
        tick_size: 0.1,
        step_size: 0.001,
        market_step_size: 0.001,
        price_precision: sym["pricePrecision"].as_u64().unwrap_or(8) as usize,
        quantity_precision: sym["quantityPrecision"].as_u64().unwrap_or(8) as usize,
        max_qty: f64::INFINITY,
        market_max_qty: f64::INFINITY,
        min_notional: 100.0,
        multiplier_up: f64::INFINITY,
        multiplier_down: 0.0,
    };
    let mut market_step_seen = false;
    for filter in sym["filters"].as_array().cloned().unwrap_or_default() {
        match filter["filterType"].as_str().unwrap_or("") {
            "PRICE_FILTER" => {
                filters.tick_size = filter["tickSize"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.tick_size)
            }
            "LOT_SIZE" => {
                filters.step_size = filter["stepSize"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.step_size);
                filters.max_qty = filter["maxQty"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.max_qty)
            }
            "MARKET_LOT_SIZE" => {
                market_step_seen = true;
                filters.market_step_size = filter["stepSize"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.step_size);
                filters.market_max_qty = filter["maxQty"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.market_max_qty)
            }
            "MIN_NOTIONAL" | "NOTIONAL" => {
                filters.min_notional = filter["notional"]
                    .as_str()
                    .or_else(|| filter["minNotional"].as_str())
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.min_notional)
            }
            "PERCENT_PRICE" => {
                filters.multiplier_up = filter["multiplierUp"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.multiplier_up);
                filters.multiplier_down = filter["multiplierDown"]
                    .as_str()
                    .and_then(|text| text.parse().ok())
                    .unwrap_or(filters.multiplier_down);
            }
            _ => {}
        }
    }
    if !market_step_seen || filters.market_step_size <= 0.0 {
        filters.market_step_size = filters.step_size;
        filters.market_max_qty = filters.max_qty;
    }
    if filters.market_max_qty <= 0.0 {
        filters.market_max_qty = filters.max_qty;
    }
    Ok(filters)
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

/// 合约账户收入流水；资金费必须以此处实际入账为准。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomeRecord {
    pub symbol: String,
    pub income_type: String,
    pub income: String,
    pub asset: String,
    pub time: i64,
    pub tran_id: i64,
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
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC 接受任意长度密钥");
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
        let v = self
            .get_json(&format!("{}/fapi/v1/time", self.base))
            .await?;
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
            .get_json(&format!("{}/fapi/v1/exchangeInfo", self.base))
            .await?;
        parse_symbol_filters(&v, symbol)
    }

    /// 当前端点实际支持交易的 USDT 永续集合。测试网与主网的上币集合不同，
    /// 扫描器必须先按执行端点过滤，不能等到 setLeverage 才发现无效 symbol。
    pub async fn active_usdt_perpetual_symbols(&self) -> Result<HashSet<String>, RestError> {
        let value = self
            .get_json(&format!("{}/fapi/v1/exchangeInfo", self.base))
            .await?;
        Ok(value["symbols"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| {
                item["status"] == "TRADING"
                    && item["contractType"] == "PERPETUAL"
                    && item["quoteAsset"] == "USDT"
            })
            .filter_map(|item| item["symbol"].as_str().map(str::to_owned))
            .collect())
    }

    /// 用执行端点的 100 档盘口和最近聚合成交评估目标名义仓位是否可双向成交。
    pub async fn liquidity_snapshot(
        &self,
        symbol: &str,
        entry_side: i32,
        notional: f64,
        depth_band_pct: f64,
        trade_window_ms: i64,
    ) -> Result<LiquiditySnapshot, RestError> {
        let depth_url = format!("{}/fapi/v1/depth?symbol={symbol}&limit=100", self.base);
        let trades_url = format!("{}/fapi/v1/aggTrades?symbol={symbol}&limit=1000", self.base);
        let (depth, trades) =
            tokio::try_join!(self.get_json(&depth_url), self.get_json(&trades_url))?;
        let bids = depth["bids"]
            .as_array()
            .ok_or_else(|| RestError::Data(format!("{symbol} depth 缺 bids")))?;
        let asks = depth["asks"]
            .as_array()
            .ok_or_else(|| RestError::Data(format!("{symbol} depth 缺 asks")))?;
        let (bid, _) = bids
            .first()
            .and_then(parse_level)
            .ok_or_else(|| RestError::Data(format!("{symbol} 买盘为空")))?;
        let (ask, _) = asks
            .first()
            .and_then(parse_level)
            .ok_or_else(|| RestError::Data(format!("{symbol} 卖盘为空")))?;
        let mid = (bid + ask) / 2.0;
        let bid_depth_usd: f64 = bids
            .iter()
            .filter_map(parse_level)
            .take_while(|(price, _)| *price >= mid * (1.0 - depth_band_pct))
            .map(|(price, qty)| price * qty)
            .sum();
        let ask_depth_usd: f64 = asks
            .iter()
            .filter_map(parse_level)
            .take_while(|(price, _)| *price <= mid * (1.0 + depth_band_pct))
            .map(|(price, qty)| price * qty)
            .sum();
        let (entry_levels, entry_reference, exit_levels, exit_reference) = if entry_side > 0 {
            (asks, ask, bids, bid)
        } else {
            (bids, bid, asks, ask)
        };
        let now_ms = self.ts();
        let cutoff = now_ms.saturating_sub(trade_window_ms);
        let recent: Vec<_> = trades
            .as_array()
            .into_iter()
            .flatten()
            .filter(|trade| trade["T"].as_i64().is_some_and(|ts| ts >= cutoff))
            .collect();
        let last_trade_ms = trades
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|trade| trade["T"].as_i64())
            .max()
            .unwrap_or(0);
        let unique_trade_prices = recent
            .iter()
            .filter_map(|trade| trade["p"].as_str())
            .collect::<HashSet<_>>()
            .len();
        Ok(LiquiditySnapshot {
            measured_at_ms: now_ms,
            bid,
            ask,
            spread_bps: (ask / bid - 1.0) * 10_000.0,
            bid_depth_usd,
            ask_depth_usd,
            entry_impact_bps: sweep_impact_bps(entry_levels, notional, entry_reference),
            exit_impact_bps: sweep_impact_bps(exit_levels, notional, exit_reference),
            recent_trade_count: recent.len(),
            unique_trade_prices,
            last_trade_age_ms: now_ms.saturating_sub(last_trade_ms),
        })
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

    /// 尝试请求目标杠杆；若交易所返回 -4028（该标的不支持），逐级回退到可用值。
    pub async fn set_leverage_up_to(
        &self,
        symbol: &str,
        requested: u32,
        minimum: u32,
    ) -> Result<u32, RestError> {
        let requested = requested.max(1);
        let minimum = minimum.clamp(1, requested);
        for leverage in (minimum..=requested).rev() {
            match self.set_leverage(symbol, leverage).await {
                Ok(()) => return Ok(leverage),
                Err(RestError::Binance { code: -4028, .. }) if leverage > minimum => continue,
                Err(error) => return Err(error),
            }
        }
        Err(RestError::Data(format!(
            "{symbol} 在 {minimum}..={requested} 范围内没有可用杠杆"
        )))
    }

    /// 当前执行端点的标记价。Paper 模式下这是 Demo Futures 的价格，可能与用于
    /// 生成信号的主网价格有偏差，限价单必须按这个市场的 PERCENT_PRICE 约束。
    pub async fn mark_price(&self, symbol: &str) -> Result<f64, RestError> {
        let value = self
            .get_json(&format!(
                "{}/fapi/v1/premiumIndex?symbol={symbol}",
                self.base
            ))
            .await?;
        value["markPrice"]
            .as_str()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| RestError::Data(format!("premiumIndex 无 {symbol} markPrice")))
    }

    /// 设逐仓（已是逐仓时币安返回 -4046 / -4059，视为成功）。
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
            Err(RestError::Binance { code, .. }) if code == -4046 || code == -4059 => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// USDT 钱包余额（GET /fapi/v2/balance，取 asset=USDT 的 balance）。
    pub async fn wallet_balance_usdt(&self) -> Result<f64, RestError> {
        let v = self
            .signed(reqwest::Method::GET, "/fapi/v2/balance", &[])
            .await?;
        v.as_array()
            .and_then(|a| {
                a.iter()
                    .find(|b| b["asset"].as_str() == Some("USDT"))
                    .and_then(|b| b["balance"].as_str())
                    .and_then(|s| s.parse().ok())
            })
            .ok_or_else(|| RestError::Data("balance 响应无 USDT".into()))
    }

    /// 合约账户真实权益：钱包余额 + 全仓未实现盈亏（GET /fapi/v2/account）。
    /// 返回 (wallet_balance, unrealized_pnl)。策略收益仍使用本地 sleeve 记账。
    pub async fn account_equity(&self) -> Result<(f64, f64), RestError> {
        let v = self
            .signed(reqwest::Method::GET, "/fapi/v2/account", &[])
            .await?;
        let wallet = v["totalWalletBalance"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Data("account 响应无 totalWalletBalance".into()))?;
        let unrealized = v["totalUnrealizedProfit"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        Ok((wallet, unrealized))
    }

    /// 当前持仓数量（带符号：多正空负；0 = 空仓）。GET /fapi/v2/positionRisk。
    pub async fn position_amt(&self, symbol: &str) -> Result<f64, RestError> {
        Ok(self.position_risk(symbol).await?.position_amt)
    }

    /// 当前单一交易对的实时仓位、标记价和币安计算的未实现盈亏。
    pub async fn position_risk(&self, symbol: &str) -> Result<PositionRisk, RestError> {
        let v = self
            .signed(
                reqwest::Method::GET,
                "/fapi/v2/positionRisk",
                &[("symbol", symbol.to_string())],
            )
            .await?;
        let row = v
            .as_array()
            .and_then(|a| a.first())
            .ok_or_else(|| RestError::Data("positionRisk 响应异常".into()))?;
        let number = |field: &str| {
            row[field]
                .as_str()
                .and_then(|value| value.parse::<f64>().ok())
                .ok_or_else(|| RestError::Data(format!("positionRisk 缺少 {field}")))
        };
        Ok(PositionRisk {
            position_amt: number("positionAmt")?,
            entry_price: number("entryPrice")?,
            mark_price: number("markPrice")?,
            unrealized_profit: number("unRealizedProfit")?,
        })
    }

    /// 账户全部非零 USDⓈ-M 持仓。多币种执行器启动时用它阻止接管外部/BTC 仓位。
    pub async fn open_position_amounts(&self) -> Result<Vec<(String, f64)>, RestError> {
        let v = self
            .signed(reqwest::Method::GET, "/fapi/v2/positionRisk", &[])
            .await?;
        let rows = v
            .as_array()
            .ok_or_else(|| RestError::Data("positionRisk 响应不是数组".into()))?;
        Ok(rows
            .iter()
            .filter_map(|position| {
                let symbol = position["symbol"].as_str()?.to_owned();
                let amount = position["positionAmt"].as_str()?.parse::<f64>().ok()?;
                (amount.abs() > 1e-12).then_some((symbol, amount))
            })
            .collect())
    }

    /// 撤销本交易对全部挂单。
    pub async fn cancel_all_open_orders(&self, symbol: &str) -> Result<(), RestError> {
        self.signed(
            reqwest::Method::DELETE,
            "/fapi/v1/allOpenOrders",
            &[("symbol", symbol.to_string())],
        )
        .await?;
        // 条件单已迁移到独立 Algo API，普通 allOpenOrders 不会清理这些止损单。
        self.signed(
            reqwest::Method::DELETE,
            "/fapi/v1/algoOpenOrders",
            &[("symbol", symbol.to_string())],
        )
        .await?;
        Ok(())
    }

    /// 撤销单个条件 Algo 订单。跟踪止损换挡时先挂新保护，再按 ID 撤旧保护，
    /// 避免 `cancel all -> place` 窗口内仓位没有交易所保护。
    pub async fn cancel_algo_order(&self, symbol: &str, algo_id: i64) -> Result<(), RestError> {
        self.signed(
            reqwest::Method::DELETE,
            "/fapi/v1/algoOrder",
            &[
                ("symbol", symbol.to_string()),
                ("algoId", algo_id.to_string()),
            ],
        )
        .await?;
        Ok(())
    }

    /// 下单。返回交易所订单标识（普通单为 orderId，条件单为 algoId）。
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
        let order_type = if kind == "LIMIT_IOC" { "LIMIT" } else { kind };
        let qty_s = quantity_string(order_type, qty, filters);
        let mut params: Vec<(&str, String)> = vec![
            ("symbol", symbol.to_string()),
            ("side", side.to_string()),
            ("type", order_type.to_string()),
            ("quantity", qty_s),
        ];
        if kind == "LIMIT" || kind == "LIMIT_IOC" {
            let p = price.ok_or_else(|| RestError::Data("LIMIT 缺 price".into()))?;
            params.push((
                "timeInForce",
                if kind == "LIMIT_IOC" { "IOC" } else { "GTC" }.into(),
            ));
            let aligned = if side == "SELL" {
                ceil_to_step(p, filters.tick_size)
            } else {
                floor_to_step(p, filters.tick_size)
            };
            params.push((
                "price",
                fmt_step_with_precision(aligned, filters.tick_size, filters.price_precision),
            ));
        }
        if kind == "STOP_MARKET" {
            let sp =
                stop_price.ok_or_else(|| RestError::Data("STOP_MARKET 缺 triggerPrice".into()))?;
            let precision_tick = filters
                .tick_size
                .max(10f64.powi(-(filters.price_precision.min(15) as i32)));
            let aligned = if side == "SELL" {
                ceil_to_step(sp, precision_tick)
            } else {
                floor_to_step(sp, precision_tick)
            };
            params.push((
                "triggerPrice",
                fmt_step_with_precision(aligned, precision_tick, filters.price_precision),
            ));
            params.push(("algoType", "CONDITIONAL".into()));
            params.push(("workingType", "CONTRACT_PRICE".into()));
        }
        if reduce_only {
            params.push(("reduceOnly", "true".into()));
        }
        let (path, id_field) = if kind == "STOP_MARKET" {
            ("/fapi/v1/algoOrder", "algoId")
        } else {
            ("/fapi/v1/order", "orderId")
        };
        let v = self.signed(reqwest::Method::POST, path, &params).await?;
        v[id_field]
            .as_i64()
            .ok_or_else(|| RestError::Data(format!("order 响应无 {id_field}: {v}")))
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

    /// 查询账户真实资金费入账（GET /fapi/v1/income）。
    pub async fn funding_income_since(
        &self,
        symbol: &str,
        start_time_ms: i64,
    ) -> Result<Vec<IncomeRecord>, RestError> {
        let v = self
            .signed(
                reqwest::Method::GET,
                "/fapi/v1/income",
                &[
                    ("symbol", symbol.to_string()),
                    ("incomeType", "FUNDING_FEE".into()),
                    ("startTime", start_time_ms.to_string()),
                    ("limit", "1000".into()),
                ],
            )
            .await?;
        Ok(serde_json::from_value(v)?)
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
        assert!((ceil_to_step(100.01, 0.1) - 100.1).abs() < 1e-9);
        assert!((floor_to_step(0.15, 0.05) - 0.15).abs() < 1e-12);
        assert_eq!(fmt_step(2.5, 2.5), "2.5");
        assert_eq!(fmt_step_with_precision(123.456, 0.001, 0), "123");
        assert_eq!(fmt_step_with_precision(0.159, 0.05, 2), "0.15");

        let filters = SymbolFilters {
            tick_size: 0.0001,
            step_size: 0.001,
            market_step_size: 1.0,
            price_precision: 4,
            quantity_precision: 3,
            max_qty: 1_000_000.0,
            market_max_qty: 100_000.0,
            min_notional: 5.0,
            multiplier_up: 1.05,
            multiplier_down: 0.95,
        };
        assert_eq!(quantity_string("MARKET", 123.456, &filters), "123");
        assert_eq!(quantity_string("STOP_MARKET", 123.456, &filters), "123");
        assert_eq!(quantity_string("LIMIT", 123.456, &filters), "123.456");
    }

    #[test]
    fn sweep_impact_requires_full_notional_and_measures_vwap() {
        let asks = serde_json::json!([["100.0", "5.0"], ["101.0", "10.0"]]);
        let levels = asks.as_array().unwrap();
        assert_eq!(sweep_impact_bps(levels, 2_000.0, 100.0), None);
        let impact = sweep_impact_bps(levels, 1_000.0, 100.0).unwrap();
        assert!(
            impact > 49.0 && impact < 51.0,
            "half the notional fills 1% higher"
        );
    }

    #[test]
    fn parses_market_lot_size_and_precision_independently() {
        let value = serde_json::json!({
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType":"PRICE_FILTER", "tickSize":"0.01000000"},
                        {"filterType":"LOT_SIZE", "stepSize":"0.00100000", "maxQty":"1000"},
                        {"filterType":"MARKET_LOT_SIZE", "stepSize":"0.00100000", "maxQty":"100"}
                    ]
                },
                {
                    "symbol": "SHELLUSDT",
                    "pricePrecision": 5,
                    "quantityPrecision": 0,
                    "filters": [
                        {"filterType":"PRICE_FILTER", "tickSize":"0.00001000"},
                        {"filterType":"LOT_SIZE", "stepSize":"0.10000000", "maxQty":"500000"},
                        {"filterType":"MARKET_LOT_SIZE", "stepSize":"1.00000000", "maxQty":"100000"},
                        {"filterType":"MIN_NOTIONAL", "notional":"5"},
                        {"filterType":"PERCENT_PRICE", "multiplierUp":"1.0500", "multiplierDown":"0.9500"}
                    ]
                }
            ]
        });
        let filters = parse_symbol_filters(&value, "SHELLUSDT").unwrap();
        assert_eq!(filters.tick_size, 0.00001);
        assert_eq!(filters.step_size, 0.1);
        assert_eq!(filters.market_step_size, 1.0);
        assert_eq!(filters.price_precision, 5);
        assert_eq!(filters.quantity_precision, 0);
        assert_eq!(filters.max_qty, 500_000.0);
        assert_eq!(filters.market_max_qty, 100_000.0);
        assert_eq!(filters.multiplier_up, 1.05);
        assert_eq!(filters.multiplier_down, 0.95);
        assert_eq!(quantity_string("MARKET", 7_695.267, &filters), "7695");
    }

    #[test]
    fn parses_funding_income() {
        let rows: Vec<IncomeRecord> = serde_json::from_str(
            r#"[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"1.25","asset":"USDT","time":1700000000000,"tranId":42,"tradeId":""}]"#,
        )
        .unwrap();
        assert_eq!(rows[0].tran_id, 42);
        assert_eq!(rows[0].income, "1.25");
    }
}
