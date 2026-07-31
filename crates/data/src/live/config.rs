//! 采集器配置：对应 `config/base.toml [collector]`。
//!
//! 全部用 serde 默认值，TOML 里缺省也能跑。

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CollectorConfig {
    /// 数据湖根目录
    pub lake_dir: String,
    /// 交易对（币安现货/合约同名，如 BTCUSDT）
    pub symbol: String,
    /// 合约 aggTrade（WS + REST 回补）
    pub enable_trades: bool,
    /// 现货 aggTrade（WS + REST 回补）
    pub enable_spot_trades: bool,
    /// 合约订单簿快照（REST 轮询）
    pub enable_book: bool,
    /// 合约 OI（REST 轮询）
    pub enable_oi: bool,
    /// 订单簿快照间隔（毫秒）
    pub book_snapshot_ms: u64,
    /// OI 轮询间隔（毫秒）
    pub oi_tick_ms: u64,
    /// 订单簿档位保留范围（距中间价 ±%）
    pub book_depth_band_pct: f64,
    /// REST depth 档数上限
    pub book_depth_limit: u32,
    /// trades 缓冲行数阈值（达到即 flush）
    pub trades_flush_rows: usize,
    /// book 缓冲行数阈值
    pub book_flush_rows: usize,
    /// 定时 flush 周期（秒；跨天切分也在此检查）
    pub flush_tick_secs: u64,
    /// 缓冲最长停留时间（秒；v1.2——即使未到阈值也强制落盘，防崩溃丢数小时数据）
    pub max_buffer_secs: u64,
    /// 出口代理（如 "http://127.0.0.1:7897"）；币安 API/WS 不可直连的网络下使用。
    /// 配置缺省时回退读环境变量 GREED_PROXY；都没有则直连。
    #[serde(default)]
    pub proxy: Option<String>,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            lake_dir: "data/lake".into(),
            symbol: "BTCUSDT".into(),
            enable_trades: true,
            enable_spot_trades: true,
            enable_book: true,
            enable_oi: true,
            book_snapshot_ms: 5000,
            oi_tick_ms: 60_000,
            book_depth_band_pct: 6.0,
            book_depth_limit: 1000,
            trades_flush_rows: 500_000,
            book_flush_rows: 2000,
            flush_tick_secs: 60,
            max_buffer_secs: 300,
            proxy: None,
        }
    }
}

impl CollectorConfig {
    /// 从 TOML 文本加载（读 `[collector]` 节；缺省字段用默认）。
    pub fn from_toml_str(text: &str) -> Result<Self, toml::de::Error> {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            collector: CollectorConfig,
        }
        Ok(toml::from_str::<Wrapper>(text)?.collector)
    }

    /// 生效的代理地址：配置优先，其次环境变量 GREED_PROXY。
    pub fn effective_proxy(&self) -> Option<String> {
        self.proxy
            .clone()
            .or_else(|| std::env::var("GREED_PROXY").ok())
            .filter(|s| !s.trim().is_empty())
    }
}

/// 账户/下单凭证配置：对应 `config/base.toml [account]`（模拟盘/实盘）。
///
/// 安全约定：**密钥不写入任何文件**，TOML 里只配环境变量名，
/// 运行时从环境变量读取。模拟盘用币安新 Demo Trading（旧合约 testnet
/// testnet.binancefuture.com 已于 2026-07 停用并跳转 demo）：
/// 在 https://demo.binance.com 的 API 管理创建 key，然后
/// `export BINANCE_API_KEY=... BINANCE_API_SECRET=...`。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AccountConfig {
    /// true = 币安 Demo Trading 模拟盘（demo-fapi.binance.com）；false = 主网（实盘，慎用）
    pub testnet: bool,
    /// API key 所在环境变量名
    pub api_key_env: String,
    /// API secret 所在环境变量名
    pub api_secret_env: String,
    /// REST 基础地址（留空按 testnet 自动选择）
    pub rest_base: String,
    /// WS 基础地址（留空按 testnet 自动选择）
    pub ws_base: String,
    /// 现货腿使用独立凭证变量（测试网与合约测试网的 key 不通用）。
    pub spot_api_key_env: String,
    pub spot_api_secret_env: String,
    /// 现货 REST 地址；空值按 testnet 自动选择。
    pub spot_rest_base: String,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            testnet: true,
            api_key_env: "BINANCE_API_KEY".into(),
            api_secret_env: "BINANCE_API_SECRET".into(),
            rest_base: String::new(),
            ws_base: String::new(),
            spot_api_key_env: "BINANCE_SPOT_API_KEY".into(),
            spot_api_secret_env: "BINANCE_SPOT_API_SECRET".into(),
            spot_rest_base: String::new(),
        }
    }
}

impl AccountConfig {
    /// 从 TOML 文本加载（读 `[account]` 节；整节缺失时用默认 = testnet 模拟盘）。
    pub fn from_toml_str(text: &str) -> Result<Self, toml::de::Error> {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            account: AccountConfig,
        }
        Ok(toml::from_str::<Wrapper>(text)?.account)
    }

    pub fn rest_base(&self) -> &str {
        if !self.rest_base.is_empty() {
            &self.rest_base
        } else if self.testnet {
            // 新 Demo Trading（旧 testnet.binancefuture.com 已停用）
            "https://demo-fapi.binance.com"
        } else {
            "https://fapi.binance.com"
        }
    }

    pub fn ws_base(&self) -> &str {
        if !self.ws_base.is_empty() {
            &self.ws_base
        } else if self.testnet {
            // 新 Demo Trading 合约 WS（裸 host，路径由使用方拼接 /ws/...）
            "wss://demo-fapi.binance.com"
        } else {
            // 旧版裸路径已于 2026-04-23 退役，市场数据走 /market
            "wss://fstream.binance.com/market"
        }
    }

    pub fn spot_rest_base(&self) -> &str {
        if !self.spot_rest_base.is_empty() {
            &self.spot_rest_base
        } else if self.testnet {
            // 新 Demo Trading 现货（旧 testnet.binance.vision 已停用）
            "https://demo-api.binance.com"
        } else {
            "https://api.binance.com"
        }
    }

    /// 从环境变量读 API key；未设置时返回 None（调用方应报明确错误）。
    pub fn api_key(&self) -> Option<String> {
        std::env::var(&self.api_key_env)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    pub fn api_secret(&self) -> Option<String> {
        std::env::var(&self.api_secret_env)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }


    pub fn spot_api_key(&self) -> Option<String> {
        std::env::var(&self.spot_api_key_env)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    pub fn spot_api_secret(&self) -> Option<String> {
        std::env::var(&self.spot_api_secret_env)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_from_base_toml_shape() {
        let text = r#"
[collector]
exchanges = ["binance_futures", "binance_spot"]
symbol = "ETHUSDT"
enable_book = false
book_snapshot_ms = 10000
"#;
        let cfg = CollectorConfig::from_toml_str(text).unwrap();
        assert_eq!(cfg.symbol, "ETHUSDT");
        assert!(!cfg.enable_book);
        assert_eq!(cfg.book_snapshot_ms, 10000);
        // 未配置字段走默认
        assert!(cfg.enable_trades);
        assert_eq!(cfg.oi_tick_ms, 60_000);
        assert!((cfg.book_depth_band_pct - 6.0).abs() < 1e-9);
    }

    #[test]
    fn empty_toml_uses_defaults() {
        let cfg = CollectorConfig::from_toml_str("").unwrap();
        assert_eq!(cfg.symbol, "BTCUSDT");
        assert_eq!(cfg.lake_dir, "data/lake");
    }

    #[test]
    fn account_defaults_to_testnet() {
        let acc = AccountConfig::from_toml_str("").unwrap();
        assert!(acc.testnet);
        assert_eq!(acc.rest_base(), "https://demo-fapi.binance.com");
        assert_eq!(acc.ws_base(), "wss://demo-fapi.binance.com");
    }

    #[test]
    fn account_mainnet_override() {
        let acc = AccountConfig::from_toml_str("[account]\ntestnet = false\n").unwrap();
        assert_eq!(acc.rest_base(), "https://fapi.binance.com");
    }
}
