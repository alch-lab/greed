use greed_strategy::StrategyConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub paper: PaperConfig,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub binance_futures_base: String,
    pub binance_futures_fallbacks: Vec<String>,
    pub binance_spot_base: String,
    pub binance_spot_fallbacks: Vec<String>,
    pub coinbase_base: String,
    pub proxy: Option<String>,
    pub poll_seconds: u64,
    pub request_spacing_ms: u64,
    pub request_timeout_seconds: u64,
    pub candle_limit: usize,
    pub journal_path: String,
    pub status_path: String,
    pub paper_state_path: String,
    pub slow_context_path: Option<String>,
    pub http_listen: String,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            binance_futures_base: "https://fapi.binance.com".into(),
            binance_futures_fallbacks: vec![
                "https://fapi1.binance.com".into(),
                "https://fapi2.binance.com".into(),
            ],
            binance_spot_base: "https://api.binance.com".into(),
            binance_spot_fallbacks: vec![
                "https://api1.binance.com".into(),
                "https://api2.binance.com".into(),
            ],
            coinbase_base: "https://api.exchange.coinbase.com".into(),
            proxy: None,
            poll_seconds: 60,
            request_spacing_ms: 150,
            request_timeout_seconds: 8,
            candle_limit: 160,
            journal_path: "data/runtime/paper-events.jsonl".into(),
            status_path: "data/runtime/status.json".into(),
            paper_state_path: "data/runtime/paper-state.json".into(),
            slow_context_path: Some("data/runtime/slow-context.json".into()),
            http_listen: "127.0.0.1:8088".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PaperConfig {
    pub initial_cash_usd: f64,
    pub fee_bps_per_side: f64,
    pub slippage_bps_per_side: f64,
    pub max_positions: usize,
}
impl Default for PaperConfig {
    fn default() -> Self {
        Self {
            initial_cash_usd: 3_000.0,
            fee_bps_per_side: 5.0,
            slippage_bps_per_side: 5.0,
            max_positions: 5,
        }
    }
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.strategy.validate()?;
        if self.runtime.poll_seconds < 15 {
            return Err("poll_seconds must be at least 15".into());
        }
        if !(120..=1000).contains(&self.runtime.candle_limit) {
            return Err("candle_limit must be 120..=1000".into());
        }
        if self.paper.initial_cash_usd <= 0.0 {
            return Err("initial_cash_usd must be positive".into());
        }
        if self.paper.max_positions == 0 {
            return Err("max_positions must be positive".into());
        }
        Ok(())
    }
}
