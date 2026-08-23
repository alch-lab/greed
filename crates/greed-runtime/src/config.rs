use greed_strategy::StrategyConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub portfolio: PortfolioConfig,
    pub backtest: BacktestConfig,
    pub execution: ExecutionConfig,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    BinanceDemo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionConfig {
    pub mode: ExecutionMode,
    pub base_url: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub recv_window_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::BinanceDemo,
            base_url: "https://demo-fapi.binance.com".into(),
            api_key_env: "BINANCE_DEMO_API_KEY".into(),
            api_secret_env: "BINANCE_DEMO_API_SECRET".into(),
            recv_window_ms: 5_000,
        }
    }
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
    pub history_path: String,
    pub status_path: String,
    pub execution_state_path: String,
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
            journal_path: "data/runtime/demo-events.jsonl".into(),
            history_path: "data/runtime/demo-history.jsonl".into(),
            status_path: "data/runtime/demo-status.json".into(),
            execution_state_path: "data/runtime/binance-demo-state.json".into(),
            slow_context_path: Some("data/runtime/slow-context.json".into()),
            http_listen: "127.0.0.1:8088".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PortfolioConfig {
    pub initial_equity_usd: f64,
    pub max_positions: usize,
}
impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            initial_equity_usd: 3_000.0,
            max_positions: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BacktestConfig {
    pub fee_bps_per_side: f64,
    pub slippage_bps_per_side: f64,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            fee_bps_per_side: 5.0,
            slippage_bps_per_side: 5.0,
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
        if self.portfolio.initial_equity_usd <= 0.0 {
            return Err("initial_equity_usd must be positive".into());
        }
        if self.portfolio.max_positions == 0 {
            return Err("max_positions must be positive".into());
        }
        if ![
            "https://demo-fapi.binance.com",
            "https://testnet.binancefuture.com",
        ]
        .contains(&self.execution.base_url.trim_end_matches('/'))
        {
            return Err(
                "binance_demo execution is restricted to Binance demo/testnet hosts".into(),
            );
        }
        if !(1_000..=10_000).contains(&self.execution.recv_window_ms) {
            return Err("execution recv_window_ms must be 1000..=10000".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_execution_rejects_mainnet_order_host() {
        let mut config: AppConfig =
            toml::from_str(include_str!("../../../config/demo.toml")).expect("paper config parses");
        config.execution.base_url = "https://fapi.binance.com".into();
        assert!(config.validate().is_err());
    }
}
