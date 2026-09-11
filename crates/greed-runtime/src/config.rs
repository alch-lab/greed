use greed_strategy::StrategyConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub portfolio: PortfolioConfig,
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
    pub leverage: u8,
    pub executable_profit_guard: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::BinanceDemo,
            base_url: "https://demo-fapi.binance.com".into(),
            api_key_env: "BINANCE_DEMO_API_KEY".into(),
            api_secret_env: "BINANCE_DEMO_API_SECRET".into(),
            recv_window_ms: 5_000,
            leverage: 5,
            executable_profit_guard: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub binance_futures_base: String,
    pub binance_futures_fallbacks: Vec<String>,
    pub binance_futures_ws_base: String,
    pub proxy: Option<String>,
    pub poll_seconds: u64,
    /// Cadence of the position/protection reconciliation loop. This is kept
    /// separate from the slower strategy-frame cadence so candidate research
    /// and universe refreshes cannot define exit responsiveness.
    pub execution_sync_millis: u64,
    pub stream_warmup_seconds: u64,
    pub request_spacing_ms: u64,
    pub request_timeout_seconds: u64,
    pub candle_limit: usize,
    pub journal_path: String,
    pub history_path: String,
    pub research_enabled: bool,
    pub research_path: String,
    pub research_snapshot_seconds: u64,
    pub research_level_map_seconds: u64,
    pub research_backfill_1m_bars: usize,
    pub research_backfill_5m_bars: usize,
    pub research_backfill_15m_bars: usize,
    pub research_backfill_1h_bars: usize,
    pub research_file_max_mb: u64,
    pub research_rotations: usize,
    pub status_path: String,
    pub execution_state_path: String,
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
            binance_futures_ws_base: "wss://fstream.binance.com".into(),
            proxy: None,
            poll_seconds: 15,
            execution_sync_millis: 500,
            stream_warmup_seconds: 20,
            request_spacing_ms: 150,
            request_timeout_seconds: 8,
            candle_limit: 160,
            journal_path: "data/runtime/alpha-events.jsonl".into(),
            history_path: "data/runtime/alpha-history.jsonl".into(),
            research_enabled: true,
            research_path: "data/research/market-research.jsonl".into(),
            research_snapshot_seconds: 15,
            research_level_map_seconds: 60,
            research_backfill_1m_bars: 360,
            research_backfill_5m_bars: 576,
            research_backfill_15m_bars: 1_200,
            research_backfill_1h_bars: 720,
            research_file_max_mb: 256,
            research_rotations: 8,
            status_path: "data/runtime/alpha-status.json".into(),
            execution_state_path: "data/runtime/binance-alpha-state.json".into(),
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
            initial_equity_usd: 5_000.0,
            max_positions: 3,
        }
    }
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.strategy.validate()?;
        if !(5..=60).contains(&self.runtime.poll_seconds) {
            return Err("poll_seconds must be 5..=60".into());
        }
        if !(250..=2_000).contains(&self.runtime.execution_sync_millis) {
            return Err("execution_sync_millis must be 250..=2000".into());
        }
        if !(5..=30).contains(&self.runtime.stream_warmup_seconds) {
            return Err("stream warmup setting is outside safe bounds".into());
        }
        if !self.runtime.binance_futures_ws_base.starts_with("wss://") {
            return Err("binance_futures_ws_base must use wss".into());
        }
        if !(120..=1500).contains(&self.runtime.candle_limit) {
            return Err("candle_limit must be 120..=1500".into());
        }
        if !(5..=60).contains(&self.runtime.research_snapshot_seconds) {
            return Err("research_snapshot_seconds must be 5..=60".into());
        }
        if !(30..=300).contains(&self.runtime.research_level_map_seconds) {
            return Err("research_level_map_seconds must be 30..=300".into());
        }
        for (name, value) in [
            (
                "research_backfill_1m_bars",
                self.runtime.research_backfill_1m_bars,
            ),
            (
                "research_backfill_5m_bars",
                self.runtime.research_backfill_5m_bars,
            ),
            (
                "research_backfill_15m_bars",
                self.runtime.research_backfill_15m_bars,
            ),
            (
                "research_backfill_1h_bars",
                self.runtime.research_backfill_1h_bars,
            ),
        ] {
            if !(60..=1500).contains(&value) {
                return Err(format!("{name} must be 60..=1500"));
            }
        }
        if !(64..=1024).contains(&self.runtime.research_file_max_mb) {
            return Err("research_file_max_mb must be 64..=1024".into());
        }
        if !(1..=32).contains(&self.runtime.research_rotations) {
            return Err("research_rotations must be 1..=32".into());
        }
        if self.portfolio.initial_equity_usd <= 0.0 {
            return Err("initial_equity_usd must be positive".into());
        }
        if self.portfolio.max_positions == 0 {
            return Err("max_positions must be positive".into());
        }
        if self.portfolio.max_positions != self.strategy.risk.max_positions {
            return Err("portfolio and strategy risk max_positions must match".into());
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
        if !(1..=5).contains(&self.execution.leverage) {
            return Err("demo leverage must be 1..=5".into());
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
