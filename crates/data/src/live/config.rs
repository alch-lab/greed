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
}
