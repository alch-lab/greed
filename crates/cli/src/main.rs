//! trader 命令行入口。
//!
//! 已接入：
//! - `ingest`   → Binance aggTrades 历史数据导入数据湖
//!
//! 待接入：
//! - `collect`  → 实时采集 daemon
//! - `backtest` → 端到端回测

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use data::{ingest_day, Lake, Market};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "trader", version, about = "TRDR 订单流量化系统", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// 导入 Binance 历史 aggTrades 到本地数据湖
    Ingest {
        /// 交易对，如 BTCUSDT
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        /// 市场：perp（USDT永续）或 spot
        #[arg(long, default_value = "perp")]
        market: String,
        /// 起始日期 yyyy-mm-dd（含）
        #[arg(long)]
        from: String,
        /// 结束日期 yyyy-mm-dd（含）
        #[arg(long)]
        to: String,
        /// 数据湖目录
        #[arg(long, default_value = "data/lake")]
        lake: String,
    },
    /// 启动三所实时采集 daemon（PR-11）
    Collect,
    /// 运行回测（PR-10）
    Backtest,
    /// 校验配置与数据（PR-10）
    Validate,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}

/// 生成 [from, to] 闭区间的日期序列（yyyy-mm-dd），要求 from <= to。
fn date_range(from: &str, to: &str) -> Result<Vec<String>> {
    let parse = |s: &str| -> Result<chrono::NaiveDate> {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .with_context(|| format!("日期格式错误（应 yyyy-mm-dd）: {}", s))
    };
    let start = parse(from)?;
    let end = parse(to)?;
    if start > end {
        anyhow::bail!("起始日期晚于结束日期: {} > {}", from, to);
    }
    let mut out = Vec::new();
    let mut d = start;
    while d <= end {
        out.push(d.format("%Y-%m-%d").to_string());
        d += chrono::Duration::days(1);
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest {
            symbol,
            market,
            from,
            to,
            lake,
        } => {
            let market = match market.as_str() {
                "perp" | "um" | "futures" => Market::UsdtPerp,
                "spot" => Market::Spot,
                other => anyhow::bail!("未知市场: {}（用 perp 或 spot）", other),
            };
            let lake = Lake::new(&lake);
            let dates = date_range(&from, &to)?;
            let client = reqwest::Client::builder()
                .user_agent("trader-ingest/0.1")
                .build()?;

            let mut total_rows = 0usize;
            let mut total_bytes = 0usize;
            let mut done = 0usize;
            let mut skipped = 0usize;
            for date in &dates {
                match ingest_day(&client, &lake, market, &symbol, date).await {
                    Ok(Some(s)) => {
                        total_rows += s.rows;
                        total_bytes += s.bytes;
                        done += 1;
                        info!(%date, rows = s.rows, "导入完成");
                    }
                    Ok(None) => {
                        skipped += 1;
                        info!(%date, "无数据，跳过");
                    }
                    Err(e) => {
                        error!(%date, error = %e, "导入失败");
                        anyhow::bail!("导入 {} 失败: {}", date, e);
                    }
                }
            }
            info!(
                days = done,
                skipped,
                total_rows,
                total_mb = total_bytes / 1_048_576,
                "全部导入完成"
            );
            Ok(())
        }
        Command::Collect => anyhow::bail!("collect 未实现（PR-11）"),
        Command::Backtest => anyhow::bail!("backtest 未实现（PR-10）"),
        Command::Validate => anyhow::bail!("validate 未实现（PR-10）"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_range_inclusive() {
        let r = date_range("2024-01-01", "2024-01-03").unwrap();
        assert_eq!(r, vec!["2024-01-01", "2024-01-02", "2024-01-03"]);
        assert_eq!(date_range("2024-01-01", "2024-01-01").unwrap().len(), 1);
        assert!(date_range("2024-01-02", "2024-01-01").is_err());
    }
}
