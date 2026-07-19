//! trader 命令行入口（PR-1：子命令骨架 + 配置加载 + 日志初始化）。
//!
//! 后续 PR 逐步接入实现：
//! - `ingest`   → PR-3  （历史数据导入）
//! - `collect`  → PR-11 （实时采集 daemon）
//! - `backtest` → PR-10 （端到端回测）

use anyhow::{Context, Result};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug)]
struct Cli {
    command: Command,
    config_path: String,
}

#[derive(Debug)]
enum Command {
    Ingest,
    Collect,
    Backtest,
    Validate,
    Help,
}

fn parse_args() -> Cli {
    let mut args = std::env::args().skip(1);
    let command = match args.next().as_deref() {
        Some("ingest") => Command::Ingest,
        Some("collect") => Command::Collect,
        Some("backtest") => Command::Backtest,
        Some("validate") => Command::Validate,
        _ => Command::Help,
    };
    let mut config_path = "config/base.toml".to_string();
    while let Some(a) = args.next() {
        if a == "--config" {
            if let Some(p) = args.next() {
                config_path = p;
            }
        }
    }
    Cli {
        command,
        config_path,
    }
}

fn print_help() {
    println!(
        "trader — TRDR 订单流量化系统\n\
         \n\
         用法: trader <命令> [--config <路径>]\n\
         \n\
         命令:\n\
         \x20 ingest     导入 Binance 历史数据到本地数据湖      (PR-3)\n\
         \x20 collect    启动三所实时采集 daemon               (PR-11)\n\
         \x20 backtest   运行回测并输出绩效报告               (PR-10)\n\
         \x20 validate   校验配置与数据完整性                (PR-10)\n"
    );
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}

fn main() -> Result<()> {
    init_tracing();
    let cli = parse_args();

    match cli.command {
        Command::Help => {
            print_help();
            Ok(())
        }
        other => {
            // 统一校验配置可读（后续 PR 换成强类型 Settings）
            let raw = std::fs::read_to_string(&cli.config_path)
                .with_context(|| format!("无法读取配置文件: {}", cli.config_path))?;
            tracing::info!(config = %cli.config_path, bytes = raw.len(), "配置加载成功");
            match other {
                Command::Ingest => anyhow::bail!("ingest 未实现（PR-3）"),
                Command::Collect => anyhow::bail!("collect 未实现（PR-11）"),
                Command::Backtest => anyhow::bail!("backtest 未实现（PR-10）"),
                Command::Validate => anyhow::bail!("validate 未实现（PR-10）"),
                Command::Help => unreachable!(),
            }
        }
    }
}
