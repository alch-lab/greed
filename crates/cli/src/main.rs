//! greed 命令行入口。
//!
//! - `ingest`   → Binance aggTrades 历史数据导入数据湖
//! - `collect`  → 实时采集 daemon
//! - `backtest` → 端到端回测（可导出 journal 供前端）
//! - `trade`    → 模拟盘/实盘执行
//! - `serve`    → HTTP 控制面（前端监控/启停/回测任务）

mod serve;
mod trade_runner;

use anyhow::{Context, Result};
use backtest::{
    build_report, pair_round_trips, to_json, to_markdown, BacktestConfig, BacktestEngine,
    Journal, JournalMeta, ReportConfig,
};
use clap::{Parser, Subcommand};
use data::live::{run_collector, CollectorConfig};
use data::{ingest_day, Lake, Market};
use signals::renko::{brick_stats, bricks_to_csv, stats_to_markdown, RenkoConfig, RenkoEngine};
use strategy::{assemble_from_toml, builtin_registry};
use tcore::types::{Exchange, Symbol, Timestamp};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "greed", version, about = "TRDR 订单流量化系统", long_about = None)]
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
    /// 启动实时采集 daemon（PR-11：trades  订单簿快照 + OI → 数据湖）
    Collect {
        /// 配置文件路径（读 [collector] 节）
        #[arg(long, default_value = "config/base.toml")]
        config: String,
        /// 试运行：只统计速率不落盘（验证连通性）
        #[arg(long)]
        dry_run: bool,
    },
    /// 运行回测（PR-10）
    Backtest {
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
        /// 策略 TOML 配置路径
        #[arg(long)]
        strategy: String,
        /// 初始资金
        #[arg(long, default_value_t = 100_000.0)]
        cash: f64,
        /// 风险分数
        #[arg(long, default_value_t = 0.0075)]
        risk_pct: f64,
        /// 几何自然反转率
        #[arg(long, default_value_t = 0.605)]
        geo_baseline: f64,
        /// 试验次数（DSR 用）
        #[arg(long, default_value_t = 1)]
        trials: usize,
        /// 限价入场单有效期（毫秒）
        #[arg(long, default_value_t = 4 * 3_600_000)]
        entry_ttl_ms: i64,
        /// 单笔最大风险（占 equity 比例，封顶 risk_pct）
        #[arg(long, default_value_t = 0.015)]
        max_risk_pct: f64,
        /// 熔断：日连亏笔数上限（0 = 关闭）
        #[arg(long, default_value_t = 0)]
        cb_max_daily_losses: u32,
        /// 熔断：日内回撤上限（0 = 关闭），如 0.02 = 2%
        #[arg(long, default_value_t = 0.0)]
        cb_daily_dd_pct: f64,
        /// 决策流水导出路径（JSON：意图/成交/权益曲线，供前端监控）
        #[arg(long)]
        journal: Option<String>,
        /// 输出前缀
        #[arg(long, default_value = "out/backtest")]
        out: String,
    },
    /// 校验配置与数据（PR-10）：装配策略配置 + 检查数据湖覆盖
    Validate {
        /// 策略 TOML 配置路径（可选；给出则装配校验）
        #[arg(long)]
        strategy: Option<String>,
        /// 交易对，如 BTCUSDT
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        /// 市场：perp（USDT永续）或 spot
        #[arg(long, default_value = "perp")]
        market: String,
        /// 数据湖目录
        #[arg(long, default_value = "data/lake")]
        lake: String,
        /// 起始日期 yyyy-mm-dd（可选；与 --to 一起给出则检查覆盖）
        #[arg(long)]
        from: Option<String>,
        /// 结束日期 yyyy-mm-dd
        #[arg(long)]
        to: Option<String>,
    },

    /// 跑 renko 砖序列并导出统计（砖 CSV + 马尔可夫基线统计）
    Renko {
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
        /// 趋势砖尺寸（美元）
        #[arg(long, default_value_t = 100.0)]
        trend: f64,
        /// 反转砖尺寸（美元）
        #[arg(long, default_value_t = 62.0)]
        reversal: f64,
        /// 输出前缀（生成 {out}-bricks.csv 与 {out}-stats.md）
        #[arg(long, default_value = "out/renko")]
        out: String,
    },
    /// 模拟盘/实盘执行（PR-12）：实时行情 → 策略决策 → testnet 下单 → journal 落盘
    Trade {
        /// 配置文件路径（读 [collector] 的 symbol/proxy 与 [account]）
        #[arg(long, default_value = "config/base.toml")]
        config: String,
        /// 策略 TOML 配置路径
        #[arg(long)]
        strategy: String,
        /// journal 输出路径（默认 data/journal/{mode}.json，原子重写）
        #[arg(long)]
        journal: Option<String>,
        /// 初始资金（仅 --dry-run 用；testnet 模式取真实钱包余额）
        #[arg(long, default_value_t = 100_000.0)]
        cash: f64,
        /// 风险分数（同回测）
        #[arg(long, default_value_t = 0.0075)]
        risk_pct: f64,
        /// 单笔最大风险（占 equity 比例上限）
        #[arg(long, default_value_t = 0.015)]
        max_risk_pct: f64,
        /// 限价入场单有效期（毫秒）
        #[arg(long, default_value_t = 4 * 3_600_000)]
        entry_ttl_ms: i64,
        /// 熔断：日连亏笔数上限（0 = 关闭）
        #[arg(long, default_value_t = 0)]
        cb_max_daily_losses: u32,
        /// 熔断：日内回撤上限（0 = 关闭）
        #[arg(long, default_value_t = 0.0)]
        cb_daily_dd_pct: f64,
        /// testnet 杠杆（启动时设置，逐仓）
        #[arg(long, default_value_t = 3)]
        leverage: u32,
        /// 干跑：不连签名接口、不下单，用模拟撮合验证全链路
        #[arg(long)]
        dry_run: bool,
        /// 覆盖行情 WS 地址（如 dry-run 想用主网更稠密的行情：
        /// --ws-base wss://fstream.binance.com）
        #[arg(long)]
        ws_base: Option<String>,
    },
    /// HTTP 控制面（PR-13）：前端监控/启停交易/跑回测
    Serve {
        /// 监听地址（公网部署时保持 127.0.0.1，由 Caddy/nginx 反代暴露）
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// 监听端口
        #[arg(long, default_value_t = 8088)]
        port: u16,
        /// 配置文件路径（读 [collector] 与 [account]）
        #[arg(long, default_value = "config/base.toml")]
        config: String,
        /// 默认策略 TOML 配置路径（启动交易/回测的默认）
        #[arg(long, default_value = "config/strategy-final.toml")]
        strategy: String,
        /// 数据湖目录（回测用）
        #[arg(long, default_value = "data/lake")]
        lake: String,
    },
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
                .user_agent("greed-ingest/0.1")
                .build()?;

            let mut total_rows = 0usize;
            let mut total_bytes = 0usize;
            let mut done = 0usize;
            let mut skipped = 0usize;
            for date in &dates {
                match ingest_day(&client, &lake, market, &symbol, date).await {
                    Ok(Some(s)) => {
                        total_rows = s.rows;
                        total_bytes = s.bytes;
                        done = 1;
                        info!(%date, rows = s.rows, "导入完成");
                    }
                    Ok(None) => {
                        skipped = 1;
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

        Command::Collect { config, dry_run } => {
            let text = std::fs::read_to_string(&config)
                .with_context(|| format!("读取配置失败: {}", config))?;
            let cfg = CollectorConfig::from_toml_str(&text)
                .with_context(|| format!("解析配置 [collector] 失败: {}", config))?;
            info!(%config, dry_run, symbol = %cfg.symbol, lake = %cfg.lake_dir, "启动采集");
            run_collector(cfg, dry_run).await?;
            Ok(())
        }

        Command::Renko {
            symbol,
            market,
            from,
            to,
            lake,
            trend,
            reversal,
            out,
        } => {
            let exchange = match market.as_str() {
                "perp" | "um" | "futures" => Exchange::BinanceFutures,
                "spot" => Exchange::BinanceSpot,
                other => anyhow::bail!("未知市场: {}（用 perp 或 spot）", other),
            };
            // [from 00:00, to+1d 00:00) UTC
            let parse = |s: &str| -> Result<chrono::NaiveDate> {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .with_context(|| format!("日期格式错误（应 yyyy-mm-dd）: {}", s))
            };
            let from_ms = parse(&from)?
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            let to_ms = (parse(&to)? + chrono::Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();

            let lake = Lake::new(&lake);
            let sym = Symbol::new(&symbol);
            info!(%symbol, %from, %to, trend, reversal, "读取数据湖");
            let trades = data::lake::read_range(
                &lake,
                exchange,
                &sym,
                Timestamp::from_millis(from_ms),
                Timestamp::from_millis(to_ms),
            )?;
            info!(rows = trades.len(), "回放逐笔 → renko 引擎");

            let mut engine = RenkoEngine::new(RenkoConfig::trend_reversal_usd(trend, reversal));
            let mut bricks = Vec::new();
            for t in &trades {
                bricks.extend(engine.on_trade(t));
            }
            let stats = brick_stats(&bricks);

            if let Some(parent) = std::path::Path::new(&out).parent() {
                std::fs::create_dir_all(parent)?;
            }
            let csv_path = format!("{}-bricks.csv", out);
            let md_path = format!("{}-stats.md", out);
            bricks_to_csv(std::fs::File::create(&csv_path)?, &bricks)?;
            std::fs::write(&md_path, stats_to_markdown(&stats))?;

            info!(
                bricks = stats.n_bricks,
                chains = stats.n_chains,
                max_run = stats.max_run,
                p_continue = ?stats.p_continue_after_reversal,
                csv = %csv_path,
                md = %md_path,
                "renko 导出完成"
            );
            println!("{}", stats_to_markdown(&stats));
            Ok(())
        }
        Command::Backtest {
            symbol,
            market,
            from,
            to,
            lake,
            strategy,
            cash,
            risk_pct,
            geo_baseline,
            trials,
            entry_ttl_ms,
            max_risk_pct,
            cb_max_daily_losses,
            cb_daily_dd_pct,
            journal,
            out,
        } => {
            let exchange = match market.as_str() {
                "perp" | "um" | "futures" => Exchange::BinanceFutures,
                "spot" => Exchange::BinanceSpot,
                other => anyhow::bail!("未知市场: {}（用 perp 或 spot）", other),
            };
            let parse = |s: &str| -> Result<chrono::NaiveDate> {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .with_context(|| format!("日期格式错误（应 yyyy-mm-dd）: {}", s))
            };
            let from_ms = parse(&from)?
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();
            let to_ms = (parse(&to)? + chrono::Duration::days(1))
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_millis();

            // 装配策略
            let toml_str = std::fs::read_to_string(&strategy)
                .with_context(|| format!("读策略配置失败: {}", strategy))?;
            let strat = assemble_from_toml(&toml_str, &builtin_registry())
                .map_err(|e| anyhow::anyhow!("装配策略失败: {}", e))?;
            info!(%strategy, "{}", strat.describe());

            // 读数据
            let lake = Lake::new(&lake);
            let sym = Symbol::new(&symbol);
            info!(%symbol, %from, %to, "读取数据湖");
            let trades = data::lake::read_range(
                &lake,
                exchange,
                &sym,
                Timestamp::from_millis(from_ms),
                Timestamp::from_millis(to_ms),
            )?;
            // OI / 资金费率（若已用 fetch_macro_data.py 回补则自动并入事件流）
            let ois = data::lake::read_oi_metrics(
                &lake,
                exchange,
                &sym,
                Timestamp::from_millis(from_ms),
                Timestamp::from_millis(to_ms),
            )?;
            let fundings = data::lake::read_funding(
                &lake,
                exchange,
                &sym,
                Timestamp::from_millis(from_ms),
                Timestamp::from_millis(to_ms),
            )?;
            info!(trades = trades.len(), oi = ois.len(), funding = fundings.len(), "回放事件流 → 回测引擎");
            let mut events: Vec<tcore::Event> = Vec::with_capacity(trades.len() + ois.len() + fundings.len());
            events.extend(trades.into_iter().map(tcore::Event::Trade));
            events.extend(ois.into_iter().map(tcore::Event::Oi));
            events.extend(fundings.into_iter().map(tcore::Event::Funding));
            events.sort_by_key(|e| e.ts());

            // 跑回测
            let cfg = BacktestConfig {
                initial_cash: cash,
                risk_pct,
                entry_ttl_ms,
                max_risk_pct,
                cb_max_daily_losses,
                cb_daily_dd_pct,
                ..Default::default()
            };
            let mut engine = BacktestEngine::new(strat, sym.clone(), cfg);
            let result = engine.run(&events);
            info!(
                fills = result.fills.len(),
                final_equity = result.final_equity,
                "回测完成"
            );

            // PR-7 报告
            let trips = pair_round_trips(&result.fills);
            let rc = ReportConfig {
                geometric_baseline: geo_baseline,
                n_trials: trials,
                risk_pct,
                ..Default::default()
            };
            let report = build_report(&trips, &result.equity_curve, &rc, &[]);

            if let Some(parent) = std::path::Path::new(&out).parent() {
                std::fs::create_dir_all(parent)?;
            }
            // 决策流水（前端监控数据源）
            if let Some(jpath) = &journal {
                let j = Journal {
                    meta: JournalMeta {
                        symbol: symbol.clone(),
                        from: from.clone(),
                        to: to.clone(),
                        strategy: strategy.clone(),
                        initial_cash: cash,
                        final_equity: result.final_equity,
                    },
                    intents: result.intents.clone(),
                    fills: result.fills.clone(),
                    equity_curve: result.equity_curve.clone(),
                    evals: Vec::new(),
                };
                if let Some(parent) = std::path::Path::new(jpath).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(jpath, serde_json::to_string_pretty(&j)?)?;
                info!(path = %jpath, "journal 已写出");
            }

            let md_path = format!("{}.md", out);
            let json_path = format!("{}.json", out);
            std::fs::write(&md_path, to_markdown(&report))?;
            std::fs::write(&json_path, to_json(&report)?)?;

            println!("{}", to_markdown(&report));
            info!(
                trips = trips.len(),
                gate = %report.gate_verdict,
                md = %md_path,
                json = %json_path,
                "报告已写出"
            );
            Ok(())
        }
        Command::Validate {
            strategy,
            symbol,
            market,
            lake,
            from,
            to,
        } => {
            let mut ok = true;

            // 1) 策略配置装配校验
            if let Some(path) = &strategy {
                let toml_str = std::fs::read_to_string(path)
                    .with_context(|| format!("读策略配置失败: {}", path))?;
                match assemble_from_toml(&toml_str, &builtin_registry()) {
                    Ok(strat) => {
                        println!("✅ 策略装配成功: {}", path);
                        println!("   {}", strat.describe());
                    }
                    Err(e) => {
                        println!("❌ 策略装配失败: {}: {}", path, e);
                        ok = false;
                    }
                }
            }

            // 2) 数据湖覆盖检查
            let exchange = match market.as_str() {
                "perp" | "um" | "futures" => Exchange::BinanceFutures,
                "spot" => Exchange::BinanceSpot,
                other => anyhow::bail!("未知市场: {}（用 perp 或 spot）", other),
            };
            let lake = Lake::new(&lake);
            let sym = Symbol::new(&symbol);
            let dir = lake.dir(exchange, &sym);
            if !dir.exists() {
                println!("❌ 数据湖目录不存在: {}", dir.display());
                ok = false;
            } else {
                let mut days: Vec<String> = std::fs::read_dir(&dir)?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| {
                        let n = e.file_name().to_string_lossy().to_string();
                        n.strip_suffix(".binlog").map(|s| s.to_string())
                    })
                    .collect();
                days.sort();
                match (days.first(), days.last()) {
                    (Some(f), Some(l)) => {
                        println!("✅ 数据湖: {} 共 {} 天（{} → {}）", dir.display(), days.len(), f, l);
                    }
                    _ => {
                        println!("❌ 数据湖为空: {}", dir.display());
                        ok = false;
                    }
                }
                // 指定区间：检查缺失天
                if let (Some(f), Some(t)) = (&from, &to) {
                    let want = date_range(f, t)?;
                    let have: std::collections::HashSet<&String> = days.iter().collect();
                    let missing: Vec<&String> =
                        want.iter().filter(|d| !have.contains(d)).collect();
                    if missing.is_empty() {
                        println!("✅ 区间 {} → {} 覆盖完整（{} 天）", f, t, want.len());
                    } else {
                        println!(
                            "❌ 区间 {} → {} 缺失 {} 天: {:?}{}",
                            f,
                            t,
                            missing.len(),
                            &missing[..missing.len().min(10)],
                            if missing.len() > 10 { " …" } else { "" }
                        );
                        ok = false;
                    }
                }
            }

            if ok {
                println!("\n校验通过 ✅");
                Ok(())
            } else {
                anyhow::bail!("校验未通过（见上方 ❌ 项）")
            }
        }
        Command::Trade {
            config,
            strategy,
            journal,
            cash,
            risk_pct,
            max_risk_pct,
            entry_ttl_ms,
            cb_max_daily_losses,
            cb_daily_dd_pct,
            leverage,
            dry_run,
            ws_base,
        } => {
            let (_tx, rx) = tokio::sync::watch::channel(false);
            trade_runner::run_trade(
                trade_runner::TradeArgs {
                    config,
                    strategy,
                    journal,
                    mode: if dry_run {
                        Some(trade_runner::TradeMode::Dry)
                    } else {
                        None // 由 [account].testnet 推导
                    },
                    cash,
                    risk_pct,
                    max_risk_pct,
                    entry_ttl_ms,
                    cb_max_daily_losses,
                    cb_daily_dd_pct,
                    leverage,
                    ws_base,
                },
                rx,
                None,
            )
            .await
        }
        Command::Serve {
            host,
            port,
            config,
            strategy,
            lake,
        } => serve::run_serve(&host, port, config, strategy, lake).await,
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
