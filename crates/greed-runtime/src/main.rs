mod backtest;
mod broker;
mod config;
mod journal;
mod monitor;
mod report;
mod source;

use anyhow::{Context, Result};
use broker::PaperBroker;
use clap::{Parser, Subcommand};
use config::AppConfig;
use greed_kernel::{Artifact, AssetClass, GraphEvaluation, Verdict};
use greed_strategy::{build_graph, StrategyConfig};
use journal::{Journal, SampleRecorder, StatusWriter};
use source::BinancePaperSource;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
#[command(
    name = "greed",
    about = "Composable strategy research and paper runtime"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Parse configuration and build the complete strategy graph without network I/O.
    Validate {
        #[arg(long, default_value = "config/paper.toml")]
        config: String,
    },
    /// Fetch one point-in-time frame and print candidates/plans.
    Once {
        #[arg(long, default_value = "config/paper.toml")]
        config: String,
    },
    /// Run the public-market-data paper loop. Zero iterations means run forever.
    Paper {
        #[arg(long, default_value = "config/paper.toml")]
        config: String,
        #[arg(long, default_value_t = 0)]
        iterations: u64,
    },
    /// Summarize a completed or still-running paper JSONL journal.
    Report {
        #[arg(long, default_value = "data/runtime/paper-events.jsonl")]
        journal: String,
    },
    /// Download official archives, select parameters on train data and report untouched validation.
    Backtest {
        #[arg(long, default_value = "config/paper.toml")]
        config: String,
        #[arg(long)]
        train_from: String,
        #[arg(long)]
        split: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "data/backtest/cache")]
        cache_dir: String,
        #[arg(long, default_value = "data/backtest/report.json")]
        output: String,
    },
}

fn load(path: &str) -> Result<AppConfig> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    let config: AppConfig = toml::from_str(&text).with_context(|| format!("parse {path}"))?;
    config.validate().map_err(anyhow::Error::msg)?;
    Ok(config)
}

fn summarize(evaluation: &GraphEvaluation) -> serde_json::Value {
    let candidates: Vec<_> = evaluation
        .artifacts
        .values()
        .filter_map(|record| match &record.artifact {
            Artifact::Candidate(value) => Some(value),
            _ => None,
        })
        .collect();
    let plans: Vec<_> = evaluation
        .artifacts
        .values()
        .filter_map(|record| match &record.artifact {
            Artifact::PositionPlan(value) => Some(value),
            _ => None,
        })
        .collect();
    serde_json::json!({"node_order":evaluation.node_order,"candidates":candidates,"plans":plans})
}

fn strategy_funnels(
    strategy: &StrategyConfig,
    evaluation: &GraphEvaluation,
    broker: &PaperBroker,
    now_ms: i64,
) -> serde_json::Value {
    serde_json::json!({
        "major": strategy_funnel(AssetClass::Major, strategy, evaluation, broker, now_ms),
        "altcoin": strategy_funnel(AssetClass::Altcoin, strategy, evaluation, broker, now_ms),
    })
}

fn strategy_funnel(
    asset_class: AssetClass,
    strategy: &StrategyConfig,
    evaluation: &GraphEvaluation,
    broker: &PaperBroker,
    now_ms: i64,
) -> serde_json::Value {
    let is_symbol = |symbol: &str| match asset_class {
        AssetClass::Major => strategy.majors.iter().any(|value| value == symbol),
        AssetClass::Altcoin => strategy.altcoins.iter().any(|value| value == symbol),
    };
    let candidates: Vec<_> = evaluation
        .artifacts
        .values()
        .filter_map(|record| record.artifact.candidate())
        .filter(|candidate| is_symbol(&candidate.symbol))
        .collect();
    let passed = candidates
        .iter()
        .filter(|candidate| candidate.verdict == Verdict::Pass)
        .count();
    let performance_gated = candidates
        .iter()
        .filter(|candidate| {
            candidate.verdict == Verdict::Pass
                && !broker.recipe_gate_status(&candidate.recipe, now_ms).allowed
        })
        .count();
    let consumed = candidates
        .iter()
        .filter(|candidate| candidate.verdict == Verdict::Pass && broker.has_seen(&candidate.id))
        .count();
    let unknown = candidates
        .iter()
        .filter(|candidate| candidate.verdict == Verdict::Unknown)
        .count();
    let blocked = candidates.len() - passed - unknown;
    let plans = evaluation
        .artifacts
        .values()
        .filter_map(|record| match &record.artifact {
            Artifact::PositionPlan(value)
                if is_symbol(&value.symbol)
                    && !broker.has_seen(&value.candidate_id)
                    && broker
                        .recipe_gate_status(
                            evaluation
                                .artifacts
                                .values()
                                .filter_map(|record| record.artifact.candidate())
                                .find(|candidate| candidate.id == value.candidate_id)
                                .map(|candidate| candidate.recipe.as_str())
                                .unwrap_or("unknown"),
                            now_ms,
                        )
                        .allowed =>
            {
                Some(value)
            }
            _ => None,
        })
        .count();
    let open_positions = broker
        .positions()
        .values()
        .filter(|position| position.asset_class == asset_class)
        .count();
    let mut blockers: std::collections::BTreeSet<String> = candidates
        .iter()
        .flat_map(|candidate| candidate.blockers.iter())
        .cloned()
        .collect();
    if performance_gated > 0 {
        blockers.insert("rolling PF gate is cooling down this recipe".into());
    }
    if consumed > 0 {
        blockers.insert("current opportunity cycle was already consumed".into());
    }
    let blockers: Vec<_> = blockers.into_iter().take(8).collect();
    let current_stage = if open_positions > 0 {
        "position_management"
    } else if candidates.is_empty() {
        "signal_scan"
    } else if passed == 0 {
        "signal_gates"
    } else if performance_gated == passed {
        "performance_gate"
    } else if consumed == passed {
        "cooldown"
    } else if plans == 0 {
        "risk_sizing"
    } else {
        "paper_execution"
    };
    serde_json::json!({
        "asset_class": asset_class,
        "current_stage": current_stage,
        "candidate_counts": {"total":candidates.len(),"pass":passed,"unknown":unknown,"block":blocked},
        "plans": plans,
        "performance_gated": performance_gated,
        "consumed": consumed,
        "open_positions": open_positions,
        "blockers": blockers,
    })
}

fn runtime_identity(config: &AppConfig, started_ms: i64) -> serde_json::Value {
    let config_bytes = serde_json::to_vec(config).unwrap_or_default();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in config_bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    serde_json::json!({
        "run_id": format!("{}-{}", started_ms, std::process::id()),
        "started_ms": started_ms,
        "version": env!("CARGO_PKG_VERSION"),
        "git_commit": git_commit(),
        "config_hash": format!("{hash:016x}"),
    })
}

fn git_commit() -> Option<String> {
    let head = std::fs::read_to_string(".git/HEAD").ok()?;
    let head = head.trim();
    if let Some(reference) = head.strip_prefix("ref: ") {
        std::fs::read_to_string(format!(".git/{reference}"))
            .ok()
            .map(|value| value.trim().chars().take(12).collect())
    } else {
        Some(head.chars().take(12).collect())
    }
}

async fn one_frame(
    config: &AppConfig,
    source: &mut BinancePaperSource,
    broker: &PaperBroker,
) -> Result<(greed_kernel::MarketFrame, GraphEvaluation)> {
    let mut strategy = config.strategy.clone();
    if strategy.universe.dynamic_enabled {
        let discovery = source
            .discover_altcoins(&strategy, chrono::Utc::now().timestamp_millis())
            .await?;
        if !discovery.symbols.is_empty() {
            strategy.altcoins = discovery.symbols;
        }
    }
    let mut frame = source
        .fetch_frame(&strategy, broker.account_frame())
        .await?;
    frame.account = broker.marked_account(&frame);
    let mut graph = build_graph(&strategy)?;
    let evaluation = graph.evaluate(&frame)?;
    Ok((frame, evaluation))
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Validate { config } => {
            let config = load(&config)?;
            let graph = build_graph(&config.strategy)?;
            drop(graph);
            println!(
                "valid: {} majors, {} altcoins, paper_only=true",
                config.strategy.majors.len(),
                config.strategy.altcoins.len()
            );
            Ok(())
        }
        Command::Once { config } => {
            let config = load(&config)?;
            let mut source = BinancePaperSource::new(config.runtime.clone())?;
            let broker = PaperBroker::with_risk(config.paper.clone(), config.strategy.risk.clone());
            let (_, evaluation) = one_frame(&config, &mut source, &broker).await?;
            println!("{}", serde_json::to_string_pretty(&summarize(&evaluation))?);
            Ok(())
        }
        Command::Paper { config, iterations } => run_paper(load(&config)?, iterations).await,
        Command::Report { journal } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report::build(&journal)?)?
            );
            Ok(())
        }
        Command::Backtest {
            config,
            train_from,
            split,
            to,
            cache_dir,
            output,
        } => {
            let report =
                backtest::run(load(&config)?, &train_from, &split, &to, &cache_dir).await?;
            let bytes = serde_json::to_vec_pretty(&report)?;
            if let Some(parent) = std::path::Path::new(&output).parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&output, &bytes)?;
            println!("{}", String::from_utf8(bytes)?);
            Ok(())
        }
    }
}

async fn run_paper(config: AppConfig, iterations: u64) -> Result<()> {
    let started_ms = chrono::Utc::now().timestamp_millis();
    let identity = runtime_identity(&config, started_ms);
    let _monitor = monitor::start(&config.runtime).await?;
    let mut source = BinancePaperSource::new(config.runtime.clone())?;
    let mut broker = PaperBroker::load_or_new(
        config.paper.clone(),
        config.strategy.risk.clone(),
        &config.runtime.paper_state_path,
        &config.strategy.majors,
    )?;
    let journal = Journal::new(&config.runtime.journal_path)?;
    let history = Journal::new(&config.runtime.history_path)?;
    let status = StatusWriter::new(&config.runtime.status_path);
    let start_payload = serde_json::json!({"paper_only":true,"config":config,"runtime":identity});
    journal.append("runner_start", start_payload.clone())?;
    history.append("runner_start", start_payload)?;
    let mut active_strategy = config.strategy.clone();
    let mut graph = build_graph(&active_strategy)?;
    let mut last_universe_refresh_ms = 0i64;
    let mut universe_status = serde_json::json!({
        "as_of_ms": started_ms,
        "symbols": active_strategy.altcoins.clone(),
        "dynamic": active_strategy.universe.dynamic_enabled,
    });
    let mut samples = SampleRecorder::default();
    let mut completed = 0u64;
    loop {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let refresh_ms = i64::from(config.strategy.universe.refresh_minutes) * 60_000;
        if config.strategy.universe.dynamic_enabled
            && now_ms - last_universe_refresh_ms >= refresh_ms
        {
            last_universe_refresh_ms = now_ms;
            match source.discover_altcoins(&config.strategy, now_ms).await {
                Ok(mut discovery) => {
                    for symbol in broker.positions().keys() {
                        if !config.strategy.majors.contains(symbol)
                            && !discovery.symbols.contains(symbol)
                        {
                            discovery.symbols.push(symbol.clone());
                        }
                    }
                    discovery.symbols.sort();
                    if !discovery.symbols.is_empty()
                        && discovery.symbols != active_strategy.altcoins
                    {
                        active_strategy.altcoins.clone_from(&discovery.symbols);
                        graph = build_graph(&active_strategy)?;
                    }
                    universe_status = serde_json::to_value(&discovery)?;
                    journal.append("universe_refresh", universe_status.clone())?;
                }
                Err(error) => {
                    warn!(error=%error,"dynamic universe refresh failed; keeping previous symbols");
                    journal.append(
                        "universe_refresh_error",
                        serde_json::json!({"ts_ms":now_ms,"error":error.to_string()}),
                    )?;
                }
            }
        }
        match source
            .fetch_frame(&active_strategy, broker.account_frame())
            .await
        {
            Ok(mut frame) => {
                samples.record(&journal, &frame)?;
                let data_health = source.health();
                journal.append("data_health", data_health.clone())?;
                let broker_events = broker.mark_to_market(&frame);
                for event in broker_events {
                    history.append(&event.kind, event.payload.clone())?;
                    journal.append(&event.kind, event.payload)?;
                }
                frame.account = broker.marked_account(&frame);
                let evaluation = graph.evaluate(&frame)?;
                journal.append("graph_evaluation", serde_json::to_value(&evaluation)?)?;
                let fill_events = broker.apply_plans(&frame, &evaluation);
                for event in fill_events {
                    history.append(&event.kind, event.payload.clone())?;
                    journal.append(&event.kind, event.payload)?;
                }
                broker.save(&config.runtime.paper_state_path)?;
                let account = broker.marked_account(&frame);
                let sleeves = broker.sleeve_snapshots(&frame);
                let funnels =
                    strategy_funnels(&active_strategy, &evaluation, &broker, frame.as_of_ms);
                let recipe_gates = broker.recipe_gate_snapshots(frame.as_of_ms);
                let observation = serde_json::json!({
                    "ts_ms": frame.as_of_ms,
                    "equity_usd": account.equity_usd,
                    "cash_usd": account.cash_usd,
                    "realized_pnl_usd": account.realized_pnl_usd,
                    "gross_exposure_usd": account.gross_exposure_usd,
                    "drawdown_pct": ((account.peak_equity_usd-account.equity_usd)/account.peak_equity_usd.max(1.0)).max(0.0),
                    "daily_loss_pct": ((account.risk_day_start_equity_usd-account.equity_usd)/account.risk_day_start_equity_usd.max(1.0)).max(0.0),
                    "sleeves": sleeves,
                    "funnels": funnels,
                    "recipe_gates": recipe_gates,
                    "data_health": data_health,
                    "runtime": identity,
                });
                history.append("paper_equity", observation.clone())?;
                journal.append("paper_equity", observation)?;
                status.write(&serde_json::json!({"as_of_ms":frame.as_of_ms,"paper_only":true,"account":account,"sleeves":sleeves,"funnels":funnels,"recipe_gates":recipe_gates,"positions":broker.position_snapshots(&frame),"graph":summarize(&evaluation),"artifacts":evaluation.artifacts,"universe":universe_status,"data_health":data_health,"runtime":identity}))?;
                completed += 1;
                info!(
                    iteration = completed,
                    equity = broker.account_frame().equity_usd,
                    "paper frame complete"
                );
            }
            Err(error) => {
                warn!(error=%error,"paper frame failed; no strategy evaluation or order simulation performed");
                let data_health = source.health();
                journal.append("data_health", data_health)?;
                journal.append(
                    "frame_error",
                    serde_json::json!({"error":error.to_string()}),
                )?;
            }
        }
        if iterations > 0 && completed >= iterations {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(config.runtime.poll_seconds.max(15))).await;
    }
}
