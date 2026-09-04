mod config;
mod execution;
mod journal;
mod market_stream;
mod monitor;
mod report;
mod source;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::AppConfig;
use execution::BinanceDemoExecution;
use greed_kernel::{AccountFrame, Artifact, GraphEvaluation, Verdict};
use greed_strategy::{build_graph, StrategyConfig};
use journal::{Journal, ResearchRecorder, SampleRecorder, StatusWriter};
use source::BinanceMarketSource;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const UNIVERSE_MISS_THRESHOLD: u8 = 4;
const UNIVERSE_MAX_REPLACEMENTS: usize = 2;

fn stabilize_universe(
    current: &[String],
    desired_ranked: &[String],
    pinned: &[String],
    max_symbols: usize,
    misses: &mut BTreeMap<String, u8>,
) -> Vec<String> {
    let desired: BTreeSet<_> = desired_ranked.iter().cloned().collect();
    let pinned: BTreeSet<_> = pinned.iter().cloned().collect();
    for symbol in current {
        if desired.contains(symbol) || pinned.contains(symbol) {
            misses.remove(symbol);
        } else {
            let value = misses.entry(symbol.clone()).or_default();
            *value = value.saturating_add(1);
        }
    }

    let mut result: BTreeSet<String> = pinned.clone();
    let mut evictions = 0usize;
    for symbol in current {
        let expired = misses
            .get(symbol)
            .is_some_and(|count| *count >= UNIVERSE_MISS_THRESHOLD);
        if expired && !pinned.contains(symbol) && evictions < UNIVERSE_MAX_REPLACEMENTS {
            evictions += 1;
            continue;
        }
        result.insert(symbol.clone());
    }

    let mut additions = 0usize;
    for symbol in desired_ranked {
        if result.len() >= max_symbols || additions >= UNIVERSE_MAX_REPLACEMENTS {
            break;
        }
        if result.insert(symbol.clone()) {
            additions += 1;
        }
    }
    misses.retain(|symbol, _| result.contains(symbol));
    result.into_iter().take(max_symbols).collect()
}

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
        #[arg(long, default_value = "config/demo.toml")]
        config: String,
    },
    /// Fetch one point-in-time frame and print candidates/plans.
    Once {
        #[arg(long, default_value = "config/demo.toml")]
        config: String,
    },
    /// Run the public-market-data paper loop. Zero iterations means run forever.
    Paper {
        #[arg(long, default_value = "config/demo.toml")]
        config: String,
        #[arg(long, default_value_t = 0)]
        iterations: u64,
    },
    /// Summarize a completed or still-running paper JSONL journal.
    Report {
        #[arg(long, default_value = "data/runtime/alpha-events.jsonl")]
        journal: String,
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

fn strategy_funnels_demo(
    _strategy: &StrategyConfig,
    evaluation: &GraphEvaluation,
    execution: &BinanceDemoExecution,
) -> serde_json::Value {
    let positions = execution.position_snapshots();
    let build = |lane: &str| {
        let is_lane = |recipe: &str| recipe == lane;
        let candidates: Vec<_> = evaluation
            .artifacts
            .values()
            .filter_map(|record| record.artifact.candidate())
            .filter(|candidate| is_lane(&candidate.recipe))
            .collect();
        let passed = candidates
            .iter()
            .filter(|candidate| candidate.verdict == Verdict::Pass)
            .count();
        let unknown = candidates
            .iter()
            .filter(|candidate| candidate.verdict == Verdict::Unknown)
            .count();
        let consumed = candidates
            .iter()
            .filter(|candidate| execution.has_seen(&candidate.id))
            .count();
        let performance_gated = candidates
            .iter()
            .filter(|candidate| {
                candidate.verdict == Verdict::Pass
                    && !execution
                        .candidate_gate_status(
                            &candidate.recipe,
                            candidate.side,
                            chrono::Utc::now().timestamp_millis(),
                        )
                        .allowed
            })
            .count();
        let plans = evaluation
            .artifacts
            .values()
            .filter_map(|record| match &record.artifact {
                Artifact::PositionPlan(value)
                    if evaluation
                        .artifacts
                        .values()
                        .filter_map(|record| record.artifact.candidate())
                        .find(|candidate| candidate.id == value.candidate_id)
                        .is_some_and(|candidate| is_lane(&candidate.recipe))
                        && !execution.has_seen(&value.candidate_id)
                        && evaluation
                            .artifacts
                            .values()
                            .filter_map(|record| record.artifact.candidate())
                            .find(|candidate| candidate.id == value.candidate_id)
                            .is_some_and(|candidate| {
                                execution
                                    .candidate_gate_status(
                                        &candidate.recipe,
                                        candidate.side,
                                        chrono::Utc::now().timestamp_millis(),
                                    )
                                    .allowed
                            }) =>
                {
                    Some(value)
                }
                _ => None,
            })
            .count();
        let open_positions = positions
            .values()
            .filter(|position| is_lane(&position.recipe))
            .count();
        let mut blocker_set = candidates
            .iter()
            .flat_map(|candidate| candidate.blockers.iter())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if performance_gated > 0 {
            blocker_set.insert("rolling PF gate is cooling down this recipe".into());
        }
        if consumed > 0 {
            blocker_set.insert("current opportunity cycle was already consumed".into());
        }
        let blockers: Vec<_> = blocker_set.into_iter().take(5).collect();
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
            "exchange_execution"
        };
        serde_json::json!({
            "lane": lane,
            "current_stage": current_stage,
            "candidate_counts": {"total":candidates.len(),"pass":passed,"unknown":unknown,"block":candidates.len()-passed-unknown},
            "plans": plans,
            "performance_gated": performance_gated,
            "consumed": consumed,
            "open_positions": open_positions,
            "blockers": blockers,
        })
    };
    serde_json::json!({
        "trend_continuation":build("trend_continuation")
    })
}

fn runtime_identity(config: &AppConfig, started_ms: i64) -> serde_json::Value {
    let config_bytes = serde_json::to_vec(config).unwrap_or_default();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in config_bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let previous = std::fs::read_to_string(&config.runtime.status_path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let (run_id, session_started_ms) = continued_session_identity(
        previous.as_ref(),
        config.strategy.risk.rolling_pf_epoch,
        started_ms,
    );
    serde_json::json!({
        "run_id": run_id,
        "started_ms": session_started_ms,
        "process_started_ms": started_ms,
        "version": env!("CARGO_PKG_VERSION"),
        "git_commit": git_commit(),
        "config_hash": format!("{hash:016x}"),
    })
}

fn continued_session_identity(
    previous: Option<&serde_json::Value>,
    performance_epoch: u32,
    process_started_ms: i64,
) -> (String, i64) {
    let reusable = previous.filter(|value| {
        value["execution"]["performance_epoch"].as_u64() == Some(u64::from(performance_epoch))
    });
    let previous_run_id = reusable
        .and_then(|value| value["runtime"]["run_id"].as_str())
        .filter(|value| !value.is_empty());
    let previous_started_ms = reusable
        .and_then(|value| value["runtime"]["started_ms"].as_i64())
        .filter(|value| *value > 0);
    match (previous_run_id, previous_started_ms) {
        (Some(run_id), Some(started_ms)) => (run_id.to_string(), started_ms),
        _ => (
            format!("{}-{}", process_started_ms, std::process::id()),
            process_started_ms,
        ),
    }
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

fn with_run_id(mut payload: serde_json::Value, identity: &serde_json::Value) -> serde_json::Value {
    if let (Some(object), Some(run_id)) = (payload.as_object_mut(), identity["run_id"].as_str()) {
        object.insert("run_id".into(), serde_json::Value::String(run_id.into()));
    }
    payload
}

async fn one_frame(
    config: &AppConfig,
    source: &mut BinanceMarketSource,
) -> Result<(greed_kernel::MarketFrame, GraphEvaluation)> {
    let mut strategy = config.strategy.clone();
    source.start_market_stream(&strategy).await?;
    if strategy.universe.dynamic_enabled {
        let discovery = source
            .discover_universe(&strategy, chrono::Utc::now().timestamp_millis())
            .await?;
        if !discovery.symbols.is_empty() {
            strategy.symbols = discovery.symbols;
        }
    }
    let frame = source
        .fetch_frame(
            &strategy,
            AccountFrame {
                equity_usd: config.portfolio.initial_equity_usd,
                cash_usd: config.portfolio.initial_equity_usd,
                realized_pnl_usd: 0.0,
                peak_equity_usd: config.portfolio.initial_equity_usd,
                risk_day_start_equity_usd: config.portfolio.initial_equity_usd,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        )
        .await?;
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
                "valid: {} seed symbols, {} funded alpha lane(s), paper_only=true, execution={:?}",
                config.strategy.symbols.len(),
                1,
                config.execution.mode,
            );
            Ok(())
        }
        Command::Once { config } => {
            let config = load(&config)?;
            let mut source = BinanceMarketSource::new(config.runtime.clone())?;
            let (_, evaluation) = one_frame(&config, &mut source).await?;
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
    }
}

async fn run_paper(config: AppConfig, iterations: u64) -> Result<()> {
    run_binance_demo(config, iterations).await
}

async fn run_binance_demo(config: AppConfig, iterations: u64) -> Result<()> {
    let started_ms = chrono::Utc::now().timestamp_millis();
    let identity = runtime_identity(&config, started_ms);
    let _monitor = monitor::start(&config.runtime).await?;
    let mut source = BinanceMarketSource::new(config.runtime.clone())?;
    if let Err(error) = source.start_market_stream(&config.strategy).await {
        // The stream hub owns reconnect loops. A cold-start timeout must halt
        // trading, but it must not kill the monitoring API or systemd service;
        // subsequent paper frames keep retrying until fresh market data exists.
        warn!(error=%error, "Binance market websocket is not warm; keeping runtime alive and blocking orders until reconnect");
    }
    let mut execution = BinanceDemoExecution::connect(
        config.execution.clone(),
        config.portfolio.clone(),
        config.strategy.risk.clone(),
        config.runtime.execution_state_path.clone(),
        config.runtime.proxy.as_deref(),
        source.market_stream_handle(),
    )
    .await
    .context("Binance demo execution initialization failed; trading runtime cannot start")?;
    let journal = Journal::new(&config.runtime.journal_path)?;
    let history = Journal::new(&config.runtime.history_path)?;
    let research_capacity_bytes = config
        .runtime
        .research_file_max_mb
        .saturating_mul(1024 * 1024)
        .saturating_mul(config.runtime.research_rotations as u64 + 1);
    let research_journal = if config.runtime.research_enabled {
        Some(Journal::bounded(
            &config.runtime.research_path,
            config.runtime.research_file_max_mb * 1024 * 1024,
            config.runtime.research_rotations,
        )?)
    } else {
        None
    };
    let mut research_samples = ResearchRecorder::new(
        config.runtime.research_path.clone(),
        config.runtime.research_snapshot_seconds,
        config.runtime.research_level_map_seconds,
        BTreeMap::from([
            (60_000, config.runtime.research_backfill_1m_bars),
            (300_000, config.runtime.research_backfill_5m_bars),
            (900_000, config.runtime.research_backfill_15m_bars),
            (3_600_000, config.runtime.research_backfill_1h_bars),
        ]),
        research_capacity_bytes,
    );
    let status = StatusWriter::new(&config.runtime.status_path);
    let start_payload = serde_json::json!({
        "paper_only": true,
        "execution": execution.health(),
        "config": config,
        "runtime": identity,
    });
    journal.append("runner_start", start_payload.clone())?;
    history.append("runner_start", start_payload)?;
    if let Some(research_journal) = &research_journal {
        research_journal.append(
            "research_session_start",
            serde_json::json!({
                "started_ms":started_ms,
                "runtime":identity,
                "snapshot_seconds":config.runtime.research_snapshot_seconds,
                "level_map_seconds":config.runtime.research_level_map_seconds,
                "backfill_bars":{
                    "1m":config.runtime.research_backfill_1m_bars,
                    "5m":config.runtime.research_backfill_5m_bars,
                    "15m":config.runtime.research_backfill_15m_bars,
                    "1h":config.runtime.research_backfill_1h_bars,
                },
                "forward_horizons_ms":[10000,30000,60000,180000,300000,900000],
            }),
        )?;
    }
    let mut active_strategy = config.strategy.clone();
    let mut graph = build_graph(&active_strategy)?;
    let mut last_universe_refresh_ms = 0i64;
    let mut universe_initialized = false;
    let mut universe_misses = BTreeMap::new();
    let mut universe_status = serde_json::json!({
        "as_of_ms": started_ms,
        "symbols": active_strategy.symbols.clone(),
        "dynamic": active_strategy.universe.dynamic_enabled,
    });
    let mut samples = SampleRecorder::default();
    let mut completed = 0u64;
    loop {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let sync_events = match execution.sync().await {
            Ok(events) => events,
            Err(error) => {
                warn!(error=%error, "Binance demo account sync failed; blocking evaluation and orders");
                journal.append(
                    "exchange_sync_error",
                    serde_json::json!({"ts_ms":now_ms,"error":error.to_string(),"venue":"binance_demo"}),
                )?;
                tokio::time::sleep(Duration::from_secs(config.runtime.poll_seconds)).await;
                continue;
            }
        };
        for event in sync_events {
            let payload = with_run_id(event.payload, &identity);
            history.append(&event.kind, payload.clone())?;
            journal.append(&event.kind, payload)?;
        }
        let refresh_ms = i64::from(config.strategy.universe.refresh_seconds) * 1_000;
        if config.strategy.universe.dynamic_enabled
            && now_ms - last_universe_refresh_ms >= refresh_ms
        {
            last_universe_refresh_ms = now_ms;
            // Mainnet market discovery can contain newly listed contracts that
            // the Demo venue does not expose yet. Request a ranked reserve so
            // filtering against Demo exchangeInfo can still fill all slots.
            let mut discovery_strategy = config.strategy.clone();
            discovery_strategy.universe.max_symbols = config
                .strategy
                .universe
                .max_symbols
                .saturating_add(10)
                .min(50);
            match source.discover_universe(&discovery_strategy, now_ms).await {
                Ok(mut discovery) => {
                    let position_symbols: Vec<_> = execution.position_symbols().cloned().collect();
                    discovery.symbols.retain(|symbol| {
                        execution.supports_symbol(symbol) && !position_symbols.contains(symbol)
                    });
                    discovery.symbols.truncate(
                        config
                            .strategy
                            .universe
                            .max_symbols
                            .saturating_sub(position_symbols.len()),
                    );
                    discovery.symbols.extend(position_symbols.iter().cloned());
                    if universe_initialized {
                        discovery.symbols = stabilize_universe(
                            &active_strategy.symbols,
                            &discovery.symbols,
                            &position_symbols,
                            config.strategy.universe.max_symbols,
                            &mut universe_misses,
                        );
                    } else {
                        universe_initialized = true;
                    }
                    discovery.symbols.sort();
                    if !discovery.symbols.is_empty() && discovery.symbols != active_strategy.symbols
                    {
                        active_strategy.symbols.clone_from(&discovery.symbols);
                        graph = build_graph(&active_strategy)?;
                        source.set_stream_symbols(&active_strategy);
                        // Existing websocket sessions apply incremental
                        // SUBSCRIBE/UNSUBSCRIBE changes. Allow newly admitted
                        // symbols several 500ms depth snapshots before their
                        // first evaluation; retained symbols remain live.
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                    universe_status = serde_json::to_value(&discovery)?;
                    journal.append("universe_refresh", universe_status.clone())?;
                }
                Err(error) => journal.append(
                    "universe_refresh_error",
                    serde_json::json!({"ts_ms":now_ms,"error":error.to_string()}),
                )?,
            }
        }
        let account = execution.account_frame()?;
        match source.fetch_frame(&active_strategy, account).await {
            Ok(mut frame) => {
                samples.record(&journal, &frame)?;
                let data_health = source.health();
                journal.append("data_health", data_health.clone())?;
                if let Some(research_journal) = &research_journal {
                    research_samples.record(research_journal, &frame)?;
                    research_samples.record_health(
                        research_journal,
                        frame.as_of_ms,
                        data_health.clone(),
                    )?;
                }
                frame.account = execution.account_frame()?;
                let evaluation = graph.evaluate(&frame)?;
                journal.append("graph_evaluation", serde_json::to_value(&evaluation)?)?;
                let order_events = execution.apply_plans(&frame, &evaluation).await;
                for event in &order_events {
                    let payload = with_run_id(event.payload.clone(), &identity);
                    history.append(&event.kind, payload.clone())?;
                    journal.append(&event.kind, payload)?;
                }
                if !order_events.is_empty() {
                    for event in execution.sync().await? {
                        let payload = with_run_id(event.payload, &identity);
                        history.append(&event.kind, payload.clone())?;
                        journal.append(&event.kind, payload)?;
                    }
                }
                let account = execution.account_frame()?;
                let funnels = strategy_funnels_demo(&active_strategy, &evaluation, &execution);
                let execution_health = execution.health();
                let observation = serde_json::json!({
                    "ts_ms":frame.as_of_ms,
                    "equity_usd":account.equity_usd,
                    "cash_usd":account.cash_usd,
                    "realized_pnl_usd":account.realized_pnl_usd,
                    "gross_exposure_usd":account.gross_exposure_usd,
                    "drawdown_pct":((account.peak_equity_usd-account.equity_usd)/account.peak_equity_usd.max(1.0)).max(0.0),
                    "daily_loss_pct":((account.risk_day_start_equity_usd-account.equity_usd)/account.risk_day_start_equity_usd.max(1.0)).max(0.0),
                    "execution":execution_health,
                    "runtime":identity,
                });
                history.append("exchange_equity", observation.clone())?;
                journal.append("exchange_equity", observation)?;
                status.write(&serde_json::json!({
                    "as_of_ms":frame.as_of_ms,
                    "paper_only":true,
                    "account":account,
                    "lanes":funnels,
                    "recipe_gates":execution.recipe_gate_snapshots(frame.as_of_ms),
                    "positions":execution.position_snapshots(),
                    "graph":summarize(&evaluation),
                    "artifacts":evaluation.artifacts,
                    "universe":universe_status,
                    "data_health":data_health,
                    "research":research_journal.as_ref().map(|journal|research_samples.status(journal)),
                    "execution":execution_health,
                    "runtime":identity,
                }))?;
                completed += 1;
                info!(
                    iteration = completed,
                    equity = account.equity_usd,
                    "Binance demo frame complete"
                );
            }
            Err(error) => {
                warn!(error=%error,"market frame failed; no Binance demo orders submitted");
                journal.append(
                    "frame_error",
                    serde_json::json!({"error":error.to_string()}),
                )?;
            }
        }
        if iterations > 0 && completed >= iterations {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(config.runtime.poll_seconds)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_universe_requires_repeated_misses_and_limits_churn() {
        let current = vec!["BTCUSDT", "ETHUSDT", "OLD1USDT", "OLD2USDT"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let desired = vec!["BTCUSDT", "ETHUSDT", "NEW1USDT", "NEW2USDT"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let pinned = vec!["BTCUSDT".to_string()];
        let mut misses = BTreeMap::new();

        for _ in 0..UNIVERSE_MISS_THRESHOLD - 1 {
            let stable = stabilize_universe(&current, &desired, &pinned, 4, &mut misses);
            assert!(stable.contains(&"OLD1USDT".to_string()));
            assert!(stable.contains(&"OLD2USDT".to_string()));
        }
        let stable = stabilize_universe(&current, &desired, &pinned, 4, &mut misses);
        assert_eq!(stable.len(), 4);
        assert!(stable.contains(&"BTCUSDT".to_string()));
        assert!(stable.contains(&"NEW1USDT".to_string()));
        assert!(stable.contains(&"NEW2USDT".to_string()));
    }

    #[test]
    fn deployment_restart_continues_the_same_performance_session() {
        let previous = serde_json::json!({
            "execution":{"performance_epoch":13},
            "runtime":{"run_id":"paper-session-1","started_ms":1_000}
        });
        assert_eq!(
            continued_session_identity(Some(&previous), 13, 2_000),
            ("paper-session-1".to_string(), 1_000)
        );
        let reset = continued_session_identity(Some(&previous), 14, 2_000);
        assert_ne!(reset.0, "paper-session-1");
        assert_eq!(reset.1, 2_000);
    }
}
