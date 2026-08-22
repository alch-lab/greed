mod backtest;
mod broker;
mod config;
mod journal;
mod report;
mod source;

use anyhow::{Context, Result};
use broker::PaperBroker;
use clap::{Parser, Subcommand};
use config::AppConfig;
use greed_kernel::{Artifact, GraphEvaluation};
use greed_strategy::build_graph;
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

async fn one_frame(
    config: &AppConfig,
    source: &mut BinancePaperSource,
    broker: &PaperBroker,
) -> Result<(greed_kernel::MarketFrame, GraphEvaluation)> {
    let mut frame = source
        .fetch_frame(&config.strategy, broker.account_frame())
        .await?;
    frame.account = broker.marked_account(&frame);
    let mut graph = build_graph(&config.strategy)?;
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
            let broker = PaperBroker::new(config.paper.clone());
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
    let mut source = BinancePaperSource::new(config.runtime.clone())?;
    let mut broker =
        PaperBroker::load_or_new(config.paper.clone(), &config.runtime.paper_state_path)?;
    let journal = Journal::new(&config.runtime.journal_path)?;
    let status = StatusWriter::new(&config.runtime.status_path);
    journal.append(
        "runner_start",
        serde_json::json!({"paper_only":true,"config":config}),
    )?;
    let mut graph = build_graph(&config.strategy)?;
    let mut samples = SampleRecorder::default();
    let mut completed = 0u64;
    loop {
        match source
            .fetch_frame(&config.strategy, broker.account_frame())
            .await
        {
            Ok(mut frame) => {
                samples.record(&journal, &frame)?;
                journal.append("data_health", source.health())?;
                let broker_events = broker.mark_to_market(&frame);
                for event in broker_events {
                    journal.append(&event.kind, event.payload)?;
                }
                frame.account = broker.marked_account(&frame);
                let evaluation = graph.evaluate(&frame)?;
                journal.append("graph_evaluation", serde_json::to_value(&evaluation)?)?;
                let fill_events = broker.apply_plans(&frame, &evaluation);
                for event in fill_events {
                    journal.append(&event.kind, event.payload)?;
                }
                broker.save(&config.runtime.paper_state_path)?;
                status.write(&serde_json::json!({"as_of_ms":frame.as_of_ms,"paper_only":true,"account":broker.marked_account(&frame),"positions":broker.positions(),"graph":summarize(&evaluation),"artifacts":evaluation.artifacts,"data_health":source.health()}))?;
                completed += 1;
                info!(
                    iteration = completed,
                    equity = broker.account_frame().equity_usd,
                    "paper frame complete"
                );
            }
            Err(error) => {
                warn!(error=%error,"paper frame failed; no strategy evaluation or order simulation performed");
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
