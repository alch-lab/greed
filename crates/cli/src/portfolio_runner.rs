//! 单账户组合运行器。
//!
//! BTC MR 与山寨币策略共用一套 Binance Futures 凭证，但使用互斥标的集合、
//! 独立虚拟本金、独立持仓状态和独立 Journal。组合层只负责生命周期、账户资金
//! 预检和状态聚合，不跨策略净额化仓位，也不让任一策略使用另一 sleeve 的利润。

use anyhow::{Context, Result};
use data::live::config::AccountConfig;
use data::live::CollectorConfig;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::altcoin_runner::{is_altcoin_strategy, run_altcoin_impulse};
use crate::trade_runner::{run_trade, TradeArgs, TradeMode};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PortfolioConfig {
    pub enabled: bool,
    #[serde(default)]
    pub allow_live: bool,
    pub mr_strategy: String,
    pub altcoin_strategy: String,
    pub mr_capital_usdt: f64,
    pub altcoin_capital_usdt: f64,
    #[serde(default = "default_mr_risk_pct")]
    pub mr_risk_pct: f64,
    #[serde(default = "default_mr_max_risk_pct")]
    pub mr_max_risk_pct: f64,
    #[serde(default = "default_mr_leverage")]
    pub mr_leverage: u32,
    #[serde(default = "default_mr_cb_max_daily_losses")]
    pub mr_cb_max_daily_losses: u32,
    #[serde(default = "default_mr_cb_daily_dd_pct")]
    pub mr_cb_daily_dd_pct: f64,
}

#[derive(Debug, Deserialize)]
struct PortfolioFile {
    portfolio: PortfolioConfig,
}

fn default_mr_risk_pct() -> f64 {
    0.0075
}

fn default_mr_max_risk_pct() -> f64 {
    0.0075
}

fn default_mr_leverage() -> u32 {
    3
}

fn default_mr_cb_max_daily_losses() -> u32 {
    2
}

fn default_mr_cb_daily_dd_pct() -> f64 {
    0.02
}

pub fn is_portfolio_strategy(path: &str) -> bool {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| toml::from_str::<PortfolioFile>(&text).ok())
        .is_some_and(|file| file.portfolio.enabled)
}

fn load_portfolio(path: &str) -> Result<PortfolioConfig> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("读取组合配置失败: {path}"))?;
    let file: PortfolioFile =
        toml::from_str(&text).with_context(|| format!("解析组合配置失败: {path}"))?;
    let cfg = file.portfolio;
    anyhow::ensure!(cfg.enabled, "组合配置未启用");
    anyhow::ensure!(
        cfg.mr_capital_usdt > 0.0 && cfg.altcoin_capital_usdt > 0.0,
        "MR 与山寨币虚拟本金必须大于 0"
    );
    anyhow::ensure!(
        cfg.mr_risk_pct > 0.0 && cfg.mr_risk_pct <= cfg.mr_max_risk_pct,
        "MR 风险参数要求 0 < risk_pct <= max_risk_pct"
    );
    anyhow::ensure!((1..=20).contains(&cfg.mr_leverage), "MR 杠杆必须为 1..=20");
    anyhow::ensure!(
        cfg.mr_cb_max_daily_losses > 0 && (0.005..=0.05).contains(&cfg.mr_cb_daily_dd_pct),
        "MR 主网风控要求连续亏损熔断开启，且日内回撤限制在 0.5%..=5%"
    );
    anyhow::ensure!(
        !is_altcoin_strategy(&cfg.mr_strategy),
        "MR 策略配置不能指向山寨币策略"
    );
    anyhow::ensure!(
        is_altcoin_strategy(&cfg.altcoin_strategy),
        "altcoin_strategy 必须是有效山寨币策略配置"
    );
    let altcoin_text = std::fs::read_to_string(&cfg.altcoin_strategy)
        .with_context(|| format!("读取山寨币配置失败: {}", cfg.altcoin_strategy))?;
    let altcoin_doc: toml::Value = toml::from_str(&altcoin_text)
        .with_context(|| format!("解析山寨币配置失败: {}", cfg.altcoin_strategy))?;
    let configured_altcoin_capital = altcoin_doc
        .get("altcoin_impulse")
        .and_then(|value| value.get("capital_usdt"))
        .and_then(toml::Value::as_float)
        .unwrap_or(0.0);
    anyhow::ensure!(
        (configured_altcoin_capital - cfg.altcoin_capital_usdt).abs() < 1e-6,
        "组合配置的 altcoin_capital_usdt 必须与山寨币配置 capital_usdt 一致"
    );
    Ok(cfg)
}

fn component_error_is_fatal(message: &str) -> bool {
    const FATAL: &[&str] = &[
        "-2015",
        "Invalid API-key",
        "缺少 API 凭证",
        "启动检查失败",
        "读取配置失败",
        "读策略配置失败",
        "解析配置",
        "装配策略失败",
        "账户存在不属于",
    ];
    FATAL.iter().any(|marker| message.contains(marker))
}

fn component_restart_status(
    component: &str,
    mode: TradeMode,
    restart_count: u32,
    retry_in_s: u64,
    message: &str,
) -> Value {
    json!({
        "state":"restarting",
        "mode":mode.as_str(),
        "component":component,
        "restart_count":restart_count,
        "retry_in_s":retry_in_s,
        "error":message
    })
}

async fn supervise_mr(
    args: TradeArgs,
    mut shutdown: watch::Receiver<bool>,
    status_tx: watch::Sender<Value>,
) -> Result<()> {
    let mut restart_count = 0u32;
    let mut backoff_s = 2u64;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match run_trade(args.clone(), shutdown.clone(), Some(status_tx.clone())).await {
            Ok(()) => return Ok(()),
            Err(cause) => {
                let message = format!("{cause:#}");
                if component_error_is_fatal(&message) {
                    error!(component = "mr", error = %message, "组合子策略致命错误");
                    return Err(cause);
                }
                restart_count += 1;
                warn!(component = "mr", restart_count, backoff_s, error = %message, "组合子策略异常，独立重启");
                let _ = status_tx.send(component_restart_status(
                    "mr",
                    args.mode.unwrap_or(TradeMode::Paper),
                    restart_count,
                    backoff_s,
                    &message,
                ));
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_s)) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                }
                backoff_s = (backoff_s * 2).min(300);
            }
        }
    }
}

async fn supervise_altcoin(
    args: TradeArgs,
    mut shutdown: watch::Receiver<bool>,
    status_tx: watch::Sender<Value>,
    daily_risk_reset: Option<watch::Receiver<u64>>,
    daily_entry_bonus: Option<watch::Receiver<u64>>,
) -> Result<()> {
    let mut restart_count = 0u32;
    let mut backoff_s = 2u64;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match run_altcoin_impulse(
            args.clone(),
            shutdown.clone(),
            Some(status_tx.clone()),
            daily_risk_reset.clone(),
            daily_entry_bonus.clone(),
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(cause) => {
                let message = format!("{cause:#}");
                if component_error_is_fatal(&message) {
                    error!(component = "altcoin", error = %message, "组合子策略致命错误");
                    return Err(cause);
                }
                restart_count += 1;
                warn!(component = "altcoin", restart_count, backoff_s, error = %message, "组合子策略异常，独立重启");
                let _ = status_tx.send(component_restart_status(
                    "altcoin",
                    args.mode.unwrap_or(TradeMode::Paper),
                    restart_count,
                    backoff_s,
                    &message,
                ));
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_s)) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                }
                backoff_s = (backoff_s * 2).min(300);
            }
        }
    }
}

fn nested_number(value: &Value, key: &str, fallback: f64) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(fallback)
}

fn aggregate_status(
    mode: TradeMode,
    strategy_name: &str,
    started_ms: i64,
    account_wallet: f64,
    cfg: &PortfolioConfig,
    mr: &Value,
    altcoin: &Value,
) -> Value {
    let mr_state = mr["state"].as_str().unwrap_or("starting");
    let altcoin_state = altcoin["state"].as_str().unwrap_or("starting");
    let state = if mr_state == "running" && altcoin_state == "running" {
        "running"
    } else if mr_state == "restarting" || altcoin_state == "restarting" {
        "restarting"
    } else {
        "starting"
    };
    let mr_equity = nested_number(mr, "equity", cfg.mr_capital_usdt);
    let altcoin_equity = nested_number(altcoin, "equity", cfg.altcoin_capital_usdt);
    let mr_cash = nested_number(mr, "cash", cfg.mr_capital_usdt);
    let altcoin_cash = nested_number(altcoin, "cash", cfg.altcoin_capital_usdt);
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mr_healthy = mr["execution_healthy"].as_bool().unwrap_or(true);
    let altcoin_healthy = altcoin["execution_healthy"].as_bool().unwrap_or(true);
    let error = [
        ("MR", mr.get("error").and_then(Value::as_str)),
        ("山寨币", altcoin.get("error").and_then(Value::as_str)),
    ]
    .into_iter()
    .filter_map(|(name, message)| message.map(|message| format!("{name}: {message}")))
    .collect::<Vec<_>>()
    .join("；");
    let restart_count = [mr, altcoin]
        .into_iter()
        .filter(|component| component["state"] == "restarting")
        .filter_map(|component| component["restart_count"].as_u64())
        .max();
    let retry_in_s = [mr, altcoin]
        .into_iter()
        .filter(|component| component["state"] == "restarting")
        .filter_map(|component| component["retry_in_s"].as_u64())
        .min();
    let btc_completed = mr["performance"]["completed_trades"].as_u64().unwrap_or(0);
    let btc_wins = mr["performance"]["wins"].as_u64().unwrap_or(0);
    let alt_completed = altcoin["altcoin_impulse"]["total_exits"]
        .as_u64()
        .unwrap_or(0);
    let alt_wins = altcoin["altcoin_impulse"]["wins"].as_u64().unwrap_or(0);
    let completed_trades = btc_completed + alt_completed;
    let wins = btc_wins + alt_wins;
    let mr_risk = mr.get("risk_control").cloned().unwrap_or_else(|| {
        json!({
            "halted":false,"reason_codes":[],"day_start_equity":0.0,
            "current_equity":mr_equity,"daily_drawdown_pct":0.0,
            "daily_drawdown_limit_pct":cfg.mr_cb_daily_dd_pct,
            "consecutive_losses":0,"consecutive_loss_limit":cfg.mr_cb_max_daily_losses
        })
    });
    let alt = &altcoin["altcoin_impulse"];
    let alt_daily_halted = alt["daily_loss_blocked"].as_bool().unwrap_or(false);
    let alt_first_week_halted = alt["first_week"]["entries_blocked"]
        .as_bool()
        .unwrap_or(false);
    let mut alt_reason_codes = Vec::new();
    if alt_daily_halted {
        alt_reason_codes.push("daily_drawdown");
    }
    if alt_first_week_halted {
        alt_reason_codes.push("first_week_loss");
    }
    let alt_risk = json!({
        "halted":alt_daily_halted || alt_first_week_halted,
        "reason_codes":alt_reason_codes,
        "day_start_equity":alt["daily_risk_baseline_equity"].as_f64().unwrap_or(0.0),
        "current_equity":altcoin_equity,
        "daily_drawdown_limit_pct":alt["daily_loss_limit"].as_f64().unwrap_or(0.0),
        "first_week":alt.get("first_week").cloned().unwrap_or(Value::Null)
    });
    let risk_control = json!({
        "halted":mr_risk["halted"].as_bool().unwrap_or(false)
            || alt_risk["halted"].as_bool().unwrap_or(false),
        "btc":mr_risk,
        "altcoin":alt_risk
    });
    json!({
        "state":state,
        "mode":mode.as_str(),
        "portfolio_mode":true,
        "strategy_name":strategy_name,
        "started_at_ms":started_ms,
        "uptime_s":(now_ms-started_ms)/1000,
        "equity":mr_equity+altcoin_equity,
        "cash":mr_cash+altcoin_cash,
        "n_intents":mr["n_intents"].as_u64().unwrap_or(0)+altcoin["n_intents"].as_u64().unwrap_or(0),
        "n_fills":mr["n_fills"].as_u64().unwrap_or(0)+altcoin["n_fills"].as_u64().unwrap_or(0),
        "execution_healthy":mr_healthy && altcoin_healthy,
        "restart_count":restart_count,
        "retry_in_s":retry_in_s,
        "error":if error.is_empty() {Value::Null} else {Value::String(error)},
        "portfolio":{
            "account_wallet_usdt":account_wallet,
            "initial_capital_usdt":cfg.mr_capital_usdt+cfg.altcoin_capital_usdt,
            "combined_equity":mr_equity+altcoin_equity,
            "combined_pnl":mr_equity+altcoin_equity-cfg.mr_capital_usdt-cfg.altcoin_capital_usdt,
            "mr_capital_usdt":cfg.mr_capital_usdt,
            "altcoin_capital_usdt":cfg.altcoin_capital_usdt,
            "risk_control":risk_control,
            "performance":{
                "completed_trades":completed_trades,
                "wins":wins,
                "win_rate":if completed_trades > 0 {
                    Value::from(wins as f64 / completed_trades as f64)
                } else {
                    Value::Null
                },
                "btc_completed_trades":btc_completed,
                "btc_wins":btc_wins,
                "altcoin_completed_trades":alt_completed,
                "altcoin_wins":alt_wins
            },
            "mr":mr,
            "altcoin":altcoin,
            "component_states":{"mr":mr_state,"altcoin":altcoin_state},
            "journal_paths":{
                "mr":format!("data/journal/portfolio-mr-{}.json",mode.as_str()),
                "altcoin":format!("data/journal/portfolio-altcoin-{}.jsonl",mode.as_str())
            }
        }
    })
}

async fn preflight_account(
    args: &TradeArgs,
    cfg: &PortfolioConfig,
    mode: TradeMode,
) -> Result<f64> {
    if mode == TradeMode::Dry {
        return Ok(cfg.mr_capital_usdt + cfg.altcoin_capital_usdt);
    }
    let base_text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("读取配置失败: {}", args.config))?;
    let collector = CollectorConfig::from_toml_str(&base_text)
        .with_context(|| format!("解析配置 [collector] 失败: {}", args.config))?;
    let mut account = AccountConfig::from_toml_str(&base_text)
        .with_context(|| format!("解析配置 [account] 失败: {}", args.config))?;
    match mode {
        TradeMode::Paper => account.use_standard_environment(true),
        TradeMode::Live => account.use_standard_environment(false),
        TradeMode::Dry => {}
    }
    let (key, secret) = match (account.api_key(), account.api_secret()) {
        (Some(key), Some(secret)) => (key, secret),
        _ => anyhow::bail!("缺少 Binance Futures API 凭证"),
    };
    let http = data::live::build_http_client(collector.effective_proxy().as_deref());
    let client = live::RestClient::new(http, account.rest_base(), key, secret);
    client.sync_time().await?;
    anyhow::ensure!(
        !client.position_mode_is_hedged().await?,
        "组合策略只支持币安单向持仓模式（One-way Mode）；请先关闭 Hedge Mode"
    );
    let wallet = client.wallet_balance_usdt().await?;
    let required = cfg.mr_capital_usdt + cfg.altcoin_capital_usdt;
    anyhow::ensure!(
        wallet + 1e-6 >= required,
        "组合需要至少 {required:.2} USDT 账户钱包余额，当前只有 {wallet:.2} USDT"
    );
    let mr_journal = format!("data/journal/portfolio-mr-{}.json", mode.as_str());
    let altcoin_state = format!(
        "data/journal/portfolio-altcoin-{}.state.json",
        mode.as_str()
    );
    if !std::path::Path::new(&mr_journal).exists() && !std::path::Path::new(&altcoin_state).exists()
    {
        let positions = client.open_position_amounts().await?;
        anyhow::ensure!(
            positions.is_empty(),
            "组合首次启动要求 Binance 账户空仓，当前仍有持仓 {positions:?}；请先平仓/重置模拟账户"
        );
        let open_orders = client.open_order_symbols().await?;
        anyhow::ensure!(
            open_orders.is_empty(),
            "组合首次启动要求 Binance 账户无挂单，当前仍有挂单 {open_orders:?}；请先撤销全部普通单和条件单"
        );
    }
    if mode == TradeMode::Live {
        anyhow::ensure!(
            account.rest_base() == "https://fapi.binance.com"
                && account.ws_base() == "wss://fstream.binance.com/market",
            "实盘端点校验失败，拒绝启动"
        );
        let filters = client.symbol_filters("BTCUSDT").await?;
        let (mark, _) = client.premium_index("BTCUSDT").await?;
        let test_qty = (filters.min_notional * 2.0 / mark).max(filters.market_step_size * 2.0);
        client
            .test_market_order_permission("BTCUSDT", test_qty, &filters)
            .await
            .context("实盘 API TRADE 权限或订单参数预检失败")?;
    }
    Ok(wallet)
}

pub async fn run_portfolio(
    args: TradeArgs,
    mut shutdown: watch::Receiver<bool>,
    status_tx: Option<watch::Sender<Value>>,
    daily_risk_reset: Option<watch::Receiver<u64>>,
    daily_entry_bonus: Option<watch::Receiver<u64>>,
) -> Result<()> {
    let cfg = load_portfolio(&args.strategy)?;
    let mode = args.mode.unwrap_or(TradeMode::Paper);
    anyhow::ensure!(
        mode != TradeMode::Live || cfg.allow_live,
        "组合策略默认禁止实盘；只允许模拟盘或 dry"
    );
    let account_wallet = preflight_account(&args, &cfg, mode).await?;
    let started_ms = chrono::Utc::now().timestamp_millis();
    let mut mr_args = args.clone();
    mr_args.strategy = cfg.mr_strategy.clone();
    mr_args.journal = Some(format!("data/journal/portfolio-mr-{}.json", mode.as_str()));
    mr_args.cash = cfg.mr_capital_usdt;
    mr_args.risk_pct = cfg.mr_risk_pct;
    mr_args.max_risk_pct = cfg.mr_max_risk_pct;
    mr_args.leverage = cfg.mr_leverage;
    mr_args.cb_max_daily_losses = cfg.mr_cb_max_daily_losses;
    mr_args.cb_daily_dd_pct = cfg.mr_cb_daily_dd_pct;
    mr_args.portfolio_mode = true;

    let mut altcoin_args = args.clone();
    altcoin_args.strategy = cfg.altcoin_strategy.clone();
    altcoin_args.journal = Some(format!(
        "data/journal/portfolio-altcoin-{}.jsonl",
        mode.as_str()
    ));
    altcoin_args.cash = cfg.altcoin_capital_usdt;
    altcoin_args.portfolio_mode = true;

    let (child_shutdown_tx, child_shutdown_rx) = watch::channel(false);
    let (mr_status_tx, mr_status_rx) = watch::channel(json!({
        "state":"starting","mode":mode.as_str(),"strategy_name":cfg.mr_strategy
    }));
    let (altcoin_status_tx, altcoin_status_rx) = watch::channel(json!({
        "state":"starting","mode":mode.as_str(),"strategy_name":cfg.altcoin_strategy
    }));
    let mut mr_task = tokio::spawn(supervise_mr(
        mr_args,
        child_shutdown_rx.clone(),
        mr_status_tx,
    ));
    let mut altcoin_task = tokio::spawn(supervise_altcoin(
        altcoin_args,
        child_shutdown_rx,
        altcoin_status_tx,
        daily_risk_reset,
        daily_entry_bonus,
    ));
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    info!(
        mode = mode.as_str(),
        account_wallet,
        mr_capital = cfg.mr_capital_usdt,
        altcoin_capital = cfg.altcoin_capital_usdt,
        "单账户双策略组合已启动"
    );

    loop {
        tokio::select! {
            _ = timer.tick() => {
                if let Some(tx) = &status_tx {
                    let status = aggregate_status(
                        mode,
                        &args.strategy,
                        started_ms,
                        account_wallet,
                        &cfg,
                        &mr_status_rx.borrow().clone(),
                        &altcoin_status_rx.borrow().clone(),
                    );
                    let _ = tx.send(status);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = child_shutdown_tx.send(true);
                    let mr_result = mr_task.await.context("MR 子任务 join 失败")?;
                    let altcoin_result = altcoin_task.await.context("山寨币子任务 join 失败")?;
                    mr_result?;
                    altcoin_result?;
                    return Ok(());
                }
            }
            result = &mut mr_task => {
                let _ = child_shutdown_tx.send(true);
                let mr_result = result.context("MR 子任务 join 失败")?;
                let altcoin_result = altcoin_task.await.context("山寨币子任务 join 失败")?;
                mr_result?;
                altcoin_result?;
                return Ok(());
            }
            result = &mut altcoin_task => {
                let _ = child_shutdown_tx.send(true);
                let altcoin_result = result.context("山寨币子任务 join 失败")?;
                let mr_result = mr_task.await.context("MR 子任务 join 失败")?;
                altcoin_result?;
                mr_result?;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployed_config() -> PortfolioConfig {
        toml::from_str::<PortfolioFile>(include_str!("../../../config/strategy-portfolio.toml"))
            .unwrap()
            .portfolio
    }

    #[test]
    fn deployed_portfolio_has_two_fifteen_hundred_usdt_sleeves() {
        let cfg = deployed_config();
        assert_eq!(cfg.mr_capital_usdt, 1_500.0);
        assert_eq!(cfg.altcoin_capital_usdt, 1_500.0);
        assert_eq!(cfg.mr_risk_pct, 0.0075);
        assert_eq!(cfg.mr_max_risk_pct, 0.0075);
        assert_eq!(cfg.mr_leverage, 3);
        assert_eq!(cfg.mr_cb_max_daily_losses, 2);
        assert_eq!(cfg.mr_cb_daily_dd_pct, 0.02);
        assert_eq!(cfg.mr_strategy, "config/strategy-final.toml");
        assert_eq!(cfg.altcoin_strategy, "config/strategy-altcoin-impulse.toml");
        assert!(cfg.allow_live);
    }

    #[test]
    fn deployed_btc_sleeve_keeps_negative_mr_observation_only() {
        let doc: toml::Value =
            toml::from_str(include_str!("../../../config/strategy-final.toml")).unwrap();
        let hybrid = &doc["strategy"]["plugins"]["HybridEntry"];
        assert_eq!(hybrid["mr_enabled"].as_bool(), Some(false));
        assert_eq!(hybrid["trend_enabled"].as_bool(), Some(true));
        assert_eq!(hybrid["tactical_enabled"].as_bool(), Some(true));
    }

    #[test]
    fn aggregate_keeps_sleeve_equity_separate() {
        let cfg = deployed_config();
        let mr = json!({
            "state":"running","equity":1_525.0,"cash":1_520.0,"execution_healthy":true,
            "performance":{"completed_trades":3,"wins":2}
        });
        let altcoin = json!({
            "state":"running","equity":1_480.0,"cash":1_475.0,"execution_healthy":true,
            "altcoin_impulse":{"total_exits":7,"wins":4}
        });
        let status = aggregate_status(
            TradeMode::Paper,
            "config/strategy-portfolio.toml",
            1,
            3_000.0,
            &cfg,
            &mr,
            &altcoin,
        );
        assert_eq!(status["equity"], 3_005.0);
        assert_eq!(status["portfolio"]["combined_pnl"], 5.0);
        assert_eq!(status["portfolio"]["mr"]["equity"], 1_525.0);
        assert_eq!(status["portfolio"]["altcoin"]["equity"], 1_480.0);
        assert_eq!(status["portfolio"]["performance"]["completed_trades"], 10);
        assert_eq!(status["portfolio"]["performance"]["wins"], 6);
        assert_eq!(status["portfolio"]["performance"]["win_rate"], 0.6);
        assert_eq!(status["portfolio"]["risk_control"]["halted"], false);
    }

    #[test]
    fn aggregate_exposes_component_risk_halts_and_reasons() {
        let cfg = deployed_config();
        let mr = json!({
            "state":"running","equity":1_470.0,"cash":1_470.0,
            "risk_control":{
                "halted":true,"reason_codes":["daily_drawdown"],
                "day_start_equity":1_500.0,"current_equity":1_470.0,
                "daily_drawdown_pct":0.02,"daily_drawdown_limit_pct":0.02,
                "consecutive_losses":1,"consecutive_loss_limit":2
            }
        });
        let altcoin = json!({
            "state":"running","equity":1_460.0,"cash":1_460.0,
            "altcoin_impulse":{
                "daily_loss_blocked":false,"daily_loss_limit":0.025,
                "daily_risk_baseline_equity":1_500.0,
                "first_week":{"entries_blocked":true,"loss_limit_pct":0.05}
            }
        });
        let status = aggregate_status(
            TradeMode::Live,
            "config/strategy-portfolio.toml",
            1,
            3_000.0,
            &cfg,
            &mr,
            &altcoin,
        );
        let risk = &status["portfolio"]["risk_control"];
        assert_eq!(risk["halted"], true);
        assert_eq!(risk["btc"]["halted"], true);
        assert_eq!(risk["btc"]["reason_codes"][0], "daily_drawdown");
        assert_eq!(risk["altcoin"]["halted"], true);
        assert_eq!(risk["altcoin"]["reason_codes"][0], "first_week_loss");
        assert_eq!(risk["altcoin"]["daily_drawdown_limit_pct"], 0.025);
    }

    #[test]
    fn aggregate_exposes_child_restart_timing() {
        let cfg = deployed_config();
        let mr = json!({"state":"running","equity":1_000.0});
        let altcoin = json!({
            "state":"restarting",
            "restart_count":4,
            "retry_in_s":16,
            "error":"temporary upstream error"
        });
        let status = aggregate_status(
            TradeMode::Paper,
            "config/strategy-portfolio.toml",
            1,
            2_000.0,
            &cfg,
            &mr,
            &altcoin,
        );
        assert_eq!(status["state"], "restarting");
        assert_eq!(status["restart_count"], 4);
        assert_eq!(status["retry_in_s"], 16);
    }
}
