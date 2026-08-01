//! 交易执行循环（CLI `greed trade` 与控制面 `greed serve` 共用）。
//!
//! 支持外部关停（watch 通道）与状态上报（每节拍发 JSON 快照），
//! 使交易循环可以作为一个可启停的后台任务被 HTTP 控制面管理。

use anyhow::{Context, Result};
use backtest::FeeModel;
use data::live::config::AccountConfig;
use data::live::CollectorConfig;
use strategy::{assemble_from_toml, builtin_registry};
use tcore::types::Exchange;
use tracing::{info, warn};

#[derive(serde::Deserialize)]
struct PortfolioFile {
    #[serde(default)]
    portfolio: live::PortfolioConfig,
}

/// 运行模式：干跑 / 模拟盘（testnet）/ 实盘（主网）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeMode {
    Dry,
    Paper,
    Live,
}

impl TradeMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeMode::Dry => "dry",
            TradeMode::Paper => "paper",
            TradeMode::Live => "live",
        }
    }
    /// 默认 journal 文件名（data/journal/ 下）
    pub fn journal_name(&self) -> &'static str {
        match self {
            TradeMode::Dry => "dry.json",
            TradeMode::Paper => "paper.json",
            TradeMode::Live => "live.json",
        }
    }
}

#[derive(Clone)]
pub struct TradeArgs {
    pub config: String,
    pub strategy: String,
    /// None → data/journal/{mode}.json
    pub journal: Option<String>,
    /// None → 由 [account].testnet 推导（true=Paper，false=Live）
    pub mode: Option<TradeMode>,
    pub cash: f64,
    pub risk_pct: f64,
    pub max_risk_pct: f64,
    pub entry_ttl_ms: i64,
    pub cb_max_daily_losses: u32,
    pub cb_daily_dd_pct: f64,
    pub leverage: u32,
    pub ws_base: Option<String>,
}

/// 交易执行循环：行情 WS → LiveEngine → journal 落盘。
///
/// - `shutdown`：收到 true 后优雅退出（落盘、不撤保护性止损）。
/// - `status_tx`：给出时每节拍上报一次 JSON 状态快照（控制面轮询用）。
pub async fn run_trade(
    args: TradeArgs,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    status_tx: Option<tokio::sync::watch::Sender<serde_json::Value>>,
) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("读取配置失败: {}", args.config))?;
    let collector = CollectorConfig::from_toml_str(&text)
        .with_context(|| format!("解析配置 [collector] 失败: {}", args.config))?;
    let mut account = AccountConfig::from_toml_str(&text)
        .with_context(|| format!("解析配置 [account] 失败: {}", args.config))?;

    // 模式裁决：显式指定优先；否则按配置的 testnet 推导
    let mode = args.mode.unwrap_or(if account.testnet {
        TradeMode::Paper
    } else {
        TradeMode::Live
    });
    // 显式模式下覆盖端点选择（Paper 强制 testnet，Live 强制主网）
    match mode {
        TradeMode::Paper => account.testnet = true,
        TradeMode::Live => account.testnet = false,
        TradeMode::Dry => {}
    }

    let journal_path = args
        .journal
        .clone()
        .unwrap_or_else(|| format!("data/journal/{}", mode.journal_name()));

    let toml_str = std::fs::read_to_string(&args.strategy)
        .with_context(|| format!("读策略配置失败: {}", args.strategy))?;
    let portfolio_cfg = toml::from_str::<PortfolioFile>(&toml_str)
        .with_context(|| format!("解析 [portfolio] 失败: {}", args.strategy))?
        .portfolio;
    if mode == TradeMode::Live && portfolio_cfg.enabled {
        anyhow::bail!("四层组合当前只允许 paper 模式；完成模拟盘验收前禁止连接主网");
    }
    let strat = assemble_from_toml(&toml_str, &builtin_registry())
        .map_err(|e| anyhow::anyhow!("装配策略失败: {}", e))?;
    info!(strategy = %args.strategy, mode = mode.as_str(), "{}", strat.describe());

    // 行情与执行分离：paper/dry 的 K 线/信号用主网公共行情（testnet 成交流稀疏、
    // 价格陈旧且偏离真实市场，与回测的主网历史数据也不一致）；订单仍由 testnet
    // 撮合。Live 用主网（与配置一致）。--ws-base 可显式覆盖。
    let ws_base = args
        .ws_base
        .clone()
        .unwrap_or_else(|| match mode {
            TradeMode::Dry | TradeMode::Paper => live::MAINNET_WS.to_string(),
            TradeMode::Live => account.ws_base().to_string(),
        });
    let http = data::live::build_http_client(collector.effective_proxy().as_deref());

    // 经纪层：dry（模拟撮合）或 testnet/主网（真实下单）
    let (broker, initial_cash, qty_step, min_notional, mut portfolio) = if mode == TradeMode::Dry {
        info!(
            cash = args.cash,
            ws = %ws_base,
            "dry-run：模拟撮合，不下真实订单"
        );
        (live::AnyBroker::dry(FeeModel::default()), args.cash, 1e-8, 0.0, None)
    } else {
        let (key, secret) = match (account.api_key(), account.api_secret()) {
            (Some(k), Some(s)) => (k, s),
            _ => anyhow::bail!(
                "缺少 API 凭证：请 export {} 与 {}（Demo Trading 创建：https://demo.binance.com → API 管理）",
                account.api_key_env,
                account.api_secret_env
            ),
        };
        let mut rest = live::RestClient::new(http.clone(), account.rest_base(), key, secret);
        rest.sync_time().await?;

        let amt = rest.position_amt(&collector.symbol).await?;
        if !portfolio_cfg.enabled && amt.abs() > 1e-9 {
            anyhow::bail!(
                "启动检查失败：{} 存在持仓 {} —— 请先手动平仓（本引擎不接管外部持仓）",
                collector.symbol,
                amt
            );
        }
        // 清掉遗留挂单，设置杠杆与逐仓
        rest.cancel_all_open_orders(&collector.symbol).await?;
        rest.set_leverage(&collector.symbol, args.leverage).await?;
        rest.set_margin_isolated(&collector.symbol).await?;

        let filters = rest.symbol_filters(&collector.symbol).await?;
        let wallet = rest.wallet_balance_usdt().await?;
        info!(
            wallet,
            leverage = args.leverage,
            tick = filters.tick_size,
            step = filters.step_size,
            min_notional = filters.min_notional,
            mode = mode.as_str(),
            "账户就绪"
        );
        if mode == TradeMode::Paper {
            info!(ws = %ws_base, "行情=主网公共 WS，执行=testnet（信号价格与回测同环境）");
        }
        if portfolio_cfg.enabled {
            let spot = match (account.spot_api_key(), account.spot_api_secret()) {
                (Some(k), Some(s)) => Some(live::SpotRestClient::new(http.clone(), account.spot_rest_base(), k, s)),
                _ if portfolio_cfg.carry_enabled => anyhow::bail!(
                    "carry 已启用：请设置 {} 与 {}（现货 Demo 凭证，https://demo.binance.com → API 管理）",
                    account.spot_api_key_env, account.spot_api_secret_env
                ),
                _ => None,
            };
            let market_base = match mode { TradeMode::Paper => live::MAINNET_FAPI, _ => account.rest_base() }.to_string();
            let executor = live::PortfolioExecutor::new(
                portfolio_cfg.clone(), collector.symbol.clone(), wallet, rest, filters,
                spot, http.clone(), market_base,
                std::path::PathBuf::from(format!("data/state/portfolio-{}.json", mode.as_str())),
            ).await?;
            (
                live::AnyBroker::dry(FeeModel::default()), wallet,
                filters.step_size, filters.min_notional, Some(executor),
            )
        } else {
            (
                live::AnyBroker::testnet(rest, &collector.symbol, filters), wallet,
                filters.step_size, filters.min_notional, None,
            )
        }
    };

    let started_at = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let started_ms = chrono::Utc::now().timestamp_millis();
    let cfg = live::LiveConfig {
        symbol: collector.symbol.clone(),
        risk_pct: args.risk_pct,
        max_risk_pct: args.max_risk_pct,
        entry_ttl_ms: args.entry_ttl_ms,
        cb_max_daily_losses: args.cb_max_daily_losses,
        cb_daily_dd_pct: args.cb_daily_dd_pct,
        qty_step,
        min_notional,
        journal_path: std::path::PathBuf::from(&journal_path),
        eval_log_path: std::path::PathBuf::from(format!(
            "data/evals/{}.jsonl",
            mode.as_str()
        )),
        strategy_name: args.strategy.clone(),
    };
    if let Some(parent) = std::path::Path::new(&journal_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all("data/evals")?;
    let mut engine = live::LiveEngine::new(strat, broker, cfg, initial_cash, started_at);
    // 重启续跑：从既有 journal 恢复账户/持仓/熔断状态（组合模式恢复持仓镜像，
    // 避免重启把 MR 仓位市价强平且费用无人认领）。
    // 必须在任何 persist_journal 之前调用，否则空状态会先覆盖 journal。
    engine.try_restore(portfolio.is_some());
    engine.persist_journal();

    // 启动预热：历史 K 线重建信号状态（免去 ~20h 实时攒线）。
    // 预热端点与行情源保持一致（paper/dry = 主网公共数据，live = 主网），
    // 预热失败不阻塞启动（退化为实时攒线）。
    let warmup_base = match mode {
        TradeMode::Dry | TradeMode::Paper => live::MAINNET_FAPI,
        TradeMode::Live => account.rest_base(),
    };
    match live::warmup_engine(&mut engine, &http, warmup_base, &collector.symbol).await {
        Ok(n) => info!(n, "预热完成，立即具备出信号能力"),
        Err(e) => warn!(error = %e, "预热失败，退化为实时攒线（出信号需等待 K 线积累）"),
    }
    engine.persist_journal();

    // 行情 feed → 引擎；1s 节拍做成交轮询/采样/对账/状态上报
    let (tx, mut rx) = tokio::sync::mpsc::channel::<tcore::Trade>(4096);
    {
        let ws = ws_base.clone();
        let sym = collector.symbol.clone();
        tokio::spawn(async move {
            live::feed::run_trade_feed(&ws, &sym, Exchange::BinanceFutures, tx).await;
        });
    }
    info!(symbol = %collector.symbol, journal = %journal_path, mode = mode.as_str(), "进入交易主循环");
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut portfolio_error: Option<String> = None;
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(t) => engine.on_trade(&t).await,
                    None => {
                        // feed 任务退出（如 panic）：不主动清仓——组合持仓由重启后的
                        // 执行器按磁盘状态接管（不重复开仓），引擎持仓从 journal 恢复。
                        // 直接报错退出，交给外层监督循环重启整个交易任务。
                        warn!("行情通道关闭（feed 任务退出），等待监督循环重启");
                        anyhow::bail!("行情通道关闭")
                    },
                }
            }
            _ = timer.tick() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                engine.on_timer(now_ms).await;
                if let Some(executor) = &mut portfolio {
                    let snap = engine.snapshot();
                    if let Some(price) = snap.last_price {
                        let mr_qty = snap.position.as_ref().map_or(0.0, |p| {
                            if p.side == "Buy" { p.qty } else { -p.qty }
                        });
                        match executor.reconcile(now_ms, price, mr_qty).await {
                            Ok(sleeves) => { portfolio_error = None; engine.set_sleeves(sleeves); },
                            Err(e) => {
                                let message = e.to_string();
                                warn!(error = %message, "组合账户对齐失败，下周期重试");
                                portfolio_error = Some(message);
                            },
                        }
                    }
                }
                if let Some(tx) = &status_tx {
                    let snap = engine.snapshot();
                    let _ = tx.send(serde_json::json!({
                        "state": "running",
                        "mode": mode.as_str(),
                        "started_at_ms": started_ms,
                        "uptime_s": (now_ms - started_ms) / 1000,
                        "journal": journal_path,
                        "last_price": snap.last_price,
                        "equity": snap.equity,
                        "cash": snap.cash,
                        "position": snap.position,
                        "account_net_qty": portfolio.as_ref().map(|p| p.actual_qty()),
                        "account_futures_equity": portfolio.as_ref().map(|p| p.real_equity().0),
                        "account_spot_equity": portfolio.as_ref().map(|p| p.real_equity().1),
                        "account_total_equity": portfolio.as_ref().map(|p| p.real_equity().2),
                        "portfolio_error": portfolio_error.clone(),
                        "n_intents": snap.n_intents,
                        "n_fills": snap.n_fills,
                        "last_eval": snap.last_eval,
                    }));
                }
            }
            _ = shutdown.changed() => {
                info!("收到外部关停信号，优雅退出");
                if let (Some(executor), Some(price)) = (&mut portfolio, engine.snapshot().last_price) {
                    let sleeves = executor.shutdown(chrono::Utc::now().timestamp_millis(), price).await?;
                    engine.set_sleeves(sleeves);
                }
                engine.shutdown().await;
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("收到 SIGINT，优雅退出");
                if let (Some(executor), Some(price)) = (&mut portfolio, engine.snapshot().last_price) {
                    let sleeves = executor.shutdown(chrono::Utc::now().timestamp_millis(), price).await?;
                    engine.set_sleeves(sleeves);
                }
                engine.shutdown().await;
                break;
            }
        }
    }
    Ok(())
}
