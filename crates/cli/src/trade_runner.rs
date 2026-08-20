//! 交易执行循环（CLI `greed trade` 与控制面 `greed serve` 共用）。
//!
//! 支持外部关停（watch 通道）与状态上报（每节拍发 JSON 快照），
//! 使交易循环可以作为一个可启停的后台任务被 HTTP 控制面管理。

use anyhow::{Context, Result};
use backtest::FeeModel;
use data::live::config::AccountConfig;
use data::live::CollectorConfig;
use sha2::{Digest, Sha256};
use strategy::{assemble_from_toml, builtin_registry};
use tcore::types::Exchange;
use tracing::{info, warn};

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
    /// 组合运行器下的虚拟 sleeve。允许账户中存在由另一个受管执行器维护的
    /// 非本标的仓位，并将 `cash` 作为本策略可用本金上限。
    pub portfolio_mode: bool,
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
    let strat = assemble_from_toml(&toml_str, &builtin_registry())
        .map_err(|e| anyhow::anyhow!("装配策略失败: {}", e))?;
    info!(strategy = %args.strategy, mode = mode.as_str(), "{}", strat.describe());

    // 行情与执行分离：paper/dry 的 K 线/信号用主网公共行情（testnet 成交流稀疏、
    // 价格陈旧且偏离真实市场，与回测的主网历史数据也不一致）；订单仍由 testnet
    // 撮合。Live 用主网（与配置一致）。--ws-base 可显式覆盖。
    let ws_base = args.ws_base.clone().unwrap_or_else(|| match mode {
        TradeMode::Dry | TradeMode::Paper => live::MAINNET_WS.to_string(),
        TradeMode::Live => account.ws_base().to_string(),
    });
    let http = data::live::build_http_client(collector.effective_proxy().as_deref());

    // 经纪层：dry（模拟撮合）或 testnet/主网（真实下单）
    let (mut broker, initial_cash, qty_step, min_notional, exchange_position_amt) = if mode
        == TradeMode::Dry
    {
        info!(
            cash = args.cash,
            ws = %ws_base,
            "dry-run：模拟撮合，不下真实订单"
        );
        (
            live::AnyBroker::dry(FeeModel::default()),
            args.cash,
            1e-8,
            0.0,
            0.0,
        )
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

        let external: Vec<_> = rest
            .open_position_amounts()
            .await?
            .into_iter()
            .filter(|(symbol, _)| symbol != &collector.symbol)
            .collect();
        anyhow::ensure!(
            args.portfolio_mode || external.is_empty(),
            "账户存在其他策略/手工持仓 {:?}；单账户模式拒绝同时启动 BTC 执行器",
            external
        );

        let amt = rest.position_amt(&collector.symbol).await?;
        // 空仓才可在恢复检查前清遗留单。有仓时先保留交易所保护性止损；若后续确认
        // journal 与仓位一致，首个行情节拍会安全撤旧并重挂。若不一致则原止损不受影响。
        if amt.abs() <= 1e-12 {
            rest.cancel_all_open_orders(&collector.symbol).await?;
        } else {
            info!(
                position_amt = amt,
                "检测到交易所持仓，恢复校验前保留现有保护单"
            );
        }
        rest.set_leverage(&collector.symbol, args.leverage).await?;
        rest.set_margin_isolated(&collector.symbol).await?;

        let filters = rest.symbol_filters(&collector.symbol).await?;
        let wallet = rest.wallet_balance_usdt().await?;
        let sleeve_cash = if args.cash.is_finite() && args.cash > 0.0 {
            wallet.min(args.cash)
        } else {
            wallet
        };
        info!(
            wallet,
            sleeve_cash,
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
        (
            live::AnyBroker::testnet(rest, &collector.symbol, filters),
            sleeve_cash,
            filters.step_size,
            filters.min_notional,
            amt,
        )
    };
    // 必须在引擎提交任何订单之前定位 userTrades 游标；否则进程重启会把账户历史成交
    // 重新当成新 fill 导入，污染本地持仓、盈亏与 Journal。
    broker.prime_fill_cursor().await?;

    let started_at = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let started_ms = chrono::Utc::now().timestamp_millis();
    let run_id = format!("{}-{}-{}", mode.as_str(), started_ms, std::process::id());
    let strategy_hash = format!("{:x}", Sha256::digest(toml_str.as_bytes()));
    let strategy_doc = toml::from_str::<toml::Value>(&toml_str).ok();
    let plugins = strategy_doc
        .as_ref()
        .and_then(|v| v.get("strategy"))
        .and_then(|v| v.get("plugins"));
    let hybrid_params = plugins.and_then(|v| v.get("HybridEntry"));
    let orderflow_params = plugins.and_then(|v| v.get("OrderFlowEntry"));
    let strategy_risk_scale = hybrid_params
        .and_then(|v| v.get("mr_risk_scale"))
        .or_else(|| orderflow_params.and_then(|v| v.get("risk_scale")))
        .and_then(toml::Value::as_float)
        .unwrap_or(0.15)
        .clamp(0.0, 1.0);
    let market_entry = hybrid_params
        .and_then(|v| v.get("mr_market_entry"))
        .or_else(|| orderflow_params.and_then(|v| v.get("market_entry")))
        .and_then(toml::Value::as_bool)
        .unwrap_or(true);
    let strategy_stop_pct = hybrid_params
        .and_then(|v| v.get("mr_stop_pct"))
        .or_else(|| orderflow_params.and_then(|v| v.get("min_stop_pct")))
        .and_then(toml::Value::as_float)
        .unwrap_or(0.005)
        .max(1e-6);
    let git_commit = crate::BUILD_GIT_COMMIT.to_owned();
    let eval_dir = format!("data/evals/{}", mode.as_str());
    let research_dir = format!("data/research/{}", mode.as_str());
    let cfg = live::LiveConfig {
        symbol: collector.symbol.clone(),
        risk_pct: args.risk_pct,
        max_risk_pct: args.max_risk_pct,
        max_leverage: args.leverage as f64,
        strategy_risk_scale,
        strategy_stop_pct,
        entry_ttl_ms: args.entry_ttl_ms,
        cb_max_daily_losses: args.cb_max_daily_losses,
        cb_daily_dd_pct: args.cb_daily_dd_pct,
        qty_step,
        min_notional,
        journal_path: std::path::PathBuf::from(&journal_path),
        eval_log_path: std::path::PathBuf::from(format!("{eval_dir}/{run_id}.jsonl")),
        research_log_dir: std::path::PathBuf::from(&research_dir),
        strategy_name: args.strategy.clone(),
        strategy_hash,
        strategy_snapshot: toml_str.clone(),
        git_commit,
        run_id,
        mode: mode.as_str().into(),
        estimated_roundtrip_fee_bps: if market_entry { 8.0 } else { 6.0 },
    };
    if let Some(parent) = std::path::Path::new(&journal_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir_all(&eval_dir)?;
    std::fs::create_dir_all(&research_dir)?;
    let mut engine = live::LiveEngine::new(strat, broker, cfg, initial_cash, started_at);
    // 重启续跑：从既有 journal 恢复单账户持仓、现金、成交与熔断状态。
    // 必须在任何 persist_journal 之前调用，否则空状态会先覆盖 journal。
    let restored = engine.try_restore(mode != TradeMode::Dry);
    if mode != TradeMode::Dry && exchange_position_amt.abs() > qty_step / 2.0 {
        let local = engine.snapshot().position.as_ref().map_or(0.0, |p| {
            if p.side == "Buy" {
                p.qty
            } else {
                -p.qty
            }
        });
        if !restored || (local - exchange_position_amt).abs() > qty_step.max(1e-8) {
            anyhow::bail!(
                "启动检查失败：交易所仓位 {} 与本地可恢复仓位 {} 不一致；为防止误接外部仓位，已拒绝启动",
                exchange_position_amt, local
            );
        }
        info!(
            exchange_position_amt,
            "交易所仓位与 journal 一致，重启后安全接管"
        );
    }
    engine.persist_journal();

    // 订单流基线只接受真实逐笔成交，不用 K 线合成 Delta。默认约 200 秒完成预热。
    info!("开始积累真实订单流基线（不使用合成成交）");

    // 行情 feed → 引擎；1s 节拍做成交轮询/采样/对账/状态上报
    let (tx, mut rx) = tokio::sync::mpsc::channel::<tcore::Trade>(4096);
    {
        let ws = ws_base.clone();
        let sym = collector.symbol.clone();
        tokio::spawn(async move {
            live::feed::run_trade_feed(&ws, &sym, Exchange::BinanceFutures, tx).await;
        });
    }
    // 现货成交仅作为 TRDR 跨市场订单流上下文；执行和仓位仍全部在 USDⓈ-M 合约。
    let (spot_tx, mut spot_rx) = tokio::sync::mpsc::channel::<tcore::Trade>(4096);
    {
        let sym = collector.symbol.clone();
        tokio::spawn(async move {
            live::feed::run_trade_feed(live::MAINNET_SPOT_WS, &sym, Exchange::BinanceSpot, spot_tx)
                .await;
        });
    }
    let (context_tx, mut context_rx) = tokio::sync::mpsc::channel::<tcore::Event>(16_384);
    {
        let sym = collector.symbol.clone();
        let client = http.clone();
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::feed::run_context_feed(
                live::MAINNET_FAPI,
                live::MAINNET_SPOT_API,
                &sym,
                client,
                tx,
            )
            .await;
        });
    }
    let okx_contract_value = live::venue_feed::fetch_okx_swap_ct_val(&http)
        .await
        .unwrap_or_else(|| {
            warn!(
                fallback = live::venue_feed::OKX_BTC_SWAP_CT_VAL,
                "OKX 合约规格获取失败，使用 BTC-USDT-SWAP 回退面值"
            );
            live::venue_feed::OKX_BTC_SWAP_CT_VAL
        });
    info!(okx_contract_value, "已加载 OKX BTC-USDT-SWAP 合约面值");
    {
        let sym = collector.symbol.clone();
        let client = http.clone();
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_external_context_feed(&sym, okx_contract_value, client, tx).await;
        });
    }
    for (url, exchange) in [
        (live::venue_feed::BYBIT_SPOT_WS, Exchange::BybitSpot),
        (live::venue_feed::BYBIT_LINEAR_WS, Exchange::BybitFutures),
    ] {
        let sym = collector.symbol.clone();
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_bybit_trade_feed(url, &sym, exchange, tx).await;
        });
    }
    for (inst_id, exchange, multiplier) in [
        ("BTC-USDT", Exchange::OkxSpot, 1.0),
        (
            "BTC-USDT-SWAP",
            Exchange::OkxFutures,
            live::venue_feed::OKX_BTC_SWAP_CT_VAL,
        ),
    ] {
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_okx_trade_feed(inst_id, exchange, multiplier, tx).await;
        });
    }
    {
        let sym = collector.symbol.clone();
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_binance_liquidation_feed(&sym, tx).await;
        });
    }
    {
        let sym = collector.symbol.clone();
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_bybit_liquidation_feed(&sym, tx).await;
        });
    }
    {
        let tx = context_tx.clone();
        tokio::spawn(async move {
            live::venue_feed::run_okx_liquidation_feed(okx_contract_value, tx).await;
        });
    }
    drop(context_tx);
    info!(symbol = %collector.symbol, journal = %journal_path, mode = mode.as_str(), "进入交易主循环");
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            Some(ev) = context_rx.recv() => engine.on_context_event(ev),
            Some(t) = spot_rx.recv() => engine.on_context_event(tcore::Event::Trade(t)),
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
                if let Some(tx) = &status_tx {
                    let snap = engine.snapshot();
                    let account_net_qty = snap
                        .position
                        .as_ref()
                        .map(|p| if p.side == "Buy" { p.qty } else { -p.qty });
                    // EngineSnapshot 已是控制面的正式契约；先整体序列化，再补运行器元数据，
                    // 避免新增引擎字段时忘记在此手工透传，导致后端有值而前端永远看不到。
                    let mut status = serde_json::to_value(&snap)
                        .expect("EngineSnapshot serialization cannot fail");
                    let runtime = serde_json::json!({
                        "state": "running",
                        "mode": mode.as_str(),
                        "started_at_ms": started_ms,
                        "uptime_s": (now_ms - started_ms) / 1000,
                        "journal": journal_path,
                        "account_net_qty": account_net_qty,
                    });
                    status
                        .as_object_mut()
                        .expect("EngineSnapshot serializes to an object")
                        .extend(
                            runtime
                                .as_object()
                                .expect("runtime metadata is an object")
                                .clone(),
                        );
                    let _ = tx.send(status);
                }
            }
            _ = shutdown.changed() => {
                info!("收到外部关停信号，优雅退出");
                engine.shutdown().await;
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("收到 SIGINT，优雅退出");
                engine.shutdown().await;
                break;
            }
        }
    }
    Ok(())
}
