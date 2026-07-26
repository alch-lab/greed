//! HTTP 控制面：`greed serve`。
//!
//! 前端（greed-web）通过本服务完成三件事：
//! - **监控**：`/api/trade/status`（引擎快照）+ `/api/journal/{mode}`（决策流水）
//! - **启停**：`/api/trade/start` / `/api/trade/stop`（dry/paper/live 三模式，
//!   交易循环作为受管后台任务运行，停 = 优雅关停落盘，不撤保护性止损）
//! - **回测**：`/api/backtest/run` 异步任务，结果 journal 落盘后可查
//!
//! 端口默认 8088（前端 vite dev server 已配置 /api 代理到此端口）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use tokio::sync::{watch, Mutex};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::trade_runner::{run_trade, TradeArgs, TradeMode};

// ============================================================================
// 共享状态
// ============================================================================

struct TradeHandle {
    mode: TradeMode,
    shutdown: watch::Sender<bool>,
    status_rx: watch::Receiver<serde_json::Value>,
    running: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct JobInfo {
    state: String, // running / done / error
    summary: serde_json::Value,
    error: Option<String>,
}

struct AppState {
    config: String,
    default_strategy: String,
    lake: String,
    trade: Mutex<Option<TradeHandle>>,
    jobs: Arc<Mutex<HashMap<String, JobInfo>>>,
    auth: AuthState,
}

/// 登录鉴权：环境变量 GREED_WEB_PASSWORD 设置后启用。
/// 单用户本地工具模型：密码校验通过签发随机会话 token（内存保存，重启失效）。
struct AuthState {
    password: Option<String>,
    tokens: Mutex<std::collections::HashSet<String>>,
}

/// 生成随机会话 token（/dev/urandom 24 字节 hex；退化用时间+PID）。
fn gen_token() -> String {
    use std::io::Read;
    let mut b = [0u8; 24];
    match std::fs::File::open("/dev/urandom") {
        Ok(mut f) => {
            let _ = f.read_exact(&mut b);
        }
        Err(_) => {
            let n = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
                ^ (std::process::id() as u128);
            for (i, chunk) in b.chunks_mut(16).enumerate() {
                chunk.copy_from_slice(&(n.wrapping_add(i as u128)).to_le_bytes());
            }
        }
    }
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

#[derive(Deserialize)]
struct LoginReq {
    password: String,
}

async fn auth_login(
    State(st): State<Arc<AppState>>,
    Json(req): Json<LoginReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match &st.auth.password {
        None => Ok(Json(serde_json::json!({ "auth_required": false }))),
        Some(pw) if req.password == *pw => {
            let t = gen_token();
            st.auth.tokens.lock().await.insert(t.clone());
            Ok(Json(serde_json::json!({ "auth_required": true, "token": t })))
        }
        _ => Err((StatusCode::UNAUTHORIZED, "密码错误".into())),
    }
}

async fn auth_check(State(st): State<Arc<AppState>>) -> StatusCode {
    // 能走到这里说明已通过中间件（或未启用鉴权）
    let _ = st;
    StatusCode::OK
}

/// 鉴权中间件：未启用（无 GREED_WEB_PASSWORD）直接放行；
/// 启用时仅放行 /api/auth/login 与 /api/health，其余要求 Bearer token。
async fn auth_mw(
    State(st): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    if st.auth.password.is_none() {
        return Ok(next.run(req).await);
    }
    let path = req.uri().path().to_string();
    if path == "/api/auth/login" || path == "/api/health" {
        return Ok(next.run(req).await);
    }
    let token = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());
    if let Some(t) = token {
        if st.auth.tokens.lock().await.contains(&t) {
            return Ok(next.run(req).await);
        }
    }
    Err(StatusCode::UNAUTHORIZED)
}

// ============================================================================
// 请求体
// ============================================================================

#[derive(Deserialize)]
struct StartReq {
    mode: TradeMode,
    strategy: Option<String>,
    risk_pct: Option<f64>,
    max_risk_pct: Option<f64>,
    leverage: Option<u32>,
    cash: Option<f64>,
}

#[derive(Deserialize)]
struct BacktestReq {
    from: String,
    to: String,
    strategy: Option<String>,
    cash: Option<f64>,
    risk_pct: Option<f64>,
    max_risk_pct: Option<f64>,
}

// ============================================================================
// 交易启停
// ============================================================================

async fn trade_status(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let guard = st.trade.lock().await;
    match guard.as_ref() {
        Some(h) => {
            let mut v = h.status_rx.borrow().clone();
            if !h.running.load(Ordering::SeqCst) {
                v["state"] = "stopped".into();
                if let Some(e) = h.error.lock().await.clone() {
                    v["error"] = e.into();
                }
            }
            Json(v)
        }
        None => Json(serde_json::json!({ "state": "stopped" })),
    }
}

async fn trade_start(
    State(st): State<Arc<AppState>>,
    Json(req): Json<StartReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut guard = st.trade.lock().await;
    if let Some(h) = guard.as_ref() {
        if h.running.load(Ordering::SeqCst) {
            return Err((
                StatusCode::CONFLICT,
                format!("已有 {} 模式交易在运行，请先停止", h.mode.as_str()),
            ));
        }
    }
    let args = TradeArgs {
        config: st.config.clone(),
        strategy: req.strategy.unwrap_or_else(|| st.default_strategy.clone()),
        journal: None, // data/journal/{mode}.json
        mode: Some(req.mode),
        cash: req.cash.unwrap_or(100_000.0),
        risk_pct: req.risk_pct.unwrap_or(0.0075),
        max_risk_pct: req.max_risk_pct.unwrap_or(0.015),
        entry_ttl_ms: 4 * 3_600_000,
        cb_max_daily_losses: 0,
        cb_daily_dd_pct: 0.0,
        leverage: req.leverage.unwrap_or(3),
        ws_base: None,
    };
    let (sd_tx, sd_rx) = watch::channel(false);
    let (st_tx, st_rx) = watch::channel(serde_json::json!({
        "state": "starting",
        "mode": req.mode.as_str(),
    }));
    let running = Arc::new(AtomicBool::new(true));
    let error = Arc::new(Mutex::new(None));
    *guard = Some(TradeHandle {
        mode: req.mode,
        shutdown: sd_tx,
        status_rx: st_rx,
        running: running.clone(),
        error: error.clone(),
    });
    drop(guard);

    tokio::spawn(async move {
        let r = run_trade(args, sd_rx, Some(st_tx)).await;
        running.store(false, Ordering::SeqCst);
        if let Err(e) = r {
            let msg = format!("{:#}", e);
            warn!(error = %msg, "交易任务退出（异常）");
            *error.lock().await = Some(msg);
        } else {
            info!("交易任务已停止");
        }
    });
    Ok(Json(
        serde_json::json!({ "ok": true, "mode": req.mode.as_str() }),
    ))
}

async fn trade_stop(
    State(st): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let h = {
        let guard = st.trade.lock().await;
        guard.as_ref().map(|h| (h.shutdown.clone(), h.running.clone()))
    };
    let Some((shutdown, running)) = h else {
        return Err((StatusCode::CONFLICT, "当前没有运行中的交易".into()));
    };
    if !running.load(Ordering::SeqCst) {
        return Err((StatusCode::CONFLICT, "交易已停止".into()));
    }
    let _ = shutdown.send(true);
    // 等待优雅退出（最多 15s）
    for _ in 0..150 {
        if !running.load(Ordering::SeqCst) {
            return Ok(Json(serde_json::json!({ "ok": true })));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err((StatusCode::REQUEST_TIMEOUT, "关停超时（15s）".into()))
}

async fn trade_journal(
    Path(mode): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let name = match mode.as_str() {
        "dry" | "paper" | "live" => format!("{}.json", mode),
        _ => return Err((StatusCode::BAD_REQUEST, "mode 应为 dry/paper/live".into())),
    };
    let path = format!("data/journal/{}", name);
    read_json_file(&path).await.map(Json)
}

// ============================================================================
// 回测任务
// ============================================================================

async fn backtest_run(
    State(st): State<Arc<AppState>>,
    Json(req): Json<BacktestReq>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    // 参数校验（日期格式）
    for d in [&req.from, &req.to] {
        chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
            .map_err(|_| (StatusCode::BAD_REQUEST, format!("日期格式错误: {}", d)))?;
    }
    let id = format!(
        "{}-{:03}",
        chrono::Utc::now().format("%Y%m%d%H%M%S"),
        (chrono::Utc::now().timestamp_subsec_millis() % 1000)
    );
    let strategy = req.strategy.unwrap_or_else(|| st.default_strategy.clone());
    let summary = serde_json::json!({
        "id": id,
        "from": req.from,
        "to": req.to,
        "strategy": strategy,
        "risk_pct": req.risk_pct.unwrap_or(0.0075),
    });
    st.jobs.lock().await.insert(
        id.clone(),
        JobInfo {
            state: "running".into(),
            summary: summary.clone(),
            error: None,
        },
    );

    let jobs = st.jobs.clone();
    let job_id = id.clone();
    let lake = st.lake.clone();
    tokio::spawn(async move {
        let journal_path = format!("data/journal/backtest/{}.json", job_id);
        let report_prefix = format!("out/api/{}", job_id);
        let res = tokio::task::spawn_blocking(move || {
            exec_backtest(
                &req.from,
                &req.to,
                &strategy,
                &lake,
                req.cash.unwrap_or(100_000.0),
                req.risk_pct.unwrap_or(0.0075),
                req.max_risk_pct.unwrap_or(0.015),
                &journal_path,
                &report_prefix,
            )
        })
        .await;
        let mut g = jobs.lock().await;
        if let Some(j) = g.get_mut(&job_id) {
            match res {
                Ok(Ok(())) => j.state = "done".into(),
                Ok(Err(e)) => {
                    j.state = "error".into();
                    j.error = Some(format!("{:#}", e));
                }
                Err(e) => {
                    j.state = "error".into();
                    j.error = Some(format!("任务 panic: {}", e));
                }
            }
        }
    });
    Ok(Json(serde_json::json!({ "job_id": id })))
}

async fn backtest_jobs(State(st): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut out: Vec<serde_json::Value> = Vec::new();
    // 磁盘上的历史任务（重启后仍可见）
    let dir = std::path::Path::new("data/journal/backtest");
    if dir.exists() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                let Some(id) = name.strip_suffix(".json") else { continue };
                let meta = std::fs::read_to_string(e.path())
                    .ok()
                    .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                    .map(|j| j["meta"].clone());
                out.push(serde_json::json!({
                    "id": id,
                    "state": "done",
                    "meta": meta,
                }));
            }
        }
    }
    // 内存中的运行中/失败任务覆盖同 id
    let g = st.jobs.lock().await;
    for (id, j) in g.iter() {
        out.retain(|o| o["id"].as_str() != Some(id.as_str()));
        out.push(serde_json::json!({
            "id": id,
            "state": j.state,
            "error": j.error,
            "summary": j.summary,
        }));
    }
    out.sort_by(|a, b| b["id"].as_str().cmp(&a["id"].as_str()));
    out.truncate(50);
    Json(serde_json::json!({ "jobs": out }))
}

async fn backtest_journal(
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err((StatusCode::BAD_REQUEST, "非法 job id".into()));
    }
    let path = format!("data/journal/backtest/{}.json", id);
    read_json_file(&path).await.map(Json)
}

async fn list_strategies() -> Json<serde_json::Value> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir("config") {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".toml") && name.starts_with("strategy") {
                out.push(format!("config/{}", name));
            }
        }
    }
    out.sort();
    Json(serde_json::json!({ "strategies": out }))
}

async fn read_json_file(path: &str) -> Result<serde_json::Value, (StatusCode, String)> {
    let text = tokio::fs::read_to_string(path)
        .await
        .map_err(|_| (StatusCode::NOT_FOUND, format!("文件不存在: {}", path)))?;
    serde_json::from_str(&text)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("JSON 解析失败: {}", e)))
}

// ============================================================================
// 回测执行（阻塞，spawn_blocking 调用）
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn exec_backtest(
    from: &str,
    to: &str,
    strategy_path: &str,
    lake_dir: &str,
    cash: f64,
    risk_pct: f64,
    max_risk_pct: f64,
    journal_path: &str,
    report_prefix: &str,
) -> Result<()> {
    use backtest::{
        build_report, pair_round_trips, to_json, to_markdown, BacktestConfig, BacktestEngine,
        Journal, JournalMeta, ReportConfig,
    };
    use data::Lake;
    use strategy::{assemble_from_toml, builtin_registry};
    use tcore::types::{Exchange, Symbol, Timestamp};

    let parse = |s: &str| -> Result<chrono::NaiveDate> {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .with_context(|| format!("日期格式错误: {}", s))
    };
    let from_ms = parse(from)?.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
    let to_ms = (parse(to)? + chrono::Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    let toml_str = std::fs::read_to_string(strategy_path)
        .with_context(|| format!("读策略配置失败: {}", strategy_path))?;
    let strat = assemble_from_toml(&toml_str, &builtin_registry())
        .map_err(|e| anyhow::anyhow!("装配策略失败: {}", e))?;

    let exchange = Exchange::BinanceFutures;
    let lake = Lake::new(lake_dir);
    let sym = Symbol::new("BTCUSDT");
    let trades = data::lake::read_range(
        &lake,
        exchange,
        &sym,
        Timestamp::from_millis(from_ms),
        Timestamp::from_millis(to_ms),
    )?;
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
    let mut events: Vec<tcore::Event> = Vec::with_capacity(trades.len() + ois.len() + fundings.len());
    events.extend(trades.into_iter().map(tcore::Event::Trade));
    events.extend(ois.into_iter().map(tcore::Event::Oi));
    events.extend(fundings.into_iter().map(tcore::Event::Funding));
    events.sort_by_key(|e| e.ts());

    let cfg = BacktestConfig {
        initial_cash: cash,
        risk_pct,
        max_risk_pct,
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(strat, sym.clone(), cfg);
    let result = engine.run(&events);

    let trips = pair_round_trips(&result.fills);
    let rc = ReportConfig {
        risk_pct,
        ..Default::default()
    };
    let report = build_report(&trips, &result.equity_curve, &rc, &[]);

    if let Some(parent) = std::path::Path::new(journal_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let j = Journal {
        meta: JournalMeta {
            symbol: "BTCUSDT".into(),
            from: from.into(),
            to: to.into(),
            strategy: strategy_path.into(),
            initial_cash: cash,
            final_equity: result.final_equity,
        },
        intents: result.intents.clone(),
        fills: result.fills.clone(),
        equity_curve: result.equity_curve.clone(),
    };
    std::fs::write(journal_path, serde_json::to_string_pretty(&j)?)?;

    if let Some(parent) = std::path::Path::new(report_prefix).parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(format!("{}.md", report_prefix), to_markdown(&report))?;
    std::fs::write(format!("{}.json", report_prefix), to_json(&report)?)?;
    info!(journal = %journal_path, trips = trips.len(), "回测任务完成");
    Ok(())
}

// ============================================================================
// 入口
// ============================================================================

pub async fn run_serve(port: u16, config: String, strategy: String, lake: String) -> Result<()> {
    let password = std::env::var("GREED_WEB_PASSWORD")
        .ok()
        .filter(|s| !s.trim().is_empty());
    if password.is_some() {
        info!("控制面鉴权已启用（GREED_WEB_PASSWORD）");
    } else {
        warn!("未设置 GREED_WEB_PASSWORD，控制面无鉴权（仅建议本机使用）");
    }
    let state = Arc::new(AppState {
        config,
        default_strategy: strategy,
        lake,
        trade: Mutex::new(None),
        jobs: Arc::new(Mutex::new(HashMap::new())),
        auth: AuthState {
            password,
            tokens: Mutex::new(std::collections::HashSet::new()),
        },
    });

    let app = Router::new()
        .route("/api/health", get(|| async { Json(serde_json::json!({"ok": true})) }))
        .route("/api/auth/login", post(auth_login))
        .route("/api/auth/check", get(auth_check))
        .route("/api/strategies", get(list_strategies))
        .route("/api/trade/status", get(trade_status))
        .route("/api/trade/start", post(trade_start))
        .route("/api/trade/stop", post(trade_stop))
        .route("/api/journal/{mode}", get(trade_journal))
        .route("/api/backtest/run", post(backtest_run))
        .route("/api/backtest/jobs", get(backtest_jobs))
        .route("/api/backtest/jobs/{id}/journal", get(backtest_journal))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_mw))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!(port, "控制面已启动（前端 /api 代理目标）");
    axum::serve(listener, app).await?;
    Ok(())
}
