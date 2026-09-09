use crate::config::RuntimeConfig;
use anyhow::{Context, Result};
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot};

const MIN_OPERATOR_PASSWORD_LENGTH: usize = 8;

pub enum ControlCommand {
    Audit {
        action: &'static str,
        actor: String,
        requested_ms: i64,
    },
    ManualClose {
        symbol: String,
        note: Option<String>,
        actor: String,
        requested_ms: i64,
        response: oneshot::Sender<Result<Value, String>>,
    },
    ResetRiskGuard {
        actor: String,
        requested_ms: i64,
        response: oneshot::Sender<Result<Value, String>>,
    },
}

pub struct Monitor {
    pub task: tokio::task::JoinHandle<()>,
    pub commands: mpsc::Receiver<ControlCommand>,
    pub paused: Arc<AtomicBool>,
}

#[derive(Clone)]
struct ApiState {
    status_path: PathBuf,
    journal_path: PathBuf,
    history_path: PathBuf,
    command_tx: mpsc::Sender<ControlCommand>,
    paused: Arc<AtomicBool>,
    operator_password: Option<Arc<str>>,
    operator_token: Option<Arc<str>>,
}

#[derive(Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Deserialize, Default)]
struct ManualCloseRequest {
    note: Option<String>,
}

#[derive(Deserialize)]
struct EventQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize)]
struct PageQuery {
    #[serde(default = "default_page_limit")]
    limit: usize,
    before: Option<u64>,
    run_id: Option<String>,
}

fn default_limit() -> usize {
    100
}

fn default_page_limit() -> usize {
    50
}

pub async fn start(config: &RuntimeConfig) -> Result<Monitor> {
    let listener = tokio::net::TcpListener::bind(&config.http_listen)
        .await
        .with_context(|| format!("bind monitoring API on {}", config.http_listen))?;
    let (command_tx, commands) = mpsc::channel(32);
    let initially_paused = std::fs::read(&config.status_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value["control"]["paused"].as_bool())
        .unwrap_or(false);
    let paused = Arc::new(AtomicBool::new(initially_paused));
    let configured_password = env::var("GREED_WEB_PASSWORD").ok();
    if configured_password
        .as_ref()
        .is_some_and(|value| value.chars().count() < MIN_OPERATOR_PASSWORD_LENGTH)
    {
        tracing::error!(
            "GREED_WEB_PASSWORD is shorter than {MIN_OPERATOR_PASSWORD_LENGTH} characters; operator controls are disabled"
        );
    }
    let operator_password = configured_password
        .filter(|value| value.chars().count() >= MIN_OPERATOR_PASSWORD_LENGTH)
        .map(Arc::<str>::from);
    let operator_token = operator_password.as_ref().map(|password| {
        let seed = format!(
            "greed-operator-session-v1:{password}:{}:{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            std::process::id()
        );
        Arc::<str>::from(hex_digest(seed.as_bytes()))
    });
    let state = ApiState {
        status_path: config.status_path.clone().into(),
        journal_path: config.journal_path.clone().into(),
        history_path: config.history_path.clone().into(),
        command_tx,
        paused: paused.clone(),
        operator_password,
        operator_token,
    };
    let router = Router::new()
        .route("/api/health", get(health))
        .route("/api/status", get(status))
        .route("/api/events", get(events))
        .route("/api/history", get(history))
        .route("/api/trades", get(trades))
        .route("/api/equity", get(equity))
        .route("/api/auth/login", post(login))
        .route("/api/auth/session", get(session))
        .route("/api/control/pause", post(pause))
        .route("/api/control/resume", post(resume))
        .route("/api/control/risk/reset", post(reset_risk_guard))
        .route("/api/positions/{symbol}/close", post(manual_close))
        .with_state(state);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(error = %error, "monitoring API stopped");
        }
    });
    Ok(Monitor {
        task,
        commands,
        paused,
    })
}

async fn health(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({
        "ok": state.status_path.exists(),
        "paper_only": true,
        "status_path": state.status_path,
        "server_ms": chrono::Utc::now().timestamp_millis(),
    }))
}

async fn status(State(state): State<ApiState>) -> Response {
    match read_json(&state.status_path) {
        Ok(mut value) => {
            reconcile_closed_positions(&mut value, &state.history_path);
            value["control"] = control_status(&state);
            Json(value).into_response()
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|value| value.kind() == std::io::ErrorKind::NotFound) =>
        {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"paper status is not ready"})),
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error":error.to_string()})),
        )
            .into_response(),
    }
}

async fn login(State(state): State<ApiState>, Json(input): Json<LoginRequest>) -> Response {
    let Some(expected) = state.operator_password.as_deref() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "operator login is disabled; set GREED_WEB_PASSWORD",
        );
    };
    if !constant_time_eq(input.password.as_bytes(), expected.as_bytes()) {
        return api_error(StatusCode::UNAUTHORIZED, "invalid password");
    }
    Json(json!({
        "role":"operator",
        "token":state.operator_token.as_deref(),
        "paused":state.paused.load(Ordering::SeqCst),
    }))
    .into_response()
}

async fn session(State(state): State<ApiState>, headers: HeaderMap) -> Json<Value> {
    Json(json!({
        "role":if is_operator(&state, &headers) { "operator" } else { "guest" },
        "login_enabled":state.operator_password.is_some(),
        "paused":state.paused.load(Ordering::SeqCst),
    }))
}

async fn pause(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    set_paused(state, headers, true).await
}

async fn resume(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    set_paused(state, headers, false).await
}

async fn reset_risk_guard(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let Some(actor) = operator_actor(&state, &headers) else {
        return api_error(StatusCode::UNAUTHORIZED, "operator login required");
    };
    let requested_ms = chrono::Utc::now().timestamp_millis();
    let (response_tx, response_rx) = oneshot::channel();
    if state
        .command_tx
        .send(ControlCommand::ResetRiskGuard {
            actor,
            requested_ms,
            response: response_tx,
        })
        .await
        .is_err()
    {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "strategy runtime is unavailable",
        );
    }
    match tokio::time::timeout(std::time::Duration::from_secs(10), response_rx).await {
        Ok(Ok(Ok(value))) => Json(value).into_response(),
        Ok(Ok(Err(error))) => api_error(StatusCode::CONFLICT, &error),
        Ok(Err(_)) => api_error(StatusCode::SERVICE_UNAVAILABLE, "strategy runtime stopped"),
        Err(_) => api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "risk reset is still pending; refresh strategy status",
        ),
    }
}

async fn set_paused(state: ApiState, headers: HeaderMap, paused: bool) -> Response {
    let Some(actor) = operator_actor(&state, &headers) else {
        return api_error(StatusCode::UNAUTHORIZED, "operator login required");
    };
    let previous = state.paused.swap(paused, Ordering::SeqCst);
    let action = if paused {
        "strategy_paused"
    } else {
        "strategy_resumed"
    };
    if previous != paused
        && state
            .command_tx
            .send(ControlCommand::Audit {
                action,
                actor,
                requested_ms: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .is_err()
    {
        state.paused.store(previous, Ordering::SeqCst);
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "strategy runtime is unavailable",
        );
    }
    Json(json!({"ok":true,"paused":paused})).into_response()
}

async fn manual_close(
    State(state): State<ApiState>,
    AxumPath(symbol): AxumPath<String>,
    headers: HeaderMap,
    Json(input): Json<ManualCloseRequest>,
) -> Response {
    let Some(actor) = operator_actor(&state, &headers) else {
        return api_error(StatusCode::UNAUTHORIZED, "operator login required");
    };
    let symbol = symbol.trim().to_ascii_uppercase();
    if !valid_symbol(&symbol) {
        return api_error(StatusCode::BAD_REQUEST, "invalid symbol");
    }
    let note = input
        .note
        .map(|value| value.trim().chars().take(240).collect::<String>())
        .filter(|value| !value.is_empty());
    let (response_tx, response_rx) = oneshot::channel();
    if state
        .command_tx
        .send(ControlCommand::ManualClose {
            symbol,
            note,
            actor,
            requested_ms: chrono::Utc::now().timestamp_millis(),
            response: response_tx,
        })
        .await
        .is_err()
    {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "strategy runtime is unavailable",
        );
    }
    match tokio::time::timeout(std::time::Duration::from_secs(30), response_rx).await {
        Ok(Ok(Ok(value))) => Json(value).into_response(),
        Ok(Ok(Err(error))) => api_error(StatusCode::CONFLICT, &error),
        Ok(Err(_)) => api_error(StatusCode::SERVICE_UNAVAILABLE, "strategy runtime stopped"),
        Err(_) => api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "manual close is still pending; check the position and activity ledger",
        ),
    }
}

fn control_status(state: &ApiState) -> Value {
    json!({
        "paused":state.paused.load(Ordering::SeqCst),
        "operator_login_enabled":state.operator_password.is_some(),
    })
}

fn is_operator(state: &ApiState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.operator_token.as_deref() else {
        return false;
    };
    bearer(headers).is_some_and(|actual| constant_time_eq(actual.as_bytes(), expected.as_bytes()))
}

fn operator_actor(state: &ApiState, headers: &HeaderMap) -> Option<String> {
    let token = bearer(headers)?;
    is_operator(state, headers).then(|| format!("operator:{}", &hex_digest(token.as_bytes())[..12]))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn api_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error":message}))).into_response()
}

fn valid_symbol(value: &str) -> bool {
    value.len() >= 5
        && value.len() <= 24
        && value.ends_with("USDT")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn hex_digest(value: &[u8]) -> String {
    use std::fmt::Write;
    Sha256::digest(value)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

fn reconcile_closed_positions(status: &mut Value, history_path: &Path) {
    let status_ms = status["as_of_ms"].as_i64().unwrap_or_default();
    let Ok((events, _)) = reverse_jsonl_page(history_path, 500, None, is_trade_event) else {
        return;
    };
    let mut latest = BTreeMap::<String, &Value>::new();
    for event in &events {
        let event_ms = event["payload"]["ts_ms"]
            .as_i64()
            .or_else(|| event["recorded_ms"].as_i64())
            .unwrap_or_default();
        if event_ms <= status_ms {
            continue;
        }
        if !matches!(
            event["kind"].as_str(),
            Some("exchange_entry" | "exchange_exit")
        ) {
            continue;
        }
        if let Some(symbol) = event["payload"]["symbol"].as_str() {
            latest.entry(symbol.to_string()).or_insert(event);
        }
    }
    let Some(positions) = status["positions"].as_object_mut() else {
        return;
    };
    for (symbol, event) in latest {
        if event["kind"].as_str() == Some("exchange_exit") {
            positions.remove(&symbol);
        }
    }
    let open_positions = positions.len();
    let gross_exposure = positions
        .values()
        .filter_map(|position| position["current_notional_usd"].as_f64())
        .map(f64::abs)
        .sum::<f64>();
    status["account"]["open_positions"] = json!(open_positions);
    status["account"]["gross_exposure_usd"] = json!(gross_exposure);
}

async fn events(State(state): State<ApiState>, Query(query): Query<EventQuery>) -> Response {
    jsonl_response(&state.journal_path, query.limit.clamp(1, 500), 1024 * 1024)
}

async fn history(State(state): State<ApiState>, Query(query): Query<EventQuery>) -> Response {
    jsonl_response(
        &state.history_path,
        query.limit.clamp(1, 20_000),
        8 * 1024 * 1024,
    )
}

async fn trades(State(state): State<ApiState>, Query(query): Query<PageQuery>) -> Response {
    let run_id = query.run_id;
    paged_jsonl_response(
        &state.history_path,
        query.limit.clamp(1, 100),
        query.before,
        move |value| {
            is_trade_event(value)
                && run_id.as_ref().is_none_or(|expected| {
                    value["payload"]["run_id"].as_str() == Some(expected.as_str())
                })
        },
    )
}

async fn equity(State(state): State<ApiState>, Query(query): Query<PageQuery>) -> Response {
    let run_id = query.run_id;
    match reverse_jsonl_page(
        &state.history_path,
        query.limit.clamp(2, 2_000),
        query.before,
        move |value| {
            value["kind"].as_str() == Some("exchange_equity")
                && run_id.as_ref().is_none_or(|expected| {
                    value["payload"]["runtime"]["run_id"].as_str() == Some(expected.as_str())
                })
        },
    ) {
        Ok((events, next_cursor)) => {
            let events: Vec<_> = events.into_iter().map(compact_equity_event).collect();
            let events = downsample_newest_first(events, 500);
            Json(json!({
                "events": events,
                "next_cursor": next_cursor,
                "has_more": next_cursor.is_some(),
            }))
            .into_response()
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Json(json!({
            "events": [],
            "next_cursor": null,
            "has_more": false,
        }))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

fn compact_equity_event(value: Value) -> Value {
    json!({
        "recorded_ms": value["recorded_ms"],
        "kind": "exchange_equity",
        "payload": {
            "ts_ms": value["payload"]["ts_ms"],
            "equity_usd": value["payload"]["equity_usd"],
            "run_id": value["payload"]["runtime"]["run_id"],
        }
    })
}

fn downsample_newest_first(values: Vec<Value>, limit: usize) -> Vec<Value> {
    if values.len() <= limit || limit < 2 {
        return values;
    }
    let last = values.len() - 1;
    (0..limit)
        .map(|index| values[index * last / (limit - 1)].clone())
        .collect()
}

fn is_trade_event(value: &Value) -> bool {
    matches!(
        value["kind"].as_str(),
        Some(
            "exchange_entry"
                | "exchange_entry_attempt_closed"
                | "exchange_partial_exit"
                | "exchange_runner_activated"
                | "exchange_exit"
                | "exchange_exit_requested"
                | "exchange_entry_canceled"
                | "exchange_plan_rejected"
                | "exchange_order_rejected"
                | "operator_manual_close_failed"
        )
    )
}

fn paged_jsonl_response(
    path: &Path,
    limit: usize,
    before: Option<u64>,
    predicate: impl Fn(&Value) -> bool,
) -> Response {
    match reverse_jsonl_page(path, limit, before, predicate) {
        Ok((events, next_cursor)) => Json(json!({
            "events": events,
            "next_cursor": next_cursor,
            "has_more": next_cursor.is_some(),
        }))
        .into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Json(json!({
            "events": [],
            "next_cursor": null,
            "has_more": false,
        }))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

fn jsonl_response(path: &Path, limit: usize, max_bytes: u64) -> Response {
    match tail_jsonl(path, limit, max_bytes) {
        Ok(values) => Json(json!({"events": values})).into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Json(json!({"events": []})).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

fn read_json(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn tail_jsonl(path: &Path, limit: usize, max_bytes: u64) -> std::io::Result<Vec<Value>> {
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    // Bounded reads keep UI polling independent of the long-running raw journal.
    let start = len.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let mut values: Vec<Value> = text
        .lines()
        .skip(usize::from(start > 0))
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    if values.len() > limit {
        values.drain(..values.len() - limit);
    }
    values.reverse();
    Ok(values)
}

/// Reads matching JSONL records newest-first without loading the full ledger.
/// `before` is the byte offset of the oldest record from the previous page.
fn reverse_jsonl_page(
    path: &Path,
    limit: usize,
    before: Option<u64>,
    predicate: impl Fn(&Value) -> bool,
) -> std::io::Result<(Vec<Value>, Option<u64>)> {
    const BLOCK_BYTES: u64 = 64 * 1024;
    let mut file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut cursor = before.unwrap_or(file_len).min(file_len);
    let mut carry = Vec::new();
    let mut matches = Vec::with_capacity(limit + 1);

    while cursor > 0 && matches.len() <= limit {
        let start = cursor.saturating_sub(BLOCK_BYTES);
        let mut block = vec![0; (cursor - start) as usize];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut block)?;
        block.extend_from_slice(&carry);

        let (complete_start, next_carry) = if start == 0 {
            (0, Vec::new())
        } else if let Some(index) = block.iter().position(|byte| *byte == b'\n') {
            (index + 1, block[..=index].to_vec())
        } else {
            carry = block;
            cursor = start;
            continue;
        };

        let complete = &block[complete_start..];
        let mut line_start = complete_start;
        let mut parsed = Vec::new();
        for line in complete.split(|byte| *byte == b'\n') {
            if !line.is_empty() {
                if let Ok(value) = serde_json::from_slice::<Value>(line) {
                    parsed.push((start + line_start as u64, value));
                }
            }
            line_start += line.len() + 1;
        }
        for (offset, value) in parsed.into_iter().rev() {
            if predicate(&value) {
                matches.push((offset, value));
                if matches.len() > limit {
                    break;
                }
            }
        }
        carry = next_carry;
        cursor = start;
    }

    let has_more = matches.len() > limit;
    matches.truncate(limit);
    let next_cursor = has_more
        .then(|| matches.last().map(|(offset, _)| *offset))
        .flatten();
    Ok((
        matches.into_iter().map(|(_, value)| value).collect(),
        next_cursor,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_returns_newest_first_and_respects_limit() {
        let path = std::env::temp_dir().join(format!("greed-monitor-{}.jsonl", std::process::id()));
        std::fs::write(&path, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n").unwrap();
        let values = tail_jsonl(&path, 2, 1024).unwrap();
        assert_eq!(values[0]["n"], 3);
        assert_eq!(values[1]["n"], 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn reverse_page_filters_paginates_and_keeps_newest_first() {
        let path =
            std::env::temp_dir().join(format!("greed-monitor-page-{}.jsonl", std::process::id()));
        std::fs::write(
            &path,
            concat!(
                "{\"n\":1,\"kind\":\"trade\"}\n",
                "{\"n\":2,\"kind\":\"equity\"}\n",
                "{\"n\":3,\"kind\":\"trade\"}\n",
                "{\"n\":4,\"kind\":\"trade\"}\n",
                "{\"n\":5,\"kind\":\"trade\"}\n"
            ),
        )
        .unwrap();
        let filter = |value: &Value| value["kind"] == "trade";
        let (first, cursor) = reverse_jsonl_page(&path, 2, None, filter).unwrap();
        assert_eq!(
            first
                .iter()
                .map(|value| value["n"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![5, 4]
        );
        let (second, cursor) = reverse_jsonl_page(&path, 2, cursor, filter).unwrap();
        assert_eq!(
            second
                .iter()
                .map(|value| value["n"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 1]
        );
        assert!(cursor.is_none());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn equity_response_omits_large_runtime_payloads() {
        let compact = compact_equity_event(json!({
            "recorded_ms": 10,
            "kind": "exchange_equity",
            "payload": {
                "ts_ms": 9,
                "equity_usd": 2042.06,
                "execution": {"large": [1,2,3]},
                "runtime": {"run_id": "session"}
            }
        }));
        assert_eq!(compact["payload"]["equity_usd"], 2042.06);
        assert!(compact["payload"].get("execution").is_none());
        assert!(compact["payload"].get("runtime").is_none());
    }

    #[test]
    fn equity_downsampling_preserves_newest_and_oldest_points() {
        let values = (0..2_000).map(|value| json!(value)).collect::<Vec<_>>();
        let sampled = downsample_newest_first(values, 500);
        assert_eq!(sampled.len(), 500);
        assert_eq!(sampled.first(), Some(&json!(0)));
        assert_eq!(sampled.last(), Some(&json!(1_999)));
    }

    #[test]
    fn status_reconciliation_removes_a_position_closed_after_snapshot() {
        let path = std::env::temp_dir().join(format!(
            "greed-monitor-reconcile-{}.jsonl",
            std::process::id()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"recorded_ms\":110,\"kind\":\"exchange_entry\",\"payload\":{\"ts_ms\":110,\"symbol\":\"VETUSDT\"}}\n",
                "{\"recorded_ms\":120,\"kind\":\"exchange_exit\",\"payload\":{\"ts_ms\":120,\"symbol\":\"VETUSDT\"}}\n"
            ),
        )
        .unwrap();
        let mut status = json!({
            "as_of_ms":100,
            "positions":{"VETUSDT":{"current_notional_usd":61.0}},
            "account":{"open_positions":1,"gross_exposure_usd":61.0}
        });
        reconcile_closed_positions(&mut status, &path);
        assert!(status["positions"].as_object().unwrap().is_empty());
        assert_eq!(status["account"]["open_positions"], 0);
        assert_eq!(status["account"]["gross_exposure_usd"], 0.0);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn operator_auth_helpers_reject_prefixes_and_invalid_symbols() {
        assert!(constant_time_eq(b"correct horse", b"correct horse"));
        assert!(!constant_time_eq(b"correct", b"correct horse"));
        assert!(!constant_time_eq(b"wrong horse", b"correct horse"));
        assert!(valid_symbol("PROMUSDT"));
        assert!(valid_symbol("1000PEPEUSDT"));
        assert!(!valid_symbol("promusdt"));
        assert!(!valid_symbol("BTCUSDC"));
        assert!(!valid_symbol("BTC/USDT"));
    }

    #[test]
    fn filled_attempt_exit_is_exposed_as_a_trade_event() {
        assert!(is_trade_event(&json!({
            "kind":"exchange_entry_attempt_closed",
            "payload":{
                "candidate_id":"trend_continuation:NEARUSDT:1",
                "attempt_entry_quantity":229.0,
                "attempt_net_pnl_usd":1.49
            }
        })));
    }
}
