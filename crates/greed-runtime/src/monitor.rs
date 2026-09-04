use crate::config::RuntimeConfig;
use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

#[derive(Clone)]
struct ApiState {
    status_path: PathBuf,
    journal_path: PathBuf,
    history_path: PathBuf,
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

pub async fn start(config: &RuntimeConfig) -> Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(&config.http_listen)
        .await
        .with_context(|| format!("bind monitoring API on {}", config.http_listen))?;
    let state = ApiState {
        status_path: config.status_path.clone().into(),
        journal_path: config.journal_path.clone().into(),
        history_path: config.history_path.clone().into(),
    };
    let router = Router::new()
        .route("/api/health", get(health))
        .route("/api/status", get(status))
        .route("/api/events", get(events))
        .route("/api/history", get(history))
        .route("/api/trades", get(trades))
        .route("/api/equity", get(equity))
        .with_state(state);
    Ok(tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(error = %error, "monitoring API stopped");
        }
    }))
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
    match reverse_jsonl_page(
        &state.history_path,
        query.limit.clamp(2, 2_000),
        query.before,
        |value| value["kind"].as_str() == Some("exchange_equity"),
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
                | "exchange_partial_exit"
                | "exchange_runner_activated"
                | "exchange_exit"
                | "exchange_exit_requested"
                | "exchange_entry_canceled"
                | "exchange_plan_rejected"
                | "exchange_order_rejected"
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
}
