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

fn default_limit() -> usize {
    100
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
    json_file(&state.status_path)
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

fn json_file(path: &Path) -> Response {
    match read_json(path) {
        Ok(value) => Json(value).into_response(),
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
}
