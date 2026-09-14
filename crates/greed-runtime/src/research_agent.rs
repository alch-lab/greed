use crate::{config::AppConfig, report};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::{fs, path::Path, time::Duration};

const RESEARCH_INSTRUCTIONS: &str = r#"
You are the research reviewer for a cryptocurrency paper-trading system. The Rust
runtime, exchange adapter, hard risk controls, accounting, and deployment are out
of your control. Analyze only the supplied causal records.

Your job is to propose falsifiable experiments, not to explain individual losses
after the fact. Never infer an edge from future data, never optimize on one trade,
and never claim improvement without a deterministic replay and rolling
out-of-sample validation. Include fees, executable bid/ask, slippage, funding,
partial fills, rejected orders, and data-health gaps. Distinguish signal errors,
execution errors, exit errors, and market-regime changes. Prefer no change when
the sample is insufficient. Candidate changes are paper-only and must not weaken
hard account risk limits.
"#;

fn review_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "decision": {"type":"string", "enum":["no_change","run_experiments"]},
            "summary": {"type":"string"},
            "data_quality": {
                "type":"object", "additionalProperties":false,
                "properties": {
                    "usable": {"type":"boolean"},
                    "issues": {"type":"array", "items":{"type":"string"}}
                },
                "required":["usable","issues"]
            },
            "observations": {"type":"array", "items":{"type":"string"}},
            "hypotheses": {
                "type":"array",
                "items": {
                    "type":"object", "additionalProperties":false,
                    "properties": {
                        "id":{"type":"string"},
                        "claim":{"type":"string"},
                        "evidence":{"type":"array","items":{"type":"string"}},
                        "falsification":{"type":"string"}
                    },
                    "required":["id","claim","evidence","falsification"]
                }
            },
            "experiments": {
                "type":"array",
                "items": {
                    "type":"object", "additionalProperties":false,
                    "properties": {
                        "id":{"type":"string"},
                        "hypothesis_id":{"type":"string"},
                        "scope":{"type":"string"},
                        "change":{"type":"string"},
                        "control":{"type":"string"},
                        "minimum_closed_trades":{"type":"integer","minimum":20},
                        "walk_forward_windows":{"type":"integer","minimum":3},
                        "acceptance":{"type":"array","items":{"type":"string"}},
                        "rejection":{"type":"array","items":{"type":"string"}},
                        "risks":{"type":"array","items":{"type":"string"}}
                    },
                    "required":["id","hypothesis_id","scope","change","control",
                        "minimum_closed_trades","walk_forward_windows","acceptance",
                        "rejection","risks"]
                }
            },
            "do_not_change": {"type":"array", "items":{"type":"string"}}
        },
        "required":["decision","summary","data_quality","observations","hypotheses",
            "experiments","do_not_change"]
    })
}

fn compact_status(path: &str) -> Option<Value> {
    let value: Value = serde_json::from_str(&fs::read_to_string(path).ok()?).ok()?;
    Some(json!({
        "as_of_ms": value.get("as_of_ms"),
        "runtime": value.get("runtime"),
        "account": value.get("account"),
        "data_health": value.get("data_health"),
        "lanes": value.get("lanes"),
        "execution": value.get("execution"),
        "recipe_gates": value.get("recipe_gates"),
        "positions": value.get("positions")
    }))
}

fn journal_reports(journal: &str, rotations: usize) -> Vec<Value> {
    let mut paths = (1..=rotations)
        .rev()
        .map(|index| format!("{journal}.{index}"))
        .filter(|path| Path::new(path).is_file())
        .collect::<Vec<_>>();
    if Path::new(journal).is_file() {
        paths.push(journal.to_owned());
    }
    paths
        .into_iter()
        .map(|path| match report::build(&path) {
            Ok(value) => value,
            Err(error) => json!({"journal":path,"error":error.to_string()}),
        })
        .collect()
}

fn extract_output_text(response: &Value) -> Option<&str> {
    response
        .get("choices")?
        .as_array()?
        .first()?
        .get("message")?
        .get("content")?
        .as_str()
}

fn validate_review(review: &Value) -> Result<()> {
    let object = review
        .as_object()
        .context("research review must be a JSON object")?;
    for required in [
        "decision",
        "summary",
        "data_quality",
        "observations",
        "hypotheses",
        "experiments",
        "do_not_change",
    ] {
        if !object.contains_key(required) {
            bail!("research review is missing required field {required}");
        }
    }
    if !matches!(
        review["decision"].as_str(),
        Some("no_change" | "run_experiments")
    ) {
        bail!("research review decision is invalid");
    }
    for experiment in review["experiments"]
        .as_array()
        .context("research review experiments must be an array")?
    {
        if experiment["minimum_closed_trades"]
            .as_u64()
            .unwrap_or_default()
            < 20
            || experiment["walk_forward_windows"]
                .as_u64()
                .unwrap_or_default()
                < 3
        {
            bail!("research experiment violates minimum validation requirements");
        }
    }
    Ok(())
}

fn atomic_json(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| format!("publish {}", path.display()))?;
    Ok(())
}

pub async fn run(
    config: &AppConfig,
    journal: &str,
    status: &str,
    output_dir: &str,
    requested_model: Option<&str>,
    dry_run: bool,
) -> Result<Value> {
    fs::create_dir_all(output_dir).with_context(|| format!("create {output_dir}"))?;
    let generated_ms = Utc::now().timestamp_millis();
    let model = requested_model
        .map(str::to_owned)
        .or_else(|| std::env::var("GREED_RESEARCH_MODEL").ok())
        .unwrap_or_else(|| "glm-5.2".to_owned());
    let snapshot = json!({
        "schema_version": 1,
        "generated_ms": generated_ms,
        "paper_only": true,
        "strategy": config.strategy,
        "portfolio": config.portfolio,
        "journal_reports": journal_reports(journal, config.runtime.journal_rotations),
        "live_status": compact_status(status),
        "constraints": {
            "strategy_behavior_baseline": "da3344b",
            "baseline_note": "signal behavior restored while retaining later execution, accounting, reconnect, and cost-protection fixes",
            "execution_engine_is_immutable": true,
            "hard_risk_limits_are_immutable": true,
            "automatic_live_deployment_allowed": false,
            "candidate_destination": "paper_canary"
        }
    });
    atomic_json(&Path::new(output_dir).join("latest-input.json"), &snapshot)?;
    if dry_run {
        return Ok(json!({
            "status":"dry_run",
            "generated_ms":generated_ms,
            "model":model,
            "input_path":Path::new(output_dir).join("latest-input.json")
        }));
    }

    let api_key = std::env::var("ZHIPU_API_KEY")
        .context("ZHIPU_API_KEY is required for the research agent")?;
    let api_base = std::env::var("GREED_RESEARCH_API_BASE")
        .unwrap_or_else(|_| "https://open.bigmodel.cn/api/paas/v4".to_owned());
    let endpoint = format!("{}/chat/completions", api_base.trim_end_matches('/'));
    let request = json!({
        "model": model,
        "messages": [
            {"role":"system", "content":format!(
                "{RESEARCH_INSTRUCTIONS}\nReturn only valid JSON matching this schema: {}",
                serde_json::to_string(&review_schema()).expect("schema serializes")
            )},
            {"role":"user", "content":serde_json::to_string(&snapshot)?}
        ],
        "temperature": 0.1,
        "stream": false
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;
    let response = client
        .post(&endpoint)
        .header(AUTHORIZATION, format!("Bearer {api_key}"))
        .header(CONTENT_TYPE, "application/json")
        .json(&request)
        .send()
        .await
        .context("call Zhipu Chat Completions API")?;
    let status_code = response.status();
    let body = response.text().await?;
    if !status_code.is_success() {
        let diagnostic = body.chars().take(2_000).collect::<String>();
        bail!(
            "Zhipu Chat Completions API returned {status_code}: {}",
            diagnostic
        );
    }
    let envelope: Value =
        serde_json::from_str(&body).context("parse Zhipu Chat Completions envelope")?;
    let output_text = extract_output_text(&envelope)
        .context("Zhipu Chat Completions returned no assistant content")?;
    let review: Value =
        serde_json::from_str(output_text).context("parse structured research review")?;
    validate_review(&review)?;
    let artifact = json!({
        "schema_version":1,
        "generated_ms":generated_ms,
        "provider":"zhipu_bigmodel",
        "model":model,
        "response_id":envelope.get("id"),
        "review":review
    });
    let timestamped = Path::new(output_dir).join(format!("review-{generated_ms}.json"));
    atomic_json(&timestamped, &artifact)?;
    atomic_json(&Path::new(output_dir).join("latest-review.json"), &artifact)?;
    Ok(json!({
        "status":"completed",
        "generated_ms":generated_ms,
        "model":model,
        "review_path":timestamped
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_structured_output_text() {
        let response = json!({"choices":[{"message":{
            "role":"assistant", "content":"{\"decision\":\"no_change\"}"
        }}]});
        assert_eq!(
            extract_output_text(&response),
            Some("{\"decision\":\"no_change\"}")
        );
    }

    #[test]
    fn schema_forbids_unbounded_changes() {
        let schema = review_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["experiments"]["items"]["properties"]["minimum_closed_trades"]
                ["minimum"],
            20
        );
    }

    #[test]
    fn rejects_reviews_that_weaken_validation_minimums() {
        let review = json!({
            "decision":"run_experiments", "summary":"x",
            "data_quality":{"usable":true,"issues":[]},
            "observations":[], "hypotheses":[], "do_not_change":[],
            "experiments":[{"minimum_closed_trades":5,"walk_forward_windows":1}]
        });
        assert!(validate_review(&review).is_err());
    }
}
