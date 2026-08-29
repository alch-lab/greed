use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::BufRead,
};

#[derive(Debug, Clone, Default)]
struct Performance {
    entries: u64,
    partial_exits: u64,
    completed_trades: u64,
    wins: u64,
    losses: u64,
    fees_usd: f64,
    net_realized_pnl_usd: f64,
    gross_profit_usd: f64,
    gross_loss_usd: f64,
    total_hold_ms: i64,
}

impl Performance {
    fn entry(&mut self, fee: f64) {
        self.entries += 1;
        self.fees_usd += fee;
        self.net_realized_pnl_usd -= fee;
    }
    fn exit_leg(&mut self, pnl: f64, fee: f64, partial: bool) {
        self.fees_usd += fee;
        self.net_realized_pnl_usd += pnl;
        if partial {
            self.partial_exits += 1;
        }
    }
    fn complete(&mut self, trade_pnl: f64, hold_ms: i64) {
        self.completed_trades += 1;
        self.total_hold_ms += hold_ms.max(0);
        if trade_pnl > 0.0 {
            self.wins += 1;
            self.gross_profit_usd += trade_pnl;
        } else if trade_pnl < 0.0 {
            self.losses += 1;
            self.gross_loss_usd += -trade_pnl;
        }
    }
    fn value(&self) -> Value {
        serde_json::json!({
            "entries":self.entries,
            "partial_exits":self.partial_exits,
            "completed_trades":self.completed_trades,
            "wins":self.wins,
            "losses":self.losses,
            "win_rate":(self.completed_trades>0).then_some(self.wins as f64/self.completed_trades as f64),
            "fees_usd":self.fees_usd,
            "net_realized_pnl_usd":self.net_realized_pnl_usd,
            "profit_factor":(self.gross_loss_usd>0.0).then_some(self.gross_profit_usd/self.gross_loss_usd),
            "average_hold_minutes":(self.completed_trades>0).then_some(self.total_hold_ms as f64/self.completed_trades as f64/60_000.0),
        })
    }
}

#[derive(Debug, Clone)]
struct OpenTrade {
    sleeve: String,
    recipe: String,
    side: String,
    entry_ms: i64,
    net_pnl_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct FunnelStats {
    unique_candidates: u64,
    pass: u64,
    unknown: u64,
    block: u64,
    plans: u64,
    entries: u64,
    plan_rejections: u64,
    blockers: BTreeMap<String, u64>,
    stopped_stages: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct DailyPerformance {
    entries: u64,
    exit_legs: u64,
    fees_usd: f64,
    net_realized_pnl_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct EquityRisk {
    observations: u64,
    min_equity_usd: Option<f64>,
    max_equity_usd: Option<f64>,
    max_drawdown_pct: f64,
    max_daily_loss_pct: f64,
}

impl EquityRisk {
    fn observe(&mut self, value: &Value) {
        let Some(equity) = value["equity_usd"].as_f64() else {
            return;
        };
        self.observations += 1;
        self.min_equity_usd = Some(self.min_equity_usd.map_or(equity, |old| old.min(equity)));
        self.max_equity_usd = Some(self.max_equity_usd.map_or(equity, |old| old.max(equity)));
        self.max_drawdown_pct = self
            .max_drawdown_pct
            .max(value["drawdown_pct"].as_f64().unwrap_or(0.0));
        self.max_daily_loss_pct = self
            .max_daily_loss_pct
            .max(value["daily_loss_pct"].as_f64().unwrap_or(0.0));
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct EndpointTotals {
    requests: u64,
    successes: u64,
    failures: u64,
    rate_limits: u64,
    total_latency_ms: u64,
    max_latency_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct TelemetryTotals {
    requests: u64,
    successes: u64,
    failures: u64,
    rate_limits: u64,
    retries: u64,
    fallback_attempts: u64,
    fallback_successes: u64,
    total_latency_ms: u64,
    max_latency_ms: u64,
    frames_requested: u64,
    frames_succeeded: u64,
    frames_failed: u64,
    latest_used_weight_1m: Option<u64>,
    max_used_weight_1m: u64,
    endpoints: BTreeMap<String, EndpointTotals>,
}

impl TelemetryTotals {
    fn from_value(value: &Value) -> Self {
        let n = |key: &str| value[key].as_u64().unwrap_or(0);
        let endpoints = value["endpoints"]
            .as_object()
            .map(|values| {
                values
                    .iter()
                    .map(|(key, value)| {
                        let n = |name: &str| value[name].as_u64().unwrap_or(0);
                        (
                            key.clone(),
                            EndpointTotals {
                                requests: n("requests"),
                                successes: n("successes"),
                                failures: n("failures"),
                                rate_limits: n("rate_limits"),
                                total_latency_ms: n("total_latency_ms"),
                                max_latency_ms: n("max_latency_ms"),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            requests: n("requests"),
            successes: n("successes"),
            failures: n("failures"),
            rate_limits: n("rate_limits"),
            retries: n("retries"),
            fallback_attempts: n("fallback_attempts"),
            fallback_successes: n("fallback_successes"),
            total_latency_ms: n("total_latency_ms"),
            max_latency_ms: n("max_latency_ms"),
            frames_requested: n("frames_requested"),
            frames_succeeded: n("frames_succeeded"),
            frames_failed: n("frames_failed"),
            latest_used_weight_1m: value["latest_used_weight_1m"].as_u64(),
            max_used_weight_1m: n("max_used_weight_1m"),
            endpoints,
        }
    }
    fn add(&mut self, other: &Self) {
        self.requests += other.requests;
        self.successes += other.successes;
        self.failures += other.failures;
        self.rate_limits += other.rate_limits;
        self.retries += other.retries;
        self.fallback_attempts += other.fallback_attempts;
        self.fallback_successes += other.fallback_successes;
        self.total_latency_ms += other.total_latency_ms;
        self.max_latency_ms = self.max_latency_ms.max(other.max_latency_ms);
        self.frames_requested += other.frames_requested;
        self.frames_succeeded += other.frames_succeeded;
        self.frames_failed += other.frames_failed;
        self.latest_used_weight_1m = other.latest_used_weight_1m.or(self.latest_used_weight_1m);
        self.max_used_weight_1m = self.max_used_weight_1m.max(other.max_used_weight_1m);
        for (key, value) in &other.endpoints {
            let endpoint = self.endpoints.entry(key.clone()).or_default();
            endpoint.requests += value.requests;
            endpoint.successes += value.successes;
            endpoint.failures += value.failures;
            endpoint.rate_limits += value.rate_limits;
            endpoint.total_latency_ms += value.total_latency_ms;
            endpoint.max_latency_ms = endpoint.max_latency_ms.max(value.max_latency_ms);
        }
    }
}

pub fn build(path: &str) -> Result<Value> {
    let file = std::fs::File::open(path).with_context(|| format!("open journal {path}"))?;
    let mut records = 0u64;
    let mut frame_errors = 0u64;
    let mut successful_frames = 0u64;
    let mut runner_starts = 0u64;
    let mut candles = BTreeSet::new();
    let mut candidate_ids = BTreeSet::new();
    let mut total = Performance::default();
    let mut sleeves: BTreeMap<String, Performance> = BTreeMap::new();
    let mut recipes: BTreeMap<String, Performance> = BTreeMap::new();
    let mut sides: BTreeMap<String, Performance> = BTreeMap::new();
    let mut recipe_sides: BTreeMap<String, Performance> = BTreeMap::new();
    let mut sleeve_funnels: BTreeMap<String, FunnelStats> = BTreeMap::new();
    let mut recipe_funnels: BTreeMap<String, FunnelStats> = BTreeMap::new();
    let mut diagnostic_states: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut diagnostic_reasons: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    let mut open_trades: BTreeMap<String, OpenTrade> = BTreeMap::new();
    let mut first_ms = None;
    let mut last_ms = None;
    let mut last_frame_ms = None;
    let mut max_frame_gap_ms = 0i64;
    let mut versions = BTreeSet::new();
    let mut config_hashes = BTreeSet::new();
    let mut daily: BTreeMap<String, DailyPerformance> = BTreeMap::new();
    let mut portfolio_risk = EquityRisk::default();
    let mut telemetry = TelemetryTotals::default();
    let mut current_run_telemetry = TelemetryTotals::default();
    let mut stream_observations = 0u64;
    let mut stream_offline_observations = 0u64;
    let mut stream_max_reconnects = 0u64;
    let mut stream_max_parse_errors = 0u64;
    let mut max_stream_message_gap_ms = 0i64;

    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(&line)?;
        records += 1;
        let recorded_ms = record["recorded_ms"].as_i64();
        first_ms = first_ms.or(recorded_ms);
        last_ms = recorded_ms.or(last_ms);
        let kind = record["kind"].as_str().unwrap_or("unknown");
        let payload = &record["payload"];
        match kind {
            "runner_start" => {
                if runner_starts > 0 {
                    telemetry.add(&current_run_telemetry);
                    current_run_telemetry = TelemetryTotals::default();
                }
                runner_starts += 1;
                if let Some(value) = payload["runtime"]["git_commit"].as_str() {
                    versions.insert(value.to_owned());
                }
                if let Some(value) = payload["runtime"]["config_hash"].as_str() {
                    config_hashes.insert(value.to_owned());
                }
            }
            "frame_error" => frame_errors += 1,
            "data_health" => {
                current_run_telemetry = TelemetryTotals::from_value(&payload["telemetry"]);
                if let Some(stream) = payload.get("stream").filter(|value| value.is_object()) {
                    stream_observations += 1;
                    if !stream["radar_connected"].as_bool().unwrap_or(false)
                        || !stream["market_connected"].as_bool().unwrap_or(false)
                        || stream
                            .get("trade_connected")
                            .is_some_and(|value| !value.as_bool().unwrap_or(false))
                        || !stream["public_connected"].as_bool().unwrap_or(false)
                    {
                        stream_offline_observations += 1;
                    }
                    stream_max_reconnects = stream_max_reconnects
                        .max(stream["reconnects"].as_u64().unwrap_or_default());
                    stream_max_parse_errors = stream_max_parse_errors
                        .max(stream["parse_errors"].as_u64().unwrap_or_default());
                    if let Some(now) = recorded_ms {
                        for key in [
                            "last_radar_message_ms",
                            "last_market_message_ms",
                            "last_trade_message_ms",
                            "last_public_message_ms",
                        ] {
                            if let Some(message_ms) = stream[key].as_i64() {
                                max_stream_message_gap_ms =
                                    max_stream_message_gap_ms.max(now - message_ms);
                            }
                        }
                    }
                }
            }
            "market_candle" => {
                candles.insert(format!(
                    "{}:{:?}:{}:{}",
                    payload["symbol"].as_str().unwrap_or(""),
                    payload["market"],
                    payload["interval_ms"].as_i64().unwrap_or_default(),
                    payload["bar"]["close_ms"].as_i64().unwrap_or(0)
                ));
            }
            "graph_evaluation" => {
                successful_frames += 1;
                if let Some(ts) = recorded_ms {
                    if let Some(previous) = last_frame_ms {
                        max_frame_gap_ms = max_frame_gap_ms.max(ts - previous);
                    }
                    last_frame_ms = Some(ts);
                }
                record_candidates(
                    payload,
                    &mut candidate_ids,
                    &mut sleeve_funnels,
                    &mut recipe_funnels,
                );
                record_plans(payload, &mut sleeve_funnels, &mut recipe_funnels);
                record_diagnostics(payload, &mut diagnostic_states, &mut diagnostic_reasons);
            }
            "exchange_entry" => {
                record_daily(payload, true, &mut daily);
                record_entry(
                    payload,
                    &mut total,
                    &mut sleeves,
                    &mut recipes,
                    &mut sides,
                    &mut recipe_sides,
                    &mut sleeve_funnels,
                    &mut recipe_funnels,
                    &mut open_trades,
                );
            }
            "exchange_partial_exit" | "exchange_exit" => {
                record_daily(payload, false, &mut daily);
                record_exit(
                    payload,
                    kind == "exchange_partial_exit",
                    &mut total,
                    &mut sleeves,
                    &mut recipes,
                    &mut sides,
                    &mut recipe_sides,
                    &mut open_trades,
                );
            }
            "exchange_plan_rejected" | "exchange_order_rejected" => {
                let (sleeve, recipe) = classify(payload);
                for stats in [
                    sleeve_funnels.entry(sleeve).or_default(),
                    recipe_funnels.entry(recipe).or_default(),
                ] {
                    stats.plan_rejections += 1;
                    let reason = payload["reason"].as_str().unwrap_or("unknown");
                    *stats.blockers.entry(reason.into()).or_default() += 1;
                }
            }
            "exchange_equity" => {
                portfolio_risk.observe(payload);
            }
            _ => {}
        }
    }
    telemetry.add(&current_run_telemetry);
    let sleeve_values: BTreeMap<_, _> = sleeves
        .iter()
        .map(|(key, value)| (key, value.value()))
        .collect();
    let recipe_values: BTreeMap<_, _> = recipes
        .iter()
        .map(|(key, value)| (key, value.value()))
        .collect();
    let side_values: BTreeMap<_, _> = sides
        .iter()
        .map(|(key, value)| (key, value.value()))
        .collect();
    let recipe_side_values: BTreeMap<_, _> = recipe_sides
        .iter()
        .map(|(key, value)| (key, value.value()))
        .collect();
    let observed_frames = successful_frames + frame_errors;
    Ok(serde_json::json!({
        "journal":path,
        "first_recorded_ms":first_ms,
        "last_recorded_ms":last_ms,
        "duration_hours":first_ms.zip(last_ms).map(|(first,last)|(last-first) as f64/3_600_000.0),
        "records":records,
        "runner_starts":runner_starts,
        "versions":versions,
        "config_hashes":config_hashes,
        "unique_market_candles":candles.len(),
        "successful_frames":successful_frames,
        "frame_errors":frame_errors,
        "frame_success_rate":(observed_frames>0).then_some(successful_frames as f64/observed_frames as f64),
        "max_frame_gap_minutes":max_frame_gap_ms as f64/60_000.0,
        "unique_candidates":candidate_ids.len(),
        "portfolio_performance":total.value(),
        "performance_by_lane":sleeve_values,
        "performance_by_recipe":recipe_values,
        "performance_by_side":side_values,
        "performance_by_recipe_and_side":recipe_side_values,
        "daily_performance":daily,
        "portfolio_risk":portfolio_risk,
        "funnel_by_lane":sleeve_funnels,
        "funnel_by_recipe":recipe_funnels,
        "diagnostic_state_counts":diagnostic_states,
        "diagnostic_reason_counts":diagnostic_reasons,
        "open_trade_count_at_report":open_trades.len(),
        "api_telemetry":telemetry,
        "api_average_latency_ms":(telemetry.requests>0).then_some(telemetry.total_latency_ms as f64/telemetry.requests as f64),
        "stream_health":{
            "observations":stream_observations,
            "offline_observations":stream_offline_observations,
            "availability":(stream_observations>0).then_some(1.0-stream_offline_observations as f64/stream_observations as f64),
            "max_reconnects":stream_max_reconnects,
            "max_parse_errors":stream_max_parse_errors,
            "max_message_gap_seconds":max_stream_message_gap_ms as f64/1_000.0,
        },
    }))
}

fn record_diagnostics(
    payload: &Value,
    states: &mut BTreeMap<String, BTreeMap<String, u64>>,
    reasons: &mut BTreeMap<String, BTreeMap<String, u64>>,
) {
    let Some(artifacts) = payload["artifacts"].as_object() else {
        return;
    };
    for key in [
        "lane.sfp_reversal.status",
        "lane.trend_continuation.status",
        "lane.relative_weakness_short.status",
        "lane.intraday_sweep_reversal.status",
        "lane.burst_exhaustion.status",
        "portfolio.risk",
    ] {
        let Some(value) = artifacts
            .get(key)
            .and_then(|record| record["artifact"]["value"].as_object())
        else {
            continue;
        };
        let state = value
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        *states
            .entry(key.into())
            .or_default()
            .entry(state.into())
            .or_default() += 1;
        if let Some(values) = value.get("reasons").and_then(Value::as_array) {
            for reason in values.iter().filter_map(Value::as_str) {
                *reasons
                    .entry(key.into())
                    .or_default()
                    .entry(reason.into())
                    .or_default() += 1;
            }
        }
    }
}

fn record_daily(payload: &Value, entry: bool, daily: &mut BTreeMap<String, DailyPerformance>) {
    let ts_ms = payload["ts_ms"].as_i64().unwrap_or(0);
    let day = chrono::DateTime::from_timestamp_millis(ts_ms)
        .map(|value| {
            value
                .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).expect("valid offset"))
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_else(|| "unknown".into());
    let stats = daily.entry(day).or_default();
    let fee = payload["fee_usd"].as_f64().unwrap_or(0.0);
    stats.fees_usd += fee;
    if entry {
        stats.entries += 1;
        stats.net_realized_pnl_usd -= fee;
    } else {
        stats.exit_legs += 1;
        stats.net_realized_pnl_usd += payload["pnl_usd"].as_f64().unwrap_or(0.0);
    }
}

fn classify(payload: &Value) -> (String, String) {
    let recipe = payload["recipe"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| recipe_from_id(payload["candidate_id"].as_str().unwrap_or("")).into());
    (lane_for_recipe(&recipe).into(), recipe)
}

fn recipe_from_id(id: &str) -> &'static str {
    if id.contains("sfp_reversal") {
        "sfp_reversal"
    } else if id.contains("trend_continuation") {
        "trend_continuation"
    } else if id.contains("relative_weakness_short") {
        "relative_weakness_short"
    } else if id.contains("intraday_sweep_reversal") {
        "intraday_sweep_reversal"
    } else if id.contains("burst_exhaustion") {
        "burst_exhaustion"
    } else {
        "unknown"
    }
}

fn lane_for_recipe(recipe: &str) -> &'static str {
    match recipe {
        "sfp_reversal" => "sfp_reversal",
        "trend_continuation" => "trend_continuation",
        "relative_weakness_short" => "relative_weakness_short",
        "intraday_sweep_reversal" => "intraday_sweep_reversal",
        "burst_exhaustion" => "burst_exhaustion",
        _ => "unknown",
    }
}

fn record_candidates(
    payload: &Value,
    ids: &mut BTreeSet<String>,
    sleeves: &mut BTreeMap<String, FunnelStats>,
    recipes: &mut BTreeMap<String, FunnelStats>,
) {
    let Some(artifacts) = payload["artifacts"].as_object() else {
        return;
    };
    for record in artifacts.values() {
        if record["artifact"]["type"] != "candidate" {
            continue;
        }
        let value = &record["artifact"]["value"];
        let id = value["id"].as_str().unwrap_or("").to_owned();
        if id.is_empty() || !ids.insert(id) {
            continue;
        }
        let recipe = value["recipe"].as_str().unwrap_or("unknown").to_owned();
        let sleeve = lane_for_recipe(&recipe).to_owned();
        for stats in [
            sleeves.entry(sleeve).or_default(),
            recipes.entry(recipe).or_default(),
        ] {
            stats.unique_candidates += 1;
            match value["verdict"].as_str().unwrap_or("unknown") {
                "pass" => stats.pass += 1,
                "block" => stats.block += 1,
                _ => stats.unknown += 1,
            }
            if let Some(blockers) = value["blockers"].as_array() {
                for blocker in blockers.iter().filter_map(Value::as_str) {
                    *stats.blockers.entry(blocker.into()).or_default() += 1;
                }
            }
        }
    }
}

fn record_plans(
    payload: &Value,
    sleeves: &mut BTreeMap<String, FunnelStats>,
    recipes: &mut BTreeMap<String, FunnelStats>,
) {
    let Some(artifacts) = payload["artifacts"].as_object() else {
        return;
    };
    for record in artifacts.values() {
        if record["artifact"]["type"] != "position_plan" {
            continue;
        }
        let value = &record["artifact"]["value"];
        let recipe = recipe_from_id(value["candidate_id"].as_str().unwrap_or("")).to_owned();
        let sleeve = lane_for_recipe(&recipe).to_owned();
        sleeves.entry(sleeve).or_default().plans += 1;
        recipes.entry(recipe).or_default().plans += 1;
    }
}

#[allow(clippy::too_many_arguments)]
fn record_entry(
    payload: &Value,
    total: &mut Performance,
    sleeves: &mut BTreeMap<String, Performance>,
    recipes: &mut BTreeMap<String, Performance>,
    sides: &mut BTreeMap<String, Performance>,
    recipe_sides: &mut BTreeMap<String, Performance>,
    sleeve_funnels: &mut BTreeMap<String, FunnelStats>,
    recipe_funnels: &mut BTreeMap<String, FunnelStats>,
    trades: &mut BTreeMap<String, OpenTrade>,
) {
    let (sleeve, recipe) = classify(payload);
    let fee = payload["fee_usd"].as_f64().unwrap_or(0.0);
    let side = payload["side"].as_str().unwrap_or("unknown").to_owned();
    total.entry(fee);
    sleeves.entry(sleeve.clone()).or_default().entry(fee);
    recipes.entry(recipe.clone()).or_default().entry(fee);
    sides.entry(side.clone()).or_default().entry(fee);
    recipe_sides
        .entry(format!("{recipe}:{side}"))
        .or_default()
        .entry(fee);
    sleeve_funnels.entry(sleeve.clone()).or_default().entries += 1;
    recipe_funnels.entry(recipe.clone()).or_default().entries += 1;
    trades.insert(
        payload["candidate_id"].as_str().unwrap_or("").into(),
        OpenTrade {
            sleeve,
            recipe,
            side,
            entry_ms: payload["ts_ms"].as_i64().unwrap_or(0),
            net_pnl_usd: -fee,
        },
    );
}

#[allow(clippy::too_many_arguments)]
fn record_exit(
    payload: &Value,
    partial: bool,
    total: &mut Performance,
    sleeves: &mut BTreeMap<String, Performance>,
    recipes: &mut BTreeMap<String, Performance>,
    sides: &mut BTreeMap<String, Performance>,
    recipe_sides: &mut BTreeMap<String, Performance>,
    trades: &mut BTreeMap<String, OpenTrade>,
) {
    let id = payload["candidate_id"].as_str().unwrap_or("").to_owned();
    let (fallback_sleeve, fallback_recipe) = classify(payload);
    let pnl = payload["pnl_usd"].as_f64().unwrap_or(0.0);
    let fee = payload["fee_usd"].as_f64().unwrap_or(0.0);
    let trade = trades.entry(id.clone()).or_insert(OpenTrade {
        sleeve: fallback_sleeve,
        recipe: fallback_recipe,
        side: payload["side"].as_str().unwrap_or("unknown").into(),
        entry_ms: payload["ts_ms"].as_i64().unwrap_or(0),
        net_pnl_usd: 0.0,
    });
    trade.net_pnl_usd += pnl;
    total.exit_leg(pnl, fee, partial);
    sleeves
        .entry(trade.sleeve.clone())
        .or_default()
        .exit_leg(pnl, fee, partial);
    recipes
        .entry(trade.recipe.clone())
        .or_default()
        .exit_leg(pnl, fee, partial);
    sides
        .entry(trade.side.clone())
        .or_default()
        .exit_leg(pnl, fee, partial);
    recipe_sides
        .entry(format!("{}:{}", trade.recipe, trade.side))
        .or_default()
        .exit_leg(pnl, fee, partial);
    if !partial {
        let trade = trades.remove(&id).expect("trade inserted above");
        let hold = payload["ts_ms"].as_i64().unwrap_or(0) - trade.entry_ms;
        let recipe_side = format!("{}:{}", trade.recipe, trade.side);
        total.complete(trade.net_pnl_usd, hold);
        sleeves
            .entry(trade.sleeve)
            .or_default()
            .complete(trade.net_pnl_usd, hold);
        recipes
            .entry(trade.recipe)
            .or_default()
            .complete(trade.net_pnl_usd, hold);
        sides
            .entry(trade.side.clone())
            .or_default()
            .complete(trade.net_pnl_usd, hold);
        recipe_sides
            .entry(recipe_side)
            .or_default()
            .complete(trade.net_pnl_usd, hold);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_attributes_trade_and_api_health() {
        let path = std::env::temp_dir().join(format!("greed-report-{}.jsonl", std::process::id()));
        let lines = [
            serde_json::json!({"recorded_ms":1,"kind":"runner_start","payload":{"runtime":{"git_commit":"abc","config_hash":"cfg"}}}),
            serde_json::json!({"recorded_ms":2,"kind":"data_health","payload":{"telemetry":{"requests":10,"successes":9,"failures":1,"rate_limits":1,"retries":1,"frames_requested":1,"frames_succeeded":1}}}),
            serde_json::json!({"recorded_ms":3,"kind":"exchange_entry","payload":{"ts_ms":3,"candidate_id":"trend_continuation:BTCUSDT:3","recipe":"trend_continuation","lane":"trend_continuation","symbol":"BTCUSDT","side":"buy","fee_usd":0.2}}),
            serde_json::json!({"recorded_ms":4,"kind":"exchange_exit","payload":{"ts_ms":64_000,"candidate_id":"trend_continuation:BTCUSDT:3","recipe":"trend_continuation","lane":"trend_continuation","symbol":"BTCUSDT","side":"buy","fee_usd":0.2,"pnl_usd":5.0}}),
            serde_json::json!({"recorded_ms":5,"kind":"exchange_plan_rejected","payload":{"ts_ms":65_000,"candidate_id":"relative_weakness_short:SOLUSDT:1","recipe":"relative_weakness_short","lane":"relative_weakness_short","symbol":"SOLUSDT","side":"sell","reason":"rolling_profit_factor_gate"}}),
        ];
        std::fs::write(
            &path,
            lines
                .iter()
                .map(|value| serde_json::to_string(value).unwrap())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let value = build(path.to_str().unwrap()).unwrap();
        assert_eq!(
            value["performance_by_lane"]["trend_continuation"]["completed_trades"],
            1
        );
        assert_eq!(
            value["performance_by_recipe"]["trend_continuation"]["wins"],
            1
        );
        assert_eq!(value["api_telemetry"]["rate_limits"], 1);
        assert_eq!(
            value["performance_by_recipe_and_side"]["trend_continuation:buy"]["completed_trades"],
            1
        );
        assert_eq!(
            value["funnel_by_recipe"]["relative_weakness_short"]["plan_rejections"],
            1
        );
        std::fs::remove_file(path).unwrap();
    }
}
