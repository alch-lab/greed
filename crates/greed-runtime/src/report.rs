use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::BufRead,
};

pub fn build(path: &str) -> Result<Value> {
    let file = std::fs::File::open(path).with_context(|| format!("open journal {path}"))?;
    let mut records = 0u64;
    let mut frame_errors = 0u64;
    let mut entries = 0u64;
    let mut exits = 0u64;
    let mut partial_exits = 0u64;
    let mut entry_fees = 0.0;
    let mut exit_fees = 0.0;
    let mut exit_net_pnl = 0.0;
    let mut profitable_exit_legs = 0u64;
    let mut losing_exit_legs = 0u64;
    let mut gross_profit = 0.0;
    let mut gross_loss = 0.0;
    let mut candidates = BTreeSet::new();
    let mut candles = BTreeSet::new();
    let mut recipe_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut verdict_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut blocker_counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut first_ms: Option<i64> = None;
    let mut last_ms: Option<i64> = None;

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
            "frame_error" => frame_errors += 1,
            "paper_entry" => {
                entries += 1;
                entry_fees += payload["fee_usd"].as_f64().unwrap_or(0.0);
            }
            "paper_partial_exit" => {
                partial_exits += 1;
                record_exit_leg(
                    payload,
                    &mut exit_net_pnl,
                    &mut exit_fees,
                    &mut profitable_exit_legs,
                    &mut losing_exit_legs,
                    &mut gross_profit,
                    &mut gross_loss,
                );
            }
            "paper_exit" => {
                exits += 1;
                record_exit_leg(
                    payload,
                    &mut exit_net_pnl,
                    &mut exit_fees,
                    &mut profitable_exit_legs,
                    &mut losing_exit_legs,
                    &mut gross_profit,
                    &mut gross_loss,
                );
            }
            "market_candle" => {
                let key = format!(
                    "{}:{:?}:{}",
                    payload["symbol"].as_str().unwrap_or(""),
                    payload["market"],
                    payload["bar"]["close_ms"].as_i64().unwrap_or(0)
                );
                candles.insert(key);
            }
            "graph_evaluation" => {
                if let Some(artifacts) = payload["artifacts"].as_object() {
                    for record in artifacts.values() {
                        if record["artifact"]["type"] != "candidate" {
                            continue;
                        }
                        let value = &record["artifact"]["value"];
                        let id = value["id"].as_str().unwrap_or("").to_owned();
                        if id.is_empty() || !candidates.insert(id) {
                            continue;
                        }
                        *recipe_counts
                            .entry(value["recipe"].as_str().unwrap_or("unknown").into())
                            .or_default() += 1;
                        *verdict_counts
                            .entry(value["verdict"].as_str().unwrap_or("unknown").into())
                            .or_default() += 1;
                        if let Some(blockers) = value["blockers"].as_array() {
                            for blocker in blockers.iter().filter_map(Value::as_str) {
                                *blocker_counts.entry(blocker.into()).or_default() += 1;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(serde_json::json!({
        "journal":path,
        "first_recorded_ms":first_ms,
        "last_recorded_ms":last_ms,
        "duration_hours":first_ms.zip(last_ms).map(|(first,last)|(last-first) as f64/3_600_000.0),
        "records":records,
        "unique_market_candles":candles.len(),
        "frame_errors":frame_errors,
        "unique_candidates":candidates.len(),
        "candidates_by_recipe":recipe_counts,
        "candidates_by_verdict":verdict_counts,
        "blockers":blocker_counts,
        "paper_entries":entries,
        "paper_partial_exits":partial_exits,
        "paper_full_exits":exits,
        "profitable_exit_legs":profitable_exit_legs,
        "losing_exit_legs":losing_exit_legs,
        "entry_fees_usd":entry_fees,
        "exit_fees_usd":exit_fees,
        "recorded_fees_usd":entry_fees+exit_fees,
        "exit_net_pnl_usd_before_entry_fees":exit_net_pnl,
        "net_realized_pnl_usd":exit_net_pnl-entry_fees,
        "exit_leg_profit_factor_after_recorded_fees":if gross_loss+entry_fees > 0.0 {Some(gross_profit/(gross_loss+entry_fees))} else {None}
    }))
}

fn record_exit_leg(
    payload: &Value,
    net_pnl: &mut f64,
    fees: &mut f64,
    profitable: &mut u64,
    losing: &mut u64,
    gross_profit: &mut f64,
    gross_loss: &mut f64,
) {
    let pnl = payload["pnl_usd"].as_f64().unwrap_or(0.0);
    *net_pnl += pnl;
    *fees += payload["fee_usd"].as_f64().unwrap_or(0.0);
    if pnl > 0.0 {
        *profitable += 1;
        *gross_profit += pnl;
    } else if pnl < 0.0 {
        *losing += 1;
        *gross_loss += -pnl;
    }
}
