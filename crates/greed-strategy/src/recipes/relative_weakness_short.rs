use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct RelativeWeaknessShortNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl RelativeWeaknessShortNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.relative_weakness_short".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn ema(values: &[&Candle], period: usize) -> Vec<f64> {
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut output = Vec::with_capacity(values.len());
    let mut current = values.first().map_or(0.0, |value| value.close);
    for value in values {
        current = alpha * value.close + (1.0 - alpha) * current;
        output.push(current);
    }
    output
}

fn atr(values: &[&Candle], end: usize, window: usize) -> f64 {
    let start = end.saturating_sub(window.saturating_sub(1));
    let total: f64 = (start..=end)
        .map(|index| {
            let prior = if index == 0 {
                values[index].close
            } else {
                values[index - 1].close
            };
            (values[index].high - values[index].low)
                .max((values[index].high - prior).abs())
                .max((values[index].low - prior).abs())
        })
        .sum();
    total / (end - start + 1) as f64
}

#[derive(Debug, Clone, Copy)]
struct CoreInputs {
    relative_1h: f64,
    relative_4h: f64,
    flow: f64,
    volume_ratio: f64,
    bearish_structure: bool,
    fresh_break: bool,
    btc_return_4h: f64,
}

fn core_ready(input: CoreInputs, config: &LaneConfig) -> bool {
    input.relative_1h <= config.weakness_max_relative_1h
        && input.relative_4h <= config.weakness_max_relative_4h
        && input.flow <= config.weakness_max_flow
        && input.volume_ratio >= config.weakness_min_hour_volume_ratio
        && input.bearish_structure
        && input.fresh_break
        && input.btc_return_4h >= -0.02
}

impl StrategyNode for RelativeWeaknessShortNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let Some(btc) = ctx.frame.instrument("BTCUSDT") else {
            return Ok(vec![status(
                &self.id,
                ctx.frame.as_of_ms,
                "waiting_for_btc_anchor",
                "BTCUSDT 15m anchor is not ready",
                0.0,
                0,
                0,
            )]);
        };
        let btc_closed: Vec<_> = btc
            .perpetual
            .values
            .iter()
            .filter(|value| value.closed)
            .collect();
        let btc_by_close: BTreeMap<_, _> = btc_closed
            .iter()
            .map(|value| (value.close_ms, *value))
            .collect();

        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0u64;
        let mut structure_hits = 0u64;
        for symbol in &self.symbols {
            if symbol == "BTCUSDT" {
                continue;
            }
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let closed: Vec<_> = instrument
                .perpetual
                .values
                .iter()
                .filter(|value| value.closed)
                .collect();
            let required = 97usize.max(self.config.weakness_breakdown_bars + 2);
            if closed.len() < required {
                continue;
            }
            inspected += 1;
            let index = closed.len() - 1;
            let bar = closed[index];
            let age_ms = ctx.frame.as_of_ms - bar.close_ms;
            if age_ms < 0 {
                continue;
            }
            let signal_fresh =
                age_ms <= i64::from(self.config.weakness_max_signal_age_seconds) * 1_000;
            let Some(btc_now) = btc_by_close.get(&bar.close_ms) else {
                continue;
            };
            let Some(btc_1h) = btc_by_close.get(&closed[index - 4].close_ms) else {
                continue;
            };
            let Some(btc_4h) = btc_by_close.get(&closed[index - 16].close_ms) else {
                continue;
            };
            let relative_1h = bar.close / closed[index - 4].close - btc_now.close / btc_1h.close;
            let relative_4h = bar.close / closed[index - 16].close - btc_now.close / btc_4h.close;
            let btc_return_4h = btc_now.close / btc_4h.close - 1.0;
            let hour_volume: f64 = closed[index - 3..=index]
                .iter()
                .map(|value| value.quote_volume)
                .sum();
            let baseline_hour_volume: f64 = closed[index - 96..index]
                .iter()
                .map(|value| value.quote_volume)
                .sum::<f64>()
                / 24.0;
            let volume_ratio = hour_volume / baseline_hour_volume.max(1.0);
            let flow = bar
                .taker_buy_quote
                .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0);
            let ema8 = ema(&closed, 8);
            let ema21 = ema(&closed, 21);
            let ema36 = ema(&closed, 36);
            let bearish_structure = ema8[index] < ema21[index] && ema21[index] < ema36[index];
            let prior_low = closed[index - self.config.weakness_breakdown_bars..index]
                .iter()
                .map(|value| value.low)
                .fold(f64::INFINITY, f64::min);
            let older_low = closed[index - self.config.weakness_breakdown_bars - 1..index - 1]
                .iter()
                .map(|value| value.low)
                .fold(f64::INFINITY, f64::min);
            let fresh_break = bar.close < prior_low && closed[index - 1].close >= older_low;
            structure_hits += u64::from(bearish_structure && fresh_break);

            let mut blockers = Vec::new();
            if !signal_fresh {
                blockers
                    .push("entry window expired; waiting for the next completed 15m candle".into());
            }
            if relative_1h > self.config.weakness_max_relative_1h {
                blockers.push(format!(
                    "1h relative return {:+.2}% / need <= {:+.2}% vs BTC",
                    relative_1h * 100.0,
                    self.config.weakness_max_relative_1h * 100.0
                ));
            }
            if relative_4h > self.config.weakness_max_relative_4h {
                blockers.push(format!(
                    "4h relative return {:+.2}% / need <= {:+.2}% vs BTC",
                    relative_4h * 100.0,
                    self.config.weakness_max_relative_4h * 100.0
                ));
            }
            match flow {
                Some(value) if value > self.config.weakness_max_flow => blockers.push(format!(
                    "15m taker flow {:+.1}% / need <= {:+.1}%",
                    value * 100.0,
                    self.config.weakness_max_flow * 100.0
                )),
                None => blockers.push("15m taker flow is not ready".into()),
                _ => {}
            }
            if volume_ratio < self.config.weakness_min_hour_volume_ratio {
                blockers.push(format!(
                    "1h volume {:.0}% of baseline / need {:.0}%",
                    volume_ratio * 100.0,
                    self.config.weakness_min_hour_volume_ratio * 100.0
                ));
            }
            if !bearish_structure {
                blockers.push("EMA8 / EMA21 / EMA36 bearish structure is not aligned".into());
            }
            if !fresh_break {
                blockers.push(format!(
                    "waiting for a fresh close below the prior {}m low",
                    self.config.weakness_breakdown_bars * 15
                ));
            }
            if btc_return_4h < -0.02 {
                blockers.push("BTC is already in a 4h waterfall; use the trend lane".into());
            }
            match instrument.book.as_ref() {
                None => blockers.push("order book is not ready".into()),
                Some(book) if !book.meta.usable_at(ctx.frame.as_of_ms) => {
                    blockers.push("order book is stale".into())
                }
                Some(book) => {
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    let depth = book.bid_depth_usd.min(book.ask_depth_usd);
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!(
                            "spread {spread:.1} bps / max {:.1} bps",
                            self.config.max_spread_bps
                        ));
                    }
                    if depth < self.config.min_depth_usd {
                        blockers.push(format!(
                            "book depth ${depth:.0} / need ${:.0}",
                            self.config.min_depth_usd
                        ));
                    }
                }
            }
            let current_atr = atr(&closed, index, 20);
            let recent_high = closed[index - 3..=index]
                .iter()
                .map(|value| value.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let reference = instrument.price;
            let structural_stop = recent_high + 0.10 * current_atr;
            let stop_pct = (structural_stop - reference) / reference.max(f64::EPSILON);
            if !(0.003..=0.03).contains(&stop_pct) {
                blockers.push(format!(
                    "structural stop {:.2}% / allowed 0.30%-3.00%",
                    stop_pct * 100.0
                ));
            }
            let ready = blockers.is_empty()
                && flow.is_some_and(|value| {
                    core_ready(
                        CoreInputs {
                            relative_1h,
                            relative_4h,
                            flow: value,
                            volume_ratio,
                            bearish_structure,
                            fresh_break,
                            btc_return_4h,
                        },
                        &self.config,
                    )
                });
            let progress = (10usize.saturating_sub(blockers.len()).min(10) as f64) / 10.0;
            let score = (-relative_1h).max(0.0)
                * (-relative_4h).max(0.0)
                * volume_ratio
                * (1.0 - flow.unwrap_or_default());
            let candidate = TradeCandidate {
                id: format!("relative_weakness_short:{symbol}:{}", bar.close_ms),
                recipe: "relative_weakness_short".into(),
                symbol: symbol.clone(),
                side: Side::Sell,
                signal_ms: bar.close_ms,
                expires_ms: bar.close_ms
                    + i64::from(self.config.weakness_max_signal_age_seconds) * 1_000,
                reference_price: reference,
                score,
                confidence: if ready { 0.80 } else { progress },
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_15m_relative_weakness"),
                    "BTCUSDT.binance_15m_anchor".into(),
                    format!("{symbol}.binance_15m_taker_flow"),
                    format!("{symbol}.book"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "relative_weakness_short".into()),
                    // Confirmed downside structure owns counter-trend shorts;
                    // ignition must never submit an opposing long first.
                    ("priority".into(), "3".into()),
                    ("relative_return_1h".into(), relative_1h.to_string()),
                    ("relative_return_4h".into(), relative_4h.to_string()),
                    ("volume_ratio".into(), volume_ratio.to_string()),
                    ("flow".into(), flow.unwrap_or_default().to_string()),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.weakness_risk_per_trade_pct.to_string(),
                    ),
                    ("stop_pct".into(), stop_pct.to_string()),
                    ("target_r".into(), self.config.weakness_target_r.to_string()),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "profit_shield_activation_r".into(),
                        self.config.weakness_profit_shield_activation_r.to_string(),
                    ),
                    (
                        "pre_tp_trailing_activation_r".into(),
                        self.config.weakness_profit_shield_activation_r.to_string(),
                    ),
                    (
                        "trailing_distance_pct".into(),
                        (stop_pct * self.config.weakness_trailing_distance_r).to_string(),
                    ),
                    (
                        "max_notional_multiple".into(),
                        self.config.weakness_max_notional_multiple.to_string(),
                    ),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.weakness_max_hold_minutes) * 60_000).to_string(),
                    ),
                    ("taker_fallback".into(), "false".into()),
                ]),
            };
            if ready {
                passed.push((score, candidate));
            } else {
                observations.push((progress, score, candidate));
            }
        }

        passed.sort_by(|left, right| right.0.total_cmp(&left.0));
        passed.truncate(self.config.max_candidates_per_lane);
        observations.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| right.1.total_cmp(&left.1))
        });
        observations.truncate(8);
        let actionable = !passed.is_empty();
        let reason = observations
            .first()
            .map(|value| format!("{}: {}", value.2.symbol, value.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for a fresh 15m relative-weakness breakdown".into());
        let score = passed
            .first()
            .map(|value| value.0)
            .or_else(|| observations.first().map(|value| value.1))
            .unwrap_or_default();
        let mut out = vec![status(
            &self.id,
            ctx.frame.as_of_ms,
            if actionable {
                "relative_weakness_short_ready"
            } else if structure_hits > 0 {
                "confirming_relative_weakness"
            } else {
                "scanning_relative_weakness"
            },
            &reason,
            score,
            inspected,
            passed.len() as u64,
        )];
        out.extend(passed.into_iter().map(|(_, candidate)| ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }));
        out.extend(
            observations
                .into_iter()
                .map(|(_, _, candidate)| ArtifactRecord {
                    key: format!("observation.{}", candidate.id),
                    producer: self.id.clone(),
                    artifact: Artifact::Candidate(candidate),
                }),
        );
        Ok(out)
    }
}

fn status(
    producer: &str,
    now_ms: i64,
    state: &str,
    reason: &str,
    score: f64,
    inspected: u64,
    passed: u64,
) -> ArtifactRecord {
    let actionable = passed > 0;
    ArtifactRecord {
        key: "lane.relative_weakness_short.status".into(),
        producer: producer.into(),
        artifact: Artifact::State(StateArtifact {
            state: state.into(),
            score,
            side: Some(Side::Sell),
            verdict: if actionable {
                Verdict::Pass
            } else {
                Verdict::Block
            },
            reasons: if actionable {
                vec![]
            } else {
                vec![reason.into()]
            },
            metrics: BTreeMap::from([
                ("inspected_symbols".into(), inspected as f64),
                ("pass_candidates".into(), passed as f64),
            ]),
            meta: meta(
                now_ms,
                30_000,
                DataQuality::Complete,
                1.0,
                vec![
                    "binance_15m".into(),
                    "BTCUSDT_15m_anchor".into(),
                    "binance_ws_depth".into(),
                ],
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_requires_relative_weakness_flow_volume_and_structure() {
        let config = LaneConfig::default();
        let valid = CoreInputs {
            relative_1h: -0.006,
            relative_4h: -0.012,
            flow: -0.06,
            volume_ratio: 0.90,
            bearish_structure: true,
            fresh_break: true,
            btc_return_4h: 0.01,
        };
        assert!(core_ready(valid, &config));
        assert!(!core_ready(
            CoreInputs {
                flow: 0.05,
                ..valid
            },
            &config
        ));
        assert!(!core_ready(
            CoreInputs {
                fresh_break: false,
                ..valid
            },
            &config
        ));
    }
}
