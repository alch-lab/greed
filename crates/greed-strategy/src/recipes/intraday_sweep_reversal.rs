use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const MINUTE_MS: i64 = 60_000;

pub struct IntradaySweepReversalNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

#[derive(Clone, Copy)]
struct Setup {
    side: Side,
    trigger: f64,
    stop: f64,
    sweep_atr: f64,
    wick_body: f64,
    volume_ratio: f64,
    flow: f64,
}

impl IntradaySweepReversalNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.intraday_sweep_reversal".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn atr(values: &[&Candle], end: usize) -> f64 {
    let start = end.saturating_sub(19).max(1);
    let rows = end - start + 1;
    (start..=end)
        .map(|index| {
            let bar = values[index];
            let prior = values[index - 1].close;
            (bar.high - bar.low)
                .max((bar.high - prior).abs())
                .max((bar.low - prior).abs())
        })
        .sum::<f64>()
        / rows as f64
}

fn setup(values: &[&Candle], index: usize, config: &LaneConfig) -> Option<Setup> {
    if index < config.intraday_lookback_bars.max(96) {
        return None;
    }
    let bar = values[index];
    let prior = &values[index - config.intraday_lookback_bars..index];
    let high = prior
        .iter()
        .map(|value| value.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let low = prior
        .iter()
        .map(|value| value.low)
        .fold(f64::INFINITY, f64::min);
    let current_atr = atr(values, index);
    let body = (bar.close - bar.open).abs();
    let denominator = body.max(0.10 * current_atr).max(f64::EPSILON);
    let upper = bar.high - bar.open.max(bar.close);
    let lower = bar.open.min(bar.close) - bar.low;
    let volume_ratio = bar.quote_volume
        / median(
            values[index - 32..index]
                .iter()
                .map(|value| value.quote_volume)
                .collect(),
        )
        .max(1.0);
    let flow = bar
        .taker_buy_quote
        .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0)?;
    let return_4h = bar.close / values[index - 16].close - 1.0;
    if return_4h.abs() > config.intraday_max_abs_return_4h
        || volume_ratio < config.intraday_min_volume_ratio
    {
        return None;
    }
    let location = (bar.close - bar.low) / (bar.high - bar.low).max(f64::EPSILON);
    let short_sweep = (bar.high - high) / current_atr.max(f64::EPSILON);
    let long_sweep = (low - bar.low) / current_atr.max(f64::EPSILON);
    let short = short_sweep >= config.intraday_min_sweep_atr
        && bar.close < high
        && upper / denominator >= config.intraday_min_wick_body
        && location <= 0.55
        && flow <= -config.intraday_min_directional_flow;
    let long = long_sweep >= config.intraday_min_sweep_atr
        && bar.close > low
        && lower / denominator >= config.intraday_min_wick_body
        && location >= 0.45
        && flow >= config.intraday_min_directional_flow;
    if short == long {
        return None;
    }
    if short {
        Some(Setup {
            side: Side::Sell,
            trigger: bar.low,
            stop: bar.high + 0.05 * current_atr,
            sweep_atr: short_sweep,
            wick_body: upper / denominator,
            volume_ratio,
            flow,
        })
    } else {
        Some(Setup {
            side: Side::Buy,
            trigger: bar.high,
            stop: bar.low - 0.05 * current_atr,
            sweep_atr: long_sweep,
            wick_body: lower / denominator,
            volume_ratio,
            flow,
        })
    }
}

fn confirmation(values: &[&Candle], setup: Setup) -> (Option<i64>, bool) {
    let mut confirmed = None;
    for bar in values {
        let stop_hit = if setup.side == Side::Buy {
            bar.low <= setup.stop
        } else {
            bar.high >= setup.stop
        };
        // A one-minute candle touching both has unknowable ordering.  Treat it
        // as invalid, matching the conservative research replay.
        if stop_hit {
            return (confirmed, true);
        }
        let trigger_hit = if setup.side == Side::Buy {
            bar.high >= setup.trigger
        } else {
            bar.low <= setup.trigger
        };
        if confirmed.is_none() && trigger_hit {
            confirmed = Some(bar.close_ms);
        }
    }
    (confirmed, false)
}

impl StrategyNode for IntradaySweepReversalNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0u64;
        let mut setup_hits = 0u64;
        let mut confirmation_hits = 0u64;
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let closed: Vec<_> = instrument
                .perpetual
                .values
                .iter()
                .filter(|bar| bar.closed)
                .collect();
            if closed.len() < 97 {
                continue;
            }
            inspected += 1;
            let search_start = closed.len().saturating_sub(3).max(96);
            let found = (search_start..closed.len())
                .rev()
                .find_map(|index| setup(&closed, index, &self.config).map(|value| (index, value)));
            let Some((index, setup)) = found else {
                continue;
            };
            setup_hits += 1;
            let signal = closed[index];
            let confirmation_deadline =
                signal.close_ms + i64::from(self.config.intraday_confirmation_minutes) * MINUTE_MS;
            let micro: Vec<_> = instrument
                .micro_perpetual
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .filter(|bar| {
                            bar.closed
                                && bar.open_ms > signal.close_ms
                                && bar.open_ms < confirmation_deadline
                        })
                        .collect()
                })
                .unwrap_or_default();
            let (confirmed_ms, invalid) = confirmation(&micro, setup);
            let mut blockers = Vec::new();
            if invalid {
                blockers.push("rejection extreme was invalidated before entry".into());
            } else if confirmed_ms.is_none() {
                blockers.push("waiting for the next 1m break of the rejection trigger".into());
            }
            let confirmed_ms = confirmed_ms.unwrap_or(signal.close_ms);
            let age_ms = ctx.frame.as_of_ms - confirmed_ms;
            if age_ms < 0 || age_ms > i64::from(self.config.intraday_max_signal_age_seconds) * 1_000
            {
                blockers.push("confirmed intraday entry window expired".into());
            }
            if ctx.frame.as_of_ms > confirmation_deadline {
                blockers.push("no confirmation inside the 30-minute window".into());
            }
            let stop_pct =
                (instrument.price - setup.stop).abs() / instrument.price.max(f64::EPSILON);
            if !(0.0025..=0.03).contains(&stop_pct) {
                blockers.push(format!(
                    "structural stop {:.2}% / allowed 0.25%-3.00%",
                    stop_pct * 100.0
                ));
            }
            let initial_risk = (setup.trigger - setup.stop).abs();
            let progress_r = setup.side.sign() * (instrument.price - setup.trigger)
                / initial_risk.max(f64::EPSILON);
            if progress_r > 0.25 {
                blockers.push(format!(
                    "entry already moved {progress_r:.2}R beyond the trigger"
                ));
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
            let ready = blockers.is_empty();
            confirmation_hits += u64::from(ready);
            let score = setup.sweep_atr * setup.wick_body * setup.volume_ratio * setup.flow.abs();
            let progress = (8usize.saturating_sub(blockers.len()).min(8) as f64) / 8.0;
            let passive_entry = instrument.book.as_ref().map(|book| {
                if setup.side == Side::Buy {
                    book.bid
                } else {
                    book.ask
                }
            });
            let candidate = TradeCandidate {
                id: format!("intraday_sweep_reversal:{symbol}:{}", signal.close_ms),
                recipe: "intraday_sweep_reversal".into(),
                symbol: symbol.clone(),
                side: setup.side,
                signal_ms: confirmed_ms,
                expires_ms: confirmed_ms
                    + i64::from(self.config.intraday_max_signal_age_seconds) * 1_000,
                reference_price: instrument.price,
                score,
                confidence: if ready { 0.84 } else { progress },
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_15m_local_sweep"),
                    format!("{symbol}.binance_1m_confirmation"),
                    format!("{symbol}.book"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "intraday_sweep_reversal".into()),
                    ("priority".into(), "3".into()),
                    ("sweep_atr".into(), setup.sweep_atr.to_string()),
                    ("wick_body".into(), setup.wick_body.to_string()),
                    ("volume_ratio".into(), setup.volume_ratio.to_string()),
                    ("directional_flow".into(), setup.flow.to_string()),
                    ("stop_pct".into(), stop_pct.to_string()),
                    ("target_r".into(), self.config.intraday_target_r.to_string()),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.intraday_risk_per_trade_pct.to_string(),
                    ),
                    ("max_notional_multiple".into(), "1.0".into()),
                    (
                        "entry_limit".into(),
                        passive_entry.unwrap_or(instrument.price).to_string(),
                    ),
                    ("entry_timeout_ms".into(), "60000".into()),
                    ("taker_fallback".into(), "false".into()),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.intraday_max_hold_minutes) * MINUTE_MS).to_string(),
                    ),
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
            .unwrap_or_else(|| "waiting for a closed 15m local sweep with directional flow".into());
        let score = passed
            .first()
            .map(|value| value.0)
            .or_else(|| observations.first().map(|value| value.1))
            .unwrap_or_default();
        let mut out = vec![ArtifactRecord {
            key: "lane.intraday_sweep_reversal.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "ready_to_trade_intraday_sweep"
                } else if setup_hits > 0 {
                    "confirming_intraday_sweep"
                } else {
                    "scanning_intraday_sweeps"
                }
                .into(),
                score,
                side: passed.first().map(|value| value.1.side),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("setup_hits".into(), setup_hits as f64),
                    ("confirmation_hits".into(), confirmation_hits as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_15m_local_sweep".into(),
                        "binance_1m_confirmation".into(),
                        "binance_ws_depth".into(),
                    ],
                ),
            }),
        }];
        out.extend(passed.into_iter().map(|(_, candidate)| ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }));
        out.extend(
            observations
                .into_iter()
                .map(|(_, _, candidate)| ArtifactRecord {
                    key: format!("candidate.{}", candidate.id),
                    producer: self.id.clone(),
                    artifact: Artifact::Candidate(candidate),
                }),
        );
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(
        index: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        flow: f64,
    ) -> Candle {
        Candle {
            open_ms: index * 900_000,
            close_ms: (index + 1) * 900_000 - 1,
            open,
            high,
            low,
            close,
            quote_volume: volume,
            taker_buy_quote: Some(volume * (flow + 1.0) * 0.5),
            closed: true,
        }
    }

    #[test]
    fn detects_directionally_confirmed_short_sweep() {
        let mut values: Vec<_> = (0..96)
            .map(|index| candle(index, 100.0, 101.0, 99.0, 100.0, 1_000.0, 0.0))
            .collect();
        for value in values.iter_mut().skip(88) {
            value.high = 100.5;
        }
        values.push(candle(96, 100.2, 102.0, 99.4, 99.8, 2_000.0, -0.40));
        let found = setup(
            &values.iter().collect::<Vec<_>>(),
            96,
            &LaneConfig::default(),
        )
        .expect("short sweep");
        assert_eq!(found.side, Side::Sell);
    }

    #[test]
    fn rejects_wick_without_directional_flow() {
        let mut values: Vec<_> = (0..96)
            .map(|index| candle(index, 100.0, 100.5, 99.0, 100.0, 1_000.0, 0.0))
            .collect();
        values.push(candle(96, 100.2, 102.0, 99.4, 99.8, 2_000.0, 0.40));
        assert!(setup(
            &values.iter().collect::<Vec<_>>(),
            96,
            &LaneConfig::default()
        )
        .is_none());
    }
}
