use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const HOUR_MS: i64 = 3_600_000;
const FIFTEEN_MINUTES_MS: i64 = 900_000;

pub struct SfpReversalNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl SfpReversalNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.sfp_reversal".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn hourly_bars(values: &[&Candle]) -> Vec<Candle> {
    let mut output = Vec::new();
    let mut index = 0;
    while index + 3 < values.len() {
        let first = values[index];
        if first.open_ms.rem_euclid(HOUR_MS) != 0 {
            index += 1;
            continue;
        }
        let group = &values[index..index + 4];
        if group.iter().enumerate().any(|(offset, value)| {
            value.open_ms != first.open_ms + offset as i64 * FIFTEEN_MINUTES_MS
        }) {
            index += 1;
            continue;
        }
        output.push(Candle {
            open_ms: first.open_ms,
            close_ms: group[3].close_ms,
            open: first.open,
            high: group
                .iter()
                .map(|value| value.high)
                .fold(f64::NEG_INFINITY, f64::max),
            low: group
                .iter()
                .map(|value| value.low)
                .fold(f64::INFINITY, f64::min),
            close: group[3].close,
            quote_volume: group.iter().map(|value| value.quote_volume).sum(),
            taker_buy_quote: group
                .iter()
                .map(|value| value.taker_buy_quote)
                .collect::<Option<Vec<_>>>()
                .map(|values| values.into_iter().sum()),
            closed: true,
        });
        index += 4;
    }
    output
}

fn atr(values: &[Candle], index: usize, window: usize) -> f64 {
    let start = index.saturating_sub(window.saturating_sub(1));
    let rows: Vec<_> = (start..=index)
        .map(|cursor| {
            let prior = if cursor == 0 {
                values[cursor].close
            } else {
                values[cursor - 1].close
            };
            (values[cursor].high - values[cursor].low)
                .max((values[cursor].high - prior).abs())
                .max((values[cursor].low - prior).abs())
        })
        .collect();
    rows.iter().sum::<f64>() / rows.len().max(1) as f64
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

#[derive(Debug, Clone, Copy)]
struct SfpSetup {
    level: f64,
    trigger: f64,
    stop: f64,
    sweep_atr: f64,
    volume_ratio: f64,
}

fn short_sfp(values: &[Candle], index: usize, config: &LaneConfig) -> Option<SfpSetup> {
    if index < config.sfp_lookback_hours || index < 36 {
        return None;
    }
    let signal = &values[index];
    let history = &values[index - config.sfp_lookback_hours..index];
    let (level_offset, level) = history
        .iter()
        .enumerate()
        .max_by(|left, right| left.1.high.total_cmp(&right.1.high))
        .map(|(offset, value)| (offset, value.high))?;
    if history.len() - level_offset < 6 {
        return None;
    }
    let current_atr = atr(values, index, 14);
    let sweep_atr = (signal.high - level) / current_atr.max(f64::EPSILON);
    let baseline = median(
        values[index - 36..index]
            .iter()
            .map(|value| value.quote_volume)
            .collect(),
    );
    let volume_ratio = signal.quote_volume / baseline.max(1.0);
    let close_location = (signal.close - signal.low) / (signal.high - signal.low).max(f64::EPSILON);
    (sweep_atr >= config.sfp_min_sweep_atr
        && sweep_atr <= config.sfp_max_sweep_atr
        && signal.close < level
        && signal.close < signal.open
        && close_location <= 0.40
        && volume_ratio >= config.sfp_min_volume_ratio)
        .then_some(SfpSetup {
            level,
            trigger: signal.low,
            stop: signal.high + 0.10 * current_atr,
            sweep_atr,
            volume_ratio,
        })
}

fn short_confirmation(values: &[&Candle], trigger: f64, stop: f64) -> (Option<i64>, bool) {
    let mut confirmation_ms = None;
    for value in values {
        // A bar that touches both is intentionally adverse: one-minute replay
        // cannot prove the trigger preceded invalidation.
        if value.high >= stop {
            return (confirmation_ms, true);
        }
        if confirmation_ms.is_none() && value.low <= trigger {
            confirmation_ms = Some(value.close_ms);
        }
    }
    (confirmation_ms, false)
}

impl StrategyNode for SfpReversalNode {
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
        let mut sweep_hits = 0u64;
        let mut confirmation_hits = 0u64;
        let mut history_ready = 0u64;
        let mut near_sweep_hits = 0u64;
        let mut nearest_sweep: Option<(f64, String, bool)> = None;

        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            inspected += 1;
            let closed: Vec<_> = instrument
                .perpetual
                .values
                .iter()
                .filter(|value| value.closed)
                .collect();
            let hourly = hourly_bars(&closed);
            if hourly.len() <= self.config.sfp_lookback_hours {
                continue;
            }
            history_ready += 1;
            let latest_index = hourly.len() - 1;
            let latest = &hourly[latest_index];
            let prior = &hourly[latest_index - self.config.sfp_lookback_hours..latest_index];
            let level = prior
                .iter()
                .map(|value| value.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let distance_atr =
                (level - latest.high) / atr(&hourly, latest_index, 14).max(f64::EPSILON);
            let swept = distance_atr <= 0.0;
            let proximity = distance_atr.max(0.0);
            near_sweep_hits += u64::from(proximity <= 0.25);
            if nearest_sweep
                .as_ref()
                .is_none_or(|current| proximity < current.0)
            {
                nearest_sweep = Some((proximity, symbol.clone(), swept));
            }
            let start = hourly
                .len()
                .saturating_sub(self.config.sfp_confirmation_hours as usize + 1)
                .max(self.config.sfp_lookback_hours);
            let setup = (start..hourly.len()).rev().find_map(|index| {
                short_sfp(&hourly, index, &self.config).map(|value| (index, value))
            });
            let Some((signal_index, setup)) = setup else {
                continue;
            };
            sweep_hits += 1;
            let signal = &hourly[signal_index];
            let expires_ms =
                signal.close_ms + i64::from(self.config.sfp_confirmation_hours) * HOUR_MS;
            let mut blockers = Vec::new();
            let fast: Vec<_> = instrument
                .fast_perpetual
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .filter(|value| {
                            value.closed
                                && value.open_ms >= signal.close_ms
                                && value.open_ms < expires_ms
                        })
                        .collect()
                })
                .unwrap_or_default();
            let (confirmation_ms, invalid) = short_confirmation(&fast, setup.trigger, setup.stop);
            if invalid {
                blockers.push("SFP was invalidated above the protected sweep extreme".into());
            } else if confirmation_ms.is_none() {
                blockers.push("waiting for price to break the SFP rejection-bar low".into());
            }
            let confirmation_ms = confirmation_ms.unwrap_or(signal.close_ms);
            let age_ms = ctx.frame.as_of_ms - confirmation_ms;
            if age_ms < 0 || age_ms > i64::from(self.config.sfp_max_signal_age_seconds) * 1_000 {
                blockers.push("confirmed SFP entry window expired".into());
            }
            if ctx.frame.as_of_ms > expires_ms {
                blockers.push("SFP did not confirm inside the three-hour window".into());
            }
            let stop_pct = (setup.stop - instrument.price) / instrument.price.max(f64::EPSILON);
            if !(0.003..=0.03).contains(&stop_pct) {
                blockers.push(format!(
                    "structural stop {:.2}% / allowed 0.30%-3.00%",
                    stop_pct * 100.0
                ));
            }
            let initial_risk = setup.stop - setup.trigger;
            let progress_r = (setup.trigger - instrument.price) / initial_risk.max(f64::EPSILON);
            if progress_r > 0.50 {
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
            let progress = (7usize.saturating_sub(blockers.len()).min(7) as f64) / 7.0;
            let score = setup.sweep_atr * setup.volume_ratio;
            let passive_entry = instrument.book.as_ref().map(|book| book.ask);
            let candidate = TradeCandidate {
                id: format!("sfp_reversal:{symbol}:{}", signal.close_ms),
                recipe: "sfp_reversal".into(),
                symbol: symbol.clone(),
                side: Side::Sell,
                signal_ms: confirmation_ms,
                expires_ms: confirmation_ms
                    + i64::from(self.config.sfp_max_signal_age_seconds) * 1_000,
                reference_price: instrument.price,
                score,
                confidence: if ready { 0.82 } else { progress },
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_1h_swing_failure"),
                    format!("{symbol}.binance_5m_confirmation"),
                    format!("{symbol}.book"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "sfp_reversal".into()),
                    ("priority".into(), "3".into()),
                    ("sfp_level".into(), setup.level.to_string()),
                    ("sfp_trigger".into(), setup.trigger.to_string()),
                    ("sfp_sweep_atr".into(), setup.sweep_atr.to_string()),
                    ("sfp_volume_ratio".into(), setup.volume_ratio.to_string()),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.sfp_risk_per_trade_pct.to_string(),
                    ),
                    ("stop_pct".into(), stop_pct.to_string()),
                    ("target_r".into(), self.config.sfp_target_r.to_string()),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "entry_limit".into(),
                        passive_entry.unwrap_or(instrument.price).to_string(),
                    ),
                    ("entry_timeout_ms".into(), "60000".into()),
                    ("taker_fallback".into(), "false".into()),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.sfp_max_hold_minutes) * 60_000).to_string(),
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
            .unwrap_or_else(|| {
                if history_ready < inspected {
                    format!(
                        "loading 12-day 1h structure history ({history_ready}/{inspected} symbols ready)"
                    )
                } else if let Some((distance, symbol, swept)) = nearest_sweep.as_ref() {
                    if *swept {
                        format!(
                            "{symbol} swept the 12-day high, but the closed 1h rejection shape or volume is not valid"
                        )
                    } else {
                        format!(
                            "{symbol} is closest: the 12-day high is {distance:.2} ATR above the latest closed 1h high"
                        )
                    }
                } else {
                    "waiting for a 1h prior-high sweep and close back below the level".into()
                }
            });
        let score = passed
            .first()
            .map(|value| value.0)
            .or_else(|| observations.first().map(|value| value.1))
            .unwrap_or_default();
        let mut out = vec![ArtifactRecord {
            key: "lane.sfp_reversal.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "ready_to_fade_failed_breakout"
                } else if sweep_hits > 0 {
                    "waiting_for_sfp_confirmation"
                } else if near_sweep_hits > 0 {
                    "approaching_hourly_liquidity_sweep"
                } else {
                    "scanning_hourly_liquidity_sweeps"
                }
                .into(),
                score,
                side: Some(Side::Sell),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("history_ready".into(), history_ready as f64),
                    ("near_sweep_hits".into(), near_sweep_hits as f64),
                    (
                        "nearest_sweep_distance_atr".into(),
                        nearest_sweep.as_ref().map_or(0.0, |value| value.0),
                    ),
                    ("sweep_hits".into(), sweep_hits as f64),
                    ("confirmation_hits".into(), confirmation_hits as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_15m_to_1h".into(),
                        "binance_ws_5m".into(),
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

    fn candle(open_ms: i64, open: f64, high: f64, low: f64, close: f64, volume: f64) -> Candle {
        Candle {
            open_ms,
            close_ms: open_ms + HOUR_MS - 1,
            open,
            high,
            low,
            close,
            quote_volume: volume,
            taker_buy_quote: Some(volume * 0.5),
            closed: true,
        }
    }

    #[test]
    fn detects_confirmable_short_sfp() {
        let config = LaneConfig {
            sfp_lookback_hours: 48,
            ..LaneConfig::default()
        };
        let mut values: Vec<_> = (0..48)
            .map(|index| candle(index * HOUR_MS, 99.0, 100.0, 98.0, 99.0, 1_000.0))
            .collect();
        values[0].high = 101.0;
        values.push(candle(48 * HOUR_MS, 100.5, 101.5, 98.5, 99.0, 1_500.0));
        let setup = short_sfp(&values, 48, &config).expect("short SFP");
        assert_eq!(setup.level, 101.0);
        assert!(setup.stop > 101.5);
    }

    #[test]
    fn rejects_breakout_that_closes_above_the_level() {
        let config = LaneConfig {
            sfp_lookback_hours: 48,
            ..LaneConfig::default()
        };
        let mut values: Vec<_> = (0..48)
            .map(|index| candle(index * HOUR_MS, 99.0, 100.0, 98.0, 99.0, 1_000.0))
            .collect();
        values[0].high = 101.0;
        values.push(candle(48 * HOUR_MS, 100.5, 102.0, 100.0, 101.5, 1_500.0));
        assert!(short_sfp(&values, 48, &config).is_none());
    }

    #[test]
    fn confirmation_is_revoked_by_a_later_stop_touch() {
        let first = candle(0, 100.0, 100.5, 98.0, 99.0, 1_000.0);
        let second = candle(HOUR_MS, 99.0, 102.0, 98.5, 101.0, 1_000.0);
        let (confirmation, invalid) = short_confirmation(&[&first, &second], 98.5, 101.5);
        assert_eq!(confirmation, Some(first.close_ms));
        assert!(invalid);
    }
}
