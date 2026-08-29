use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const MINUTE_MS: i64 = 60_000;

pub struct BurstExhaustionNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

#[derive(Clone, Copy)]
struct Setup {
    side: Side,
    signal_ms: i64,
    impulse_return: f64,
    volume_ratio: f64,
    reversal_return: f64,
    reversal_flow: f64,
    stop: f64,
}

impl BurstExhaustionNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.burst_exhaustion".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn flow(bar: &Candle) -> Option<f64> {
    bar.taker_buy_quote
        .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0)
}

fn setup(values: &[&Candle], reversal_index: usize, config: &LaneConfig) -> Option<Setup> {
    if reversal_index < 24 {
        return None;
    }
    let reversal = values[reversal_index];
    let reversal_flow = flow(reversal)?;
    // An impulse may complete one to six 5m bars before the reversal. Select
    // the strongest qualifying event; all inputs are closed before signaling.
    (reversal_index.saturating_sub(6)..reversal_index)
        .filter(|&impulse_index| impulse_index >= 18 && impulse_index >= 5)
        .filter_map(|impulse_index| {
            let impulse = values[impulse_index];
            let impulse_return = impulse.close / values[impulse_index - 5].open - 1.0;
            let direction = impulse_return.signum();
            if direction == 0.0 || impulse_return.abs() < config.burst_min_return_30m {
                return None;
            }
            let recent_volume: f64 = values[impulse_index - 2..=impulse_index]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum();
            let baseline = median(
                (impulse_index - 18..impulse_index)
                    .map(|index| {
                        values[index - 2..=index]
                            .iter()
                            .map(|bar| bar.quote_volume)
                            .sum()
                    })
                    .collect(),
            )
            .max(1.0);
            let volume_ratio = recent_volume / baseline;
            if volume_ratio < config.burst_min_volume_ratio {
                return None;
            }
            let reversal_return = reversal.close / reversal.open - 1.0;
            if direction * reversal_return > -config.burst_min_reversal_return
                || direction * reversal_flow > -config.burst_min_opposing_flow
            {
                return None;
            }
            let body_efficiency = (reversal.close - reversal.open).abs()
                / (reversal.high - reversal.low).max(f64::EPSILON);
            if body_efficiency >= 0.45 && reversal.quote_volume < 0.65 * impulse.quote_volume {
                return None;
            }
            let origin = values[impulse_index - 5].open;
            let event = &values[impulse_index..=reversal_index];
            let extreme = if direction > 0.0 {
                event
                    .iter()
                    .map(|bar| bar.high)
                    .fold(f64::NEG_INFINITY, f64::max)
            } else {
                event
                    .iter()
                    .map(|bar| bar.low)
                    .fold(f64::INFINITY, f64::min)
            };
            let buffer = 0.10 * (extreme - origin).abs() / event.len() as f64;
            Some(Setup {
                side: if direction > 0.0 {
                    Side::Sell
                } else {
                    Side::Buy
                },
                signal_ms: reversal.close_ms,
                impulse_return,
                volume_ratio,
                reversal_return,
                reversal_flow,
                stop: extreme + direction * buffer,
            })
        })
        .max_by(|left, right| {
            (left.impulse_return.abs() * left.volume_ratio)
                .total_cmp(&(right.impulse_return.abs() * right.volume_ratio))
        })
}

impl StrategyNode for BurstExhaustionNode {
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
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(series) = instrument.fast_perpetual.as_ref() else {
                continue;
            };
            let closed: Vec<_> = series.values.iter().filter(|bar| bar.closed).collect();
            if closed.len() < 25 {
                continue;
            }
            inspected += 1;
            let Some(value) = setup(&closed, closed.len() - 1, &self.config) else {
                continue;
            };
            setup_hits += 1;
            let age_ms = ctx.frame.as_of_ms - value.signal_ms;
            let stop_pct =
                (instrument.price - value.stop).abs() / instrument.price.max(f64::EPSILON);
            let mut blockers = Vec::new();
            if age_ms < 0 || age_ms > i64::from(self.config.burst_max_signal_age_seconds) * 1_000 {
                blockers.push("completed 5m reversal is outside the execution window".into());
            }
            if !(0.0025..=0.03).contains(&stop_pct) {
                blockers.push(format!(
                    "structural stop {:.2}% / allowed 0.25%-3.00%",
                    stop_pct * 100.0
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
            let score = value.impulse_return.abs() * value.volume_ratio * value.reversal_flow.abs();
            let progress = (6usize.saturating_sub(blockers.len()).min(6) as f64) / 6.0;
            let candidate = TradeCandidate {
                id: format!("burst_exhaustion:{symbol}:{}", value.signal_ms),
                recipe: "burst_exhaustion".into(),
                symbol: symbol.clone(),
                side: value.side,
                signal_ms: value.signal_ms,
                expires_ms: value.signal_ms
                    + i64::from(self.config.burst_max_signal_age_seconds) * 1_000,
                reference_price: instrument.price,
                score,
                confidence: if ready { 0.86 } else { progress },
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_5m_burst"),
                    format!("{symbol}.binance_5m_aggressor_reversal"),
                    format!("{symbol}.book"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "burst_exhaustion".into()),
                    ("priority".into(), "5".into()),
                    (
                        "impulse_return_30m".into(),
                        value.impulse_return.to_string(),
                    ),
                    ("volume_ratio".into(), value.volume_ratio.to_string()),
                    ("reversal_return".into(), value.reversal_return.to_string()),
                    ("reversal_flow".into(), value.reversal_flow.to_string()),
                    ("stop_pct".into(), stop_pct.to_string()),
                    ("target_r".into(), self.config.burst_target_r.to_string()),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.burst_risk_per_trade_pct.to_string(),
                    ),
                    ("max_notional_multiple".into(), "1.0".into()),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.burst_max_hold_minutes) * MINUTE_MS).to_string(),
                    ),
                ]),
            };
            if ready {
                passed.push((score, candidate));
            } else {
                observations.push((progress, score, candidate));
            }
        }
        passed.sort_by(|a, b| b.0.total_cmp(&a.0));
        passed.truncate(self.config.max_candidates_per_lane);
        observations.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| b.1.total_cmp(&a.1)));
        observations.truncate(8);
        let actionable = !passed.is_empty();
        let reason = observations
            .first()
            .map(|v| format!("{}: {}", v.2.symbol, v.2.blockers.join(" · ")))
            .unwrap_or_else(|| {
                "waiting for a 30m burst, volume climax and opposing 5m aggressor flow".into()
            });
        let mut out = vec![ArtifactRecord {
            key: "lane.burst_exhaustion.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "ready_to_trade_burst_exhaustion"
                } else if setup_hits > 0 {
                    "checking_burst_execution"
                } else {
                    "scanning_burst_exhaustion"
                }
                .into(),
                score: passed
                    .first()
                    .map(|v| v.0)
                    .or_else(|| observations.first().map(|v| v.1))
                    .unwrap_or_default(),
                side: passed.first().map(|v| v.1.side),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("setup_hits".into(), setup_hits as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_5m_klines".into(), "binance_ws_depth".into()],
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

    fn candle(index: i64, open: f64, close: f64, volume: f64, flow: f64) -> Candle {
        Candle {
            open_ms: index * 300_000,
            close_ms: (index + 1) * 300_000 - 1,
            open,
            high: open.max(close) * 1.001,
            low: open.min(close) * 0.999,
            close,
            quote_volume: volume,
            taker_buy_quote: Some(volume * (1.0 + flow) * 0.5),
            closed: true,
        }
    }

    fn exhaustion(flow: f64) -> Vec<Candle> {
        let mut out = (0..19)
            .map(|index| candle(index, 100.0, 100.0, 100.0, 0.0))
            .collect::<Vec<_>>();
        let closes = [100.4, 100.9, 101.4, 102.0, 102.7];
        for (offset, close) in closes.into_iter().enumerate() {
            let open = out.last().expect("seed candle").close;
            out.push(candle(19 + offset as i64, open, close, 300.0, 0.20));
        }
        out.push(candle(24, 102.7, 101.8, 250.0, flow));
        out
    }

    #[test]
    fn detects_volume_climax_with_opposing_aggressor_flow() {
        let values = exhaustion(-0.20);
        let refs = values.iter().collect::<Vec<_>>();
        let found = setup(&refs, refs.len() - 1, &LaneConfig::default()).expect("setup");
        assert_eq!(found.side, Side::Sell);
        assert!(found.volume_ratio >= 2.0);
    }

    #[test]
    fn rejects_price_reversal_without_opposing_aggressor_flow() {
        let values = exhaustion(-0.02);
        let refs = values.iter().collect::<Vec<_>>();
        assert!(setup(&refs, refs.len() - 1, &LaneConfig::default()).is_none());
    }
}
