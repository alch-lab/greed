use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, PositionExitIntent, Side, StateArtifact,
    StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

/// Cross-sectional continuation that only trades when the market itself is
/// moving together.  The breadth gate avoids mistaking an isolated pump for a
/// durable regime; ranking then concentrates the small portfolio in the single
/// strongest leader (or laggard) instead of chasing every mover.
pub struct BreadthMomentumNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

#[derive(Clone)]
struct Observation {
    symbol: String,
    signal_ms: i64,
    price: f64,
    return_window: f64,
    hour_volume_usd: f64,
    volume_ratio: f64,
    spread_bps: Option<f64>,
    depth_usd: Option<f64>,
}

impl BreadthMomentumNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.breadth_momentum".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn breadth_direction(positive_breadth: f64, threshold: f64) -> Option<Side> {
    if positive_breadth >= threshold {
        Some(Side::Buy)
    } else if positive_breadth <= 1.0 - threshold {
        Some(Side::Sell)
    } else {
        None
    }
}

fn is_hourly_close(close_ms: i64) -> bool {
    (close_ms + 1).rem_euclid(3_600_000) == 0
}

impl StrategyNode for BreadthMomentumNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut observations = Vec::new();
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
            let lookback = self.config.breadth_lookback_bars;
            if closed.len() < 97 || closed.len() <= lookback {
                continue;
            }
            let i = closed.len() - 1;
            let bar = closed[i];
            if ctx.frame.as_of_ms - bar.close_ms > instrument.perpetual.interval_ms {
                continue;
            }
            let hour_volume_usd = closed[i - 3..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum::<f64>();
            let baseline_hour_volume = closed[i - 95..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum::<f64>()
                / 24.0;
            let (spread_bps, depth_usd) = instrument
                .book
                .as_ref()
                .filter(|book| book.meta.usable_at(ctx.frame.as_of_ms))
                .map_or((None, None), |book| {
                    (
                        Some(
                            (book.ask - book.bid) / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                                * 10_000.0,
                        ),
                        Some(book.bid_depth_usd.min(book.ask_depth_usd)),
                    )
                });
            observations.push(Observation {
                symbol: symbol.clone(),
                signal_ms: bar.close_ms,
                price: instrument.price,
                return_window: bar.close / closed[i - lookback].close - 1.0,
                hour_volume_usd,
                volume_ratio: hour_volume_usd / baseline_hour_volume.max(1.0),
                spread_bps,
                depth_usd,
            });
        }

        let positive_breadth = if observations.is_empty() {
            0.5
        } else {
            observations
                .iter()
                .filter(|value| value.return_window > 0.0)
                .count() as f64
                / observations.len() as f64
        };
        let entry_side = breadth_direction(positive_breadth, self.config.breadth_entry_threshold);
        let hourly_signal = observations
            .first()
            .is_some_and(|value| is_hourly_close(value.signal_ms));

        let mut out = Vec::new();
        // Existing positions leave when cross-sectional participation decays,
        // even if their own candle has not hit the disaster stop or 3h cap.
        if !observations.is_empty() && positive_breadth < self.config.breadth_exit_threshold {
            out.push(ArtifactRecord {
                key: "exit.breadth_momentum.buy".into(),
                producer: self.id.clone(),
                artifact: Artifact::PositionExitIntent(PositionExitIntent {
                    recipe: "breadth_momentum".into(),
                    side: Side::Buy,
                    reason: "market_breadth_decay".into(),
                }),
            });
        }
        if !observations.is_empty() && positive_breadth > 1.0 - self.config.breadth_exit_threshold {
            out.push(ArtifactRecord {
                key: "exit.breadth_momentum.sell".into(),
                producer: self.id.clone(),
                artifact: Artifact::PositionExitIntent(PositionExitIntent {
                    recipe: "breadth_momentum".into(),
                    side: Side::Sell,
                    reason: "market_breadth_decay".into(),
                }),
            });
        }

        if let Some(side) = entry_side.filter(|_| hourly_signal) {
            observations.sort_by(|a, b| {
                (side.sign() * b.return_window).total_cmp(&(side.sign() * a.return_window))
            });
            for (rank, observation) in observations
                .iter()
                .take(self.config.breadth_max_candidates)
                .enumerate()
            {
                let mut blockers = Vec::new();
                if side.sign() * observation.return_window < self.config.breadth_min_abs_return {
                    blockers.push(format!(
                        "4h move {:.2}% / need {:.2}%",
                        side.sign() * observation.return_window * 100.0,
                        self.config.breadth_min_abs_return * 100.0
                    ));
                }
                if observation.hour_volume_usd < self.config.breadth_min_hour_volume_usd {
                    blockers.push(format!(
                        "1h volume ${:.0} / need ${:.0}",
                        observation.hour_volume_usd, self.config.breadth_min_hour_volume_usd
                    ));
                }
                if observation.volume_ratio < self.config.breadth_min_hour_volume_ratio {
                    blockers.push(format!(
                        "1h volume {:.2}x baseline / need {:.2}x",
                        observation.volume_ratio, self.config.breadth_min_hour_volume_ratio
                    ));
                }
                match observation.spread_bps {
                    Some(value) if value > self.config.max_spread_bps => blockers.push(format!(
                        "spread {value:.1} bps / max {:.1} bps",
                        self.config.max_spread_bps
                    )),
                    None => blockers.push("order book is not ready".into()),
                    _ => {}
                }
                match observation.depth_usd {
                    Some(value) if value < self.config.min_depth_usd => blockers.push(format!(
                        "book depth ${value:.0} / need ${:.0}",
                        self.config.min_depth_usd
                    )),
                    None => {}
                    _ => {}
                }
                let verdict = if blockers.is_empty() {
                    Verdict::Pass
                } else {
                    Verdict::Block
                };
                let progress = (5usize.saturating_sub(blockers.len()).min(5) as f64) / 5.0;
                let tags = BTreeMap::from([
                    ("lane".into(), "breadth_momentum".into()),
                    ("priority".into(), "2".into()),
                    ("breadth".into(), positive_breadth.to_string()),
                    ("cross_section_rank".into(), (rank + 1).to_string()),
                    ("return_4h".into(), observation.return_window.to_string()),
                    (
                        "hour_volume_usd".into(),
                        observation.hour_volume_usd.to_string(),
                    ),
                    (
                        "hour_volume_ratio".into(),
                        observation.volume_ratio.to_string(),
                    ),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.breadth_risk_per_trade_pct.to_string(),
                    ),
                    ("stop_pct".into(), self.config.breadth_stop_pct.to_string()),
                    (
                        "target_r".into(),
                        (self.config.breadth_target_pct / self.config.breadth_stop_pct).to_string(),
                    ),
                    ("take_profit_fraction".into(), "1".into()),
                    ("max_notional_multiple".into(), "0.25".into()),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.breadth_max_hold_minutes) * 60_000).to_string(),
                    ),
                ]);
                let candidate = TradeCandidate {
                    id: format!(
                        "breadth_momentum:{}:{}:{}",
                        observation.symbol,
                        observation.signal_ms,
                        if side == Side::Buy { "long" } else { "short" }
                    ),
                    recipe: "breadth_momentum".into(),
                    symbol: observation.symbol.clone(),
                    side,
                    signal_ms: observation.signal_ms,
                    expires_ms: observation.signal_ms + 5 * 60_000,
                    reference_price: observation.price,
                    score: side.sign() * observation.return_window * observation.volume_ratio,
                    confidence: if verdict == Verdict::Pass {
                        0.75
                    } else {
                        progress
                    },
                    verdict,
                    blockers,
                    evidence: vec![
                        "binance_cross_section_15m".into(),
                        format!("{}.binance_15m", observation.symbol),
                        format!("{}.book", observation.symbol),
                    ],
                    tags,
                };
                out.push(ArtifactRecord {
                    key: format!("candidate.{}", candidate.id),
                    producer: self.id.clone(),
                    artifact: Artifact::Candidate(candidate),
                });
            }
        }

        let ready = out.iter().any(|record| {
            record
                .artifact
                .candidate()
                .is_some_and(|candidate| candidate.verdict == Verdict::Pass)
        });
        out.push(ArtifactRecord {
            key: "lane.breadth_momentum.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if ready {
                    "breadth_regime_ready"
                } else if entry_side.is_some() && hourly_signal {
                    "ranking_leaders_and_liquidity"
                } else if entry_side.is_some() {
                    "waiting_for_hourly_ranking"
                } else {
                    "waiting_for_market_breadth"
                }
                .into(),
                score: (positive_breadth - 0.5).abs() * 2.0,
                side: entry_side,
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                reasons: if entry_side.is_none() {
                    vec![format!(
                        "positive breadth {:.0}% / need at least {:.0}% or at most {:.0}%",
                        positive_breadth * 100.0,
                        self.config.breadth_entry_threshold * 100.0,
                        (1.0 - self.config.breadth_entry_threshold) * 100.0
                    )]
                } else if !hourly_signal {
                    vec!["breadth regime is active; next ranking runs on the hourly close".into()]
                } else {
                    Vec::new()
                },
                metrics: BTreeMap::from([
                    ("eligible_symbols".into(), observations.len() as f64),
                    ("positive_breadth".into(), positive_breadth),
                    ("pass_candidates".into(), usize::from(ready) as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_cross_section_15m".into()],
                ),
            }),
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breadth_direction_requires_a_real_cross_sectional_extreme() {
        assert_eq!(breadth_direction(0.70, 0.70), Some(Side::Buy));
        assert_eq!(breadth_direction(0.30, 0.70), Some(Side::Sell));
        assert_eq!(breadth_direction(0.50, 0.70), None);
    }

    #[test]
    fn entries_only_refresh_on_completed_hour_boundaries() {
        assert!(is_hourly_close(3_599_999));
        assert!(!is_hourly_close(899_999));
        assert!(is_hourly_close(7_199_999));
    }
}
