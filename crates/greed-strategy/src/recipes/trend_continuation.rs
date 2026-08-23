use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct TrendContinuationNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl TrendContinuationNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.trend_continuation".into(),
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

impl StrategyNode for TrendContinuationNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut ranked = Vec::new();
        let mut inspected = 0u64;
        let mut trend_hits = 0u64;
        let mut reclaim_hits = 0u64;
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let closed: Vec<_> = instrument
                .perpetual
                .values
                .iter()
                .filter(|value| value.closed)
                .collect();
            if closed.len() < 97 {
                continue;
            }
            inspected += 1;
            let i = closed.len() - 1;
            let bar = closed[i];
            let age = ctx.frame.as_of_ms - bar.close_ms;
            if age < 0 || age > i64::from(self.config.trend_max_signal_age_seconds) * 1_000 {
                continue;
            }
            let return_4h = bar.close / closed[i - 16].close - 1.0;
            let return_12h = bar.close / closed[i - 48].close - 1.0;
            let path: f64 = (i - 15..=i)
                .map(|j| (closed[j].close / closed[j - 1].close - 1.0).abs())
                .sum();
            let efficiency = return_4h.abs() / path.max(f64::EPSILON);
            let ema8 = ema(&closed, 8);
            let ema21 = ema(&closed, 21);
            let ema36 = ema(&closed, 36);
            let side = if return_4h >= self.config.trend_min_return_4h
                && return_12h > 0.0
                && ema21[i] > ema36[i]
            {
                Some(Side::Buy)
            } else if return_4h <= -self.config.trend_min_return_4h
                && return_12h < 0.0
                && ema21[i] < ema36[i]
            {
                Some(Side::Sell)
            } else {
                None
            };
            let Some(side) = side else { continue };
            if efficiency < self.config.trend_min_efficiency {
                continue;
            }
            trend_hits += 1;
            let sign = side.sign();
            let touched = if side == Side::Buy {
                closed[i - 3..i]
                    .iter()
                    .any(|value| value.low <= ema21[i - 1])
            } else {
                closed[i - 3..i]
                    .iter()
                    .any(|value| value.high >= ema21[i - 1])
            };
            let reclaimed = if side == Side::Buy {
                bar.close > ema8[i] && bar.close > closed[i - 1].close
            } else {
                bar.close < ema8[i] && bar.close < closed[i - 1].close
            };
            let Some(taker_buy) = bar.taker_buy_quote else {
                continue;
            };
            let imbalance = (2.0 * taker_buy / bar.quote_volume.max(1.0) - 1.0).clamp(-1.0, 1.0);
            let hour_volume: f64 = closed[i - 3..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum();
            let baseline: f64 = closed[i - 95..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum::<f64>()
                / 24.0;
            let volume_ratio = hour_volume / baseline.max(1.0);
            let book_ok = instrument.book.as_ref().is_some_and(|book| {
                let spread = (book.ask - book.bid)
                    / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                    * 10_000.0;
                spread <= self.config.max_spread_bps
                    && book.bid_depth_usd.min(book.ask_depth_usd) >= self.config.min_depth_usd
                    && book.meta.usable_at(ctx.frame.as_of_ms)
            });
            if !touched
                || !reclaimed
                || sign * imbalance < self.config.trend_min_flow_imbalance
                || volume_ratio < self.config.trend_min_hour_volume_ratio
                || !book_ok
            {
                continue;
            }
            reclaim_hits += 1;
            let score = return_4h.abs() * efficiency * volume_ratio;
            ranked.push((
                score,
                TradeCandidate {
                    id: format!("trend_continuation:{symbol}:{}", bar.close_ms),
                    recipe: "trend_continuation".into(),
                    symbol: symbol.clone(),
                    side,
                    signal_ms: bar.close_ms,
                    expires_ms: bar.close_ms
                        + i64::from(self.config.trend_max_signal_age_seconds) * 1_000,
                    reference_price: instrument.price,
                    score,
                    confidence: (0.65 + efficiency * 0.25 + (volume_ratio - 0.65).max(0.0) * 0.05)
                        .min(0.95),
                    verdict: Verdict::Pass,
                    blockers: vec![],
                    evidence: vec![
                        format!("{symbol}.binance_15m_trend"),
                        format!("{symbol}.book"),
                    ],
                    tags: BTreeMap::from([("lane".into(), "trend_continuation".into())]),
                },
            ));
        }
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        ranked.truncate(self.config.max_candidates_per_lane);
        let mut out = vec![ArtifactRecord {
            key: "lane.trend_continuation.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if ranked.is_empty() {
                    "scanning_for_strict_trend_reclaim"
                } else {
                    "actionable"
                }
                .into(),
                score: ranked.first().map_or(0.0, |value| value.0),
                side: ranked.first().map(|value| value.1.side),
                verdict: if ranked.is_empty() {
                    Verdict::Block
                } else {
                    Verdict::Pass
                },
                reasons: if ranked.is_empty() {
                    vec!["no 15m pullback reclaim currently satisfies the 4h trend, efficiency, flow, volume, and executable-book gates".into()]
                } else {
                    vec![]
                },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("trend_hits".into(), trend_hits as f64),
                    ("reclaim_hits".into(), reclaim_hits as f64),
                    ("pass_candidates".into(), ranked.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_ws_15m".into(), "binance_ws_depth".into()],
                ),
            }),
        }];
        out.extend(ranked.into_iter().map(|(_, candidate)| ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }));
        Ok(out)
    }
}
