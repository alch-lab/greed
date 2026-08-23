use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct CrossVenueCrowdingNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}
impl CrossVenueCrowdingNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.cross_venue_crowding".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}
impl StrategyNode for CrossVenueCrowdingNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &[]
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut ranked = Vec::new();
        let mut paired = 0u64;
        let mut extremes = 0u64;
        let mut reversals = 0u64;
        for symbol in &self.symbols {
            let Some(i) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(cross) = i.cross_venue.as_ref() else {
                continue;
            };
            if !cross.meta.usable_at(ctx.frame.as_of_ms) {
                continue;
            }
            paired += 1;
            let gap = cross.funding_gap_per_hour.unwrap_or(0.0);
            let premium = cross
                .mark_premium_pct
                .or(cross.hyper_premium_pct)
                .unwrap_or(0.0);
            let oi = i
                .derivatives
                .as_ref()
                .and_then(|d| d.open_interest_change_pct)
                .unwrap_or(0.0);
            if gap.abs() < self.config.crowding_min_funding_gap_per_hour
                || premium.abs() < self.config.crowding_min_premium_abs_pct
                || oi < self.config.crowding_min_oi_change_pct
            {
                continue;
            }
            extremes += 1;
            let crowded = if gap > 0.0 && premium > 0.0 {
                Side::Buy
            } else if gap < 0.0 && premium < 0.0 {
                Side::Sell
            } else {
                continue;
            };
            let side = crowded.opposite();
            let imbalance = i
                .microstructure
                .as_ref()
                .filter(|value| value.meta.usable_at(ctx.frame.as_of_ms))
                .and_then(|m| m.trade_imbalance())
                .unwrap_or(0.0);
            if side.sign() * imbalance < 0.15 {
                continue;
            }
            reversals += 1;
            let score = gap.abs() / self.config.crowding_min_funding_gap_per_hour * premium.abs();
            ranked.push((
                score,
                TradeCandidate {
                    id: format!(
                        "cross_venue_crowding:{symbol}:{}",
                        ctx.frame.as_of_ms / 60_000
                    ),
                    recipe: "cross_venue_crowding".into(),
                    symbol: symbol.clone(),
                    side,
                    signal_ms: ctx.frame.as_of_ms,
                    expires_ms: ctx.frame.as_of_ms + 90_000,
                    reference_price: i.price,
                    score,
                    confidence: 0.75,
                    verdict: Verdict::Pass,
                    blockers: vec![],
                    evidence: vec![
                        format!("{symbol}.hyperliquid_context"),
                        format!("{symbol}.binance_funding"),
                        format!("{symbol}.agg_trade_reversal"),
                    ],
                    tags: BTreeMap::from([("lane".into(), "cross_venue_crowding".into())]),
                },
            ));
        }
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        ranked.truncate(self.config.max_candidates_per_lane);
        let mut out = vec![ArtifactRecord {
            key: "lane.cross_venue_crowding.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if !ranked.is_empty() {
                    "actionable"
                } else if extremes > 0 {
                    "waiting_for_flow_reversal"
                } else if paired > 0 {
                    "scanning_crowding"
                } else {
                    "waiting_for_hyperliquid"
                }
                .into(),
                score: ranked.first().map(|x| x.0).unwrap_or(0.0),
                side: ranked.first().map(|x| x.1.side),
                verdict: if ranked.is_empty() {
                    Verdict::Block
                } else {
                    Verdict::Pass
                },
                reasons: if ranked.is_empty() {
                    vec![
                        "no cross-venue funding and premium extreme has reversed its taker flow"
                            .into(),
                    ]
                } else {
                    vec![]
                },
                metrics: BTreeMap::from([
                    ("paired_symbols".into(), paired as f64),
                    ("crowding_extremes".into(), extremes as f64),
                    ("flow_reversals".into(), reversals as f64),
                    ("pass_candidates".into(), ranked.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    if paired > 0 { 1.0 } else { 0.0 },
                    vec!["hyperliquid_public_context".into(), "binance_ws".into()],
                ),
            }),
        }];
        out.extend(ranked.into_iter().map(|(_, c)| ArtifactRecord {
            key: format!("candidate.{}", c.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(c),
        }));
        Ok(out)
    }
}
