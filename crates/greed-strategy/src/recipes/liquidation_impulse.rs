use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct LiquidationImpulseNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}
impl LiquidationImpulseNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.liquidation_impulse".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

impl StrategyNode for LiquidationImpulseNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &[]
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut ranked = Vec::new();
        let mut observed = 0u64;
        let mut bursts = 0u64;
        let mut confirmed = 0u64;
        for symbol in &self.symbols {
            let Some(i) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(micro) = i.microstructure.as_ref() else {
                continue;
            };
            if !micro.meta.usable_at(ctx.frame.as_of_ms) {
                continue;
            }
            observed += 1;
            let long = micro.long_liquidations_60s;
            let short = micro.short_liquidations_60s;
            let total = long + short;
            let relative_threshold = i
                .derivatives
                .as_ref()
                .and_then(|value| value.open_interest_usd)
                .unwrap_or_default()
                * self.config.liquidation_min_oi_fraction;
            let threshold = self
                .config
                .liquidation_min_notional_usd
                .max(relative_threshold);
            if total < threshold {
                continue;
            }
            bursts += 1;
            let oi_flush = i
                .derivatives
                .as_ref()
                .filter(|value| value.meta.usable_at(ctx.frame.as_of_ms))
                .and_then(|d| d.open_interest_change_pct)
                .unwrap_or(0.0);
            if oi_flush > -self.config.liquidation_min_oi_flush_pct {
                continue;
            }
            let Some(series) = i.micro_perpetual.as_ref() else {
                continue;
            };
            let Some(bar) = series.values.iter().rev().find(|b| b.closed) else {
                continue;
            };
            let return_1m = bar.close / bar.open.max(f64::EPSILON) - 1.0;
            let side = if long > short { Side::Buy } else { Side::Sell };
            if side.sign() * return_1m < self.config.liquidation_min_reclaim_pct {
                continue;
            }
            confirmed += 1;
            let score = total / threshold.max(1.0) * (-oi_flush) * 100.0;
            ranked.push((
                score,
                TradeCandidate {
                    id: format!("liquidation_impulse:{symbol}:{}", bar.close_ms),
                    recipe: "liquidation_impulse".into(),
                    symbol: symbol.clone(),
                    side,
                    signal_ms: bar.close_ms,
                    expires_ms: bar.close_ms + 120_000,
                    reference_price: i.price,
                    score,
                    confidence: (0.65 + (-oi_flush).min(0.02) * 10.0).min(0.92),
                    verdict: Verdict::Pass,
                    blockers: vec![],
                    evidence: vec![
                        format!("{symbol}.force_orders"),
                        format!("{symbol}.open_interest"),
                        format!("{symbol}.reclaim"),
                    ],
                    tags: BTreeMap::from([("lane".into(), "liquidation_impulse".into())]),
                },
            ));
        }
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        ranked.truncate(self.config.max_candidates_per_lane);
        let mut out = vec![ArtifactRecord {
            key: "lane.liquidation_impulse.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if !ranked.is_empty() {
                    "actionable"
                } else if bursts > 0 {
                    "waiting_for_oi_flush_and_reclaim"
                } else {
                    "scanning_liquidations"
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
                        "no liquidation burst has both an OI reset and a one-minute reclaim".into(),
                    ]
                } else {
                    vec![]
                },
                metrics: BTreeMap::from([
                    ("observed_symbols".into(), observed as f64),
                    ("liquidation_bursts".into(), bursts as f64),
                    ("confirmed_reclaims".into(), confirmed as f64),
                    ("pass_candidates".into(), ranked.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    30_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_ws_force_order".into(),
                        "binance_open_interest".into(),
                    ],
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
