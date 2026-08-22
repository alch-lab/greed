use crate::{primitives::meta, RiskConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, PositionPlan, StateArtifact,
    StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct PositionPlannerNode {
    id: String,
    dependencies: Vec<String>,
    config: RiskConfig,
}
impl PositionPlannerNode {
    pub fn new(dependencies: Vec<String>, config: RiskConfig) -> Self {
        Self {
            id: "portfolio.position_planner".into(),
            dependencies,
            config,
        }
    }
}
impl StrategyNode for PositionPlannerNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let daily_loss = ((ctx.frame.account.risk_day_start_equity_usd
            - ctx.frame.account.equity_usd)
            / ctx.frame.account.risk_day_start_equity_usd.max(1.0))
        .max(0.0);
        let drawdown = (ctx.frame.account.peak_equity_usd - ctx.frame.account.equity_usd)
            / ctx.frame.account.peak_equity_usd.max(1.0);
        let risk_halted = daily_loss >= self.config.daily_loss_limit_pct
            || drawdown >= self.config.peak_drawdown_halt_pct;
        let existing_gross =
            ctx.frame.account.gross_exposure_usd / ctx.frame.account.equity_usd.max(1.0);
        let mut major_gross =
            ctx.frame.account.major_gross_exposure_usd / ctx.frame.account.equity_usd.max(1.0);
        let mut alt_gross =
            ctx.frame.account.alt_gross_exposure_usd / ctx.frame.account.equity_usd.max(1.0);
        let mut metrics = BTreeMap::new();
        metrics.insert("daily_loss_pct".into(), daily_loss);
        metrics.insert("peak_drawdown_pct".into(), drawdown);
        metrics.insert("gross_exposure_multiple".into(), existing_gross);
        metrics.insert("major_gross_multiple".into(), major_gross);
        metrics.insert("alt_gross_multiple".into(), alt_gross);
        metrics.insert(
            "open_positions".into(),
            ctx.frame.account.open_positions as f64,
        );
        let mut out = vec![ArtifactRecord {
            key: "portfolio.risk".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if risk_halted { "halted" } else { "open" }.into(),
                score: daily_loss.max(drawdown).max(existing_gross),
                side: None,
                verdict: if risk_halted {
                    Verdict::Block
                } else {
                    Verdict::Pass
                },
                reasons: if risk_halted {
                    vec!["daily loss or peak drawdown circuit breaker is active".into()]
                } else {
                    vec![]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["paper_account".into()],
                ),
            }),
        }];
        if risk_halted || existing_gross >= self.config.max_total_gross {
            return Ok(out);
        }
        let mut candidates: Vec<_> = ctx
            .artifacts
            .values()
            .filter_map(|record| record.artifact.candidate())
            .filter(|candidate| {
                candidate.verdict == Verdict::Pass && candidate.expires_ms >= ctx.frame.as_of_ms
            })
            .collect();
        candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
        let mut gross = existing_gross;
        for candidate in candidates {
            let Some(instrument) = ctx.frame.instrument(&candidate.symbol) else {
                continue;
            };
            let per_trade = if instrument.asset_class == AssetClass::Major {
                self.config.major_gross_per_trade
            } else {
                self.config.alt_gross_per_trade
            };
            let bucket_has_room = match instrument.asset_class {
                AssetClass::Major => major_gross + per_trade <= self.config.major_max_gross,
                AssetClass::Altcoin => alt_gross + per_trade <= self.config.alt_max_gross,
            };
            if gross + per_trade > self.config.max_total_gross + f64::EPSILON {
                continue;
            }
            if !bucket_has_room {
                continue;
            }
            gross += per_trade;
            match instrument.asset_class {
                AssetClass::Major => major_gross += per_trade,
                AssetClass::Altcoin => alt_gross += per_trade,
            }
            let stop_pct = match candidate.tags.get("stop_profile").map(String::as_str) {
                Some("exhaustion") => self.config.initial_stop_pct * 0.75,
                Some("alt_shock") => self.config.initial_stop_pct * 1.5,
                Some("alt_cross") => self.config.alt_cross_stop_pct,
                _ => self.config.initial_stop_pct,
            };
            let take_profit_pct = match candidate.tags.get("stop_profile").map(String::as_str) {
                Some("alt_cross") => self.config.alt_cross_take_profit_pct,
                _ => self.config.first_take_profit_pct,
            };
            let stop_price = candidate.reference_price * (1.0 - candidate.side.sign() * stop_pct);
            let tp = candidate.reference_price * (1.0 + candidate.side.sign() * take_profit_pct);
            let max_hold_minutes = match candidate.tags.get("hold_profile").map(String::as_str) {
                Some("alt_cross") => self.config.alt_cross_max_hold_minutes,
                _ => self.config.max_hold_minutes,
            };
            let plan = PositionPlan {
                candidate_id: candidate.id.clone(),
                symbol: candidate.symbol.clone(),
                side: candidate.side,
                reference_price: candidate.reference_price,
                notional_usd: ctx.frame.account.equity_usd * per_trade,
                entry_limit: None,
                stop_price,
                take_profit_prices: vec![(tp, self.config.first_take_profit_fraction)],
                trailing_activation_pct: Some(self.config.trailing_activation_pct),
                trailing_distance_pct: Some(self.config.trailing_distance_pct),
                max_hold_ms: max_hold_minutes as i64 * 60_000,
            };
            out.push(ArtifactRecord {
                key: format!("plan.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::PositionPlan(plan),
            });
        }
        Ok(out)
    }
}
