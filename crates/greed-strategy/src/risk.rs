use crate::{primitives::meta, RiskConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, PositionPlan, StateArtifact, StrategyNode,
    Verdict,
};
use std::collections::BTreeMap;

fn tag_f64(candidate: &greed_kernel::TradeCandidate, key: &str) -> Option<f64> {
    candidate.tags.get(key)?.parse().ok()
}

fn tag_i64(candidate: &greed_kernel::TradeCandidate, key: &str) -> Option<i64> {
    candidate.tags.get(key)?.parse().ok()
}

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
        let a = &ctx.frame.account;
        let daily = ((a.risk_day_start_equity_usd - a.equity_usd)
            / a.risk_day_start_equity_usd.max(1.0))
        .max(0.0);
        let dd = ((a.peak_equity_usd - a.equity_usd) / a.peak_equity_usd.max(1.0)).max(0.0);
        let gross = a.gross_exposure_usd / a.equity_usd.max(1.0);
        let halted = daily >= self.config.daily_loss_limit_pct
            || dd >= self.config.peak_drawdown_halt_pct
            || a.open_positions >= self.config.max_positions
            || gross >= self.config.max_total_gross_multiple;
        let mut out = vec![ArtifactRecord {
            key: "portfolio.risk".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if halted { "halted" } else { "open" }.into(),
                score: daily.max(dd).max(gross),
                side: None,
                verdict: if halted {
                    Verdict::Block
                } else {
                    Verdict::Pass
                },
                reasons: if halted {
                    vec!["daily loss, peak drawdown, position count, or gross exposure limit is active".into()]
                } else {
                    vec![]
                },
                metrics: BTreeMap::from([
                    ("daily_loss_pct".into(), daily),
                    ("peak_drawdown_pct".into(), dd),
                    ("gross_exposure_multiple".into(), gross),
                    ("open_positions".into(), a.open_positions as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_demo_account".into()],
                ),
            }),
        }];
        if halted {
            return Ok(out);
        }
        let mut candidates: Vec<_> = ctx
            .artifacts
            .values()
            .filter_map(|r| r.artifact.candidate())
            .filter(|c| c.verdict == Verdict::Pass && c.expires_ms >= ctx.frame.as_of_ms)
            .collect();
        candidates.sort_by(|a, b| {
            tag_f64(b, "priority")
                .unwrap_or(1.0)
                .total_cmp(&tag_f64(a, "priority").unwrap_or(1.0))
                .then_with(|| b.score.total_cmp(&a.score))
        });
        let mut planned_gross = gross;
        let mut slots = self.config.max_positions.saturating_sub(a.open_positions);
        let mut planned_symbols = std::collections::BTreeSet::new();
        for c in candidates {
            if slots == 0 {
                break;
            }
            // A failed breakout and a continuation signal can coexist briefly
            // in the same frame.  Fund only the higher-priority interpretation
            // instead of submitting opposing plans for one contract.
            if !planned_symbols.insert(c.symbol.clone()) {
                continue;
            }
            let Some(i) = ctx.frame.instrument(&c.symbol) else {
                planned_symbols.remove(&c.symbol);
                continue;
            };
            let market_ok = i
                .book
                .as_ref()
                .is_some_and(|b| b.meta.usable_at(ctx.frame.as_of_ms))
                && i.perpetual.meta.usable_at(ctx.frame.as_of_ms);
            if !market_ok {
                planned_symbols.remove(&c.symbol);
                continue;
            }
            let default_risk_pct = if c.confidence >= self.config.high_confidence_threshold {
                self.config.high_confidence_risk_per_trade_pct
            } else {
                self.config.risk_per_trade_pct
            };
            let risk_pct = tag_f64(c, "risk_per_trade_pct").unwrap_or(default_risk_pct);
            let stop_pct = tag_f64(c, "stop_pct")
                .unwrap_or(self.config.initial_stop_pct)
                .clamp(0.003, 0.03);
            let target_r = tag_f64(c, "target_r").unwrap_or(self.config.first_take_profit_r);
            let take_fraction = tag_f64(c, "take_profit_fraction")
                .unwrap_or(self.config.first_take_profit_fraction)
                .clamp(0.1, 1.0);
            let notional = (a.equity_usd * risk_pct / stop_pct).min(
                a.equity_usd
                    * tag_f64(c, "max_notional_multiple")
                        .unwrap_or(self.config.max_notional_per_trade_multiple)
                        .clamp(0.20, self.config.max_notional_per_trade_multiple),
            );
            let multiple = notional / a.equity_usd.max(1.0);
            if planned_gross + multiple > self.config.max_total_gross_multiple + f64::EPSILON {
                planned_symbols.remove(&c.symbol);
                continue;
            }
            planned_gross += multiple;
            slots -= 1;
            let sign = c.side.sign();
            let stop = c.reference_price * (1.0 - sign * stop_pct);
            let tp1 = c.reference_price * (1.0 + sign * stop_pct * target_r);
            let mut take_profit_prices = vec![(tp1, take_fraction)];
            if take_fraction < 1.0 && self.config.runner_take_profit_r > 0.0 {
                let runner = c.reference_price
                    * (1.0
                        + sign * self.config.initial_stop_pct * self.config.runner_take_profit_r);
                take_profit_prices.push((runner, 1.0 - self.config.first_take_profit_fraction));
            }
            let plan = PositionPlan {
                candidate_id: c.id.clone(),
                symbol: c.symbol.clone(),
                side: c.side,
                reference_price: c.reference_price,
                notional_usd: notional,
                entry_limit: tag_f64(c, "entry_limit"),
                entry_timeout_ms: tag_i64(c, "entry_timeout_ms").unwrap_or_default(),
                taker_fallback: c
                    .tags
                    .get("taker_fallback")
                    .is_some_and(|value| value == "true"),
                max_entry_adverse_bps: tag_f64(c, "max_entry_adverse_bps").unwrap_or_default(),
                taker_fallback_max_adverse_bps: tag_f64(c, "taker_fallback_max_adverse_bps")
                    .unwrap_or_default(),
                taker_fallback_size_multiplier: tag_f64(c, "taker_fallback_size_multiplier")
                    .unwrap_or(1.0),
                stop_price: stop,
                take_profit_prices,
                break_even_after_fraction: (take_fraction < 1.0).then_some(take_fraction),
                break_even_buffer_pct: self.config.break_even_buffer_pct,
                profit_shield_activation_pct: (take_fraction < 1.0
                    && self.config.profit_shield_activation_r > 0.0)
                    .then_some(stop_pct * self.config.profit_shield_activation_r),
                // Start locking profit before TP1. Waiting until TP1 meant a
                // position could reach roughly +1R, miss the 2R partial, and
                // surrender almost all open profit back to the cost shield.
                trailing_activation_pct: (take_fraction < 1.0)
                    .then_some(stop_pct * self.config.pre_tp_trailing_activation_r),
                trailing_distance_pct: (take_fraction < 1.0)
                    .then_some(self.config.trailing_distance_pct),
                max_hold_ms: tag_i64(c, "max_hold_ms")
                    .unwrap_or_else(|| i64::from(self.config.max_hold_minutes) * 60_000),
            };
            out.push(ArtifactRecord {
                key: format!("plan.{}", c.id),
                producer: self.id.clone(),
                artifact: Artifact::PositionPlan(plan),
            });
        }
        Ok(out)
    }
}
