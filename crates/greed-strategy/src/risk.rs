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
        let mut suppressed_symbol_conflicts = 0u64;
        for c in candidates {
            if slots == 0 {
                break;
            }
            // A failed breakout and a continuation signal can coexist briefly
            // in the same frame.  Fund only the higher-priority interpretation
            // instead of submitting opposing plans for one contract.
            if !planned_symbols.insert(c.symbol.clone()) {
                suppressed_symbol_conflicts += 1;
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
            let risk_pct = tag_f64(c, "risk_per_trade_pct")
                .unwrap_or(default_risk_pct)
                .clamp(0.001, 0.02);
            let stop_pct = tag_f64(c, "stop_pct")
                .unwrap_or(self.config.initial_stop_pct)
                .clamp(0.003, 0.10);
            let target_r = tag_f64(c, "target_r").unwrap_or(self.config.first_take_profit_r);
            let take_fraction = tag_f64(c, "take_profit_fraction")
                .unwrap_or(self.config.first_take_profit_fraction)
                .clamp(0.1, 1.0);
            let profit_shield_activation_r = tag_f64(c, "profit_shield_activation_r")
                .unwrap_or(self.config.profit_shield_activation_r);
            let trailing_activation_r = tag_f64(c, "pre_tp_trailing_activation_r")
                .unwrap_or(self.config.pre_tp_trailing_activation_r);
            let trailing_distance_pct =
                tag_f64(c, "trailing_distance_pct").unwrap_or(self.config.trailing_distance_pct);
            let explicit_profit_protection = c.tags.contains_key("profit_shield_activation_r")
                || c.tags.contains_key("pre_tp_trailing_activation_r")
                || c.tags.contains_key("trailing_distance_pct");
            let early_failure_after_ms = tag_i64(c, "early_failure_after_ms").unwrap_or_default();
            let early_failure_adverse_r = tag_f64(c, "early_failure_adverse_r").unwrap_or_default();
            let early_failure_max_mfe_r = tag_f64(c, "early_failure_max_mfe_r").unwrap_or_default();
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
                entry_invalidation_bps: tag_f64(c, "entry_invalidation_bps").unwrap_or_default(),
                entry_guard_max_opposing_flow: tag_f64(c, "entry_guard_max_opposing_flow")
                    .unwrap_or_default(),
                entry_guard_max_opposing_return_bps: tag_f64(
                    c,
                    "entry_guard_max_opposing_return_bps",
                )
                .unwrap_or_default(),
                stop_price: stop,
                take_profit_prices,
                break_even_after_fraction: (take_fraction < 1.0).then_some(take_fraction),
                break_even_buffer_pct: self.config.break_even_buffer_pct,
                profit_shield_activation_pct: ((take_fraction < 1.0 || explicit_profit_protection)
                    && profit_shield_activation_r > 0.0)
                    .then_some(stop_pct * profit_shield_activation_r),
                // Start locking profit before TP1. Waiting until TP1 meant a
                // position could reach roughly +1R, miss the 2R partial, and
                // surrender almost all open profit back to the cost shield.
                trailing_activation_pct: (take_fraction < 1.0 || explicit_profit_protection)
                    .then_some(stop_pct * trailing_activation_r),
                trailing_distance_pct: (take_fraction < 1.0 || explicit_profit_protection)
                    .then_some(trailing_distance_pct),
                early_failure_after_ms,
                early_failure_adverse_pct: stop_pct * early_failure_adverse_r,
                early_failure_max_favorable_pct: stop_pct * early_failure_max_mfe_r,
                max_hold_ms: tag_i64(c, "max_hold_ms")
                    .unwrap_or_else(|| i64::from(self.config.max_hold_minutes) * 60_000),
            };
            out.push(ArtifactRecord {
                key: format!("plan.{}", c.id),
                producer: self.id.clone(),
                artifact: Artifact::PositionPlan(plan),
            });
        }
        if let Some(ArtifactRecord {
            artifact: Artifact::State(state),
            ..
        }) = out.first_mut()
        {
            state.metrics.insert(
                "suppressed_symbol_conflicts".into(),
                suppressed_symbol_conflicts as f64,
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greed_kernel::{
        AccountFrame, BookState, CandleSeries, InstrumentFrame, MarketFrame, MarketKind,
        ObservationMeta, Side, TradeCandidate,
    };

    fn meta() -> ObservationMeta {
        ObservationMeta {
            event_ms: 1_000,
            received_ms: 1_000,
            expires_ms: 10_000,
            source: "test".into(),
            quality: DataQuality::Complete,
        }
    }

    fn instrument(symbol: &str) -> InstrumentFrame {
        InstrumentFrame {
            symbol: symbol.into(),
            price: 100.0,
            perpetual: CandleSeries {
                venue: "test".into(),
                market: MarketKind::Perpetual,
                interval_ms: 900_000,
                meta: meta(),
                values: vec![],
            },
            hourly_perpetual: None,
            fast_perpetual: None,
            micro_perpetual: None,
            open_interest: None,
            book: Some(BookState {
                meta: meta(),
                bid: 99.99,
                ask: 100.01,
                bid_depth_usd: 100_000.0,
                ask_depth_usd: 100_000.0,
                expected_buy_slippage_bps: Some(1.0),
                expected_sell_slippage_bps: Some(1.0),
                bids: vec![],
                asks: vec![],
            }),
            microstructure: None,
        }
    }

    fn candidate(id: &str, recipe: &str, symbol: &str, priority: u8) -> ArtifactRecord {
        ArtifactRecord {
            key: format!("candidate.{id}"),
            producer: format!("lane.{recipe}"),
            artifact: Artifact::Candidate(TradeCandidate {
                id: id.into(),
                recipe: recipe.into(),
                symbol: symbol.into(),
                side: Side::Buy,
                signal_ms: 1_000,
                expires_ms: 10_000,
                reference_price: 100.0,
                score: 1.0,
                confidence: 0.8,
                verdict: Verdict::Pass,
                blockers: vec![],
                evidence: vec![],
                tags: BTreeMap::from([("priority".into(), priority.to_string())]),
            }),
        }
    }

    #[test]
    fn one_symbol_gets_only_the_highest_priority_strategy_plan() {
        let records = [
            candidate("trend:BTC", "trend_continuation", "BTCUSDT", 1),
            candidate("sfp:BTC", "sfp_reversal", "BTCUSDT", 4),
            candidate("intraday:ETH", "intraday_sweep_reversal", "ETHUSDT", 2),
        ];
        let artifacts = records
            .into_iter()
            .map(|record| (record.key.clone(), record))
            .collect();
        let frame = MarketFrame {
            as_of_ms: 2_000,
            instruments: BTreeMap::from([
                ("BTCUSDT".into(), instrument("BTCUSDT")),
                ("ETHUSDT".into(), instrument("ETHUSDT")),
            ]),
            account: AccountFrame {
                equity_usd: 2_000.0,
                cash_usd: 2_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 2_000.0,
                risk_day_start_equity_usd: 2_000.0,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        };
        let mut planner = PositionPlannerNode::new(vec![], RiskConfig::default());
        let output = planner
            .evaluate(&NodeContext {
                frame: &frame,
                artifacts: &artifacts,
            })
            .unwrap();
        let plans: Vec<_> = output
            .iter()
            .filter_map(|record| match &record.artifact {
                Artifact::PositionPlan(plan) => Some(plan),
                _ => None,
            })
            .collect();
        assert_eq!(plans.len(), 2);
        assert!(plans.iter().any(|plan| plan.candidate_id == "sfp:BTC"));
        assert!(!plans.iter().any(|plan| plan.candidate_id == "trend:BTC"));
        assert!(plans.iter().any(|plan| plan.candidate_id == "intraday:ETH"));
    }
}
