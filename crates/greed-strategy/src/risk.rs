use crate::{primitives::meta, RiskConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, BookState, DataQuality, NodeContext, PositionPlan, PriceLevel, Side,
    StateArtifact, StrategyNode, Verdict,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
struct LiquiditySizing {
    notional_cap_usd: f64,
    visible_exit_depth_usd: f64,
    impact_cap_usd: f64,
    expected_exit_slippage_bps: Option<f64>,
}

const PROFIT_SHIELD_ROUND_TRIP_FEE_BPS: f64 = 7.0;
const PROFIT_SHIELD_SLIPPAGE_STRESS_MULTIPLIER: f64 = 1.5;
const PROFIT_SHIELD_MIN_NET_BPS: f64 = 2.0;

fn cost_aware_profit_shield_buffer(
    configured_buffer_pct: f64,
    expected_exit_slippage_bps: Option<f64>,
) -> f64 {
    let Some(exit_slippage_bps) = expected_exit_slippage_bps else {
        return configured_buffer_pct;
    };
    let cost_floor_pct = (PROFIT_SHIELD_ROUND_TRIP_FEE_BPS
        + exit_slippage_bps.max(0.0) * PROFIT_SHIELD_SLIPPAGE_STRESS_MULTIPLIER
        + PROFIT_SHIELD_MIN_NET_BPS)
        / 10_000.0;
    configured_buffer_pct.max(cost_floor_pct).min(0.003)
}

fn sweep_slippage_bps(levels: &[PriceLevel], notional_usd: f64, reference: f64) -> Option<f64> {
    if levels.is_empty() || notional_usd <= 0.0 || reference <= 0.0 {
        return None;
    }
    let mut remaining = notional_usd;
    let mut quantity = 0.0;
    let mut cost = 0.0;
    for level in levels {
        if level.price <= 0.0 || level.quantity <= 0.0 {
            continue;
        }
        let quote = level.price * level.quantity;
        let take = remaining.min(quote);
        quantity += take / level.price;
        cost += take;
        remaining -= take;
        if remaining <= f64::EPSILON {
            break;
        }
    }
    if remaining > 1e-6 || quantity <= f64::EPSILON {
        return None;
    }
    Some((cost / quantity / reference - 1.0).abs() * 10_000.0)
}

fn sweep_capacity_usd(levels: &[PriceLevel], reference: f64, max_slippage_bps: f64) -> f64 {
    if levels.is_empty() || reference <= 0.0 {
        return 0.0;
    }
    let total: f64 = levels
        .iter()
        .map(|level| level.price * level.quantity)
        .filter(|value| value.is_finite() && *value > 0.0)
        .sum();
    if total <= f64::EPSILON {
        return 0.0;
    }
    if sweep_slippage_bps(levels, total, reference)
        .is_some_and(|slippage| slippage <= max_slippage_bps)
    {
        return total;
    }
    let mut low = 0.0;
    let mut high = total;
    for _ in 0..40 {
        let middle = (low + high) * 0.5;
        if sweep_slippage_bps(levels, middle, reference)
            .is_some_and(|slippage| slippage <= max_slippage_bps)
        {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

fn liquidity_sizing(
    book: &BookState,
    side: Side,
    desired_notional_usd: f64,
    config: &RiskConfig,
) -> LiquiditySizing {
    // Entries are passive, so the immediately executable side that matters
    // most is the protective exit: bids for a long, asks for a short.
    let (levels, reference, visible_depth, fallback_slippage) = match side {
        Side::Buy => (
            book.bids.as_slice(),
            book.bid,
            book.bid_depth_usd,
            book.expected_sell_slippage_bps,
        ),
        Side::Sell => (
            book.asks.as_slice(),
            book.ask,
            book.ask_depth_usd,
            book.expected_buy_slippage_bps,
        ),
    };
    let impact_cap = if levels.is_empty() {
        fallback_slippage
            .filter(|slippage| *slippage <= config.max_book_slippage_bps)
            .map_or(0.0, |_| visible_depth)
    } else {
        sweep_capacity_usd(levels, reference, config.max_book_slippage_bps)
    };
    let participation_cap = visible_depth.max(0.0) * config.max_book_participation_pct;
    let notional_cap = impact_cap.min(participation_cap).max(0.0);
    let sized_notional = desired_notional_usd.min(notional_cap);
    let expected_exit_slippage_bps = if levels.is_empty() {
        fallback_slippage
    } else {
        sweep_slippage_bps(levels, sized_notional, reference)
    };
    LiquiditySizing {
        notional_cap_usd: notional_cap,
        visible_exit_depth_usd: visible_depth,
        impact_cap_usd: impact_cap,
        expected_exit_slippage_bps,
    }
}

fn tag_f64(candidate: &greed_kernel::TradeCandidate, key: &str) -> Option<f64> {
    candidate.tags.get(key)?.parse().ok()
}

fn tag_i64(candidate: &greed_kernel::TradeCandidate, key: &str) -> Option<i64> {
    candidate.tags.get(key)?.parse().ok()
}

fn tag_bool(candidate: &greed_kernel::TradeCandidate, key: &str) -> bool {
    candidate
        .tags
        .get(key)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn take_profit_ladder(candidate: &greed_kernel::TradeCandidate) -> Option<Vec<(f64, f64)>> {
    let encoded = candidate.tags.get("take_profit_ladder")?;
    let values: Option<Vec<_>> = encoded
        .split(',')
        .map(|leg| {
            let (target_r, fraction) = leg.split_once(':')?;
            Some((target_r.parse::<f64>().ok()?, fraction.parse::<f64>().ok()?))
        })
        .collect();
    values.filter(|legs| {
        !legs.is_empty()
            && legs.iter().all(|(target_r, fraction)| {
                target_r.is_finite() && *target_r > 0.0 && fraction.is_finite() && *fraction > 0.0
            })
            && legs.iter().map(|(_, fraction)| *fraction).sum::<f64>() <= 1.0 + 1e-6
    })
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
        let mut liquidity_rejections = 0u64;
        let mut liquidity_scaled_plans = 0u64;
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
            let mut break_even_buffer_pct = tag_f64(c, "break_even_buffer_pct")
                .unwrap_or(self.config.break_even_buffer_pct)
                .clamp(0.0, 0.01);
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
            let desired_notional = (a.equity_usd * risk_pct / stop_pct).min(
                a.equity_usd
                    * tag_f64(c, "max_notional_multiple")
                        .unwrap_or(self.config.max_notional_per_trade_multiple)
                        .clamp(0.20, self.config.max_notional_per_trade_multiple),
            );
            let book = i.book.as_ref().expect("market_ok requires a live book");
            let liquidity = liquidity_sizing(book, c.side, desired_notional, &self.config);
            let minimum_notional = (desired_notional * self.config.min_liquidity_size_ratio)
                .max(a.equity_usd * self.config.min_liquidity_notional_multiple)
                .min(desired_notional);
            let notional = desired_notional.min(liquidity.notional_cap_usd);
            let liquidity_ok = notional + 1e-6 >= minimum_notional;
            let was_scaled = liquidity_ok && notional + 1e-6 < desired_notional;
            if tag_bool(c, "cost_aware_profit_shield") {
                break_even_buffer_pct = cost_aware_profit_shield_buffer(
                    break_even_buffer_pct,
                    liquidity.expected_exit_slippage_bps,
                );
            }
            let liquidity_reason = if !liquidity_ok {
                Some(format!(
                    "{} exit liquidity supports ${notional:.0} / minimum ${minimum_notional:.0} (wanted ${desired_notional:.0})",
                    if c.side == Side::Buy { "bid" } else { "ask" }
                ))
            } else if was_scaled {
                Some(format!(
                    "liquidity scaled ${desired_notional:.0} to ${notional:.0}"
                ))
            } else {
                None
            };
            out.push(ArtifactRecord {
                key: format!("portfolio.liquidity.{}", c.id),
                producer: self.id.clone(),
                artifact: Artifact::State(StateArtifact {
                    state: if !liquidity_ok {
                        "too_thin"
                    } else if was_scaled {
                        "scaled"
                    } else {
                        "full_size"
                    }
                    .into(),
                    score: notional / desired_notional.max(1.0),
                    side: Some(c.side),
                    verdict: if liquidity_ok {
                        Verdict::Pass
                    } else {
                        Verdict::Block
                    },
                    reasons: liquidity_reason.into_iter().collect(),
                    metrics: BTreeMap::from([
                        ("desired_notional_usd".into(), desired_notional),
                        ("sized_notional_usd".into(), notional),
                        ("minimum_notional_usd".into(), minimum_notional),
                        (
                            "visible_exit_depth_usd".into(),
                            liquidity.visible_exit_depth_usd,
                        ),
                        ("impact_cap_usd".into(), liquidity.impact_cap_usd),
                        ("liquidity_cap_usd".into(), liquidity.notional_cap_usd),
                        (
                            "expected_exit_slippage_bps".into(),
                            liquidity.expected_exit_slippage_bps.unwrap_or(-1.0),
                        ),
                    ]),
                    meta: meta(
                        ctx.frame.as_of_ms,
                        30_000,
                        DataQuality::Complete,
                        1.0,
                        vec![format!("{}.book", c.symbol)],
                    ),
                }),
            });
            if !liquidity_ok {
                liquidity_rejections += 1;
                planned_symbols.remove(&c.symbol);
                continue;
            }
            liquidity_scaled_plans += u64::from(was_scaled);
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
            let configured_ladder = take_profit_ladder(c);
            let fixed_time_exit = tag_bool(c, "fixed_time_exit");
            let disable_take_profit = tag_bool(c, "disable_take_profit");
            let mut take_profit_prices = configured_ladder
                .as_ref()
                .map(|legs| {
                    legs.iter()
                        .map(|(target_r, fraction)| {
                            (
                                c.reference_price * (1.0 + sign * stop_pct * target_r),
                                *fraction,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec![(tp1, take_fraction)]);
            if disable_take_profit {
                take_profit_prices.clear();
            }
            if !disable_take_profit
                && configured_ladder.is_none()
                && take_fraction < 1.0
                && self.config.runner_take_profit_r > 0.0
            {
                let runner = c.reference_price
                    * (1.0
                        + sign * self.config.initial_stop_pct * self.config.runner_take_profit_r);
                take_profit_prices.push((runner, 1.0 - self.config.first_take_profit_fraction));
            }
            let staged_exit_fraction = take_profit_prices
                .iter()
                .map(|(_, fraction)| *fraction)
                .sum::<f64>();
            let first_exit_fraction = take_profit_prices
                .first()
                .map(|value| value.1)
                .unwrap_or_default();
            let mut signal_context = c.tags.clone();
            signal_context.insert(
                "effective_profit_shield_buffer_pct".into(),
                break_even_buffer_pct.to_string(),
            );
            signal_context.insert("desired_notional_usd".into(), desired_notional.to_string());
            signal_context.insert(
                "liquidity_cap_usd".into(),
                liquidity.notional_cap_usd.to_string(),
            );
            signal_context.insert("liquidity_sized_notional_usd".into(), notional.to_string());
            signal_context.insert(
                "visible_exit_depth_usd".into(),
                liquidity.visible_exit_depth_usd.to_string(),
            );
            if let Some(slippage) = liquidity.expected_exit_slippage_bps {
                signal_context.insert(
                    "expected_sized_exit_slippage_bps".into(),
                    slippage.to_string(),
                );
            }
            let plan = PositionPlan {
                candidate_id: c.id.clone(),
                signal_context,
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
                min_fill_ratio: tag_f64(c, "min_fill_ratio")
                    .unwrap_or_default()
                    .clamp(0.0, 1.0),
                min_managed_fill_ratio: tag_f64(c, "min_managed_fill_ratio")
                    .unwrap_or_default()
                    .clamp(0.0, 1.0),
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
                break_even_after_fraction: (!disable_take_profit
                    && staged_exit_fraction < 1.0 - 1e-6)
                    .then_some(first_exit_fraction),
                unprotected_runner_fraction: tag_f64(c, "unprotected_runner_fraction")
                    .filter(|fraction| (0.01..=0.25).contains(fraction)),
                break_even_buffer_pct,
                profit_shield_activation_pct: (!fixed_time_exit
                    && !disable_take_profit
                    && (take_fraction < 1.0 || explicit_profit_protection)
                    && !c.tags.contains_key("unprotected_runner_fraction")
                    && profit_shield_activation_r > 0.0)
                    .then_some(stop_pct * profit_shield_activation_r),
                // Start locking profit before TP1. Waiting until TP1 meant a
                // position could reach roughly +1R, miss the 2R partial, and
                // surrender almost all open profit back to the cost shield.
                trailing_activation_pct: (!fixed_time_exit
                    && !disable_take_profit
                    && (take_fraction < 1.0 || explicit_profit_protection)
                    && !c.tags.contains_key("unprotected_runner_fraction"))
                .then_some(stop_pct * trailing_activation_r),
                trailing_distance_pct: (!fixed_time_exit
                    && !disable_take_profit
                    && (take_fraction < 1.0 || explicit_profit_protection)
                    && !c.tags.contains_key("unprotected_runner_fraction"))
                .then_some(trailing_distance_pct),
                early_failure_after_ms: if fixed_time_exit {
                    0
                } else {
                    early_failure_after_ms
                },
                early_failure_adverse_pct: if fixed_time_exit {
                    0.0
                } else {
                    stop_pct * early_failure_adverse_r
                },
                early_failure_max_favorable_pct: if fixed_time_exit {
                    0.0
                } else {
                    stop_pct * early_failure_max_mfe_r
                },
                max_hold_ms: tag_i64(c, "max_hold_ms")
                    .unwrap_or_else(|| i64::from(self.config.max_hold_minutes) * 60_000),
                fixed_time_exit,
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
            state
                .metrics
                .insert("liquidity_rejections".into(), liquidity_rejections as f64);
            state.metrics.insert(
                "liquidity_scaled_plans".into(),
                liquidity_scaled_plans as f64,
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
    fn liquidity_cap_uses_the_protective_exit_side_and_visible_participation() {
        let book = BookState {
            meta: meta(),
            bid: 100.0,
            ask: 100.01,
            bid_depth_usd: 10_000.0,
            ask_depth_usd: 100_000.0,
            expected_buy_slippage_bps: Some(0.0),
            expected_sell_slippage_bps: Some(0.0),
            bids: vec![PriceLevel {
                price: 100.0,
                quantity: 100.0,
            }],
            asks: vec![PriceLevel {
                price: 100.01,
                quantity: 1_000.0,
            }],
        };
        let sizing = liquidity_sizing(&book, Side::Buy, 6_000.0, &RiskConfig::default());
        assert!((sizing.notional_cap_usd - 3_500.0).abs() < 1e-6);
        assert_eq!(sizing.visible_exit_depth_usd, 10_000.0);
        assert_eq!(sizing.expected_exit_slippage_bps, Some(0.0));
    }

    #[test]
    fn liquidity_impact_cap_stops_before_a_deep_but_distant_level() {
        let levels = vec![
            PriceLevel {
                price: 100.0,
                quantity: 10.0,
            },
            PriceLevel {
                price: 99.0,
                quantity: 100.0,
            },
        ];
        let cap = sweep_capacity_usd(&levels, 100.0, 8.0);
        assert!(cap > 1_000.0);
        assert!(cap < 2_000.0);
        assert!(sweep_slippage_bps(&levels, cap, 100.0).unwrap() <= 8.01);
    }

    fn planned_output_for_bid_depth(bid_depth_usd: f64) -> Vec<ArtifactRecord> {
        let mut record = candidate("trend:ALT", "trend_continuation", "ALTUSDT", 1);
        let Artifact::Candidate(value) = &mut record.artifact else {
            panic!("candidate fixture must contain a candidate");
        };
        value
            .tags
            .insert("risk_per_trade_pct".into(), "0.015".into());
        let artifacts = BTreeMap::from([(record.key.clone(), record)]);
        let mut market = instrument("ALTUSDT");
        let book = market.book.as_mut().expect("book fixture");
        book.bid_depth_usd = bid_depth_usd;
        book.bids = vec![PriceLevel {
            price: book.bid,
            quantity: bid_depth_usd / book.bid,
        }];
        let frame = MarketFrame {
            as_of_ms: 2_000,
            instruments: BTreeMap::from([("ALTUSDT".into(), market)]),
            account: AccountFrame {
                equity_usd: 5_000.0,
                cash_usd: 5_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 5_000.0,
                risk_day_start_equity_usd: 5_000.0,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        };
        PositionPlannerNode::new(vec![], RiskConfig::default())
            .evaluate(&NodeContext {
                frame: &frame,
                artifacts: &artifacts,
            })
            .unwrap()
    }

    #[test]
    fn planner_scales_a_trade_when_liquidity_still_supports_meaningful_size() {
        let output = planned_output_for_bid_depth(12_000.0);
        let plan = output.iter().find_map(|record| match &record.artifact {
            Artifact::PositionPlan(plan) => Some(plan),
            _ => None,
        });
        assert!((plan.expect("scaled plan").notional_usd - 4_200.0).abs() < 1e-6);
        let state = output
            .iter()
            .find(|record| record.key.starts_with("portfolio.liquidity."))
            .and_then(|record| match &record.artifact {
                Artifact::State(state) => Some(state),
                _ => None,
            })
            .expect("liquidity state");
        assert_eq!(state.state, "scaled");
        assert_eq!(state.verdict, Verdict::Pass);
    }

    #[test]
    fn planner_rejects_a_liquidity_reduction_below_half_desired_size() {
        let output = planned_output_for_bid_depth(6_000.0);
        assert!(!output
            .iter()
            .any(|record| matches!(record.artifact, Artifact::PositionPlan(_))));
        let state = output
            .iter()
            .find(|record| record.key.starts_with("portfolio.liquidity."))
            .and_then(|record| match &record.artifact {
                Artifact::State(state) => Some(state),
                _ => None,
            })
            .expect("liquidity state");
        assert_eq!(state.state, "too_thin");
        assert_eq!(state.verdict, Verdict::Block);
    }

    #[test]
    fn one_symbol_gets_only_the_highest_priority_strategy_plan() {
        let records = [
            candidate("trend:BTC", "trend_continuation", "BTCUSDT", 1),
            candidate("primary:BTC", "primary", "BTCUSDT", 4),
            candidate("secondary:ETH", "secondary", "ETHUSDT", 2),
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
        assert!(plans.iter().any(|plan| plan.candidate_id == "primary:BTC"));
        assert!(!plans.iter().any(|plan| plan.candidate_id == "trend:BTC"));
        assert!(plans
            .iter()
            .any(|plan| plan.candidate_id == "secondary:ETH"));
    }

    #[test]
    fn parses_a_multi_stage_take_profit_ladder() {
        let mut record = candidate("ladder:BTC", "ladder", "BTCUSDT", 5);
        let Artifact::Candidate(value) = &mut record.artifact else {
            panic!("candidate fixture must contain a candidate");
        };
        value.tags.insert(
            "take_profit_ladder".into(),
            "0.8:0.20,1.5:0.25,2.5:0.25,3.5:0.20".into(),
        );
        let legs = take_profit_ladder(value).expect("valid ladder");
        assert_eq!(legs.len(), 4);
        assert!((legs.iter().map(|(_, fraction)| fraction).sum::<f64>() - 0.90).abs() < 1e-9);
    }

    #[test]
    fn trend_plan_uses_risk_budget_and_rejects_tiny_managed_fills() {
        let mut record = candidate("trend:BTC", "trend_continuation", "BTCUSDT", 1);
        let Artifact::Candidate(value) = &mut record.artifact else {
            panic!("candidate fixture must contain a candidate");
        };
        value
            .tags
            .insert("risk_per_trade_pct".into(), "0.006".into());
        value.tags.insert("min_fill_ratio".into(), "0.80".into());
        value
            .tags
            .insert("profit_shield_activation_r".into(), "0.32".into());
        value
            .tags
            .insert("break_even_buffer_pct".into(), "0.0015".into());
        let artifacts = BTreeMap::from([(record.key.clone(), record)]);
        let frame = MarketFrame {
            as_of_ms: 2_000,
            instruments: BTreeMap::from([("BTCUSDT".into(), instrument("BTCUSDT"))]),
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
        let risk = RiskConfig {
            initial_stop_pct: 0.0125,
            ..RiskConfig::default()
        };
        let mut planner = PositionPlannerNode::new(vec![], risk);
        let output = planner
            .evaluate(&NodeContext {
                frame: &frame,
                artifacts: &artifacts,
            })
            .unwrap();
        let plan = output
            .iter()
            .find_map(|record| match &record.artifact {
                Artifact::PositionPlan(plan) => Some(plan),
                _ => None,
            })
            .expect("trend candidate should produce a plan");
        assert!((plan.notional_usd - 960.0).abs() < 1e-9);
        assert!((plan.min_fill_ratio - 0.80).abs() < 1e-9);
        assert_eq!(plan.profit_shield_activation_pct, Some(0.004));
        assert!((plan.break_even_buffer_pct - 0.0015).abs() < 1e-9);
        assert_eq!(
            plan.signal_context
                .get("risk_per_trade_pct")
                .map(String::as_str),
            Some("0.006")
        );
        assert!(!plan.taker_fallback);
    }

    #[test]
    fn cost_aware_profit_shield_reserves_stressed_exit_cost() {
        assert!((cost_aware_profit_shield_buffer(0.0015, None) - 0.0015).abs() < 1e-12);
        // 7 bps fees + 1.5 * 8 bps expected exit impact + 2 bps desired net.
        assert!((cost_aware_profit_shield_buffer(0.0015, Some(8.0)) - 0.0021).abs() < 1e-12);
        assert!((cost_aware_profit_shield_buffer(0.0025, Some(2.0)) - 0.0025).abs() < 1e-12);
        assert_eq!(cost_aware_profit_shield_buffer(0.0015, Some(80.0)), 0.003);
    }

    #[test]
    fn fast_plan_scales_out_twice_and_trails_the_runner() {
        let mut record = candidate("fast:ALT", "fast_trend_activation", "ALTUSDT", 1);
        let Artifact::Candidate(value) = &mut record.artifact else {
            panic!("candidate fixture must contain a candidate");
        };
        value.tags.extend(BTreeMap::from([
            ("stop_pct".into(), "0.005".into()),
            ("risk_per_trade_pct".into(), "0.005".into()),
            ("max_notional_multiple".into(), "1.0".into()),
            ("take_profit_ladder".into(), "1.0:0.30,2.0:0.40".into()),
            ("take_profit_fraction".into(), "0.30".into()),
            ("profit_shield_activation_r".into(), "0.5".into()),
            ("pre_tp_trailing_activation_r".into(), "1.0".into()),
            ("trailing_distance_pct".into(), "0.0035".into()),
        ]));
        let artifacts = BTreeMap::from([(record.key.clone(), record)]);
        let frame = MarketFrame {
            as_of_ms: 2_000,
            instruments: BTreeMap::from([("ALTUSDT".into(), instrument("ALTUSDT"))]),
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
        let plan = output
            .iter()
            .find_map(|record| match &record.artifact {
                Artifact::PositionPlan(plan) => Some(plan),
                _ => None,
            })
            .expect("fast candidate should produce a plan");

        assert!((plan.notional_usd - 2_000.0).abs() < 1e-9);
        assert_eq!(plan.take_profit_prices.len(), 2);
        assert!((plan.take_profit_prices[0].0 - 100.5).abs() < 1e-9);
        assert!((plan.take_profit_prices[0].1 - 0.30).abs() < 1e-9);
        assert!((plan.take_profit_prices[1].0 - 101.0).abs() < 1e-9);
        assert!((plan.take_profit_prices[1].1 - 0.40).abs() < 1e-9);
        assert_eq!(plan.break_even_after_fraction, Some(0.30));
        assert_eq!(plan.profit_shield_activation_pct, Some(0.0025));
        assert_eq!(plan.trailing_activation_pct, Some(0.005));
        assert_eq!(plan.trailing_distance_pct, Some(0.0035));
    }

    #[test]
    fn liquidation_plan_has_stop_only_and_a_fixed_deadline() {
        let mut record = candidate(
            "liquidation_exhaustion_reversal:ALTUSDT:1000",
            "liquidation_exhaustion_reversal",
            "ALTUSDT",
            2,
        );
        let Artifact::Candidate(value) = &mut record.artifact else {
            panic!("candidate fixture must contain a candidate");
        };
        value.tags.extend(BTreeMap::from([
            ("stop_pct".into(), "0.02".into()),
            ("risk_per_trade_pct".into(), "0.01".into()),
            ("max_notional_multiple".into(), "1.0".into()),
            ("disable_take_profit".into(), "true".into()),
            ("fixed_time_exit".into(), "true".into()),
            ("max_hold_ms".into(), "900000".into()),
        ]));
        let artifacts = BTreeMap::from([(record.key.clone(), record)]);
        let frame = MarketFrame {
            as_of_ms: 2_000,
            instruments: BTreeMap::from([("ALTUSDT".into(), instrument("ALTUSDT"))]),
            account: AccountFrame {
                equity_usd: 5_000.0,
                cash_usd: 5_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 5_000.0,
                risk_day_start_equity_usd: 5_000.0,
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
        let plan = output
            .iter()
            .find_map(|record| match &record.artifact {
                Artifact::PositionPlan(plan) => Some(plan),
                _ => None,
            })
            .unwrap();
        assert!((plan.notional_usd - 2_500.0).abs() < 1e-9);
        assert!(plan.take_profit_prices.is_empty());
        assert_eq!(plan.max_hold_ms, 900_000);
        assert!(plan.fixed_time_exit);
        assert_eq!(plan.profit_shield_activation_pct, None);
        assert_eq!(plan.trailing_activation_pct, None);
        assert_eq!(plan.trailing_distance_pct, None);
        assert_eq!(plan.early_failure_after_ms, 0);
        assert_eq!(plan.early_failure_adverse_pct, 0.0);
        assert_eq!(plan.early_failure_max_favorable_pct, 0.0);
    }
}
