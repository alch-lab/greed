use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

// Paper-only sprint experiment: close the entire position once its executable
// profit can contribute roughly 0.30% of the current sleeve equity. The
// planner converts this account-level objective into a price target after the
// actual liquidity-sized notional and estimated round-trip cost are known.
const FAST_TARGET_ACCOUNT_PROFIT_PCT: f64 = 0.003;

pub struct FastTrendActivationNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl FastTrendActivationNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.fast_trend_activation".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn is_major(symbol: &str) -> bool {
    ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].contains(&symbol)
}

fn median(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut values: Vec<_> = values.filter(|value| value.is_finite()).collect();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    match values.len() {
        0 => None,
        length if length % 2 == 0 => Some((values[middle - 1] + values[middle]) * 0.5),
        _ => Some(values[middle]),
    }
}

fn price_range(values: &[&Candle]) -> f64 {
    let high = values.iter().map(|bar| bar.high).fold(0.0, f64::max);
    let low = values
        .iter()
        .map(|bar| bar.low)
        .fold(f64::INFINITY, f64::min);
    high / low.max(f64::EPSILON) - 1.0
}

fn atr(values: &[&Candle], end: usize, length: usize) -> f64 {
    let start = end.saturating_sub(length.saturating_sub(1)).max(1);
    median((start..=end).map(|index| {
        let bar = values[index];
        (bar.high - bar.low)
            .max((bar.high - values[index - 1].close).abs())
            .max((bar.low - values[index - 1].close).abs())
    }))
    .unwrap_or_default()
}

fn oi_change(values: &[(i64, f64)], at_ms: i64, lookback_ms: i64) -> Option<f64> {
    let current = values.iter().rev().find(|(ts, _)| *ts <= at_ms)?.1;
    let prior = values
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= at_ms - lookback_ms)?
        .1;
    (prior > 0.0).then_some(current / prior - 1.0)
}

#[allow(clippy::too_many_arguments)]
fn ignition_pattern(
    breakout: bool,
    directional_body_return: f64,
    volume_ratio: f64,
    directional_flow: Option<f64>,
    compression_ratio: f64,
    directional_prebreak_return_1h: f64,
    directional_return_4h: f64,
    config: &LaneConfig,
) -> Option<&'static str> {
    let common = breakout
        && directional_body_return >= config.fast_min_body_return_5m
        && volume_ratio >= config.fast_min_volume_ratio_5m
        && directional_flow.is_some_and(|value| value >= config.fast_min_flow_5m)
        && directional_prebreak_return_1h <= config.fast_max_prebreak_return_1h
        && directional_return_4h <= config.fast_max_return_4h;
    if common && compression_ratio <= config.fast_max_compression_ratio {
        return Some("compression_breakout");
    }
    (common
        && config.fast_reacceleration_enabled
        && compression_ratio <= config.fast_reacceleration_max_compression_ratio
        && directional_body_return >= config.fast_reacceleration_min_body_return_5m
        && volume_ratio >= config.fast_reacceleration_min_volume_ratio_5m
        && directional_flow.is_some_and(|value| value >= config.fast_reacceleration_min_flow_5m))
    .then_some("trend_reacceleration")
}

fn direct_confirmation_allowed(
    pattern: Option<&str>,
    directional_market_breadth: f64,
    directional_market_return_1h: f64,
    directional_prebreak_return_1h: f64,
    directional_extension: f64,
    config: &LaneConfig,
) -> bool {
    match pattern {
        Some("compression_breakout") => {
            // Do not cross immediately on a single bar that merely snaps
            // against the preceding hour. Require a later touch-and-reclaim;
            // this is the observed RUNE failure shape.
            directional_prebreak_return_1h >= -config.fast_min_market_return_1h
        }
        Some("trend_reacceleration") => {
            directional_market_return_1h >= config.fast_reacceleration_direct_min_market_return_1h
                && directional_extension <= config.fast_reacceleration_direct_max_extension
                && (directional_market_breadth
                    >= config.fast_reacceleration_direct_min_market_breadth
                    || directional_extension <= config.fast_reacceleration_direct_early_extension)
        }
        _ => false,
    }
}

fn confirmation_pattern(
    minute: &Candle,
    side: Side,
    breakout_level: f64,
    direct_reacceleration_ready: bool,
) -> Option<(&'static str, f64)> {
    let sign = side.sign();
    let minute_flow = minute
        .taker_buy_quote
        .map(|buy| 2.0 * buy / minute.quote_volume.max(1.0) - 1.0)?;
    let directional_minute_flow = sign * minute_flow;
    let beyond = if side == Side::Buy {
        minute.close >= breakout_level
    } else {
        minute.close <= breakout_level
    };
    let touched = if side == Side::Buy {
        minute.low <= breakout_level * 1.0015
    } else {
        minute.high >= breakout_level * 0.9985
    };
    if beyond && touched && directional_minute_flow >= 0.02 {
        Some(("shallow_reclaim", minute_flow))
    } else if beyond && directional_minute_flow >= 0.08 && direct_reacceleration_ready {
        Some(("direct_continuation", minute_flow))
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn is_late_exhaustion(
    entry_pattern: &str,
    directional_extension: f64,
    directional_return_4h: f64,
    directional_market_breadth: f64,
    directional_market_return_1h: f64,
    directional_flow: Option<f64>,
    config: &LaneConfig,
) -> bool {
    config.fast_late_exhaustion_veto_enabled
        && entry_pattern == "shallow_reclaim"
        && directional_extension >= config.fast_late_exhaustion_min_extension
        && directional_return_4h >= config.fast_late_exhaustion_min_return_4h
        && directional_market_breadth < config.fast_late_exhaustion_max_market_breadth
        && directional_market_return_1h < 0.0
        && directional_flow.is_some_and(|value| value < config.fast_late_exhaustion_max_flow)
}

impl StrategyNode for FastTrendActivationNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let market_returns: Vec<_> = self
            .symbols
            .iter()
            .filter(|symbol| !is_major(symbol))
            .filter_map(|symbol| ctx.frame.instrument(symbol))
            .filter_map(|instrument| instrument.fast_perpetual.as_ref())
            .filter_map(|series| {
                let closed: Vec<_> = series.values.iter().filter(|bar| bar.closed).collect();
                (closed.len() >= 13).then(|| {
                    closed.last().expect("length checked").close / closed[closed.len() - 13].close
                        - 1.0
                })
            })
            .collect();
        let market_return_1h = median(market_returns.iter().copied()).unwrap_or_default();
        let market_breadth = if market_returns.is_empty() {
            0.0
        } else {
            market_returns.iter().filter(|value| **value > 0.0).count() as f64
                / market_returns.len() as f64
        };
        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0_u64;
        let mut ignition_hits = 0_u64;
        let mut confirmation_hits = 0_u64;
        let mut oi_ready = 0_u64;

        for symbol in self.symbols.iter().filter(|symbol| !is_major(symbol)) {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(fast) = instrument.fast_perpetual.as_ref() else {
                continue;
            };
            let closed: Vec<_> = fast.values.iter().filter(|bar| bar.closed).collect();
            if closed.len() < 90 {
                continue;
            }
            inspected += 1;
            let index = closed.len() - 1;
            let bar = closed[index];
            let prior = &closed[index - 12..index];
            let breakout_high = prior.iter().map(|value| value.high).fold(0.0, f64::max);
            let breakout_low = prior
                .iter()
                .map(|value| value.low)
                .fold(f64::INFINITY, f64::min);
            let body_return = bar.close / bar.open.max(f64::EPSILON) - 1.0;
            let side = if bar.close < breakout_low {
                Side::Sell
            } else {
                Side::Buy
            };
            let sign = side.sign();
            let breakout_level = if side == Side::Buy {
                breakout_high
            } else {
                breakout_low
            };
            let directional_body_return = sign * body_return;
            let baseline_volume = median(
                closed[index - 36..index]
                    .iter()
                    .map(|value| value.quote_volume),
            )
            .unwrap_or_default();
            let volume_ratio = bar.quote_volume / baseline_volume.max(1.0);
            let flow = bar
                .taker_buy_quote
                .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0);
            let directional_flow = flow.map(|value| sign * value);
            let current_range = price_range(prior);
            let compression_ratio = current_range
                / median((1..=6).map(|window| {
                    let end = index - 12 * window;
                    price_range(&closed[end - 12..end])
                }))
                .unwrap_or_default()
                .max(f64::EPSILON);
            let prebreak_return_1h = bar.open / closed[index - 12].open - 1.0;
            let return_4h = bar.close / closed[index - 48].close - 1.0;
            let directional_prebreak_return_1h = sign * prebreak_return_1h;
            let directional_return_4h = sign * return_4h;
            let directional_market_return_1h = sign * market_return_1h;
            let directional_market_breadth = if market_returns.is_empty() {
                0.0
            } else {
                market_returns
                    .iter()
                    .filter(|value| sign * **value > 0.0)
                    .count() as f64
                    / market_returns.len() as f64
            };
            // A fast single-name ignition should not require the entire
            // altcoin market to move first. Keep the market only as a strong
            // opposing-regime veto; otherwise idiosyncratic leaders are
            // systematically discovered after their useful move is over.
            let market_ready = directional_market_return_1h
                > -self.config.fast_min_market_return_1h
                || directional_market_breadth >= 1.0 - self.config.fast_min_market_breadth;
            let breakout = if side == Side::Buy {
                bar.close > breakout_level
            } else {
                bar.close < breakout_level
            };
            let ignition_pattern = ignition_pattern(
                breakout,
                directional_body_return,
                volume_ratio,
                directional_flow,
                compression_ratio,
                directional_prebreak_return_1h,
                directional_return_4h,
                &self.config,
            );
            let ignition = ignition_pattern.is_some();
            ignition_hits += u64::from(ignition);

            let directional_extension = directional_prebreak_return_1h + directional_body_return;
            let direct_reacceleration_ready = direct_confirmation_allowed(
                ignition_pattern,
                directional_market_breadth,
                directional_market_return_1h,
                directional_prebreak_return_1h,
                directional_extension,
                &self.config,
            );

            let confirmation_source = instrument
                .micro_perpetual
                .as_ref()
                .map(|series| series.meta.source.as_str())
                .unwrap_or("missing");
            let confirmation = instrument
                .micro_perpetual
                .as_ref()
                .into_iter()
                .flat_map(|series| series.values.iter())
                .filter(|minute| {
                    minute.closed
                        && minute.close_ms > bar.close_ms
                        && minute.close_ms <= bar.close_ms + 3 * 60_000
                })
                .find_map(|minute| {
                    confirmation_pattern(minute, side, breakout_level, direct_reacceleration_ready)
                        .map(|(pattern, flow)| (minute, pattern, flow))
                });
            confirmation_hits += u64::from(ignition && confirmation.is_some());
            let entry_pattern = confirmation
                .map(|(_, pattern, _)| pattern)
                .unwrap_or("awaiting_confirmation");
            let late_exhaustion = is_late_exhaustion(
                entry_pattern,
                directional_extension,
                directional_return_4h,
                directional_market_breadth,
                directional_market_return_1h,
                directional_flow,
                &self.config,
            );

            let oi_values: Vec<_> = instrument
                .open_interest
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .map(|value| (value.timestamp_ms, value.value_usd))
                        .collect()
                })
                .unwrap_or_default();
            let oi_15m = oi_change(&oi_values, bar.close_ms, 15 * 60_000);
            let oi_60m = oi_change(&oi_values, bar.close_ms, 60 * 60_000);
            // Moderate deleveraging can drive a squeeze just as effectively as
            // fresh positioning. Reject only an oversized OI shock; requiring
            // OI to be positive discarded both short squeezes and long
            // liquidation continuations.
            let oi_ok = oi_15m
                .is_some_and(|value| value.abs() <= self.config.fast_max_oi_change_15m)
                && oi_60m
                    .is_some_and(|value| value.abs() <= self.config.fast_max_oi_change_15m * 2.0);
            oi_ready += u64::from(oi_15m.is_some() && oi_60m.is_some());

            let mut blockers = Vec::new();
            if !market_ready {
                blockers.push(format!(
                    "altcoin market strongly opposes the signal: directional 1h median {:.2}% / breadth {:.0}%",
                    directional_market_return_1h * 100.0,
                    directional_market_breadth * 100.0
                ));
            }
            if late_exhaustion {
                blockers.push(format!(
                    "late shallow reclaim lacks broad confirmation: extension {:.2}%, 4h {:.2}%, directional breadth {:.0}%, market 1h {:.2}%, taker pressure {:.0}%",
                    directional_extension * 100.0,
                    directional_return_4h * 100.0,
                    directional_market_breadth * 100.0,
                    directional_market_return_1h * 100.0,
                    directional_flow.unwrap_or_default() * 100.0,
                ));
            }
            if !breakout {
                blockers.push("waiting for a completed 5m range breakout".into());
            }
            if directional_body_return < self.config.fast_min_body_return_5m {
                blockers.push(format!(
                    "5m body {:.2}% is below activation",
                    directional_body_return * 100.0
                ));
            }
            if volume_ratio < self.config.fast_min_volume_ratio_5m {
                blockers.push(format!("5m volume {volume_ratio:.2}x is below activation"));
            }
            match directional_flow {
                Some(value) if value < self.config.fast_min_flow_5m => blockers.push(format!(
                    "5m taker pressure {:.0}% is below activation",
                    value * 100.0
                )),
                None => blockers.push("5m taker flow is warming".into()),
                _ => {}
            }
            let compressed = compression_ratio <= self.config.fast_max_compression_ratio;
            let strong_reacceleration = self.config.fast_reacceleration_enabled
                && compression_ratio <= self.config.fast_reacceleration_max_compression_ratio
                && directional_body_return >= self.config.fast_reacceleration_min_body_return_5m
                && volume_ratio >= self.config.fast_reacceleration_min_volume_ratio_5m
                && directional_flow
                    .is_some_and(|value| value >= self.config.fast_reacceleration_min_flow_5m);
            if !compressed && !strong_reacceleration {
                blockers.push(format!(
                    "prior range {compression_ratio:.2}x is neither compressed nor backed by a strong reacceleration"
                ));
            }
            if directional_prebreak_return_1h > self.config.fast_max_prebreak_return_1h
                || directional_return_4h > self.config.fast_max_return_4h
            {
                blockers.push("move is already mature; the fast lane will not chase it".into());
            }
            if ignition && confirmation.is_none() {
                if ignition_pattern == Some("trend_reacceleration") && !direct_reacceleration_ready
                {
                    blockers.push(format!(
                        "late reacceleration requires a 1m touch-and-reclaim: breadth {:.0}%, directional market 1h {:.2}%, extension {:.2}%",
                        directional_market_breadth * 100.0,
                        directional_market_return_1h * 100.0,
                        directional_extension * 100.0
                    ));
                } else {
                    blockers
                        .push("waiting up to 3m for a 1m reclaim or direct continuation".into());
                }
            }
            if oi_15m.is_none() || oi_60m.is_none() {
                blockers.push("open-interest history is warming".into());
            } else if !oi_ok {
                blockers.push(format!(
                    "OI shock is too large: require |15m| <= {:.1}% and |60m| <= {:.1}%",
                    self.config.fast_max_oi_change_15m * 100.0,
                    self.config.fast_max_oi_change_15m * 200.0
                ));
            }

            match instrument.book.as_ref() {
                Some(book) if book.meta.usable_at(ctx.frame.as_of_ms) => {
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread:.1} bps is too wide"));
                    }
                }
                _ => blockers.push("order book is warming".into()),
            }

            let signal_ms = confirmation.map_or(bar.close_ms, |(value, _, _)| value.close_ms);
            if ctx.frame.as_of_ms - signal_ms
                > i64::from(self.config.fast_entry_timeout_seconds) * 1_000
            {
                blockers.push("the 1m confirmation expired".into());
            }
            let reference_price =
                confirmation.map_or(instrument.price, |(value, _, _)| value.close);
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let stop_pct =
                (1.25 * atr(&closed, index, 20) / reference_price.max(f64::EPSILON)).max(0.0035);
            let score = directional_body_return.max(0.0)
                * volume_ratio
                * (1.0 + directional_flow.unwrap_or_default().max(0.0))
                * (1.0 + directional_market_return_1h * 10.0);
            let progress = (11_usize.saturating_sub(blockers.len()).min(11) as f64) / 11.0;
            let mut tags = BTreeMap::from([
                ("lane".into(), "fast_trend_activation".into()),
                ("priority".into(), "0.8".into()),
                ("entry_pattern".into(), entry_pattern.into()),
                (
                    "confirmation_flow_1m".into(),
                    confirmation
                        .map(|(_, _, flow)| flow.to_string())
                        .unwrap_or_else(|| "missing".into()),
                ),
                (
                    "confirmation_close_ms".into(),
                    confirmation
                        .map(|(minute, _, _)| minute.close_ms.to_string())
                        .unwrap_or_else(|| "missing".into()),
                ),
                (
                    "confirmation_series_source".into(),
                    confirmation_source.into(),
                ),
                (
                    "confirmation_terminal_verified".into(),
                    confirmation.is_some().to_string(),
                ),
                ("market_return_1h".into(), market_return_1h.to_string()),
                (
                    "market_breadth_1h".into(),
                    directional_market_breadth.to_string(),
                ),
                ("body_return_5m".into(), body_return.to_string()),
                ("volume_ratio_5m".into(), volume_ratio.to_string()),
                (
                    "directional_flow_5m".into(),
                    flow.unwrap_or_default().to_string(),
                ),
                ("compression_ratio".into(), compression_ratio.to_string()),
                ("prebreak_return_1h".into(), prebreak_return_1h.to_string()),
                ("return_4h".into(), return_4h.to_string()),
                (
                    "directional_extension".into(),
                    directional_extension.to_string(),
                ),
                (
                    "direct_reacceleration_ready".into(),
                    direct_reacceleration_ready.to_string(),
                ),
                (
                    "late_exhaustion_veto_triggered".into(),
                    late_exhaustion.to_string(),
                ),
                (
                    "ignition_pattern".into(),
                    ignition_pattern.unwrap_or("none").into(),
                ),
                (
                    "oi_change_15m".into(),
                    oi_15m.unwrap_or_default().to_string(),
                ),
                (
                    "oi_change_60m".into(),
                    oi_60m.unwrap_or_default().to_string(),
                ),
                ("stop_pct".into(), stop_pct.to_string()),
                ("target_r".into(), "1.0".into()),
                ("take_profit_fraction".into(), "1.0".into()),
                (
                    "target_account_profit_pct".into(),
                    FAST_TARGET_ACCOUNT_PROFIT_PCT.to_string(),
                ),
                ("cost_aware_full_take_profit".into(), "true".into()),
                // The planner replaces these placeholders with thresholds
                // derived from the final liquidity-sized dollar target.
                ("profit_shield_activation_r".into(), "1.0".into()),
                ("pre_tp_trailing_activation_r".into(), "1.0".into()),
                ("trailing_distance_pct".into(), "0.001".into()),
                ("cost_aware_profit_shield".into(), "true".into()),
                (
                    "risk_per_trade_pct".into(),
                    self.config.fast_risk_per_trade_pct.to_string(),
                ),
                ("max_notional_multiple".into(), "1.0".into()),
                (
                    "entry_timeout_ms".into(),
                    (i64::from(self.config.fast_entry_timeout_seconds) * 1_000).to_string(),
                ),
                (
                    "min_fill_ratio".into(),
                    self.config.fast_min_fill_ratio.to_string(),
                ),
                (
                    "min_managed_fill_ratio".into(),
                    self.config.fast_min_managed_fill_ratio.to_string(),
                ),
                ("entry_invalidation_bps".into(), "25".into()),
                ("max_entry_adverse_bps".into(), "6".into()),
                ("taker_fallback".into(), "false".into()),
                ("max_hold_ms".into(), (10 * 60_000).to_string()),
            ]);
            if verdict == Verdict::Pass {
                if let Some(book) = instrument.book.as_ref() {
                    if entry_pattern == "direct_continuation" {
                        tags.insert("bounded_taker_ioc".into(), "true".into());
                        tags.insert("max_entry_adverse_bps".into(), "6".into());
                        tags.insert("entry_timeout_ms".into(), "0".into());
                        tags.insert("min_fill_ratio".into(), "1.0".into());
                        tags.insert("min_managed_fill_ratio".into(), "0.0".into());
                    } else {
                        let entry_limit = if side == Side::Buy {
                            (reference_price * 0.9996).min(book.bid)
                        } else {
                            (reference_price * 1.0004).max(book.ask)
                        };
                        tags.insert("entry_limit".into(), entry_limit.to_string());
                        tags.insert("taker_fallback".into(), "true".into());
                        tags.insert("taker_fallback_max_adverse_bps".into(), "6".into());
                        tags.insert("taker_fallback_size_multiplier".into(), "0.5".into());
                    }
                }
            }
            let candidate = TradeCandidate {
                id: format!("fast_trend_activation:{symbol}:{signal_ms}"),
                recipe: "fast_trend_activation".into(),
                symbol: symbol.clone(),
                side,
                signal_ms,
                expires_ms: signal_ms + i64::from(self.config.fast_entry_timeout_seconds) * 1_000,
                reference_price,
                score,
                confidence: if verdict == Verdict::Pass {
                    0.78
                } else {
                    progress
                },
                verdict,
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_5m_ignition"),
                    format!("{symbol}.binance_1m_reclaim"),
                    format!("{symbol}.binance_open_interest"),
                    format!("{symbol}.book"),
                ],
                tags,
            };
            if verdict == Verdict::Pass {
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
        observations.truncate(6);
        let actionable = !passed.is_empty();
        let reason = observations
            .first()
            .map(|value| format!("{}: {}", value.2.symbol, value.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for complete 5m, 1m, OI and book data".into());
        let mut output = vec![ArtifactRecord {
            key: "lane.fast_trend_activation.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable { "ready" } else { "scanning" }.into(),
                score: passed
                    .first()
                    .map(|value| value.0)
                    .or_else(|| observations.first().map(|value| value.1))
                    .unwrap_or_default(),
                side: passed.first().map(|value| value.1.side),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("market_return_1h".into(), market_return_1h),
                    ("market_breadth_1h".into(), market_breadth),
                    ("ignition_hits".into(), ignition_hits as f64),
                    ("confirmation_hits".into(), confirmation_hits as f64),
                    ("oi_ready_symbols".into(), oi_ready as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_5m".into(),
                        "binance_1m".into(),
                        "binance_oi".into(),
                    ],
                ),
            }),
        }];
        for (_, candidate) in passed.into_iter().chain(
            observations
                .into_iter()
                .map(|(_, score, candidate)| (score, candidate)),
        ) {
            output.push(ArtifactRecord {
                key: format!("candidate.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::Candidate(candidate),
            });
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_even_and_odd_samples() {
        assert_eq!(median([3.0, 1.0, 2.0].into_iter()), Some(2.0));
        assert_eq!(median([4.0, 1.0, 3.0, 2.0].into_iter()), Some(2.5));
        assert_eq!(median(std::iter::empty()), None);
    }

    #[test]
    fn oi_change_is_causal() {
        let values = [(0, 100.0), (300_000, 101.0), (900_000, 102.0)];
        let change = oi_change(&values, 900_000, 900_000).unwrap();
        assert!((change - 0.02).abs() < 1e-9);
    }

    #[test]
    fn early_surge_thresholds_admit_the_first_4usdt_leg() {
        let config = LaneConfig::default();
        assert_eq!(
            ignition_pattern(true, 0.0092, 2.1, Some(0.20), 1.033, 0.029, 0.052, &config,),
            Some("compression_breakout")
        );
        assert!(0.0233 <= config.fast_max_oi_change_15m);
    }

    #[test]
    fn strong_reacceleration_does_not_require_compression() {
        let config = LaneConfig::default();
        assert_eq!(
            ignition_pattern(true, 0.010, 5.0, Some(0.30), 2.2, 0.015, 0.04, &config,),
            Some("trend_reacceleration")
        );
    }

    #[test]
    fn late_reacceleration_requires_a_reclaim_instead_of_a_direct_cross() {
        let config = LaneConfig::default();
        assert!(!direct_confirmation_allowed(
            Some("trend_reacceleration"),
            0.515,
            0.0008,
            0.01,
            0.0267,
            &config,
        ));
        assert!(direct_confirmation_allowed(
            Some("trend_reacceleration"),
            0.771,
            0.0262,
            0.01,
            0.0165,
            &config,
        ));
        assert!(direct_confirmation_allowed(
            Some("trend_reacceleration"),
            0.556,
            0.0054,
            0.01,
            0.0121,
            &config,
        ));
        assert!(direct_confirmation_allowed(
            Some("compression_breakout"),
            0.40,
            -0.01,
            0.0,
            0.04,
            &config,
        ));
        assert!(!direct_confirmation_allowed(
            Some("compression_breakout"),
            0.476,
            -0.0004,
            -0.0072,
            0.0034,
            &config,
        ));
    }

    #[test]
    fn directional_inputs_make_short_ignition_symmetric() {
        let config = LaneConfig::default();
        assert_eq!(
            ignition_pattern(true, 0.0092, 2.1, Some(0.20), 1.033, 0.029, 0.052, &config,),
            Some("compression_breakout")
        );
    }

    #[test]
    fn opposing_terminal_minute_flow_cannot_confirm_a_long_reclaim() {
        let minute = Candle {
            open_ms: 0,
            close_ms: 59_999,
            open: 2.369,
            high: 2.384,
            low: 2.369,
            close: 2.380,
            quote_volume: 100.0,
            // Final NEAR-like candle flow is -20%, despite its green body.
            taker_buy_quote: Some(40.0),
            closed: true,
        };
        assert_eq!(confirmation_pattern(&minute, Side::Buy, 2.370, true), None);
        let (pattern, flow) = confirmation_pattern(&minute, Side::Sell, 2.390, true).unwrap();
        assert_eq!(pattern, "direct_continuation");
        assert!((flow + 0.2).abs() < 1e-9);
    }

    #[test]
    fn late_exhaustion_veto_matches_rays_without_becoming_a_global_gate() {
        let config = LaneConfig::default();
        assert!(is_late_exhaustion(
            "shallow_reclaim",
            0.0166,
            0.0402,
            0.361,
            -0.00113,
            Some(0.202),
            &config,
        ));

        // An early idiosyncratic leader is still admitted even without broad
        // participation, preserving the fast lane's intended frequency.
        assert!(!is_late_exhaustion(
            "shallow_reclaim",
            0.010,
            0.020,
            0.35,
            -0.002,
            Some(0.20),
            &config,
        ));
        assert!(!is_late_exhaustion(
            "shallow_reclaim",
            0.020,
            0.040,
            0.35,
            -0.002,
            Some(0.35),
            &config,
        ));
        assert!(!is_late_exhaustion(
            "direct_continuation",
            0.020,
            0.040,
            0.35,
            -0.002,
            Some(0.20),
            &config,
        ));
    }
}
