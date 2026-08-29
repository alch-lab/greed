use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

/// Finds a 15m trend birth and enters only its first completed 5m pullback.
/// The previous single-bar ignition model was removed after failing the recent
/// walk-forward window.
pub struct IgnitionSprintNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl IgnitionSprintNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.ignition_sprint".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn ema(values: &[&Candle], end: usize, period: usize) -> f64 {
    let alpha = 2.0 / (period as f64 + 1.0);
    values[..=end]
        .iter()
        .fold(None, |state, bar| {
            Some(state.map_or(bar.close, |previous| {
                alpha * bar.close + (1.0 - alpha) * previous
            }))
        })
        .unwrap_or_default()
}

fn atr(values: &[&Candle], end: usize) -> f64 {
    let start = end.saturating_sub(13).max(1);
    let rows: Vec<_> = (start..=end)
        .map(|index| {
            let bar = values[index];
            let previous = values[index - 1].close;
            (bar.high - bar.low)
                .max((bar.high - previous).abs())
                .max((bar.low - previous).abs())
        })
        .collect();
    rows.iter().sum::<f64>() / rows.len().max(1) as f64
}

fn flow(values: &[&Candle]) -> Option<f64> {
    let quote = values.iter().map(|bar| bar.quote_volume).sum::<f64>();
    let buy = values
        .iter()
        .map(|bar| bar.taker_buy_quote)
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .sum::<f64>();
    (quote > 0.0).then_some(2.0 * buy / quote - 1.0)
}

fn closed_fast<'a>(ctx: &'a NodeContext<'_>, symbol: &str) -> Vec<&'a Candle> {
    ctx.frame
        .instrument(symbol)
        .and_then(|instrument| instrument.fast_perpetual.as_ref())
        .map(|series| series.values.iter().filter(|bar| bar.closed).collect())
        .unwrap_or_default()
}

fn cross_section_return(ctx: &NodeContext<'_>, close_ms: i64) -> f64 {
    median(
        ctx.frame
            .instruments
            .values()
            .filter_map(|instrument| {
                let values: Vec<_> = instrument
                    .fast_perpetual
                    .as_ref()?
                    .values
                    .iter()
                    .filter(|bar| bar.closed)
                    .collect();
                let index = values.iter().position(|bar| bar.close_ms == close_ms)?;
                (index >= 2).then(|| values[index].close / values[index - 2].open - 1.0)
            })
            .collect(),
    )
}

fn btc_return_1h(ctx: &NodeContext<'_>, close_ms: i64) -> Option<f64> {
    let values = closed_fast(ctx, "BTCUSDT");
    let index = values.iter().position(|bar| bar.close_ms == close_ms)?;
    (index >= 12).then(|| values[index].close / values[index - 12].close - 1.0)
}

fn birth_metrics(
    ctx: &NodeContext<'_>,
    config: &LaneConfig,
    values: &[&Candle],
    index: usize,
) -> (Vec<String>, f64, f64, f64) {
    let bar = values[index];
    let return_15m = bar.close / values[index - 2].open - 1.0;
    let relative = return_15m - cross_section_return(ctx, bar.close_ms);
    let return_60m = bar.close / values[index - 11].open - 1.0;
    let pre_birth = values[index - 3].close / values[index - 11].open - 1.0;
    let quote = values[index - 2..=index]
        .iter()
        .map(|bar| bar.quote_volume)
        .sum::<f64>();
    let baseline = median(
        (index - 35..index - 2)
            .map(|row| {
                values[row - 2..=row]
                    .iter()
                    .map(|bar| bar.quote_volume)
                    .sum::<f64>()
            })
            .collect(),
    );
    let volume_ratio = quote / baseline.max(1.0);
    let birth_flow = flow(&values[index - 2..=index]);
    let prior_high = values[index - 14..index - 2]
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let birth_atr = atr(values, index);
    let path = (values[index - 1].close - values[index - 2].close).abs()
        + (values[index].close - values[index - 1].close).abs();
    let efficiency = (bar.close - values[index - 2].open).abs() / path.max(birth_atr);
    let single_bar = values[index - 2..=index]
        .iter()
        .map(|bar| (bar.close / bar.open - 1.0).abs())
        .fold(0.0, f64::max);
    let ema21 = ema(values, index, 21);
    let ema36 = ema(values, index, 36);
    let mut blockers = Vec::new();
    if return_15m < config.ignition_min_return_15m {
        blockers.push(format!(
            "15m rise {:.2}% / need {:.2}%",
            return_15m * 100.0,
            config.ignition_min_return_15m * 100.0
        ));
    }
    if relative < config.ignition_min_relative_15m {
        blockers.push(format!(
            "15m relative strength {:.2}% / need {:.2}%",
            relative * 100.0,
            config.ignition_min_relative_15m * 100.0
        ));
    }
    if return_60m > config.ignition_max_extension_60m {
        blockers.push("one-hour move is already too extended".into());
    }
    if pre_birth > config.ignition_max_pre_birth_return {
        blockers.push("the move was already mature before ignition".into());
    }
    if volume_ratio < config.ignition_min_volume_ratio {
        blockers.push(format!(
            "15m volume {volume_ratio:.2}x / need {:.2}x",
            config.ignition_min_volume_ratio
        ));
    }
    if birth_flow.is_none_or(|value| value < config.ignition_min_flow_imbalance) {
        blockers.push("15m taker flow does not confirm buying".into());
    }
    if bar.close <= prior_high {
        blockers.push("waiting for the first one-hour range break".into());
    }
    if !(bar.close > ema21 && ema21 > ema36) || efficiency < 0.55 {
        blockers.push("15m path is not an efficient new uptrend".into());
    }
    if single_bar > 0.025 {
        blockers.push("a single 5m bar is already too extended".into());
    }
    if btc_return_1h(ctx, bar.close_ms).is_none_or(|value| value < -0.002) {
        blockers.push("BTC one-hour direction conflicts with an ignition long".into());
    }
    if baseline * 4.0 < 500_000.0 {
        blockers.push("recent quote liquidity is below the research floor".into());
    }
    (blockers, birth_atr, relative, volume_ratio)
}

impl StrategyNode for IgnitionSprintNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut candidates = Vec::new();
        let mut inspected = 0u64;
        let mut births = 0u64;
        let mut reclaims = 0u64;
        let wait_bars = usize::try_from(self.config.ignition_max_wait_seconds / 300)
            .unwrap_or(6)
            .max(1);
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let values = closed_fast(ctx, symbol);
            if values.len() < 80 {
                continue;
            }
            inspected += 1;
            let latest = values.len() - 1;
            let first = latest.saturating_sub(wait_bars).max(35);
            let mut selected = None;
            let mut nearest = None;
            for index in (first..=latest).rev() {
                let metrics = birth_metrics(ctx, &self.config, &values, index);
                if nearest.is_none() {
                    nearest = Some((index, metrics.0.clone(), metrics.1, metrics.2, metrics.3));
                }
                if metrics.0.is_empty() {
                    selected = Some((index, metrics.1, metrics.2, metrics.3));
                    break;
                }
            }
            let (birth_index, birth_atr, relative, volume_ratio, mut blockers) =
                if let Some((index, atr, relative, volume)) = selected {
                    births += 1;
                    (index, atr, relative, volume, Vec::new())
                } else {
                    let (index, reasons, atr, relative, volume) = nearest.expect("latest birth");
                    (index, atr, relative, volume, reasons)
                };
            let birth = values[birth_index];
            let mut extreme = birth.high;
            let mut pullback_seen = false;
            let mut reclaim = None;
            for index in birth_index + 1..=latest {
                let bar = values[index];
                extreme = extreme.max(bar.high);
                let adverse = (extreme - bar.low) / birth_atr.max(f64::EPSILON);
                if adverse > self.config.ignition_max_pullback_atr {
                    blockers.push("first pullback exceeded the invalidation distance".into());
                    break;
                }
                pullback_seen |= adverse >= self.config.ignition_min_pullback_atr;
                let location = (bar.close - bar.low) / (bar.high - bar.low).max(f64::EPSILON);
                let extension = (bar.close - birth.close) / birth_atr.max(f64::EPSILON);
                if pullback_seen
                    && bar.close > bar.open
                    && location >= 0.65
                    && bar.close > values[index - 1].close
                    && flow(&[bar])
                        .is_some_and(|value| value >= self.config.ignition_reclaim_flow_imbalance)
                    && (-0.35..=1.25).contains(&extension)
                {
                    reclaim = Some(bar);
                    break;
                }
            }
            if blockers.is_empty() && !pullback_seen {
                blockers.push("waiting for the first shallow 5m pullback".into());
            } else if blockers.is_empty() && reclaim.is_none() {
                blockers.push("waiting for a completed 5m reclaim".into());
            }
            if reclaim.is_some_and(|bar| ctx.frame.as_of_ms - bar.close_ms > 300_000) {
                blockers.push("the first reclaim has expired".into());
            }
            let mut entry_limit = instrument.price;
            match instrument.book.as_ref() {
                None => blockers.push("order book is not ready".into()),
                Some(book) if !book.meta.usable_at(ctx.frame.as_of_ms) => {
                    blockers.push("order book is stale".into())
                }
                Some(book) => {
                    let spread_bps = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    let depth = book.bid_depth_usd.min(book.ask_depth_usd);
                    if spread_bps > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread_bps:.1} bps is too wide"));
                    }
                    if depth < self.config.min_depth_usd {
                        blockers.push(format!("book depth ${depth:.0} is too thin"));
                    }
                    entry_limit = book
                        .bid
                        .min(instrument.price - self.config.ignition_limit_offset_atr * birth_atr);
                }
            }
            let ready = blockers.is_empty() && reclaim.is_some();
            reclaims += u64::from(ready);
            let progress = 10usize.saturating_sub(blockers.len()).min(10) as f64 / 10.0;
            let signal_ms = reclaim.map_or(birth.close_ms, |bar| bar.close_ms);
            let stop_pct =
                self.config.ignition_stop_atr_multiple * birth_atr / entry_limit.max(f64::EPSILON);
            candidates.push((
                ready,
                progress,
                TradeCandidate {
                    id: format!("ignition_sprint:{symbol}:{signal_ms}"),
                    recipe: "ignition_sprint".into(),
                    symbol: symbol.clone(),
                    side: Side::Buy,
                    signal_ms,
                    expires_ms: signal_ms + 300_000,
                    reference_price: entry_limit,
                    score: relative * volume_ratio,
                    confidence: if ready { 0.78 } else { progress },
                    verdict: if ready { Verdict::Pass } else { Verdict::Block },
                    blockers,
                    evidence: vec![
                        format!("{symbol}.binance_ws_5m_trend_birth"),
                        "BTCUSDT.binance_ws_5m_anchor".into(),
                        format!("{symbol}.binance_ws_5m_first_pullback"),
                        format!("{symbol}.book"),
                    ],
                    tags: BTreeMap::from([
                        ("lane".into(), "ignition_sprint".into()),
                        ("priority".into(), "2".into()),
                        ("entry_limit".into(), entry_limit.to_string()),
                        (
                            "entry_timeout_ms".into(),
                            (i64::from(self.config.ignition_entry_timeout_seconds) * 1_000)
                                .to_string(),
                        ),
                        (
                            "risk_per_trade_pct".into(),
                            self.config.ignition_risk_per_trade_pct.to_string(),
                        ),
                        ("stop_pct".into(), stop_pct.to_string()),
                        ("target_r".into(), self.config.ignition_target_r.to_string()),
                        ("take_profit_fraction".into(), "1.0".into()),
                        (
                            "max_hold_ms".into(),
                            (i64::from(self.config.ignition_max_hold_minutes) * 60_000).to_string(),
                        ),
                    ]),
                },
            ));
        }
        candidates.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.total_cmp(&a.1))
                .then_with(|| b.2.score.total_cmp(&a.2.score))
        });
        let passed = candidates.iter().filter(|row| row.0).count();
        candidates.truncate(8);
        let nearest = candidates
            .first()
            .map(|row| format!("{}: {}", row.2.symbol, row.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for complete 5m and order-book data".into());
        let mut out = vec![ArtifactRecord {
            key: "lane.ignition_sprint.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if passed > 0 {
                    "first_pullback_entry_ready"
                } else if births > 0 {
                    "waiting_for_first_pullback"
                } else {
                    "scanning_for_early_trend"
                }
                .into(),
                score: candidates.first().map_or(0.0, |row| row.2.score),
                side: Some(Side::Buy),
                verdict: if passed > 0 {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if passed > 0 { vec![] } else { vec![nearest] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("trend_birth_hits".into(), births as f64),
                    ("first_pullback_reclaims".into(), reclaims as f64),
                    ("pass_candidates".into(), passed as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    30_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_ws_5m_depth".into()],
                ),
            }),
        }];
        out.extend(
            candidates
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
    #[test]
    fn ema_tracks_flat_series() {
        let candles: Vec<_> = (0..40)
            .map(|index| Candle {
                open_ms: index * 300_000,
                close_ms: (index + 1) * 300_000,
                open: 10.0,
                high: 10.0,
                low: 10.0,
                close: 10.0,
                quote_volume: 1_000.0,
                taker_buy_quote: Some(500.0),
                closed: true,
            })
            .collect();
        let refs: Vec<_> = candles.iter().collect();
        assert!((ema(&refs, 39, 21) - 10.0).abs() < 1e-12);
    }
}
