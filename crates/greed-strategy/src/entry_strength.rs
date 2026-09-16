use greed_kernel::{EntryStrength, InstrumentFrame, TradeCandidate};

#[derive(Debug, Clone, Copy)]
pub struct EntryAssessment {
    pub strength: EntryStrength,
    pub score: f64,
    pub lsr: Option<f64>,
    pub vsr: Option<f64>,
    pub evidence_count: usize,
}

impl EntryAssessment {
    /// Account-risk budget.  Even the strongest setup risks only 30 bps of
    /// equity, so one stopped trade cannot erase several normal $10-$20 wins.
    pub fn risk_pct(self) -> f64 {
        match self.strength {
            EntryStrength::Probe => 0.00075,
            EntryStrength::Confirmed => 0.0015,
            EntryStrength::Conviction => 0.003,
        }
    }

    pub fn fast_target_account_pct(self) -> f64 {
        match self.strength {
            EntryStrength::Probe => 0.001,
            EntryStrength::Confirmed => 0.002,
            EntryStrength::Conviction => 0.004,
        }
    }
}

fn tag(candidate: &TradeCandidate, key: &str) -> Option<f64> {
    candidate.tags.get(key)?.parse().ok()
}

fn push(checks: &mut Vec<bool>, value: Option<f64>, predicate: impl FnOnce(f64) -> bool) {
    if let Some(value) = value.filter(|value| value.is_finite()) {
        checks.push(predicate(value));
    }
}

/// Volatility-state ratio: RMS return of the last three completed 5m bars
/// divided by the preceding twelve-bar RMS.  It is deliberately dimensionless
/// and causal. `None` means there is not enough usable history.
pub fn volatility_state_ratio(instrument: &InstrumentFrame, now_ms: i64) -> Option<f64> {
    let series = instrument.fast_perpetual.as_ref()?;
    if !series.meta.usable_at(now_ms) {
        return None;
    }
    let closes: Vec<_> = series
        .values
        .iter()
        .filter(|bar| bar.closed && bar.close > 0.0)
        .map(|bar| bar.close)
        .collect();
    if closes.len() < 16 {
        return None;
    }
    let returns: Vec<_> = closes
        .windows(2)
        .map(|pair| (pair[1] / pair[0]).ln())
        .collect();
    let split = returns.len() - 3;
    let baseline_start = split.saturating_sub(12);
    let rms = |values: &[f64]| {
        (values.iter().map(|value| value * value).sum::<f64>() / values.len() as f64).sqrt()
    };
    let baseline = rms(&returns[baseline_start..split]);
    (baseline > 1e-8).then(|| rms(&returns[split..]) / baseline)
}

pub fn assess(
    candidate: &TradeCandidate,
    instrument: &InstrumentFrame,
    now_ms: i64,
) -> EntryAssessment {
    let sign = candidate.side.sign();
    let lsr = instrument
        .long_short_ratio
        .as_ref()
        .filter(|series| series.meta.usable_at(now_ms))
        .and_then(|series| series.values.last())
        .map(|point| point.ratio)
        .filter(|value| value.is_finite() && *value > 0.0);
    let vsr = volatility_state_ratio(instrument, now_ms);
    let mut checks = Vec::new();

    match candidate.recipe.as_str() {
        "fast_trend_activation" => {
            push(&mut checks, tag(candidate, "body_return_5m"), |v| {
                sign * v >= 0.010
            });
            push(&mut checks, tag(candidate, "volume_ratio_5m"), |v| v >= 3.0);
            push(&mut checks, tag(candidate, "directional_flow_5m"), |v| {
                sign * v >= 0.30
            });
            push(&mut checks, tag(candidate, "market_breadth_1h"), |v| {
                v >= 0.60
            });
            push(&mut checks, tag(candidate, "market_return_1h"), |v| {
                sign * v >= 0.0
            });
            push(&mut checks, tag(candidate, "prebreak_return_1h"), |v| {
                sign * v <= 0.009
            });
            push(&mut checks, tag(candidate, "compression_ratio"), |v| {
                v <= 0.93
            });
            push(&mut checks, tag(candidate, "directional_extension"), |v| {
                v <= 0.0185
            });
            push(&mut checks, tag(candidate, "confirmation_flow_1m"), |v| {
                sign * v >= 0.25
            });
            push(&mut checks, vsr, |v| (0.75..=2.50).contains(&v));
        }
        "liquidation_exhaustion_reversal" => {
            push(
                &mut checks,
                tag(candidate, "liquidation_depth_ratio_3s"),
                |v| v >= 1.6,
            );
            push(
                &mut checks,
                tag(candidate, "liquidation_aligned_return_bps_3s"),
                |v| (14.0..=27.5).contains(&v),
            );
            push(
                &mut checks,
                tag(candidate, "liquidation_reversal_bps"),
                |v| v <= 49.0,
            );
            push(&mut checks, tag(candidate, "signal_age_ms"), |v| {
                v <= 5_000.0
            });
            push(
                &mut checks,
                tag(candidate, "live_1m_opposing_body_pct"),
                |v| v <= 0.012,
            );
            push(&mut checks, tag(candidate, "live_1m_opposing_flow"), |v| {
                v <= 0.117
            });
            push(&mut checks, vsr, |v| v <= 3.5);
        }
        "trend_continuation" | "trend_continuation_reentry" => {
            push(&mut checks, tag(candidate, "trend_efficiency"), |v| {
                v >= 0.50
            });
            push(&mut checks, tag(candidate, "trend_extension_atr"), |v| {
                v <= 2.5
            });
            push(&mut checks, tag(candidate, "trend_reclaim_body_atr"), |v| {
                v >= 0.8
            });
            push(
                &mut checks,
                tag(candidate, "market_directional_breadth"),
                |v| v >= 0.70,
            );
            push(&mut checks, tag(candidate, "trend_age_bars"), |v| v <= 1.0);
            push(&mut checks, vsr, |v| (0.65..=3.0).contains(&v));
        }
        _ => {}
    }

    // Same-side account crowding is a risk downgrade, not a directional veto.
    // Missing/stale LSR is omitted instead of silently becoming zero.
    if let Some(ratio) = lsr {
        let aligned_crowding = sign * ratio.ln();
        checks.push(aligned_crowding <= 1.8_f64.ln());
    }
    let score = if checks.is_empty() {
        candidate.confidence.clamp(0.0, 1.0)
    } else {
        checks.iter().filter(|value| **value).count() as f64 / checks.len() as f64
    };
    let (confirmed, conviction) = match candidate.recipe.as_str() {
        "liquidation_exhaustion_reversal" => (0.50, 0.70),
        "trend_continuation" | "trend_continuation_reentry" => (0.60, 0.80),
        _ => (0.68, 0.80),
    };
    let strength = if score >= conviction {
        EntryStrength::Conviction
    } else if score >= confirmed {
        EntryStrength::Confirmed
    } else {
        EntryStrength::Probe
    };
    EntryAssessment {
        strength,
        score,
        lsr,
        vsr,
        evidence_count: checks.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_is_bounded_and_monotonic() {
        let probe = EntryAssessment {
            strength: EntryStrength::Probe,
            score: 0.4,
            lsr: None,
            vsr: None,
            evidence_count: 1,
        };
        let confirmed = EntryAssessment {
            strength: EntryStrength::Confirmed,
            score: 0.7,
            lsr: None,
            vsr: None,
            evidence_count: 1,
        };
        let conviction = EntryAssessment {
            strength: EntryStrength::Conviction,
            score: 0.9,
            lsr: None,
            vsr: None,
            evidence_count: 1,
        };
        assert!(probe.risk_pct() < confirmed.risk_pct());
        assert!(confirmed.risk_pct() < conviction.risk_pct());
        assert_eq!(conviction.risk_pct(), 0.003);
    }
}
