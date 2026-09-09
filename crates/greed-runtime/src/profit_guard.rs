//! Executable-value protection. Never substitutes public-market depth for the
//! execution venue. The exchange stop remains the outage/gap backstop.
use anyhow::{anyhow, Result};
use greed_kernel::Side;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const COST_RESERVE: f64 = 0.001; // 4 bps entry + 4 exit + 2 stress; conservative estimate.
pub const MAX_QUOTE_AGE_MS: i64 = 1_500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfitGuard {
    pub quantity: f64,
    pub peak_net_return: f64,
    pub current_net_return: f64,
    pub floor_net_return: Option<f64>,
    pub observed_ms: i64,
    pub exchange_ms: i64,
    pub exit_vwap: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct ExitQuote {
    pub vwap: f64,
    pub exchange_ms: i64,
    pub observed_ms: i64,
}

pub fn quote(
    value: &Value,
    side: Side,
    quantity: f64,
    now_ms: i64,
    offset: i64,
) -> Result<ExitQuote> {
    let exchange_ms = value["E"]
        .as_i64()
        .ok_or_else(|| anyhow!("depth timestamp missing"))?;
    let age = now_ms + offset - exchange_ms;
    if !(-250..=MAX_QUOTE_AGE_MS).contains(&age) || !quantity.is_finite() || quantity <= 0.0 {
        return Err(anyhow!("depth stale or invalid quantity; age_ms={age}"));
    }
    let parse = |key: &str| -> Result<Vec<(f64, f64)>> {
        value[key]
            .as_array()
            .ok_or_else(|| anyhow!("missing {key}"))?
            .iter()
            .map(|row| {
                let p = row[0].as_str().and_then(|v| v.parse::<f64>().ok());
                let q = row[1].as_str().and_then(|v| v.parse::<f64>().ok());
                match (p, q) {
                    (Some(p), Some(q)) if p.is_finite() && p > 0.0 && q.is_finite() && q > 0.0 => {
                        Ok((p, q))
                    }
                    _ => Err(anyhow!("invalid depth level")),
                }
            })
            .collect()
    };
    let bids = parse("bids")?;
    let asks = parse("asks")?;
    if bids.is_empty()
        || asks.is_empty()
        || bids[0].0 >= asks[0].0
        || bids.windows(2).any(|w| w[0].0 <= w[1].0)
        || asks.windows(2).any(|w| w[0].0 >= w[1].0)
    {
        return Err(anyhow!("crossed or unordered depth"));
    }
    let levels = if side == Side::Buy { &bids } else { &asks };
    let mut remaining = quantity;
    let mut cost = 0.0;
    for &(price, size) in levels {
        let take = remaining.min(size);
        cost += take * price;
        remaining -= take;
        if remaining <= quantity * 1e-10 {
            break;
        }
    }
    if remaining > quantity * 1e-10 {
        return Err(anyhow!(
            "insufficient execution depth for remaining quantity"
        ));
    }
    Ok(ExitQuote {
        vwap: cost / quantity,
        exchange_ms,
        observed_ms: now_ms,
    })
}

#[derive(Clone, Copy)]
pub struct Protection {
    pub activation: Option<f64>,
    pub floor: f64,
    pub trailing_activation: Option<f64>,
    pub trailing_distance: Option<f64>,
    pub partial_activated: bool,
}

/// Thresholds remain the persisted gross-price settings, converted to the same
/// estimated-net basis. Reset on quantity changes: old full-size depth peaks
/// must not be inherited by a resized remainder. No historical peak invented.
pub fn observe(
    state: &mut Option<ProfitGuard>,
    quote: ExitQuote,
    side: Side,
    entry: f64,
    quantity: f64,
    protection: Protection,
) -> bool {
    if !entry.is_finite()
        || entry <= 0.0
        || !quantity.is_finite()
        || quantity <= 0.0
        || !quote.vwap.is_finite()
        || quote.vwap <= 0.0
    {
        return false;
    }
    let net = side.sign() * (quote.vwap / entry - 1.0) - COST_RESERVE;
    let reset = state
        .as_ref()
        .is_none_or(|s| (s.quantity - quantity).abs() > quantity * 1e-8);
    if reset {
        *state = Some(ProfitGuard {
            quantity,
            peak_net_return: net,
            current_net_return: net,
            floor_net_return: None,
            observed_ms: quote.observed_ms,
            exchange_ms: quote.exchange_ms,
            exit_vwap: quote.vwap,
        });
    }
    let s = state.as_mut().expect("initialized above");
    s.current_net_return = net;
    s.peak_net_return = s.peak_net_return.max(net);
    s.observed_ms = quote.observed_ms;
    s.exchange_ms = quote.exchange_ms;
    s.exit_vwap = quote.vwap;
    let activated = protection.partial_activated
        || protection
            .activation
            .is_some_and(|a| s.peak_net_return >= (a - COST_RESERVE).max(0.0));
    if activated {
        // Arm only above the floor. Never liquidate a legacy position just
        // because an inherited TP already happened before its first quote.
        let mut floor = (protection.floor - COST_RESERVE).max(0.0);
        if let (Some(activation), Some(distance)) =
            (protection.trailing_activation, protection.trailing_distance)
        {
            if s.peak_net_return >= (activation - COST_RESERVE).max(0.0) {
                floor = floor.max(s.peak_net_return - distance);
            }
        }
        if s.floor_net_return.is_some() || net > floor {
            s.floor_net_return = Some(s.floor_net_return.map_or(floor, |old| old.max(floor)));
        }
    }
    s.floor_net_return.is_some_and(|floor| net <= floor)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn depth() -> Value {
        serde_json::json!({"E":1000,"bids":[["100","2"],["99","3"]],"asks":[["101","2"],["102","3"]]})
    }
    #[test]
    fn vwap_side_depth_and_freshness() {
        assert_eq!(quote(&depth(), Side::Buy, 4.0, 1000, 0).unwrap().vwap, 99.5);
        assert_eq!(
            quote(&depth(), Side::Sell, 4.0, 1000, 0).unwrap().vwap,
            101.5
        );
        assert!(quote(&depth(), Side::Buy, 6.0, 1000, 0).is_err());
        assert!(quote(&depth(), Side::Buy, 1.0, 3000, 0).is_err());
        let mut v = depth();
        v["asks"][0][0] = "99".into();
        assert!(quote(&v, Side::Buy, 1.0, 1000, 0).is_err());
    }
    fn p() -> Protection {
        Protection {
            activation: Some(0.0025),
            floor: 0.0015,
            trailing_activation: Some(0.005),
            trailing_distance: Some(0.003),
            partial_activated: false,
        }
    }
    fn q(vwap: f64) -> ExitQuote {
        ExitQuote {
            vwap,
            exchange_ms: 1000,
            observed_ms: 1000,
        }
    }
    #[test]
    fn restart_preserves_armed_floor_and_quantity_change_resets() {
        let mut s = None;
        assert!(!observe(&mut s, q(100.4), Side::Buy, 100.0, 4.0, p()));
        let mut restored = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert!(observe(&mut restored, q(100.1), Side::Buy, 100.0, 4.0, p()));
        assert!(!observe(
            &mut restored,
            q(100.1),
            Side::Buy,
            100.0,
            2.0,
            p()
        ));
    }
    #[test]
    fn no_fake_historical_peak_and_short_is_symmetric() {
        let mut s = None;
        assert!(!observe(&mut s, q(100.1), Side::Buy, 100.0, 1.0, p()));
        let mut s = None;
        assert!(!observe(&mut s, q(99.6), Side::Sell, 100.0, 1.0, p()));
        assert!(observe(&mut s, q(99.9), Side::Sell, 100.0, 1.0, p()));
    }

    #[test]
    fn trailing_floor_never_loosens_and_invalid_entry_is_ignored() {
        let mut s = None;
        assert!(!observe(&mut s, q(101.0), Side::Buy, 0.0, 1.0, p()));
        assert!(s.is_none());
        assert!(!observe(&mut s, q(101.0), Side::Buy, 100.0, 1.0, p()));
        let floor = s.as_ref().unwrap().floor_net_return.unwrap();
        assert!(!observe(&mut s, q(100.9), Side::Buy, 100.0, 1.0, p()));
        assert_eq!(s.as_ref().unwrap().floor_net_return, Some(floor));
        assert!(observe(&mut s, q(100.6), Side::Buy, 100.0, 1.0, p()));
    }

    #[test]
    fn missing_future_and_malformed_depth_never_scores() {
        let mut v = depth();
        v.as_object_mut().unwrap().remove("E");
        assert!(quote(&v, Side::Buy, 1.0, 1000, 0).is_err());
        assert!(quote(&depth(), Side::Buy, 1.0, 0, 0).is_err());
        let mut v = depth();
        v["bids"][0][1] = "NaN".into();
        assert!(quote(&v, Side::Buy, 1.0, 1000, 0).is_err());
    }
}
