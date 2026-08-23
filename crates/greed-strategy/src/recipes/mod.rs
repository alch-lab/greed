pub mod alt_cross_section;
pub mod alt_outlier_momentum;
pub mod alt_shock_reversal;
pub mod major_exhaustion;
pub mod major_trend_pullback;

use greed_kernel::{StateArtifact, Verdict};

pub(crate) fn aligned(
    states: &[&StateArtifact],
    side: greed_kernel::Side,
) -> (Verdict, Vec<String>) {
    let mut blockers = Vec::new();
    let mut unknown = false;
    for state in states {
        if state.verdict == Verdict::Unknown {
            unknown = true;
            blockers.extend(state.reasons.clone());
        } else if state.verdict != Verdict::Pass || state.side.is_some_and(|value| value != side) {
            blockers.push(format!("{} does not support {:?}", state.state, side));
        }
    }
    let verdict = if blockers.is_empty() {
        Verdict::Pass
    } else if unknown {
        Verdict::Unknown
    } else {
        Verdict::Block
    };
    (verdict, blockers)
}
