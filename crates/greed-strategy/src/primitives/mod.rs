pub mod cross_section;
pub mod derivatives;
pub mod external;
pub mod flow;
pub mod liquidity;
pub mod market_profile;
pub mod price;
pub mod regime;

use greed_kernel::{ArtifactMeta, DataQuality};

pub(crate) fn meta(
    as_of_ms: i64,
    ttl_ms: i64,
    quality: DataQuality,
    confidence: f64,
    lineage: Vec<String>,
) -> ArtifactMeta {
    ArtifactMeta {
        as_of_ms,
        expires_ms: as_of_ms + ttl_ms,
        quality,
        confidence: confidence.clamp(0.0, 1.0),
        lineage,
    }
}

pub(crate) fn closed_bars(series: &greed_kernel::CandleSeries) -> Vec<&greed_kernel::Candle> {
    series.values.iter().filter(|bar| bar.closed).collect()
}

pub(crate) fn path_efficiency(bars: &[&greed_kernel::Candle]) -> f64 {
    let Some(first) = bars.first() else {
        return 0.0;
    };
    let Some(last) = bars.last() else { return 0.0 };
    let path: f64 = bars
        .windows(2)
        .map(|pair| (pair[1].close - pair[0].close).abs())
        .sum();
    if path <= f64::EPSILON {
        0.0
    } else {
        (last.close - first.close).abs() / path
    }
}
