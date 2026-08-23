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
