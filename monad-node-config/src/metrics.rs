use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeMetricsConfig {
    pub otel_endpoint: String,
    pub record_metrics_interval_seconds: u64,
}
