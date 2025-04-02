use monad_metrics::{metrics_bft, MetricsPolicy};

metrics_bft! {
    StateSync {
        namespace: statesync,
        gauges: [
            syncing,
            progress_estimate,
            last_target,
        ]
    }
}
