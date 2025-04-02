use monad_metrics::{metrics_bft, MetricsPolicy};

metrics_bft! {
    RaptorCast {
        namespace: raptorcast,
        counters: [
            a
        ],
    }
}
