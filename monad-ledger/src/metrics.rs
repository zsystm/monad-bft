use monad_metrics::{metrics_bft, MetricsPolicy};

metrics_bft! {
    Ledger {
        namespace: ledger,
        counters: [
            num_commits,
            num_tx_commits,
        ],
        gauges: [
            block_num,
        ]
    }
}
