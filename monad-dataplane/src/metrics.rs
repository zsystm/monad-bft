use monad_metrics::{metrics_bft, MetricsPolicy};

metrics_bft! {
    Dataplane {
        namespace: dataplane,
        counters: [
            dropped_msgs_full_tcp,
            dropped_msgs_full_udp,
        ],
        components: [
            Tcp {
                namespace: tcp,
                components: [
                    Rx {
                        namespace: rx,
                        counters: [
                            new_connection,
                            bytes,
                        ],
                        gauges: [
                            connections
                        ]
                    },
                    Tx {
                        namespace: tx,
                        counters: [
                            new_connection,
                            bytes_total,
                            bytes_success,
                        ],
                        gauges: [
                            connections
                        ]
                    }
                ]
            },
            Udp {
                namespace: udp,
                components: [
                    Rx {
                        namespace: rx,
                        counters: [
                            bytes,
                        ]
                    },
                    Tx {
                        namespace: tx,
                        counters: [
                            bytes_total,
                            bytes_success,
                        ]
                    }
                ]
            }
        ]
    }
}
