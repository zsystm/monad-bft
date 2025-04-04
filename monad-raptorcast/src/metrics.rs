use monad_dataplane::metrics::DataplaneMetrics;
use monad_metrics::{metrics_bft, MetricsPolicy};

metrics_bft! {
    RaptorCast {
        namespace: raptorcast,
        counters: [
            a
        ],
    }
}

pub struct RaptorCastDataplaneMetrics<MP>
where
    MP: MetricsPolicy,
{
    pub raptorcast: RaptorCastMetrics<MP>,
    pub dataplane: DataplaneMetrics<MP>,
}

impl<MP> Default for RaptorCastDataplaneMetrics<MP>
where
    MP: MetricsPolicy,
    MP::Counter: Default,
    MP::Gauge: Default,
{
    fn default() -> Self {
        Self {
            raptorcast: Default::default(),
            dataplane: Default::default(),
        }
    }
}
