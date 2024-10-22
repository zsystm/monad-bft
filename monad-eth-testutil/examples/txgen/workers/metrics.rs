use super::*;

#[derive(Default)]
pub struct Metrics {
    pub accts_with_nonzero_bal: AtomicUsize,
    pub total_txs_sent: AtomicUsize,
    pub total_rpc_calls: AtomicUsize,
}

impl Metrics {
    pub async fn run(self: Arc<Metrics>) {
        let mut report_interval = tokio::time::interval(Duration::from_secs(2));
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut prev_non_zero = self.accts_with_nonzero_bal.load(SeqCst);
        let mut prev_txs_sent = self.total_txs_sent.load(SeqCst);
        let mut prev_rpc_calls = self.total_rpc_calls.load(SeqCst);
        let mut last = Instant::now();

        loop {
            let now = report_interval.tick().await;

            let total_nonzero_accts = self.accts_with_nonzero_bal.load(SeqCst);
            let total_txs_sent = self.total_txs_sent.load(SeqCst);
            let total_rpc_call = self.total_rpc_calls.load(SeqCst);

            let elapsed = last.elapsed().as_secs_f64();

            let accts_created_per_sec =
                ((total_nonzero_accts - prev_non_zero) as f64 / elapsed).round() as u32;
            let tps = ((total_txs_sent - prev_txs_sent) as f64 / elapsed).round() as u32;
            let rps = ((total_rpc_call - prev_rpc_calls) as f64 / elapsed).round() as u32;

            info!(
                total_nonzero_accts,
                total_rpc_call, total_txs_sent, rps, accts_created_per_sec, tps, "Metrics"
            );

            prev_non_zero = total_nonzero_accts;
            prev_rpc_calls = total_rpc_call;
            prev_txs_sent = total_txs_sent;
            last = now;
        }
    }
}
