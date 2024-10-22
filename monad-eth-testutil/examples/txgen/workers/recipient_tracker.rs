use super::*;

pub struct RecipientTracker {
    pub client: ReqwestClient,
    pub rpc_sender_rx: mpsc::Receiver<AccountsWithTime>,
    pub erc20: ERC20,
    pub delay: Duration,

    pub metrics: Arc<Metrics>,
}
impl RecipientTracker {
    pub async fn run(mut self) {
        let mut fetch_interval = tokio::time::interval(Duration::from_millis(10));
        fetch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        while let Some(AccountsWithTime { mut accts, sent }) = self.rpc_sender_rx.recv().await {
            debug!(
                num_accts = accts.len(),
                channel_len = self.rpc_sender_rx.len(),
                "Recipient tracker received accts from rpc sender"
            );

            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!(
                    num_recipients = accts.len(),
                    "Recipient tracker waited delay, refreshing batch..."
                );
            }

            for batch in accts.chunks_mut(BATCH_SIZE) {
                if batch.is_empty() {
                    break;
                }

                let addr_strings = batch.iter().map(|a| a.addr.to_string()).collect::<Vec<_>>();

                self.handle_batch(addr_strings);

                fetch_interval.tick().await;
            }
        }
    }

    fn handle_batch(&self, addr_strings: Vec<String>) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let now = Instant::now();
            trace!("before recipient refresh");

            let bals = match client.batch_get_balance(&addr_strings).await {
                Ok(bals) => bals,
                Err(e) => {
                    warn!("Recipient tracker failed to refresh batch: {e}");
                    return;
                }
            };

            trace!(
                elapsed_ms = now.elapsed().as_millis(),
                "after recipient refresh"
            );

            let non_zero = bals
                .into_iter()
                .filter_map(Result::ok)
                .filter(|b| !b.is_zero())
                .count();

            metrics.accts_with_nonzero_bal.fetch_add(non_zero, SeqCst);
        });
    }
}
