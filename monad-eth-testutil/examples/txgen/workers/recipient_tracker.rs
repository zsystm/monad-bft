use super::*;

pub struct RecipientTracker {
    pub client: ReqwestClient,
    pub rpc_sender_rx: mpsc::UnboundedReceiver<AddrsWithTime>,
    pub erc20: ERC20,
    pub delay: Duration,

    pub non_zero: Arc<DashSet<Address>>,

    pub metrics: Arc<Metrics>,
}

impl RecipientTracker {
    pub async fn run(mut self) {
        let mut fetch_interval = tokio::time::interval(Duration::from_millis(10));
        fetch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        while let Some(AddrsWithTime { mut addrs, sent }) = self.rpc_sender_rx.recv().await {
            debug!(
                num_accts = addrs.len(),
                channel_len = self.rpc_sender_rx.len(),
                "Recipient tracker received accts from rpc sender"
            );

            // remove addresses we've already seen
            addrs.retain(|a| !self.non_zero.contains(a));
            if addrs.is_empty() {
                continue;
            }

            if sent + self.delay >= Instant::now() {
                tokio::time::sleep_until(sent + self.delay).await;
                debug!(
                    num_recipients = addrs.len(),
                    "Recipient tracker waited delay, refreshing batch..."
                );
            }

            // todo: should we group these up more?
            // for batch in addrs.chunks_mut(BATCH_SIZE) {
            if addrs.len() > BATCH_SIZE {
                error!(
                    addrs_len = addrs.len(),
                    BATCH_SIZE, "Addrs len should be less than batch size"
                );
            }

            self.handle_batch(addrs, self.non_zero.clone());

            fetch_interval.tick().await;
            // }
        }
    }

    fn handle_batch(&self, addrs: Vec<Address>, seen_non_zero: Arc<DashSet<Address>>) {
        let client = self.client.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let addr_strings = addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>();
            let now = Instant::now();
            trace!("before recipient refresh");
                
            // let erc20_bals = match client.batch_get_erc20_balance(&addrs, ).await {
            //     Ok(bals) => bals,
            //     Err(e) => {
            //         warn!("Recipient tracker failed to refresh batch: {e}");
            //         return;
            //     }
            // };

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

            let new_non_zero_addr_idxs =
                bals.into_iter().enumerate().filter_map(|(i, b)| match b {
                    Ok(b) => {
                        if !b.is_zero() {
                            Some(i)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                });
            for idx in new_non_zero_addr_idxs {
                seen_non_zero.insert(addrs[idx]);
            }

            metrics
                .accts_with_nonzero_bal
                .store(seen_non_zero.len(), SeqCst);
        });
    }
}
