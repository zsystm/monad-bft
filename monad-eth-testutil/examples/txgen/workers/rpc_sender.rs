use tokio::time::MissedTickBehavior;

use super::*;

pub struct RpcSender {
    pub gen_rx: mpsc::Receiver<AccountsWithTxs>,
    pub refresh_sender: mpsc::Sender<AccountsWithTime>,
    pub recipient_sender: mpsc::UnboundedSender<AddrsWithTime>,

    pub client: ReqwestClient,
    pub target_tps: u64,
    pub metrics: Arc<Metrics>,
}

impl RpcSender {
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            BATCH_SIZE as u64 * 1000 / self.target_tps,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!(
            interval_ms = interval.period().as_millis(),
            "Starting rpc sender loop"
        );
        while let Some(AccountsWithTxs {
            accts,
            txs,
            to_addrs: to_accts,
        }) = self.gen_rx.recv().await
        {
            info!(
                num_accts = accts.len(),
                num_txs = txs.len(),
                channel_len = self.gen_rx.len(),
                "Received accounts with txs from gen"
            );

            for (batch, to_accts) in txs.into_iter().zip(to_accts.into_iter()) {
                self.spawn_send_batch(batch, to_accts);

                // limit sending batch by interval
                interval.tick().await;
            }

            debug!("Sending accts to refresher...");
            self.refresh_sender
                .send(AccountsWithTime {
                    accts,
                    sent: Instant::now(),
                })
                .await
                .expect("Sender not closed");
            debug!("Accts sent to refresher...");
        }
    }

    fn spawn_send_batch(&self, batch: Vec<TransactionSigned>, to_addrs: Vec<Address>) {
        if batch.is_empty() {
            return; // unnecessary?
        }
        debug!(batch_size = batch.len(), "Sending batch of txs...");

        let recipient_sender = self.recipient_sender.clone();
        let client = self.client.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            send_batch(&client, batch, &metrics).await;

            debug!("Tx batch sent");

            trace!("Sending accts to recipient tracker...");
            recipient_sender
                .send(AddrsWithTime {
                    addrs: to_addrs,
                    sent: Instant::now(),
                })
                .expect("recipient tracker rx closed");
            trace!("Sent accts to recipient tracker");
        });
    }
}

pub async fn send_batch(client: &ReqwestClient, txs: Vec<TransactionSigned>, metrics: &Metrics) {
    let now = Instant::now();

    let mut batch_req = client.new_batch();
    let num_txs = txs.len();

    let futs = txs
        .into_iter()
        .filter_map(|tx| {
            batch_req
                .add_call::<_, TxHash>("eth_sendRawTransaction", &[tx.envelope_encoded()])
                .ok() // todo: handle better
        })
        .collect::<FuturesUnordered<_>>();

    if let Err(e) = batch_req.send().await {
        error!("Failed to send batch: {e}");
        return;
    }

    metrics.total_txs_sent.fetch_add(num_txs, SeqCst);

    let num_resp = futs.count().await;
    if num_txs != num_resp {
        debug!(
            num_txs,
            num_resp, "Expected all txs from batch to make it or all to not"
        );
    }
    trace!(elapsed_ms = now.elapsed().as_millis(), "send_batch latency");
}
