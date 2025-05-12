use alloy_primitives::{Address, FixedBytes, Log};
use alloy_rpc_types::Topic;
use futures::{Stream, TryStreamExt};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    IndexModel,
};

use crate::{
    model_v2::{resolve_inline_or_ref, resolve_trace_inline_or_ref, vec2bin, ModelV2, TxDoc},
    prelude::*,
};

impl ModelV2 {
    /// Create indexes for eth_getLogs queries if they don't exist
    pub async fn ensure_logs_indexes(&self) -> Result<()> {
        let keys = doc! {
            "block_number": 1,
            "logs.address": 1,
            "logs.topic_0": 1,
            "logs.topic_1": 1,
            "logs.topic_2": 1,
            "logs.topic_3": 1,
        };

        self.txs
            .create_index(IndexModel::builder().keys(keys).build())
            .await
            .wrap_err("Failed to create logs indexes")?;

        Ok(())
    }

    /// Index logs for a block - populate the logs field in tx documents
    pub async fn index_block_logs(&self, block: &Block, receipts: &BlockReceipts) -> Result<()> {
        let txs_with_logs = TxLogsDocument::from_receipts(block, receipts);
        if txs_with_logs.is_empty() {
            return Ok(());
        }

        futures::stream::iter(txs_with_logs.iter())
            .map(|tx_logs| {
                let txs_collection = self.txs.clone();
                let tx_logs = tx_logs.clone();
                async move {
                    txs_collection
                        .update_one(
                            doc! { "_id": vec2bin(tx_logs.tx_hash.to_vec()) },
                            doc! {
                                "$set": {
                                    "logs": tx_logs.logs,
                                }
                            },
                        )
                        .await
                        .wrap_err("Failed to update logs")
                }
            })
            .buffer_unordered(self.concurrency)
            .try_collect::<Vec<_>>()
            .await
            .wrap_err("Failed to index logs")?;
        Ok(())
    }

    /// Query the logs index for a given range of blocks and filter criteria
    /// Note: query_logs returns a superset of the txs containing matching logs
    /// This means another pass of filtering must be done by client
    pub async fn query_logs<'a>(
        &'a self,
        from: u64,
        to: u64,
        addresses: impl IntoIterator<Item = &Address> + 'a + Send,
        topics: &'a [Topic],
    ) -> Result<impl Stream<Item = Result<TxIndexedData>> + 'a + Send> {
        let mut query = doc! {
            "block_number": { "$gte": from as i64, "$lte": to as i64 },
        };

        let address_prefixes = addresses
            .into_iter()
            .map(|a| fb_to_bson(a))
            .collect::<Vec<_>>();
        let num_addresses = address_prefixes.len();
        if !address_prefixes.is_empty() {
            query.insert("logs.address", doc! { "$in": address_prefixes });
        }

        let convert_topic = |t: &Topic| t.iter().map(fb_to_bson).collect::<Vec<_>>();
        for (i, topic) in topics.iter().enumerate() {
            if topic.is_empty() {
                continue;
            }
            query.insert(
                format!("logs.topic_{}", i),
                doc! { "$in": convert_topic(topic) },
            );
        }

        debug!(
            "Querying logs from {from} to {to}, with {num_addresses} addresses and {} topics",
            topics.len()
        );
        debug!("Logs mongo query: {query:?}");

        // Query for the tx documents
        let cursor = self
            .txs
            .find(query)
            .await
            .wrap_err("Failed to query logs")?;

        // Convert to TxIndexedData stream
        Ok(cursor
            .then(move |doc| {
                let model = self.clone();
                async move {
                    let tx_doc: TxDoc = doc?;

                    // Fetch all the data needed for TxIndexedData
                    let (header, tx, trace, receipt) = futures::try_join!(
                        model.get_header(tx_doc.block_number as u64),
                        resolve_inline_or_ref::<TxEnvelopeWithSender>(
                            &model.s3_client,
                            tx_doc.body,
                            tx_doc.body_ref,
                        ),
                        resolve_trace_inline_or_ref(
                            &model.s3_client,
                            tx_doc.trace,
                            tx_doc.trace_ref
                        ),
                        resolve_inline_or_ref::<ReceiptWithLogIndex>(
                            &model.s3_client,
                            tx_doc.receipt,
                            tx_doc.receipt_ref,
                        ),
                    )?;

                    let header_subset = header.to_header_subset(tx_doc.tx_index, &tx_doc.gas_used);

                    Ok(TxIndexedData {
                        tx,
                        trace,
                        receipt,
                        header_subset,
                    })
                }
            })
            .boxed())
    }
}

#[derive(Clone)]
struct TxLogsDocument {
    pub tx_hash: alloy_primitives::TxHash,
    pub logs: Vec<Document>,
}

impl TxLogsDocument {
    pub fn from_receipts(block: &Block, receipts: &BlockReceipts) -> Vec<Self> {
        receipts
            .iter()
            .zip(&block.body.transactions)
            .filter_map(|(receipt, tx)| {
                let logs = receipt.receipt.logs();
                if logs.is_empty() {
                    None
                } else {
                    Some(TxLogsDocument {
                        tx_hash: *tx.tx.tx_hash(),
                        logs: logs_to_doc(logs),
                    })
                }
            })
            .collect()
    }
}

pub fn logs_to_doc(logs: &[Log]) -> Vec<Document> {
    logs.iter()
        .map(|log| {
            let mut doc = doc! {
                "address": fb_to_bson(&log.address),
            };

            for (i, topic) in log.topics().iter().enumerate() {
                doc.insert(format!("topic_{}", i), fb_to_bson(topic));
            }

            doc
        })
        .collect()
}

pub fn fb_to_bson<const N: usize>(fb: &FixedBytes<N>) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: fb.as_slice()[0..4].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy_primitives::{Bloom, Log, LogData, B256};
    use serial_test::serial;

    use super::*;
    use crate::{
        kvstore::{memory::MemoryStorage, mongo::{mongo_tests::TestMongoContainer, new_client}, object_store::ObjectStore},
        test_utils::{mock_block, mock_tx},
    };

    async fn setup() -> Result<(TestMongoContainer, ModelV2)> {
        let container = TestMongoContainer::new().await?;
        let client = new_client(&format!("mongodb://localhost:{}", container.port)).await?;

        // Create test database first
        client.database("test_db").create_collection("test").await?;

        // For tests, use MemoryStorage wrapped in ObjectStore enum
        let memory_storage = MemoryStorage::new("test-bucket");

        let model_v2 = ModelV2::from_client(client, ObjectStore::Memory(memory_storage), 10, "test_db".to_string()).await?;

        // Ensure indexes are created
        model_v2.ensure_logs_indexes().await?;

        Ok((container, model_v2))
    }

    fn create_receipt_with_logs(
        cumulative_gas: u128,
        emitter: Address,
        topics: Vec<B256>,
    ) -> ReceiptWithLogIndex {
        let log = Log {
            address: emitter,
            data: LogData::new(topics, vec![42; 100].into()).unwrap(),
        };

        ReceiptWithLogIndex {
            receipt: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
                Receipt {
                    logs: vec![log],
                    status: alloy_consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: cumulative_gas,
                },
                Bloom::default(),
            )),
            starting_log_index: 0,
        }
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_logs_indexing() -> Result<()> {
        let (_container, model) = setup().await?;

        // Create test data
        let block_num = 100;
        let test_address = Address::from([0x11; 20]);
        let test_topic = B256::from([0x22; 32]);

        let txs = vec![mock_tx(1), mock_tx(2)];
        let block = mock_block(block_num, txs);

        let receipts = vec![
            create_receipt_with_logs(1000, test_address, vec![test_topic]),
            create_receipt_with_logs(
                2000,
                Address::from([0x33; 20]),
                vec![B256::from([0x44; 32])],
            ),
        ];

        let traces = vec![vec![1; 100], vec![2; 100]];

        // Archive the block
        model
            .archive_block_data(block.clone(), receipts.clone(), traces)
            .await?;

        // Index the logs
        model.index_block_logs(&block, &receipts).await?;

        // Query logs with address filter
        let results: Vec<_> = model
            .query_logs(block_num, block_num, &[test_address], &[])
            .await?
            .try_collect()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].header_subset.block_number, block_num);

        // Query logs with topic filter
        let results: Vec<_> = model
            .query_logs(
                block_num,
                block_num,
                &[],
                &[alloy_rpc_types::FilterSet::from(test_topic)],
            )
            .await?
            .try_collect()
            .await?;

        assert_eq!(results.len(), 1);

        Ok(())
    }
}
