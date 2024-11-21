use std::{collections::HashMap, sync::Arc, time::Duration};

use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    operation::get_item::GetItemOutput,
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use eyre::{bail, Context, Result};
use futures::future::join_all;
use reth_primitives::TxHash;
use tokio::sync::Semaphore;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};


#[derive(Clone)]
pub struct DynamoDBArchive {
    pub client: Client,
    pub table: String,
    pub semaphore: Arc<Semaphore>,
}

impl DynamoDBArchive {
    const READ_BATCH_SIZE: usize = 100;
    const WRITE_BATCH_SIZE: usize = 25;

    pub fn new(table: String, config: &SdkConfig, concurrency: usize) -> Self {
        let client = Client::new(config);
        Self {
            client,
            table,
            semaphore: Arc::new(Semaphore::new(concurrency)),
        }
    }

    pub async fn batch_get_block_nums(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<u64>>> {
        let mut results = Vec::new();
        let batches = keys.chunks(Self::READ_BATCH_SIZE);

        for batch in batches {
            // Prepare the keys for this batch
            let mut key_maps = Vec::new();
            for key in batch {
                let key = key.trim_start_matches("0x");
                let mut key_map = HashMap::new();
                key_map.insert("tx_hash".to_string(), AttributeValue::S(key.to_string()));
                key_maps.push(key_map);
            }

            // Build the batch request
            let mut request_items = HashMap::new();
            request_items.insert(
                self.table.clone(),
                KeysAndAttributes::builder()
                    .set_keys(Some(key_maps))
                    .build()?,
            );

            // Execute the batch_get_item API
            let response = self
                .client
                .batch_get_item()
                .set_request_items(Some(request_items))
                .send()
                .await?;

            // Collect retrieved items
            if let Some(responses) = response.responses {
                if let Some(items) = responses.get(&self.table) {
                    results.extend(items.iter().map(block_number_from_map));
                }
            }

            // Retry unprocessed keys
            let mut unprocessed_keys = response.unprocessed_keys;
            while let Some(unprocessed) = unprocessed_keys {
                let response_retry = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(unprocessed))
                    .send()
                    .await?;

                if let Some(responses_retry) = response_retry.responses {
                    if let Some(items_retry) = responses_retry.get(&self.table) {
                        results.extend(items_retry.iter().map(block_number_from_map));
                    }
                }
                unprocessed_keys = response_retry.unprocessed_keys;
            }
        }

        Ok(results)
    }

    pub async fn get_block_number_by_tx_hash(
        &self,
        tx_hash: &str,
    ) -> Result<Option<u64>> {
        // Create the key map
        let tx_hash = tx_hash.trim_start_matches("0x");
        let mut key = HashMap::new();
        key.insert(
            "tx_hash".to_string(),
            AttributeValue::S(tx_hash.to_string()),
        );

        // Perform the get_item operation
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .set_key(Some(key))
            .send()
            .await?;

        // Extract and return the block_number attribute if the item is found
        Ok(block_number_from_resp(result))
    }

    pub async fn index_block(
        &self,
        hashes: Vec<TxHash>,
        block_num: u64,
    ) -> Result<()> {
        let mut requests = Vec::new();

        for hash in hashes {
            let mut attribute_map = HashMap::new();
            attribute_map.insert("tx_hash".to_string(), AttributeValue::S(hex::encode(hash)));
            attribute_map.insert(
                "block_number".to_string(),
                AttributeValue::S(block_num.to_string()),
            );

            let put_request = PutRequest::builder()
                .set_item(Some(attribute_map))
                .build()?;

            let write_request = WriteRequest::builder().put_request(put_request).build();

            requests.push(write_request);
        }

        let batch_writes = split_into_batches(requests, Self::WRITE_BATCH_SIZE);
        // let batch_writes = split_into_batches(requests, 1);
        let mut batch_write_handles = Vec::new();
        for batch_write in batch_writes {
            let batch_write = batch_write.clone();

            let this = (*self).clone();
            let handle = tokio::spawn(async move { this.upload_to_db(batch_write).await });

            batch_write_handles.push(handle);
        }

        let results = join_all(batch_write_handles).await;

        for (idx, batch_write_result) in results.into_iter().enumerate() {
            batch_write_result.wrap_err_with(|| format!("Failed to upload index {idx}"))??;
        }
        Ok(())
    }

    pub async fn upload_to_db(&self, values: Vec<WriteRequest>) -> Result<()> {
        if values.len() > 25 {
            panic!("Batch size larger than limit = 25")
        }

        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        // TODO: Only deal with unprocessed items, but it's pretty complicated
        Retry::spawn(retry_strategy, || {
            let values = values.clone();
            // let client = client;
            let client = &self.client;
            let table = self.table.clone();
            let semeaphore = Arc::clone(&self.semaphore);

            async move {
                let _permit = semeaphore.acquire().await.expect("semaphore dropped");
                let mut batch_write: HashMap<String, Vec<WriteRequest>> = HashMap::new();
                batch_write.insert(table.clone(), values.clone());

                let response = client
                    .batch_write_item()
                    .set_request_items(Some(batch_write.clone()))
                    .send()
                    .await
                    .wrap_err_with(|| {
                        format!("Failed to upload to table {}. Retrying...", table)
                    })?;

                // Check for unprocessed items
                if let Some(unprocessed) = response.unprocessed_items() {
                    if !unprocessed.is_empty() {
                        bail!(
                            "Unprocessed items detected for table {}: {:?}. Retrying...",
                            table,
                            unprocessed.get(&table).map(|v| v.len()).unwrap_or(0)
                        );
                    }
                }

                Ok(())
            }
        })
        .await
        .map(|_| ())
        .wrap_err_with(|| format!("Failed to upload to table {} after retries", self.table))
    }
}

fn split_into_batches(values: Vec<WriteRequest>, batch_size: usize) -> Vec<Vec<WriteRequest>> {
    values
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn block_number_from_map(item: &HashMap<String, AttributeValue>) -> Option<u64> {
    if let Some(block_number_attr) = item.get("block_number") {
        if let AttributeValue::S(block_number) = block_number_attr {
            return block_number.parse().ok();
        } else {
            dbg!("failed to get block_bumber from attr");
        }
    } else {
        dbg!("block number attr not found");
    }
    None
}

fn block_number_from_resp(result: GetItemOutput) -> Option<u64> {
    if let Some(item) = result.item {
        return block_number_from_map(&item);
    } else {
        dbg!("no item in result");
    }
    None
}
