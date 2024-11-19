use std::{collections::HashMap, time::Duration};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region, SdkConfig};
use aws_sdk_dynamodb::{
    types::{AttributeValue, PutRequest, WriteRequest},
    Client,
};
use eyre::{bail, Context};
use futures::future::join_all;
use monad_archive::errors::ArchiveError;
use reth_primitives::TxHash;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::cli::Cli;

const BLOCK_PADDING_WIDTH: usize = 12;
const MAX_BATCH_SIZE: usize = 25;

#[derive(Clone)]
pub struct DynamoDBArchive {
    // For DynamoDB
    pub client: Client,
    pub table: String,
}

impl DynamoDBArchive {
    pub async fn new(cli: &Cli) -> Result<Self, eyre::ErrReport> {
        let region_provider = RegionProviderChain::default_provider().or_else(
            cli.aws
                .as_ref()
                .unwrap()
                .region
                .clone()
                .map(Region::new)
                .unwrap_or_else(|| Region::new("us-east-2")),
        );

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        Ok(Self {
            client,
            table: cli.dynamodb_table.clone(),
        })
    }

    pub async fn index_block(
        &self,
        hashes: Vec<TxHash>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
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
                .build()
                .expect("Build successful");

            let write_request = WriteRequest::builder().put_request(put_request).build();

            requests.push(write_request);
        }

        let batch_writes = split_into_batches(requests, MAX_BATCH_SIZE);
        let mut batch_write_handles = Vec::new();
        for batch_write in batch_writes {
            let batch_write = batch_write.clone();

            let this = (*self).clone();
            let handle = tokio::spawn(async move { this.upload_to_db(batch_write).await });

            batch_write_handles.push(handle);
        }

        let results = join_all(batch_write_handles).await;

        for (idx, batch_write_result) in results.into_iter().enumerate() {
            if let Err(e) = batch_write_result {
                bail!("Failed to upload index: {}, {:?}", idx, e);
            }
        }
        Ok(())
    }

    pub async fn upload_to_db(&self, values: Vec<WriteRequest>) -> Result<(), ArchiveError> {
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

            async move {
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
                            "Unprocessed items detected for table {}: {}. Retrying...",
                            table,
                            unprocessed.get(&table).map(|v| v.len()).unwrap_or(0)
                        );
                    }
                }

                Ok(())
            }
        })
        .await
        .wrap_err_with(|| format!("Failed to upload to table {} after retries", self.table))?;
        Ok(())
    }
}

fn split_into_batches(values: Vec<WriteRequest>, batch_size: usize) -> Vec<Vec<WriteRequest>> {
    values
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}
