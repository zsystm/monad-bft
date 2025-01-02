use std::{collections::HashMap, time::Duration};

use alloy_primitives::TxHash;
use alloy_rlp::Decodable;
use base64::{prelude::BASE64_STANDARD, Engine};
use eyre::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use tokio_retry::Retry;
use url::Url;

use crate::{retry_strategy, IndexStoreReader, TxIndexedData};

#[derive(Clone)]
pub struct CloudProxyReader {
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
    pub concurrency: usize,
}

impl CloudProxyReader {
    pub fn new(api_key: &str, url: Url, table: String, concurrency: usize) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert("x-api-key", HeaderValue::from_str(&api_key)?);
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self {
            client,
            url,
            table,
            concurrency,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProxyResponse {
    tx_hash: TxHash,
    data: String,
}

impl IndexStoreReader for CloudProxyReader {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        // TODO: real bulk requests
        futures::stream::iter(keys)
            .map(|key| {
                Retry::spawn(retry_strategy(), move || {
                    let this = self.clone();
                    async move { this.get(key).await.map(|data| (data, key)) }
                })
            })
            .buffered(self.concurrency)
            .try_fold(
                HashMap::with_capacity(keys.len()),
                |mut map, (maybe_data, key)| async move {
                    if let Some(data) = maybe_data {
                        map.insert(*key, data);
                    }
                    Ok(map)
                },
            )
            .await
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        let url = format!("{}?txhash={:x}&table={}", self.url, key, self.table);
        let resp = self.client.get(url).send().await?;
        if resp.status() == 404 {
            return Ok(None);
        }
        let resp: ProxyResponse = resp.json().await?;
        let bytes = base64::prelude::BASE64_STANDARD.decode(resp.data)?;

        let data = TxIndexedData::decode(&mut bytes.as_slice())
            .wrap_err("Failed to rlp decode TxIndexedData from cloud proxy")?;
        Ok(Some(data))
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::hex::FromHex;

    use super::*;

    #[tokio::test]
    pub async fn test_cloud_proxy_reader_bulk_get_jank() {
        let reader = CloudProxyReader::new(
            "eoc53hGz4C4ZEFbfhB9YH3nd8iCQwqAz52plpZ7H",
            url::Url::parse("https://9df09fanz1.execute-api.us-east-2.amazonaws.com/prod").unwrap(),
            "stressnet-devnet2-4".to_owned(),
            1,
        )
        .unwrap();

        let tx_hashes = [
            TxHash::from_hex("87fbcd96526a7672bdae9ea13f7a07863025c8c1dc8ed797c81cfbe36aef3307")
                .unwrap(),
            // not actually present
            TxHash::from_hex("f3f6a2d1dd3a10bb7d8f25aa21f410c3c85e7cbd7705c787251d5f87182cb42d") 
                .unwrap(),
            TxHash::from_hex("f3f6a0d1dd3a10bb7d8f25aa21f410c3c85e7cbd7705c787251d5f87182cb42d")
                .unwrap(),
        ];

        let result = reader.bulk_get(&tx_hashes).await.unwrap();

        assert!(result.contains_key(&tx_hashes[0]));
        assert!(!result.contains_key(&tx_hashes[1]));
        assert!(result.contains_key(&tx_hashes[2]));

        assert_eq!(result.get(&tx_hashes[0]).unwrap().tx.tx_hash(), &tx_hashes[0]);
    }

    #[tokio::test]
    pub async fn test_cloud_proxy_reader_jank() {
        let reader = CloudProxyReader::new(
            "eoc53hGz4C4ZEFbfhB9YH3nd8iCQwqAz52plpZ7H",
            url::Url::parse("https://9df09fanz1.execute-api.us-east-2.amazonaws.com/prod").unwrap(),
            "stressnet-devnet2-4".to_owned(),
            1,
        )
        .unwrap();

        let txhash =
            TxHash::from_hex("87fbcd96526a7672bdae9ea13f7a07863025c8c1dc8ed797c81cfbe36aef3307")
                .unwrap();
        let result = reader.get(&txhash).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.tx.tx_hash(), &txhash);


        // not present
        let txhash =
            TxHash::from_hex("f3f6a2d1dd3a10bb7d8f25aa21f410c3c85e7cbd7705c787251d5f87182cb42d") 
                .unwrap();
        let result = reader.get(&txhash).await.unwrap();
        assert!(result.is_none());
    }
}
