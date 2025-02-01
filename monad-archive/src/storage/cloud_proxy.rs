use std::time::Duration;

use alloy_primitives::TxHash;
use base64::Engine;
use bytes::Bytes;
use eyre::Result;
use reqwest::header::HeaderValue;
use url::Url;

use crate::prelude::*;

#[derive(Clone)]
pub struct CloudProxyReader(Arc<CloudProxyReaderInner>);

struct CloudProxyReaderInner {
    pub client: reqwest::Client,
    pub url: Url,
    pub table: String,
}

impl CloudProxyReader {
    pub fn new(api_key: &str, url: Url, table: String) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::with_capacity(1);
        headers.insert("x-api-key", HeaderValue::from_str(api_key)?);
        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .timeout(Duration::from_secs(2))
            .build()?;
        Ok(Self(Arc::new(CloudProxyReaderInner { client, url, table })))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ProxyResponse {
    tx_hash: TxHash,
    data: String,
}

impl KVReader for CloudProxyReader {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let url = format!("{}?txhash={}&table={}", self.0.url, key, self.0.table);
        let resp = self.0.client.get(url).send().await?;
        if resp.status() == 404 {
            return Ok(None);
        }
        let resp: ProxyResponse = resp.json().await?;
        let bytes = base64::prelude::BASE64_STANDARD.decode(resp.data)?;

        Ok(Some(bytes.into()))
    }
}
