use monad_bls::BlsSignatureCollection;
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use monad_secp::SecpSignature;
use serde::Deserialize;

pub use self::{
    bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig},
    consensus::NodeConsensusConfig,
    fullnode::{FullNodeConfig, FullNodeIdentityConfig},
    metrics::NodeMetricsConfig,
    network::NodeNetworkConfig,
};

mod bootstrap;
mod consensus;
mod fullnode;
mod metrics;
mod network;
mod util;

pub type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    ////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    ////////////////////////////////
    pub node_name: String,
    pub network_name: String,

    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub ipc_tx_batch_size: u32,
    pub ipc_max_queued_batches: u8,
    // must be <= ipc_max_queued_batches
    pub ipc_queued_batches_watermark: u8,

    pub statesync_threshold: u16,
    pub statesync_max_concurrent_requests: u8,
    pub statesync_request_timeout_ms: u16,

    pub bootstrap: NodeBootstrapConfig,
    pub fullnode: FullNodeConfig,
    pub network: NodeNetworkConfig,
    pub metrics: Option<NodeMetricsConfig>,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}
