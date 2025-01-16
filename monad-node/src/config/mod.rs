use alloy_primitives::Address;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthExecutionProtocol};
use monad_secp::SecpSignature;
use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

pub mod consensus;
pub use consensus::NodeConsensusConfig;

mod fullnode;
pub use fullnode::{FullNodeConfig, FullNodeIdentityConfig};

mod network;
pub use network::NodeNetworkConfig;

mod sync_peers;
#[allow(unused_imports)]
pub use sync_peers::{BlockSyncPeersConfig, SyncPeerIdentityConfig};

pub mod util;

pub(crate) type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ExecutionProtocolType = EthExecutionProtocol;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    ////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    ////////////////////////////////
    pub name: Option<String>,
    pub network_name: Option<String>,

    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: Address,

    pub ipc_tx_batch_size: u32,
    pub ipc_max_queued_batches: u8,
    // must be <= ipc_max_queued_batches
    pub ipc_queued_batches_watermark: u8,

    pub statesync_threshold: u16,
    pub statesync_max_concurrent_requests: u8,
    pub statesync_request_timeout_ms: u16,

    pub bootstrap: NodeBootstrapConfig,
    pub fullnode: FullNodeConfig,
    pub blocksync_override: BlockSyncPeersConfig,
    pub network: NodeNetworkConfig,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}

pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;
