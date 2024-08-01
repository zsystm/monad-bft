use monad_bls::{BlsPubKey, BlsSignatureCollection};
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::{serde::deserialize_eth_address_from_str, EthAddress};
use monad_secp::{PubKey, SecpSignature};
use monad_types::Stake;
use serde::Deserialize;

mod bootstrap;
pub use bootstrap::{NodeBootstrapConfig, NodeBootstrapPeerConfig};

pub mod consensus;
pub use consensus::NodeConsensusConfig;

mod network;
pub use network::NodeNetworkConfig;

pub mod util;
use util::{deserialize_bls12_381_pubkey, deserialize_secp256k1_pubkey};

pub(crate) type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    ////////////////////////////////
    // NODE-SPECIFIC CONFIGURATION //
    ////////////////////////////////
    pub name: Option<String>,
    pub network_name: Option<String>,

    #[serde(deserialize_with = "deserialize_eth_address_from_str")]
    pub beneficiary: EthAddress,

    pub ipc_tx_batch_size: u32,
    pub ipc_max_queued_batches: u8,
    // must be <= ipc_max_queued_batches
    pub ipc_queued_batches_watermark: u8,

    pub bootstrap: NodeBootstrapConfig,
    pub network: NodeNetworkConfig,

    // TODO split network-wide configuration into separate file
    ////////////////////////////////
    // NETWORK-WIDE CONFIGURATION //
    ////////////////////////////////
    pub chain_id: u64,
    pub consensus: NodeConsensusConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenesisConfig {
    pub validators: Vec<ValidatorConfig>,
}

pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorConfig {
    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,

    #[serde(deserialize_with = "deserialize_bls12_381_pubkey")]
    pub bls12_381_pubkey: BlsPubKey,

    pub stake: Stake,
}
