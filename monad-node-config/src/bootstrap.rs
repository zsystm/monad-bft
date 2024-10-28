use monad_secp::PubKey;
use serde::Deserialize;

use super::util::deserialize_secp256k1_pubkey;

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapConfig {
    pub peers: Vec<NodeBootstrapPeerConfig>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBootstrapPeerConfig {
    pub address: String,

    #[serde(deserialize_with = "deserialize_secp256k1_pubkey")]
    pub secp256k1_pubkey: PubKey,
}
