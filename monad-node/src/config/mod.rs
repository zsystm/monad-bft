use monad_bls::BlsSignatureCollection;
use monad_consensus_types::checkpoint::Checkpoint;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::SecpSignature;

pub type SignatureType = SecpSignature;
pub type SignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
pub type ExecutionProtocolType = EthExecutionProtocol;
pub type ForkpointConfig = Checkpoint<SignatureCollectionType>;
pub type NodeConfig = monad_node_config::NodeConfig<CertificateSignaturePubKey<SignatureType>>;
