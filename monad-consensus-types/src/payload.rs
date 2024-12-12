use std::collections::BTreeMap;

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use auto_impl::auto_impl;
use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{DontCare, Round, SeqNum};
use reth_primitives::Header;
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

const BLOOM_SIZE: usize = 256;

/// Type to represent the Ethereum Logs Bloom Filter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bloom(pub [u8; BLOOM_SIZE]);

impl Bloom {
    pub fn zero() -> Self {
        Bloom([0x00_u8; BLOOM_SIZE])
    }
}

impl AsRef<[u8]> for Bloom {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Ethereum unit of gas
#[repr(transparent)]
#[derive(Debug, Default, Copy, Clone, Eq, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct Gas(pub u64);

impl AsRef<[u8]> for Gas {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// A subset of Ethereum block header fields that are included in consensus
/// proposals. The values are populated from the results of executing the
/// previous block
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ExecutionProtocol {
    pub delayed_execution_result: Header,
    pub seq_num: SeqNum,
    pub beneficiary: EthAddress,
    pub randao_reveal: RandaoReveal,
}

impl DontCare for ExecutionProtocol {
    fn dont_care() -> Self {
        ExecutionProtocol {
            delayed_execution_result: Header::default(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        }
    }
}

impl Hashable for ExecutionProtocol {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

/// RLP encoded list of a set of full RLP encoded Eth transactions
// Do NOT derive or implement Default!
// Empty byte array is not valid RLP
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)] //FIXME: don't derive rlpencodable
pub struct FullTransactionList(Bytes);

impl FullTransactionList {
    pub fn empty() -> Self {
        Self::new(vec![EMPTY_RLP_TX_LIST].into())
    }

    pub fn new(txs: Bytes) -> Self {
        Self(txs)
    }

    pub fn bytes(&self) -> &Bytes {
        &self.0
    }
}

impl std::fmt::Debug for FullTransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Txns").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for FullTransactionList {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// randao_reveal uses a proposer's public key to contribute randomness
#[derive(Debug, Clone, Default, PartialEq, Eq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct RandaoReveal(pub Vec<u8>);

impl RandaoReveal {
    pub fn new<CS: CertificateSignature>(round: Round, keypair: &CS::KeyPairType) -> Self {
        Self(CS::sign(&round.0.to_le_bytes(), keypair).serialize())
    }

    pub fn verify<CS: CertificateSignature>(
        &self,
        round: Round,
        pubkey: &CertificateSignaturePubKey<CS>,
    ) -> Result<(), CS::Error> {
        let sig = CS::deserialize(&self.0)?;

        sig.verify(&round.0.to_le_bytes(), pubkey)
    }
}

impl AsRef<[u8]> for RandaoReveal {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct Payload {
    pub txns: FullTransactionList,
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field("txn_payload_len", &format!("{}", self.txns.bytes().len()))
            .finish_non_exhaustive()
    }
}

impl Payload {
    pub fn get_id(&self) -> PayloadId {
        let hash = HasherType::hash_object(self);
        PayloadId(hash)
    }
}

impl Hashable for Payload {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.txns.bytes())
    }
}

impl DontCare for Payload {
    fn dont_care() -> Self {
        Self {
            txns: FullTransactionList::empty(),
        }
    }
}

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct PayloadId(pub Hash);

impl std::fmt::Debug for PayloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}
