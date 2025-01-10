use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{DontCare, EnumDiscriminant, Round, SeqNum};
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

use crate::state_root_hash::StateRootHash;

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

pub const BASE_FEE_PER_GAS: u64 = 50_000_000_000;
pub const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
/// Max proposal size in bytes (average transactions ~400 bytes)
pub const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

/// A subset of Ethereum block header fields that are included in consensus
/// proposals. The values are populated from the results of executing the
/// previous block
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProtocol {
    pub state_root: StateRootHash,
    pub seq_num: SeqNum,
    pub beneficiary: EthAddress,
    pub randao_reveal: RandaoReveal,
}

impl DontCare for ExecutionProtocol {
    fn dont_care() -> Self {
        ExecutionProtocol {
            state_root: Default::default(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        }
    }
}

impl Hashable for ExecutionProtocol {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.state_root);
        state.update(self.seq_num);
        state.update(self.beneficiary);
        state.update(&self.randao_reveal);
    }
}

/// RLP encoded list of a set of full RLP encoded Eth transactions
// Do NOT derive or implement Default!
// Empty byte array is not valid RLP
#[derive(Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionPayload {
    List(FullTransactionList),
    Null,
}

impl Hashable for TransactionPayload {
    fn hash(&self, state: &mut impl Hasher) {
        match self {
            TransactionPayload::List(txns) => {
                EnumDiscriminant(1).hash(state);
                state.update(txns);
            }
            TransactionPayload::Null => {
                EnumDiscriminant(2).hash(state);
            }
        }
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Clone, PartialEq, Eq)]
pub struct Payload {
    pub txns: TransactionPayload,
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field(
                "txn_payload_len",
                &match &self.txns {
                    TransactionPayload::List(txns) => {
                        format!("{:?}", txns.bytes().len())
                    }
                    TransactionPayload::Null => "null".to_owned(),
                },
            )
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
        self.txns.hash(state);
    }
}

impl DontCare for Payload {
    fn dont_care() -> Self {
        Self {
            txns: TransactionPayload::List(FullTransactionList::empty()),
        }
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// The outcomes of validating state root hashes
#[derive(Debug, PartialEq)]
pub enum StateRootResult {
    /// State root hash is valid
    Success,

    /// State root hash does not match our expected value
    Mismatch,

    /// State root hash is from a sequence number that exceeds
    /// what this node has executed. Implies that this node has
    /// fallen behind
    OutOfRange,

    /// State root hash for the sequence number is missing. Implies
    /// this node has missed an update from execution
    Missing,
}

/// Special hash value used in proposals for the first delay-num blocks
/// from genesis when there isn't a valid state root hash to include.
pub const INITIAL_DELAY_STATE_ROOT_HASH: StateRootHash = StateRootHash(Hash([0; 32]));
