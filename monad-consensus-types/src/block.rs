use alloy_rlp::{RlpDecodable, RlpEncodable};
use auto_impl::auto_impl;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};
use reth_primitives::Header;

use crate::{
    payload::{ExecutionProtocol, Payload, PayloadId},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

/// Represent a range of blocks the last of which is `last_block_id` and includes
/// all blocks upto to `root_seq_num`
/// For a valid block range, the seq num of block `last_block_id` >= `root_seq_num`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRange {
    pub last_block_id: BlockId,
    pub max_blocks: SeqNum,
}

impl Hashable for BlockRange {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

/// This trait represents a consensus block
pub trait BlockType<SCT: SignatureCollection>: Clone + PartialEq + Eq {
    type NodeIdPubKey: PubKey;
    type TxnHash: PartialEq + Eq + std::hash::Hash;
    /// Unique hash for the block
    fn get_id(&self) -> BlockId;

    /// Round in which this block was proposed
    fn get_round(&self) -> Round;

    /// Epoch in which this block was proposed
    fn get_epoch(&self) -> Epoch;

    /// Node which proposed this block
    fn get_author(&self) -> NodeId<Self::NodeIdPubKey>;

    /// Payload associated with this block
    fn get_payload(&self) -> Payload;

    /// Unique hash of the associated payload
    fn get_payload_id(&self) -> PayloadId;

    /// returns the BlockId for the block referenced by
    /// the QC contained in this block
    fn get_parent_id(&self) -> BlockId;

    /// returns the Round for the block referenced by
    /// the QC contained in this block
    fn get_parent_round(&self) -> Round;

    /// Sequence number when this block was proposed
    fn get_seq_num(&self) -> SeqNum;

    fn get_delayed_execution_result(&self) -> &Header;

    /// get list of all txn hashes in this block
    fn get_txn_hashes(&self) -> Vec<Self::TxnHash>;

    fn get_txn_list_len(&self) -> usize;

    /// get a reference to the block's QC
    fn get_qc(&self) -> &QuorumCertificate<SCT>;

    fn get_timestamp(&self) -> u64;

    fn get_unvalidated_block(self) -> Block<SCT>;

    fn get_unvalidated_block_ref(&self) -> &Block<SCT>;

    fn get_full_block(self) -> FullBlock<SCT>;
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone, RlpDecodable, RlpEncodable)]
pub struct Block<SCT: SignatureCollection> {
    /// proposer of this block
    pub author: NodeId<SCT::NodeIdPubKey>,

    /// Epoch this block was proposed in
    pub epoch: Epoch,

    /// round this block was proposed in
    pub round: Round,

    /// data related to the execution side of the protocol
    pub execution: ExecutionProtocol,

    /// identifier for the transaction payload of this block
    pub payload_id: PayloadId,

    /// Certificate of votes for the parent block
    pub qc: QuorumCertificate<SCT>,

    pub timestamp: u64,
}

impl<SCT: SignatureCollection> PartialEq for Block<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.get_id() == other.get_id()
    }
}
impl<SCT: SignatureCollection> Eq for Block<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for Block<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .field("timestamp", &self.timestamp)
            .field("qc", &self.qc)
            .field("id", &self.get_id())
            .field("payload_id", &self.payload_id)
            .field("seq_num", &self.execution.seq_num)
            .field(
                "execution_state_root",
                &self.execution.delayed_execution_result.state_root.0,
            )
            .finish_non_exhaustive()
    }
}

impl<SCT: SignatureCollection> Hashable for Block<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl<SCT: SignatureCollection> Block<SCT> {
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        timestamp: u64,
        epoch: Epoch,
        round: Round,
        execution: &ExecutionProtocol,
        payload_id: PayloadId,
        qc: &QuorumCertificate<SCT>,
    ) -> Self {
        Self {
            author,
            timestamp,
            epoch,
            round,
            execution: execution.clone(),
            payload_id,
            qc: qc.clone(),
        }
    }
}

// TODO, this should be removed, we don't need to implement BlockType for Block because we won't
// use this as a validatedBlock type going forward in the passthru policy, FullBlock can be used
// there
impl<SCT: SignatureCollection> BlockType<SCT> for Block<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        let mut state = HasherType::new();

        let mut _block_hash_span = tracing::trace_span!("block_hash_span").entered();
        let rlp_encode = alloy_rlp::encode(self);
        state.update(rlp_encode);

        BlockId(state.hash())
    }

    fn get_round(&self) -> Round {
        self.round
    }

    fn get_epoch(&self) -> Epoch {
        self.epoch
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.author
    }

    fn get_payload(&self) -> Payload {
        todo!()
    }

    fn get_payload_id(&self) -> PayloadId {
        self.payload_id
    }

    fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.execution.seq_num
    }

    fn get_delayed_execution_result(&self) -> &Header {
        &self.execution.delayed_execution_result
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn get_txn_list_len(&self) -> usize {
        // FIXME
        0
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.qc
    }

    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        self
    }

    fn get_full_block(self) -> FullBlock<SCT> {
        todo!();
    }
}

#[derive(Debug, PartialEq)]
pub enum BlockPolicyError {
    BlockNotCoherent,
    StateBackendError(StateBackendError),
}

impl From<StateBackendError> for BlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

/// Trait that represents how inner contents of a block should be validated
#[auto_impl(Box)]
pub trait BlockPolicy<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + std::fmt::Debug
        + BlockType<SCT, NodeIdPubKey = SCT::NodeIdPubKey>
        + Hashable
        + Send;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError>;

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn update_committed_block(&mut self, block: &Self::ValidatedBlock);

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn reset(&mut self, last_delay_non_null_committed_blocks: Vec<&Self::ValidatedBlock>);
}

/// A block policy which does not validate the inner contents of the block
#[derive(Copy, Clone, Default)]
pub struct PassthruBlockPolicy;

impl<SCT> BlockPolicy<SCT, InMemoryState> for PassthruBlockPolicy
where
    SCT: SignatureCollection,
{
    type ValidatedBlock = FullBlock<SCT>;

    fn check_coherency(
        &self,
        _: &Self::ValidatedBlock,
        _: Vec<&Self::ValidatedBlock>,
        _: &InMemoryState,
    ) -> Result<(), BlockPolicyError> {
        Ok(())
    }

    fn update_committed_block(&mut self, _: &Self::ValidatedBlock) {}
    fn reset(&mut self, _: Vec<&Self::ValidatedBlock>) {}
}

#[derive(Debug, Clone)]
pub struct FullBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub payload: Payload,
}

impl<SCT: SignatureCollection> PartialEq for FullBlock<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.block.get_id() == other.block.get_id()
    }
}
impl<SCT: SignatureCollection> Eq for FullBlock<SCT> {}

impl<SCT: SignatureCollection> BlockType<SCT> for FullBlock<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        self.block.get_id()
    }

    fn get_round(&self) -> Round {
        self.block.round
    }

    fn get_epoch(&self) -> Epoch {
        self.block.epoch
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.block.author
    }

    fn get_payload(&self) -> Payload {
        self.payload.clone()
    }

    fn get_payload_id(&self) -> PayloadId {
        self.block.payload_id
    }

    fn get_parent_id(&self) -> BlockId {
        self.block.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.execution.seq_num
    }

    fn get_delayed_execution_result(&self) -> &Header {
        &self.block.execution.delayed_execution_result
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn get_txn_list_len(&self) -> usize {
        self.payload.txns.bytes().len()
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.block.qc
    }

    fn get_timestamp(&self) -> u64 {
        self.block.timestamp
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self.block
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        &self.block
    }

    fn get_full_block(self) -> FullBlock<SCT> {
        self
    }
}

impl<SCT: SignatureCollection> Hashable for FullBlock<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.hash(state);
        self.payload.hash(state);
    }
}
