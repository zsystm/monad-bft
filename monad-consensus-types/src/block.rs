use std::{fmt::Debug, ops::Deref};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use auto_impl::auto_impl;
use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::{BlockId, Epoch, ExecutionProtocol, FinalizedHeader, NodeId, Round, SeqNum};

use crate::{
    block_validator::BlockValidationError,
    checkpoint::RootInfo,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

pub const GENESIS_TIMESTAMP: u128 = 0;

/// Represent a range of blocks the last of which is `last_block_id` and includes `num_blocks`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRange {
    pub last_block_id: BlockId,
    pub num_blocks: SeqNum,
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// round this block was first proposed in
    /// note that this will differ from proposal_round for a reproposal
    pub block_round: Round,
    /// Epoch this block was proposed in
    pub epoch: Epoch,
    /// Certificate of votes for the parent block
    pub qc: QuorumCertificate<SCT>,
    /// proposer of this block
    pub author: NodeId<CertificateSignaturePubKey<ST>>,

    pub seq_num: SeqNum,
    pub timestamp_ns: u128,
    // This is SCT::SignatureType because SCT signatures are guaranteed to be deterministic
    pub round_signature: RoundSignature<SCT::SignatureType>,

    /// data related to the execution side of the protocol
    pub delayed_execution_results: Vec<EPT::FinalizedHeader>,
    pub execution_inputs: EPT::ProposedHeader,
    /// identifier for the transaction payload of this block
    pub block_body_id: ConsensusBlockBodyId,
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_id(&self) -> BlockId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        BlockId(hasher.hash())
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }
}

impl<ST, SCT, EPT> Debug for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusBlockHeader")
            .field("author", &self.author)
            .field("epoch", &self.epoch)
            .field("block_round", &self.block_round)
            .field("block_body_id", &self.block_body_id)
            .field("qc", &self.qc)
            .field("seq_num", &self.seq_num)
            .field("timestamp_ns", &self.timestamp_ns)
            .field("id", &self.get_id())
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        epoch: Epoch,
        block_round: Round,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        execution_inputs: EPT::ProposedHeader,
        block_body_id: ConsensusBlockBodyId,
        qc: QuorumCertificate<SCT>,
        seq_num: SeqNum,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
    ) -> Self {
        Self {
            author,
            epoch,
            block_round,
            delayed_execution_results,
            execution_inputs,
            block_body_id,
            qc,
            seq_num,
            timestamp_ns,
            round_signature,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BlockPolicyError {
    BlockNotCoherent,
    StateBackendError(StateBackendError),
    TimestampError,
    ExecutionResultMismatch,
}

impl From<StateBackendError> for BlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

/// Trait that represents how inner contents of a block should be validated
#[auto_impl(Box)]
pub trait BlockPolicy<ST, SCT, EPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend,
{
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + Debug
        + Send
        + Deref<Target = ConsensusFullBlock<ST, SCT, EPT>>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError>;

    fn get_expected_execution_results(
        &self,
        block_seq_num: SeqNum,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<Vec<EPT::FinalizedHeader>, StateBackendError>;

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn update_committed_block(&mut self, block: &Self::ValidatedBlock);

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn reset(&mut self, last_delay_committed_blocks: Vec<&Self::ValidatedBlock>);
}

/// A block policy which does not validate the inner contents of the block
#[derive(Copy, Clone, Default)]
pub struct PassthruBlockPolicy;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassthruWrappedBlock<ST, SCT, EPT>(pub ConsensusFullBlock<ST, SCT, EPT>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol;

impl<ST, SCT, EPT> Deref for PassthruWrappedBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Target = ConsensusFullBlock<ST, SCT, EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<ST, SCT, EPT> From<ConsensusFullBlock<ST, SCT, EPT>> for PassthruWrappedBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(block: ConsensusFullBlock<ST, SCT, EPT>) -> Self {
        Self(block)
    }
}

impl<ST, SCT, EPT> BlockPolicy<ST, SCT, EPT, InMemoryState> for PassthruBlockPolicy
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ValidatedBlock = PassthruWrappedBlock<ST, SCT, EPT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &InMemoryState,
    ) -> Result<(), BlockPolicyError> {
        // check coherency against the block being extended or against the root of the blocktree if
        // there is no extending branch
        let (extending_seq_num, extending_timestamp) =
            if let Some(extended_block) = extending_blocks.last() {
                (extended_block.get_seq_num(), extended_block.get_timestamp())
            } else {
                (blocktree_root.seq_num, 0) //TODO: add timestamp to RootInfo
            };

        if block.get_seq_num() != extending_seq_num + SeqNum(1) {
            return Err(BlockPolicyError::BlockNotCoherent);
        }

        if block.get_timestamp() <= extending_timestamp {
            // timestamps must be monotonically increasing
            return Err(BlockPolicyError::TimestampError);
        }

        let expected_execution_results = self.get_expected_execution_results(
            block.get_seq_num(),
            extending_blocks,
            state_backend,
        )?;
        if block.get_execution_results() != &expected_execution_results {
            return Err(BlockPolicyError::ExecutionResultMismatch);
        }

        Ok(())
    }

    fn get_expected_execution_results(
        &self,
        _block_seq_num: SeqNum,
        _extending_blocks: Vec<&Self::ValidatedBlock>,
        _state_backend: &InMemoryState,
    ) -> Result<Vec<EPT::FinalizedHeader>, StateBackendError> {
        Ok(Vec::new())
    }

    fn update_committed_block(&mut self, _: &Self::ValidatedBlock) {}
    fn reset(&mut self, _: Vec<&Self::ValidatedBlock>) {}
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    header: ConsensusBlockHeader<ST, SCT, EPT>,
    body: ConsensusBlockBody<EPT>,
}

impl<ST, SCT, EPT> PartialEq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn eq(&self, other: &Self) -> bool {
        self.header.get_id() == other.header.get_id()
    }
}
impl<ST, SCT, EPT> Eq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
}

impl<ST, SCT, EPT> ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        header: ConsensusBlockHeader<ST, SCT, EPT>,
        body: ConsensusBlockBody<EPT>,
    ) -> Result<Self, BlockValidationError> {
        if body.get_id() != header.block_body_id {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }
        Ok(Self { header, body })
    }

    pub fn header(&self) -> &ConsensusBlockHeader<ST, SCT, EPT> {
        &self.header
    }
    pub fn body(&self) -> &ConsensusBlockBody<EPT> {
        &self.body
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.header.qc.get_block_id()
    }
    pub fn get_id(&self) -> BlockId {
        self.header.get_id()
    }
    pub fn get_body_id(&self) -> ConsensusBlockBodyId {
        self.header.block_body_id
    }
    pub fn get_block_round(&self) -> Round {
        self.header.block_round
    }
    pub fn get_parent_round(&self) -> Round {
        self.header.qc.get_round()
    }
    pub fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.header.qc
    }
    pub fn get_epoch(&self) -> Epoch {
        self.header.epoch
    }
    pub fn get_seq_num(&self) -> SeqNum {
        self.header.seq_num
    }
    pub fn get_timestamp(&self) -> u128 {
        self.header.timestamp_ns
    }
    pub fn get_author(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
        &self.header.author
    }

    pub fn get_execution_results(&self) -> &Vec<EPT::FinalizedHeader> {
        &self.header.delayed_execution_results
    }

    pub fn split(self) -> (ConsensusBlockHeader<ST, SCT, EPT>, ConsensusBlockBody<EPT>) {
        (self.header, self.body)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct ProposedExecutionInputs<EPT>
where
    EPT: ExecutionProtocol,
{
    pub header: EPT::ProposedHeader,
    pub body: EPT::Body,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct MockExecutionProtocol {}

impl ExecutionProtocol for MockExecutionProtocol {
    type ProposedHeader = MockExecutionProposedHeader;
    type Body = MockExecutionBody;
    type FinalizedHeader = MockExecutionFinalizedHeader;
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct MockExecutionProposedHeader {}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct MockExecutionBody {
    pub data: Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct MockExecutionFinalizedHeader {
    number: SeqNum,
}
impl FinalizedHeader for MockExecutionFinalizedHeader {
    fn seq_num(&self) -> SeqNum {
        self.number
    }
}

// This type is one level "higher" than the OptimisticCommit type below in that this type retains
// the block policy's validated block type. This is useful when passing blocks to executors like the
// txpool which leverage information about the block body itself, which is only available at the
// block policy validated level, rather than using it in a "type abstracted" way like the ledger
// which uses the "Encodable" trait to simply write the bytes to a file without needing to inspect
// the body itself.
#[derive(Debug)]
pub enum OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    Proposed(BPT::ValidatedBlock),
    Finalized(BPT::ValidatedBlock),
}

#[derive(Debug)]
pub enum OptimisticCommit<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Proposed(ConsensusFullBlock<ST, SCT, EPT>),
    Finalized(ConsensusFullBlock<ST, SCT, EPT>),
}
impl<ST, SCT, EPT, BPT, SBT> From<&OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT>>
    for OptimisticCommit<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn from(value: &OptimisticPolicyCommit<ST, SCT, EPT, BPT, SBT>) -> Self {
        match value {
            OptimisticPolicyCommit::Proposed(block) => Self::Proposed(block.deref().to_owned()),
            OptimisticPolicyCommit::Finalized(block) => Self::Finalized(block.deref().to_owned()),
        }
    }
}
