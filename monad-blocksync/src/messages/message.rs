use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hashable, Hasher},
};
use monad_types::{EnumDiscriminant, ExecutionProtocol};
use zerocopy::AsBytes;

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum BlockSyncRequestMessage {
    Headers(BlockRange),
    Payload(ConsensusBlockBodyId),
}

impl Hashable for BlockSyncRequestMessage {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncRequestMessage::Headers(block_range) => {
                EnumDiscriminant(1).hash(state);
                block_range.hash(state);
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Found((BlockRange, Vec<ConsensusBlockHeader<ST, SCT, EPT>>)),
    NotAvailable(BlockRange),
}

impl<ST, SCT, EPT> BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_block_range(&self) -> BlockRange {
        match self {
            BlockSyncHeadersResponse::Found((block_range, _)) => *block_range,
            BlockSyncHeadersResponse::NotAvailable(block_range) => *block_range,
        }
    }
}

impl<ST, SCT, EPT> Hashable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncHeadersResponse::Found((block_range, blocks)) => {
                EnumDiscriminant(1).hash(state);
                block_range.hash(state);
                for block in blocks {
                    block.hash(state);
                }
            }
            BlockSyncHeadersResponse::NotAvailable(block_range) => {
                EnumDiscriminant(2).hash(state);
                block_range.hash(state);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    Found(ConsensusBlockBody<EPT>),
    NotAvailable(ConsensusBlockBodyId),
}

impl<EPT> BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn get_payload_id(&self) -> ConsensusBlockBodyId {
        match self {
            BlockSyncBodyResponse::Found(payload) => payload.get_id(),
            BlockSyncBodyResponse::NotAvailable(payload_id) => *payload_id,
        }
    }
}

impl<EPT> Hashable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            Self::Found(payload) => {
                EnumDiscriminant(1).hash(state);
                let encoded_payload = alloy_rlp::encode(payload);
                state.update(&encoded_payload);
            }
            Self::NotAvailable(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    HeadersResponse(BlockSyncHeadersResponse<ST, SCT, EPT>),
    PayloadResponse(BlockSyncBodyResponse<EPT>),
}

impl<ST, SCT, EPT> BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn found_headers(
        block_range: BlockRange,
        headers: Vec<ConsensusBlockHeader<ST, SCT, EPT>>,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::Found((
            block_range,
            headers,
        )))
    }

    pub fn headers_not_available(
        block_range: BlockRange,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(
            block_range,
        ))
    }

    pub fn found_payload(body: ConsensusBlockBody<EPT>) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::Found(body))
    }

    pub fn payload_not_available(
        block_body_id: ConsensusBlockBodyId,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::NotAvailable(
            block_body_id,
        ))
    }
}

impl<ST, SCT, EPT> Hashable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                EnumDiscriminant(1).hash(state);
                headers_response.hash(state);
            }
            BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                EnumDiscriminant(2).hash(state);
                payload_response.hash(state)
            }
        }
    }
}
