use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use monad_consensus_types::{
    block::{Block, BlockRange},
    payload::{Payload, PayloadId},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hashable, Hasher};

const BLOCK_SYNC_REQUEST_MESSAGE_NAME: &str = "BlockSyncRequestMessage";
const BLOCK_SYNC_RESPONSE_MESSAGE_NAME: &str = "BlockSyncResponseMessage";
const BLOCK_SYNC_HEADERS_RESPONSE_NAME: &str = "BlockSyncHeadersResponse";
const BLOCK_SYNC_PAYLOAD_RESPONSE_NAME: &str = "BlockSyncPayloadResponse";

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum BlockSyncRequestMessage {
    Headers(BlockRange),
    Payload(PayloadId),
}

impl Encodable for BlockSyncRequestMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_REQUEST_MESSAGE_NAME;
        match self {
            Self::Headers(b) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &b];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Payload(id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for BlockSyncRequestMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_REQUEST_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncRequestMessage",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Headers(BlockRange::decode(&mut payload)?)),
            2 => Ok(Self::Payload(PayloadId::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncRequestMessage",
            )),
        }
    }
}

impl Hashable for BlockSyncRequestMessage {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncHeadersResponse<SCT: SignatureCollection> {
    Found((BlockRange, Vec<Block<SCT>>)),
    NotAvailable(BlockRange),
}

impl<SCT: SignatureCollection> BlockSyncHeadersResponse<SCT> {
    pub fn get_block_range(&self) -> BlockRange {
        match self {
            BlockSyncHeadersResponse::Found((block_range, _)) => *block_range,
            BlockSyncHeadersResponse::NotAvailable(block_range) => *block_range,
        }
    }
}

impl<SCT: SignatureCollection> Encodable for BlockSyncHeadersResponse<SCT> {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_HEADERS_RESPONSE_NAME;
        match self {
            Self::Found((block_range, blocks)) => {
                let enc: [&dyn Encodable; 4] = [&name, &1u8, &block_range, &blocks];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(block_range) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &block_range];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<SCT: SignatureCollection> Decodable for BlockSyncHeadersResponse<SCT> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_HEADERS_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncHeaderResponse",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Found((
                BlockRange::decode(&mut payload)?,
                Vec::<_>::decode(&mut payload)?,
            ))),
            2 => Ok(Self::NotAvailable(BlockRange::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncHeadersResponse",
            )),
        }
    }
}

impl<SCT: SignatureCollection> Hashable for BlockSyncHeadersResponse<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncPayloadResponse {
    Found(Payload),
    NotAvailable(PayloadId),
}

impl BlockSyncPayloadResponse {
    pub fn get_payload_id(&self) -> PayloadId {
        match self {
            BlockSyncPayloadResponse::Found(payload) => payload.get_id(),
            BlockSyncPayloadResponse::NotAvailable(payload_id) => *payload_id,
        }
    }
}

impl Encodable for BlockSyncPayloadResponse {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_PAYLOAD_RESPONSE_NAME;
        match self {
            Self::Found(payload) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &payload];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(payload_id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &payload_id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for BlockSyncPayloadResponse {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_PAYLOAD_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncPayloadResponse",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Found(Payload::decode(&mut payload)?)),
            2 => Ok(Self::NotAvailable(PayloadId::decode(&mut payload)?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncPayloadResponse",
            )),
        }
    }
}

impl Hashable for BlockSyncPayloadResponse {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<SCT: SignatureCollection> {
    HeadersResponse(BlockSyncHeadersResponse<SCT>),
    PayloadResponse(BlockSyncPayloadResponse),
}

impl<SCT: SignatureCollection> BlockSyncResponseMessage<SCT> {
    pub fn found_headers(
        block_range: BlockRange,
        headers: Vec<Block<SCT>>,
    ) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::Found((
            block_range,
            headers,
        )))
    }

    pub fn headers_not_available(block_range: BlockRange) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(
            block_range,
        ))
    }

    pub fn found_payload(payload: Payload) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncPayloadResponse::Found(payload))
    }

    pub fn payload_not_available(payload_id: PayloadId) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncPayloadResponse::NotAvailable(
            payload_id,
        ))
    }
}

impl<SCT: SignatureCollection> Encodable for BlockSyncResponseMessage<SCT> {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_RESPONSE_MESSAGE_NAME;
        match self {
            Self::HeadersResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::PayloadResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<SCT: SignatureCollection> Decodable for BlockSyncResponseMessage<SCT> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_RESPONSE_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncResponseMessage",
            ));
        }
        match u8::decode(&mut payload)? {
            1 => Ok(Self::HeadersResponse(BlockSyncHeadersResponse::decode(
                &mut payload,
            )?)),
            2 => Ok(Self::PayloadResponse(BlockSyncPayloadResponse::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncResponseMessage",
            )),
        }
    }
}

impl<SCT: SignatureCollection> Hashable for BlockSyncResponseMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}
