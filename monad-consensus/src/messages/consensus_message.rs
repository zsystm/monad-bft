use std::fmt::Debug;

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hashable, Hasher},
};
use monad_types::Round;

use crate::{
    messages::message::{ProposalMessage, TimeoutMessage, VoteMessage},
    validation::signing::{Validated, Verified},
};

const PROTOCOL_MESSAGE_NAME: &str = "ProtocolMessage";

/// Consensus protocol messages
#[derive(Clone, PartialEq, Eq)]
pub enum ProtocolMessage<SCT: SignatureCollection> {
    /// Consensus protocol proposal message
    Proposal(ProposalMessage<SCT>),

    /// Consensus protocol vote message
    Vote(VoteMessage<SCT>),

    /// Consensus protocol timeout message
    Timeout(TimeoutMessage<SCT>),
}

impl<SCT: Debug + SignatureCollection> Debug for ProtocolMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ProtocolMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ProtocolMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
        }
    }
}

/// Integrity hash
impl<SCT> Hashable for ProtocolMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

// FIXME-2:
// it can be confusing as we are hashing only part of the message
// in the signature refactoring, we might want a clean split between:
//      integrity sig: sign over the entire serialized struct
//      protocol sig: signatures outlined in the protocol
impl<SCT> Encodable for ProtocolMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let name = PROTOCOL_MESSAGE_NAME;
        match self {
            ProtocolMessage::Proposal(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Vote(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Timeout(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &3u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<SCT> Decodable for ProtocolMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != PROTOCOL_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type ProtocolMessage",
            ));
        }

        match u8::decode(&mut payload)? {
            1 => Ok(ProtocolMessage::Proposal(ProposalMessage::decode(
                &mut payload,
            )?)),
            2 => Ok(ProtocolMessage::Vote(VoteMessage::decode(&mut payload)?)),
            3 => Ok(ProtocolMessage::Timeout(TimeoutMessage::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown ProtocolMessage",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ConsensusMessage<SCT: SignatureCollection> {
    pub version: u32,
    pub message: ProtocolMessage<SCT>,
}

impl<SCT> Hashable for ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl<SCT> ConsensusMessage<SCT>
where
    SCT: SignatureCollection,
{
    pub fn sign<ST>(
        self,
        keypair: &ST::KeyPairType,
    ) -> Verified<ST, Validated<ConsensusMessage<SCT>>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        Verified::new(Validated::new(self), keypair)
    }

    pub fn get_round(&self) -> Round {
        match &self.message {
            ProtocolMessage::Proposal(p) => p.block.round,
            ProtocolMessage::Vote(v) => v.vote.vote_info.round,
            ProtocolMessage::Timeout(t) => t.timeout.tminfo.round,
        }
    }
}
