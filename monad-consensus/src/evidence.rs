use monad_consensus_types::signature_collection::SignatureCollection;

use crate::messages::message::VoteMessage;

/// In order to decouple message signature, ConsensusViolation
/// stores the message signature as bytes.
/// Internally, individual nodes should never re-verify signature
/// but when sharing/sending evidence with other nodes,
/// original signature on the violation must be retained.
pub type SerializedSignature = Vec<u8>;
type ProofOfVote<SCT> = (VoteMessage<SCT>, SerializedSignature);

/// The design of Evidence Interface is
/// Evidence(Violation[proof_a, proof_b, ...])
pub enum ConsensusViolation<SCT>
where
    SCT: SignatureCollection,
{
    InvalidVoteInfoHash(ProofOfVote<SCT>),
}
