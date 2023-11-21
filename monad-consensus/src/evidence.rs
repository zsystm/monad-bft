use monad_consensus_types::{signature_collection::SignatureCollection, validation::Error};

use crate::messages::{
    consensus_message::ConsensusMessage,
    message::{TimeoutMessage, VoteMessage},
};

/// Internally, individual nodes should never re-verify signature
/// but when sharing/sending evidence with other nodes,
/// original signature is used as a proof of violation
pub type SerializedSignature = Vec<u8>;
// vote message itself carries a SCT signature, sufficient for evidence
type ProofOfVote<SCT> = VoteMessage<SCT>;
type ProofOfTimeout<SCT> = TimeoutMessage<SCT>;
type ProofOfMessage<SCT> = (ConsensusMessage<SCT>, SerializedSignature);
/// The design of Evidence Interface is
/// Evidence(node_id, Violation{proof_a, proof_b, ...})
pub enum ConsensusViolation<SCT>
where
    SCT: SignatureCollection,
{
    // consensus state related violation
    InvalidVoteInfoHash(ProofOfVote<SCT>),
    InvalidVoteSignature(ProofOfVote<SCT>),
    InvalidTimeoutSignature(ProofOfTimeout<SCT>),
    // monad state related violation
    FailedMessageVerification(Error, ProofOfMessage<SCT>),
}
