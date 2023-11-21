use monad_consensus_types::signature_collection::SignatureCollection;

use crate::messages::message::{TimeoutMessage, VoteMessage};

/// Internally, individual nodes should never re-verify signature
/// but when sharing/sending evidence with other nodes,
/// original signature is used as a proof of violation
pub type SerializedSignature = Vec<u8>;
// vote message itself carries a SCT signature, sufficient for evidence
type ProofOfVote<SCT> = VoteMessage<SCT>;
type ProofOfTimeOut<SCT> = TimeoutMessage<SCT>;

/// The design of Evidence Interface is
/// Evidence(node_id, Violation{proof_a, proof_b, ...})
pub enum ConsensusViolation<SCT>
where
    SCT: SignatureCollection,
{
    InvalidVoteInfoHash(ProofOfVote<SCT>),
    InvalidVoteSignature(ProofOfVote<SCT>),
    InvalidTimeOutSignature(ProofOfTimeOut<SCT>),
}
