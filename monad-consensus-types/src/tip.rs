use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, ExecutionProtocol, NodeId, Round, SeqNum};

use crate::{
    no_endorsement::NoEndorsementCertificate,
    payload::{ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

#[derive(Debug, Clone, Eq, PartialEq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ConsensusTip<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// round this block was proposed in
    pub round: Round,
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

    pub block_id: BlockId,
    pub block_body_id: ConsensusBlockBodyId,

    // TODO add consensus header delayed execution stuff
    //
    //
    pub nec: Option<NoEndorsementCertificate<SCT>>,
}
