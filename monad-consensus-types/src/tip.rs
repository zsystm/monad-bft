use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

use crate::{
    no_endorsement::NoEndorsementCertificate, payload::RoundSignature,
    quorum_certificate::QuorumCertificate, signature_collection::SignatureCollection,
};

#[derive(Debug, Clone, Eq, PartialEq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ConsensusTip<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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
    pub block_id: BlockId,

    pub nec: Option<NoEndorsementCertificate<SCT>>,
}
