use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::{Epoch, Round};

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementMessage {
    /// The epoch this message was generated in
    pub epoch: Epoch,

    /// The round this message was generated
    pub round: Round,

    /// The highest-round QC that the author of this message has seen
    pub high_qc_round: Round,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementCertificate<SCT> {
    pub msg: NoEndorsementMessage,
    pub signatures: SCT,
}
