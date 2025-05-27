use std::collections::HashMap;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::{Epoch, NodeId, Round};

use crate::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsement {
    /// The epoch this message was generated in
    pub epoch: Epoch,

    /// The round this message was generated
    pub round: Round,

    /// The highest-round QC that the author of this message has seen
    pub high_qc_round: Round,
    // TODO add BlockId as well ?
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementSigCol<SCT> {
    pub no_endorsement: NoEndorsement,
    pub signatures: SCT,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementCertificate<SCT> {
    pub round: Round,
    pub epoch: Epoch,
    pub no_endorsement_sig_col: Vec<NoEndorsementSigCol<SCT>>,
}

impl<SCT> NoEndorsementCertificate<SCT> {
    pub fn get_round(&self) -> Round {
        self.round
    }

    pub fn get_epoch(&self) -> Epoch {
        self.epoch
    }
}

impl<SCT: SignatureCollection> NoEndorsementCertificate<SCT> {
    pub fn new(
        round: Round,
        epoch: Epoch,
        no_endorsement_sig_tuple: &[(
            NodeId<SCT::NodeIdPubKey>,
            NoEndorsement,
            SCT::SignatureType,
        )],
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Result<Self, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>> {
        let mut sigs = HashMap::new();
        for (node_id, no_endorsement, sig) in no_endorsement_sig_tuple {
            let entry = sigs.entry(*no_endorsement).or_insert(Vec::new());
            entry.push((*node_id, *sig));
        }

        let mut no_endorsement_sig_col = Vec::new();
        for (no_endorsement, sigs) in sigs.into_iter() {
            let msg = alloy_rlp::encode(no_endorsement);
            let signatures = SCT::new(sigs, validator_mapping, msg.as_ref())?;
            no_endorsement_sig_col.push(NoEndorsementSigCol {
                no_endorsement,
                signatures,
            });
        }

        Ok(Self {
            round,
            epoch,
            no_endorsement_sig_col,
        })
    }
}
