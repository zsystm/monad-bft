use std::collections::BTreeMap;

use monad_consensus_types::{
    no_endorsement::NoEndorsementCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetType;

use crate::messages::message::NoEndorsementMessage;

#[derive(Debug, Default)]
pub struct RoundRecoveryState<SCT: SignatureCollection> {
    pending_no_endorsements: BTreeMap<NodeId<SCT::NodeIdPubKey>, NoEndorsementMessage<SCT>>,
}

impl<SCT: SignatureCollection> RoundRecoveryState<SCT> {
    pub fn new() -> Self {
        Self {
            pending_no_endorsements: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn process_no_endorsement_message<VST>(
        &mut self,
        validators: &VST,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
        author: NodeId<SCT::NodeIdPubKey>,
        no_endorsement_msg: NoEndorsementMessage<SCT>,
    ) -> Option<NoEndorsementCertificate<SCT>>
    where
        VST: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        self.pending_no_endorsements
            .insert(author, no_endorsement_msg.clone());

        let mut node_ids: Vec<NodeId<_>> = self.pending_no_endorsements.keys().copied().collect();

        let mut maybe_nec = None;
        while validators.has_super_majority_votes(&node_ids) {
            let no_endorsement_sig_tuple: Vec<_> = self
                .pending_no_endorsements
                .iter()
                .map(|(node_id, no_endorsement_msg)| {
                    (
                        *node_id,
                        no_endorsement_msg.no_endorsement,
                        no_endorsement_msg.sig,
                    )
                })
                .collect();
            match NoEndorsementCertificate::new(
                no_endorsement_msg.no_endorsement.round,
                no_endorsement_msg.no_endorsement.epoch,
                no_endorsement_sig_tuple.as_slice(),
                validator_mapping,
            ) {
                Ok(nec) => {
                    maybe_nec = Some(nec);
                    break;
                }
                Err(err) => match err {
                    SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs) => {
                        // remove invalid signatures and try to create NEC again
                        for (node_id, sig) in invalid_sigs {
                            let removed = self.pending_no_endorsements.remove(&node_id);
                            debug_assert_eq!(removed.expect("NoEndorsement removed").sig, sig);
                        }
                        // TODO: collect evidence of invalid signatures

                        node_ids = self.pending_no_endorsements.keys().copied().collect();
                    }
                    _ => {
                        unreachable!("unexpected error {}", err)
                    }
                },
            }
        }

        maybe_nec
    }
}
