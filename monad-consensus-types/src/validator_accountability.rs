use monad_types::{NodeId, Round};
use crate::{signature_collection::SignatureCollection, timeout::TimeoutCertificate};
#[derive(Clone)] 

pub struct ValidatorAccountability<SCT: SignatureCollection> {
    pub validator_id: NodeId<SCT::NodeIdPubKey>,
    pub failure_counter: Round,
    pub latest_failure_certificate: TimeoutCertificate<SCT>,
}
