use monad_consensus_types::signature_collection::SignatureCollection;
use monad_consensus_types::timeout::TimeoutCertificate;
use monad_types::NodeId;
use std::collections::HashMap;

pub struct ValidatorMonitor<SCT: SignatureCollection> {
    validator_failures: HashMap<NodeId<SCT::NodeIdPubKey>, u32>,
    // Now including the specific SignatureCollection type for TimeoutCertificate
    validator_latest_failure: HashMap<NodeId<SCT::NodeIdPubKey>, TimeoutCertificate<SCT>>,
}

impl<SCT: SignatureCollection> ValidatorMonitor<SCT> {
    pub fn new() -> Self {
        Self {
            validator_failures: HashMap::new(),
            validator_latest_failure: HashMap::new(),
        }
    }
    //FIXME: Before calling this function make sure that the validator is the leader for the same round in which the timeout certificate is generated.
    pub fn record_failure(
        &mut self,
        validator_id: NodeId<SCT::NodeIdPubKey>,
        timeout_certificate: TimeoutCertificate<SCT>,
    ) {
        let mut update_failure_count = false;

        // Use a reference to `timeout_certificate` in `and_modify` to avoid moving it.
        self.validator_latest_failure
            .entry(validator_id)
            .and_modify(|existing_certificate| {
                if timeout_certificate.round > existing_certificate.round {
                    *existing_certificate = timeout_certificate.clone(); // Clone here if the round is greater.
                    update_failure_count = true; // We'll update the failure count because the round is greater.
                }
            })
            .or_insert_with(|| timeout_certificate.clone()); // Clone here for insertion if needed.

        if update_failure_count {
            let failures = self.validator_failures.entry(validator_id).or_insert(0);
            *failures += 1;
        }
    }

    pub fn reset_failure(&mut self, validator_id: NodeId<SCT::NodeIdPubKey>) {
        self.validator_failures.insert(validator_id, 0);
    }

    pub fn check_threshold(
        &self,
        validator_id: &NodeId<SCT::NodeIdPubKey>,
        threshold: u32,
    ) -> bool {
        self.validator_failures.get(validator_id).unwrap_or(&0) >= &threshold
    }
}
