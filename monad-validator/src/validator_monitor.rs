use monad_consensus_types::signature_collection::SignatureCollection;
use monad_consensus_types::timeout::TimeoutCertificate;
use monad_consensus_types::validator_accountability::ValidatorAccountability;
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

impl<SCT: SignatureCollection> ValidatorMonitor<SCT> {
    pub fn from_validator_monitor(
        validator_monitor: &ValidatorMonitor<SCT>,
        threshold: u32,
    ) -> Vec<ValidatorAccountability<SCT>> {
        use monad_consensus_types::validator_accountability::ValidatorAccountability;

        let mut accountability_vec = Vec::new();

        for (validator_id, failure_count) in &validator_monitor.validator_failures {
            if *failure_count > threshold {
                if let Some(cert) = validator_monitor.validator_latest_failure.get(validator_id) {
                    accountability_vec.push(ValidatorAccountability {
                        validator_id: *validator_id,
                        failure_counter: *failure_count,
                        latest_failure_certificate: cert.clone(),
                    });
                }
            }
        }

        accountability_vec
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use monad_consensus_types::{
        signature_collection::{
            SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        },
        timeout::{HighQcRound, HighQcRoundSigColTuple, TimeoutCertificate, TimeoutInfo},
        voting::ValidatorMapping,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature, DummySignature},
        hasher::{Hash, Hashable, Hasher},
    };
    use monad_types::{NodeId, Round};
    use std::collections::HashSet;

    #[derive(Clone, Debug)]
    struct TestSignatureCollection; // Implementing SignatureCollection for testing

    impl SignatureCollection for TestSignatureCollection {
        type NodeIdPubKey = u32; // Using a simple integer for NodeIdPubKey for testing
        type SignatureType = Self; // Using Self for simplicity

        fn new(
            _sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
            _validator_mapping: &ValidatorMapping<
                Self::NodeIdPubKey,
                SignatureCollectionKeyPairType<Self>,
            >,
            _msg: &[u8],
        ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>
        {
            Ok(TestSignatureCollection) // Always succeed in creating a signature collection for testing
        }

        fn get_hash(&self) -> Hash {
            Hash([0; 32]) // Dummy hash for simplicity
        }

        fn verify(
            &self,
            _validator_mapping: &ValidatorMapping<
                Self::NodeIdPubKey,
                SignatureCollectionKeyPairType<Self>,
            >,
            _msg: &[u8],
        ) -> Result<
            Vec<NodeId<Self::NodeIdPubKey>>,
            SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>,
        > {
            Ok(vec![]) // Always succeed for testing
        }

        fn get_participants(
            &self,
            _validator_mapping: &ValidatorMapping<
                Self::NodeIdPubKey,
                SignatureCollectionKeyPairType<Self>,
            >,
            _msg: &[u8],
        ) -> HashSet<NodeId<Self::NodeIdPubKey>> {
            HashSet::new() // Empty set for simplicity
        }

        fn num_signatures(&self) -> usize {
            0 // Return 0 for simplicity
        }

        fn serialize(&self) -> Vec<u8> {
            vec![] // Empty vector for simplicity
        }

        fn deserialize(
            _data: &[u8],
        ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>
        {
            Ok(TestSignatureCollection) // Always succeed for testing
        }
    }

    // Helper function to create a mock TimeoutCertificate
    fn mock_timeout_certificate(round: Round) -> TimeoutCertificate<TestSignatureCollection> {
        TimeoutCertificate {
            round,
            high_qc_rounds: vec![HighQcRoundSigColTuple {
                high_qc_round: HighQcRound { qc_round: Round(0) },
                sigs: TestSignatureCollection,
            }],
        }
    }

    #[test]
    fn test_record_and_reset_failures() {
        let mut monitor = ValidatorMonitor::<TestSignatureCollection>::new();
        let node_id = NodeId(1); // Simplified NodeId for testing

        // Record a failure for the first time
        monitor.record_failure(node_id, mock_timeout_certificate(Round(1)));
        assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1);

        // Record another failure for the same node
        monitor.record_failure(node_id, mock_timeout_certificate(Round(2)));
        assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 2);

        // Reset failures for the node
        monitor.reset_failure(node_id);
        assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 0);
    }

    #[test]
    fn test_check_threshold() {
        let mut monitor = ValidatorMonitor::<TestSignatureCollection>::new();
        let node_id = NodeId(1);

        // No failures recorded yet, should be below any positive threshold
        assert!(!monitor.check_threshold(&node_id, 1));

        // Record some failures
        monitor.record_failure(node_id, mock_timeout_certificate(Round(1)));
        monitor.record_failure(node_id, mock_timeout_certificate(Round(2)));

        // Check against a threshold
        assert!(monitor.check_threshold(&node_id, 1)); // Should be true, threshold is 1 and we have 2 failures
        assert!(monitor.check_threshold(&node_id, 2)); // Should be true, threshold is 2 and we have 2 failures
        assert!(!monitor.check_threshold(&node_id, 3)); // Should be false, threshold is 3 and we have only 2 failures
    }
}
