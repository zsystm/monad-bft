use monad_consensus_types::signature_collection::SignatureCollection;
use monad_consensus_types::timeout::TimeoutCertificate;
use monad_consensus_types::validator_accountability::ValidatorAccountability;
use monad_types::NodeId;
use std::collections::HashMap;
// Add the missing `monad_testutil` crate to the project's dependencies


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
            .or_insert_with(|| timeout_certificate.clone()); 

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
    use monad_crypto::{NopSignature, NopPubKey,NopKeyPair};
    use monad_types::NodeId;
    use std::collections::HashSet;
    use monad_testutil::mock_signature_collection::MockSignatureCollection;
    use monad_testutil::mock_signature_collection::MockSignatures;
    use monad_consensus_types::{
        block::Block, quorum_certificate::QuorumCertificate, signature_collection::{
            SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
        }, timeout::{TimeoutCertificate, TimeoutInfo}, voting::ValidatorMapping
    };

    #[test]
    fn test_signature_collection() {
        // Setup a NodeId using NopPubKey with default initialization
        let node_id = NodeId::new(NopPubKey::new(None));  // Using NopPubKey with optional default
    
        let signature = NopSignature::new(); // Assume this creates a valid NopSignature
    
        // Prepare signatures for MockSignatures
        let signatures = vec![(node_id.clone(), signature)];
        let mock_signatures = MockSignatures::with_pubkeys(&signatures.iter().map(|(node, _)| node.pubkey()).collect::<Vec<_>>());
    
        // Now create MockSignatureCollection using the MockSignatures instance
        let mock_collection = MockSignatureCollection::<NopSignature>::new(mock_signatures);
    
        // Assuming you need to prepare a ValidatorMapping and a message for get_participants
        let validator_mapping = ValidatorMapping::<NopPubKey, NopKeyPair>::new(Vec::<(NodeId<NopPubKey>, NopPubKey)>::new());
        let message = &[]; // Example empty message

        // Now, verify some behavior.
        assert_eq!(mock_collection.num_signatures(), 1, "There should be exactly one signature in the collection.");
    
        // Now using get_participants correctly
        let participants: HashSet<_> = mock_collection.get_participants(&validator_mapping, message);
        assert!(participants.contains(&node_id), "The collection should contain the node ID.");

        // Other assertions can test serialization, deserialization, hashing, etc.
    }
}
/* 
#[cfg(test)]
mod tests {

    use monad_consensus_types::signature_collection;
    use monad_consensus_types::voting::ValidatorMapping;
    //use monad_consensus_types::{HighQcRound, HighQcRoundSigColTuple, NodeId, Round, SignatureCollection};
    use monad_crypto::{NopPubKey, NopSignature};
    use monad_crypto::certificate_signature::CertificateKeyPair;
    use monad_multi_sig::MultiSig;
    use monad_types::Round;
  //  use monad_testutil::{
    //    signing::{get_certificate_key, get_key},
       // validators::create_keys_w_validators,
    //};   
    use monad_testutil::signing::create_keys; 
    use monad_types::NodeId;
    use monad_consensus_types::quorum_certificate::QuorumCertificate;
    use monad_consensus_types::timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo};
    use super::*;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use std::marker::PhantomData;
    
    use monad_consensus_types::{
        signature_collection::{SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType},
        voting::ValidatorMapping,
    };
    use monad_crypto::hasher::{Hash, Hashable, Hasher};
    use monad_types::NodeId;
    
    pub struct NoOpTimeoutCertificate<S> {
        round: Round,
        _phantom: PhantomData<S>,
    }
    
    impl<S> NoOpTimeoutCertificate<S> {
        pub fn new(round: Round) -> Self {
            Self {
                round,
                _phantom: PhantomData,
            }
        }
    }
    
    impl<S> Default for NoOpTimeoutCertificate<S> {
        fn default() -> Self {
            Self::new(Round(0))
        }
    }
    
    impl<S: SignatureCollection> Hashable for NoOpTimeoutCertificate<S> {
        fn hash(&self, state: &mut impl Hasher) {
            self.round.hash(state);
        }
    }
    
    impl<S: SignatureCollection> NoOpTimeoutCertificate<S> {
        pub fn max_round(&self) -> Round {
            self.round
        }
    }
    



    #[test]
fn test_record_and_reset_failures() {
    let num_keys = 1;
    let keys = create_keys::<NopSignature>(num_keys);
    let node_id = NodeId::new(keys[0].pubkey());
    let no_op_timeout_certificate = NoOpTimeoutCertificate::new(Round(1));

    let mut monitor: ValidatorMonitor<dyn SignatureCollection> = ValidatorMonitor::<dyn SignatureCollection>::new();
    monitor.record_failure(node_id.clone(), no_op_timeout_certificate.clone());

    monitor.reset_failure(node_id.clone());
    assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 0);
}

#[test]
fn test_check_threshold() {
// Replace the trait object with a concrete type
    let mut monitor: ValidatorMonitor<ConcreteSignatureCollectionType> = ValidatorMonitor::<ConcreteSignatureCollectionType>::new();
    let keys = create_keys::<NopSignature>(1);
    let node_id = NodeId::new(keys[0].pubkey());

    assert!(!monitor.check_threshold(&node_id, 1));

    let no_op_timeout_certificate_1 = NoOpTimeoutCertificate::new(Round(1));
    let no_op_timeout_certificate_2 = NoOpTimeoutCertificate::new(Round(2));

    monitor.record_failure(node_id.clone(), no_op_timeout_certificate_1.clone());
    monitor.record_failure(node_id.clone(), no_op_timeout_certificate_2.clone());

    assert!(monitor.check_threshold(&node_id, 1));
    assert!(monitor.check_threshold(&node_id, 2));
    assert!(!monitor.check_threshold(&node_id, 3));
}

}
*/

