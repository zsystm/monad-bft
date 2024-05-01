    use monad_consensus_types::signature_collection::SignatureCollection;
    use monad_consensus_types::timeout::TimeoutCertificate;
    use monad_consensus_types::validator_accountability::ValidatorAccountability;
    use monad_types::NodeId;
    use std::collections::HashMap;
    // Add the missing `monad_testutil` crate to the project's dependencies


    use monad_crypto::certificate_signature::PubKey; 

    pub struct ValidatorMonitor<NodeIdPubKey>
    where
        NodeIdPubKey: PubKey, // Add the trait bound
    {
        validator_failures: HashMap<NodeId<NodeIdPubKey>, u32>,
        validator_latest_failure: HashMap<NodeId<NodeIdPubKey>, TimeoutCertificate>, 
    }

    impl <NodeIdPubKey: PubKey> ValidatorMonitor<NodeIdPubKey> {
        pub fn new() -> Self {
            Self {
                validator_failures: HashMap::new(),
                validator_latest_failure: HashMap::new(),
            }
        }

        pub fn record_failure(
            &mut self,
            validator_id: NodeId<NodeIdPubKey>,
            timeout_certificate: TimeoutCertificate<NodeIdPubKey>,
        ) {
        let mut update_failure_count = false;
        self.validator_latest_failure
            .entry(validator_id)
            .and_modify(|existing_certificate| {
                if timeout_certificate.round > existing_certificate.round {
                    *existing_certificate = timeout_certificate.clone();
                    update_failure_count = true;
                }
            })
            .or_insert_with(|| timeout_certificate.clone()); 

        if update_failure_count {
            *self.validator_failures.entry(validator_id).or_insert(0) += 1;
        }
    }

    pub fn reset_failure(&mut self, validator_id: NodeId<NodeIdPubKey>) {
        self.validator_failures.insert(validator_id, 0);
    }

    pub fn check_threshold(&self, validator_id: &NodeId<NodeIdPubKey>, threshold: u32) -> bool {
        self.validator_failures.get(validator_id).unwrap_or(&0) >= &threshold
    }
     // This method generates a list of ValidatorAccountability based on a threshold
        // Make sure you understand what each associated type's role is
        pub fn from_validator_monitor(
            validator_monitor: &ValidatorMonitor<NodeIdPubKey>,
            threshold: u32,
        ) -> Vec<ValidatorAccountability<NodeIdPubKey>>
       
        {
            validator_monitor.validator_failures.iter().filter_map(|(validator_id, &failure_count)| {
                if failure_count > threshold {
                    validator_monitor.validator_latest_failure.get(validator_id).map(|latest_failure| {
                        ValidatorAccountability {
                            validator_id: *validator_id,
                            failure_counter: failure_count,
                            latest_failure_certificate: latest_failure.clone(),
                        }
                    })
                } else {
                    None
                }
            }).collect()
        }
       
  
     
}

#[cfg(test)]
mod tests {
    use super::*;
    use monad_testutil::mock_signature_collection::{MockSignatureCollection, MockSignatures, create_timeout_certificate};

    fn mock_validator_mapping<SCT: SignatureCollection>() -> ValidatorMapping<SCT::NodeIdPubKey, SCT::SignatureType> {
        ValidatorMapping::new(vec![(NodeId::new(NopPubKey::new(None)), NopSignature::new())])
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature, PubKey};
        use monad_testutil::mock_signature_collection::{create_timeout_certificate, MockSignatureCollection, MockSignatures};
        use monad_types::NodeId;
    
        fn mock_validator_mapping<SCT: SignatureCollection>() -> ValidatorMapping<SCT::NodeIdPubKey, SCT::SignatureType> {
            ValidatorMapping::new(vec![(NodeId::new(NopPubKey::new(None)), NopSignature::new())])
        }
    
        #[test]
        fn test_record_and_reset_failures() {
            let mut monitor = ValidatorMonitor::<NopPubKey>::new();
            let node_id = NodeId::new(NopPubKey::new(None));
    
            let round = Round(1);
            let validator_mapping = mock_validator_mapping::<MockSignatureCollection<NopSignature>>();
    
            let tc = create_timeout_certificate(round, &validator_mapping).unwrap();
    
            monitor.record_failure(node_id.clone(), tc);
            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1);
    
            monitor.reset_failure(node_id.clone());
            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 0);
        }
    
        #[test]
        fn test_check_threshold() {
            let mut monitor = ValidatorMonitor::<NopPubKey>::new();
            let node_id = NodeId::new(NopPubKey::new(None));
    
            assert!(!monitor.check_threshold(&node_id, 1));
    
            let round = Round(1);
            let validator_mapping = mock_validator_mapping::<MockSignatureCollection<NopSignature>>();
    
            let tc1 = create_timeout_certificate(round, &validator_mapping).unwrap();
            let tc2 = create_timeout_certificate(Round(2), &validator_mapping).unwrap();
    
            monitor.record_failure(node_id.clone(), tc1);
            monitor.record_failure(node_id.clone(), tc2);
    
            assert!(monitor.check_threshold(&node_id, 1));
            assert!(monitor.check_threshold(&node_id, 2));
            assert!(!monitor.check_threshold(&node_id, 3));
        }
    }
    

  