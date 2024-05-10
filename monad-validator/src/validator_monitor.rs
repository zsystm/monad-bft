    use monad_consensus_types::signature_collection::SignatureCollection;
    use monad_consensus_types::timeout::TimeoutCertificate;
    use monad_consensus_types::validator_accountability::ValidatorAccountability;
    use monad_types::NodeId;
    use std::collections::HashMap;
    use monad_consensus_types::quorum_certificate::QuorumCertificate;


    use monad_crypto::certificate_signature::PubKey; 

   
 

  
    pub struct ValidatorMonitor<NodeIdPubKey,SC>
    where
        NodeIdPubKey: PubKey,
        SC: SignatureCollection,
    {
        validator_failures: HashMap<NodeId<NodeIdPubKey>, u32>,
        validator_latest_failure: HashMap<NodeId<NodeIdPubKey>, TimeoutCertificate<SC>>,
    }
    
    impl<NodeIdPubKey, SC> ValidatorMonitor<NodeIdPubKey, SC>
    where
        NodeIdPubKey: PubKey,
        SC: SignatureCollection,
    {
        pub fn new() -> Self {
            Self {
                validator_failures: HashMap::new(),
                validator_latest_failure: HashMap::new(),
            }
        }
    
        
    
        pub fn record_failure(
            &mut self,
            validator_id: NodeId<NodeIdPubKey>,
            timeout_certificate: TimeoutCertificate<SC>,
        ) {
            println!("Recording failure for Validator ID: {:?}", validator_id);
            println!("Timeout Certificate Round: {:?}", timeout_certificate.round);
        
            let mut update_failure_count = false;
        
            self.validator_latest_failure
                .entry(validator_id)
                .and_modify(|existing_certificate| {
                    println!("Existing Certificate Round: {:?}", existing_certificate.round);
                    if timeout_certificate.round > existing_certificate.round {
                        println!("Updating to new round: {:?}", timeout_certificate.round);
                        *existing_certificate = timeout_certificate.clone();
                        update_failure_count = true;
                    }
                })
                .or_insert_with(|| {
                    println!("Inserting new certificate for the first time");
                    timeout_certificate.clone()
                });
        
            if update_failure_count {
                *self.validator_failures.entry(validator_id).or_insert(0) += 1;
            } else {
                println!("Setting initial failure count for {:?}", validator_id);
                self.validator_failures.insert(validator_id, 1);
            }
        
            println!("Updated Validator Failures: {:?}", self.validator_failures);
        }
        
    
        pub fn reset_failure(&mut self, validator_id: NodeId<NodeIdPubKey>) {
            self.validator_failures.insert(validator_id, 0);
        }
    
        pub fn check_threshold(&self, validator_id: &NodeId<NodeIdPubKey>, threshold: u32) -> bool {
            self.validator_failures.get(validator_id).copied().unwrap_or(0) >= threshold
        }
    
        // This method generates a list of ValidatorAccountability based on a threshold
        pub fn from_validator_monitor(
            validator_monitor: &ValidatorMonitor<NodeIdPubKey,SC>,
            threshold: u32,
        ) -> Vec<ValidatorAccountability<NodeIdPubKey,  SC>>
        where
            NodeIdPubKey: 'static,
        {
            validator_monitor
                .validator_failures
                .iter()
                .filter_map(|(validator_id, &failure_count)| {
                    if failure_count > threshold {
                        validator_monitor
                            .validator_latest_failure
                            .get(validator_id)
                            .map(|latest_failure| ValidatorAccountability {
                                validator_id: *validator_id,
                                failure_counter: failure_count,
                                latest_failure_certificate: latest_failure.clone(),
                            })
                    } else {
                        None
                    }
                })
                .collect()
        }
    
    }


    #[cfg(test)]
    mod tests {
       // use monad_validator::validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory};
        use monad_consensus_types::{
            signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
            voting::ValidatorMapping,
            quorum_certificate::QuorumCertificate,
        };
  //  use crate::validator_set::ValidatorSetFactory;
    use monad_consensus_types::timeout::{TimeoutCertificate, TimeoutInfo};
    use monad_crypto::certificate_signature::{CertificateKeyPair, CertificateSignature};
    use monad_crypto::NopPubKey;
    use monad_crypto::NopSignature;
    use crate::{
        validator_set::ValidatorSetFactory,
        test_utils::create_keys_w_validators, // Use the locally copied function
    };

    use monad_testutil::signing::MockSignatures;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_types::{NodeId, Round};
  //  use monad_testutil::validators::create_keys_w_validators;
    use super::*;


        // Mock Validator Monitor Tests
        type SignatureType = NopSignature;
        type SignatureCollectionType = MockSignatures<SignatureType>;
    
     
        
        #[test]
        fn test_record_and_reset_failures() {
            // Instantiate a ValidatorSetFactory compatible with the required trait
            let validator_factory = ValidatorSetFactory::<NopPubKey>::default();
            
            // Generate keys and validators using the `create_keys_w_validators` function
            let (_, _, _, validator_mapping) = create_keys_w_validators::<
                SignatureType,
                SignatureCollectionType,
                ValidatorSetFactory<NopPubKey>,
            >(4, validator_factory);
        
            // Proceeding to use this mapping in `ValidatorMonitor` tests
            let mut monitor = ValidatorMonitor::<
                CertificateSignaturePubKey<SignatureType>,
                SignatureCollectionType,
            >::new();
            
            // Log each individual entry in the validator mapping
            for (node_id, key_pair) in validator_mapping.map.iter() {
                println!("Node ID: {:?}, Key Pair: {:?}", node_id, key_pair);
            }
        
            let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
            let round = Round(1);
            let tc = TimeoutCertificate::<SignatureCollectionType>::new(
                round,
                &[],
                &validator_mapping,
            ).expect("TimeoutCertificate creation failed");
        
            // Record and reset failure logic
            monitor.record_failure(node_id.clone(), tc.clone());
            println!("Timeout Certificate round: {:?}", tc.round);
            println!("Validator Failures: {:?}", monitor.validator_failures);
        
            assert!(
                monitor.validator_failures.contains_key(&node_id),
                "Node ID not found in failures"
            );
            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1);
        }
        
    }

/* 
    #[test]
    fn test_check_threshold() {
        let mut monitor = ValidatorMonitor::<NopPubKey,MockSignatureCollection>::new();
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
    */

    

