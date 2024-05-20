    use monad_consensus_types::signature_collection::SignatureCollection;
    use monad_consensus_types::timeout::TimeoutCertificate;
    use monad_consensus_types::validator_accountability::ValidatorAccountability;
    use monad_types::NodeId;
    use std::collections::HashMap;
    use monad_consensus_types::quorum_certificate::QuorumCertificate;


    use monad_crypto::certificate_signature::PubKey; 

   
 //Validator Monitor is used to keep track of the number of consecutive failures for each validator and the latest failure certificate for each validator. If a node receives blacklisting request as vector of ValidatorAccountability field in a block, it can use this information to verify if a validator should be blacklisted. If correctly validated, then a validator will vote for such a block and upon commit, the validator will be blacklisted.


  
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
        // reset_failure method is used to remove the validator from the validator_failures hashmap. This method is called when a validator while being a leader succesfully proposes a block and this function resets the failure count if  the leader's consecutive failures haven't reached the threshold value.
        pub fn reset_failure(&mut self, validator_id: NodeId<NodeIdPubKey>, threshold: u32) {
            if self.validator_failures.contains_key(&validator_id){
                if let Some(failure_count) = self.validator_failures.get_mut(&validator_id) {
                    if *failure_count < threshold {
                        self.validator_failures.remove(&validator_id);
                    } 
                }
            }
               }
        //subtract_threshold value from the failure count of the validator if the validator is blacklisted. If the failure count becomes zero, the validator is removed from the validator_failures hashmap.
        pub fn substract_threshold(&mut self, validator_id: NodeId<NodeIdPubKey>, threshold: u32) {
            if let Some(failure_count) = self.validator_failures.get_mut(&validator_id) {
                if *failure_count >= threshold {
                    *failure_count -= threshold;
                    if *failure_count == 0 {
                        self.validator_failures.remove(&validator_id);
                    }
                }
            }
        }
        pub fn check_threshold(&self, validator_id: &NodeId<NodeIdPubKey>, threshold: u32) -> bool {
            self.validator_failures.get(validator_id).copied().unwrap_or(0) >= threshold
        }

        //This function verifies if the blacklist request  is valid. It checks if the failure count of the validator is greater than or equal to the threshold and if the latest failure certificate and failure counter match the ones in the ValidatorAccountability struct.   
       
        pub fn verify_blacklist_request(
            &self,
            validator_id: &NodeId<NodeIdPubKey>,
            threshold: u32,
            validator_accountability: &ValidatorAccountability<NodeIdPubKey, SC>,
        ) -> bool {
            self.check_threshold(validator_id, threshold)
                && self.validator_latest_failure.get(validator_id) == Some(&validator_accountability.latest_failure_certificate)&& self.validator_failures.get(validator_id) == Some(&validator_accountability.failure_counter)
        }
    
        // This method generates a list of ValidatorAccountability based on a threshold
        pub fn from_validator_monitor(
            validator_monitor: &ValidatorMonitor<NodeIdPubKey, SC>,
            threshold: u32,
        ) -> Vec<ValidatorAccountability<NodeIdPubKey, SC>>
        where
            NodeIdPubKey: 'static,
        {
            validator_monitor
                .validator_failures
                .iter()
                .filter_map(|(validator_id, &failure_count)| {
                    println!("Validator ID: {:?}, Failure Count: {}", validator_id, failure_count);
                    if failure_count >= threshold {
                        println!("Validator ID {:?} exceeded threshold", validator_id);
                        validator_monitor
                            .validator_latest_failure
                            .get(validator_id)
                            .map(|latest_failure| {
                                println!(
                                    "Latest Failure Certificate for Validator ID {:?}: {:?}",
                                    validator_id, latest_failure
                                );
                                ValidatorAccountability {
                                    validator_id: *validator_id,
                                    failure_counter: failure_count,
                                    latest_failure_certificate: latest_failure.clone(),
                                }
                            })
                    } else {
                        println!("Validator ID {:?} did not exceed threshold", validator_id);
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

        
            assert!(
                monitor.validator_failures.contains_key(&node_id),
                "Node ID not found in failures"
            );
            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1);
        }

        #[test]
        fn test_record_and_reset_multiple_failures() {
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
                      
        
            let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
            let round = Round(1);
            let tc_1 = TimeoutCertificate::<SignatureCollectionType>::new(
                round,
                &[],
                &validator_mapping,
            ).expect("TimeoutCertificate creation failed");
            let round = Round(2);

            let tc_2 = TimeoutCertificate::<SignatureCollectionType>::new(
                round,
                &[],
                &validator_mapping,
            ).expect("TimeoutCertificate creation failed");
        
            // Record and reset failure logic
            monitor.record_failure(node_id.clone(), tc_1.clone());
            assert!(
                monitor.validator_failures.contains_key(&node_id),
                "Node ID not found in failures"
            );
            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1);

            monitor.record_failure(node_id.clone(), tc_2.clone());

            assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 2);   
            
        }
        
        #[test]
fn test_check_threshold() {
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
    
    let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
    let round_1 = Round(1);
    let round_2 = Round(2);
    
    let tc_1 = TimeoutCertificate::<SignatureCollectionType>::new(
        round_1,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");
    
    let tc_2 = TimeoutCertificate::<SignatureCollectionType>::new(
        round_2,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");

    // Record multiple failures
    monitor.record_failure(node_id.clone(), tc_1);
    monitor.record_failure(node_id.clone(), tc_2);

    // Test different thresholds
    assert!(monitor.check_threshold(&node_id, 1), "Threshold check failed for value 1");
    assert!(monitor.check_threshold(&node_id, 2), "Threshold check failed for value 2");
    assert!(!monitor.check_threshold(&node_id, 3), "Threshold check failed for value 3");
}
    

#[test]
fn test_subtract_threshold_from_failure_counter() {
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
    
    let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
    let round = Round(1);
    
    let tc = TimeoutCertificate::<SignatureCollectionType>::new(
        round,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");

    // Record failure
    monitor.record_failure(node_id.clone(), tc);
    assert!(monitor.validator_failures.contains_key(&node_id), "Node ID not found in failures");
    assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1, "Failure count mismatch");

    monitor.substract_threshold(node_id.clone(),1);
    assert!(!monitor.validator_failures.contains_key(&node_id), "Node ID not found in failures");

    
    }


    #[test]
// Test the `from_validator_monitor` method. It generates a list of ValidatorAccountability based on a threshold that will be included in the block as a blacklisting request.
fn test_from_validator_monitor() {
    // Instantiate a ValidatorSetFactory compatible with the required trait
    let validator_factory = ValidatorSetFactory::<NopPubKey>::default();
    
    // Generate keys and validators using the `create_keys_w_validators` function
    let (_, _, _, validator_mapping) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        ValidatorSetFactory<NopPubKey>,
    >(4, validator_factory);

    // Create a ValidatorMonitor and add some failures
    let mut monitor = ValidatorMonitor::<
        CertificateSignaturePubKey<SignatureType>,
        SignatureCollectionType,
    >::new();

    let node_id1 = NodeId::new(NopPubKey::new(Some([1; 32])));
    let node_id2 = NodeId::new(NopPubKey::new(Some([2; 32])));
    let round = Round(1);
    let tc1 = TimeoutCertificate::<SignatureCollectionType>::new(
        round,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");
    let round = Round(2);

    let tc2 = TimeoutCertificate::<SignatureCollectionType>::new(
        round,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");

    // Record some failures
    monitor.record_failure(node_id1.clone(), tc1.clone());
    monitor.record_failure(node_id1.clone(), tc2.clone());
    monitor.record_failure(node_id2.clone(), tc1.clone());

    // Set the threshold
    let threshold = 1;

    // Generate the ValidatorAccountability list
    let accountability_list = ValidatorMonitor::from_validator_monitor(&monitor, threshold);

    assert_eq!(accountability_list.len(), 2);
    assert!(accountability_list.iter().any(|va| va.validator_id == node_id1 && va.failure_counter == 2));
    assert!(accountability_list.iter().any(|va| va.validator_id == node_id2 && va.failure_counter == 1));

}
#[test]
fn test_verify_blacklist_request() {
    let validator_factory = ValidatorSetFactory::<NopPubKey>::default();
    
    // Generate keys and validators using the `create_keys_w_validators` function
    let (_, _, _, validator_mapping) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        ValidatorSetFactory<NopPubKey>,
    >(4, validator_factory);
    
    let mut monitor = ValidatorMonitor::<
        CertificateSignaturePubKey<SignatureType>,
        SignatureCollectionType,
    >::new();
    
    let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
    let round = Round(1);
    let tc = TimeoutCertificate::<SignatureCollectionType>::new(
        round,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");

    // Record a failure
    monitor.record_failure(node_id.clone(), tc.clone());

    // Create a ValidatorAccountability instance
    let validator_accountability = ValidatorAccountability {
        validator_id: node_id.clone(),
        failure_counter: 1,
        latest_failure_certificate: tc.clone(),
    };

    // Check if the blacklist request is verified correctly
    assert!(monitor.verify_blacklist_request(&node_id, 1, &validator_accountability));
    
    // Modify the ValidatorAccountability instance to make the verification fail
    let invalid_validator_accountability = ValidatorAccountability {
        validator_id: node_id.clone(),
        failure_counter: 2, // Changed the failure counter
        latest_failure_certificate: tc.clone(),
    };

    // Check if the blacklist request verification fails for modified ValidatorAccountability
    assert!(!monitor.verify_blacklist_request(&node_id, 1, &invalid_validator_accountability));
}

#[test]
 fn test_reset_failure_counter() {
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
    
    let node_id = NodeId::new(NopPubKey::new(Some([127; 32])));
    let round = Round(1);
    
    let tc = TimeoutCertificate::<SignatureCollectionType>::new(
        round,
        &[],
        &validator_mapping,
    ).expect("TimeoutCertificate creation failed");

    // Record failure
    monitor.record_failure(node_id.clone(), tc);
    assert!(monitor.validator_failures.contains_key(&node_id), "Node ID not found in failures");
    assert_eq!(*monitor.validator_failures.get(&node_id).unwrap(), 1, "Failure count mismatch");
    let threshold=2;
    monitor.reset_failure(node_id.clone(),2);
    assert!(!monitor.validator_failures.contains_key(&node_id), "Node ID not found in failures");

    
    }
}



    

