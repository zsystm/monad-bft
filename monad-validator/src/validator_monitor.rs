    use monad_consensus_types::signature_collection::SignatureCollection;
    use monad_consensus_types::timeout::TimeoutCertificate;
    use monad_consensus_types::validator_accountability::ValidatorAccountability;
    use monad_types::NodeId;
    use std::collections::HashMap;
    // Add the missing `monad_testutil` crate to the project's dependencies


    pub struct ValidatorMonitor<SCT: SignatureCollection> {
    validator_failures: HashMap<NodeId<SCT::NodeIdPubKey>, u32>,
    validator_latest_failure: HashMap<NodeId<SCT::NodeIdPubKey>, TimeoutCertificate<SCT>>,
}

impl<SCT: SignatureCollection> ValidatorMonitor<SCT>
where
    SCT: SignatureCollection,
    SCT::NodeIdPubKey: SignatureCollection,
{
    pub fn new() -> Self {
        Self {
            validator_failures: HashMap::new(),
            validator_latest_failure: HashMap::new(),
        }
    }

    pub fn record_failure(
        &mut self,
        validator_id: NodeId<SCT::NodeIdPubKey>,
        timeout_certificate: TimeoutCertificate<SCT>,
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

    pub fn reset_failure(&mut self, validator_id: NodeId<SCT::NodeIdPubKey>) {
        self.validator_failures.insert(validator_id, 0);
    }

    pub fn check_threshold(&self, validator_id: &NodeId<SCT::NodeIdPubKey>, threshold: u32) -> bool {
        self.validator_failures.get(validator_id).unwrap_or(&0) >= &threshold
    }
     // This method generates a list of ValidatorAccountability based on a threshold
        // Make sure you understand what each associated type's role is
        pub fn from_validator_monitor(
            validator_monitor: &ValidatorMonitor<SCT>,
            threshold: u32,
        ) -> Vec<ValidatorAccountability<SCT::NodeIdPubKey>>
        where
            SCT::NodeIdPubKey: SignatureCollection, Vec<ValidatorAccountability<<SCT as SignatureCollection>::NodeIdPubKey>>: FromIterator<ValidatorAccountability<SCT>>,
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

    #[test]
    fn test_record_and_reset_failures() {
        let mut monitor = ValidatorMonitor::<MockSignatureCollection<NopSignature>>::new();
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
        let mut monitor = ValidatorMonitor::<MockSignatureCollection<NopSignature>>::new();
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

