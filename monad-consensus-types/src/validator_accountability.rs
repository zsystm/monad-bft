    use crate::{signature_collection::SignatureCollection, timeout::TimeoutCertificate};
    use monad_crypto::hasher::{Hashable, Hasher};
    use monad_types::NodeId;
    #[derive(Clone, PartialEq, Eq, Debug)]

    pub struct ValidatorAccountability<SCT: SignatureCollection> {
        pub validator_id: NodeId<SCT::NodeIdPubKey>,
        pub failure_counter: u32,
        pub latest_failure_certificate: TimeoutCertificate<SCT>,
    }

    impl<SCT: SignatureCollection> Hashable for ValidatorAccountability<SCT> {
        fn hash(&self, state: &mut impl Hasher) {
            // Hash the validator_id
            self.validator_id.hash(state);

            // Hash the failure_counter
            state.update(&self.failure_counter.to_le_bytes());

            // Hash the latest_failure_certificate
            // We assume TimeoutCertificate already implements Hashable or you will need to implement it
            self.latest_failure_certificate.hash(state);
        }
    }

   
