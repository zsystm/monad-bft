            use crate::{timeout::TimeoutCertificate};
            use monad_crypto::{certificate_signature::PubKey, hasher::{Hash, Hashable, Hasher}};
            use monad_types::NodeId;
            use std::collections::HashMap;

            #[derive(Clone, PartialEq, Eq, Debug)]
            pub struct ValidatorAccountability<P: PubKey> {
                pub validator_id: NodeId<P>,
                pub failure_counter: u32,
                pub latest_failure_certificate: TimeoutCertificate<Hash>,
            }
            


            impl<P: PubKey> Hashable for ValidatorAccountability<P> {
                fn hash(&self, state: &mut impl Hasher) {
                    self.validator_id.hash(state);
                    self.failure_counter.hash(state);
                    self.latest_failure_certificate.hash(state);
                }
            }
    

   
