            use crate::{signature_collection::SignatureCollection, timeout::TimeoutCertificate};
            use monad_crypto::{certificate_signature::PubKey, hasher::{Hash, Hashable, Hasher}};
            use monad_types::NodeId;
            use std::collections::HashMap;

            #[derive(Clone, PartialEq, Eq, Debug)]
            pub struct ValidatorAccountability<P: PubKey, S:SignatureCollection> {
                pub validator_id: NodeId<P>,
                pub failure_counter: u32,
                pub latest_failure_certificate: TimeoutCertificate<S>,
            }
            


            impl<P: PubKey, S: SignatureCollection> Hashable for ValidatorAccountability<P, S> {
                fn hash(&self, state: &mut impl Hasher) {
                    self.validator_id.hash(state);
                    self.failure_counter.hash(state);
                    self.latest_failure_certificate.hash(state);
                }
            }
    

   
