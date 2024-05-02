use std::{collections::HashSet, marker::PhantomData};
use monad_consensus::validation::signing::{Unvalidated, Unverified};
use monad_consensus_types::{
    block::Block, ledger::CommitResult, quorum_certificate::{QcInfo, QuorumCertificate}, signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    }, timeout::{TimeoutCertificate, TimeoutInfo}, voting::{ValidatorMapping, Vote, VoteInfo}
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    }, hasher::{ Hashable, Hasher, HasherType}, NopPubKey, NopSignature
};
use monad_types::{BlockId, NodeId, Round, SeqNum};
use std::iter::Iterator; // Add the missing import
/// Mock implementation of a signature collection for testing purposes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockSignatureCollection<ST: CertificateSignatureRecoverable> {
    pub signatures: MockSignatures<ST>,
    _phantom: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> MockSignatureCollection<ST> {
    pub fn new(signatures: MockSignatures<ST>) -> Self {
        Self {
            signatures,
            _phantom: PhantomData,
        }
    }
}
impl<ST: CertificateSignatureRecoverable> Hashable for MockSignatureCollection<ST> {
    fn hash(&self, state: &mut impl Hasher) {
        // Propagate the hash call to the `signatures` field
        self.signatures.hash(state);
    }
}


impl<ST: CertificateSignatureRecoverable> Hashable for MockSignatures<ST> {
    fn hash(&self, state: &mut impl monad_crypto::hasher::Hasher) {
        for pubkey in &self.pubkey {
            // Assume CertificateSignaturePubKey has a method to return a byte representation
            let pubkey_bytes = pubkey.bytes(); // this method needs to exist or be implemented
            state.update(pubkey_bytes);
        }
    }
}


impl SignatureCollection for MockSignatureCollection<NopSignature> {
    type NodeIdPubKey = NopPubKey; // Define the public key type
    type SignatureType = NopSignature; // Define the signature type contained within the collection

    fn new(
        sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        _validator_mapping: &ValidatorMapping<Self::NodeIdPubKey, SignatureCollectionKeyPairType<Self>>,
        _msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        let signatures = sigs.into_iter().map(|(P, _)| P.pubkey()).collect::<Vec<_>>();
        Ok(MockSignatureCollection {
            signatures: MockSignatures { pubkey: signatures },
            _phantom: PhantomData,
        })
    }

    fn get_hash(&self) -> monad_crypto::hasher::Hash {
        todo!()
    }
    
    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<
        Vec<NodeId<Self::NodeIdPubKey>>,
        SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>,
    > {
        todo!()
    }
    
    fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> HashSet<NodeId<Self::NodeIdPubKey>> {
        HashSet::new()
    }
    
    fn num_signatures(&self) -> usize {
        //self.signatures.len()
        0
    }
    
    fn serialize(&self) -> Vec<u8> {
        vec![]
    }
    
    fn deserialize(
        data: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        todo!("Implement deserialization")
    }
    
    // Other trait methods as required
}



/// Mock implementation of a key pair for testing purposes.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct MockSignatures<ST: CertificateSignatureRecoverable> {
    pubkey: Vec<CertificateSignaturePubKey<ST>>,
}

impl<ST: CertificateSignatureRecoverable> MockSignatures<ST> {
    pub fn with_pubkeys(pubkeys: &[CertificateSignaturePubKey<ST>]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }

    pub fn verify(&self) -> Result<Vec<NodeId<CertificateSignaturePubKey<ST>>>, SignatureCollectionError<CertificateSignaturePubKey<ST>, ST>> {
        Ok(self.pubkey.iter().map(|pubkey| NodeId::new(*pubkey)).collect())
    }

    pub fn get_participants(&self) -> HashSet<NodeId<CertificateSignaturePubKey<ST>>> {
        self.pubkey.iter().map(|pubkey| NodeId::new(*pubkey)).collect()
    }

    pub fn num_signatures(&self) -> usize {
        self.pubkey.len()
    }

    pub fn get_hash(&self) -> monad_crypto::hasher::Hash {
        monad_crypto::hasher::Hash([0u8; 32]) // Mock hash for testing
    }

    pub fn serialize(&self) -> Vec<u8> {
        vec![] // Mock serialization for testing
    }
    
}

pub fn create_timeout_certificate<SCT: SignatureCollection<SignatureType = NopSignature> + CertificateSignatureRecoverable>(
    round: Round,
    qc: &QuorumCertificate<SCT>,
    validator_mapping: &ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
) -> Result<TimeoutCertificate<SCT>, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>>
where
    SCT::NodeIdPubKey: Eq + std::hash::Hash,
{
    let participants = qc.get_participants(validator_mapping);
    
    let timeout_infos = participants.iter().map(|node_id| {
        // Create a new NopSignature for each participant, using a new or default public key
        let signature = NopSignature {
            pubkey: NopPubKey::new(None), // Using None to get the default key
            id: 0, // Default ID, customize this if you have a meaningful identifier
        };

        (
            *node_id,
            TimeoutInfo {
                round: qc.get_round(),
                high_qc: qc.clone(),
            },
            signature
        )
    }).collect::<Vec<_>>();

    TimeoutCertificate::new(round, &timeout_infos, validator_mapping)
}


