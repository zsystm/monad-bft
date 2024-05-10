use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::{certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
}, hasher::{Hasher, HasherType}};
use monad_types::{NodeId, Stake};
use crate::validator_set::ValidatorSetTypeFactory; // Ensure you import the local trait

pub fn create_keys_w_validators<ST, SCT, VTF>(
    num_nodes: u32,
    validator_set_factory: VTF,
) -> (
    Vec<ST::KeyPairType>,
    Vec<SignatureCollectionKeyPairType<SCT>>,
    VTF::ValidatorSetType,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let keys = create_keys::<ST>(num_nodes);
    let certificate_keys = create_certificate_keys::<SCT>(num_nodes);
    let (validators, validator_mapping) = complete_keys_w_validators::<ST, SCT, VTF>(
        &keys,
        &certificate_keys,
        validator_set_factory,
    );
    (keys, certificate_keys, validators, validator_mapping)
}

fn complete_keys_w_validators<ST, SCT, VTF>(
    keys: &[ST::KeyPairType],
    certificate_keys: &[SignatureCollectionKeyPairType<SCT>],
    validator_set_factory: VTF,
) -> (
    VTF::ValidatorSetType,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake(1)))
        .collect::<Vec<_>>();

    let voting_identity = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certificate_keys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let validators = validator_set_factory
        .create(staking_list)
        .expect("create validator set");
    let validator_mapping = ValidatorMapping::new(voting_identity);

    (validators, validator_mapping)
}

fn create_keys<ST: CertificateSignatureRecoverable>(num_keys: u32) -> Vec<ST::KeyPairType> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_key::<ST>(i.into());
        res.push(keypair);
    }

    res
}

fn create_certificate_keys<SCT: SignatureCollection>(
    num_keys: u32,
) -> Vec<SignatureCollectionKeyPairType<SCT>> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_certificate_key::<SCT>(i as u64 + u32::MAX as u64);
        res.push(keypair);
    }
    res
}

fn get_key<ST: CertificateSignatureRecoverable>(seed: u64) -> ST::KeyPairType {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}

fn get_certificate_key<SCT: SignatureCollection>(
    seed: u64,
) -> SignatureCollectionKeyPairType<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}
