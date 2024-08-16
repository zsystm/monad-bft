use criterion::{criterion_group, criterion_main, Criterion};
use monad_bls::{BlsSignatureCollection, ScaledBlsSignatureCollection};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    hasher::{Hasher, HasherType},
    NopSignature,
};
use monad_testutil::validators::create_keys_w_validators;
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetFactory;

const N: u32 = 1000;
const TEST_SEED: [u8; 32] = [
    112, 206, 169, 225, 137, 171, 70, 56, 226, 237, 168, 147, 179, 114, 113, 192, 61, 170, 244,
    120, 178, 59, 5, 67, 57, 126, 15, 143, 90, 175, 96, 223,
];

type SignatureType = NopSignature;
// This type has the same pubkey/signature setup. Only used for key generation
type KeygenSignatureCollectionType =
    BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
type SignatureCollectionType =
    ScaledBlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

// TODO: benchmark with fewer cores available to rayon
fn criterion_benchmark(c: &mut Criterion) {
    let (keys, certkeys, _, validator_mapping) = create_keys_w_validators::<
        SignatureType,
        KeygenSignatureCollectionType,
        _,
    >(N, ValidatorSetFactory::default());
    let mut hasher = HasherType::new();
    hasher.update(b"hello world");
    let data = hasher.hash();

    // sign
    c.bench_function("scaled_bls_sign", |b| {
        b.iter(|| certkeys[0].sign(data.as_ref()))
    });

    let mut sigs = Vec::new();
    for (node_id, certkey) in keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certkeys.iter())
    {
        sigs.push((node_id, certkey.sign(data.as_ref())));
    }

    // aggregate N signatures
    c.bench_function("scaled_bls_aggregate_1000", |b| {
        b.iter_batched(
            || sigs.clone(),
            |sigs| {
                SignatureCollectionType::new(sigs, &validator_mapping, data.as_ref(), &TEST_SEED)
                    .unwrap()
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // verify
    c.bench_function("scaled_bls_verify", |b| {
        b.iter_batched(
            || {
                SignatureCollectionType::new(
                    sigs.clone(),
                    &validator_mapping,
                    data.as_ref(),
                    &TEST_SEED,
                )
                .unwrap()
            },
            |sig_col| sig_col.verify(&validator_mapping, data.as_ref()).unwrap(),
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
