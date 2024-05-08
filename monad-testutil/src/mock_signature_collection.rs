use monad_consensus_types::{
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType},
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{CertificateSignatureRecoverable, CertificateSignaturePubKey},
    hasher::HasherType,
};
//use monad_testutil::mock_signature_collection::{create_timeout_certificate, monad_testutil::signing::MockSignatures};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_types::{NodeId, Round};
use crate::signing::MockSignatures;
////
pub fn create_timeout_certificate<ST>(
    round: Round,
    qc: &QuorumCertificate<MockSignatures<ST>>,
    validator_mapping: &ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<MockSignatures<ST>>>,
) -> Result<TimeoutCertificate<MockSignatures<ST>>, SignatureCollectionError<CertificateSignaturePubKey<ST>, ST>>
where
    ST: CertificateSignatureRecoverable + Default,
{
    // Example participant key generation and signature creation logic
    let participants = qc.get_participants(validator_mapping);

    let timeout_infos = participants.iter().map(|node_id| {
        // Using a deterministic seed for the example, replace with proper key generation as needed
        let mut secret: [u8; 32] = [127; 32];
        let keypair = <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut secret)
            .expect("Failed to create key pair");

        // Create a signature of type ST
        let message = b"example-message"; // Adjust the message as necessary
        let signature = ST::sign(message, &keypair);

        (
            *node_id,
            TimeoutInfo {
                round,
                high_qc: qc.clone(),
            },
            signature,
        )
    }).collect::<Vec<_>>();

    // Create and return the TimeoutCertificate
    TimeoutCertificate::<MockSignatures<ST>>::new(round, &timeout_infos[..], validator_mapping)
}