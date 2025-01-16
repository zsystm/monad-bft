use monad_consensus::messages::message::{TimeoutMessage, VoteMessage};
use monad_consensus_types::{
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::{HighQcRound, HighQcRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hashable, Hasher, HasherType},
    NopSignature,
};
use monad_multi_sig::MultiSig;
use monad_testutil::signing::*;
use monad_types::*;
use zerocopy::AsBytes;

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<NopSignature>;

#[test]
fn timeout_digest() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            Vote {
                ..DontCare::dont_care()
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.epoch);
    hasher.update(ti.round);
    hasher.update(ti.high_qc.get_round());
    let h1 = hasher.hash();

    let h2 = ti.timeout_digest();

    assert_eq!(h1, h2);
}

#[test]
fn timeout_info_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            Vote {
                ..DontCare::dont_care()
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let mut hasher = HasherType::new();
    hasher.update(ti.epoch);
    hasher.update(ti.round.0.as_bytes());
    hasher.update(ti.high_qc.get_block_id().0.as_bytes());
    hasher.update(ti.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&ti);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            Vote {
                ..DontCare::dont_care()
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let mut hasher = HasherType::new();
    hasher.update(tmo.tminfo.epoch.0.as_bytes());
    hasher.update(tmo.tminfo.round.0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_block_id().0.as_bytes());
    hasher.update(tmo.tminfo.high_qc.get_hash());
    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo);

    assert_eq!(h1, h2);
}

#[test]
fn timeout_msg_hash() {
    let ti = TimeoutInfo {
        epoch: Epoch(1),
        round: Round(10),
        high_qc: QuorumCertificate::<MockSignatures<SignatureType>>::new(
            Vote {
                ..DontCare::dont_care()
            },
            MockSignatures::with_pubkeys(&[]),
        ),
    };

    let tmo = Timeout {
        tminfo: ti,
        last_round_tc: None,
    };

    let cert_key = get_certificate_key::<MockSignatures<SignatureType>>(7);

    let tmo_msg = TimeoutMessage::new(tmo, &cert_key);

    let mut hasher = HasherType::new();
    hasher.update(tmo_msg.timeout.tminfo.epoch.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.round.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.high_qc.info.id.0.as_bytes());
    hasher.update(tmo_msg.timeout.tminfo.high_qc.get_hash());
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(tmo_msg.sig);
        hasher.update(sig_bytes);
    }

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&tmo_msg);

    assert_eq!(h1, h2);
}

#[test]
fn max_high_qc() {
    let high_qc_rounds = [
        HighQcRound { qc_round: Round(1) },
        HighQcRound { qc_round: Round(3) },
        HighQcRound { qc_round: Round(1) },
    ]
    .iter()
    .map(|x| {
        let msg = HasherType::hash_object(x);
        let keypair = get_key::<SignatureType>(0);
        HighQcRoundSigColTuple {
            high_qc_round: *x,
            sigs: SignatureType::sign(msg.as_ref(), &keypair),
        }
    })
    .collect();

    let tc = TimeoutCertificate {
        epoch: Epoch(1),
        round: Round(2),
        high_qc_rounds,
    };

    assert_eq!(tc.max_round(), Round(3));
}

#[test]
fn vote_msg_hash() {
    let v = Vote {
        ..DontCare::dont_care()
    };

    let certkey = get_certificate_key::<SignatureCollectionType>(7);
    let vm = VoteMessage::<SignatureCollectionType>::new(v, &certkey);

    let mut hasher = HasherType::new();
    v.hash(&mut hasher);
    unsafe {
        let sig_bytes = std::mem::transmute::<
            <SignatureCollectionType as SignatureCollection>::SignatureType,
            [u8; std::mem::size_of::<
                <SignatureCollectionType as SignatureCollection>::SignatureType,
            >()],
        >(vm.sig);
        hasher.update(sig_bytes);
    }

    let h1 = hasher.hash();

    let h2 = HasherType::hash_object(&vm);

    assert_eq!(h1, h2);
}
