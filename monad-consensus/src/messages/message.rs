use std::{fmt::Debug, marker::PhantomData};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    no_endorsement::{self, NoEndorsement, NoEndorsementCertificate},
    payload::ConsensusBlockBody,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate, TimeoutVote},
    tip::ConsensusTip,
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hashable, Hasher},
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round};

/// Consensus protocol vote message
///
/// The signature is a protocol signature, can be collected into the
/// corresponding SignatureCollection type, used to create QC from the votes
#[derive(PartialEq, Eq, Clone, RlpEncodable, RlpDecodable)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

/// Explicitly implementing Copy because derive macro can't resolve that
/// SCT::SignatureType is actually Copy
impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl<SCT: SignatureCollection> VoteMessage<SCT> {
    pub fn new(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_enc = alloy_rlp::encode(vote);
        let sig = <SCT::SignatureType as CertificateSignature>::sign(vote_enc.as_ref(), key);

        Self { vote, sig }
    }
}

/// Consensus protocol timeout message
///
/// The signature is a protocol signature,can be collected into the
/// corresponding SignatureCollection type, used to create TC from the timeouts
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable)]
#[rlp(trailing)]
pub struct TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub timeout: Timeout<ST, SCT, EPT>,
    pub sig: SCT::SignatureType,
    pub node_id: NodeId<SCT::NodeIdPubKey>,
    pub high_vote: Option<TimeoutVote<ST, SCT>>,
}

impl<ST, SCT, EPT> TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        node_id: NodeId<SCT::NodeIdPubKey>,
        timeout: Timeout<ST, SCT, EPT>,
        key: &SignatureCollectionKeyPairType<SCT>,
    ) -> Self {
        let tmo_enc = alloy_rlp::encode(timeout.tminfo.timeout_digest());
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_enc.as_ref(), key);

        let high_vote = match timeout.tminfo.high_tip {
            Some(ht) => {
                let high_vote = Vote {
                    id: ht.block_id,
                    round: ht.round,
                    epoch: ht.epoch,
                    parent_id: todo!(),
                    parent_round: todo!(),
                };

                let high_vote_enc = alloy_rlp::encode(high_vote);
                let high_vote_sig =
                    <SCT::SignatureType as CertificateSignature>::sign(high_vote_enc.as_ref(), key);

                Some(VoteMessage::<SCT> {
                    vote: high_vote,
                    sig: high_vote_sig,
                })
            }
            None => None,
        };

        let high_vote = high_vote.map(|v| TimeoutVote {
            node_id,
            vote: v.vote,
            sig: v.sig,
            phantom: PhantomData,
        });

        Self {
            timeout,
            sig,
            node_id,
            high_vote,
        }
    }
}

/// An integrity hash over all the fields
impl<ST, SCT, EPT> Hashable for TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block_header: ConsensusBlockHeader<ST, SCT, EPT>,
    pub block_body: ConsensusBlockBody<EPT>,
    pub last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
    pub nec: Option<NoEndorsementCertificate<SCT>>,
}

/// The last_round_tc can be independently verified. The message hash is over
/// the block only
impl<ST, SCT, EPT> Hashable for ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct RoundRecoveryMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub round: Round,
    pub epoch: Epoch,
    pub high_tip: ConsensusTip<ST, SCT, EPT>,
    pub tc: TimeoutCertificate<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> Hashable for RoundRecoveryMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct NoEndorsementMessage<SCT: SignatureCollection> {
    pub no_endorsement: NoEndorsement,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> NoEndorsementMessage<SCT> {
    pub fn new(no_endorsement: NoEndorsement, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let enc = alloy_rlp::encode(no_endorsement);
        let sig = <SCT::SignatureType as CertificateSignature>::sign(enc.as_ref(), key);

        Self {
            no_endorsement,
            sig,
        }
    }
}

impl<SCT: SignatureCollection> Hashable for NoEndorsementMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[cfg(test)]
mod tests {
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::{
        block::MockExecutionProtocol, quorum_certificate::QuorumCertificate, timeout::TimeoutInfo,
    };
    use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
    use monad_multi_sig::MultiSig;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::{get_certificate_key, node_id};
    use monad_types::{Epoch, Round};

    use super::*;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

    type MockSigType = NopSignature;
    type MockSignatureCollectionType = MultiSig<MockSigType>;
    type ExecutionProtocolType = MockExecutionProtocol;

    #[test]
    fn timeout_message_serdes_1() {
        let key = get_certificate_key::<SignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<SignatureCollectionType>::genesis_qc();

        let tminfo = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc: genesis_qc.clone(),
            high_tip: None,
        };

        let tm = Timeout {
            tminfo,
            last_round_tc: None,
        };

        let msg =
            TimeoutMessage::<SignatureType, SignatureCollectionType, ExecutionProtocolType>::new(
                node_id::<SignatureType>(),
                tm,
                &key,
            );

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.timeout.tminfo.epoch, Epoch(12));
        assert_eq!(c.timeout.tminfo.round, Round(123));
        assert_eq!(c.timeout.tminfo.high_qc, genesis_qc);
        assert!(c.timeout.last_round_tc.is_none());
    }

    #[test]
    fn timeout_message_serdes_2() {
        let key = get_certificate_key::<MockSignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<MockSignatureCollectionType>::genesis_qc();

        let tminfo = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc: genesis_qc.clone(),
            high_tip: None,
        };

        let tm = Timeout {
            tminfo,
            last_round_tc: None,
        };

        let msg =
            TimeoutMessage::<MockSigType, MockSignatureCollectionType, ExecutionProtocolType>::new(
                node_id::<MockSigType>(),
                tm,
                &key,
            );

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<MockSigType, MockSignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.timeout.tminfo.epoch, Epoch(12));
        assert_eq!(c.timeout.tminfo.round, Round(123));
        assert_eq!(c.timeout.tminfo.high_qc, genesis_qc);
        assert!(c.timeout.last_round_tc.is_none());
    }
}
