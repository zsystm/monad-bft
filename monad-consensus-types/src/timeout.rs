use std::{collections::HashMap, marker::PhantomData};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::*;

use super::quorum_certificate::QuorumCertificate;
use crate::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    tip::ConsensusTip,
    voting::{ValidatorMapping, Vote},
};

/// Timeout message to broadcast to other nodes after a local timeout
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable)]
#[rlp(trailing)]
pub struct Timeout<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub tminfo: TimeoutInfo<ST, SCT, EPT>,
    /// if the high qc round != tminfo.round-1, then this must be the
    /// TC for tminfo.round-1. Otherwise it must be None
    pub last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
}

/// Data to include in a timeout
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct TimeoutInfo<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Epoch where the timeout happens
    pub epoch: Epoch,
    /// The round that timed out
    pub round: Round,
    /// The node's highest known qc
    pub high_qc: QuorumCertificate<SCT>,

    pub high_tip: Option<ConsensusTip<ST, SCT, EPT>>,
}

/// This is the set of fields over which TimeoutMessages are signed
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct TimeoutDigest {
    pub epoch: Epoch,
    pub round: Round,
    pub high_tip_digest: Option<TimeoutTipDigest>,
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct TimeoutTipDigest {
    pub high_tip_round: Round,
    pub high_tip_qc_round: Round,
    pub high_tip_nec_round: Option<Round>,
}

impl<ST, SCT, EPT> TimeoutInfo<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn timeout_digest(&self) -> TimeoutDigest {
        TimeoutDigest {
            epoch: self.epoch,
            round: self.round,
            high_tip_digest: self.high_tip.as_ref().map(|ht| TimeoutTipDigest {
                high_tip_round: ht.round,
                high_tip_qc_round: ht.qc.get_round(),
                high_tip_nec_round: ht.nec.as_ref().map(|nec| nec.msg.round),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct HighQcRoundSigColTuple<SCT> {
    pub tminfo_digest: TimeoutDigest,
    pub sigs: SCT,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TimeoutVote<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub node_id: NodeId<SCT::NodeIdPubKey>,
    pub vote: Vote,
    pub sig: SCT::SignatureType,
    pub phantom: PhantomData<ST>,
}

/// TimeoutCertificate is used to advance rounds when a QC is unable to
/// form for a round
/// A collection of Timeout messages is the basis for building a TC
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TimeoutCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// The epoch where the TC is created
    pub epoch: Epoch,
    /// The Timeout messages must have been for the same round
    /// to create a TC
    pub round: Round,

    pub tips: Vec<ConsensusTip<ST, SCT, EPT>>,
    pub timeout_votes: Vec<TimeoutVote<ST, SCT>>,

    /// signatures over the round of the TC and the high qc round,
    /// proving that the supermajority of the network is locked on the
    /// same high_qc
    pub high_tip_digest_sigs: Vec<HighQcRoundSigColTuple<SCT>>,
}

impl<ST, SCT, EPT> TimeoutCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        epoch: Epoch,
        round: Round,
        high_qc_round_sig_tuple: &[(
            NodeId<SCT::NodeIdPubKey>,
            TimeoutInfo<ST, SCT, EPT>,
            SCT::SignatureType,
            Option<TimeoutVote<ST, SCT>>,
        )],
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Result<Self, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>> {
        let mut sigs = HashMap::new();
        for (node_id, tmo_info, sig, _) in high_qc_round_sig_tuple {
            let high_tip_round = tmo_info.high_tip.as_ref().map_or(Round(0), |t| t.round);
            let tminfo_digest = tmo_info.timeout_digest();
            let entry = sigs
                .entry(high_tip_round)
                .or_insert((tminfo_digest, Vec::new()));
            assert_eq!(entry.0, tminfo_digest);
            entry.1.push((*node_id, *sig));
        }
        let mut high_tip_digest_sigs = Vec::new();
        for (high_qc_round, (tminfo_digest, sigs)) in sigs.into_iter() {
            let tminfo_digest_enc = alloy_rlp::encode(tminfo_digest);
            let sct = SCT::new(sigs, validator_mapping, tminfo_digest_enc.as_ref())?;
            high_tip_digest_sigs.push(HighQcRoundSigColTuple::<SCT> {
                tminfo_digest,
                sigs: sct,
            });
        }
        Ok(Self {
            epoch,
            round,
            timeout_votes: vec![],
            tips: vec![],
            high_tip_digest_sigs,
        })
    }

    pub fn new_v2(
        epoch: Epoch,
        round: Round,
        timeout_msgs: &[(
            NodeId<SCT::NodeIdPubKey>,
            TimeoutInfo<ST, SCT, EPT>,
            SCT::SignatureType,
            Option<TimeoutVote<ST, SCT>>,
        )],
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Result<Self, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>> {
        // get the matching timeout digests, their signatures, the tips and the votes
        let mut timeouts = HashMap::new();
        let mut tips = Vec::new();
        let mut votes = Vec::new();

        for (node_id, tmo_info, sig, vote) in timeout_msgs {
            let tmo_digest = tmo_info.timeout_digest();

            let entry = timeouts.entry(tmo_digest).or_insert(Vec::new());
            entry.push((*node_id, *sig));

            if let Some(tip) = &tmo_info.high_tip {
                tips.push(tip.clone());
            }

            if let Some(vote) = vote {
                votes.push(vote.clone());
            }
        }

        let mut high_tip_digest_sigs = Vec::new();
        for (tminfo_digest, sigs) in timeouts.into_iter() {
            let tminfo_digest_enc = alloy_rlp::encode(tminfo_digest);
            let sct = SCT::new(sigs, validator_mapping, tminfo_digest_enc.as_ref())?;

            high_tip_digest_sigs.push(HighQcRoundSigColTuple::<SCT> {
                tminfo_digest,
                sigs: sct,
            });
        }

        Ok(Self {
            epoch,
            round,
            tips,
            timeout_votes: votes,
            high_tip_digest_sigs,
        })
    }
}

impl<ST, SCT, EPT> TimeoutCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn max_round(&self) -> Round {
        self.high_tip_digest_sigs
            .iter()
            .map(|v| v.tminfo_digest.round)
            .max()
            .expect("verification of received TimeoutCertificates should have rejected any with empty high_qc_rounds")
    }

    pub fn find_high_tip(&self) -> ConsensusTip<ST, SCT, EPT> {
        todo!();
        //find_high_tip(&self.tips).expect("TimeoutCertificate must have a non-empty set of tips")
    }
}

pub fn find_high_tip<ST, SCT, EPT>(
    tips: &[ConsensusTip<ST, SCT, EPT>],
) -> Option<ConsensusTip<ST, SCT, EPT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    if tips.iter().any(|tip| !is_tip_fresh_proposal(tip)) {
        return None;
    }

    tips.iter().max_by_key(|tip| tip.round).cloned()
}

pub fn is_tip_fresh_proposal<ST, SCT, EPT>(tip: &ConsensusTip<ST, SCT, EPT>) -> bool
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    if tip.round == tip.qc.get_round() + Round(1) {
        return true;
    }

    if let Some(nec) = &tip.nec {
        return tip.round == nec.get_round() && tip.qc.get_round() == nec.get_high_qc_round();
    }

    false
}
