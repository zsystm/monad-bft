// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::marker::PhantomData;

use monad_consensus::{
    messages::message::{ProposalMessage, TimeoutMessage},
    validation::signing::Verified,
};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{HighExtend, HighTipRoundSigColTuple, Timeout, TimeoutCertificate, TimeoutInfo},
    tip::ConsensusTip,
    voting::{ValidatorMapping, Vote},
    RoundCertificate,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round, SeqNum, GENESIS_ROUND};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

#[derive(Clone)]
pub struct ProposalGen<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    epoch: Epoch,
    round: Round,
    qc: QuorumCertificate<SCT>,
    qc_seq_num: SeqNum,
    high_qc: QuorumCertificate<SCT>,
    high_qc_seq_num: SeqNum,
    last_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
    timestamp: u128,
    phantom: PhantomData<(ST, EPT)>,
}

impl<ST, SCT, EPT> Default for ProposalGen<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ST, SCT, EPT> ProposalGen<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new() -> Self {
        let genesis_qc = QuorumCertificate::genesis_qc();
        ProposalGen {
            epoch: Epoch(1),
            round: Round(0),
            qc: genesis_qc.clone(),
            qc_seq_num: SeqNum(0),
            high_qc: genesis_qc,
            high_qc_seq_num: SeqNum(0),
            last_tc: None,
            timestamp: 0,
            phantom: PhantomData,
        }
    }
    pub fn with_timestamp(mut self, timestamp: u128) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn next_proposal<
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    >(
        &mut self,
        keys: &[ST::KeyPairType],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        election: &LT,
        create_execution_inputs: impl FnOnce(
            SeqNum,
            u128,
            RoundSignature<SCT::SignatureType>,
        ) -> EPT::ProposedHeader,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
    ) -> Verified<ST, ProposalMessage<ST, SCT, EPT>> {
        // high_qc is the highest qc seen in a proposal
        let (qc, last_seq_num) = if self.last_tc.is_some() {
            (&self.high_qc, self.high_qc_seq_num)
        } else {
            // entering new round from qc
            self.round += Round(1);
            self.epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
            (&self.qc, self.qc_seq_num)
        };
        self.timestamp += 1;

        let (leader_key, leader_certkey) = keys
            .iter()
            .zip(certkeys)
            .find(|(k, _)| {
                let epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
                k.pubkey()
                    == election
                        .get_leader(
                            self.round,
                            val_epoch_map.get_val_set(&epoch).unwrap().get_members(),
                        )
                        .pubkey()
            })
            .expect("key not in valset");

        let seq_num = last_seq_num + SeqNum(1);
        let round_signature = RoundSignature::new(self.round, leader_certkey);
        let block_body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EPT::Body::default(),
        });
        let block_header = ConsensusBlockHeader::new(
            NodeId::new(leader_key.pubkey()),
            self.epoch,
            self.round,
            delayed_execution_results,
            create_execution_inputs(seq_num, self.timestamp, round_signature.clone()),
            block_body.get_id(),
            qc.clone(),
            seq_num,
            self.timestamp,
            round_signature,
        );

        let validator_cert_pubkeys = val_epoch_map
            .get_cert_pubkeys(&epoch_manager.get_epoch(self.round).expect("epoch exists"))
            .expect("should have the current validator certificate pubkeys");
        self.high_qc = self.qc.clone();
        self.high_qc_seq_num = self.qc_seq_num;
        self.qc = self.get_next_qc(certkeys, &block_header, validator_cert_pubkeys);
        self.qc_seq_num = seq_num;

        let proposal = ProposalMessage {
            proposal_epoch: self.epoch,
            proposal_round: self.round,
            tip: ConsensusTip::new(
                leader_key,
                block_header,
                None, // FIXME
            ),
            block_body,
            last_round_tc: self.last_tc.clone(),
        };
        self.last_tc = None;

        Verified::new(proposal, leader_key)
    }

    // next_tc uses the keys to generate a timeout certificate
    // to ensure that the consensus state is consistent with the ProposalGen state
    // call state.pacemaker.handle_event(&mut state.safety, &state.high_qc);
    // before adding the state's key to keys
    pub fn next_tc<VT: ValidatorSetType<NodeIdPubKey = CertificateSignaturePubKey<ST>>>(
        &mut self,
        keys: &[ST::KeyPairType],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valset: &VT,
        epoch_manager: &EpochManager,
        validator_mapping: &ValidatorMapping<
            CertificateSignaturePubKey<ST>,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Vec<Verified<ST, TimeoutMessage<ST, SCT, EPT>>> {
        let node_ids = keys
            .iter()
            .map(|keypair| NodeId::new(keypair.pubkey()))
            .collect::<Vec<_>>();
        if !valset.has_super_majority_votes(&node_ids).unwrap() {
            return Vec::new();
        }

        let tminfo = TimeoutInfo {
            epoch: self.epoch,
            round: self.round,
            high_qc_round: self.high_qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let tmo_digest = alloy_rlp::encode(&tminfo);
        // aggregate all tmo signatures into one collection because all nodes share a global state
        // in reality we don't have this configuration because timeout messages
        // can't all contain TC carrying signatures from all validators. It's fine
        // for tests
        let mut tc_sigs = Vec::new();
        for (node_id, certkey) in node_ids.iter().zip(certkeys.iter()) {
            let sig = <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Timeout>(
                tmo_digest.as_ref(),
                certkey,
            );
            tc_sigs.push((*node_id, sig));
        }
        let tmo_sig_col =
            SCT::new::<signing_domain::Timeout>(tc_sigs, validator_mapping, tmo_digest.as_ref())
                .unwrap();
        let tc = TimeoutCertificate::<ST, SCT, EPT> {
            epoch: self.epoch,
            round: self.round,
            tip_rounds: vec![HighTipRoundSigColTuple {
                high_qc_round: self.high_qc.get_round(),
                high_tip_round: GENESIS_ROUND,
                sigs: tmo_sig_col,
            }],
            high_extend: HighExtend::Qc(self.high_qc.clone()),
        };

        let mut tmo_msgs = Vec::new();
        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let timeout = Timeout::new(
                certkey,
                tminfo.clone(),
                HighExtend::Qc(self.high_qc.clone()),
                Some(RoundCertificate::Tc(tc.clone())),
            );
            tmo_msgs.push(Verified::<ST, _>::new(timeout.into(), key));
        }

        // entering new round through tc
        self.round += Round(1);
        self.epoch = epoch_manager.get_epoch(self.round).expect("epoch exists");
        self.last_tc = Some(tc);
        tmo_msgs
    }

    fn get_next_qc(
        &self,
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        block: &ConsensusBlockHeader<ST, SCT, EPT>,
        validator_mapping: &ValidatorMapping<
            CertificateSignaturePubKey<ST>,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> QuorumCertificate<SCT> {
        let vote = Vote {
            id: block.get_id(),
            epoch: block.epoch,
            round: block.block_round,
        };

        let msg = alloy_rlp::encode(vote);

        let mut sigs = Vec::new();
        for ck in certkeys {
            let sig = <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(
                msg.as_ref(),
                ck,
            );

            for (node_id, pubkey) in validator_mapping.map.iter() {
                if *pubkey == ck.pubkey() {
                    sigs.push((*node_id, sig));
                }
            }
        }

        let sigcol =
            SCT::new::<signing_domain::Vote>(sigs, validator_mapping, msg.as_ref()).unwrap();

        QuorumCertificate::new(vote, sigcol)
    }
}
