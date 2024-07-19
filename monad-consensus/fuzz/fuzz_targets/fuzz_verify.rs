#![no_main]
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Error, Unstructured},
    fuzz_target,
};
use monad_consensus::messages::{
    consensus_message::{ConsensusMessage, ProtocolMessage},
    message::ProposalMessage,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopSignature};
use monad_testutil::{block::set_block_and_qc, signing::TestSigner, validators::setup_val_state};
use monad_types::{Epoch, NodeId, Round, SeqNum};
type SignatureType = NopSignature;

static _NUM_NODES: u32 = 4;
static _VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(2000);
static _EPOCH_START_DELAY: Round = Round(50);

#[derive(Clone, Debug)]
struct VerifyBlockData {
    pub block_round: Round,
    pub qc_round: Round,
    pub parent_round: Round,
    pub qc_seq_num: SeqNum,
    pub block_seq_num: SeqNum,
}

impl<'a> Arbitrary<'a> for VerifyBlockData {
    fn arbitrary(raw: &mut Unstructured<'a>) -> Result<Self, Error> {
        let mut buf = [0; 40];
        raw.fill_buffer(&mut buf)?;

        Ok(VerifyBlockData {
            block_round: Round(u64::from_ne_bytes(
                buf[0..8].try_into().expect("slice with incorrect length"),
            )),
            qc_round: Round(u64::from_ne_bytes(
                buf[8..16].try_into().expect("slice with incorrect length"),
            )),
            parent_round: Round(u64::from_ne_bytes(
                buf[16..24].try_into().expect("slice with incorrect length"),
            )),
            qc_seq_num: SeqNum(u64::from_ne_bytes(
                buf[24..32].try_into().expect("slice with incorrect length"),
            )),
            block_seq_num: SeqNum(u64::from_ne_bytes(
                buf[32..40].try_into().expect("slice with incorrect length"),
            )),
        })
    }
}

impl VerifyBlockData {
    fn unwrap(self) -> (Round, Round, Round, SeqNum, SeqNum) {
        (
            self.block_round,
            self.qc_round,
            self.parent_round,
            self.qc_seq_num,
            self.block_seq_num,
        )
    }
}

fuzz_target!(|data: VerifyBlockData| {
    let (f_block_round, f_qc_round, f_parent_round, f_qc_seq_num, f_block_seq_num) = data.unwrap();
    let known_epoch = Epoch(1); // Can be Constant
    let known_round = Round(0); // Can be Constant
    let val_epoch = known_epoch; // Can be Constant
    let qc_epoch = Epoch(1); // Can be Constant
    let block_epoch = Epoch(1); // Can be Constant

    let block_round = f_block_round; //Free variable
    let block_seq_num = f_block_seq_num; //Free variable
    let qc_round = f_qc_round; //Free variable
    let qc_parent_round = f_parent_round; //Free variable
    let qc_seq_num = f_qc_seq_num; //Free variable

    let (keypairs, _certkeys, epoch_manager, val_epoch_map) = setup_val_state(
        known_epoch,
        known_round,
        val_epoch,
        _NUM_NODES,
        _VAL_SET_UPDATE_INTERVAL,
        _EPOCH_START_DELAY,
    );
    let author = NodeId::new(keypairs[0].pubkey());
    let proposal = ProtocolMessage::Proposal(ProposalMessage {
        block: set_block_and_qc(
            author,
            block_epoch,
            block_round,
            block_seq_num,
            qc_epoch,
            qc_round,
            qc_parent_round,
            qc_seq_num,
            keypairs
                .iter()
                .map(|kp| kp.pubkey())
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        last_round_tc: None,
    });
    let conmsg = ConsensusMessage {
        version: "TEST".into(),
        message: proposal,
    };
    let sp = TestSigner::<SignatureType>::sign_object(conmsg, &keypairs[0]);

    assert!(sp
        .verify(&epoch_manager, &val_epoch_map, &keypairs[0].pubkey())
        .is_ok());
});
