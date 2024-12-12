use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::{BlockId, Hash, Round, SeqNum};
use reth_primitives::{Address, Header, TransactionSigned, Withdrawal};

#[derive(RlpEncodable, RlpDecodable)]
pub struct ExecutableBlockHeader {
    bft_block_id: [u8; 32],
    pub round: u64,
    pub parent_round: u64,

    beneficiary: Address,
    difficulty: u64,
    number: u64, // == seq_num
    gas_limit: u64,
    timestamp: u64,
    mix_hash: [u8; 32],
    nonce: [u8; 8],
    extra_data: [u8; 32],
    base_fee_per_gas: u64,
}

#[derive(RlpEncodable, RlpDecodable)]
pub struct ExecutableBlock {
    pub header: ExecutableBlockHeader,

    transactions: Vec<TransactionSigned>,
    ommers: Vec<Header>,
    withdrawals: Vec<Withdrawal>,
}

impl ExecutableBlock {
    pub fn new(
        bft_block_id: BlockId,
        round: Round,
        parent_round: Round,

        beneficiary: Address,
        transactions: Vec<TransactionSigned>,
        seq_num: SeqNum,
        gas_limit: u64,
        timestamp: u64,
        mix_hash: Hash,
        base_fee_per_gas: u64,
    ) -> Self {
        Self {
            header: ExecutableBlockHeader {
                bft_block_id: bft_block_id.0 .0,
                round: round.0,
                parent_round: parent_round.0,

                beneficiary,
                difficulty: 0,
                number: seq_num.0,
                gas_limit,
                timestamp,
                mix_hash: mix_hash.0,
                nonce: [0_u8; 8],
                extra_data: [0_u8; 32],
                base_fee_per_gas,
            },

            transactions,
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        }
    }

    pub fn bft_block_id(&self) -> &[u8; 32] {
        &self.header.bft_block_id
    }

    pub fn number(&self) -> u64 {
        self.header.number
    }

    pub fn num_tx(&self) -> usize {
        self.transactions.len()
    }

    pub fn transactions(&self) -> &Vec<TransactionSigned> {
        &self.transactions
    }
}

pub fn format_block_id(block_id: &[u8; 32]) -> String {
    hex::encode(block_id)
}
