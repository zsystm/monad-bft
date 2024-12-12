use alloy_rlp::Encodable;
use monad_crypto::hasher::Hash;
use monad_ledger::executable_block::{format_block_id, ExecutableBlock};
use monad_types::{BlockId, Round, SeqNum};
use reth_primitives::Address;

fn main() {
    let bft_block_id = BlockId(Hash([1; 32]));
    let block = ExecutableBlock::new(
        bft_block_id,      // bft_block_id
        Round(11),         // round
        Round(10),         // parent_round
        Address::random(), // beneficiary
        Vec::new(),        // transactions
        SeqNum(1),         // seq_num
        1_000,             // gas_limit
        1731018255,        // timestamp
        Hash([3; 32]),     // mix_hash
        1_000,             // base_fee_per_gas
    );

    let mut header_bytes = Vec::default();
    block.encode(&mut header_bytes);

    std::fs::write(format_block_id(block.bft_block_id()), &header_bytes)
        .expect("failed to write file");
}
