use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use alloy_rlp::Decodable;
use monad_crypto::hasher::Hash;
use monad_ledger::executable_block::{format_block_id, ExecutableBlock};
use monad_types::BlockId;

fn main() {
    let eth_block_path = PathBuf::from("ledger");
    let wal_path = {
        let mut wal_path = eth_block_path.clone();
        wal_path.push("wal");
        wal_path
    };
    let wal = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(wal_path)
        .expect("failed to open WAL");
    let wal_len = wal.metadata().expect("failed to get wal metadata").len();
    const event_len: u64 = 33; // FIXME don't hardcode
    wal.set_len(wal_len / event_len * event_len)
        .expect("failed to set wal len");
    let num_events = wal_len / event_len;

    let mut wal = BufReader::new(wal);

    for idx in 0..num_events {
        let mut buf = [0_u8; event_len as usize];
        wal.read_exact(&mut buf)
            .expect("failed to read event from wal");
        let block_id = BlockId(Hash(buf[1..].try_into().expect("blockid not 32 bytes")));
        let formatted_block = format_block_id(&block_id.0 .0);
        let Ok(mut file) = File::open(format!("ledger/{}", &formatted_block)) else {
            println!("skipping event {idx}: ledger/{formatted_block}");
            continue;
        };
        let mut rlp_buf = Vec::new();
        file.read_to_end(&mut rlp_buf).unwrap();

        let block = ExecutableBlock::decode(&mut rlp_buf.as_slice()).unwrap();

        let event_type = match buf[0] {
            0 => "proposed",
            1 => "finalized",
            2 => "verified",
            _ => unreachable!(),
        };
        println!(
            "{} round={}, parent-round={}, block-num={}, {}",
            event_type,
            block.header.round,
            block.header.parent_round,
            block.number(),
            formatted_block,
        );
        // for tx in block.transactions() {
        //     println!(
        //         "round={}, parent-round={}, block-num={}, {} => {} => signer={:?}, nonce={:?}",
        //         block.header.round,
        //         block.header.parent_round,
        //         block.number(),
        //         formatted_block,
        //         buf[0],
        //         tx.recover_signer(),
        //         tx.nonce()
        //     );
        // }
    }

    println!("total num events: {num_events}");
}
