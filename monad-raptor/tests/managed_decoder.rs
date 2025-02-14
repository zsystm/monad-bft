// Tests the managed Raptor decoder.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    time::Instant,
};

use monad_raptor::{Encoder, ManagedDecoder};
use rand::{prelude::SliceRandom, thread_rng, Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

const SYMBOL_LEN: usize = 4;

fn test_single_decode(src: Vec<u8>) {
    let encoder: Encoder = Encoder::new(&src, SYMBOL_LEN).unwrap();

    let num_source_symbols = encoder.num_source_symbols();

    let mut decoder = ManagedDecoder::new(num_source_symbols, SYMBOL_LEN).unwrap();

    let mut esis: Vec<usize> = (0..2 * num_source_symbols).collect();
    esis.shuffle(&mut thread_rng());

    for esi in &esis {
        let mut buf: Box<[u8]> = vec![0; SYMBOL_LEN].into_boxed_slice();
        encoder.encode_symbol(&mut buf, *esi);

        decoder.received_encoded_symbol(&buf, *esi).unwrap();

        // Make sure that we detect multiple submissions of the same ESI.
        if rand::thread_rng().gen_ratio(1, 100) {
            decoder.received_encoded_symbol(&buf, *esi).unwrap_err();
        }

        if decoder.try_decode() {
            break;
        }
    }

    if !decoder.decoding_done() {
        panic!("{:#?}", decoder);
    }

    let mut reconstructed_source_data = decoder
        .reconstruct_source_data()
        .expect("Error recovering source data");

    reconstructed_source_data.truncate(src.len());

    assert_eq!(*src, *reconstructed_source_data);
}

#[test]
fn test_managed_decoder() {
    let max_bytes = if cfg!(debug_assertions) { 128 } else { 2048 };

    for bytes in 0..=max_bytes {
        println!("Testing bytes = {}", bytes);

        let mut src = vec![0u8; bytes];

        thread_rng().fill_bytes(&mut src);

        test_single_decode(src);
    }
}

#[test]
fn test_symbol_time() {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("/home/bharath/testground/monad-bft/symbol_times")
        .unwrap();

    for _ in 0..5 {
        let mut src = vec![0u8; 2000000 + (thread_rng().next_u64() % 200) as usize];
        let mut rng = ChaCha20Rng::seed_from_u64(thread_rng().next_u64());
        rng.fill_bytes(src.as_mut_slice());

        let symbol_len = 1220;
        let encoder: Encoder = Encoder::new(&src, symbol_len).unwrap();
        let num_source_symbols = encoder.num_source_symbols();

        let mut decoder = ManagedDecoder::new(num_source_symbols, symbol_len).unwrap();

        let mut esis: Vec<usize> = (0..3 * num_source_symbols).collect();
        esis.shuffle(&mut rng);

        let mut symbol_times = Vec::new();
        for (index, esi) in esis.iter().enumerate() {
            let mut buf: Box<[u8]> = vec![0; symbol_len].into_boxed_slice();
            encoder.encode_symbol(&mut buf, *esi);

            let now = Instant::now();
            decoder.received_encoded_symbol(&buf, *esi).unwrap();
            symbol_times.push(now.elapsed().as_micros());

            if decoder.try_decode() {
                println!("decoding took {} symbols", index + 1);
                break;
            }
        }

        let mut reconstructed_source_data = decoder
            .reconstruct_source_data()
            .expect("Error recovering source data");

        reconstructed_source_data.truncate(src.len());

        assert_eq!(*src, *reconstructed_source_data);

        let symbol_times: Vec<String> = symbol_times
            .into_iter()
            .map(|num| num.to_string())
            .collect();
        f.write_all(symbol_times.join(", ").as_bytes()).unwrap();
        f.write_all("\n".as_bytes()).unwrap();
    }
}
