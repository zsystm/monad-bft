use std::{
    env,
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    time::Instant,
};

use alloy_consensus::TxEnvelope;
use alloy_rlp::{Decodable, Encodable};
use monad_compress::{
    brotli::BrotliCompression, deflate::DeflateCompression, lz4::Lz4Compression, CompressionAlgo,
};
use peak_alloc::PeakAlloc;
use rand::{seq::SliceRandom, thread_rng};

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

const PROPOSAL_SIZE: usize = 2_000_000;

// decode raw transactions, shuffle, chunk by PROPOSAL_SIZE, and encode each chunk
fn get_chunked_txs(raw_txs_dir: &str) -> Vec<Vec<u8>> {
    let raw_txs_files = fs::read_dir(raw_txs_dir).unwrap();
    let mut txs = Vec::new();
    for raw_txs_file in raw_txs_files.take(100) {
        let file = File::open(raw_txs_file.unwrap().path()).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();
            if line.len() == 0 {
                continue;
            }

            let raw_tx_bytes: Vec<u8> = (0..line.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&line[i..i + 2], 16))
                .map(|s| s.unwrap())
                .collect();

            let tx = TxEnvelope::decode(&mut raw_tx_bytes.as_slice()).unwrap();
            txs.push(tx);
        }
    }

    txs.shuffle(&mut thread_rng());

    let mut encoded_txs_chunks = Vec::new();

    let mut curr_chunk = Vec::new();
    let mut curr_len = 0;
    for tx in txs {
        let tx_len = tx.length();

        if curr_len + tx_len > PROPOSAL_SIZE {
            let mut curr_chunk_encoded = Vec::new();
            curr_chunk.encode(&mut curr_chunk_encoded);
            encoded_txs_chunks.push(curr_chunk_encoded);

            curr_chunk.clear();
            curr_len = 0;
        }

        curr_chunk.push(tx);
        curr_len += tx_len;
    }

    if curr_len > PROPOSAL_SIZE - 100_000 {
        let mut curr_chunk_encoded = Vec::new();
        curr_chunk.encode(&mut curr_chunk_encoded);
        encoded_txs_chunks.push(curr_chunk_encoded);
    }

    encoded_txs_chunks
}

fn bench_compression_algo(algo: impl CompressionAlgo, compression_level: u32, txns: &[u8]) {
    let bg_mem_usage = PEAK_ALLOC.current_usage_as_mb();

    PEAK_ALLOC.reset_peak_usage();

    let mut compressed = Vec::new();
    let start_time = Instant::now();
    algo.compress(&txns, &mut compressed)
        .expect("compression success");
    let compression_time = start_time.elapsed().as_millis();

    let mut decompressed = Vec::new();
    let start_time = Instant::now();
    algo.decompress(compressed.as_slice(), &mut decompressed)
        .expect("decompression success");
    let decompression_time = start_time.elapsed().as_millis();

    assert!(txns == &decompressed);

    println!(
        "{:<10} | {:<10.3} | {:<10.3} | {:<15} | {:<15} | {:<15}",
        compression_level,
        compressed.len() as f64 / txns.len() as f64,
        PEAK_ALLOC.peak_usage_as_mb() - bg_mem_usage,
        compressed.len() / 1024,
        compression_time,
        decompression_time,
    );
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <raw_txs_directory> [<dictionary_path>]", args[0]);
        return;
    }
    let raw_txs_directory = &args[1];
    let dictionary_path = (args.len() > 2).then(|| &args[2]);

    let chunked_txns = get_chunked_txs(raw_txs_directory);
    let txns = chunked_txns.first().unwrap();

    let dictionary = dictionary_path.map(|dictionary_path| {
        let mut dictionary = Vec::new();
        let mut file = File::open(dictionary_path).expect("file exists");
        file.read_to_end(&mut dictionary)
            .expect("file read success");
        dictionary
    });

    println!("{:-^90}", "brotli");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<15} | {:<15} | {:<15}",
        "level", "ratio", "mem (MB)", "compressed (KB)", "comp time (ms)", "decomp time (ms)"
    );
    for compression_level in 0..=11 {
        let algo = BrotliCompression::new(compression_level, 22, Vec::new());
        bench_compression_algo(algo, compression_level, &txns);
    }

    if let Some(dictionary) = dictionary {
        println!("{:-^90}", "brotli-dict");
        println!(
            "{:<10} | {:<10} | {:<10} | {:<15} | {:<15} | {:<15}",
            "level", "ratio", "mem (MB)", "compressed (KB)", "comp time (ms)", "decomp time (ms)"
        );
        for compression_level in 0..=11 {
            let cloned_dict = dictionary.clone();
            let algo = BrotliCompression::new(compression_level, 22, cloned_dict);
            bench_compression_algo(algo, compression_level, &txns);
        }
    }

    println!("{:-^90}", "deflate");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<15} | {:<15} | {:<15}",
        "level", "ratio", "mem (MB)", "compressed (KB)", "comp time (ms)", "decomp time (ms)"
    );
    for compression_level in 0..=11 {
        let algo = DeflateCompression::new(compression_level, 0, Vec::new());
        bench_compression_algo(algo, compression_level, &txns);
    }

    println!("{:-^90}", "lz4");
    println!(
        "{:<10} | {:<10} | {:<10} | {:<15} | {:<15} | {:<15}",
        "level", "ratio", "mem (MB)", "compressed (KB)", "comp time (ms)", "decomp time (ms)"
    );
    for compression_level in 0..=16 {
        let algo = Lz4Compression::new(compression_level, 0, Vec::new());
        bench_compression_algo(algo, compression_level, &txns);
    }
}
