use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use alloy_primitives::U256;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use itertools::Itertools;
use lru::LruCache;
use monad_crypto::hasher::{Hasher, HasherType};
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_raptor::ManagedDecoder;
use monad_raptorcast::{
    udp::{build_messages, parse_message, MAX_REDUNDANCY, SIGNATURE_CACHE_SIZE},
    util::{BuildTarget, EpochValidators, FullNodes, Redundancy, Validator},
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{NodeId, Stake};

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    let message_size = 10_000 * 400;
    let message: Bytes = vec![123_u8; message_size].into();

    let mut group = c.benchmark_group("encoder/decoder");
    group.throughput(Throughput::Bytes(message_size as u64));
    group.bench_function("Encoding", |b| {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| {
                    (
                        NodeId::new(key.pubkey()),
                        Validator {
                            stake: Stake(U256::ONE),
                        },
                    )
                })
                .collect(),
        };

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let full_nodes = FullNodes::new(Vec::new());

        b.iter(|| {
            let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);
            let _ = build_messages::<SecpSignature>(
                &keys[0],
                DEFAULT_SEGMENT_SIZE, // segment_size
                message.clone(),
                Redundancy::from_u8(2),
                0, // epoch_no
                0, // unix_ts_ms
                BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
                &known_addresses,
            );
        });
    });

    group.bench_function("Decoding", |b| {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| {
                    (
                        NodeId::new(key.pubkey()),
                        Validator {
                            stake: Stake(U256::ONE),
                        },
                    )
                })
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);
        let full_nodes = FullNodes::new(Vec::new());

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let messages = build_messages::<SecpSignature>(
            &keys[0],
            DEFAULT_SEGMENT_SIZE, // segment_size
            message.clone(),
            Redundancy::from_u8(2),
            0, // epoch_no
            0, // unix_ts_ms
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
            &known_addresses,
        )
        .into_iter()
        .map(|(_to, message)| message)
        .collect_vec();

        let example_chunk = parse_message::<SecpSignature>(
            &mut LruCache::new(SIGNATURE_CACHE_SIZE),
            messages[0].clone().split_to(DEFAULT_SEGMENT_SIZE.into()),
            u64::MAX,
        )
        .expect("valid chunk");

        b.iter_batched(
            || messages.clone(),
            |messages| {
                let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);
                let mut decoder = {
                    let symbol_len = example_chunk.chunk.len();

                    // data_size is always greater than zero, so this division is safe
                    let num_source_symbols = message_size.div_ceil(symbol_len);
                    let encoded_symbol_capacity = MAX_REDUNDANCY.scale(num_source_symbols).unwrap();

                    ManagedDecoder::new(num_source_symbols, encoded_symbol_capacity, symbol_len)
                        .unwrap()
                };
                let mut decode_success = false;
                for mut message in messages {
                    while !message.is_empty() {
                        let parsed_message = parse_message::<SecpSignature>(
                            &mut signature_cache,
                            message.split_to(DEFAULT_SEGMENT_SIZE.into()),
                            u64::MAX,
                        )
                        .expect("valid message");
                        decoder.received_encoded_symbol(
                            &parsed_message.chunk,
                            parsed_message.chunk_id.into(),
                        );
                        if decoder.try_decode() {
                            decode_success = true;
                            break;
                        }
                    }
                }
                assert!(decode_success);
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
