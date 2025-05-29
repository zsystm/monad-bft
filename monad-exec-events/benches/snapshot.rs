use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use monad_event_ring::{SnapshotEventRing, TypedEventDescriptor, TypedEventReader, TypedEventRing};
use monad_exec_events::ExecEventRingType;

fn bench_snapshot(c: &mut Criterion) {
    const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
    const SNAPSHOT_ZSTD_BYTES: &[u8] =
        include_bytes!("../../monad-exec-events/test/data/exec-events-emn-30b-15m.zst");

    let mut g = c.benchmark_group("snapshot");

    let items = {
        let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
            SNAPSHOT_ZSTD_BYTES,
            SNAPSHOT_NAME,
        )
        .unwrap();

        let mut event_reader = snapshot.create_reader();

        let mut items = 0;

        while event_reader.next().is_some() {
            items += 1;
        }

        items
    };

    g.throughput(criterion::Throughput::Elements(items));

    g.bench_function("snapshot", |b| {
        b.iter_batched(
            || {
                SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
                    SNAPSHOT_ZSTD_BYTES,
                    SNAPSHOT_NAME,
                )
                .unwrap()
            },
            |snapshot| {
                let mut event_reader = snapshot.create_reader();

                while let Some(event_descriptor) = event_reader.next() {
                    let actual_payload: Option<u64> =
                        event_descriptor.try_filter_map(|exec_event_ref| {
                            black_box(exec_event_ref);
                            black_box(Some(1))
                        });

                    black_box(actual_payload);
                }

                assert!(event_reader.next().is_none());
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_snapshot);
criterion_main!(benches);
