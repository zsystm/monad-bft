use monad_event_ring::{
    event_reader::{EventReader, NextResult},
    event_ring::{monad_event_descriptor, monad_event_ring_header, EventRing, EventRingType},
    event_test_util::*,
    exec_event_types_metadata::EXEC_EVENT_DOMAIN_METADATA,
};

struct ExportedMemArray<T> {
    bytes: Vec<u8>,
    len: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T> ExportedMemArray<T> {
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.bytes.as_ptr() as *const T, self.len) }
    }
}

struct EventRingFileSections {
    descriptors: ExportedMemArray<monad_event_descriptor>,
    payload_buf: ExportedMemArray<u8>,
}

fn load_event_ring_sections(zstd_bytes: &[u8]) -> EventRingFileSections {
    let mut decompressed_bits = Vec::new();
    zstd::stream::copy_decode(zstd_bytes, &mut decompressed_bits).expect("could not decompress");

    let event_ring_header =
        unsafe { &*(decompressed_bits.as_ptr() as *const monad_event_ring_header) };

    let descriptor_offset: usize = 1 << 21; // TODO(ken): this shouldn't be hard-coded here
    let descriptor_map_len =
        event_ring_header.size.descriptor_capacity * size_of::<monad_event_descriptor>();
    let payload_buf_offset = descriptor_offset + descriptor_map_len;
    let payload_buf_end = payload_buf_offset + event_ring_header.size.payload_buf_size;

    let descriptor_bits = &decompressed_bits.as_slice()[descriptor_offset..payload_buf_offset];
    let payload_buf_bits = &decompressed_bits.as_slice()[payload_buf_offset..payload_buf_end];

    assert!(descriptor_bits.len().is_power_of_two());
    assert!(payload_buf_bits.len().is_power_of_two());

    let descriptors = ExportedMemArray {
        bytes: Vec::from(descriptor_bits),
        len: event_ring_header.size.descriptor_capacity,
        _marker: std::marker::PhantomData,
    };

    let payload_buf = ExportedMemArray {
        bytes: Vec::from(payload_buf_bits),
        len: payload_buf_bits.len(),
        _marker: std::marker::PhantomData,
    };

    EventRingFileSections {
        descriptors,
        payload_buf,
    }
}

// This test manually loads the saved event ring snapshot in the test zstd
// file (see the `map_shm_snapshot_from_file` function) and checks that the
// event API gives back the exact same data we know it contains
#[test]
fn basic_test() {
    const TEST_SCENARIO: &ExecEventTestScenario = &ETHEREUM_MAINNET_30B_15M;
    let snapshot = EventRingSnapshot::load_from_scenario(TEST_SCENARIO);
    let expected_data = load_event_ring_sections(TEST_SCENARIO.event_ring_snapshot_zst);

    let event_ring = EventRing::mmap_from_fd(
        libc::PROT_READ,
        0,
        snapshot.snapshot_fd,
        snapshot.snapshot_off,
        TEST_SCENARIO.name,
    )
    .unwrap();
    let event_reader = EventReader::new(
        &event_ring,
        EventRingType::Exec,
        &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
    );
    if let Err(e) = event_reader {
        panic!("unable to open scenario {}: {}", TEST_SCENARIO.name, e);
    }
    let mut event_reader = event_reader.unwrap();
    let mut actual_event = monad_event_descriptor::default();
    event_reader.read_last_seqno = 0;

    let expected_descriptors: &[monad_event_descriptor] = expected_data.descriptors.as_slice();
    let expected_payload_buf: &[u8] = expected_data.payload_buf.as_slice();

    while event_reader.read_last_seqno <= TEST_SCENARIO.last_block_seqno {
        let next_result = event_reader.try_next(&mut actual_event);
        assert_eq!(next_result, NextResult::Success);

        let expected_index = (actual_event.seqno - 1) as usize;
        assert!(expected_index < expected_descriptors.len());
        let expected_event = &expected_descriptors[expected_index];
        assert_eq!(*expected_event, actual_event);

        let payload_begin =
            (expected_event.payload_buf_offset as usize) & (expected_payload_buf.len() - 1);
        let payload_end = payload_begin + expected_event.payload_size as usize;
        let expected_payload: &[u8] = &expected_payload_buf[payload_begin..payload_end];
        let actual_payload = event_reader.payload_peek(&actual_event);

        assert_eq!(expected_payload, actual_payload);
        assert!(event_reader.payload_check(&actual_event));
    }
}
