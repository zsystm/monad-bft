use std::mem::MaybeUninit;

use monad_event_ring::{ffi::monad_event_ring_type, EventRingType, RawEventDescriptorInfo};

use self::ffi::{
    g_monad_exec_event_metadata_hash, monad_exec_account_access,
    monad_exec_account_access_list_header, monad_exec_block_finalized, monad_exec_block_header,
    monad_exec_block_qc, monad_exec_block_reject, monad_exec_block_result,
    monad_exec_block_verified, monad_exec_evm_error, monad_exec_flow_info,
    monad_exec_storage_access, monad_exec_txn_call_frame, monad_exec_txn_log,
    monad_exec_txn_receipt, monad_exec_txn_reject, monad_exec_txn_start,
};
pub use self::{
    block_builder::{
        BlockBuilder, BlockBuilderResult, ExecutedBlock, ExecutedTxn, ExecutedTxnLog,
        ReassemblyError,
    },
    consensus_state::{BlockCommitState, ConsensusStateTracker, ConsensusStateTrackerResult},
    ffi::{
        monad_c_transaction_type_MONAD_TXN_EIP1559, monad_c_transaction_type_MONAD_TXN_EIP2930,
        monad_c_transaction_type_MONAD_TXN_LEGACY,
    },
};

mod block_builder;
mod consensus_state;
mod ffi;

#[derive(Debug)]
pub struct ExecEventRingType;

#[derive(Clone, Debug)]
pub enum ExecEvents {
    BlockStart(monad_exec_block_header),
    BlockEnd(monad_exec_block_result),
    BlockQC(monad_exec_block_qc),
    BlockFinalized(monad_exec_block_finalized),
    BlockVerified(monad_exec_block_verified),
    BlockReject(monad_exec_block_reject),
    TxnStart {
        txn_index: u32,
        txn_start: monad_exec_txn_start,
        data_bytes: Box<[u8]>,
    },
    TxnReject {
        txn_index: u32,
        reject: monad_exec_txn_reject,
    },
    TxnReceipt {
        txn_index: u32,
        receipt: monad_exec_txn_receipt,
    },
    TxnLog {
        txn_index: u32,
        txn_log: monad_exec_txn_log,
        topic_bytes: Box<[u8]>,
        data_bytes: Box<[u8]>,
    },
    TxnCallFrame {
        txn_index: u32,
        txn_call_frame: monad_exec_txn_call_frame,
        input_bytes: Box<[u8]>,
        return_bytes: Box<[u8]>,
    },
    AccountAccessListHeader(monad_exec_account_access_list_header),
    AccountAccess(monad_exec_account_access),
    StorageAccess(monad_exec_storage_access),
    EvmError(monad_exec_evm_error),
}

#[derive(Debug)]
pub enum ExecEventsRef<'reader> {
    BlockStart(&'reader monad_exec_block_header),
    BlockEnd(&'reader monad_exec_block_result),
    BlockQC(&'reader monad_exec_block_qc),
    BlockFinalized(&'reader monad_exec_block_finalized),
    BlockVerified(&'reader monad_exec_block_verified),
    BlockReject(&'reader monad_exec_block_reject),
    TxnStart {
        txn_index: u32,
        txn_start: &'reader monad_exec_txn_start,
        data_bytes: &'reader [u8],
    },
    TxnReject {
        txn_index: u32,
        reject: &'reader monad_exec_txn_reject,
    },
    TxnReceipt {
        txn_index: u32,
        receipt: &'reader monad_exec_txn_receipt,
    },
    TxnLog {
        txn_index: u32,
        txn_log: &'reader monad_exec_txn_log,
        topic_bytes: &'reader [u8],
        data_bytes: &'reader [u8],
    },
    TxnCallFrame {
        txn_index: u32,
        txn_call_frame: &'reader monad_exec_txn_call_frame,
        input_bytes: &'reader [u8],
        return_bytes: &'reader [u8],
    },
    AccountAccessListHeader(&'reader monad_exec_account_access_list_header),
    AccountAccess(&'reader monad_exec_account_access),
    StorageAccess(&'reader monad_exec_storage_access),
    EvmError(&'reader monad_exec_evm_error),
}

fn split_ref_from_bytes<'reader, T>(
    bytes: &'reader [u8],
) -> Result<(&'reader T, &'reader [u8]), String> {
    let Some((bytes, rest)) = bytes.split_at_checked(size_of::<T>()) else {
        return Err(format!(
            "Expected slice with length at least {} but slice has length {}",
            size_of::<T>(),
            bytes.len()
        ));
    };

    assert_eq!(bytes.len(), size_of::<T>());

    let bytes_ptr = bytes.as_ptr();

    if (bytes_ptr as usize) % align_of::<T>() != 0 {
        return Err(format!(
            "Expected slice with alignment {} but slice has ptr {}",
            align_of::<T>(),
            bytes_ptr as usize
        ));
    }

    Ok((unsafe { &*(bytes_ptr as *const T) }, rest))
}

fn ref_from_bytes<'reader, T>(bytes: &'reader [u8]) -> Result<&'reader T, String> {
    let (value_ref, rest) = split_ref_from_bytes(bytes)?;

    if !rest.is_empty() {
        return Err(format!(
            "Expected slice with length {} but slice has length {}",
            size_of::<T>(),
            bytes.len()
        ));
    }

    Ok(value_ref)
}

fn parse_with_trailing<'reader, T, const N: usize>(
    bytes: &'reader [u8],
    trailing_lengths: impl FnOnce(&T) -> [usize; N],
) -> Result<(&'reader T, [&'reader [u8]; N]), String> {
    let (value, mut bytes) = split_ref_from_bytes::<T>(bytes)?;

    let trailing_lengths = trailing_lengths(value);

    let mut trailing: [MaybeUninit<&'reader [u8]>; N] =
        unsafe { MaybeUninit::uninit().assume_init() };

    for (idx, length) in trailing_lengths.into_iter().enumerate() {
        let Some((bytes_next, bytes_rest)) = bytes.split_at_checked(length) else {
            return Err(format!("idk"));
        };

        trailing[idx] = MaybeUninit::new(bytes_next);
        bytes = bytes_rest;
    }

    if !bytes.is_empty() {
        return Err(format!("iddkeke"));
    }

    let trailing = std::array::from_fn(|i| unsafe { trailing[i].assume_init() });

    Ok((value, trailing))
}

fn txn_index_from_info(info: &RawEventDescriptorInfo) -> u32 {
    let flow_info = unsafe { std::mem::transmute::<u64, monad_exec_flow_info>(info.user[0]) };

    flow_info.txn_id.checked_sub(1).unwrap()
}

impl<'reader> ExecEventsRef<'reader> {
    fn into_owned(self) -> ExecEvents {
        match self {
            Self::BlockStart(block_start) => ExecEvents::BlockStart(*block_start),
            Self::BlockEnd(block_end) => ExecEvents::BlockEnd(*block_end),
            Self::BlockQC(block_qc) => ExecEvents::BlockQC(*block_qc),
            Self::BlockFinalized(block_finalized) => ExecEvents::BlockFinalized(*block_finalized),
            Self::BlockVerified(block_verified) => ExecEvents::BlockVerified(*block_verified),
            Self::BlockReject(block_reject) => ExecEvents::BlockReject(*block_reject),
            Self::TxnStart {
                txn_index,
                txn_start,
                data_bytes,
            } => ExecEvents::TxnStart {
                txn_index,
                txn_start: *txn_start,
                data_bytes: data_bytes.to_vec().into_boxed_slice(),
            },
            Self::TxnReject { txn_index, reject } => ExecEvents::TxnReject {
                txn_index,
                reject: *reject,
            },
            Self::TxnReceipt { txn_index, receipt } => ExecEvents::TxnReceipt {
                txn_index,
                receipt: *receipt,
            },
            Self::TxnLog {
                txn_index,
                txn_log,
                topic_bytes,
                data_bytes,
            } => ExecEvents::TxnLog {
                txn_index,
                txn_log: *txn_log,
                topic_bytes: topic_bytes.to_vec().into_boxed_slice(),
                data_bytes: data_bytes.to_vec().into_boxed_slice(),
            },
            Self::TxnCallFrame {
                txn_index,
                txn_call_frame,
                input_bytes,
                return_bytes,
            } => ExecEvents::TxnCallFrame {
                txn_index,
                txn_call_frame: *txn_call_frame,
                input_bytes: input_bytes.to_vec().into_boxed_slice(),
                return_bytes: return_bytes.to_vec().into_boxed_slice(),
            },
            Self::AccountAccessListHeader(account_access_list_header) => {
                ExecEvents::AccountAccessListHeader(*account_access_list_header)
            }
            Self::AccountAccess(account_access) => ExecEvents::AccountAccess(*account_access),
            Self::StorageAccess(storage_access) => ExecEvents::StorageAccess(*storage_access),
            Self::EvmError(evm_error) => ExecEvents::EvmError(*evm_error),
        }
    }
}

impl EventRingType for ExecEventRingType {
    type Event = ExecEvents;
    type EventRef<'reader> = ExecEventsRef<'reader>;

    fn ring_ctype() -> monad_event_ring_type {
        2
    }

    fn ring_metadata_hash() -> &'static [u8; 32] {
        unsafe { &g_monad_exec_event_metadata_hash }
    }

    fn raw_to_event_ref<'reader>(
        info: RawEventDescriptorInfo,
        bytes: &'reader [u8],
    ) -> Self::EventRef<'reader> {
        match info.event_type {
            ffi::monad_exec_event_type_MONAD_EXEC_NONE => {
                panic!("ExecEventRingType encountered NONE event_type");
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_START => {
                ExecEventsRef::BlockStart(ref_from_bytes(bytes).expect("BlockStart event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_END => {
                ExecEventsRef::BlockEnd(ref_from_bytes(bytes).expect("BlockEnd event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_QC => {
                ExecEventsRef::BlockQC(ref_from_bytes(bytes).expect("BlockQC event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_FINALIZED => ExecEventsRef::BlockFinalized(
                ref_from_bytes(bytes).expect("BlockFinalized event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_VERIFIED => ExecEventsRef::BlockVerified(
                ref_from_bytes(bytes).expect("BlockVerified event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_REJECT => {
                ExecEventsRef::BlockReject(ref_from_bytes(bytes).expect("BlockReject event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_START => {
                let (txn_start, [data_bytes]) =
                    parse_with_trailing::<monad_exec_txn_start, 1>(bytes, |txn_start| {
                        [txn_start.txn_header.data_length.try_into().unwrap()]
                    })
                    .expect("TxnStart event valid");

                ExecEventsRef::TxnStart {
                    txn_index: txn_index_from_info(&info),
                    txn_start,
                    data_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_REJECT => ExecEventsRef::TxnReject {
                txn_index: txn_index_from_info(&info),
                reject: ref_from_bytes(bytes).expect("TxnReject event valid"),
            },
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_RECEIPT => ExecEventsRef::TxnReceipt {
                txn_index: txn_index_from_info(&info),
                receipt: ref_from_bytes(bytes).expect("TxnReceipt event valid"),
            },
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_LOG => {
                let (txn_log, [topic_bytes, data_bytes]) =
                    parse_with_trailing::<monad_exec_txn_log, 2>(bytes, |txn_log| {
                        [
                            TryInto::<usize>::try_into(txn_log.topic_count)
                                .unwrap()
                                .checked_mul(size_of::<ffi::monad_c_bytes32>())
                                .unwrap(),
                            txn_log.data_length.try_into().unwrap(),
                        ]
                    })
                    .expect("TxnLog event valid");

                ExecEventsRef::TxnLog {
                    txn_index: txn_index_from_info(&info),
                    txn_log,
                    topic_bytes,
                    data_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_CALL_FRAME => {
                let (txn_call_frame, [input_bytes, return_bytes]) =
                    parse_with_trailing::<monad_exec_txn_call_frame, 2>(bytes, |txn_call_frame| {
                        [
                            txn_call_frame.input_length.try_into().unwrap(),
                            txn_call_frame.return_length.try_into().unwrap(),
                        ]
                    })
                    .expect("TxnCallFrame event valid");

                ExecEventsRef::TxnCallFrame {
                    txn_index: txn_index_from_info(&info),
                    txn_call_frame,
                    input_bytes,
                    return_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS_LIST_HEADER => {
                ExecEventsRef::AccountAccessListHeader(
                    ref_from_bytes(bytes).expect("AccountAccessListHeader event valid"),
                )
            }
            ffi::monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS => ExecEventsRef::AccountAccess(
                ref_from_bytes(bytes).expect("AccountAccess event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_STORAGE_ACCESS => ExecEventsRef::StorageAccess(
                ref_from_bytes(bytes).expect("StorageAccess event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_EVM_ERROR => {
                ExecEventsRef::EvmError(ref_from_bytes(bytes).expect("EvmError event valid"))
            }
            event_type => panic!("ExecEventRingType encountered unknown event_type {event_type}"),
        }
    }

    fn event_ref_to_event<'reader>(event_ref: Self::EventRef<'reader>) -> Self::Event {
        Self::EventRef::into_owned(event_ref)
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{SnapshotEventRing, TypedEventReader, TypedEventRing};

    use crate::ExecEventRingType;

    #[test]
    fn basic_test() {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../test/data/exec-events-emn-30b-15m.zst");

        let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
            SNAPSHOT_ZSTD_BYTES,
            SNAPSHOT_NAME,
        )
        .unwrap();

        let mut event_reader = snapshot.create_reader();

        while let Some(event_descriptor) = event_reader.next() {
            let event = event_descriptor.try_read_payload();

            eprintln!("event: {event:#?}");
        }
    }
}
