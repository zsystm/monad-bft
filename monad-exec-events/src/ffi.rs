//! This module contains low-level bindings to the monad execution client event types.

pub(crate) use self::bindings::{
    g_monad_exec_event_metadata_hash, monad_exec_event_type,
    monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS,
    monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS_LIST_HEADER,
    monad_exec_event_type_MONAD_EXEC_BLOCK_END, monad_exec_event_type_MONAD_EXEC_BLOCK_FINALIZED,
    monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_ENTER,
    monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_EXIT,
    monad_exec_event_type_MONAD_EXEC_BLOCK_QC, monad_exec_event_type_MONAD_EXEC_BLOCK_REJECT,
    monad_exec_event_type_MONAD_EXEC_BLOCK_START, monad_exec_event_type_MONAD_EXEC_BLOCK_VERIFIED,
    monad_exec_event_type_MONAD_EXEC_EVM_ERROR, monad_exec_event_type_MONAD_EXEC_NONE,
    monad_exec_event_type_MONAD_EXEC_STORAGE_ACCESS,
    monad_exec_event_type_MONAD_EXEC_TXN_CALL_FRAME, monad_exec_event_type_MONAD_EXEC_TXN_END,
    monad_exec_event_type_MONAD_EXEC_TXN_EVM_OUTPUT, monad_exec_event_type_MONAD_EXEC_TXN_LOG,
    monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_ENTER,
    monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_EXIT,
    monad_exec_event_type_MONAD_EXEC_TXN_REJECT, monad_exec_event_type_MONAD_EXEC_TXN_START,
    monad_exec_flow_type_MONAD_FLOW_ACCOUNT_INDEX, monad_exec_flow_type_MONAD_FLOW_BLOCK_SEQNO,
    monad_exec_flow_type_MONAD_FLOW_TXN_ID,
};
pub use self::bindings::{
    monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_c_eth_txn_receipt,
    monad_exec_account_access, monad_exec_account_access_list_header, monad_exec_block_end,
    monad_exec_block_finalized, monad_exec_block_qc, monad_exec_block_reject,
    monad_exec_block_start, monad_exec_block_tag, monad_exec_block_verified, monad_exec_evm_error,
    monad_exec_storage_access, monad_exec_txn_call_frame, monad_exec_txn_evm_output,
    monad_exec_txn_log, monad_exec_txn_reject, monad_exec_txn_start,
};

#[allow(dead_code, missing_docs, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    use ::monad_event_ring::ffi::{monad_event_descriptor, monad_event_iterator, monad_event_ring};

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use ::monad_event_ring::ffi::{monad_event_descriptor, monad_event_iterator, monad_event_ring};

pub(crate) fn monad_exec_ring_get_block_number(
    c_event_ring: &monad_event_ring,
    c_event_descriptor: &monad_event_descriptor,
) -> Result<u64, ()> {
    let mut block_number = 0;

    let success = unsafe {
        self::bindings::monad_exec_ring_get_block_number(
            c_event_ring,
            c_event_descriptor,
            &mut block_number,
        )
    };

    success.then_some(block_number).ok_or(())
}

pub(crate) fn monad_exec_ring_block_id_matches(
    c_event_ring: &monad_event_ring,
    c_event_descriptor: &monad_event_descriptor,
    block_id: &monad_c_bytes32,
) -> bool {
    unsafe {
        self::bindings::monad_exec_ring_block_id_matches(c_event_ring, c_event_descriptor, block_id)
    }
}

pub(crate) fn monad_exec_iter_consensus_prev(
    c_event_iterator: &mut monad_event_iterator,
    c_event_ring: &monad_event_ring,
    c_exec_event_filter: monad_exec_event_type,
) -> Result<monad_event_descriptor, ()> {
    let mut c_event_descriptor: monad_event_descriptor = unsafe { std::mem::zeroed() };

    let success = unsafe {
        self::bindings::monad_exec_iter_consensus_prev(
            c_event_iterator,
            c_event_ring,
            c_exec_event_filter,
            &mut c_event_descriptor,
        )
    };

    success.then_some(c_event_descriptor).ok_or(())
}

pub(crate) fn monad_exec_iter_block_number_prev(
    c_event_iterator: &mut monad_event_iterator,
    c_event_ring: &monad_event_ring,
    block_number: u64,
    c_exec_event_filter: monad_exec_event_type,
) -> Result<monad_event_descriptor, ()> {
    let mut c_event_descriptor: monad_event_descriptor = unsafe { std::mem::zeroed() };

    let success = unsafe {
        self::bindings::monad_exec_iter_block_number_prev(
            c_event_iterator,
            c_event_ring,
            block_number,
            c_exec_event_filter,
            &mut c_event_descriptor,
        )
    };

    success.then_some(c_event_descriptor).ok_or(())
}

pub(crate) fn monad_exec_iter_block_id_prev(
    c_event_iterator: &mut monad_event_iterator,
    c_event_ring: &monad_event_ring,
    block_id: &monad_c_bytes32,
    c_exec_event_filter: monad_exec_event_type,
) -> Result<monad_event_descriptor, ()> {
    let mut c_event_descriptor: monad_event_descriptor = unsafe { std::mem::zeroed() };

    let success = unsafe {
        self::bindings::monad_exec_iter_block_id_prev(
            c_event_iterator,
            c_event_ring,
            block_id,
            c_exec_event_filter,
            &mut c_event_descriptor,
        )
    };

    success.then_some(c_event_descriptor).ok_or(())
}
