# Architectural Issues from V2 Model Integration

## Summary of Major Issues

1. Latest tracking mismatch between V1 and V2 models
2. Missing rechecker module and verification functionality
3. Missing checker methods for fault inspection
4. Removed BlockDataWithOffsets structure

## 1. Latest Tracking Mismatch between V1 and V2 Models

### Issue Description
The V2 model simplified the BlockDataReader trait to have a single `get_latest()` method without parameters, while V1 model tracks two separate "latest" values:
- `latest` (uploaded) - tracks the latest block that has been archived
- `latest_indexed` - tracks the latest block that has been indexed for transaction lookups

### Current Problem
- `IndexReaderImpl::get_latest_indexed()` calls `block_data_reader.get_latest()` which returns the uploaded latest, not the indexed latest
- `TxIndexArchiver::update_latest_indexed()` tries to call `update_latest_kind(block_num, LatestKind::Indexed)` which doesn't exist in the V2 trait
- This causes test failures in `workers::index_worker::tests` where tests expect `get_latest_indexed()` to return the indexed progress

### Failed Tests
- `test_index_blocks_stops_at_error` - panics on unwrap of None
- `test_index_worker_basic_operation` - assertion fails on indexed.is_ok()
- `test_index_blocks_with_checkpoint` - panics on unwrap of None  
- `test_index_worker_resumes_after_error` - wrong latest value returned
- `test_index_worker_handles_new_blocks` - assertion fails on indexed.is_ok()

### Potential Solutions (To Be Decided)
1. Restore LatestKind parameter to BlockDataReader trait methods
2. Make IndexReader track its own latest separately from BlockDataReader
3. Unify the concept of uploaded vs indexed latest in some other way
4. Accept that V1 and V2 models work differently and adapt tests accordingly

## 2. Missing Rechecker Module

### Issue Description
The rechecker module was deleted in HEAD but the V2 changes reference it for fault verification functionality.

### Current Problem
- `fault_fixer.rs` imports `rechecker::recheck_fault_chunk` which no longer exists
- Fault verification after fixes cannot be performed
- Tests that verify fault fixes are now incomplete

### Affected Code
- `fault_fixer.rs:80` - Verification after fixing faults
- `fault_fixer.rs:530` - Test verification of fault fixes

## 3. Missing Checker Model Methods

### Issue Description
The inspector module references methods that don't exist in the checker model:
- `find_chunk_starts_with_faults_by_replica`
- `find_chunk_starts_with_faults`

### Current Problem
- `inspector.rs` cannot compile due to missing methods
- Fault inspection functionality is broken

## 4. Removed BlockDataWithOffsets

### Issue Description
The V2 changes removed the `BlockDataWithOffsets` struct and `get_block_data_with_offsets` method.

### Current Problem
- `migrate_logs.rs` uses this removed functionality
- Had to refactor to use separate calls to `get_block_by_number` and `get_block_receipts`

### Resolution Applied
- Updated `migrate_logs.rs` to use separate method calls (lines 64-65)