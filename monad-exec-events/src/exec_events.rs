//! This module defines Rust types modeling execution events. An execution
//! event is a notification that the Ethereum Virtual Machine has performed
//! some action, such as emitting a transaction log or changing an account
//! balance.
//!
//! The types in this module are typically constructed from data in more "raw"
//! event structures (defined in the `exec_event_ctypes.rs` module) which are
//! C layout compatible. The C layout types are typically being streamed to us
//! over a shared memory communication channel, connected to the (C++)
//! execution daemon running in a separate process; within Rust, their
//! #[repr(C)] wrappers are mostly suitable for unsafe or zero-copy usage.
//!
//! By contrast, the types in this module are more ergonomic to work with in
//! native Rust programs. A utility in the `exec_event_stream.rs` module is
//! used to build these Rust-native types from their corresponding C inputs.

use alloy_primitives::{Address, Bytes, B256, B64, U256};

/// Models the level of confidence we have in a block, according to the
/// Monad network's consensus algorithm
#[derive(
    Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ConsensusState {
    /// No consensus; the leader of a MonadBFT round has proposed a block, and
    /// it appears valid according to the local node, but the initial consensus
    /// vote has not concluded yet
    Proposed,

    /// A vote has concluded with a super-majority stake passing a referendum
    /// on the block for inclusion, creating a Quorum Certificate; this is
    /// unlikely to reverted, but an additional round of voting is still needed
    /// for final commitment. For details see [MonadBFT]
    ///
    /// [MonadBFT] https://docs.monad.xyz/monad-arch/consensus/monad-bft
    QC,

    /// A super-majority stake has passed a referendum finalizing the block
    /// for commitment to a Monad-protocol blockchain
    Finalized,

    /// A super-majority stake has passed a referendum verifying that the
    /// state effects of block execution are agreed upon. Conceptually,
    /// this state does not exist in Ethereum, see [Asynchronous Execution]
    ///
    /// [Asynchronous Execution] https://docs.monad.xyz/monad-arch/consensus/asynchronous-execution
    Verified,
}

/// Unique identifier for a block proposal; until the consensus algorithm
/// concludes, multiple block proposals may compete to become their
/// specified block number; the competing proposals are distinguishable by
/// this unique ID
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MonadBlockId(pub B256);

/// Attributes of a block proposal used by the consensus algorithm
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ProposalMetadata {
    /// Consensus round when the block was proposed
    pub round: u64,
    /// Consensus epoch when the block was proposed
    pub epoch: u64,
    /// If the proposal is accepted, the block will be commited to the
    /// blockchain with this height
    pub block_number: u64,
    /// Unique ID that distinguishes different block proposals competing
    /// for the same block_number
    pub id: MonadBlockId,
    /// Consensus round in which the parent block was originally proposed
    pub parent_round: u64,
    /// Unique ID of the parent block
    pub parent_id: MonadBlockId,
}

/// Fields of an Ethereum block header which are known at the _start_ of block
/// execution. This is a subset of the canonical Ethereum header, because the
/// full header includes fields (e.g., state_root) which are not known until
/// execution completes. By contrast, the execution event system reports these
/// "execution input" fields as soon as the EVM begins executing a block,
/// long before the outputs will be known
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EthBlockExecInput {
    /// H_p: parent block hash
    pub parent_hash: B256,
    /// H_o: hash of ommer blocks
    pub ommers_hash: B256,
    /// H_c: recipient address of priority gas fees
    pub beneficiary: Address,
    /// H_t: hash of block transaction list
    pub transactions_root: B256,
    /// H_d: Proof-of-work difficulty scaling parameter
    pub difficulty: u64,
    /// H_i: number of ancestor blocks ("block height")
    pub number: u64,
    /// H_l: max gas expenditure allowed for the full block
    pub gas_limit: u64,
    /// H_s: UNIX epoch timestamp of block inception
    pub timestamp: u64,
    /// H_x: extra metadata about this block
    pub extra_data: Bytes,
    /// H_a: source of randomness
    pub prev_randao: B256,
    /// H_n: historical proof-of-work puzzle solution; now zero
    pub nonce: B64,
    /// H_f: wei burned per unit gas (EIP-1559)
    pub base_fee_per_gas: Option<U256>,
    /// H_w: consensus-initiated withdrawals (EIP-4895)
    pub withdrawals_root: Option<B256>,
    /// Number of transactions in block; this is the number of
    /// ExecEvent::TransactionStart events that should follow
    pub transaction_count: u64,
}

/// Trace information about an execution context that was created during an EVM
/// contract invocation ("call"), or contract creation.
///
/// Formally, the EVM operates through concepts called 'message calls' and
/// 'contract creations'. Each of these defines an execution environment, which
/// contains data such as the account causing the code to execute. A formal list
/// of all the items in the environment is part of the official specification.
///
/// Each call (and contract creation) gets its own environment. The environments
/// are set up in different ways, depending on how the call occurs (e.g., a CALL
/// vs. DELEGATECALL opcode). A call frame is a summary of the inputs and
/// outputs to an execution environment, whether the halting was normal or
/// exceptional, and other information useful for tracing the call tree.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CallFrame {
    /// EVM opcode of instruction that created this call frame
    pub opcode: u8,

    /// Address initiating call; in Geth tracers this is called "from". For
    /// CALL and STATICCALL, it is sender ("caller"); for other call types,
    /// it is the contextual address of the call (i.e., the address whose
    /// storage is implicitly accesses by instructions like SLOAD / SSTORE
    pub caller: Address,

    /// Target address receiving the call; in Geth tracers this is called "to"
    /// and its meaning follows that convention
    pub call_target: Address,

    /// I_v: value passed to account during execution
    pub value: U256,

    /// g: gas available for message execution
    pub gas: u64,

    /// Gas used by call
    pub gas_used: u64,

    /// Result of executing this frame, communicated as the value of the evmc
    /// type `enum evmc_status_code`
    pub evmc_status_code: i32,

    /// Call depth at which this frame was recorded; because call frames
    /// are recorded in depth-first order, the depth can be used to
    /// reconstruct the call tree (the latest frame call at `depth - 1`
    /// is the enclosing execution context)
    pub depth: u64,

    /// Call input
    pub input: Bytes,

    /// Call return value
    pub return_value: Bytes,
}

/// Account access events are recorded in some particular context, most
/// commonly in the context of a transaction that reads or writes state;
/// other execution contexts that touch accounts are block-level system
/// operations that occur outside the scope of any transaction
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AccountAccessContext {
    /// System-level account access that occurs prior to the execution of
    /// any transaction execution such as the DAO hard fork balance
    /// transfers and [EIP-4788] when running in Ethereum compatibility mode
    ///
    /// [EIP-4788] https://eips.ethereum.org/EIPS/eip-4788
    BlockPrologue,

    /// Account access that occurs in the scope of a transaction with the
    /// given index
    Transaction(u64),

    /// System-level account access that occurs after transaction processing
    /// has finished, e.g., block rewards and [EIP-4895] withdrawals
    ///
    /// [EIP-4895] https://github.com/ethereum/EIPs/blob/master/EIPS/eip-4895.md
    BlockEpilogue,
}

/// Every account that is accessed by the EVM emits one of these records,
/// which the code hash, initial and potentially changed values of the
/// balance and nonce, and the number of subsequent StorageAccess events
/// that will be reported
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AccountAccess {
    /// Address of account being accessed
    pub address: Address,

    /// `sigma[a]_n`: nonce value of account when the [`AccountAccessContext`]
    /// is first pushed
    pub original_nonce: u64,

    /// Value of the nonce at the end of the scope for the active
    /// [`AccountAccessContext`] or `None` if the nonce has not changed; for
    /// contract accounts which are explicitly destroyed, e.g., via
    /// `SELFDESTRUCT`, this will be 0 (EIP-161).
    pub modified_nonce: Option<u64>,

    /// `sigma[a]_b`: native token balance of account when the
    /// [`AccountAccessContext`] is first pushed
    pub original_balance: U256,

    /// Value of the nonce at the end of the scope for the active
    /// [`AccountAccessContext`] or `None` if the balance has not changed
    pub modified_balance: Option<U256>,

    /// `sigma[a]_c`: EVM code hash
    pub code_hash: B256,

    /// Number of subsequent storage access events that will be reported
    /// for `SLOAD` / `SSTORE` operations
    pub storage_key_count: u32,

    /// Number of subsequent storage access events that will be reported
    /// for `TLOAD` / `TSTORE` operations
    pub transient_key_count: u32,
}

/// Every storage slot (regular and transient) that is accessed by the EVM
/// emits one of these records, which summarizes
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageAccess {
    /// Address whose storage was read or written
    pub address: Address,

    /// True -> event is from `TLOAD`/`TSTORE`, false -> `SLOAD`/`SSTORE`
    pub is_transient: bool,

    /// Storage key
    pub key: B256,

    /// Value of storage slot when the [`AccountAccessContext`] is first pushed
    pub original_value: B256,

    /// Value of the storage slot at the end of the scope for the active
    /// [`AccountAccessContext`] or `None` if the slot has not changed
    pub modified_value: Option<B256>,
}

/// Each kind of execution event is a variant of this enumeration type; where
/// possible, the variants use alloy types. Our own types are used only for
/// Monad-specific concepts, such as the states in Monad's consensus algorithm,
/// which is different from Ethereum.
///
/// Each event reports something happening inside the EVM. The Category Labs
/// EVM does not execute blocks but "block proposals": speculative executions
/// of blocks that _may or may not_ become part of the finalized Monad
/// blockchain, as the consensus algorithm runs in parallel and decides. Thus,
/// these events may not actually "happen."
///
/// To tell if an event will happen, note the proposal metadata communicated
/// in the [`ExecEvent::BlockStart`] variant. All subsequent events until
/// [`ExecEvent::BlockEnd`] (or one of the error events) are scoped to this
/// block. At some time later, a [`ExecEvent::Referendum`] event will
/// communicate the result of the consensus algorithm for each proposal you
/// have seen.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ExecEvent {
    /// Event that marks the start of a new block proposal
    BlockStart {
        /// Consensus state the block proposal was in at the time of execution
        consensus_state: ConsensusState,
        /// Describes and uniquely identifies the block proposal being executed
        proposal_meta: ProposalMetadata,
        /// Blockchain associated with all events in this block
        chain_id: alloy_primitives::ChainId,
        /// Execution input
        exec_input: EthBlockExecInput,
    },

    /// Event the marks the end of a successful execution of a block proposal
    BlockEnd {
        /// Computed Ethereum block hash, KEK(RLP(B))
        eth_block_hash: B256,
        /// H_r: MPT root hash of state trie
        state_root: B256,
        /// H_e: MPT root hash of receipt trie
        receipts_root: B256,
        /// H_b: bloom filter of transaction logs
        logs_bloom: Box<alloy_primitives::Bloom>,
        /// H_g: gas used by all txns in block
        gas_used: u64,
    },

    /// Event emitted when block execution did not complete successfully,
    /// because of a validation error
    BlockReject {
        /// A value in the `BlockError` enumeration in `validate_block.hpp`
        reject_code: u32,
    },

    /// Event emitted when the consensus algorithm has conducted a vote and
    /// changed the state of a block proposal that was seen previously. Note
    /// that the consensus algorithm does not explicitly abandon proposals; if
    /// multiple proposals compete to become block number `N`, a single
    /// [`ExecEvent::Referendum`] event will finalize exactly one of them,
    /// _implicitly_ abandoning the others. No event is produced for the failed
    /// proposals. To receive notifications about abandonment, you can use the
    /// utilities from the `consensus_state_tracker.rs` module
    Referendum {
        /// Block proposal this referendum relates to
        proposal_meta: ProposalMetadata,
        /// The new consensus state of the block proposal, as set by the referendum
        outcome: ConsensusState,
    },

    /// Event that marks the start of execution for a particular transaction
    TransactionStart {
        /// Index of transaction; all subsequent events related to this
        /// transaction will be tied together using this index. The events
        /// of different transactions (e.g., the logs of transaction 1 and 2)
        /// are _not_ necessarily recorded in order, for performance reasons.
        /// _Within_ a single transaction, all events have a defined order
        txn_index: u64,

        /// Address of account sending the transaction
        sender: Address,

        /// Definition of the transaction
        txn_envelope: alloy_consensus::TxEnvelope,
    },

    /// Event emitted when transaction execution did not complete successfully,
    /// because of a validation error. This is _not_ an exceptional halt of the
    /// EVM (e.g., due to running out of gas) but is instead a more severe
    /// error which should not happen. A valid block should not contain an
    /// invalid transaction. Exceptional halts, by contrast, are normal EVM
    /// behavior that can occur for a valid transaction, cause the well-defined
    /// revert behavior, and report a receipt with an EIP-658 code noting the
    /// halt. A TransactionReject event is a more unexpected condition that
    /// a properly-functioning input validation should have detected and never
    /// tried to execute; they cause the current block to implicitly abort
    /// execution entirely
    TransactionReject {
        /// Index of transaction which failed to validate
        txn_index: u64,
        /// A value in the `TransactionError` enumeration in
        /// `validate_transaction.hpp`
        reject_code: u32,
    },

    /// First event emitted when a transaction completes execution; subsequent
    /// events related to the transaction (logs, call frames, state changes,
    /// etc.) are reported occur after this one. This event serves as a "header"
    /// of sorts, and announcing the size of the arrays of subsequent events
    /// (e.g., the number of logs in the `log_count` field), so that the user
    /// can preallocate memory if desired, or decide to filter out those
    /// subsequent events if they're not interesting. The transaction-level
    /// state changes are reported after using a different header event,
    /// [`ExecEvent::AccountAccessListHeader`], whose [`AccountAccessContext`]
    /// has the same transaction index
    TransactionReceipt {
        /// Receipt is for this transaction
        txn_index: u64,
        /// Reports if transaction execution was successful or halted exceptionally
        status: alloy_consensus::Eip658Value,
        /// Number of [`ExecEvent::TransactionLog`] events that will follow
        log_count: usize,
        /// Number of [`ExecEvent::CallFrame`] events that will follow
        call_frame_count: usize,
        /// Incremental (not cumulative!) gas used by this transaction; because
        /// transactions are potentially recorded out-of-order, cumulative
        /// transaction gas usage must be computed manually when the block ends
        #[serde(with = "alloy_rpc_types::serde_helpers::quantity")]
        txn_gas_used: u128,
    },

    /// Event emitted for a transaction log
    TransactionLog {
        txn_index: u64,
        log_index: u32,
        log: alloy_primitives::Log,
    },

    /// Event emitted for a transaction call frame
    TransactionCallFrame {
        txn_index: u64,
        call_frame_index: u32,
        call_frame: CallFrame,
    },

    /// "Header" event which announces the number of [`ExecEvent::AccountAccess`]
    /// events that will immediately follow, and what [`AccountAccessContext`]
    /// they occurred in. For account accesses that occur in the scope of a
    /// transaction ([`AccountAccessContext::Transaction`]), this event will be
    /// emitted after the [`ExecEvent::TransactionReceipt`].
    AccountAccessListHeader {
        access_context: AccountAccessContext,
        entry_count: u32,
    },

    /// Event emitted each time an account is accessed in some particular
    /// context (i.e., within the scope of a particular transaction, or directly
    /// at the end of the block to pay rewards). This reports the state, along
    /// with any modifications, to the scalar fields of the account. It also
    /// serves as a "header" event, reporting the number of StorageAccess
    /// events that will be reported immediately after
    AccountAccess {
        access_context: AccountAccessContext,
        account_index: u32,
        access_info: AccountAccess,
    },

    /// Event emitted each time a storage (or transient storage) slot is
    /// accessed in some particular context (i.e., within the scope of a
    /// particular transaction). This reports the storage value for the
    /// slit, along with any modification
    StorageAccess {
        access_context: AccountAccessContext,
        account_index: u32,
        storage_index: u32,
        access_info: StorageAccess,
    },

    /// Event that reports an error that is not part of the defined
    /// behavior of the EVM, but an internal error in the execution daemon;
    /// the fields of this variant are for debugging only, and have no
    /// defined meaning in the protocol
    EvmError {
        domain_id: u64,
        status_code: i64,
        txn_id: u64,
    },
}

impl std::str::FromStr for ConsensusState {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "proposed" => Ok(ConsensusState::Proposed),
            "qc" => Ok(ConsensusState::QC),
            "finalized" => Ok(ConsensusState::Finalized),
            "verified" => Ok(ConsensusState::Verified),
            _ => Err(()),
        }
    }
}
