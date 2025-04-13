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
    /// for final commitment, see:
    ///   https://docs.monad.xyz/monad-arch/consensus/monad-bft
    QC,

    /// A super-majority stake has passed a referendum finalizing the block
    /// for commitment to a Monad-protocol blockchain
    Finalized,

    /// A super-majority stake has passed a referendum verifying that the
    /// state effects of block execution are agreed upon. Conceptually,
    /// this state does not exist in Ethereum, see:
    ///   https://docs.monad.xyz/monad-arch/consensus/asynchronous-execution
    Verified,
}

/// Unique identifier for a block proposal; until the consensus algorithm
/// concludes, multiple block proposals may compete to become the next
/// block; the competing proposals are distinguishable by this ID
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MonadBlockId(pub B256);

/// Attributes of a block proposal used by the consensus algorithm
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ProposalMetadata {
    pub round: u64,
    pub epoch: u64,
    pub block_number: u64,
    pub id: MonadBlockId,
    pub parent_round: u64,
    pub parent_id: MonadBlockId,
}

/// Fields of an Ethereum block header which are known at the start of
/// execution
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct EthBlockExecInput {
    pub parent_hash: B256,              // H_p: parent block hash
    pub ommers_hash: B256,              // H_o: hash of ommer blocks
    pub beneficiary: Address,           // H_c: recipient addr of prio gas fees
    pub transactions_root: B256,        // H_t: hash of block txn list
    pub difficulty: u64,                // H_d: PoW difficulty scaling param
    pub number: u64,                    // H_i: # of ancestor blocks ("height")
    pub gas_limit: u64,                 // H_l: max gas expenditure we're allowed
    pub timestamp: u64,                 // H_s: UNIX epoch timestamp of block inception
    pub extra_data: Bytes,              // H_x: extra metadata about this block
    pub prev_randao: B256,              // H_a: source of randomness
    pub nonce: B64,                     // H_n: PoW puzzle solution; now zero
    pub base_fee_per_gas: Option<U256>, // H_f: wei burned per unit gas
    pub withdrawals_root: Option<B256>, // H_w: consensus-initiated withdrawals
    pub transaction_count: u64,         // Number of transactions in block
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CallFrame {
    pub opcode: u8,
    pub caller: Address,
    pub call_target: Address,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub evmc_status_code: i32,
    pub depth: u64,
    pub input: Bytes,
    pub return_value: Bytes,
}

/// Each kind of execution event is a variant of this enumeration type; where
/// possible, the variants use alloy types. Our own types are used only for
/// Monad-specific concepts, such as the states in Monad's consensus algorithm,
/// which is different from Ethereum
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ExecEvent {
    BlockStart {
        consensus_state: ConsensusState,
        proposal_meta: ProposalMetadata,
        chain_id: alloy_primitives::ChainId,
        exec_input: EthBlockExecInput,
    },

    BlockEnd {
        eth_block_hash: B256,
        state_root: B256,                         // H_r: MPT root hash of state trie
        receipts_root: B256,                      // H_e: MPT root hash of receipt trie
        logs_bloom: Box<alloy_primitives::Bloom>, // H_b: bloom filter of transaction logs
        gas_used: u64,                            // H_g: gas used by all txns in block
    },

    BlockReject {
        reject_code: u32,
    },

    Referendum {
        proposal_meta: ProposalMetadata,
        outcome: ConsensusState,
    },

    TransactionStart {
        index: u64,
        sender: Address,
        tx_envelope: alloy_consensus::TxEnvelope,
    },

    TransactionLog {
        index: u64,
        log: alloy_primitives::Log,
    },

    TransactionReceipt {
        index: u64,
        status: alloy_consensus::Eip658Value,
        log_count: usize,
        call_frame_count: usize,
        tx_gas_used: u128,
    },

    TransactionCallFrame {
        index: u64,
        call_frame: CallFrame,
    },

    TransactionReject {
        index: u64,
        reject_code: u32,
    },

    EvmError {
        domain_id: u64,
        status_code: i64,
        tx_id: u64,
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
