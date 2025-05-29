use crate::{
    monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_c_eth_txn_receipt,
    monad_exec_block_header, monad_exec_block_result,
};

/// Block reconstructed from execution events.
#[allow(missing_docs)]
#[derive(Debug)]
pub struct ExecutedBlock {
    pub header: monad_exec_block_header,
    pub execution_result: monad_exec_block_result,
    pub txns: Box<[ExecutedTxn]>,
}

/// Transaction reconstructed from execution events.
#[allow(missing_docs)]
#[derive(Debug)]
pub struct ExecutedTxn {
    pub hash: monad_c_bytes32,
    pub sender: monad_c_address,
    pub header: monad_c_eth_txn_header,
    pub input: Box<[u8]>,
    pub logs: Box<[ExecutedTxnLog]>,
    pub receipt: monad_c_eth_txn_receipt,
}

/// Transaction log reconstructed from execution events.
#[allow(missing_docs)]
#[derive(Debug)]
pub struct ExecutedTxnLog {
    pub address: monad_c_address,
    pub topic: Box<[u8]>,
    pub data: Box<[u8]>,
}

/// Result produced by [`BlockBuilder`](crate::BlockBuilder) while attempting to reassemble a block
/// from execution events.
///
/// This type is generic so it can be shared with
/// [`ConsensusStateTracker`](crate::ConsensusStateTracker) to produce reassembled block updates
/// along with the block's commit state.
pub enum BlockBuilderResult<T> {
    /// Contains the success value.
    Ok(T),

    /// The block was rejected by execution.
    Rejected,

    /// The payload expired while reading the event.
    PayloadExpired,

    /// The block cannot be correctly reassembled.
    ///
    /// This variant is produced when the [`BlockBuilder`](crate::BlockBuilder) is not given the
    /// entire sequential stream of event descriptors containing execution events from some block's
    /// initial [`monad_exec_block_header`] event to the final [`monad_exec_block_result`] event.
    /// This can occur if the user skips, duplicates, or reorders event descriptors or, even worse,
    /// provides event descriptors from different event rings. The most likely of these scenarios
    /// arises if the upstream event ring gaps and
    /// [`BlockBuilder::reset`](crate::BlockBuilder::reset) is not called during the user's event
    /// recovery phase.
    ImplicitDrop {
        /// Header of the block being reassembled.
        block: monad_exec_block_header,
        /// Explains why the block could not be reassembled.
        reassembly_error: ReassemblyError,
    },
}

impl<T> BlockBuilderResult<T> {
    /// Maps the [`BlockBuilderResult::Ok`] branch value.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> BlockBuilderResult<U> {
        match self {
            Self::Ok(value) => BlockBuilderResult::Ok(f(value)),
            Self::Rejected => BlockBuilderResult::Rejected,
            Self::PayloadExpired => BlockBuilderResult::PayloadExpired,
            Self::ImplicitDrop {
                block,
                reassembly_error,
            } => BlockBuilderResult::ImplicitDrop {
                block,
                reassembly_error,
            },
        }
    }
}

/// Error produced by [`BlockBuilder`](crate::BlockBuilder) while attempting to reassemble a block
/// from execution events.
#[derive(Clone, Debug)]
pub enum ReassemblyError {
    /// A block header event was received while the current block was still being reassembled.
    ///
    /// It is guaranteed by monad-execution that all events for a given block begin with a
    /// [`monad_exec_block_header`] and end with a [`monad_exec_block_result`], and that events for
    /// different blocks do not interleave. This implies that processing another block header before
    /// processing the previous block's block result means the block builder missed an event.
    UnterminatedBlock {
        /// The unexpected block header.
        unexpected_header: monad_exec_block_header,
    },

    /// Transaction with the given index is missing
    MissingTransaction {
        /// The missing transaction index.
        index: u64,
    },

    /// Transaction with the given index does not have the expected number
    /// of logs
    MissingTransactionLog {
        /// Missing index
        index: u64,
        /// The log count expected from the transaction receipt.
        expected_log_count: usize,
        /// The actual number of log events received.
        actual_log_count: usize,
    },
}
