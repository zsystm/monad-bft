use std::collections::HashSet;

use alloy_primitives::{Address, TxHash, B256};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EthTxPoolEvent {
    pub tx_hash: B256,
    pub action: EthTxPoolEventAction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EthTxPoolEventAction {
    /// The tx was inserted into the txpool's (pending/tracked) tx list.
    Insert {
        address: Address,
        owned: bool,
        tracked: bool,
    },

    /// The tx was dropped for the attached reason.
    Drop { reason: EthTxPoolDropReason },

    /// The tx was promoted from the txpool's pending tx list to it's tracked tx list.
    Promoted,

    /// The tx was committed and is thus finalized.
    Commit,

    /// The tx timed out and was evicted.
    Evict { reason: EthTxPoolEvictReason },
}

// allow for more fine grain debugging if needed
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionError {
    InvalidChainId,
    MaxPriorityFeeTooHigh,
    InitCodeLimitExceeded,
    GasLimitTooLow,
    GasLimitTooHigh,
    UnsupportedTransactionType,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolDropReason {
    NotWellFormed(TransactionError),
    InvalidSignature,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    ExistingHigherPriority,
    ReplacedByHigherPriority,
    PoolFull,
    PoolNotReady,
    Internal(EthTxPoolInternalDropReason),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolInternalDropReason {
    StateBackendError,
    NotReady,
}

impl EthTxPoolDropReason {
    pub fn as_user_string(&self) -> String {
        match self {
            EthTxPoolDropReason::NotWellFormed(err) => match err {
                TransactionError::InvalidChainId => "Invalid chain ID",
                TransactionError::MaxPriorityFeeTooHigh => "Max priority fee too high",
                TransactionError::InitCodeLimitExceeded => "Init code size limit exceeded",
                TransactionError::GasLimitTooLow => "Gas limit too low",
                TransactionError::GasLimitTooHigh => "Exceeds block gas limit",
                TransactionError::UnsupportedTransactionType => {
                    "EIP4844 and EIP7702 transactions unsupported"
                }
            },
            EthTxPoolDropReason::InvalidSignature => "Transaction signature is invalid",
            EthTxPoolDropReason::NonceTooLow => "Transaction nonce too low",
            EthTxPoolDropReason::FeeTooLow => "Transaction fee too low",
            EthTxPoolDropReason::InsufficientBalance => "Signer had insufficient balance",
            EthTxPoolDropReason::PoolFull => "Transaction pool is full",
            EthTxPoolDropReason::ExistingHigherPriority => {
                "An existing transaction had higher priority"
            }
            EthTxPoolDropReason::ReplacedByHigherPriority => {
                "A newer transaction had higher priority"
            }
            EthTxPoolDropReason::PoolNotReady => "Transaction pool is not ready",
            EthTxPoolDropReason::Internal(_) => "Internal error",
        }
        .to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolEvictReason {
    Expired,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxPoolSnapshot {
    pub pending: HashSet<TxHash>,
    pub tracked: HashSet<TxHash>,
}
