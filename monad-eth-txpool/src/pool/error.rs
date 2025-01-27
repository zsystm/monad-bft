#[derive(Debug)]
pub enum TxPoolInsertionError {
    NotWellFormed,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    PoolFull,
    ExistingHigherPriority,
}
