use thiserror::Error;

#[derive(Error, Debug)]
pub enum EthTxPoolEventLoopError {
    #[error("PoisonError")]
    PoisonError,
}
