use monad_state_backend::StateBackendError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EthTxPoolEventLoopError {
    #[error("PoisonError")]
    PoisonError,

    #[error(transparent)]
    StateBackendError(#[from] StateBackendError),
}
