use std::sync::Arc;

use crate::eth_json_types::MonadNotification;

#[derive(Clone, Debug)]
pub enum EventServerEvent {
    Gap,

    MonadBlock {
        header_speculative: Arc<MonadNotification<Arc<alloy_rpc_types::eth::Header>>>,
        serialized: Arc<serde_json::Value>,
    },

    FinalizedBlock {
        header: Arc<alloy_rpc_types::eth::Header>,
        serialized: Arc<serde_json::Value>,
    },

    MonadLogs {
        header: Arc<alloy_rpc_types::eth::Header>,
        logs_speculative: Arc<
            Vec<(
                MonadNotification<Arc<alloy_rpc_types::eth::Log>>,
                Arc<serde_json::Value>,
            )>,
        >,
    },

    FinalizedLogs {
        header: Arc<alloy_rpc_types::eth::Header>,

        logs: Arc<Vec<(Arc<alloy_rpc_types::eth::Log>, Arc<serde_json::Value>)>>,
    },
}
