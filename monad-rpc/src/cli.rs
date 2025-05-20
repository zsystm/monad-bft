use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    /// Set the mempool ipc path
    #[arg(long)]
    pub ipc_path: PathBuf,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    /// Set the address for RPC to bind to
    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    pub rpc_addr: String,

    /// Set the port number for RPC to listen
    #[arg(long, default_value_t = 8080)]
    pub rpc_port: u16,

    /// Set the node config path
    #[arg(long)]
    pub node_config: PathBuf,

    /// Enable the WebSocket server
    #[arg(long, default_value_t = false)]
    pub ws_enabled: bool,

    /// Set the port number for the WebSocket server
    #[arg(long, default_value_t = 8081)]
    pub ws_port: u16,

    /// Set the max number of requests in a batch request
    #[arg(long, default_value_t = 5000)]
    pub batch_request_limit: u16,

    /// Set the max request size in bytes (default 2MB)
    #[arg(long, default_value_t = 2_000_000)]
    pub max_request_size: usize,

    /// Set the max response size in bytes
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Otel endpoint to collect metrics data
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Allow pre EIP-155 transactions
    #[arg(long, default_value_t = false)]
    pub allow_unprotected_txs: bool,

    /// Set the max block range for eth_getLogs
    #[arg(long, default_value_t = 1000)]
    pub eth_get_logs_max_block_range: u64,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long, default_value_t = 1000)]
    pub eth_call_max_concurrent_requests: u32,

    /// Set the number of threads used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 2)]
    pub eth_call_executor_threads: u32,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 64)]
    pub eth_call_executor_fibers: u32,

    /// Set the size of the node cache when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 102400)]
    pub eth_call_executor_node_lru_size: u32,

    /// Set the gas limit for eth_call
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_call_provider_gas_limit: u64,

    /// Set the gas limit for eth_estimateGas
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_estimate_gas_provider_gas_limit: u64,

    /// Enable admin_ethCallStatistics method
    #[arg(long, default_value_t = false)]
    pub enable_admin_eth_call_statistics: bool,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 30)]
    pub eth_call_executor_queuing_timeout: u32,

    /// Set the max concurrent requests for triedb reads
    #[arg(long, default_value_t = 20_000)]
    pub triedb_max_buffered_read_requests: u32,

    /// Set the max number of concurrently executing async triedb read requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 10_000)]
    pub triedb_max_async_read_concurrency: u32,

    /// Set the max concurrent requests for triedb traversals
    #[arg(long, default_value_t = 40)]
    pub triedb_max_buffered_traverse_requests: u32,

    /// Set the max number of concurrently executing async triedb traverse requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 20)]
    pub triedb_max_async_traverse_concurrency: u32,

    /// Set the RPC rayon threadpool size
    #[arg(long, default_value_t = 1)]
    pub compute_threadpool_size: usize,

    /// Set the maximum number of finalized blocks in cache
    #[arg(long, default_value_t = 200)]
    pub max_finalized_block_cache_len: u64,

    /// Set the maximum number of voted blocks in cache
    #[arg(long, default_value_t = 3)]
    pub max_voted_block_cache_len: u64,

    /* Archive Options */
    /// Set the s3 bucket name to read archive data from
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// Set the s3 region to read archive data from
    #[arg(long)]
    pub region: Option<String>,

    /// Set the archive URL to read archive data from
    #[arg(long)]
    pub archive_url: Option<String>,

    /// Set the API key to read archive data from
    #[arg(long)]
    pub archive_api_key: Option<String>,

    /// Set the mongo url to read archive data from
    #[arg(long)]
    pub mongo_url: Option<String>,

    /// Set the mongo db name to read archive data from
    #[arg(long)]
    pub mongo_db_name: Option<String>,

    /// Use mongo index to serve eth_getLogs
    #[arg(long)]
    pub use_eth_get_logs_index: bool,

    /// Dry run using mongo index for eth_getLogs
    #[arg(long)]
    pub dry_run_get_logs_index: bool,

    /// Sets the socket path for the monad execution event server
    #[arg(long, default_value = monad_exec_events::exec_event_ctypes::EXEC_EVENT_DEFAULT_RING_PATH)]
    pub exec_event_path: PathBuf,
}
