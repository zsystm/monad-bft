use std::collections::HashMap;

use alloy_consensus::{Transaction, TxReceipt};
use alloy_primitives::U256;
use alloy_rpc_types::eth::{Filter, FilteredParams};
use monad_types::BlockId;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::{
    eth_json_types::{
        EthSubscribeResult, FixedData, SpeculativeLog, SpeculativeNewHead, SubscriptionKind,
        SubscriptionResult,
    },
    exec_update_builder::{
        BlockConsensusState, BlockPollResult, BlockUpdate, EventStreamError, ExecutedBlockInfo,
    },
    websocket::WebSocketSessionCommand,
};

const MAX_SUBSCRIPTIONS: usize = 50;

#[derive(Clone)]
pub struct WebSocketServer {
    // All active websocket sessions.
    sessions: HashMap<SessionId, Session>,
    max_sessions: usize,
    rx: flume::Receiver<BlockPollResult>,
    cmd_rx: flume::Receiver<WebSocketServerCommand>,
    pending_block_map: HashMap<BlockId, Box<ExecutedBlockInfo>>,
}

// Commands send to WebSocketServer from active websocket sessions.
#[derive(Debug)]
pub enum WebSocketServerCommand {
    AddSubscription {
        conn_id: SessionId,
        kind: SubscriptionKind,
        filter: Option<Filter>,
        res_tx: tokio::sync::oneshot::Sender<Option<SubscriptionId>>,
    },
    RemoveSubscription {
        conn_id: SessionId,
        id: SubscriptionId,
        res_tx: tokio::sync::oneshot::Sender<bool>,
    },
    AddSession {
        res_tx: tokio::sync::oneshot::Sender<Option<SessionId>>,
        conn_tx: mpsc::UnboundedSender<WebSocketSessionCommand>,
    },
    RemoveSession {
        conn_id: SessionId,
    },
}

#[derive(Clone)]
pub struct Session {
    conn_tx: mpsc::UnboundedSender<WebSocketSessionCommand>,
    subscriptions: HashMap<SubscriptionId, SubscriptionInfo>,
}

#[derive(Clone)]
struct SubscriptionInfo {
    conn_id: SessionId,
    kind: SubscriptionKind,
    filter: Option<Filter>,
}

#[derive(Debug)]
struct PublishMessage {
    id: SubscriptionId,
    kind: SubscriptionKind,
    block_header: alloy_rpc_types::eth::Header,
    logs: Vec<alloy_rpc_types::Log>,
    filter: Option<Filter>,
    commit_state: BlockConsensusState,
    block_id: BlockId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Deserialize, Serialize)]
pub struct SubscriptionId(pub FixedData<16>);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct SessionId(FixedData<16>);

fn new_id() -> FixedData<16> {
    let mut rng = rand::thread_rng();
    let random_bytes: [u8; 16] = rng.gen();
    FixedData(random_bytes)
}

impl WebSocketServer {
    pub fn new(
        cmd_rx: flume::Receiver<WebSocketServerCommand>,
        rx: flume::Receiver<BlockPollResult>,
        max_sessions: usize,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            rx,
            cmd_rx,
            max_sessions,
            pending_block_map: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut accept_connections: bool = true;
        loop {
            tokio::select! {
                    Ok(poll_result) = self.rx.recv_async() => {
                                match poll_result {
                    BlockPollResult::Error(err) => {
                        match err {
                            EventStreamError::Disconnected => {
                                error!(
                                    "event stream disconnected; stopping all websocket sessions"
                                );
                                self.disconnect_all();
                                accept_connections = false;
                            }
                            EventStreamError::Gap { last_seqno, cur_seqno } => {
                                error!(
                                    "event stream gap detected; last seqno: {}, cur seqno: {}",
                                    last_seqno, cur_seqno
                                );
                                self.disconnect_all();
                                accept_connections = false;
                            }
                            EventStreamError::PayloadExpired { payload_offset, buffer_window_start } => {
                                warn!("event stream has expired payload (offset: {}, window start: {}); skipping", payload_offset, buffer_window_start);
                            }
                            EventStreamError::ProtocolError(err) => {
                                error!("error polling for finalized block: {:?}", err);
                                self.disconnect_all();
                                accept_connections = false;
                            }
                            EventStreamError::PayloadTruncated(_) => {
                                error!("event stream payload was truncated");
                            },
                        }
                    }
                    BlockPollResult::Ready(update) => {
                        match update {
                            BlockUpdate::Failed(failed_info)=>{
                                error!("event stream received failed block, execution daemon is likely dead: {:?}",failed_info);
                                self.disconnect_all();
                                accept_connections=false;
                            }
                            BlockUpdate::ConsensusStateChanged {
                                new_state,
                                bft_block_id,
                                block_number,
                                has_untracked_proposal,
                            } => {
                                match new_state {
                                    BlockConsensusState::Abandoned | BlockConsensusState::Verified => {
                                        if let Some(exec_info) = self.pending_block_map.remove(&bft_block_id) {
                                            self.process_block_update(&exec_info, new_state);
                                        }
                                    },
                                    _ => {
                                        if let Some(exec_info) = self.pending_block_map.get(&bft_block_id).cloned() {
                                            self.process_block_update(&exec_info, new_state);
                                        }
                                    }
                                }
                            }
                            BlockUpdate::Executed {
                                consensus_state,
                                exec_info,
                            } => {
                                self.process_block_update(&exec_info, consensus_state);
                                if matches!(consensus_state, BlockConsensusState::Proposed) {
                                    self.pending_block_map.insert(exec_info.bft_block_id, exec_info);
                                }
                            }
                        }
                    }
                    BlockPollResult::NotReady => unreachable!(),
                }
            },
                    Ok(cmd) = self.cmd_rx.recv_async() => {
                        match cmd {
                            WebSocketServerCommand::AddSubscription { conn_id,kind, filter, res_tx } => {
                                let id = self.add_subscription(conn_id, kind, filter);
                                res_tx.send(id);
                            }
                            WebSocketServerCommand::RemoveSubscription {conn_id, id, res_tx } => {
                                let res = self.remove_subscription(conn_id, id);
                                res_tx.send(res);
                            }
                            WebSocketServerCommand::AddSession { res_tx, conn_tx } => {
                                if !accept_connections {
                                    res_tx.send(None);
                                } else {
                                    let conn_id = SessionId(new_id());
                                    let ok = self.add_session(conn_id, conn_tx);
                                    if ok {
                                        res_tx.send(Some(conn_id));
                                    } else {
                                        res_tx.send(None);
                                    }
                                }
                            }
                            WebSocketServerCommand::RemoveSession {conn_id} => {
                                self.sessions.remove(&conn_id);
                            }
                        }
                    }
                }
        }
    }

    fn add_subscription(
        &mut self,
        conn_id: SessionId,
        kind: SubscriptionKind,
        filter: Option<Filter>,
    ) -> Option<SubscriptionId> {
        self.sessions.get_mut(&conn_id).map_or(None, |session| {
            if session.subscriptions.len() >= MAX_SUBSCRIPTIONS {
                warn!("max subscriptions reached for session: {:?}", conn_id);
                None
            } else {
                let sub_id = SubscriptionId(new_id());
                session.subscriptions.insert(
                    sub_id,
                    SubscriptionInfo {
                        conn_id,
                        kind,
                        filter,
                    },
                );
                Some(sub_id)
            }
        })
    }

    fn remove_subscription(&mut self, conn_id: SessionId, id: SubscriptionId) -> bool {
        self.sessions
            .get_mut(&conn_id)
            .map(|session| session.subscriptions.remove(&id).is_some())
            .unwrap_or(false)
    }

    fn process_block_update(
        &mut self,
        block_update: &ExecutedBlockInfo,
        commit_state: BlockConsensusState,
    ) {
        // Given a block update, convert it to a rpc block and logs.
        let difficulty = block_update.eth_header.difficulty;
        let block_hash = block_update.eth_block_hash;
        let block_number = block_update.eth_header.number;
        let block_timestamp = block_update.eth_header.timestamp;
        // TODO(ken): we don't know the block size in bytes, since
        //    we neither export that as a field in BLOCK_END, nor
        //    do we export the full RLP of the encoded block.
        //    Either could be done if anyone cares.
        let rpc_header = alloy_rpc_types::eth::Header {
            hash: block_hash,
            inner: block_update.eth_header.clone(),
            total_difficulty: Some(difficulty),
            size: Some(U256::ZERO),
        };
        let mut rpc_txns: Vec<alloy_rpc_types::eth::Transaction> = Vec::new();
        for txn_info in block_update.txns.iter() {
            if txn_info.is_none() {
                continue;
            }
            if let Some(txn_info) = txn_info {
                let eff_gas_price = txn_info
                    .tx_header
                    .effective_gas_price(rpc_header.base_fee_per_gas.clone().take());
                rpc_txns.push(alloy_rpc_types::eth::Transaction {
                    inner: txn_info.tx_header.clone(),
                    block_hash: Some(block_update.eth_block_hash),
                    block_number: Some(rpc_header.number),
                    transaction_index: Some(txn_info.index as u64),
                    effective_gas_price: Some(eff_gas_price),
                    from: txn_info.sender,
                });
            } else {
                error!("txn_info for block {} is None", block_update.eth_block_hash);
            }
        }

        let block = alloy_rpc_types::eth::Block {
            header: rpc_header,
            uncles: vec![], // TODO(ken): not currently available, only historical?
            transactions: alloy_rpc_types::eth::BlockTransactions::Full(rpc_txns),
            withdrawals: None,
        };

        // Create a list of logs
        let mut log_idx = 0;
        let logs: Vec<alloy_rpc_types::Log> = block_update
            .txns
            .iter()
            .filter_map(|txn| {
                // If the transaction is None, we can skip it.
                let (receipt, tx_id, tx_index) = match txn {
                    Some(txn) => (
                        txn.receipt.clone(),
                        txn.tx_header.tx_hash().clone(),
                        txn.index,
                    ),
                    None => return None,
                };

                let filtered_logs: Vec<alloy_primitives::Log> =
                    receipt.logs().into_iter().cloned().collect();

                let rpc_logs = filtered_logs
                    .iter()
                    .map(|log| {
                        let rpc_log = alloy_rpc_types::Log {
                            inner: log.clone(),
                            block_hash: Some(block_hash),
                            block_number: Some(block_number),
                            block_timestamp: Some(block_timestamp),
                            transaction_hash: Some(tx_id),
                            transaction_index: Some(tx_index.try_into().unwrap_or_default()),
                            log_index: Some(log_idx),
                            ..Default::default()
                        };
                        log_idx += 1;
                        rpc_log
                    })
                    .collect::<Vec<alloy_rpc_types::Log>>();
                Some(rpc_logs)
            })
            .flatten()
            .collect();

        // Publish the block and logs to all subscriptions
        self.sessions.iter().for_each(|(_, session)| {
            session.subscriptions.iter().for_each(|(id, info)| {
                let subscription_msg = PublishMessage {
                    id: *id,
                    kind: info.kind.clone(),
                    block_header: block.header.clone(),
                    logs: logs.clone(),
                    filter: info.filter.clone(),
                    commit_state: commit_state.clone(),
                    block_id: block_update.bft_block_id,
                };

                self.publish_subscription(subscription_msg, info.conn_id);
            });
        });
    }

    fn add_session(
        &mut self,
        conn_id: SessionId,
        conn_tx: mpsc::UnboundedSender<WebSocketSessionCommand>,
    ) -> bool {
        if self.sessions.len() >= self.max_sessions {
            warn!("max sessions reached, not adding new session");
            return false;
        };

        self.sessions.insert(
            conn_id,
            Session {
                conn_tx,
                subscriptions: HashMap::new(),
            },
        );
        true
    }

    fn disconnect_all(&mut self) {
        self.sessions.iter().for_each(|(_, session)| {
            let _ = session.conn_tx.send(WebSocketSessionCommand::Disconnect {});
        });
    }

    fn publish_subscription(&self, msg: PublishMessage, session: SessionId) {
        debug!("Handling subscription message: {:?}", msg);

        let ctx = match self.sessions.get(&session) {
            Some(ctx) => &ctx.conn_tx,
            None => {
                error!("session not found for id: {:?}", session);
                return;
            }
        };

        match msg.kind {
            SubscriptionKind::NewHeads
                if matches!(msg.commit_state, BlockConsensusState::Finalized) =>
            {
                let body = EthSubscribeResult {
                    subscription: msg.id.0,
                    result: SubscriptionResult::NewHeads(msg.block_header),
                };

                let _ = ctx.send(WebSocketSessionCommand::PublishMessage {
                    messages: vec![body],
                });
            }
            SubscriptionKind::Logs
                if matches!(msg.commit_state, BlockConsensusState::Finalized) =>
            {
                let Some(filtered_logs) = maybe_filter_logs(msg.filter, msg.block_header, msg.logs)
                else {
                    return;
                };

                let messages = filtered_logs
                    .into_iter()
                    .map(|log| {
                        let body = EthSubscribeResult {
                            subscription: msg.id.0,
                            result: SubscriptionResult::Logs(log),
                        };
                        body
                    })
                    .collect();
                let _ = ctx.send(WebSocketSessionCommand::PublishMessage { messages });
            }
            SubscriptionKind::MonadNewHeads => {
                let body = EthSubscribeResult {
                    subscription: msg.id.0,
                    result: SubscriptionResult::SpeculativeNewHeads(SpeculativeNewHead {
                        header: msg.block_header,
                        block_id: msg.block_id,
                        commit_state: msg.commit_state,
                    }),
                };

                let _ = ctx.send(WebSocketSessionCommand::PublishMessage {
                    messages: vec![body],
                });
            }
            SubscriptionKind::MonadLogs => {
                let Some(filtered_logs) = maybe_filter_logs(msg.filter, msg.block_header, msg.logs)
                else {
                    return;
                };

                let messages = filtered_logs
                    .into_iter()
                    .map(|log| {
                        let body = EthSubscribeResult {
                            subscription: msg.id.0,
                            result: SubscriptionResult::SpeculativeLogs(SpeculativeLog {
                                log,
                                block_id: msg.block_id,
                                commit_state: msg.commit_state.clone(),
                            }),
                        };
                        body
                    })
                    .collect();
                let _ = ctx.send(WebSocketSessionCommand::PublishMessage { messages });
            }
            _ => {}
        };
    }
}

fn maybe_filter_logs(
    filter: Option<Filter>,
    header: alloy_rpc_types::eth::Header,
    logs: Vec<alloy_rpc_types::Log>,
) -> Option<Vec<alloy_rpc_types::Log>> {
    match filter {
        Some(filter) => {
            // Before doing any work, check if the block's bloom filter matches the filter.
            if FilteredParams::matches_address(
                header.logs_bloom,
                &FilteredParams::address_filter(&filter.address),
            ) && FilteredParams::matches_topics(
                header.logs_bloom,
                &FilteredParams::topics_filter(&filter.topics),
            ) {
                let filtered_params = FilteredParams::new(Some(filter));

                // Filter all logs
                let logs: Vec<alloy_rpc_types::Log> = logs
                    .into_iter()
                    .filter_map(|log| {
                        if !(filtered_params.filter.is_some()
                            && (!filtered_params.filter_address(&log.address())
                                || !filtered_params.filter_topics(log.topics())))
                        {
                            Some(log)
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(logs)
            } else {
                // The block's bloom filter doesn't match the filter, so we can skip this block.
                None
            }
        }
        None => {
            // Return all logs in the block
            Some(logs)
        }
    }
}
