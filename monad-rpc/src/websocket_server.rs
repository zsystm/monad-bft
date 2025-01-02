use std::collections::HashMap;

use alloy_consensus::{Transaction, TxReceipt};
use alloy_primitives::U256;
use alloy_rpc_types::eth::{Filter, FilteredParams};
use monad_exec_events::{
    block_builder::{BlockBuilder, BlockUpdate, ExecutedBlockInfo},
    consensus_state_tracker::{ConsensusStateResult, ConsensusStateTracker},
    exec_event_stream::PollResult,
    exec_events::{ConsensusState, ExecEvent},
};
use monad_types::BlockId;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::{
    eth_json_types::{
        BlockCommitState, EthSubscribeResult, FixedData, SpeculativeLog, SpeculativeNewHead,
        StreamEvent, StreamItem, SubscriptionKind, SubscriptionResult,
    },
    websocket::WebSocketSessionCommand,
};

const MAX_SUBSCRIPTIONS: usize = 50;

pub struct WebSocketServer {
    // All active websocket sessions.
    sessions: HashMap<SessionId, Session>,
    max_sessions: usize,
    rx: flume::Receiver<PollResult>,
    cmd_rx: flume::Receiver<WebSocketServerCommand>,
    block_builder: BlockBuilder,
    consensus_state_tracker: ConsensusStateTracker<Box<ExecutedBlockInfo>>,
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
    commit_state: BlockCommitState,
    block_id: BlockId,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Deserialize, Serialize)]
pub struct SubscriptionId(pub FixedData<16>);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct SessionId(FixedData<16>);

enum ReferendumOutcome {
    Advanced(ConsensusState),
    Abandoned,
}

fn new_id() -> FixedData<16> {
    let mut rng = rand::thread_rng();
    let random_bytes: [u8; 16] = rng.gen();
    FixedData(random_bytes)
}

impl WebSocketServer {
    pub fn new(
        cmd_rx: flume::Receiver<WebSocketServerCommand>,
        rx: flume::Receiver<PollResult>,
        max_sessions: usize,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            rx,
            cmd_rx,
            max_sessions,
            block_builder: BlockBuilder::new(),
            consensus_state_tracker: ConsensusStateTracker::new(),
        }
    }

    pub async fn run(mut self) {
        let mut accept_connections: bool = true;
        loop {
            tokio::select! {
                Ok(poll_result) = self.rx.recv_async() => {
                    match poll_result {
                        PollResult::Disconnected => {
                            error!(
                                "event stream disconnected; stopping all websocket sessions"
                            );
                            self.disconnect_all();
                            accept_connections = false;
                        },
                        PollResult::Gap { last_read_seqno, last_write_seqno } => {
                            warn!("event stream gap detected: {last_read_seqno} -> {last_write_seqno}");
                            self.process_event_stream_item(StreamEvent::StreamGap {
                                last_read_seqno,
                                next_seqno: last_write_seqno,
                            });
                            self.block_builder.drop_block();
                            self.consensus_state_tracker.reset_all();
                        }
                        PollResult::PayloadExpired { expired_seqno, last_write_seqno, .. } => {
                            warn!("event stream has expired payload at {expired_seqno} -> {last_write_seqno}");
                            self.process_event_stream_item(StreamEvent::StreamGap {
                                last_read_seqno: expired_seqno - 1,
                                next_seqno: last_write_seqno
                            });
                            self.block_builder.drop_block();
                            self.consensus_state_tracker.reset_all();
                        }
                        PollResult::Ready{ seqno, event } => {
                            if let ExecEvent::Referendum { proposal_meta, outcome } = &event {
                                if let ConsensusStateResult::Finalization{ finalized_proposal: _, abandoned_proposals } = self.consensus_state_tracker.update_proposal(proposal_meta.block_number, &proposal_meta.id, *outcome) {
                                    for abandoned in abandoned_proposals {
                                        self.process_block_update(&abandoned.user_data, ReferendumOutcome::Abandoned);
                                        self.process_event_stream_item(StreamEvent::AbandonedProposal {
                                            proposal_meta: abandoned.proposal_meta
                                        });
                                    }
                                }

                                if let Some(opt_proposal_state) = self.consensus_state_tracker.get(proposal_meta.block_number, &proposal_meta.id) {
                                    self.process_block_update(&opt_proposal_state.user_data, ReferendumOutcome::Advanced(*outcome));
                                }
                            }

                            // TODO(ken): cloning this is a bad idea
                            self.process_event_stream_item(StreamEvent::ExecutionEvent {
                                seqno,
                                event: event.clone()
                            });

                            match self.block_builder.try_append(event) {
                                Some(BlockUpdate::Failed(failed_info)) => {
                                    error!("event stream received failed block, execution daemon is likely dead: {:?}",failed_info);
                                    self.disconnect_all();
                                    accept_connections=false;
                                }

                                Some(BlockUpdate::Executed(exec_info)) => {
                                    let proposal_meta = exec_info.proposal_meta;
                                    let consensus_state = exec_info.consensus_state;
                                    assert_eq!(consensus_state, ConsensusState::Proposed);
                                    if let ConsensusStateResult::Outstanding{ updated_proposal, .. } = self.consensus_state_tracker.add_proposal(proposal_meta, consensus_state, exec_info) {
                                        self.process_block_update(&updated_proposal.user_data,
                                            ReferendumOutcome::Advanced(ConsensusState::Proposed))
                                    }
                                },

                                implicit_drop @ Some(BlockUpdate::ImplicitDrop{ .. }) => {
                                    // Because we explicitly call BlockBuilder::drop_block, we
                                    // don't expect this to ever happen
                                    panic!("implicit_drop should never happen here, but it did: {:?}", implicit_drop.unwrap());
                                }

                                Some(BlockUpdate::OrphanedEvent(_)) | Some(BlockUpdate::NonBlockEvent(_)) | None => (),
                            }
                        }
                        PollResult::NotReady => unreachable!(),
                    }
                }
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
        self.sessions.get_mut(&conn_id).and_then(|session| {
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
        referendum_outcome: ReferendumOutcome,
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
        for txn_info in block_update.transactions.iter() {
            assert!(txn_info.is_some());
            if let Some(txn_info) = txn_info {
                let eff_gas_price = txn_info
                    .txn_envelope
                    .effective_gas_price(rpc_header.base_fee_per_gas.clone().take());
                rpc_txns.push(alloy_rpc_types::eth::Transaction {
                    inner: txn_info.txn_envelope.clone(),
                    block_hash: Some(block_update.eth_block_hash),
                    block_number: Some(rpc_header.number),
                    transaction_index: Some(txn_info.txn_index),
                    effective_gas_price: Some(eff_gas_price),
                    from: txn_info.sender,
                });
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
            .transactions
            .iter()
            .filter_map(|txn| {
                // If the transaction is None, we can skip it.
                let (receipt, tx_id, tx_index) = match txn {
                    Some(txn) => (
                        txn.receipt.clone(),
                        *txn.txn_envelope.tx_hash(),
                        txn.txn_index,
                    ),
                    None => return None,
                };

                let filtered_logs: Vec<alloy_primitives::Log> = receipt.logs().to_vec();

                let rpc_logs = filtered_logs
                    .iter()
                    .map(|log| {
                        let rpc_log = alloy_rpc_types::Log {
                            inner: log.clone(),
                            block_hash: Some(block_hash),
                            block_number: Some(block_number),
                            block_timestamp: Some(block_timestamp),
                            transaction_hash: Some(tx_id),
                            transaction_index: Some(tx_index),
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

        let commit_state = match referendum_outcome {
            ReferendumOutcome::Advanced(ConsensusState::Proposed) => BlockCommitState::Proposed,
            ReferendumOutcome::Advanced(ConsensusState::QC) => BlockCommitState::Voted,
            ReferendumOutcome::Advanced(ConsensusState::Finalized) => BlockCommitState::Finalized,
            ReferendumOutcome::Advanced(ConsensusState::Verified) => BlockCommitState::Verified,
            ReferendumOutcome::Abandoned => BlockCommitState::Abandoned,
        };

        // Publish the block and logs to all subscriptions
        self.sessions.iter().for_each(|(_, session)| {
            session.subscriptions.iter().for_each(|(id, info)| {
                let subscription_msg = PublishMessage {
                    id: *id,
                    kind: info.kind.clone(),
                    block_header: block.header.clone(),
                    logs: logs.clone(),
                    filter: info.filter.clone(),
                    commit_state,
                    block_id: BlockId(monad_types::Hash(block_update.proposal_meta.id.0 .0)),
                };

                self.publish_subscription(subscription_msg, info.conn_id);
            });
        });
    }

    fn process_event_stream_item(&mut self, event: StreamEvent) {
        self.sessions.iter().for_each(|(_, session)| {
            session.subscriptions.iter().for_each(|(id, info)| {
                if matches!(info.kind, SubscriptionKind::MonadEventStream) {
                    let _ = session
                        .conn_tx
                        .send(WebSocketSessionCommand::PublishMessage {
                            messages: vec![EthSubscribeResult {
                                subscription: id.0,
                                result: SubscriptionResult::MonadEventStream(StreamItem {
                                    protocol_version: 1,
                                    event: event.clone(),
                                }),
                            }],
                        });
                }
            })
        })
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
                if matches!(msg.commit_state, BlockCommitState::Finalized) =>
            {
                let body = EthSubscribeResult {
                    subscription: msg.id.0,
                    result: SubscriptionResult::NewHeads(msg.block_header),
                };

                let _ = ctx.send(WebSocketSessionCommand::PublishMessage {
                    messages: vec![body],
                });
            }
            SubscriptionKind::Logs if matches!(msg.commit_state, BlockCommitState::Finalized) => {
                let Some(filtered_logs) = maybe_filter_logs(msg.filter, msg.block_header, msg.logs)
                else {
                    return;
                };

                let messages = filtered_logs
                    .into_iter()
                    .map(|log| EthSubscribeResult {
                        subscription: msg.id.0,
                        result: SubscriptionResult::Logs(log),
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
                    .map(|log| EthSubscribeResult {
                        subscription: msg.id.0,
                        result: SubscriptionResult::SpeculativeLogs(SpeculativeLog {
                            log,
                            block_id: msg.block_id,
                            commit_state: msg.commit_state,
                        }),
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
