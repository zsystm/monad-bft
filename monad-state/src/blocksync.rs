use std::{marker::PhantomData, time::Duration};

use monad_blocksync::blocksync::{
    BlockCache, BlockSync, BlockSyncCommand, BlockSyncSelfRequester, BlockSyncWrapper,
};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    block::BlockPolicy, block_validator::BlockValidator, metrics::StateMetrics,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, Command, ConsensusEvent, LedgerCommand, LoopbackCommand, MonadEvent,
    RouterCommand, StateSyncEvent, TimeoutVariant, TimerCommand,
};
use monad_metrics::MetricsPolicy;
use monad_state_backend::StateBackend;
use monad_types::{ExecutionProtocol, NodeId, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager, validator_set::ValidatorSetTypeFactory,
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::{ConsensusMode, MonadState, VerifiedMonadMessage};

pub(super) struct BlockSyncChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, MP>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    MP: MetricsPolicy,
{
    block_sync: &'a mut BlockSync<ST, SCT, EPT>,

    /// BlockSync queries consensus first when receiving BlockSyncRequest
    consensus: &'a ConsensusMode<ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    delta: &'a Duration,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,

    metrics: &'a mut StateMetrics<MP>,

    _phantom: PhantomData<(ST, SCT, EPT, BPT, SBT, VTF, LT, BVT)>,
}

impl<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, MP>
    BlockSyncChildState<'a, ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, MP>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<ST, SCT, EPT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    MP: MetricsPolicy,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, EPT, BPT, SBT, VTF, LT, BVT, CCT, CRT, MP>,
    ) -> Self {
        Self {
            block_sync: &mut monad_state.block_sync,
            consensus: &monad_state.consensus,
            epoch_manager: &monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,
            delta: &monad_state.consensus_config.delta,
            nodeid: &monad_state.nodeid,
            metrics: &mut monad_state.metrics,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(
        &mut self,
        event: BlockSyncEvent<ST, SCT, EPT>,
    ) -> Vec<WrappedBlockSyncCommand<ST, SCT, EPT>> {
        let block_cache = match self.consensus {
            ConsensusMode::Sync { block_buffer, .. } => {
                BlockCache::BlockBuffer(block_buffer.get_payload_cache())
            }
            ConsensusMode::Live(consensus) => BlockCache::BlockTree(consensus.blocktree()),
        };

        let mut block_sync_wrapper = BlockSyncWrapper {
            block_sync: self.block_sync,
            block_cache,
            metrics: &mut self.metrics.blocksync_events,
            nodeid: self.nodeid,
            current_epoch: self.consensus.current_epoch(),
            epoch_manager: self.epoch_manager,
            val_epoch_map: self.val_epoch_map,
        };

        let cmds = match event {
            BlockSyncEvent::Request { sender, request } => {
                block_sync_wrapper.handle_peer_request(sender, request)
            }
            BlockSyncEvent::SelfRequest {
                requester,
                block_range,
            } => block_sync_wrapper.handle_self_request(requester, block_range),
            BlockSyncEvent::SelfCancelRequest {
                requester,
                block_range,
            } => {
                block_sync_wrapper.handle_self_cancel_request(requester, block_range);
                Vec::new()
            }
            BlockSyncEvent::SelfResponse { response } => {
                block_sync_wrapper.handle_ledger_response(response)
            }
            BlockSyncEvent::Response { sender, response } => {
                block_sync_wrapper.handle_peer_response(sender, response)
            }
            BlockSyncEvent::Timeout(request) => block_sync_wrapper.handle_timeout(request),
        };
        cmds.into_iter()
            .map(|command| WrappedBlockSyncCommand {
                // TODO: timeout should be more aggressive for headers request
                request_timeout: *self.delta * 7,
                command,
            })
            .collect()
    }
}

pub(crate) struct WrappedBlockSyncCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    request_timeout: Duration,
    command: BlockSyncCommand<ST, SCT, EPT>,
}

impl<ST, SCT, EPT, BPT, SBT> From<WrappedBlockSyncCommand<ST, SCT, EPT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn from(wrapped: WrappedBlockSyncCommand<ST, SCT, EPT>) -> Self {
        match wrapped.command {
            BlockSyncCommand::SendRequest { to, request } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::TcpPointToPoint {
                        to,
                        completion: None,
                    },
                    message: VerifiedMonadMessage::BlockSyncRequest(request),
                })]
            }
            BlockSyncCommand::ScheduleTimeout(request) => {
                vec![Command::TimerCommand(TimerCommand::Schedule {
                    duration: wrapped.request_timeout,
                    variant: TimeoutVariant::BlockSync(request),
                    on_timeout: MonadEvent::BlockSyncEvent(BlockSyncEvent::Timeout(request)),
                })]
            }
            BlockSyncCommand::ResetTimeout(block_id) => {
                vec![Command::TimerCommand(TimerCommand::ScheduleReset(
                    TimeoutVariant::BlockSync(block_id),
                ))]
            }
            BlockSyncCommand::SendResponse { to, response } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::TcpPointToPoint {
                        to,
                        completion: None,
                    },
                    message: VerifiedMonadMessage::BlockSyncResponse(response),
                })]
            }
            BlockSyncCommand::FetchHeaders(block_range) => {
                vec![Command::LedgerCommand(LedgerCommand::LedgerFetchHeaders(
                    block_range,
                ))]
            }
            BlockSyncCommand::FetchPayload(payload_id) => {
                vec![Command::LedgerCommand(LedgerCommand::LedgerFetchPayload(
                    payload_id,
                ))]
            }
            BlockSyncCommand::Emit(requester, (block_range, full_blocks)) => {
                vec![Command::LoopbackCommand(LoopbackCommand::Forward(
                    match requester {
                        BlockSyncSelfRequester::StateSync => {
                            MonadEvent::StateSyncEvent(StateSyncEvent::BlockSync {
                                block_range,
                                full_blocks,
                            })
                        }
                        BlockSyncSelfRequester::Consensus => {
                            MonadEvent::ConsensusEvent(ConsensusEvent::BlockSync {
                                block_range,
                                full_blocks,
                            })
                        }
                    },
                ))]
            }
        }
    }
}
