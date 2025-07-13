use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::ErrorKind,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use monad_block_persist::{BlockPersist, FileBlockPersist};
use monad_blocksync::messages::message::{
    BlockSyncBodyResponse, BlockSyncHeadersResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, ConsensusFullBlock, OptimisticCommit},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_types::{BlockId, Round, SeqNum, GENESIS_ROUND};
use tracing::{info, trace, warn};

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    bft_block_persist: FileBlockPersist<ST, SCT, EthExecutionProtocol>,

    metrics: ExecutorMetrics,
    last_commit: Option<(SeqNum, Round)>,

    block_cache_size: usize,
    block_cache: HashMap<BlockId, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    block_payload_cache: HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EthExecutionProtocol>>,
    block_cache_index: BTreeMap<Round, (BlockId, ConsensusBlockBodyId)>,

    fetches_tx:
        tokio::sync::mpsc::UnboundedSender<BlockSyncResponseMessage<ST, SCT, EthExecutionProtocol>>,
    fetches: tokio::sync::mpsc::UnboundedReceiver<
        BlockSyncResponseMessage<ST, SCT, EthExecutionProtocol>,
    >,

    phantom: PhantomData<ST>,
}

const GAUGE_EXECUTION_LEDGER_NUM_COMMITS: &str = "monad.execution_ledger.num_commits";
const GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS: &str = "monad.execution_ledger.num_tx_commits";
const GAUGE_EXECUTION_LEDGER_BLOCK_NUM: &str = "monad.execution_ledger.block_num";

impl<ST, SCT> MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(ledger_path: PathBuf) -> Self {
        match std::fs::create_dir(&ledger_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }

        let bft_block_persist = FileBlockPersist::new(ledger_path);

        let (fetches_tx, fetches) = tokio::sync::mpsc::unbounded_channel();
        Self {
            bft_block_persist,

            metrics: Default::default(),
            last_commit: None,

            block_cache_size: 1_000, // TODO configurable

            block_cache: Default::default(),
            block_payload_cache: Default::default(),
            block_cache_index: Default::default(),

            fetches_tx,
            fetches,

            phantom: PhantomData,
        }
    }

    pub fn last_commit(&self) -> Option<SeqNum> {
        let (last_commit_seq_num, _) = self.last_commit?;
        Some(last_commit_seq_num)
    }

    fn update_cache(&mut self, monad_block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) {
        let block_id = monad_block.get_id();
        let payload_id = monad_block.get_body_id();
        let block_round = monad_block.get_block_round();

        let maybe_removed = self
            .block_cache_index
            .insert(block_round, (block_id, payload_id));
        if let Some((block_id, payload_id)) = maybe_removed {
            // in the case of equivocated tips, replacing the old tip here may not be optimal
            // however, this does not matter because the cache is just there for a fast-path
            //
            // all reads are backed by disk
            self.block_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }

        if self.block_cache_index.len() > self.block_cache_size {
            let (evicted_round, (block_id, payload_id)) =
                self.block_cache_index.pop_first().expect("nonempty");
            let last_commit_round = self
                .last_commit
                .map(|(_, last_commit_round)| last_commit_round);
            if evicted_round >= last_commit_round.unwrap_or(GENESIS_ROUND) {
                warn!(
                    ?evicted_round,
                    ?last_commit_round,
                    "evicted round from block_cache that's higher than last_commit_round"
                )
            };
            self.block_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }

        // insert at the end in case payload got evicted
        self.block_payload_cache
            .insert(payload_id, monad_block.body().clone());
        self.block_cache.insert(block_id, monad_block);
    }

    fn write_bft_block(&mut self, full_block: &ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) {
        // unwrap because failure to persist a finalized block is fatal error

        // write payload first so that header always points to payload that exists
        self.bft_block_persist
            .write_bft_body(full_block.body())
            .unwrap();
        self.bft_block_persist
            .write_bft_header(full_block.header())
            .unwrap();
    }

    fn ledger_fetch_headers(
        &self,
        block_range: BlockRange,
    ) -> BlockSyncHeadersResponse<ST, SCT, EthExecutionProtocol> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.num_blocks.0 {
            // TODO add max number of headers to read
            let block_header = if let Some(cached_block) = self.block_cache.get(&next_block_id) {
                cached_block.header().clone()
            } else if let Ok(block) = self.bft_block_persist.read_bft_header(&next_block_id) {
                block
            } else {
                trace!(?block_range, "requested headers not available in ledger");
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };

            next_block_id = block_header.get_parent_id();
            headers.push_front(block_header);
        }

        trace!(?block_range, "found requested headers in ledger");
        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn ledger_fetch_payload(
        &self,
        payload_id: ConsensusBlockBodyId,
    ) -> BlockSyncBodyResponse<EthExecutionProtocol> {
        if let Some(cached_payload) = self.block_payload_cache.get(&payload_id) {
            // payload in cache
            trace!(?payload_id, "found requested payload in ledger cache");
            BlockSyncBodyResponse::Found(cached_payload.clone())
        } else if let Ok(payload) = self.bft_block_persist.read_bft_body(&payload_id) {
            // payload read from block persist
            trace!(
                ?payload_id,
                "found requested payload in ledger blockpersist"
            );
            BlockSyncBodyResponse::Found(payload)
        } else {
            trace!(?payload_id, "requested payload not available in ledger");
            BlockSyncBodyResponse::NotAvailable(payload_id)
        }
    }
}

impl<ST, SCT> Executor for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<ST, SCT, EthExecutionProtocol>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(OptimisticCommit::Proposed(block)) => {
                    let block_id = block.get_id();
                    let block_round = block.get_block_round();

                    // this can panic because failure to persist a block is fatal error
                    self.write_bft_block(&block);

                    self.update_cache(block);

                    self.bft_block_persist
                        .update_proposed_head(&block_id)
                        .unwrap();
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Finalized(block)) => {
                    self.metrics[GAUGE_EXECUTION_LEDGER_NUM_COMMITS] += 1;

                    let block_id = block.get_id();
                    let block_round = block.get_block_round();
                    let num_tx = block.body().execution_body.transactions.len() as u64;
                    let block_num = block.get_seq_num().0;
                    info!(num_tx, block_num, "committed block");
                    self.metrics[GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS] += num_tx;
                    self.metrics[GAUGE_EXECUTION_LEDGER_BLOCK_NUM] = block_num;

                    self.last_commit = Some((block.get_seq_num(), block.get_block_round()));

                    self.bft_block_persist
                        .update_finalized_head(&block_id)
                        .unwrap();
                }
                LedgerCommand::LedgerFetchHeaders(block_range) => {
                    // TODO cap max concurrent LedgerFetch? DOS vector
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::HeadersResponse(
                        self.ledger_fetch_headers(block_range),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::PayloadResponse(
                        self.ledger_fetch_payload(payload_id),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.fetches.poll_recv(cx).map(|response| {
            let response = response.expect("fetches_tx never dropped");
            Some(MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfResponse {
                response,
            }))
        })
    }
}
