use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::{ErrorKind, Write},
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use alloy_consensus::{Block as AlloyBlock, BlockBody, Header, TxEnvelope};
use alloy_eips::eip4895::Withdrawals;
use alloy_primitives::{Bloom, FixedBytes, Uint};
use alloy_rlp::Encodable;
use futures::Stream;
use monad_block_persist::{BlockPersist, FileBlockPersist};
use monad_blocksync::messages::message::{
    BlockSyncBodyResponse, BlockSyncHeadersResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader, ConsensusFullBlock},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_types::{BlockId, Round, SeqNum};
use tracing::{info, trace};

type EthBlock = AlloyBlock<TxEnvelope>;

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    eth_block_path: PathBuf,
    bft_block_persist: FileBlockPersist<ST, SCT, EthExecutionProtocol>,

    metrics: ExecutorMetrics,
    last_commit: Option<SeqNum>,

    block_cache_size: usize,
    block_header_cache: HashMap<BlockId, ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>>,
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
    pub fn new(eth_block_path: PathBuf, bft_block_path: PathBuf, payload_path: PathBuf) -> Self {
        match fs::create_dir(&eth_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&bft_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&payload_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        let (fetches_tx, fetches) = tokio::sync::mpsc::unbounded_channel();
        Self {
            eth_block_path,
            bft_block_persist: FileBlockPersist::new(bft_block_path, payload_path),

            metrics: Default::default(),
            last_commit: Default::default(),

            block_cache_size: 100, // TODO configurable
            block_header_cache: Default::default(),
            block_payload_cache: Default::default(),
            block_cache_index: Default::default(),

            fetches_tx,
            fetches,

            phantom: PhantomData,
        }
    }

    pub fn last_commit(&self) -> Option<SeqNum> {
        self.last_commit
    }

    fn update_cache(&mut self, monad_block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) {
        let block_id = monad_block.get_id();
        let payload_id = monad_block.get_body_id();
        let block_round = monad_block.get_round();

        let (header, body) = monad_block.split();

        self.block_header_cache.insert(block_id, header);
        self.block_payload_cache.insert(payload_id, body);
        self.block_cache_index
            .insert(block_round, (block_id, payload_id));

        if self.block_cache_index.len() > self.block_cache_size {
            let (_, (block_id, payload_id)) = self.block_cache_index.pop_first().expect("nonempty");
            self.block_header_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }
    }

    fn write_eth_block(&self, seq_num: SeqNum, buf: &[u8]) -> std::io::Result<()> {
        let mut file_path = PathBuf::from(&self.eth_block_path);
        file_path.push(format!("{}", seq_num.0));

        let mut f = File::create(file_path).unwrap();
        f.write_all(buf).unwrap();

        Ok(())
    }

    fn create_eth_block(
        &mut self,
        block: &ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
    ) -> EthBlock {
        assert!(!block.header().is_empty_block());

        let block_body = BlockBody {
            transactions: block.body().execution_body.transactions.clone(),
            ommers: Vec::new(),
            withdrawals: Some(Withdrawals(Vec::new())),
        };
        let header = generate_header(block.header());

        let mut header_bytes = Vec::default();
        header.encode(&mut header_bytes);

        EthBlock {
            header,
            body: block_body,
        }
    }

    fn encode_eth_blocks(
        &self,
        full_blocks: &[(
            SeqNum,
            Option<EthBlock>,
            ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
        )],
    ) -> Vec<(SeqNum, Vec<u8>)> {
        full_blocks
            .iter()
            .filter_map(|(seqnum, maybe_eth_block, _)| {
                maybe_eth_block
                    .as_ref()
                    .map(|eth_block| (*seqnum, encode_eth_block(eth_block)))
            })
            .collect()
    }

    fn create_and_zip_with_eth_blocks(
        &mut self,
        full_bft_blocks: Vec<ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    ) -> Vec<(
        SeqNum,
        Option<EthBlock>,
        ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
    )> {
        full_bft_blocks
            .into_iter()
            .map(|b| {
                if b.header().is_empty_block() {
                    (b.get_seq_num(), None, b)
                } else {
                    self.update_cache(b.clone());
                    (b.get_seq_num(), Some(self.create_eth_block(&b)), b)
                }
            })
            .collect()
    }

    fn write_bft_blocks(
        &self,
        full_blocks: &Vec<(
            SeqNum,
            Option<EthBlock>,
            ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
        )>,
    ) {
        // unwrap because failure to persist a finalized block is fatal error
        for (_, _, bft_full_block) in full_blocks {
            // write payload first so that header always points to payload that exists
            self.bft_block_persist
                .write_bft_body(bft_full_block.body())
                .unwrap();
            self.bft_block_persist
                .write_bft_header(bft_full_block.header())
                .unwrap();
        }
    }

    fn trace_and_metrics(
        &mut self,
        full_blocks: &Vec<(
            SeqNum,
            Option<EthBlock>,
            ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
        )>,
    ) {
        for block in full_blocks {
            if let (_, Some(eth_block), _) = block {
                self.metrics[GAUGE_EXECUTION_LEDGER_NUM_COMMITS] += 1;
                self.metrics[GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS] +=
                    eth_block.body.transactions.len() as u64;
                self.metrics[GAUGE_EXECUTION_LEDGER_BLOCK_NUM] = eth_block.header.number;
                info!(
                    num_tx = eth_block.body.transactions.len(),
                    block_num = eth_block.header.number,
                    "committed block"
                );

                for t in &eth_block.body.transactions {
                    trace!(txn_hash = ?t.tx_hash(), "txn committed");
                }
            }
        }
    }

    fn ledger_fetch_headers(
        &self,
        block_range: BlockRange,
    ) -> BlockSyncHeadersResponse<ST, SCT, EthExecutionProtocol> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.num_blocks.0 {
            // TODO add max number of headers to read
            let block_header =
                if let Some(cached_block) = self.block_header_cache.get(&next_block_id) {
                    cached_block.clone()
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
                LedgerCommand::LedgerCommit(full_blocks) => {
                    let full_blocks = self.create_and_zip_with_eth_blocks(full_blocks);
                    self.trace_and_metrics(&full_blocks);

                    let encoded_eth_blocks = self.encode_eth_blocks(&full_blocks);
                    // this can panic because failure to persist a finalized block is fatal error
                    self.write_bft_blocks(&full_blocks);

                    for (seqnum, b) in encoded_eth_blocks {
                        self.write_eth_block(seqnum, &b).unwrap();
                        self.last_commit = Some(seqnum);
                    }

                    for (_, _, full_block) in full_blocks {
                        self.update_cache(full_block);
                    }
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

fn encode_eth_block(block: &EthBlock) -> Vec<u8> {
    let mut buf = Vec::default();
    block.encode(&mut buf);
    buf
}

// TODO-2: Review integration with execution team
/// Use data from the MonadBlock to generate an Ethereum Header
fn generate_header<ST, SCT>(
    monad_block: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
) -> Header
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    Header {
        parent_hash: monad_block
            .delayed_execution_results
            .first()
            .map(|x| x.0.hash_slow())
            .unwrap_or_default(),
        ommers_hash: monad_block.execution_inputs.ommers_hash.into(),
        beneficiary: monad_block.execution_inputs.beneficiary,
        state_root: FixedBytes::default(),
        transactions_root: monad_block.execution_inputs.transactions_root.into(),
        receipts_root: FixedBytes::default(),
        withdrawals_root: Some(monad_block.execution_inputs.withdrawals_root.into()),
        logs_bloom: Bloom(FixedBytes::default()),
        difficulty: Uint::ZERO,
        number: monad_block.execution_inputs.number,
        gas_limit: monad_block.execution_inputs.gas_limit,
        gas_used: 0,
        timestamp: monad_block.execution_inputs.timestamp,
        mix_hash: monad_block.execution_inputs.mix_hash.into(),
        nonce: monad_block.execution_inputs.nonce.into(),
        base_fee_per_gas: monad_block.execution_inputs.base_fee_per_gas.into(),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None, // execution is stil shanghai
        extra_data: monad_block.execution_inputs.extra_data.into(),

        requests_hash: None,
        target_blobs_per_block: None,
    }
}
