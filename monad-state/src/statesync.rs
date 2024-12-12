use std::collections::{HashMap, VecDeque};

use monad_consensus::{messages::message::ProposalMessage, validation::safety::commit_condition};
use monad_consensus_types::{
    block::{Block, BlockRange, BlockType, FullBlock},
    checkpoint::RootInfo,
    payload::{Payload, PayloadId},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM};
use reth_primitives::Header;

/// BlockBuffer is responsible for tracking pending blocks mid-statesync
/// It performs a function very similar to the blocktree, but specifically for statesync purposes
/// This could likely be unified with the blocktree, but will be a larger implementation lift
#[derive(Clone)]
pub(crate) struct BlockBuffer<SCT: SignatureCollection> {
    max_buffered_proposals: usize,

    /// trigger resync once passively observe new_root > current_root + resync_threshold
    resync_threshold: SeqNum,
    state_root_delay: SeqNum,
    /// Different in sequence number between a block that satisfies the commit condition and the
    /// seqnum that gets committed
    commit_distance: SeqNum,

    root: BlockId,
    // blocks <= root
    full_blocks: HashMap<BlockId, FullBlock<SCT>>,
    payload_cache: HashMap<PayloadId, Payload>,
    // block headers >= root
    block_headers: HashMap<BlockId, Block<SCT>>,

    // cache of last max_buffered_proposals proposals
    proposal_buffer: VecDeque<(NodeId<SCT::NodeIdPubKey>, ProposalMessage<SCT>)>,
}

impl<SCT: SignatureCollection> BlockBuffer<SCT> {
    pub fn new(
        state_root_delay: SeqNum,
        root: BlockId,
        resync_threshold: SeqNum,
        commit_distance: SeqNum,
    ) -> Self {
        Self {
            max_buffered_proposals: resync_threshold.0 as usize,
            resync_threshold,
            state_root_delay,
            commit_distance,

            root,

            full_blocks: Default::default(),
            payload_cache: Default::default(),
            block_headers: Default::default(),
            proposal_buffer: Default::default(),
        }
    }

    pub fn get_payload_cache(&self) -> &HashMap<PayloadId, Payload> {
        &self.payload_cache
    }

    pub fn root_seq_num(&self) -> Option<SeqNum> {
        Some(self.root_info()?.seq_num)
    }

    pub fn root_info(&self) -> Option<RootInfo> {
        if self.root == GENESIS_BLOCK_ID {
            return Some(RootInfo {
                seq_num: GENESIS_SEQ_NUM,
                round: Round(0),
                epoch: Epoch(1),
                block_id: GENESIS_BLOCK_ID,
            });
        }

        let root = self.full_blocks.get(&self.root)?;
        Some(RootInfo {
            round: root.get_round(),
            seq_num: root.get_seq_num(),
            epoch: root.get_epoch(),
            block_id: root.get_id(),
        })
    }

    pub fn root_delayed_execution_result(&self) -> Option<&Header> {
        let root = self.full_blocks.get(&self.root)?;

        Some(root.get_delayed_execution_result())
    }

    /// returns a new sync_target is applicable.
    ///
    /// concretely, if new_root > current_root + resync_threshold
    pub fn handle_proposal(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        proposal: ProposalMessage<SCT>,
    ) -> Option<(Block<SCT>, QuorumCertificate<SCT>)> {
        // TODO more validation? leader checking? more sophisticated eviction?

        let root_seq_num = self.root_seq_num()?;

        if proposal.block.get_seq_num() < root_seq_num {
            return None;
        }
        let block_header = proposal.block.clone();
        self.block_headers
            .insert(proposal.block.get_id(), block_header.clone());

        self.proposal_buffer.push_back((author, proposal));
        if self.proposal_buffer.len() > self.max_buffered_proposals {
            self.proposal_buffer.pop_front();
        }

        if commit_condition(block_header.get_round(), block_header.get_qc().info) {
            let committed_seq_num = block_header.get_seq_num() - self.commit_distance;
            if committed_seq_num > root_seq_num + self.resync_threshold {
                let target_blockid = block_header.get_qc().info.vote.vote_info.parent_id;
                if let Some(target_block) = self.block_headers.get(&target_blockid) {
                    assert_eq!(target_block.get_seq_num(), committed_seq_num);
                    return Some((target_block.clone(), block_header.get_qc().clone()));
                };
            }
        }

        None
    }

    pub fn handle_blocksync(&mut self, block: FullBlock<SCT>) {
        if self
            .root_seq_num()
            .is_some_and(|root_seq_num| block.get_seq_num() > root_seq_num)
        {
            // this should never happen, but here for clarity
            return;
        }

        self.payload_cache
            .insert(block.get_payload_id(), block.get_payload());
        self.full_blocks.insert(block.get_id(), block);
    }

    pub fn re_root(&mut self, new_root: Block<SCT>) {
        // remove obsolete full_blocks
        self.full_blocks.retain(|_id, block| {
            block.get_seq_num() + self.state_root_delay >= new_root.get_seq_num()
        });
        for (_sender, proposal) in &self.proposal_buffer {
            if proposal.block.get_seq_num() <= new_root.get_seq_num() {
                self.full_blocks.insert(
                    proposal.block.get_id(),
                    FullBlock {
                        block: proposal.block.clone(),
                        payload: proposal.payload.clone(),
                    },
                );
            }
            // we could also evict from proposal_buffer here, but unnecessary
        }

        // remove obsolete block headers
        self.block_headers
            .retain(|_id, block| block.get_seq_num() >= new_root.get_seq_num());

        self.root = new_root.get_id();
        self.payload_cache = self
            .full_blocks
            .values()
            .map(|block| (block.get_payload_id(), block.get_payload()))
            .collect();
    }

    pub fn proposals(
        &self,
    ) -> impl Iterator<Item = &(NodeId<SCT::NodeIdPubKey>, ProposalMessage<SCT>)> {
        self.proposal_buffer.iter()
    }

    /// chain of blocks starting with root (highest to lowest seq_num)
    pub fn root_parent_chain(&self) -> Vec<&FullBlock<SCT>> {
        let mut next_block_id = self.root;
        let mut root_parent_chain = Vec::new();
        while let Some(block) = self.full_blocks.get(&next_block_id) {
            root_parent_chain.push(block);
            next_block_id = block.get_parent_id();
        }
        root_parent_chain
    }

    pub fn needs_blocksync(&self) -> Option<BlockRange> {
        if self.root == GENESIS_BLOCK_ID {
            return None;
        }

        let chain = self.root_parent_chain();

        let Some(last) = chain.last() else {
            let request_range = BlockRange {
                last_block_id: self.root,
                max_blocks: self.state_root_delay,
            };
            tracing::debug!(?request_range, "statesync blocksyncing blocks");
            return Some(request_range);
        };

        let block_parent_id = last.get_parent_id();

        if chain.len() < self.state_root_delay.0 as usize {
            let request_range = BlockRange {
                last_block_id: block_parent_id,
                max_blocks: self.state_root_delay - SeqNum(chain.len() as u64),
            };
            tracing::debug!(?request_range, "statesync blocksyncing blocks");
            return Some(request_range);
        }

        None
    }
}
