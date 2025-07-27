use std::{
    collections::VecDeque,
    fmt::{self, Debug},
};

use monad_consensus_types::{
    block::{BlockPolicy, BlockPolicyError, BlockRange, ConsensusBlockHeader},
    checkpoint::RootInfo,
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, ExecutionProtocol, Round, SeqNum};

use crate::tree::{BlockTreeEntry, Tree};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Root {
    info: RootInfo,
    children_blocks: Vec<BlockId>,
}

pub struct BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    /// The round and block_id of last committed block
    root: Root,
    /// Uncommitted blocks
    /// First level of blocks in the tree have block.get_parent_id() == root.block_id
    tree: Tree<ST, SCT, EPT, BPT, SBT>,
}

impl<ST, SCT, EPT, BPT, SBT> Debug for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockTree")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT, BPT, SBT> PartialEq<Self> for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root && self.tree == other.tree
    }
}

impl<ST, SCT, EPT, BPT, SBT> Eq for BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
}

impl<ST, SCT, EPT, BPT, SBT> BlockTree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    pub fn new(root: RootInfo) -> Self {
        Self {
            root: Root {
                info: root,
                children_blocks: Vec::new(),
            },
            tree: Default::default(),
        }
    }

    pub fn root(&self) -> &RootInfo {
        &self.root.info
    }

    /// Prune the block tree and returns the blocks to commit along that branch
    /// in increasing round. After a successful prune, `new_root` is the root of
    /// the block tree. All blocks pruned are deallocated
    ///
    /// Must be called IFF is_coherent
    ///
    /// The prune algorithm removes all blocks with lower round than `new_root`
    ///
    /// Short proof on the correctness of the algorithm
    ///
    /// - On commit, this function is called with the block to commit as
    ///   the`new_root`. Recall the commit rule stating a QC-of-QC commits the
    ///   block
    ///
    /// - Nodes that aren't descendents of `new_root` are added before
    ///   `new_root` and have round number smaller than `new_root` It only
    ///   prunes blocks not part of the `new_root` subtree -> accurate
    ///
    /// - When prune is called on round `new_root` block, only round `n+1` block
    ///   is added (as `new_root`'s children) All the blocks that remains
    ///   shouldn't be pruned -> complete
    pub fn prune(&mut self, new_root: &BlockId) -> Vec<BPT::ValidatedBlock> {
        assert!(self.is_coherent(new_root));
        let mut commit: Vec<BPT::ValidatedBlock> = Vec::new();

        if new_root == &self.root.info.block_id {
            return commit;
        }

        let new_root_entry = self
            .tree
            .remove(new_root)
            .expect("new root must exist in blocktree");
        let mut entry_to_commit = new_root_entry.clone();

        // traverse up the branch from new_root, removing blocks and pushing to
        // the commit list.
        loop {
            assert!(entry_to_commit.is_coherent, "must be coherent");

            let validated_block = entry_to_commit.validated_block;

            let parent_id = validated_block.get_parent_id();

            commit.push(validated_block);

            if parent_id == self.root.info.block_id {
                break;
            }
            entry_to_commit = self
                .tree
                .remove(&parent_id)
                .expect("path to root must exist")
        }

        // garbage collect old blocks
        // remove any blocks less than or equal to round `n`
        let blocks_to_delete: Vec<_> = self
            .tree
            .iter()
            .filter_map(|(block_id, block)| {
                if block.validated_block.get_parent_round()
                    < new_root_entry.validated_block.get_block_round()
                {
                    Some(block_id)
                } else {
                    None
                }
            })
            .copied()
            .collect();
        for block_to_delete in blocks_to_delete {
            self.tree.remove(&block_to_delete);
        }
        self.root = Root {
            info: RootInfo {
                round: new_root_entry.validated_block.get_block_round(),
                seq_num: new_root_entry.validated_block.get_seq_num(),
                epoch: new_root_entry.validated_block.get_epoch(),
                block_id: new_root_entry.validated_block.get_id(),
                timestamp_ns: new_root_entry.validated_block.get_timestamp(),
            },
            children_blocks: new_root_entry.children_blocks,
        };

        commit.reverse();
        commit
    }

    /// Add a new block to the block tree if it's not in the tree and is higher
    /// than the root block's round number
    pub fn add(&mut self, block: BPT::ValidatedBlock) {
        if !self.is_valid_to_insert(block.header()) {
            return;
        }

        let new_block_id = block.get_id();
        let parent_id = block.get_parent_id();

        self.tree.insert(block);

        if parent_id == self.root.info.block_id {
            self.root.children_blocks.push(new_block_id);
        }
    }

    pub fn try_update_coherency(
        &mut self,
        metrics: &mut Metrics,
        block_id: BlockId,
        block_policy: &mut BPT,
        state_backend: &SBT,
    ) -> Vec<BPT::ValidatedBlock> {
        let Some(path_from_root) = self.get_blocks_on_path_from_root(&block_id) else {
            return Vec::new();
        };
        let Some(incoherent_parent_or_self) = path_from_root.iter().find(|block| {
            !self
                .tree
                .get(&block.get_id())
                .expect("block doesn't exist")
                .is_coherent
        }) else {
            // no incoherent_parent_or_self, already is coherent
            return Vec::new();
        };

        let mut block_ids_to_update: VecDeque<BlockId> =
            vec![incoherent_parent_or_self.get_id()].into();

        let mut retval = vec![];
        while !block_ids_to_update.is_empty() {
            // Next block to check coherency
            let next_block_id = block_ids_to_update.pop_front().unwrap();
            let mut extending_blocks = self
                .get_blocks_on_path_from_root(&next_block_id)
                .expect("path to root must exist");
            // Remove the block itself
            let next_block = extending_blocks
                .pop()
                .expect("next_block is included in path_from_root");

            // extending blocks are always coherent, because we only call
            // update_coherency on the first incoherent block in the chain
            match block_policy.check_coherency(
                next_block,
                extending_blocks,
                self.root.info,
                state_backend,
            ) {
                Ok(()) => {
                    let next_block = next_block.clone();
                    self.tree
                        .set_coherent(&next_block_id, true)
                        .expect("should be in tree");

                    retval.push(next_block);

                    // Can check coherency of children blocks now
                    block_ids_to_update.extend(
                        self.tree
                            .get(&next_block_id)
                            .expect("should be in tree")
                            .children_blocks
                            .iter()
                            .cloned(),
                    );
                }
                Err(BlockPolicyError::StateBackendError(StateBackendError::NotAvailableYet)) => {
                    metrics.consensus_events.rx_execution_lagging += 1;
                }
                Err(BlockPolicyError::ExecutionResultMismatch) => {
                    metrics.consensus_events.rx_bad_state_root += 1;
                }
                Err(
                    BlockPolicyError::BlockNotCoherent
                    | BlockPolicyError::TimestampError
                    | BlockPolicyError::StateBackendError(StateBackendError::NeverAvailable),
                ) => {
                    // TODO add metrics
                }
            }
        }
        retval
    }

    /// Iterate the block tree and return highest QC that have path to block tree
    /// root and is committable, if exists
    ///
    /// FIXME this does not take high_qc into account, which makes this more pessimistic than it
    /// needs to be
    pub fn get_high_committable_qc(&self) -> Option<QuorumCertificate<SCT>> {
        let mut high_commit_qc: Option<QuorumCertificate<SCT>> = None;
        let mut iter: VecDeque<BlockId> = self.root.children_blocks.clone().into();
        while let Some(bid) = iter.pop_front() {
            let block = self.tree.get(&bid).expect("block in tree");

            // queue up children
            iter.extend(block.children_blocks.iter().cloned());

            let qc = block.validated_block.get_qc();
            if high_commit_qc
                .as_ref()
                .is_some_and(|high_commit_qc| high_commit_qc.get_round() >= qc.get_round())
            {
                // we already have observed a higher committable QC
                continue;
            }

            let Some(qc_parent_block) = self.tree.get(&qc.get_block_id()) else {
                // parent block doesn't exist, or parent block is root
                continue;
            };

            let Some(committable_block_id) =
                qc.get_committable_id(qc_parent_block.validated_block.header())
            else {
                // qc is not committable (not consecutive rounds)
                continue;
            };

            if committable_block_id == self.root.info.block_id {
                // nothing new to commit
                continue;
            }

            if !self.is_coherent(&committable_block_id) {
                // the committable block is not (yet) coherent, likely because execution is lagging
                // can also happen if committable_block_id is the parent of root
                //
                // TODO can we return out early here, because we're BFS?
                continue;
            }

            high_commit_qc = Some(qc.clone());
        }
        high_commit_qc
    }

    /// returns a BlockRange that should be requested to fill the path from `qc` to the tree root.
    pub fn maybe_fill_path_to_root(&self, qc: &QuorumCertificate<SCT>) -> Option<BlockRange> {
        if self.root.info.round >= qc.get_round() || self.root.info.block_id == qc.get_block_id() {
            // root cannot be an ancestor of qc
            return None;
        }

        let mut maybe_unknown_bid = qc.get_block_id();
        let mut num_blocks = SeqNum(1);
        while let Some(known_block_entry) = self.tree.get(&maybe_unknown_bid) {
            // If the parent round == self.root, we have path to root
            if known_block_entry.validated_block.get_parent_id() == self.root.info.block_id {
                return None;
            }
            maybe_unknown_bid = known_block_entry.validated_block.get_parent_id();
            // FIXME replace with below once null blocks are deleted
            num_blocks = (known_block_entry.validated_block.get_seq_num() - self.root.info.seq_num)
                .max(SeqNum(2))
                - SeqNum(1);
            // num_blocks = known_block_entry.validated_block.get_seq_num() - self.root.info.seq_num - SeqNum(1);
        }

        Some(BlockRange {
            last_block_id: maybe_unknown_bid,
            num_blocks,
        })
    }

    pub fn is_coherent(&self, b: &BlockId) -> bool {
        if b == &self.root.info.block_id {
            return true;
        }

        if let Some(blocktree_entry) = self.tree.get(b) {
            return blocktree_entry.is_coherent;
        }

        false
    }

    /// Fetches blocks on path from root
    pub fn get_blocks_on_path_from_root(&self, b: &BlockId) -> Option<Vec<&BPT::ValidatedBlock>> {
        let mut blocks = Vec::new();
        if b == &self.root.info.block_id {
            return Some(blocks);
        }

        let mut visit = *b;

        while let Some(blocktree_entry) = self.tree.get(&visit) {
            let btb = &blocktree_entry.validated_block;
            blocks.push(btb);

            if btb.get_parent_id() == self.root.info.block_id {
                blocks.reverse();
                return Some(blocks);
            }

            visit = btb.get_parent_id();
        }

        None
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // seq_num
    pub fn get_seq_num_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<SeqNum> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.get_root_seq_num());
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_seq_num())
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // timestamp
    pub fn get_timestamp_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<u128> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.get_root_timestamp());
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_timestamp())
    }

    // Take a QC and look for the block it certifies in the blocktree. If it exists, return its
    // round
    pub fn get_block_round_of_qc(&self, qc: &QuorumCertificate<SCT>) -> Option<Round> {
        let block_id = qc.get_block_id();
        if self.root.info.block_id == block_id {
            return Some(self.root.info.round);
        }
        let certified_block = self.tree.get(&block_id)?;
        Some(certified_block.validated_block.get_block_round())
    }

    /// A block is valid to insert if it does not already exist in the block
    /// tree and its round is greater than the round of the root
    pub fn is_valid_to_insert(&self, b: &ConsensusBlockHeader<ST, SCT, EPT>) -> bool {
        !self.tree.contains_key(&b.get_id()) && b.block_round > self.root.info.round
    }

    pub fn tree(&self) -> &Tree<ST, SCT, EPT, BPT, SBT> {
        &self.tree
    }

    pub fn size(&self) -> usize {
        self.tree.len()
    }

    pub fn get_root_seq_num(&self) -> SeqNum {
        self.root.info.seq_num
    }

    pub fn get_root_timestamp(&self) -> u128 {
        self.root.info.timestamp_ns
    }

    /// Note that this returns None if the block_id is root!
    pub fn get_block(&self, block_id: &BlockId) -> Option<&BPT::ValidatedBlock> {
        self.tree.get(block_id).map(|block| &block.validated_block)
    }

    pub fn get_payload(
        &self,
        block_body_id: &ConsensusBlockBodyId,
    ) -> Option<ConsensusBlockBody<EPT>> {
        self.tree.get_payload(block_body_id).cloned()
    }

    pub fn get_entry(&self, block_id: &BlockId) -> Option<&BlockTreeEntry<ST, SCT, EPT, BPT, SBT>> {
        self.tree.get(block_id)
    }

    /// Notably does NOT need to be a chain to root
    /// chain is returned in order of lowest round to highest
    pub fn get_parent_block_chain(&self, block_id: &BlockId) -> Vec<&BPT::ValidatedBlock> {
        let Some(base_block) = self.tree.get(block_id) else {
            return Default::default();
        };

        let mut chain = vec![&base_block.validated_block];
        while let Some(parent) = self.tree.get(&chain.last().unwrap().get_parent_id()) {
            chain.push(&parent.validated_block);
        }
        chain.reverse();

        chain
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{
            ConsensusBlockHeader, ConsensusFullBlock, MockExecutionBody,
            MockExecutionProposedHeader, MockExecutionProtocol, PassthruBlockPolicy,
            GENESIS_TIMESTAMP,
        },
        metrics::Metrics,
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        },
        NopKeyPair, NopSignature,
    };
    use monad_eth_types::{Balance, EMPTY_RLP_TX_LIST};
    use monad_state_backend::{InMemoryState, InMemoryStateInner};
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};

    use super::BlockTree;
    use crate::blocktree::RootInfo;

    type SignatureType = NopSignature;
    type ExecutionProtocolType = MockExecutionProtocol;
    type StateBackendType = InMemoryState;
    type BlockPolicyType = PassthruBlockPolicy;
    type BlockTreeType = BlockTree<
        SignatureType,
        MockSignatures<SignatureType>,
        ExecutionProtocolType,
        BlockPolicyType,
        StateBackendType,
    >;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type Block =
        ConsensusBlockHeader<SignatureType, MockSignatures<SignatureType>, ExecutionProtocolType>;
    type FullBlock =
        ConsensusFullBlock<SignatureType, MockSignatures<SignatureType>, ExecutionProtocolType>;
    type QC = QuorumCertificate<MockSignatures<SignatureType>>;

    fn node_id() -> NodeId<PubKeyType> {
        let mut privkey: [u8; 32] = [127; 32];
        let keypair =
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut privkey,
            )
            .unwrap();
        NodeId::new(keypair.pubkey())
    }

    fn get_vote(block: &Block) -> Vote {
        Vote {
            id: block.get_id(),
            epoch: block.epoch,
            round: block.block_round,
            v0_parent_id: None,
            v0_parent_round: None,
        }
    }

    pub fn mock_qc(vote: Vote) -> QC {
        QC::new(vote, MockSignatures::with_pubkeys(&[]))
    }

    pub fn mock_qc_for_block(block: &Block) -> QC {
        let vote = Vote {
            id: block.get_id(),
            epoch: block.epoch,
            round: block.block_round,
            v0_parent_id: None,
            v0_parent_round: None,
        };
        QC::new(vote, MockSignatures::with_pubkeys(&[]))
    }

    fn get_genesis_block() -> FullBlock {
        let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: Default::default(),
            },
        });
        let header = Block::new(
            node_id(),
            Epoch(1),
            Round(1),
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            body.get_id(),
            QC::genesis_qc(),
            SeqNum(1),
            1,
            RoundSignature::new(Round(1), &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap()),
        );

        FullBlock::new(header, body).unwrap()
    }

    fn get_next_block(
        parent: &FullBlock,
        maybe_round: Option<Round>,
        tx_bytes: &[u8],
    ) -> FullBlock {
        let parent = parent.header();
        let round = maybe_round.unwrap_or(parent.block_round + Round(1));

        let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody {
                data: tx_bytes.to_vec().into(),
            },
        });
        let header = Block::new(
            node_id(),
            parent.epoch,
            round,
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            body.get_id(),
            mock_qc_for_block(parent),
            parent.seq_num + SeqNum(1),
            parent.timestamp_ns + 1,
            RoundSignature::new(round, &NopKeyPair::from_bytes(&mut [1_u8; 32]).unwrap()),
        );

        FullBlock::new(header, body).unwrap()
    }

    #[test]
    fn test_prune() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();

        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&g, None, &[3]);
        let b4 = get_next_block(&g, None, &[4]);
        let b5 = get_next_block(&b3, None, &[5]);
        let b6 = get_next_block(&b5, None, &[6]);
        let b7 = get_next_block(&b6, None, &[7]);

        // Initial blocktree
        //        g
        //   /    |     \
        //  b1    b3    b4
        //  |     |
        //  b2    b5
        //        |
        //        b6
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());

        blocktree.add(b1.clone().into());
        blocktree.add(b2.clone().into());
        blocktree.add(b3.clone().into());
        blocktree.add(b4.clone().into());
        blocktree.add(b5.clone().into());
        blocktree.add(b6.clone().into());
        println!("{:?}", blocktree);

        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b4.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b5.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b5.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b6.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b6.get_id()));

        // pruning on the old root should return no committable blocks
        let commit = blocktree.prune(&g.get_parent_id());
        assert_eq!(commit.len(), 0);

        let commit = blocktree.prune(&b5.get_id());
        assert_eq!(
            Vec::from_iter(commit.iter().map(|b| b.get_id())),
            vec![g.get_id(), b3.get_id(), b5.get_id()]
        );
        println!("{:?}", blocktree);
        println!("{:?}", blocktree.tree);

        // Pruned blocktree
        //     b5
        //     |
        //     b6

        // try pruning all other nodes should return err
        assert!(!blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        // Pruned blocktree after insertion
        //     b5
        //   /    \
        //  b6    b8
        //  |
        //  b7

        blocktree.add(b7.into());

        let b8 = get_next_block(&b5, None, &[8]);

        blocktree.add(b8.into());
        println!("{:?}", blocktree);
    }

    #[test]
    fn test_add_parent_not_exist() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();

        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let gid = g.get_id();
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.into());

        blocktree.add(b2.clone().into());
        assert_eq!(blocktree.tree.len(), 2);
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b1.clone().into());
        assert_eq!(blocktree.tree.len(), 3);
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        assert_eq!(
            blocktree.get_block(&b2.get_id()).unwrap().get_parent_id(),
            b1.get_id()
        );
        assert_eq!(
            blocktree.get_block(&b1.get_id()).unwrap().get_parent_id(),
            gid
        );
    }

    #[test]
    fn equal_level_branching() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&g, None, &[2]);
        let b3 = get_next_block(&b1, None, &[3]);

        // Initial blocktree
        //        g
        //   /    |
        //  b1    b2
        //  |
        //  b3
        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.add(b2.clone().into());
        blocktree.add(b3.into());

        assert_eq!(blocktree.size(), 4);

        // prune called on b1, we expect new tree to be
        // b1
        // |
        // b3
        // and the commit blocks should only contain b1 (not b2)
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        let commit = blocktree.prune(&b1.get_id());
        assert_eq!(commit.len(), 2);
        assert_eq!(commit[0].get_id(), g.get_id());
        assert_eq!(commit[1].get_id(), b1.get_id());
        assert_eq!(blocktree.size(), 1);
        assert!(!blocktree.is_coherent(&b2.get_id()));
    }

    #[test]
    fn duplicate_blocks() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let block_policy = PassthruBlockPolicy;
        blocktree.add(g.into());
        blocktree.add(b1.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.add(b1.into());

        assert_eq!(blocktree.tree.len(), 2);
    }

    #[test]
    fn path_to_root_repair_update_coherency_all_children() {
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b1, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));

        blocktree.add(b2.clone().into());
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b3.clone().into());
        assert!(!blocktree.is_coherent(&b3.get_id()));

        blocktree.add(b4.clone().into());
        assert!(!blocktree.is_coherent(&b4.get_id()));

        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b4.get_id()));

        blocktree.prune(&b3.get_id());

        assert!(!blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
    }

    #[test]
    fn test_get_missing_ancestor() {
        // Initial blocktree
        //        g
        //        |
        //        b1
        //  |  -  |  -  |
        //  b2    b3    b4
        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b1, Some(Round(5)), &[4]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b4.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b4.header().qc)
                .unwrap()
                .last_block_id
                == b4.header().get_parent_id()
        );

        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );

        assert!(blocktree.maybe_fill_path_to_root(&b1.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());

        blocktree.prune(&b1.get_id());

        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b1.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());

        assert_eq!(blocktree.size(), 3);
    }

    #[test]
    fn test_parent_update_coherency() {
        // Initial blocktree
        //  g
        //  |
        //  b1 (coherent = false)
        //  |
        //  ...
        //
        // blocktree is updated with b2

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            epoch: genesis_qc.get_epoch(),
            seq_num: GENESIS_SEQ_NUM,
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        blocktree.add(b1.clone().into());
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );

        let b1_entry = blocktree.tree.get(&b1.get_id()).unwrap();
        assert!(b1_entry.is_coherent);
        // set b1 to be incoherent
        blocktree.tree.set_coherent(&b1.get_id(), false).unwrap();
        assert!(!blocktree.is_coherent(&b1.get_id()));

        // when b2 is added, b1 coherency should be updated
        blocktree.add(b2.clone().into());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
    }

    #[test]
    fn test_update_coherency_one_block() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b2
        //
        // blocktree is updated with b1

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        // root must be coherent but b2 isn't
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
    }

    #[test]
    fn test_update_coherency_two_blocks_scenario_one() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b3
        //
        // blocktree is updated with b2 followed by b1

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&b2, None, &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b3 should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        // root must be coherent but b3 and b2 should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));

        // add block 1
        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_two_blocks_scenario_two() {
        // Initial blocktree
        //  g
        //  |
        // ...
        //  |
        //  b3
        //
        // blocktree is updated with b1 followed by b2

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, None, &[2]);
        let b3 = get_next_block(&b2, None, &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b3 should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 1
        blocktree.add(b1.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root and block 1 must be coherent but b3 should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        // add block 2
        blocktree.add(b2.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_multiple_children() {
        // Initial blocktree
        //      g
        //      |
        //  ___...___
        //  |       |
        //  b2      b3
        //
        // blocktree is updated with b1

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, None, &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        // root must be coherent but b2 and b3 should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
    }

    #[test]
    fn test_update_coherency_multiple_grandchildren() {
        // Initial blocktree
        //      g
        //      |
        //  ___...____
        //  |        |
        //  b2    __ b3 __
        //  |     |      |
        //  b4    b5     b6
        //
        // blocktree is updated with missing b1

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b1 = get_next_block(&g, Some(Round(2)), &[1]);
        let b2 = get_next_block(&b1, Some(Round(3)), &[2]);
        let b3 = get_next_block(&b1, Some(Round(4)), &[3]);
        let b4 = get_next_block(&b2, Some(Round(5)), &[4]);
        let b5 = get_next_block(&b3, Some(Round(6)), &[5]);
        let b6 = get_next_block(&b3, Some(Round(7)), &[6]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;
        blocktree.add(g.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&g.header().qc).is_none()); // root naturally don't have missing ancestor

        blocktree.add(b2.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b2.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b3.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b3.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b4.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b4.header().qc)
                .unwrap()
                .last_block_id
                == b2.header().get_parent_id()
        );

        blocktree.add(b5.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b5.header().qc)
                .unwrap()
                .last_block_id
                == b3.header().get_parent_id()
        );

        blocktree.add(b6.clone().into());
        assert!(
            blocktree
                .maybe_fill_path_to_root(&b6.header().qc)
                .unwrap()
                .last_block_id
                == b3.get_parent_id()
        );

        // root must be coherent but rest of the blocks should not
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        assert!(!blocktree.is_coherent(&b2.get_id()));
        assert!(!blocktree.is_coherent(&b3.get_id()));
        assert!(!blocktree.is_coherent(&b4.get_id()));
        assert!(!blocktree.is_coherent(&b5.get_id()));
        assert!(!blocktree.is_coherent(&b6.get_id()));

        blocktree.add(b1.clone().into());
        assert!(blocktree.maybe_fill_path_to_root(&b2.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b3.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b4.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b5.header().qc).is_none());
        assert!(blocktree.maybe_fill_path_to_root(&b6.header().qc).is_none());

        // all blocks must be coherent
        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        assert!(blocktree.is_coherent(&g.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b1.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b1.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b2.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b2.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b3.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b3.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b4.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b4.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b5.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b5.get_id()));
        blocktree.try_update_coherency(
            &mut metrics,
            b6.get_id(),
            &mut block_policy,
            &state_backend,
        );
        assert!(blocktree.is_coherent(&b6.get_id()));
    }

    #[test]
    fn test_children_update() {
        // Initial block tree
        //   root
        //    |
        //   ... missing b1
        //    |
        //   b2

        let mut metrics = Metrics::default();
        let b1 = get_genesis_block();
        let b2 = get_next_block(&b1, None, &[1]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let block_policy = PassthruBlockPolicy;
        blocktree.add(b2.clone().into());
        assert!(blocktree.root.children_blocks.is_empty());

        blocktree.add(b1.clone().into());
        assert_eq!(blocktree.root.children_blocks, vec![b1.get_id()]);
        let b1_children = blocktree
            .tree
            .get(&b1.get_id())
            .unwrap()
            .children_blocks
            .clone();
        assert_eq!(b1_children, vec![b2.get_id()]);
    }

    #[test]
    fn test_get_high_committable_qc() {
        // Initial block tree. It can't be constructed with honest consensus.
        // Just created for testing purpose
        //      g(b1)
        //      |
        //     (b3) - not received
        //    /   \
        //  b4     b9
        //  |      |
        //  b5     b10
        //  |      |
        //  b6     b11
        //  |
        //  b7

        let mut metrics = Metrics::default();
        let g = get_genesis_block();
        let b3 = get_next_block(&g, Some(Round(3)), &[EMPTY_RLP_TX_LIST]);
        let b4 = get_next_block(&b3, Some(Round(4)), &[EMPTY_RLP_TX_LIST]);
        let b5 = get_next_block(&b4, Some(Round(5)), &[EMPTY_RLP_TX_LIST]);
        let b6 = get_next_block(&b5, Some(Round(6)), &[EMPTY_RLP_TX_LIST]);
        let b7 = get_next_block(&b6, Some(Round(7)), &[EMPTY_RLP_TX_LIST]);
        let b9 = get_next_block(&b3, Some(Round(9)), &[EMPTY_RLP_TX_LIST]);
        let b10 = get_next_block(&b9, Some(Round(10)), &[EMPTY_RLP_TX_LIST]);
        let b11 = get_next_block(&b10, Some(Round(11)), &[EMPTY_RLP_TX_LIST]);

        let genesis_qc: QC = QuorumCertificate::genesis_qc();
        let mut blocktree = BlockTreeType::new(RootInfo {
            round: genesis_qc.get_round(),
            seq_num: GENESIS_SEQ_NUM,
            epoch: genesis_qc.get_epoch(),
            block_id: genesis_qc.get_block_id(),
            timestamp_ns: GENESIS_TIMESTAMP,
        });
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(4));
        let mut block_policy = PassthruBlockPolicy;

        // insertion order: insert all blocks except b3, then b3
        blocktree.add(g.clone().into());
        blocktree.add(b4.into());
        blocktree.add(b5.into());
        blocktree.add(b6.into());
        blocktree.add(b7.into());
        blocktree.add(b9.into());
        blocktree.add(b10.into());
        blocktree.add(b11.clone().into());

        blocktree.add(b3.into());

        blocktree.try_update_coherency(&mut metrics, g.get_id(), &mut block_policy, &state_backend);
        let high_commit_qc = blocktree.get_high_committable_qc();
        assert_eq!(high_commit_qc, Some(b11.get_qc().clone()));
    }
}
