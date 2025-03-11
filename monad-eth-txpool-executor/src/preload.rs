use std::{
    collections::{btree_map::Entry, BTreeMap, HashSet},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_primitives::Address;
use futures::{FutureExt, Stream};
use indexmap::IndexSet;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthValidatedBlock;
use monad_types::{DropTimer, Round, SeqNum};
use tokio::time::Sleep;
use tracing::{debug, warn};

// Each account lookup takes ~30us so this blocks the thread for at most a touch under 8ms.
const PRELOAD_CHUNK_MAX_ADDRESSES: usize = 256;
const PRELOAD_INTERVAL_MS: u64 = 1;

pub struct EthTxPoolPreloadEntry {
    // On insertion, we set preload to true. After that, if the SeqNum associated with the entry
    // is no longer in the upcoming predicted leader seqnums then preload is set to false.
    preload: bool,
    pending: IndexSet<Address>,
    done: HashSet<Address>,
}

pub struct EthTxPoolPreloadManager {
    map: BTreeMap<SeqNum, EthTxPoolPreloadEntry>,
    timer: Pin<Box<Sleep>>,
    waker: Option<Waker>,
}

impl Default for EthTxPoolPreloadManager {
    fn default() -> Self {
        Self {
            map: Default::default(),
            timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waker: None,
        }
    }
}

impl EthTxPoolPreloadManager {
    pub fn enter_round(
        &mut self,
        round: Round,
        last_commit_seqnum: SeqNum,
        upcoming_leader_rounds: Vec<Round>,
        generate_requests: impl FnOnce() -> Vec<Address>,
    ) {
        self.map = self.map.split_off(&(last_commit_seqnum + SeqNum(1)));

        let upcoming_predicted_proposal_seqnums = upcoming_leader_rounds
            .iter()
            .filter_map(|leader_round| {
                let round_diff = leader_round.0.checked_sub(round.0)?;

                Some(last_commit_seqnum + SeqNum(2) + SeqNum(round_diff))
            })
            .collect::<HashSet<SeqNum>>();

        self.map.retain(|predicted_proposal_seqnum, entry| {
            // We recreate the pending sets from the txpool snapshot if we're a leader in the
            // current round or the next.
            entry.pending.clear();

            // If the last round TCd then the round would advance while the seqnum does not, in
            // which case some of the previously predicted proposal seqnums are no longer valid.
            // These entries should be invalidated to avoid unnecessary preloading.
            if !upcoming_predicted_proposal_seqnums.contains(predicted_proposal_seqnum) {
                if entry.done.is_empty() {
                    return false;
                }

                entry.preload = false;
            }

            true
        });

        let preload_round = upcoming_leader_rounds.contains(&round);
        let preload_next_round = upcoming_leader_rounds.contains(&(round + Round(1)));

        if !(preload_round || preload_next_round) {
            return;
        }

        if preload_next_round {
            let next_round_predicted_proposal_seqnum = last_commit_seqnum + SeqNum(2) + SeqNum(1);

            self.map
                .entry(next_round_predicted_proposal_seqnum)
                .and_modify(|entry| {
                    // We enable preload for the next round, which remains enabled until we receive a
                    // CreateProposal event for that seqnum and it gets disabled or the entry is
                    // removed.
                    entry.preload = true;
                })
                .or_insert(EthTxPoolPreloadEntry {
                    preload: true,
                    pending: IndexSet::default(),
                    done: HashSet::default(),
                });
        }

        let requests = generate_requests();

        self.add_requests(requests.iter());

        for (predicted_proposal_seqnum, entry) in self.map.iter() {
            debug!(
                ?predicted_proposal_seqnum,
                preload = entry.preload,
                pending = entry.pending.len(),
                done = entry.done.len(),
                upcoming_leader_rounds = ?upcoming_leader_rounds,
                upcoming_predicted_proposal_seqnums =?upcoming_predicted_proposal_seqnums,
                "txpool executor preload manager state"
            );
        }
    }

    pub fn add_requests<'a>(&mut self, requests: impl Iterator<Item = &'a Address> + Clone) {
        for entry in self.map.values_mut() {
            Self::_add_requests(entry, requests.clone(), &mut self.waker);
        }
    }

    fn _add_requests<'a>(
        entry: &mut EthTxPoolPreloadEntry,
        requests: impl Iterator<Item = &'a Address>,
        waker: &mut Option<Waker>,
    ) {
        if !entry.preload {
            return;
        }

        for address in requests {
            if entry.done.contains(address) {
                continue;
            }

            entry.pending.insert(*address);
        }

        if let Some(waker) = waker.take() {
            waker.wake();
        }
    }

    pub fn update_committed_block<ST, SCT>(&mut self, committed_block: &EthValidatedBlock<ST, SCT>)
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        self.map = self
            .map
            .split_off(&(committed_block.get_seq_num() + SeqNum(1)))
    }

    pub fn update_on_create_proposal(&mut self, create_proposal_seqnum: SeqNum) {
        for (seqnum, entry) in self.map.range_mut(..=create_proposal_seqnum) {
            entry.preload = false;
            entry.pending.clear();
        }
    }

    pub fn poll_requests(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<(SeqNum, IndexSet<Address>)> {
        if self.waker.is_some() {
            return Poll::Pending;
        }

        let Poll::Ready(_) = self.timer.poll_unpin(cx) else {
            return Poll::Pending;
        };

        for (predicted_proposal_seqnum, entry) in self.map.iter_mut() {
            if !entry.preload || entry.pending.is_empty() {
                continue;
            }

            let addresses = if PRELOAD_CHUNK_MAX_ADDRESSES > entry.pending.len() {
                std::mem::take(&mut entry.pending)
            } else {
                let mut addresses = entry.pending.split_off(PRELOAD_CHUNK_MAX_ADDRESSES);
                std::mem::swap(&mut entry.pending, &mut addresses);
                addresses
            };

            return Poll::Ready((*predicted_proposal_seqnum, addresses));
        }

        self.waker = Some(cx.waker().to_owned());
        Poll::Pending
    }

    pub fn complete_polled_requests(
        &mut self,
        predicted_proposal_seqnum: SeqNum,
        requests: impl Iterator<Item = Address>,
    ) {
        self.timer.set(tokio::time::sleep(Duration::from_millis(
            PRELOAD_INTERVAL_MS,
        )));

        let Entry::Occupied(mut entry) = self.map.entry(predicted_proposal_seqnum) else {
            warn!(
                ?predicted_proposal_seqnum,
                "txpool executor preload manager received complete request for unknown seqnum"
            );
            return;
        };

        entry.get_mut().done.extend(requests);

        for (predicted_proposal_seqnum, entry) in self.map.iter() {
            debug!(
                ?predicted_proposal_seqnum,
                preload = entry.preload,
                pending = entry.pending.len(),
                done = entry.done.len(),
                "txpool executor preload manager state after completed requests"
            );
        }
    }
}
