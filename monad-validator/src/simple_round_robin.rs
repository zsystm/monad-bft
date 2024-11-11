use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::leader_election::LeaderElection;

#[derive(Clone)]
pub struct SimpleRoundRobin<PT>(PhantomData<PT>);
impl<PT> Default for SimpleRoundRobin<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> LeaderElection for SimpleRoundRobin<PT> {
    type NodeIdPubKey = PT;

    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        let validators: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| (*stake != Stake(0)).then_some(node_id))
            .collect();
        *validators[round.0 as usize % validators.len()]
    }

    fn next_leader_round(
        &self,
        current_round: Round,
        leader: NodeId<Self::NodeIdPubKey>,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> Option<Round> {
        let validators: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| (*stake != Stake(0)).then_some(node_id))
            .collect();

        let leader_idx = validators
            .iter()
            .enumerate()
            .find_map(|(idx, nodeid)| leader.eq(nodeid).then_some(idx))?
            as u64;

        let current_round_idx = current_round.0 / (validators.len() as u64);

        Some(match current_round_idx.cmp(&leader_idx) {
            std::cmp::Ordering::Less => Round(current_round.0 + leader_idx - current_round_idx),
            std::cmp::Ordering::Equal => current_round,
            std::cmp::Ordering::Greater => {
                Round(current_round.0 + validators.len() as u64 + leader_idx - current_round_idx)
            }
        })
    }
}
