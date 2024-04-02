// src/voter.rs

use monad_types::{NodeId, Stake};
use std::cmp::Ordering;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Voter {
    pub address: NodeId,
    pub voting_power: Stake,
}

impl Voter {
    pub fn new(address: NodeId, voting_power: Stake) -> Self {
        Self { address, voting_power }
    }

    pub fn verified(&self) -> bool {
        self.voting_power > Stake(0)
    }
}

impl Ord for Voter {
    fn cmp(&self, other: &Self) -> Ordering {
        self.voting_power.cmp(&other.voting_power).reverse()
    }
}

impl PartialOrd for Voter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
