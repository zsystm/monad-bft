// src/voter.rs

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Stake};
use std::cmp::Ordering;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Voter<PT: PubKey> {
    pub address: NodeId<PT>,
    pub voting_power: Stake,
}

impl<PT: PubKey> Voter<PT> {
    pub fn new(address: NodeId<PT>, voting_power: Stake) -> Self {
        Self {
            address,
            voting_power,
        }
    }

    pub fn verified(&self) -> bool {
        self.voting_power > Stake(0)
    }
}

impl<PT: PubKey> Ord for Voter<PT> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.voting_power.cmp(&other.voting_power).reverse()
    }
}

impl<PT: PubKey> PartialOrd for Voter<PT> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
