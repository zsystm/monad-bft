use std::collections::BTreeMap;

use super::leader_election::LeaderElection;
use crate::voter::Voter;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round, Stake};
use rand::{distributions::WeightedIndex, prelude::*};
use rand_chacha::ChaCha20Rng;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct WeightedRandomLeaderSelection<PT: PubKey> {
    voters: Vec<Voter<PT>>,
}

impl<PT: PubKey> Default for WeightedRandomLeaderSelection<PT> {
    fn default() -> Self {
        Self {
            voters: Default::default(),
        }
    }
}

impl<PT: PubKey> LeaderElection for WeightedRandomLeaderSelection<PT> {
    type NodeIdPubKey = PT;
    fn get_leader(
        &self,
        round: Round,
        _epoch: Epoch,
        _validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        // The `_validator_list` parameter is included to maintain compatibility with the `LeaderElection` trait interface.
        // It's not used in the current implementation of `WeightedRandomLeaderSelection` because voters already provide the validator list. But it allows for other
        // extensions where the validator list might be needed  for other leader selection algorithms (weighted_round_robin).

        // Check if the voters list is empty at the very beginning
        self.panic_if_empty(); //
        let seed = round.0;
        let mut rng = ChaCha20Rng::seed_from_u64(seed);

        let stakes: Vec<u64> = self
            .voters
            .iter()
            .map(|v| v.voting_power.0 as u64)
            .collect();
        //FIXME: consider overflow:
        if let Ok(dist) = WeightedIndex::new(stakes) {
            let selected_index = dist.sample(&mut rng);
            self.voters[selected_index].address
        } else {
            // If creating a weighted distribution fails, log a warning.
            warn!("Failed to create a weighted distribution. No valid leader can be selected.");
            panic!("No valid leader can be selected due to distribution creation failure.");
            // FIXME: Not sure if we want panic here or return a result type( Result<NodeId, &'static str> ) and let the caller decide on what to do in this case.
        }
    }
}

impl<PT: PubKey> WeightedRandomLeaderSelection<PT> {
    fn panic_if_empty(&self) {
        if self.voters.is_empty() {
            panic!("Voter list is empty, cannot select a leader.");
        }
    }
    pub fn start_new_epoch(&mut self, voting_powers: Vec<(NodeId<PT>, Stake)>) {
        self.voters.clear();
        for (addr, vp) in voting_powers.into_iter() {
            let voter = Voter {
                address: addr,
                voting_power: vp,
            };
            if voter.verified() {
                self.voters.push(voter);
            } else {
                warn!("Ignoring voter {:?} with zero voting power", voter.address);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopPubKey};
    use monad_types::{NodeId, Round, Stake};
    use rand::{thread_rng, RngCore};
    use std::collections::HashMap;

    fn generate_unique_node_id(input: u8) -> NodeId<NopPubKey> {
        let mut rng = rand::thread_rng();
        let mut key_bytes = [0u8; 32];
        rng.fill_bytes(&mut key_bytes);
        // Modify the first byte with the input to ensure uniqueness for simplicity.
        key_bytes[0] = input;

        let key_pair = NopKeyPair::from_bytes(&mut key_bytes)
            .expect("Failed to create a valid key pair from bytes");
        NodeId::new(key_pair.pubkey())
    }

    #[test]
    fn test_new_weighted_random_leader_selection() {
        let selection = WeightedRandomLeaderSelection::<NopPubKey>::default();
        assert_eq!(selection.voters.len(), 0);
    }

    #[test]
    fn test_add_voters_and_select_leader() {
        let node_id1 = generate_unique_node_id(1);
        let node_id2 = generate_unique_node_id(2);
        let node_id3 = generate_unique_node_id(3);

        let mut selection = WeightedRandomLeaderSelection::default();
        selection.start_new_epoch(vec![
            (node_id1, Stake(20)),
            (node_id2, Stake(10)),
            (node_id3, Stake(70)),
        ]);

        let leader = selection.get_leader(Round(4), Epoch(0), &Default::default());
        assert_eq!(leader, node_id3);
    }

    #[test]
    fn test_leader_selection_with_zero_stakes() {
        let node_id1 = generate_unique_node_id(1);
        let node_id2 = generate_unique_node_id(2);
        let node_id3 = generate_unique_node_id(3);

        let mut selection = WeightedRandomLeaderSelection::default();
        selection.start_new_epoch(vec![
            (node_id1, Stake(0)),
            (node_id2, Stake(0)),
            (node_id3, Stake(10)),
        ]);

        let leader = selection.get_leader(Round(1), Epoch(0), &Default::default());
        println!(
            "Number of voters after start_new_epoch: {}",
            selection.voters.len()
        );
        assert_ne!(leader, node_id1);
        assert_ne!(leader, node_id2);
        assert_eq!(leader, node_id3);
    }

    #[test]
    fn test_voter_verification() {
        let node_id1 = generate_unique_node_id(1);
        let node_id2 = generate_unique_node_id(2);
        let voter = Voter {
            address: node_id1,
            voting_power: Stake(0), // Unverified voter due to zero stake
        };
        assert!(!voter.verified());

        let voter = Voter {
            address: node_id2,
            voting_power: Stake(10), // Verified voter
        };
        assert!(voter.verified());
    }

    #[test]
    fn test_leader_election_distribution() {
        // Create a vector to hold voters with different stakes
        let voters = vec![
            (generate_unique_node_id(1), Stake(100)),
            (generate_unique_node_id(2), Stake(300)),
            (generate_unique_node_id(3), Stake(600)),
        ];

        let mut selection = WeightedRandomLeaderSelection::default();
        selection.start_new_epoch(voters.clone());

        // Map to count the number of times each leader is selected
        let mut leader_counts: HashMap<NodeId<_>, u32> = HashMap::new();
        let total_iterations = 1000;

        // Simulate leader selection 1000 times
        for _ in 0..total_iterations {
            let round = Round(thread_rng().next_u64());
            let leader =
                selection.get_leader(round, Epoch(0), &voters.clone().into_iter().collect());
            *leader_counts.entry(leader).or_insert(0) += 1;
        }

        // Calculate total stake for normalization
        let total_stake: i64 = voters.iter().map(|(_, stake)| stake.0).sum();

        // Check the distribution roughly matches the stake distribution
        for (node_id, stake) in &voters {
            let count = *leader_counts.get(node_id).unwrap_or(&0) as f64;
            let stake_ratio = stake.0 as f64 / total_stake as f64;
            let election_ratio = count / total_iterations as f64;

            println!("NodeId {:?} with stake {:?} was elected {} times. Stake ratio: {}, Election ratio: {}",
                     node_id, stake, count, stake_ratio, election_ratio);

            // Assert that the election ratio is within a reasonable range of the stake ratio
            // This range can be adjusted based on the expected accuracy of your leader selection algorithm
            assert!(
                (stake_ratio - election_ratio).abs() < 0.1,
                "Election frequency does not match stake distribution closely enough."
            );
        }
    }
}
