use std::collections::{HashMap, VecDeque};

use log::info;
use monad_consensus_types::evidence::Evidence;
use monad_types::Hash;

use crate::{EvidenceCommand, Executor};

#[derive(Default)]
pub struct MockEvidencePool {
    evidence_list: VecDeque<Evidence>,
    evidence_store: HashMap<Hash, Evidence>,
}

impl MockEvidencePool {
    fn add_evidence(&mut self, ev: Evidence) {
        let hash = ev.get_hash();
        self.evidence_store.insert(hash, ev.clone());
        self.evidence_list.push_back(ev);
    }
}

impl Executor for MockEvidencePool {
    type Command = EvidenceCommand;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                EvidenceCommand::AddEvidence(ev) => {
                    let hash = ev.get_hash();
                    if self.evidence_store.contains_key(&hash) {
                        info!("Evidence already exists");
                        continue;
                    }
                    self.add_evidence(ev);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use monad_consensus_types::evidence::{Evidence, EvidenceType};
    use monad_testutil::signing::get_key;
    use monad_types::{NodeId, Round};

    use super::*;

    #[test]
    fn test_add_evidence() {
        let mut evpool = MockEvidencePool::default();
        let pk = get_key(0).pubkey();
        let ev = Evidence {
            round: Round(1),
            evidence_type: EvidenceType::InvalidProposal,
            malicious_node: NodeId(pk),
            signed_invalid_msg: Default::default(),
        };
        let hash = ev.get_hash();
        evpool.exec(vec![EvidenceCommand::AddEvidence(ev)]);
        assert!(evpool.evidence_store.contains_key(&hash));
        assert_eq!(evpool.evidence_list.len(), 1);
        assert_eq!(evpool.evidence_list[0].round, Round(1));
        assert_eq!(
            evpool.evidence_list[0].evidence_type,
            EvidenceType::InvalidProposal
        );
    }
}
