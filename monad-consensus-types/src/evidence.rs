use monad_types::{Hash, NodeId, Round};
use sha2::Digest;
use zerocopy::AsBytes;

#[derive(Debug, PartialEq, Eq)]
pub enum EvidencePresence {
    Found,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
#[repr(u8)]
pub enum EvidenceType {
    Equivocation = 0,
    DDoS = 1,
    Amnesia = 2,
    InvalidProposal = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    pub evidence_type: EvidenceType,
    pub round: Round,
    pub malicious_node: NodeId,
    pub signed_invalid_msg: Vec<u8>,
}

impl Evidence {
    pub fn get_hash(&self) -> Hash {
        let mut hasher = sha2::Sha256::new();
        hasher.update(self.round.as_bytes());
        hasher.update([self.evidence_type as u8]);
        hasher.update(self.malicious_node.0.bytes());
        hasher.update(&self.signed_invalid_msg);
        Hash(hasher.finalize().into())
    }
}
