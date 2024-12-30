use std::ops::Deref;

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_crypto::hasher::{Hash, Hashable};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};

/// Execution state root hash
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    Serialize,
    Deserialize,
    RlpDecodableWrapper,
    RlpEncodableWrapper,
)]
pub struct StateRootHash(pub Hash);

impl Deref for StateRootHash {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl AsRef<[u8]> for StateRootHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Votes on the state root hash after executing block `seq_num`. `round` is the
/// consensus round where the block is proposed
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
pub struct StateRootHashInfo {
    pub state_root_hash: StateRootHash,
    pub seq_num: SeqNum,
}

impl Hashable for StateRootHashInfo {
    fn hash(&self, state: &mut impl monad_crypto::hasher::Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}
