use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConsensusConfig {
    pub execution_delay: u64,
    pub epoch_length: u64,
}
