use monad_consensus_types::signature_collection::SignatureCollection;
use monad_eth_block_policy::EthValidatedBlock;
use monad_eth_tx::EthTransaction;
use monad_types::Round;

#[derive(Debug)]
pub enum EthTxPoolEventLoopEvent<SCT>
where
    SCT: SignatureCollection,
{
    TxBatch(Vec<EthTransaction>),

    CommittedBlock(EthValidatedBlock<SCT>),

    RoundUpdate {
        current_round: Round,
        next_leader_round: Option<Round>,
    },
}
