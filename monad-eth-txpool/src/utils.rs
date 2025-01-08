use alloy_consensus::{Transaction, TxEnvelope};
use monad_eth_tx::EthTransaction;

pub fn effective_tip_per_gas(transaction: &EthTransaction) -> u128 {
    match transaction.tx() {
        TxEnvelope::Legacy(tx) => tx.tx().gas_price,
        TxEnvelope::Eip2930(tx) => tx.tx().gas_price,
        TxEnvelope::Eip1559(tx) => tx.tx().max_priority_fee_per_gas,
        TxEnvelope::Eip4844(tx) => tx.tx().tx().max_priority_fee_per_gas,
        tx => tx.gas_price().unwrap_or_default(), // FIXME is this a sane default?
    }
}
