// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use common::SignatureType;
use criterion::{criterion_group, criterion_main, Criterion};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_types::GENESIS_SEQ_NUM;

use self::common::{
    run_txpool_benches, BenchController, SignatureCollectionType, EXECUTION_DELAY, RESERVE_BALANCE,
};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy: EthBlockPolicy<SignatureType, SignatureCollectionType> =
        EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337, RESERVE_BALANCE);

    run_txpool_benches(
        c,
        "insert",
        |controller_config| {
            let pool = EthTxPool::default_testing();

            let (pending_txs, txs) = BenchController::generate_txs(
                controller_config.accounts,
                controller_config.txs,
                controller_config.nonce_var,
                0,
            );

            assert!(pending_txs.is_empty());

            let state_backend = BenchController::generate_state_backend_for_txs(&txs);

            (pool, txs, state_backend)
        },
        |(pool, txs, state_backend)| {
            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(&EthTxPoolMetrics::default(), &mut Vec::default()),
                &block_policy,
                state_backend,
                txs.to_owned(),
                true,
                |_| {},
            );
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
