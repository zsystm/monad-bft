#!/bin/bash

# Extract the dynamically assigned port for node0-rpc
RPC_PORT=$(docker ps --filter "name=node0-rpc" --format "{{.Ports}}" | awk -F'[:>-]' '{print $2}')

# Run the cargo command with the dynamically extracted port
cargo run --package monad-eth-testutil \
    --example tx_generator \
    -- \
    --rpc-url "http://0.0.0.0:$RPC_PORT" \
    --rpc-sender-interval-ms 100 \
    --num-final-accounts 500 \
    --num-rpc-senders 1 \
    --txgen-strategy many-to-one \
    --txn-batch-size 100
