FROM ubuntu:24.04 AS base

WORKDIR /usr/src/monad-bft

RUN apt update && apt install -y \
  binutils \
  iproute2 \
  clang \
  curl \
  make \
  ca-certificates \
  pkg-config \
  gnupg \
  software-properties-common \
  wget \
  cgroup-tools \
  libstdc++-13-dev \
  gcc-13 \
  g++-13

RUN apt update && apt install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt update && apt install -y \
  libabsl-dev \
  libarchive-dev \
  libbenchmark-dev \
  libbrotli-dev \
  libcap-dev \
  libcgroup-dev \
  libcli11-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

FROM base AS builder

RUN apt update && apt install -y \
  cmake \
  clang \
  libssl-dev

RUN apt update && apt install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"
RUN rustup toolchain install 1.85.0-x86_64-unknown-linux-gnu
ARG TRIEDB_TARGET=triedb_driver

#   Rust and Cxx build.
#
#   We try to copy over just the files that we need to build (especially
#   avoiding test and packaging files in this layer) so that when we're
#   changing things not in the build (particularly tests), we can re-use
#   the cached layer with the last build.
#
#   We do not yet have the --exclude option (that's in 1.7-labs), and COPY
#   copies the *contents* of the source dirctory to the target dir (i.e.
#   `COPY .git/ ./` will copy .git/HEAD to ./HEAD, etc.), so we currently
#   need to specify every subdir individually. (WARNING: This is error
#   prone; it needs to be updated every time we add a new monad-*
#   directory!) Perhaps we should consider collecting most most monad-*
#   dirs into a single monad/ subdir. (We could also try an external tar
#   and have ADD untar into the container, but that requires support code
#   to run before `docker build`.)
#
COPY Cargo.lock Cargo.toml rust-toolchain.toml ./
COPY monad-archive/ monad-archive/
COPY monad-block-persist/ monad-block-persist/
COPY monad-blocksync/ monad-blocksync/
COPY monad-blocktree/ monad-blocktree/
COPY monad-bls/ monad-bls/
COPY monad-chain-config/ monad-chain-config/
COPY monad-compress/ monad-compress/
COPY monad-consensus/ monad-consensus/
COPY monad-consensus-state/ monad-consensus-state/
COPY monad-consensus-types/ monad-consensus-types/
COPY monad-control-panel/ monad-control-panel/
COPY monad-crypto/ monad-crypto/
COPY monad-cxx/ monad-cxx/
COPY monad-dataplane/ monad-dataplane/
COPY monad-debug-node/ monad-debug-node/
COPY monad-debugger/ monad-debugger/
COPY monad-discovery/ monad-discovery/
COPY monad-eth-block-policy/ monad-eth-block-policy/
COPY monad-eth-block-validator/ monad-eth-block-validator/
COPY monad-eth-ledger/ monad-eth-ledger/
COPY monad-eth-testutil/ monad-eth-testutil/
COPY monad-eth-txpool/ monad-eth-txpool/
COPY monad-eth-txpool-executor/ monad-eth-txpool-executor/
COPY monad-eth-txpool-ipc/ monad-eth-txpool-ipc/
COPY monad-eth-txpool-types/ monad-eth-txpool-types/
COPY monad-eth-types/ monad-eth-types/
COPY monad-ethcall/ monad-ethcall/
COPY monad-executor/ monad-executor/
COPY monad-executor-glue/ monad-executor-glue/
COPY monad-keystore/ monad-keystore/
COPY monad-ledger/ monad-ledger/
COPY monad-merkle/ monad-merkle/
COPY monad-mock-swarm/ monad-mock-swarm/
COPY monad-multi-sig/ monad-multi-sig/
COPY monad-node/ monad-node/
COPY monad-node-config/ monad-node-config/
COPY monad-perf-util/ monad-perf-util/
COPY monad-proto/ monad-proto/
COPY monad-randomized-tests/ monad-randomized-tests/
COPY monad-raptor/ monad-raptor/
COPY monad-raptorcast/ monad-raptorcast/
COPY monad-router-filter/ monad-router-filter/
COPY monad-router-scheduler/ monad-router-scheduler/
COPY monad-rpc/ monad-rpc/
COPY monad-scripts/ monad-scripts/
COPY monad-secp/ monad-secp/
COPY monad-state/ monad-state/
COPY monad-state-backend/ monad-state-backend/
COPY monad-statesync/ monad-statesync/
COPY monad-testground/ monad-testground/
COPY monad-testutil/ monad-testutil/
COPY monad-transformer/ monad-transformer/
COPY monad-triedb/ monad-triedb/
COPY monad-triedb-cache/ monad-triedb-cache/
COPY monad-triedb-utils/ monad-triedb-utils/
COPY monad-twins/ monad-twins/
COPY monad-types/ monad-types/
COPY monad-updaters/ monad-updaters/
COPY monad-validator/ monad-validator/
COPY monad-wal/ monad-wal/

RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell -DQUILL_ACTIVE_LOG_LEVEL=QUILL_LOG_LEVEL_CRITICAL" \
    CC=gcc-13 CXX=g++-13 cargo build --release --bin monad-node --bin monad-keystore --bin debug-node --example ledger-tail --example wal2json --example wal-tool --example triedb-debug --example triedb-bench && \
    mv target/release/monad-node node && \
    mv target/release/monad-keystore keystore && \
    mv target/release/debug-node debug-node && \
    mv target/release/examples/ledger-tail ledger-tail && \
    mv target/release/examples/wal2json wal2json && \
    mv target/release/examples/wal-tool wal-tool && \
    mv target/release/examples/triedb-debug triedb-debug && \
    mv target/release/examples/triedb-bench triedb-bench && \
    cp `ls -Lt $(find target/release | grep -e "libtriedb_driver.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libmonad_statesync.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libquill.so") | awk -F/ '!seen[$NF]++'` . && \
    cp `ls -Lt $(find target/release | grep -e "libkeccak.so") | awk -F/ '!seen[$NF]++'` .

# Debug runner
FROM base AS runner-debug

WORKDIR /usr/src/monad-bft
ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/src/monad-bft/node /usr/local/bin/monad-node
COPY --from=builder /usr/src/monad-bft/keystore /usr/local/bin/keystore
COPY --from=builder /usr/src/monad-bft/debug-node /usr/local/bin/debug-node
COPY --from=builder /usr/src/monad-bft/ledger-tail /usr/local/bin/ledger-tail
COPY --from=builder /usr/src/monad-bft/wal2json /usr/local/bin/wal2json
COPY --from=builder /usr/src/monad-bft/wal-tool /usr/local/bin/wal-tool
COPY --from=builder /usr/src/monad-bft/triedb-debug /usr/local/bin/triedb-debug
COPY --from=builder /usr/src/monad-bft/triedb-bench /usr/local/bin/triedb-bench
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib

# Runner
FROM runner-debug AS runner

RUN strip \
    /usr/local/bin/monad-node \
    /usr/local/bin/keystore \
    /usr/local/bin/debug-node \
    /usr/local/bin/ledger-tail \
    /usr/local/bin/wal2json \
    /usr/local/bin/wal-tool \
    /usr/local/bin/triedb-debug \
    /usr/local/bin/triedb-bench \
    /usr/local/lib/*.so \
    /usr/local/lib/*.so.*
