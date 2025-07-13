FROM ubuntu:25.04 AS base

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
  git \
  python-is-python3 \
  cgroup-tools \
  libstdc++-15-dev \
  gcc-15 \
  g++-15

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

# Builder
COPY . .
RUN ASMFLAGS="-march=haswell" CFLAGS="-march=haswell" CXXFLAGS="-march=haswell -DQUILL_ACTIVE_LOG_LEVEL=QUILL_LOG_LEVEL_CRITICAL" \
    CC=gcc-15 CXX=g++-15 cargo build --release --bin monad-node --features full-node --bin monad-keystore --bin monad-debug-node --example ledger-tail --example wal2json --example triedb-bench --example sign-name-record && \
    mv target/release/monad-node monad-node && \
    mv target/release/monad-keystore keystore && \
    mv target/release/monad-debug-node monad-debug-node && \
    mv target/release/examples/ledger-tail ledger-tail && \
    mv target/release/examples/wal2json wal2json && \
    mv target/release/examples/triedb-bench triedb-bench && \
    mv target/release/examples/sign-name-record sign-name-record && \
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

COPY --from=builder /usr/src/monad-bft/monad-node/monad-node /usr/local/bin/monad-full-node
COPY --from=builder /usr/src/monad-bft/keystore /usr/local/bin/keystore
COPY --from=builder /usr/src/monad-bft/monad-debug-node/monad-debug-node /usr/local/bin/monad-debug-node
COPY --from=builder /usr/src/monad-bft/ledger-tail /usr/local/bin/ledger-tail
COPY --from=builder /usr/src/monad-bft/wal2json /usr/local/bin/wal2json
COPY --from=builder /usr/src/monad-bft/triedb-bench /usr/local/bin/triedb-bench
COPY --from=builder /usr/src/monad-bft/sign-name-record /usr/local/bin/sign-name-record
COPY --from=builder /usr/src/monad-bft/*.so /usr/local/lib
COPY --from=builder /usr/src/monad-bft/*.so.* /usr/local/lib

# Runner
FROM runner-debug AS runner

RUN strip \
    /usr/local/bin/monad-full-node \
    /usr/local/bin/keystore \
    /usr/local/bin/monad-debug-node \
    /usr/local/bin/ledger-tail \
    /usr/local/bin/wal2json \
    /usr/local/bin/triedb-bench \
    /usr/local/bin/sign-name-record \
    /usr/local/lib/*.so \
    /usr/local/lib/*.so.*
