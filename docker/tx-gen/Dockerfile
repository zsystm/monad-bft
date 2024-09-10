FROM rust:1-bullseye AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --example tx_generator && \
    mv target/release/examples/tx_generator tx_generator

# Runner
FROM debian:bullseye-slim as runner
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/tx_generator /usr/local/bin/tx_generator

ENV RUST_LOG=info
CMD tx_generator