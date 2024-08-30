FROM rust:1-bullseye AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --example token_xfer_gen && \
    mv target/release/examples/token_xfer_gen token_xfer_gen

# Runner
FROM debian:bullseye-slim
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/token_xfer_gen /usr/local/bin/monad-txn-gen

ENV RUST_LOG=info
ENTRYPOINT ["monad-txn-gen"]
