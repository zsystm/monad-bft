FROM rust:1-bullseye AS builder

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/monad-bft/target \
    cargo build --release --example txgen && \
    mv target/release/examples/txgen txgen

# Runner
FROM debian:bullseye-slim as runner
WORKDIR /usr/src/monad-bft

COPY --from=builder /usr/src/monad-bft/txgen /usr/local/bin/txgen

ENV RUST_LOG=info
CMD txgen