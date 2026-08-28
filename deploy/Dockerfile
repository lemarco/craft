# Production-oriented multi-stage image for the crafty-node reference binary.
#
# Build context is the repo root:
#   docker build -f deploy/Dockerfile -t crafty-node:latest .
#
# The runtime image is distroless (no shell/package manager) for a small,
# hardened surface. Mount TLS PEMs under /etc/crafty/…; the admin port (8080)
# serves health and introspection endpoints.

FROM rust:1.90-slim AS build
WORKDIR /src
# Cache dependencies separately from source for faster incremental rebuilds.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# A release build: this image is meant for real deployments, not just smoke E2E.
RUN cargo build --release -p crafty-node

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /src/target/release/crafty-node /usr/local/bin/crafty-node
# QUIC (peer + client + actor) and the admin HTTP port.
EXPOSE 7443/udp 8080/tcp
ENTRYPOINT ["crafty-node"]
