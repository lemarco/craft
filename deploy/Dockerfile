# Production-oriented multi-stage image for the trembita-node reference binary.
#
# Build context is the repo root:
#   docker build -f deploy/Dockerfile -t trembita-node:latest .
#
# The runtime image is distroless (no shell/package manager) for a small,
# hardened surface. Mount TLS PEMs under /etc/trembita/…; the admin port (8080)
# serves health and introspection endpoints.

FROM rust:1.90-slim AS build
WORKDIR /src
# Cache dependencies separately from source for faster incremental rebuilds.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# A release build: this image is meant for real deployments, not just smoke E2E.
RUN cargo build --release -p trembita-node

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /src/target/release/trembita-node /usr/local/bin/trembita-node
# QUIC (peer + client + actor) and the admin HTTP port.
EXPOSE 7443/udp 8080/tcp
ENTRYPOINT ["trembita-node"]
