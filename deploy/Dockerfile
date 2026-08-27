# Production-oriented multi-stage image for the craft-node reference binary,
# used by the Kubernetes manifests in deploy/kubernetes/ (ADR 027 item 6).
#
# Build context is the repo root:
#   docker build -f deploy/Dockerfile -t craft-node:latest .
#
# The runtime image is distroless (no shell/package manager) for a small,
# hardened surface. Certs are mounted from cert-manager Secrets under
# /etc/craft/…; the admin port (8080) serves Kubernetes probes (ADR 025).

FROM rust:1.98-slim AS build
WORKDIR /src
# Cache dependencies separately from source for faster incremental rebuilds.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# A release build: this image is meant for real deployments, not just smoke E2E.
RUN cargo build --release -p craft-node

FROM gcr.io/distroless/cc-debian12 AS runtime
COPY --from=build /src/target/release/craft-node /usr/local/bin/craft-node
# QUIC (peer + client + actor) and the admin HTTP port.
EXPOSE 7443/udp 8080/tcp
ENTRYPOINT ["craft-node"]
