# Build + runtime image for the craft-node reference binary, used by the
# docker-compose E2E cluster (backlog T8). Build context is the repo root:
#   docker build -f e2e/Dockerfile -t craft-node .
FROM rust:slim AS build
WORKDIR /src
COPY . .
# Debug build keeps the E2E image quick to produce; correctness, not speed, is
# what this image is for.
RUN cargo build -p craft-node

FROM debian:stable-slim
# openssl for the cert-provisioning script; curl for health polling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/debug/craft-node /usr/local/bin/craft-node
COPY examples/certs/generate.sh /app/generate.sh
RUN chmod +x /app/generate.sh
ENTRYPOINT ["craft-node"]
