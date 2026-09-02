# Build + runtime image for the trembita-node reference binary, used by the
# docker-compose E2E cluster (backlog T8). Build context is the repo root:
#   docker build -f e2e/Dockerfile -t trembita-node .
FROM rust:slim AS build
WORKDIR /src
COPY . .
# Debug build keeps the E2E image quick to produce; correctness, not speed, is
# what this image is for.
RUN cargo build -p trembita-node -p trembita-e2e-client -p trembita-e2e-queue-client

FROM debian:stable-slim
# openssl for the cert-provisioning script; curl for health polling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/debug/trembita-node /usr/local/bin/trembita-node
COPY --from=build /src/target/debug/trembita-e2e-queue-client /usr/local/bin/trembita-e2e-queue-client
COPY dev/certs/generate.sh /app/generate.sh
RUN chmod +x /app/generate.sh
ENTRYPOINT ["trembita-node"]
