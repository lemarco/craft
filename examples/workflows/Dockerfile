# Release build of the workflows showcase for docker compose.
FROM rust:slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --manifest-path examples/workflows/Cargo.toml

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/examples/workflows/target/release/trembita-showcase-workflows /usr/local/bin/showcase
COPY dev/certs/generate.sh /app/generate.sh
RUN chmod +x /app/generate.sh
ENTRYPOINT ["showcase"]
