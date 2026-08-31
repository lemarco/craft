# Release build of the background-jobs showcase for docker compose.
FROM rust:slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --manifest-path examples/background-jobs/Cargo.toml

FROM debian:stable-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/examples/background-jobs/target/release/crafty-showcase-background-jobs /usr/local/bin/showcase
COPY dev/certs/generate.sh /app/generate.sh
RUN chmod +x /app/generate.sh
ENTRYPOINT ["showcase"]
