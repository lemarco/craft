# Copy to deploy/.env — product surface (see docs/env.md in trembita repo).

# Required
TREMBITA_DATA_DIR=/data
TREMBITA_LISTEN=0.0.0.0:443
TREMBITA_CERT_DIR=/certs

# Seed (first node)
TREMBITA_ALLOW_JOIN=1

# Joiners only (uncomment on node 2+)
# TREMBITA_JOIN_SEEDS=1@node1:443

# Multi-node gateway (required when joiners use TREMBITA_JOIN_SEEDS — same value on every node)
# TREMBITA_GATEWAY_SESSION_SECRET=change-me-at-least-16-bytes

# Optional
GATEWAY_TOKEN=dev-change-me
# TREMBITA_JOB_QUEUE=app.jobs

# Do not set in product deploy: TREMBITA_NODE_ID, TREMBITA_PEERS, TREMBITA_HTTP (except `-` for QUIC-only)

# OTLP (when telemetry feature enabled)
# OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
# OTEL_SERVICE_NAME={{PROJECT_NAME}}
