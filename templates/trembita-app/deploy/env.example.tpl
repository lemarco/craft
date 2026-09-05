# Copy to deploy/.env and adjust for local cluster runs.
TREMBITA_DATA_DIR=/data
TREMBITA_ALLOW_JOIN=1
TREMBITA_GATEWAY=0.0.0.0:8090
GATEWAY_TOKEN=dev-change-me

# OTLP (when telemetry feature enabled)
# OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
# OTEL_SERVICE_NAME={{PROJECT_NAME}}
