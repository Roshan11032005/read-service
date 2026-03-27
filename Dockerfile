# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — builder
# ─────────────────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

# ca-certificates → HTTPS to S3 / MinIO
# git             → go mod download resolves VCS-backed modules
RUN apk add --no-cache ca-certificates git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/read-orchestrator ./cmd

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image
# ─────────────────────────────────────────────────────────────────────────────
FROM alpine:3.19

# ca-certificates → TLS for S3 / OTEL endpoints
# tzdata          → time.LoadLocation works correctly
# su-exec         → drop from root → appuser after fixing volume permissions
RUN apk add --no-cache ca-certificates tzdata su-exec

# Non-root user for the process
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Pre-create the hot-cache dir owned by appuser.
# NOTE: a -v mount at runtime replaces this directory, so the entrypoint
#       script re-applies ownership after the mount lands.
RUN mkdir -p /data/hotcache && chown appuser:appgroup /data/hotcache

WORKDIR /app
COPY --from=builder /out/read-orchestrator .

# Entrypoint script: runs as root, fixes /data/hotcache ownership,
# then drops to appuser before exec-ing the binary.
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8089

VOLUME ["/data/hotcache"]

ENV HTTP_PORT=8089 \
    S3_ENDPOINT=http://localhost:9100 \
    S3_REGION=us-east-1 \
    OTEL_ENDPOINT="" \
    OTEL_SERVICE_NAME=read-orchestrator \
    OTEL_SERVICE_VERSION=0.1.0 \
    HOT_CACHE_ENABLED=false \
    HOT_CACHE_DIR=/data/hotcache \
    HOT_CACHE_MAX_RECORDS=10000000 \
    HOT_CACHE_TTL=24h \
    MAX_CONCURRENT_QUERIES=256 \
    S3_MAX_CONNECTIONS=256 \
    S3_BATCH_CONCURRENCY=64 \
    IN_MEM_CACHE_SIZE=100000

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:${HTTP_PORT}/health || exit 1

# Run as root so the entrypoint can chown the mounted volume,
# then su-exec drops to appuser before the binary starts.
USER root
ENTRYPOINT ["/app/entrypoint.sh"]