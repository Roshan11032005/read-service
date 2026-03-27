# Integration Testing — Read Service

## Architecture

```
┌──────────┐     POST /query     ┌──────────────┐
│   k6     │ ──────────────────► │ read-service  │
│ (load)   │                     │ (your code)   │
└────┬─────┘                     └──────┬───────┘
     │ k6 metrics (OTLP)               │ app metrics + traces (OTLP)
     ▼                                 ▼
┌───────────────────────────────────────────────┐
│          OpenTelemetry Collector               │
│  (receives from both k6 and read-service)      │
└──────┬────────────────────────┬───────────────┘
       ▼                        ▼
┌─────────────┐          ┌─────────────┐
│ Prometheus  │          │    Tempo    │
│ (metrics)   │          │  (traces)   │
└──────┬──────┘          └──────┬──────┘
       ▼                        ▼
┌───────────────────────────────────────┐
│              Grafana                   │
│  Dashboard: "Read Service — Perf"     │
└───────────────────────────────────────┘
```

## Prerequisites

```bash
# Install k6
brew install k6

# Ensure Docker is running
docker info
```

## Quick Start

### 1. Start the observability stack

```bash
docker compose -f test/docker-compose.observability.yml up -d
```

Wait ~15 seconds for all services to be healthy.

### 2. Configure your read-service to send OTEL to the collector

Set these env vars in your `.env` or shell before starting the read-service:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318
```

Then start the read-service as usual.

### 3. Run a test

```bash
# Smoke test (30 seconds, quick sanity check)
k6 run test/k6/smoke.js

# With OTEL output → Grafana
K6_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 \
k6 run --out experimental-opentelemetry test/k6/smoke.js
```

### 4. Open Grafana

Navigate to [http://localhost:3000](http://localhost:3000) (admin/admin).

The **"Read Service — Performance"** dashboard is auto-provisioned under the "Read Service" folder.

## Test Profiles

| Script      | Purpose                      | Duration | VUs                | When to use                    |
| ----------- | ---------------------------- | -------- | ------------------ | ------------------------------ |
| `smoke.js`  | Sanity check                 | 30s      | 1-2                | After every deploy             |
| `load.js`   | Normal traffic simulation    | 20 min   | 100 (configurable) | Before releases                |
| `soak.js`   | **Billion-record endurance** | 4+ hours | 30 (configurable)  | Detect memory leaks, GC issues |
| `stress.js` | Find breaking point          | 32 min   | 500 (configurable) | Capacity planning              |

## Running the Soak Test (Billion-Record Scenario)

The soak test is specifically designed for your billion-record endurance testing:

```bash
# Default: 30 VUs for 4 hours
K6_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 \
k6 run --out experimental-opentelemetry test/k6/soak.js

# Custom: 50 VUs for 8 hours with 1M ref_id pool
K6_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 \
k6 run --out experimental-opentelemetry \
  -e SOAK_VUS=50 \
  -e SOAK_DURATION=8h \
  -e REF_ID_POOL_SIZE=1000000 \
  test/k6/soak.js
```

### Query distribution in soak test

- **60%** single ref_id lookups
- **25%** small batches (2-5 ref_ids)
- **15%** large batches (10-50 ref_ids)
- Variable time windows: 1h, 1d, 3d, 7d

### What to watch on the dashboard during soak

| Panel            | Healthy signal     | Problem signal                     |
| ---------------- | ------------------ | ---------------------------------- |
| Query Latency    | Flat p95/p99 lines | Upward trend over hours            |
| Error Rate       | Near zero          | Spikes or gradual increase         |
| Bytes Read/sec   | Steady             | Sudden drops (S3 issues)           |
| RocksDB Latency  | Flat               | Increasing (compaction pressure)   |
| S3 Fetch Latency | Flat               | Drift (connection pool exhaustion) |

## Environment Variables

| Variable                         | Default                 | Description                           |
| -------------------------------- | ----------------------- | ------------------------------------- |
| `BASE_URL`                       | `http://localhost:8088` | read-service URL                      |
| `REF_ID_POOL_SIZE`               | `1000000`               | Size of random ref_id pool            |
| `REF_ID_PREFIX`                  | `user-`                 | Prefix for generated ref_ids          |
| `MAX_VUS`                        | `100`                   | Max VUs for load test                 |
| `SOAK_VUS`                       | `30`                    | VUs for soak test                     |
| `SOAK_DURATION`                  | `4h`                    | Duration of soak test sustained phase |
| `PEAK_VUS`                       | `500`                   | Peak VUs for stress test              |
| `K6_OTEL_EXPORTER_OTLP_ENDPOINT` | —                       | OTEL Collector gRPC endpoint          |

## Distributed k6 (for extreme scale)

For truly massive load beyond what a single machine can generate:

```bash
# Install k6-operator on your Kubernetes cluster
kubectl apply -f https://github.com/grafana/k6-operator/releases/latest/download/bundle.yaml

# Create a ConfigMap from your test script
kubectl create configmap soak-test --from-file=test/k6/soak.js

# Run distributed (example: 10 pods × 50 VUs = 500 VUs total)
# See: https://grafana.com/docs/k6/latest/testing-guides/running-distributed-tests/
```

## Cleanup

```bash
docker compose -f test/docker-compose.observability.yml down -v
```
