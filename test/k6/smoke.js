// ─── Smoke Test ──────────────────────────────────────────────────────────────
//
// Quick sanity check: 1-2 VUs for 30 seconds.
// Run:  k6 run test/k6/smoke.js
//
// With OTEL output:
//   k6 run --out experimental-opentelemetry test/k6/smoke.js
//   (set K6_OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317)

import { sleep } from 'k6';
import { randomRefIDs, buildPayload, queryEndpoint, checkResponse } from './helpers.js';

export const options = {
  stages: [
    { duration: '10s', target: 1 },
    { duration: '20s', target: 2 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],  // p95 < 2s
    http_req_failed:   ['rate<0.01'],   // < 1% errors
  },
};

export default function () {
  // Query 1-3 random ref IDs per request
  const count   = Math.floor(Math.random() * 3) + 1;
  const refIDs  = randomRefIDs(count);
  const payload = buildPayload(refIDs);

  const res = queryEndpoint(payload);
  checkResponse(res);

  sleep(1);
}
