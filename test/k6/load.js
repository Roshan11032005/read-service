// ─── Load Test ───────────────────────────────────────────────────────────────
//
// Ramp up to target VUs, sustain, then ramp down.
// Simulates normal production traffic patterns.
//
// Run:
//   k6 run --out experimental-opentelemetry test/k6/load.js
//
// Override VUs:
//   k6 run -e MAX_VUS=200 test/k6/load.js

import { sleep } from 'k6';
import { randomRefIDs, buildPayload, queryEndpoint, checkResponse } from './helpers.js';

const MAX_VUS = parseInt(__ENV.MAX_VUS || '100', 10);

export const options = {
  stages: [
    { duration: '2m',  target: Math.floor(MAX_VUS * 0.5) },  // ramp to 50%
    { duration: '5m',  target: MAX_VUS },                     // ramp to 100%
    { duration: '10m', target: MAX_VUS },                     // sustain
    { duration: '3m',  target: 0 },                           // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<3000', 'p(99)<5000'],
    http_req_failed:   ['rate<0.05'],
  },
};

export default function () {
  // Mix of single and batch queries
  const isBatch = Math.random() < 0.3;
  const count   = isBatch ? Math.floor(Math.random() * 10) + 5 : 1;
  const refIDs  = randomRefIDs(count);

  // Optionally add time range filter (70% of requests)
  const opts = {};
  if (Math.random() < 0.7) {
    const now = Math.floor(Date.now() / 1000);
    opts.startTime = now - 86400;  // last 24h
    opts.endTime   = now;
  }

  const payload = buildPayload(refIDs, opts);
  const res = queryEndpoint(payload);
  checkResponse(res);

  sleep(0.5 + Math.random() * 1.5);  // 0.5-2s think time
}
