// ─── Soak Test ───────────────────────────────────────────────────────────────
//
// Run at moderate load for an EXTENDED period (4+ hours).
// Detects memory leaks, connection pool exhaustion, GC pressure,
// file descriptor leaks, and performance degradation over time.
//
// THIS IS THE TEST FOR YOUR BILLION-RECORD SCENARIO.
//
// Run:
//   k6 run --out experimental-opentelemetry test/k6/soak.js
//
// Override duration:
//   k6 run -e SOAK_DURATION=8h -e SOAK_VUS=50 test/k6/soak.js

import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { randomRefIDs, buildPayload, queryEndpoint, checkResponse } from './helpers.js';

// ─── Custom metrics ──────────────────────────────────────────────────────────
const eventsReturned = new Counter('custom_events_returned');
const queryLatency   = new Trend('custom_query_latency_ms');
const bytesReceived  = new Counter('custom_bytes_received');

// ─── Config ──────────────────────────────────────────────────────────────────
const SOAK_VUS      = parseInt(__ENV.SOAK_VUS      || '30', 10);
const SOAK_DURATION = __ENV.SOAK_DURATION           || '4h';
const RAMP_DURATION = __ENV.RAMP_DURATION           || '5m';

export const options = {
  stages: [
    { duration: RAMP_DURATION, target: SOAK_VUS },   // ramp up
    { duration: SOAK_DURATION, target: SOAK_VUS },   // sustain for hours
    { duration: RAMP_DURATION, target: 0 },           // ramp down
  ],
  thresholds: {
    http_req_duration:     ['p(95)<5000', 'p(99)<10000'],
    http_req_failed:       ['rate<0.02'],
    custom_query_latency_ms: ['p(95)<5000'],
  },
};

export default function () {
  // Realistic query patterns:
  //   - 60% single ref_id lookups
  //   - 25% small batch (2-5 ref_ids)
  //   - 15% large batch (10-50 ref_ids)
  const roll = Math.random();
  let count;
  if (roll < 0.60) {
    count = 1;
  } else if (roll < 0.85) {
    count = Math.floor(Math.random() * 4) + 2;    // 2-5
  } else {
    count = Math.floor(Math.random() * 41) + 10;  // 10-50
  }

  const refIDs = randomRefIDs(count);

  // Time range filter: last 1h to last 7d
  const now       = Math.floor(Date.now() / 1000);
  const windows   = [3600, 86400, 259200, 604800];  // 1h, 1d, 3d, 7d
  const window    = windows[Math.floor(Math.random() * windows.length)];

  const payload = buildPayload(refIDs, {
    startTime: now - window,
    endTime:   now,
  });

  const res = queryEndpoint(payload);
  checkResponse(res);

  // Track custom metrics
  queryLatency.add(res.timings.duration);
  bytesReceived.add(res.body ? res.body.length : 0);

  try {
    const body = JSON.parse(res.body);
    if (body.total_events) {
      eventsReturned.add(body.total_events);
    }
  } catch (_) { /* ignore parse errors */ }

  // Variable think time: 0.3-2s
  sleep(0.3 + Math.random() * 1.7);
}
