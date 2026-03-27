// ─── Stress Test ─────────────────────────────────────────────────────────────
//
// Push the service past breaking point to find the ceiling.
// Progressively increases VUs until errors spike, then ramps down
// to verify recovery.
//
// Run:
//   k6 run --out experimental-opentelemetry test/k6/stress.js

import { sleep } from 'k6';
import { randomRefIDs, buildPayload, queryEndpoint, checkResponse } from './helpers.js';

const PEAK_VUS = parseInt(__ENV.PEAK_VUS || '500', 10);

export const options = {
  stages: [
    // Staircase ramp
    { duration: '2m',  target: Math.floor(PEAK_VUS * 0.2) },   // 20%
    { duration: '3m',  target: Math.floor(PEAK_VUS * 0.2) },   // hold
    { duration: '2m',  target: Math.floor(PEAK_VUS * 0.4) },   // 40%
    { duration: '3m',  target: Math.floor(PEAK_VUS * 0.4) },   // hold
    { duration: '2m',  target: Math.floor(PEAK_VUS * 0.6) },   // 60%
    { duration: '3m',  target: Math.floor(PEAK_VUS * 0.6) },   // hold
    { duration: '2m',  target: Math.floor(PEAK_VUS * 0.8) },   // 80%
    { duration: '3m',  target: Math.floor(PEAK_VUS * 0.8) },   // hold
    { duration: '2m',  target: PEAK_VUS },                      // 100% PEAK
    { duration: '5m',  target: PEAK_VUS },                      // SUSTAIN PEAK
    { duration: '5m',  target: 0 },                             // recovery
  ],
  thresholds: {
    // More relaxed — we EXPECT some failures at peak
    http_req_duration: ['p(95)<10000'],
    http_req_failed:   ['rate<0.30'],   // up to 30% at extreme load
  },
};

export default function () {
  // Heavier queries under stress: 1-20 ref_ids
  const count   = Math.floor(Math.random() * 20) + 1;
  const refIDs  = randomRefIDs(count);
  const payload = buildPayload(refIDs);

  const res = queryEndpoint(payload);
  checkResponse(res);

  // Minimal think time to maximize pressure
  sleep(0.1 + Math.random() * 0.4);
}
