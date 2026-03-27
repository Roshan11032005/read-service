// ─── Shared helpers for all k6 test scripts ─────────────────────────────────
//
// Usage:
//   import { buildPayload, queryEndpoint, checkResponse } from './helpers.js';

import http from "k6/http";
import { check } from "k6";

// ─── Configuration ───────────────────────────────────────────────────────────

export const BASE_URL = __ENV.BASE_URL || "http://localhost:8088";

// Pool of reference IDs to query.
// In a real billion-record test, generate these from your seeder output.
// You can also load from a file: open('./ref_ids.txt').split('\n')
const REF_ID_POOL_SIZE = parseInt(__ENV.REF_ID_POOL_SIZE || "1000000", 10);
const REF_ID_PREFIX = __ENV.REF_ID_PREFIX || "user-";

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Generate a random reference ID from the pool.
 */
export function randomRefID() {
  const idx = Math.floor(Math.random() * REF_ID_POOL_SIZE);
  return `${REF_ID_PREFIX}${idx}`;
}

/**
 * Generate N random, unique reference IDs.
 */
export function randomRefIDs(n) {
  const ids = new Set();
  while (ids.size < n) {
    ids.add(randomRefID());
  }
  return Array.from(ids);
}

/**
 * Build a POST /query JSON payload.
 */
export function buildPayload(refIDs, opts = {}) {
  const payload = { reference_ids: refIDs };
  if (opts.startTime) payload.start_time = opts.startTime;
  if (opts.endTime) payload.end_time = opts.endTime;
  if (opts.category) payload.category = opts.category;
  if (opts.eventType) payload.event_type = opts.eventType;
  if (opts.limit) payload.limit = opts.limit;
  return JSON.stringify(payload);
}

/**
 * POST /query and return the response.
 */
export function queryEndpoint(payload) {
  return http.post(`${BASE_URL}/query`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /query" },
  });
}

/**
 * Standard response checks.
 */
export function checkResponse(res) {
  return check(res, {
    "status is 200": (r) => r.status === 200,
    "has events array": (r) => {
      try {
        return JSON.parse(r.body).events !== undefined;
      } catch {
        return false;
      }
    },
    "latency < 5s": (r) => r.timings.duration < 5000,
    "no server error": (r) => r.status < 500,
  });
}
