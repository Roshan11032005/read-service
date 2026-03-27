// Package reader implements the auth-event read path.
//
// Lookup flow (with hot cache enabled):
//
//  1. Hash "ref:{refID}" on the consistent ring → shardID
//  2. Call RangeQuery on that RocksDB shard → []Metadata (S3 pointers + byte ranges)
//  3. For each Metadata entry:
//     a. Build the canonical pointer: "bucket|key|startByte|endByte"
//     b. Check hot cache (BadgerDB) using the pointer as the signature key
//        HIT  → use cached bytes, skip S3 entirely
//        MISS → fetch byte range from S3, write result into hot cache
//  4. Unmarshal the raw NDJSON line → AuthEvent
//  5. Return all events in QueryResult
//
// When hot cache is disabled (cache field is nil) the path is identical to the
// original: RocksDB → S3, with no overhead from the cache code.
package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"read-orchestrator/internal/hotcache"
	"read-orchestrator/internal/ring"
	"read-orchestrator/internal/s3store"
	"read-orchestrator/internal/shard"
	"read-orchestrator/internal/telemetry"
	pb "read-orchestrator/proto/rocksdb"
)

// ─── Types ───────────────────────────────────────────────────────────────────

// AuthEvent matches the shape the test client expects exactly.
type AuthEvent struct {
	EventID     string         `json:"event_id"`
	ReferenceID string         `json:"reference_id"`
	Timestamp   int64          `json:"timestamp"`
	Category    string         `json:"category"`
	EventType   string         `json:"event_type"`
	Data        map[string]any `json:"data"`
}

// QueryMetrics carries timing and counter data for one /query call.
type QueryMetrics struct {
	RocksDBQueriesCount  int   `json:"rocksdb_queries_count"`
	RocksDBQueryTimeMs   int64 `json:"rocksdb_query_time_ms"`
	S3FetchesCount       int   `json:"s3_fetches_count"`
	S3FetchTimeMs        int64 `json:"s3_fetch_time_ms"`
	CacheHits            int   `json:"cache_hits"`
	CacheMisses          int   `json:"cache_misses"`
	MetadataEntriesFound int   `json:"metadata_entries_found"`
	EventsParsed         int   `json:"events_parsed"`
	BytesRead            int64 `json:"bytes_read"`
}

// QueryResult is the full response body returned from POST /query.
type QueryResult struct {
	Events      []AuthEvent  `json:"events"`
	TotalEvents int          `json:"total_events"`
	QueryTimeMs int64        `json:"query_time_ms"`
	Metrics     QueryMetrics `json:"metrics"`
}

// ─── Service ─────────────────────────────────────────────────────────────────

// Service resolves auth events by reference ID.
type Service struct {
	ring    *ring.Ring
	shards  *shard.Pool
	s3      *s3store.Client
	cache   *hotcache.Cache    // nil when HOT_CACHE_ENABLED=false; all paths nil-safe
	metrics *telemetry.Metrics // nil when OTEL unavailable; all paths nil-safe

	querySem         chan struct{} // bounds concurrent in-flight queries
	s3BatchConcurrency int        // concurrent S3 fetches within a batch
}

// New wires up a Service. Apply functional options to attach the cache and
// metrics; both are optional and the service works correctly without either.
func New(r *ring.Ring, shards *shard.Pool, s3 *s3store.Client, opts ...Option) *Service {
	svc := &Service{
		ring:               r,
		shards:             shards,
		s3:                 s3,
		querySem:           make(chan struct{}, 256), // default; overridden by WithMaxConcurrency
		s3BatchConcurrency: 64,                      // default; overridden by WithS3BatchConcurrency
	}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// Option is a functional option for Service.
type Option func(*Service)

// WithMetrics attaches OTEL metrics recording to the service.
func WithMetrics(m *telemetry.Metrics) Option {
	return func(s *Service) { s.metrics = m }
}

// WithCache attaches the hot-data cache to the service.
// When attached, every resolved S3 pointer is checked in the cache before any
// network call is made. A pointer hit serves bytes from local disk.
// When nil (default), the service queries S3 directly as before.
func WithCache(c *hotcache.Cache) Option {
	return func(s *Service) { s.cache = c }
}

// WithMaxConcurrency caps the number of concurrent in-flight queries.
func WithMaxConcurrency(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.querySem = make(chan struct{}, n)
		}
	}
}

// WithS3BatchConcurrency sets the max concurrent S3 fetches per batch.
func WithS3BatchConcurrency(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.s3BatchConcurrency = n
		}
	}
}

// ─── LookupMany ──────────────────────────────────────────────────────────────

// LookupMany fetches events for multiple refIDs concurrently.
// This backs POST /query.
//
// The flow is split into phases to maximise throughput:
//
//  1. For each refID, two goroutines start in parallel:
//     a. RocksDB range query (gRPC network call) → []Metadata
//     b. BadgerDB prefix scan (local disk I/O)    → map[pointer]→data
//     The cache scan finishes while the RocksDB call is still in-flight,
//     so by the time metadata arrives we already know what's cached.
//  2. Compare: pointers in metadata vs cached set → identify misses.
//  3. All cache misses across every refID are collected into a single batch
//     and sent to s3store.GetRangeBatch, which merges nearby byte ranges in
//     the same S3 object into fewer, larger GetObject calls.
//  4. Results are distributed back, the hot cache is populated (with ref
//     index), and events are parsed.
func (s *Service) LookupMany(ctx context.Context, refIDs []string, startTime, endTime string) (*QueryResult, error) {
	if len(refIDs) == 0 {
		return nil, fmt.Errorf("at least one reference_id is required")
	}

	// Acquire concurrency slot — prevents thundering herd at high QPS.
	select {
	case s.querySem <- struct{}{}:
		defer func() { <-s.querySem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	queryStart := time.Now()
	if m := s.metrics; m != nil {
		m.QueriesTotal.Add(ctx, 1)
		m.RefIDsQueried.Add(ctx, int64(len(refIDs)))
	}

	var (
		rocksdbCount    atomic.Int64
		rocksdbTimeMs   atomic.Int64
		s3Count         atomic.Int64
		s3TimeMs        atomic.Int64
		cacheHits       atomic.Int64
		cacheMisses     atomic.Int64
		metadataEntries atomic.Int64
		bytesRead       atomic.Int64
	)

	// ── Phase 1: RocksDB ∥ BadgerDB cache scan (parallel per refID) ───────
	type fetchItem struct {
		refIdx  int
		metaIdx int
		ref     string
		bucket  string
		key     string
		start   int64
		end     int64
		ptr     string
		ts      int64
	}

	type refState struct {
		events   []AuthEvent
		resolved []bool
		err      error
	}
	perRef := make([]refState, len(refIDs))

	var (
		toFetchMu sync.Mutex
		toFetch   []fetchItem
	)

	var wg sync.WaitGroup
	for i, refID := range refIDs {
		wg.Add(1)
		go func(idx int, ref string) {
			defer wg.Done()

			// Launch two concurrent sub-tasks per refID:
			//   A — RocksDB range query (network)
			//   B — BadgerDB prefix scan (local disk)
			type rocksResult struct {
				metas []*pb.Metadata
				err   error
			}
			rocksCh := make(chan rocksResult, 1)
			cacheCh := make(chan map[string][]byte, 1)

			// ── A: RocksDB range query ────────────────────────────────
			go func() {
				rocksStart := time.Now()
				shardID := s.ring.Get("ref:" + ref)
				log.Printf("[reader] lookup ref=%s → shard %d", ref, shardID)

				metas, err := s.shards.RangeQuery(ctx, shardID, ref, startTime, endTime)
				rocksElapsed := time.Since(rocksStart)
				rocksdbCount.Add(1)
				rocksdbTimeMs.Add(rocksElapsed.Milliseconds())
				if m := s.metrics; m != nil {
					m.RocksDBQueriesTotal.Add(ctx, 1)
					m.RocksDBQueryDuration.Record(ctx, rocksElapsed.Seconds())
				}
				rocksCh <- rocksResult{metas: metas, err: err}
			}()

			// ── B: BadgerDB prefix scan (runs in parallel with A) ─────
			go func() {
				if hc := s.cache; hc != nil {
					cached, err := hc.GetCachedForRef(ref)
					if err != nil {
						log.Printf("[reader] hotcache prefix scan ref=%s: %v", ref, err)
						cacheCh <- nil
						return
					}
					cacheCh <- cached
				} else {
					cacheCh <- nil
				}
			}()

			// Wait for both.
			rr := <-rocksCh
			cachedMap := <-cacheCh // nil when cache disabled or scan failed

			if rr.err != nil {
				if m := s.metrics; m != nil {
					m.RocksDBQueriesFailed.Add(ctx, 1)
				}
				perRef[idx] = refState{err: fmt.Errorf("ref %s range_query: %w", ref, rr.err)}
				return
			}
			if len(rr.metas) == 0 {
				return
			}
			metadataEntries.Add(int64(len(rr.metas)))

			events := make([]AuthEvent, len(rr.metas))
			resolved := make([]bool, len(rr.metas))
			var misses []fetchItem

			for j, meta := range rr.metas {
				bucket, key, start, end, perr := parsePointer(meta.S3Path, meta.StartByte, meta.EndByte)
				if perr != nil {
					log.Printf("[reader] warning ref=%s meta[%d] pointer parse: %v", ref, j, perr)
					continue
				}
				ptr := hotcache.Pointer(bucket, key, start, end)

				// ── Check the pre-fetched cache map first ─────────
				if cachedMap != nil {
					if raw, ok := cachedMap[ptr]; ok {
						cacheHits.Add(1)
						bytesRead.Add(int64(len(raw)))
						if m := s.metrics; m != nil {
							m.BytesRead.Add(ctx, int64(len(raw)))
						}
						ev, perr := parseEvent(raw, ref, meta.Timestamp)
						if perr != nil {
							log.Printf("[reader] warning ref=%s meta[%d] parse cached: %v", ref, j, perr)
							continue
						}
						events[j] = ev
						resolved[j] = true
						continue
					}
					cacheMisses.Add(1)
				}

				misses = append(misses, fetchItem{
					refIdx: idx, metaIdx: j, ref: ref,
					bucket: bucket, key: key,
					start: start, end: end,
					ptr: ptr, ts: meta.Timestamp,
				})
			}

			perRef[idx] = refState{events: events, resolved: resolved}

			if len(misses) > 0 {
				toFetchMu.Lock()
				toFetch = append(toFetch, misses...)
				toFetchMu.Unlock()
			}
		}(i, refID)
	}
	wg.Wait()

	// ── Phase 2: Batch S3 fetch (all cache misses, merged) ────────────────
	if len(toFetch) > 0 {
		reqs := make([]s3store.RangeRequest, len(toFetch))
		for i, f := range toFetch {
			reqs[i] = s3store.RangeRequest{
				Bucket:    f.bucket,
				Key:       f.key,
				StartByte: f.start,
				EndByte:   f.end,
			}
		}

		fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		batchResults, stats := s.s3.GetRangeBatch(fctx, reqs, s.s3BatchConcurrency)
		cancel()

		s3Count.Add(int64(stats.S3Calls))
		s3TimeMs.Add(stats.TotalTime.Milliseconds())
		if m := s.metrics; m != nil {
			m.S3FetchesTotal.Add(ctx, int64(stats.S3Calls))
			m.S3FetchDuration.Record(ctx, stats.TotalTime.Seconds())
		}

		for i, res := range batchResults {
			f := toFetch[i]
			if res.Err != nil {
				if m := s.metrics; m != nil {
					m.S3FetchesFailed.Add(ctx, 1)
				}
				log.Printf("[reader] warning ref=%s s3 %s/%s [%d,%d): %v", f.ref, f.bucket, f.key, f.start, f.end, res.Err)
				continue
			}
			bytesRead.Add(int64(len(res.Data)))
			if m := s.metrics; m != nil {
				m.BytesRead.Add(ctx, int64(len(res.Data)))
			}

			// Populate hot cache with ref index (best-effort).
			if hc := s.cache; hc != nil {
				if serr := hc.SetWithRef(f.ref, f.ptr, res.Data); serr != nil {
					log.Printf("[reader] hotcache set err ptr=%s: %v", f.ptr, serr)
				}
			}

			parseStart := time.Now()
			ev, perr := parseEvent(res.Data, f.ref, f.ts)
			if m := s.metrics; m != nil {
				m.EventParseDuration.Record(ctx, time.Since(parseStart).Seconds())
			}
			if perr != nil {
				log.Printf("[reader] warning ref=%s parse: %v", f.ref, perr)
				continue
			}
			perRef[f.refIdx].events[f.metaIdx] = ev
			perRef[f.refIdx].resolved[f.metaIdx] = true
		}
	}

	// ── Phase 3: Aggregate results ────────────────────────────────────────
	var allEvents []AuthEvent
	var failedCount int
	for i := range refIDs {
		if perRef[i].err != nil {
			log.Printf("[reader] warning: %v", perRef[i].err)
			failedCount++
			continue
		}
		for j, ev := range perRef[i].events {
			if perRef[i].resolved != nil && perRef[i].resolved[j] {
				allEvents = append(allEvents, ev)
			}
		}
	}

	if m := s.metrics; m != nil {
		m.QueryDuration.Record(ctx, time.Since(queryStart).Seconds())
		m.EventsReturned.Add(ctx, int64(len(allEvents)))
		if failedCount > 0 {
			m.QueriesFailure.Add(ctx, 1)
		} else {
			m.QueriesSuccess.Add(ctx, 1)
		}
	}

	if failedCount > 0 && len(allEvents) == 0 {
		return nil, fmt.Errorf("all %d ref_id lookups failed", failedCount)
	}
	if failedCount > 0 {
		log.Printf("[reader] partial failure: %d/%d ref_ids failed", failedCount, len(refIDs))
	}

	return &QueryResult{
		Events:      allEvents,
		TotalEvents: len(allEvents),
		QueryTimeMs: time.Since(queryStart).Milliseconds(),
		Metrics: QueryMetrics{
			RocksDBQueriesCount:  int(rocksdbCount.Load()),
			RocksDBQueryTimeMs:   rocksdbTimeMs.Load(),
			S3FetchesCount:       int(s3Count.Load()),
			S3FetchTimeMs:        s3TimeMs.Load(),
			CacheHits:            int(cacheHits.Load()),
			CacheMisses:          int(cacheMisses.Load()),
			MetadataEntriesFound: int(metadataEntries.Load()),
			EventsParsed:         len(allEvents),
			BytesRead:            bytesRead.Load(),
		},
	}, nil
}

// Lookup returns events for a single refID — used by GET /events/{ref_id}.
func (s *Service) Lookup(ctx context.Context, refID, startTime, endTime string) ([]AuthEvent, error) {
	res, err := s.LookupMany(ctx, []string{refID}, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return res.Events, nil
}

// LookupOne returns the single event matching refID + eventID, or nil.
func (s *Service) LookupOne(ctx context.Context, refID, eventID string) (*AuthEvent, error) {
	events, err := s.Lookup(ctx, refID, "", "")
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		ev := ev
		if ev.EventID == eventID {
			return &ev, nil
		}
	}
	return nil, nil
}

// ─── pointer parsing ─────────────────────────────────────────────────────────

// parsePointer resolves (bucket, key, startByte, endByte) from the value
// stored by the index-builder in RocksDB metadata.
//
// Case A — index-builder raw format: "bucket|objectKey|startByte|endByte"
// Case B — rocksdb-node proto format: "bucket|objectKey" with start/end in proto fields
func parsePointer(s3Path string, metaStart, metaEnd int64) (bucket, key string, start, end int64, err error) {
	s3Path = strings.TrimLeft(s3Path, "/")

	// Case A: four pipe-separated fields
	if parts := strings.SplitN(s3Path, "|", 4); len(parts) == 4 {
		sb, e1 := strconv.ParseInt(parts[2], 10, 64)
		eb, e2 := strconv.ParseInt(parts[3], 10, 64)
		if e1 != nil || e2 != nil {
			return "", "", 0, 0, fmt.Errorf("bad byte offsets in %q", s3Path)
		}
		return parts[0], parts[1], sb, eb, nil
	}

	// Case B: "bucket|objectKey" with byte range in proto fields
	if metaEnd > metaStart {
		if idx := strings.IndexByte(s3Path, '|'); idx > 0 {
			return s3Path[:idx], s3Path[idx+1:], metaStart, metaEnd, nil
		}
		return "", s3Path, metaStart, metaEnd, nil
	}

	return "", "", 0, 0, fmt.Errorf("cannot resolve pointer s3_path=%q start=%d end=%d",
		s3Path, metaStart, metaEnd)
}

// ─── event parsing ───────────────────────────────────────────────────────────

func parseEvent(raw []byte, refID string, metaTS int64) (AuthEvent, error) {
	// Trim trailing newlines directly on the byte slice — avoids the
	// string(raw) → []byte(line) double-copy that the old code did.
	raw = bytes.TrimRight(raw, "\n\r")

	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return AuthEvent{}, fmt.Errorf("unmarshal JSON: %w", err)
	}

	eventID, _ := fields["_event_id"].(string)
	category, _ := fields["_category"].(string)
	eventType, _ := fields["_event_type"].(string)

	ts := metaTS
	if tsStr, ok := fields["_event_timestamp"].(string); ok {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, tsStr); err == nil {
				ts = t.Unix()
				break
			}
		}
	}

	data := make(map[string]any, len(fields))
	for k, v := range fields {
		data[k] = v
	}
	delete(data, "_event_id")
	delete(data, "_event_timestamp")
	delete(data, "reference_id")

	return AuthEvent{
		EventID:     eventID,
		ReferenceID: refID,
		Timestamp:   ts,
		Category:    category,
		EventType:   eventType,
		Data:        data,
	}, nil
}