// Package reader implements the auth-event read path.
//
// Lookup flow:
//  1. Hash "ref:{refID}" on the consistent ring → shardID
//  2. Call RangeQuery on that RocksDB shard
//  3. Parse each result's S3 pointer: "bucket|objectKey|startByte|endByte"
//  4. Fetch the exact byte range from S3 (parallel, one goroutine per event)
//  5. Unmarshal the raw JSON line and return as AuthEvent
package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"read-orchestrator/internal/ring"
	"read-orchestrator/internal/s3store"
	"read-orchestrator/internal/shard"
)

// ─── Types ────────────────────────────────────────────────────────────────────

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

// ─── Service ──────────────────────────────────────────────────────────────────

// Service resolves auth events by reference ID.
type Service struct {
	ring   *ring.Ring
	shards *shard.Pool
	s3     *s3store.Client
}

// New wires up a Service with the given dependencies.
func New(r *ring.Ring, shards *shard.Pool, s3 *s3store.Client) *Service {
	return &Service{ring: r, shards: shards, s3: s3}
}

// LookupMany fetches events for multiple refIDs concurrently.
// This backs POST /query.
func (s *Service) LookupMany(ctx context.Context, refIDs []string, startTime, endTime string) (*QueryResult, error) {
	if len(refIDs) == 0 {
		return nil, fmt.Errorf("at least one reference_id is required")
	}

	queryStart := time.Now()

	var (
		rocksdbCount    atomic.Int64
		rocksdbTimeMs   atomic.Int64
		s3Count         atomic.Int64
		s3TimeMs        atomic.Int64
		metadataEntries atomic.Int64
		bytesRead       atomic.Int64
	)

	type perRefResult struct {
		events []AuthEvent
		err    error
	}

	perRef := make([]perRefResult, len(refIDs))
	var wg sync.WaitGroup

	for i, refID := range refIDs {
		wg.Add(1)
		go func(idx int, ref string) {
			defer wg.Done()

			// ── RocksDB range query ────────────────────────────────────────
			rocksStart := time.Now()
			shardID := s.ring.Get("ref:" + ref)
			log.Printf("[reader] lookup ref=%s → shard %d", ref, shardID)

			metas, err := s.shards.RangeQuery(ctx, shardID, ref, startTime, endTime)
			rocksdbCount.Add(1)
			rocksdbTimeMs.Add(time.Since(rocksStart).Milliseconds())

			if err != nil {
				perRef[idx] = perRefResult{err: fmt.Errorf("ref %s range_query: %w", ref, err)}
				return
			}
			if len(metas) == 0 {
				return
			}
			metadataEntries.Add(int64(len(metas)))

			// ── S3 byte-range fetches — capped concurrency via semaphore ────
			// Firing thousands of S3 requests simultaneously overwhelms MinIO.
			// Allow at most s3Concurrency in-flight GetRange calls at once.
			const s3Concurrency = 32
			sem := make(chan struct{}, s3Concurrency)

			events := make([]AuthEvent, len(metas))
			fetchErrs := make([]error, len(metas))
			var fetchWg sync.WaitGroup

			for j, meta := range metas {
				fetchWg.Add(1)
				go func(jdx int, s3Path string, mStart, mEnd, mTS int64) {
					defer fetchWg.Done()

					bucket, key, start, end, err := parsePointer(s3Path, mStart, mEnd)
					if err != nil {
						fetchErrs[jdx] = err
						return
					}

					// Acquire semaphore slot before touching S3.
					sem <- struct{}{}
					defer func() { <-sem }()

					fetchStart := time.Now()
					fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
					defer cancel()

					raw, err := s.s3.GetRange(fctx, key, start, end)
					s3Count.Add(1)
					s3TimeMs.Add(time.Since(fetchStart).Milliseconds())

					if err != nil {
						fetchErrs[jdx] = fmt.Errorf("s3 %s/%s [%d,%d): %w", bucket, key, start, end, err)
						return
					}
					bytesRead.Add(int64(len(raw)))

					ev, err := parseEvent(raw, ref, mTS)
					if err != nil {
						fetchErrs[jdx] = err
						return
					}
					events[jdx] = ev
				}(j, meta.S3Path, meta.StartByte, meta.EndByte, meta.Timestamp)
			}
			fetchWg.Wait()

			var out []AuthEvent
			for j, ev := range events {
				if fetchErrs[j] != nil {
					log.Printf("[reader] warning ref=%s fetch[%d]: %v", ref, j, fetchErrs[j])
					continue
				}
				if ev.EventID != "" || ev.ReferenceID != "" {
					out = append(out, ev)
				}
			}
			perRef[idx] = perRefResult{events: out}
		}(i, refID)
	}
	wg.Wait()

	var allEvents []AuthEvent
	for _, r := range perRef {
		if r.err != nil {
			log.Printf("[reader] warning: %v", r.err)
			continue
		}
		allEvents = append(allEvents, r.events...)
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

// ─── pointer parsing ──────────────────────────────────────────────────────────

// parsePointer resolves the S3 location from a RangeQuery Metadata result.
//
// The rocksdb-node returns s3_path as the full path including bucket:
//   "events-bucket/events/timestamp/writer/segment.ndjson"
// with StartByte / EndByte pre-populated in the Metadata fields.
//
// The index-builder (Case A fallback) stores a pipe-delimited raw value:
//   "bucket|objectKey|startByte|endByte"
func parsePointer(s3Path string, metaStart, metaEnd int64) (bucket, key string, start, end int64, err error) {
	s3Path = strings.TrimLeft(s3Path, "/")

	// Case A: raw builder value "bucket|key|startByte|endByte"
	if parts := strings.SplitN(s3Path, "|", 4); len(parts) == 4 {
		sb, e1 := strconv.ParseInt(parts[2], 10, 64)
		eb, e2 := strconv.ParseInt(parts[3], 10, 64)
		if e1 != nil || e2 != nil {
			return "", "", 0, 0, fmt.Errorf("bad byte offsets in %q", s3Path)
		}
		return parts[0], parts[1], sb, eb, nil
	}

	// Case B: rocksdb-node format "bucket|objectKey" with start/end in proto fields.
	// s3Path = "events-bucket|events/1773644165/ec964fbc/42ce5f75.ndjson"
	// Split on "|" — left side is bucket, right side is the full object key
	// (including the "events/" prefix). Do NOT split on "/" which would treat
	// "events" as the bucket and strip the prefix from the key.
	if metaEnd > metaStart {
		if idx := strings.IndexByte(s3Path, '|'); idx > 0 {
			return s3Path[:idx], s3Path[idx+1:], metaStart, metaEnd, nil
		}
		// No separator — treat entire path as the object key.
		return "", s3Path, metaStart, metaEnd, nil
	}

	return "", "", 0, 0, fmt.Errorf("cannot resolve pointer s3_path=%q start=%d end=%d",
		s3Path, metaStart, metaEnd)
}

// ─── event parsing ────────────────────────────────────────────────────────────

func parseEvent(raw []byte, refID string, metaTS int64) (AuthEvent, error) {
	line := strings.TrimRight(string(raw), "\n\r")

	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		return AuthEvent{}, fmt.Errorf("unmarshal JSON: %w", err)
	}

	eventID, _ := fields["_event_id"].(string)
	category, _ := fields["_category"].(string)   // seeder uses "_category"
	eventType, _ := fields["_event_type"].(string) // seeder uses "_event_type"

	// Prefer timestamp embedded in the event JSON.
	ts := metaTS
	if tsStr, ok := fields["_event_timestamp"].(string); ok {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, tsStr); err == nil {
				ts = t.Unix()
				break
			}
		}
	}

	// Strip internal bookkeeping fields from data — callers don't need them.
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