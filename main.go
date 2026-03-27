package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cespare/xxhash/v2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// ─────────────────────────────────────────────
//  Config
// ─────────────────────────────────────────────

type BucketConfig struct {
	Name      string
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

type ShardNode struct {
	ID  int
	URL string
}

type Config struct {
	Buckets []BucketConfig
	Shards  []ShardNode

	HTTPPort string

	OTELEndpoint       string
	OTELServiceName    string
	OTELServiceVersion string

	MaxConcurrentS3Fetches int
	MaxConcurrentRefIDs    int
	QueryTimeout           time.Duration
	S3FetchTimeout         time.Duration
	RocksDBQueryTimeout    time.Duration
}

func loadConfig() Config {
	var buckets []BucketConfig
	for i := 0; ; i++ {
		val := os.Getenv(fmt.Sprintf("BUCKET_%d", i))
		if val == "" {
			break
		}
		parts := strings.Split(val, "|")
		if len(parts) != 5 {
			log.Printf("Invalid BUCKET_%d format", i)
			continue
		}
		buckets = append(buckets, BucketConfig{
			Name:      parts[0],
			Endpoint:  parts[1],
			AccessKey: parts[2],
			SecretKey: parts[3],
			Region:    parts[4],
		})
	}

	if len(buckets) == 0 {
		buckets = []BucketConfig{
			{
				Name:      getEnv("S3_BUCKET", "events-bucket"),
				Endpoint:  getEnv("S3_ENDPOINT", "http://localhost:9002"),
				AccessKey: getEnv("S3_ACCESS_KEY", "admin"),
				SecretKey: getEnv("S3_SECRET_KEY", "strongpassword"),
				Region:    getEnv("S3_REGION", "us-east-1"),
			},
		}
	}

	shards := loadShards()
	if len(shards) == 0 {
		log.Fatal("[read-service] FATAL: No shards configured")
	}

	return Config{
		Buckets:                buckets,
		Shards:                 shards,
		HTTPPort:               getEnv("HTTP_PORT", "8088"),
		OTELEndpoint:           getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		OTELServiceName:        getEnv("OTEL_SERVICE_NAME", "read-service"),
		OTELServiceVersion:     getEnv("OTEL_SERVICE_VERSION", "1.0.0"),
		MaxConcurrentS3Fetches: getEnvInt("MAX_CONCURRENT_S3_FETCHES", 10),
		MaxConcurrentRefIDs:    getEnvInt("MAX_CONCURRENT_REF_IDS", 20),
		QueryTimeout:           getEnvDuration("QUERY_TIMEOUT", 30*time.Second),
		S3FetchTimeout:         getEnvDuration("S3_FETCH_TIMEOUT", 10*time.Second),
		RocksDBQueryTimeout:    getEnvDuration("ROCKSDB_QUERY_TIMEOUT", 5*time.Second),
	}
}

func loadShards() []ShardNode {
	if shardURLs := os.Getenv("SHARD_URLS"); shardURLs != "" {
		return parseShardURLs(shardURLs)
	}

	var shards []ShardNode
	for i := 0; ; i++ {
		shardURL := os.Getenv(fmt.Sprintf("SHARD_%d", i))
		if shardURL == "" {
			break
		}
		shards = append(shards, ShardNode{
			ID:  i,
			URL: strings.TrimSuffix(shardURL, "/"),
		})
		log.Printf("[config] Shard %d: %s", i, shardURL)
	}

	if len(shards) == 0 {
		numShards := getEnvInt("NUM_SHARDS", 0)
		startPort := getEnvInt("NODE_START_PORT", 4001)
		host := getEnv("SHARD_HOST", "localhost")

		if numShards > 0 {
			for i := 0; i < numShards; i++ {
				shardURL := fmt.Sprintf("http://%s:%d", host, startPort+i)
				shards = append(shards, ShardNode{
					ID:  i,
					URL: shardURL,
				})
			}
		}
	}

	return shards
}

func parseShardURLs(urlList string) []ShardNode {
	var shards []ShardNode
	urls := strings.Split(urlList, ",")
	for i, rawURL := range urls {
		rawURL = strings.TrimSpace(rawURL)
		if rawURL == "" {
			continue
		}
		shards = append(shards, ShardNode{
			ID:  i,
			URL: strings.TrimSuffix(rawURL, "/"),
		})
		log.Printf("[config] Shard %d: %s", i, rawURL)
	}
	return shards
}

// ─────────────────────────────────────────────
//  OpenTelemetry Setup
// ─────────────────────────────────────────────

type OTELMetrics struct {
	queriesTotal            metric.Int64Counter
	queriesSuccess          metric.Int64Counter
	queriesFailure          metric.Int64Counter
	refIDsQueried           metric.Int64Counter
	rocksDBQueriesTotal     metric.Int64Counter
	rocksDBQueriesFailed    metric.Int64Counter
	s3FetchesTotal          metric.Int64Counter
	s3FetchesFailed         metric.Int64Counter
	eventsReturned          metric.Int64Counter
	bytesRead               metric.Int64Counter

	queryDuration          metric.Float64Histogram
	rocksDBQueryDuration   metric.Float64Histogram
	s3FetchDuration        metric.Float64Histogram
	eventParseDuration     metric.Float64Histogram
	metadataLookupDuration metric.Float64Histogram
}

func initOpenTelemetry(cfg Config) (func(context.Context) error, *OTELMetrics, error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.OTELServiceName),
			semconv.ServiceVersion(cfg.OTELServiceVersion),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.OTELEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tracerProvider)

	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.OTELEndpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	meter := meterProvider.Meter(cfg.OTELServiceName)
	metrics := &OTELMetrics{}
	var mErr error

	metrics.queriesTotal, mErr = meter.Int64Counter("queries.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: queries.total: %v", mErr)
	}
	metrics.queriesSuccess, mErr = meter.Int64Counter("queries.success")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: queries.success: %v", mErr)
	}
	metrics.queriesFailure, mErr = meter.Int64Counter("queries.failure")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: queries.failure: %v", mErr)
	}
	metrics.refIDsQueried, mErr = meter.Int64Counter("queries.ref_ids.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: queries.ref_ids.total: %v", mErr)
	}
	metrics.rocksDBQueriesTotal, mErr = meter.Int64Counter("rocksdb.queries.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: rocksdb.queries.total: %v", mErr)
	}
	metrics.rocksDBQueriesFailed, mErr = meter.Int64Counter("rocksdb.queries.failed")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: rocksdb.queries.failed: %v", mErr)
	}
	metrics.s3FetchesTotal, mErr = meter.Int64Counter("s3.fetches.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: s3.fetches.total: %v", mErr)
	}
	metrics.s3FetchesFailed, mErr = meter.Int64Counter("s3.fetches.failed")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: s3.fetches.failed: %v", mErr)
	}
	metrics.eventsReturned, mErr = meter.Int64Counter("events.returned.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: events.returned.total: %v", mErr)
	}
	metrics.bytesRead, mErr = meter.Int64Counter("bytes.read.total")
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: bytes.read.total: %v", mErr)
	}

	metrics.queryDuration, mErr = meter.Float64Histogram("query.duration", metric.WithUnit("s"))
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: query.duration: %v", mErr)
	}
	metrics.rocksDBQueryDuration, mErr = meter.Float64Histogram("rocksdb.query.duration", metric.WithUnit("s"))
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: rocksdb.query.duration: %v", mErr)
	}
	metrics.s3FetchDuration, mErr = meter.Float64Histogram("s3.fetch.duration", metric.WithUnit("s"))
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: s3.fetch.duration: %v", mErr)
	}
	metrics.eventParseDuration, mErr = meter.Float64Histogram("event.parse.duration", metric.WithUnit("s"))
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: event.parse.duration: %v", mErr)
	}
	metrics.metadataLookupDuration, mErr = meter.Float64Histogram("metadata.lookup.duration", metric.WithUnit("s"))
	if mErr != nil {
		log.Printf("[read-service] OTEL metric init warning: metadata.lookup.duration: %v", mErr)
	}

	shutdown := func(ctx context.Context) error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("tracer provider shutdown: %w", err)
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("meter provider shutdown: %w", err)
		}
		return nil
	}

	log.Printf("[read-service] OpenTelemetry initialized — endpoint: %s", cfg.OTELEndpoint)
	return shutdown, metrics, nil
}

// ─────────────────────────────────────────────
//  Types
// ─────────────────────────────────────────────

type Event struct {
	EventID     string                 `json:"event_id"`
	ReferenceID string                 `json:"reference_id"`
	Timestamp   int64                  `json:"timestamp"`
	Category    string                 `json:"category"`
	EventType   string                 `json:"event_type"`
	Data        map[string]interface{} `json:"data"`
}

type Metadata struct {
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	StartByte int64  `json:"start_byte"`
	EndByte   int64  `json:"end_byte"`
	Timestamp int64  `json:"timestamp"`
}

type QueryRequest struct {
	ReferenceIDs []string `json:"reference_ids"`
	StartTime    int64    `json:"start_time,omitempty"`
	EndTime      int64    `json:"end_time,omitempty"`
	Category     string   `json:"category,omitempty"`
	EventType    string   `json:"event_type,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

type QueryResponse struct {
	Events      []Event      `json:"events"`
	TotalEvents int          `json:"total_events"`
	QueryTimeMs int64        `json:"query_time_ms"`
	Metrics     QueryMetrics `json:"metrics"`
}

type QueryMetrics struct {
	RocksDBQueriesCount  int   `json:"rocksdb_queries_count"`
	RocksDBQueryTimeMs   int64 `json:"rocksdb_query_time_ms"`
	S3FetchesCount       int   `json:"s3_fetches_count"`
	S3FetchTimeMs        int64 `json:"s3_fetch_time_ms"`
	MetadataEntriesFound int   `json:"metadata_entries_found"`
	EventsParsed         int   `json:"events_parsed"`
	BytesRead            int64 `json:"bytes_read"`
}

// ─────────────────────────────────────────────
//  Service
// ─────────────────────────────────────────────

type ReadService struct {
	cfg          Config
	s3Clients    map[string]*s3.Client
	httpClient   *http.Client
	tracer       trace.Tracer
	metrics      *OTELMetrics
	shutdownOTEL func(context.Context) error

	totalQueries   atomic.Int64
	totalEvents    atomic.Int64
	totalS3Fetches atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
}

func NewReadService(cfg Config) (*ReadService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	shutdownOTEL, metrics, err := initOpenTelemetry(cfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize OpenTelemetry: %w", err)
	}

	tracer := otel.Tracer(cfg.OTELServiceName)

	svc := &ReadService{
		cfg:          cfg,
		s3Clients:    make(map[string]*s3.Client),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		tracer:       tracer,
		metrics:      metrics,
		shutdownOTEL: shutdownOTEL,
		ctx:          ctx,
		cancel:       cancel,
	}

	return svc, nil
}

func (s *ReadService) Start() error {
	_, span := s.tracer.Start(s.ctx, "service.start")
	defer span.End()

	log.Printf("[read-service] Starting with %d shard(s), %d bucket(s)",
		len(s.cfg.Shards), len(s.cfg.Buckets))

	for _, bucket := range s.cfg.Buckets {
		client, err := newS3Client(bucket)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "S3 init failed")
			return fmt.Errorf("S3 init failed for bucket %s: %w", bucket.Name, err)
		}
		s.s3Clients[bucket.Name] = client
		log.Printf("[read-service] S3 client ready for bucket: %s endpoint: %s",
			bucket.Name, bucket.Endpoint)
	}

	if err := s.testShardConnectivity(); err != nil {
		log.Printf("[read-service] ⚠️  WARNING: Shard connectivity test failed: %v", err)
	}

	go s.startHTTPServer()

	span.SetStatus(codes.Ok, "Service started")
	log.Printf("[read-service] Service started on port %s", s.cfg.HTTPPort)
	return nil
}

func (s *ReadService) Stop() {
	log.Printf("[read-service] Shutting down...")

	_, span := s.tracer.Start(context.Background(), "service.shutdown")
	defer span.End()

	s.cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.shutdownOTEL(shutdownCtx); err != nil {
		log.Printf("[read-service] OTEL shutdown error: %v", err)
	}

	span.SetStatus(codes.Ok, "Shutdown complete")
	log.Printf("[read-service] Shutdown complete")
}

func (s *ReadService) testShardConnectivity() error {
	log.Printf("[read-service] Testing connectivity to %d shards...", len(s.cfg.Shards))

	var wg sync.WaitGroup
	errCh := make(chan error, len(s.cfg.Shards))

	for _, shard := range s.cfg.Shards {
		wg.Add(1)
		go func(sh ShardNode) {
			defer wg.Done()

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(sh.URL + "/health")
			if err != nil {
				errCh <- fmt.Errorf("shard %d (%s): %w", sh.ID, sh.URL, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("shard %d (%s): status %d", sh.ID, sh.URL, resp.StatusCode)
				return
			}

			log.Printf("[read-service] ✓ Shard %d (%s) is reachable", sh.ID, sh.URL)
		}(shard)
	}

	wg.Wait()
	close(errCh)

	var errors []string
	for err := range errCh {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("unreachable shards: %s", strings.Join(errors, "; "))
	}

	log.Printf("[read-service] ✓ All %d shards are reachable", len(s.cfg.Shards))
	return nil
}

// ─────────────────────────────────────────────
//  Query Execution
// ─────────────────────────────────────────────

func (s *ReadService) ExecuteQuery(ctx context.Context, req QueryRequest) (*QueryResponse, error) {
	ctx, span := s.tracer.Start(ctx, "query.execute",
		trace.WithAttributes(
			attribute.Int("query.ref_ids.count", len(req.ReferenceIDs)),
			attribute.Int64("query.start_time", req.StartTime),
			attribute.Int64("query.end_time", req.EndTime),
			attribute.String("query.category", req.Category),
			attribute.String("query.event_type", req.EventType),
		))
	defer span.End()

	queryStart := time.Now()

	s.totalQueries.Add(1)
	s.metrics.queriesTotal.Add(ctx, 1)
	s.metrics.refIDsQueried.Add(ctx, int64(len(req.ReferenceIDs)))

	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()

	var queryMetrics QueryMetrics

	metadataStart := time.Now()
	metadataByRefID, err := s.queryMetadataForRefIDs(queryCtx, req.ReferenceIDs, req.StartTime, req.EndTime)
	metadataDuration := time.Since(metadataStart)

	queryMetrics.RocksDBQueriesCount = len(req.ReferenceIDs)
	queryMetrics.RocksDBQueryTimeMs = metadataDuration.Milliseconds()

	s.metrics.metadataLookupDuration.Record(ctx, metadataDuration.Seconds(),
		metric.WithAttributes(attribute.Int("ref_ids.count", len(req.ReferenceIDs))))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Metadata lookup failed")
		s.metrics.queriesFailure.Add(ctx, 1)
		return nil, fmt.Errorf("metadata lookup failed: %w", err)
	}

	var allMetadata []Metadata
	for _, metaList := range metadataByRefID {
		allMetadata = append(allMetadata, metaList...)
		queryMetrics.MetadataEntriesFound += len(metaList)
	}

	span.SetAttributes(attribute.Int("metadata.entries.found", len(allMetadata)))

	if len(allMetadata) == 0 {
		span.AddEvent("no_metadata_found")
		s.metrics.queriesSuccess.Add(ctx, 1)

		queryDuration := time.Since(queryStart)
		s.metrics.queryDuration.Record(ctx, queryDuration.Seconds())

		return &QueryResponse{
			Events:      []Event{},
			TotalEvents: 0,
			QueryTimeMs: queryDuration.Milliseconds(),
			Metrics:     queryMetrics,
		}, nil
	}

	s3FetchStart := time.Now()
	events, bytesRead, err := s.fetchEventsFromS3(queryCtx, allMetadata, req.Category, req.EventType, req.Limit)
	s3FetchDuration := time.Since(s3FetchStart)

	queryMetrics.S3FetchesCount = len(s.groupMetadataByS3Path(allMetadata))
	queryMetrics.S3FetchTimeMs = s3FetchDuration.Milliseconds()
	queryMetrics.EventsParsed = len(events)
	queryMetrics.BytesRead = bytesRead

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "S3 fetch failed")
		s.metrics.queriesFailure.Add(ctx, 1)
		return nil, fmt.Errorf("S3 fetch failed: %w", err)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp < events[j].Timestamp
	})

	s.totalEvents.Add(int64(len(events)))
	s.metrics.eventsReturned.Add(ctx, int64(len(events)))
	s.metrics.bytesRead.Add(ctx, bytesRead)

	queryDuration := time.Since(queryStart)
	s.metrics.queryDuration.Record(ctx, queryDuration.Seconds(),
		metric.WithAttributes(
			attribute.Int("events.count", len(events)),
			attribute.Int("ref_ids.count", len(req.ReferenceIDs)),
		))

	span.SetAttributes(
		attribute.Int("events.returned", len(events)),
		attribute.Int64("bytes.read", bytesRead),
		attribute.Float64("query.duration.seconds", queryDuration.Seconds()),
	)
	span.SetStatus(codes.Ok, "Query complete")

	s.metrics.queriesSuccess.Add(ctx, 1)

	log.Printf("[read-service] Query complete: %d events from %d ref_ids in %s (RocksDB: %s, S3: %s)",
		len(events), len(req.ReferenceIDs), queryDuration, metadataDuration, s3FetchDuration)

	return &QueryResponse{
		Events:      events,
		TotalEvents: len(events),
		QueryTimeMs: queryDuration.Milliseconds(),
		Metrics:     queryMetrics,
	}, nil
}

// ─────────────────────────────────────────────
//  RocksDB Metadata Lookup - OPTIMIZED
// ─────────────────────────────────────────────

func (s *ReadService) queryMetadataForRefIDs(ctx context.Context, refIDs []string, startTime, endTime int64) (map[string][]Metadata, error) {
	ctx, span := s.tracer.Start(ctx, "rocksdb.query_multiple_refs",
		trace.WithAttributes(attribute.Int("ref_ids.count", len(refIDs))))
	defer span.End()

	// OPTIMIZED: Query only ONE shard per reference ID
	// Each ref_id's events are all on the same shard

	result := make(map[string][]Metadata)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, len(refIDs))

	sem := make(chan struct{}, s.cfg.MaxConcurrentRefIDs)

	s.metrics.rocksDBQueriesTotal.Add(ctx, int64(len(refIDs)))

	for _, refID := range refIDs {
		wg.Add(1)
		sem <- struct{}{}

		go func(ref string) {
			defer wg.Done()
			defer func() { <-sem }()

			shardCtx, cancel := context.WithTimeout(ctx, s.cfg.RocksDBQueryTimeout)
			defer cancel()

			// OPTIMIZED: Calculate which shard contains this ref_id
			refIDPart := "ref:" + ref
			shardID := consistentHash(refIDPart, len(s.cfg.Shards))
			shard := s.cfg.Shards[shardID]

			metadata, err := s.queryShardForRefID(shardCtx, shard.URL, ref, startTime, endTime)
			if err != nil {
				errCh <- err
				s.metrics.rocksDBQueriesFailed.Add(ctx, 1)
				return
			}

			if len(metadata) > 0 {
				mu.Lock()
				result[ref] = metadata
				mu.Unlock()
			}
		}(refID)
	}

	wg.Wait()
	close(errCh)

	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	span.SetAttributes(
		attribute.Int("results.count", len(result)),
		attribute.Int("errors.count", len(errors)),
	)

	if len(errors) > 0 {
		span.AddEvent("partial_failures", trace.WithAttributes(attribute.Int("failure.count", len(errors))))
		log.Printf("[read-service] %d/%d queries had errors", len(errors), len(refIDs))

		// If ALL lookups failed, surface the error to the caller.
		if len(result) == 0 {
			span.SetStatus(codes.Error, "All metadata lookups failed")
			return nil, fmt.Errorf("%d/%d metadata lookups failed: %v", len(errors), len(refIDs), errors[0])
		}
	}

	span.SetStatus(codes.Ok, "Metadata lookup complete")
	return result, nil
}

func (s *ReadService) queryShardForRefID(ctx context.Context, shardURL, refID string, startTime, endTime int64) ([]Metadata, error) {
	ctx, span := s.tracer.Start(ctx, "rocksdb.query_shard",
		trace.WithAttributes(
			attribute.String("ref_id", refID),
			attribute.String("shard.url", shardURL),
		))
	defer span.End()

	url := fmt.Sprintf("%s/get?ref_id=%s", shardURL, refID)
	if startTime > 0 {
		url += fmt.Sprintf("&start_time=%d", startTime)
	}
	if endTime > 0 {
		url += fmt.Sprintf("&end_time=%d", endTime)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Request creation failed")
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "HTTP request failed")
		return nil, fmt.Errorf("query shard: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		span.AddEvent("ref_id_not_found")
		return []Metadata{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("shard returned status %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Non-OK status")
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Response read failed")
		return nil, err
	}

	type RocksDBMetadata struct {
		RefID     string `json:"ref_id"`
		S3Path    string `json:"s3_path"`
		StartByte int64  `json:"start_byte"`
		EndByte   int64  `json:"end_byte"`
		Timestamp int64  `json:"timestamp"`
	}

	var rocksMetadata []RocksDBMetadata
	if err := json.Unmarshal(body, &rocksMetadata); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "JSON unmarshal failed")
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}

	metadata := make([]Metadata, 0, len(rocksMetadata))
	for _, rm := range rocksMetadata {
		bucket, objectKey := parseS3Path(rm.S3Path)
		if bucket == "" || objectKey == "" {
			log.Printf("[read-service] Invalid S3 path format: %s", rm.S3Path)
			continue
		}

		metadata = append(metadata, Metadata{
			Bucket:    bucket,
			ObjectKey: objectKey,
			StartByte: rm.StartByte,
			EndByte:   rm.EndByte,
			Timestamp: rm.Timestamp,
		})
	}

	span.SetAttributes(
		attribute.Int("metadata.count", len(metadata)),
		attribute.Int("response.bytes", len(body)),
	)
	span.SetStatus(codes.Ok, "Query complete")

	return metadata, nil
}

func parseS3Path(s3Path string) (bucket, objectKey string) {
	if strings.Contains(s3Path, "|") {
		parts := strings.SplitN(s3Path, "|", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
	}

	if strings.HasPrefix(s3Path, "s3://") {
		s3Path = strings.TrimPrefix(s3Path, "s3://")
		parts := strings.SplitN(s3Path, "/", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
	}

	return "", ""
}

// ─────────────────────────────────────────────
//  S3 Event Fetching
// ─────────────────────────────────────────────

func (s *ReadService) fetchEventsFromS3(ctx context.Context, metadata []Metadata, category, eventType string, limit int) ([]Event, int64, error) {
	ctx, span := s.tracer.Start(ctx, "s3.fetch_events",
		trace.WithAttributes(attribute.Int("metadata.count", len(metadata))))
	defer span.End()

	groupedMetadata := s.groupMetadataByS3Path(metadata)

	span.SetAttributes(attribute.Int("s3.files.count", len(groupedMetadata)))

	eventsCh := make(chan []Event, len(groupedMetadata))
	errCh := make(chan error, len(groupedMetadata))

	sem := make(chan struct{}, s.cfg.MaxConcurrentS3Fetches)
	var wg sync.WaitGroup

	var totalBytesRead atomic.Int64

	for s3Path, metaList := range groupedMetadata {
		wg.Add(1)
		sem <- struct{}{}

		go func(path string, metas []Metadata) {
			defer wg.Done()
			defer func() { <-sem }()

			fetchCtx, cancel := context.WithTimeout(ctx, s.cfg.S3FetchTimeout)
			defer cancel()

			events, bytesRead, err := s.fetchEventsFromS3File(fetchCtx, path, metas, category, eventType)
			totalBytesRead.Add(bytesRead)

			if err != nil {
				errCh <- fmt.Errorf("fetch %s: %w", path, err)
				s.metrics.s3FetchesFailed.Add(ctx, 1)
				return
			}

			if len(events) > 0 {
				eventsCh <- events
			}
		}(s3Path, metaList)
	}

	wg.Wait()
	close(eventsCh)
	close(errCh)

	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		span.RecordError(errors[0])
		span.AddEvent("partial_failures", trace.WithAttributes(attribute.Int("failure.count", len(errors))))
		log.Printf("[read-service] S3 fetch: %d/%d files failed", len(errors), len(groupedMetadata))
	}

	var allEvents []Event
	for events := range eventsCh {
		allEvents = append(allEvents, events...)

		if limit > 0 && len(allEvents) >= limit {
			allEvents = allEvents[:limit]
			break
		}
	}

	bytesRead := totalBytesRead.Load()

	span.SetAttributes(
		attribute.Int("events.fetched", len(allEvents)),
		attribute.Int64("bytes.read", bytesRead),
		attribute.Int("errors.count", len(errors)),
	)
	span.SetStatus(codes.Ok, "S3 fetch complete")

	s.totalS3Fetches.Add(int64(len(groupedMetadata)))
	s.metrics.s3FetchesTotal.Add(ctx, int64(len(groupedMetadata)))

	return allEvents, bytesRead, nil
}

func (s *ReadService) fetchEventsFromS3File(ctx context.Context, s3Path string, metadata []Metadata, category, eventType string) ([]Event, int64, error) {
	ctx, span := s.tracer.Start(ctx, "s3.fetch_single_file",
		trace.WithAttributes(
			attribute.String("s3.path", s3Path),
			attribute.Int("metadata.count", len(metadata)),
		))
	defer span.End()

	fetchStart := time.Now()

	if len(metadata) == 0 {
		span.AddEvent("no_metadata")
		return []Event{}, 0, nil
	}

	bucket := metadata[0].Bucket
	objectKey := metadata[0].ObjectKey

	client, ok := s.s3Clients[bucket]
	if !ok {
		err := fmt.Errorf("no S3 client for bucket %s", bucket)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Unknown bucket")
		return nil, 0, err
	}

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "S3 GetObject failed")
		return nil, 0, fmt.Errorf("S3 get object: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "S3 read failed")
		return nil, 0, fmt.Errorf("read S3 object: %w", err)
	}

	bytesRead := int64(len(data))

	fetchDuration := time.Since(fetchStart)
	s.metrics.s3FetchDuration.Record(ctx, fetchDuration.Seconds(),
		metric.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.Int64("bytes", bytesRead),
		))

	span.SetAttributes(
		attribute.Int64("s3.bytes.downloaded", bytesRead),
		attribute.Float64("s3.fetch.duration.seconds", fetchDuration.Seconds()),
	)

	var events []Event
	parseStart := time.Now()

	for _, meta := range metadata {
		if meta.StartByte >= int64(len(data)) {
			log.Printf("[read-service] Byte range out of bounds: start=%d, file_size=%d", meta.StartByte, len(data))
			continue
		}

		endByte := meta.EndByte
		if endByte > int64(len(data)) {
			endByte = int64(len(data))
		}

		eventData := data[meta.StartByte:endByte]

		event, err := s.parseEvent(ctx, eventData)
		if err != nil {
			log.Printf("[read-service] Failed to parse event at byte range %d-%d: %v", meta.StartByte, endByte, err)
			span.AddEvent("event.parse.failed",
				trace.WithAttributes(
					attribute.Int64("start_byte", meta.StartByte),
					attribute.Int64("end_byte", endByte),
					attribute.String("error", err.Error()),
				))
			continue
		}

		if category != "" && event.Category != category {
			continue
		}
		if eventType != "" && event.EventType != eventType {
			continue
		}

		events = append(events, event)
	}

	parseDuration := time.Since(parseStart)
	s.metrics.eventParseDuration.Record(ctx, parseDuration.Seconds(),
		metric.WithAttributes(attribute.Int("events.count", len(events))))

	span.SetAttributes(
		attribute.Int("events.parsed", len(events)),
		attribute.Float64("parse.duration.seconds", parseDuration.Seconds()),
	)
	span.SetStatus(codes.Ok, "File fetch complete")

	log.Printf("[read-service] Fetched %d events from s3://%s/%s (%.1f KB in %s)",
		len(events), bucket, objectKey, float64(bytesRead)/1024, fetchDuration)

	return events, bytesRead, nil
}

func (s *ReadService) parseEvent(ctx context.Context, data []byte) (Event, error) {
	var rawEvent map[string]interface{}
	if err := json.Unmarshal(data, &rawEvent); err != nil {
		return Event{}, fmt.Errorf("unmarshal: %w", err)
	}

	event := Event{
		Data: rawEvent,
	}

	if eventID, ok := rawEvent["_event_id"].(string); ok {
		event.EventID = eventID
	}
	if refID, ok := rawEvent["reference_id"].(string); ok {
		event.ReferenceID = refID
	}
	if category, ok := rawEvent["_category"].(string); ok {
		event.Category = category
	}
	if eventType, ok := rawEvent["_event_type"].(string); ok {
		event.EventType = eventType
	}
	if tsStr, ok := rawEvent["_event_timestamp"].(string); ok {
		if ts, err := parseTimestamp(tsStr); err == nil {
			event.Timestamp = ts
		}
	}

	return event, nil
}

func (s *ReadService) groupMetadataByS3Path(metadata []Metadata) map[string][]Metadata {
	grouped := make(map[string][]Metadata)
	for _, meta := range metadata {
		path := fmt.Sprintf("%s/%s", meta.Bucket, meta.ObjectKey)
		grouped[path] = append(grouped[path], meta)
	}
	return grouped
}

// ─────────────────────────────────────────────
//  HTTP Server
// ─────────────────────────────────────────────

func (s *ReadService) startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", s.handleQuery)
	mux.HandleFunc("/internal/health", s.handleHealth)
	mux.HandleFunc("/internal/metrics", s.handleMetrics)

	addr := ":" + s.cfg.HTTPPort
	log.Printf("[read-service] HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[read-service] HTTP error: %v", err)
	}
}

func (s *ReadService) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.ReferenceIDs) == 0 {
		http.Error(w, "reference_ids is required", http.StatusBadRequest)
		return
	}

	if req.EndTime == 0 {
		req.EndTime = time.Now().Unix()
	}

	resp, err := s.ExecuteQuery(r.Context(), req)
	if err != nil {
		log.Printf("[read-service] Query failed: %v", err)
		http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *ReadService) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"shards":  len(s.cfg.Shards),
		"buckets": len(s.cfg.Buckets),
	})
}

func (s *ReadService) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_queries":    s.totalQueries.Load(),
		"total_events":     s.totalEvents.Load(),
		"total_s3_fetches": s.totalS3Fetches.Load(),
	})
}

// ─────────────────────────────────────────────
//  Main
// ─────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	svc, err := NewReadService(cfg)
	if err != nil {
		log.Fatalf("[read-service] Failed to create service: %v", err)
	}

	if err := svc.Start(); err != nil {
		log.Fatalf("[read-service] Failed to start: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	svc.Stop()
}

// ─────────────────────────────────────────────
//  Utilities
// ─────────────────────────────────────────────

func newS3Client(bucket BucketConfig) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(bucket.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(bucket.AccessKey, bucket.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               bucket.Endpoint,
					HostnameImmutable: true,
					SigningRegion:     bucket.Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})
	return client, nil
}

func consistentHash(key string, numShards int) int {
	return int(xxhash.Sum64String(key) % uint64(numShards))
}

func parseTimestamp(s string) (int64, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.Unix(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.Unix(), nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.Unix(), nil
	}
	return 0, fmt.Errorf("cannot parse timestamp: %q", s)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int
	if n, err := fmt.Sscanf(v, "%d", &i); n != 1 || err != nil {
		log.Printf("[read-service] WARNING: env var %s=%q is not a valid integer, using default %d", key, v, def)
		return def
	}
	return i
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}