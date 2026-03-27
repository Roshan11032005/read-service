// Package config loads all runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds every runtime setting for the read-orchestrator.
type Config struct {
	// ── HTTP ──────────────────────────────────────────────────────────────────
	HTTPPort string // HTTP_PORT  (default: "8080")

	// ── S3 / MinIO ────────────────────────────────────────────────────────────
	S3Endpoint  string // S3_ENDPOINT   e.g. "http://minio:9000"
	S3Region    string // S3_REGION     e.g. "us-east-1"
	S3Bucket    string // S3_BUCKET     e.g. "events-bucket"
	S3AccessKey string // S3_ACCESS_KEY
	S3SecretKey string // S3_SECRET_KEY

	// ── RocksDB shards ────────────────────────────────────────────────────────
	// SHARD_URLS is a comma-separated list of gRPC addresses, one per shard.
	// e.g. "rocksdb-0:50051,rocksdb-1:50051"
	ShardURLs []string

	// ── OpenTelemetry ─────────────────────────────────────────────────────────
	OTELEndpoint       string // OTEL_ENDPOINT        e.g. "http://otel-collector:4317"
	OTELServiceName    string // OTEL_SERVICE_NAME    (default: "read-orchestrator")
	OTELServiceVersion string // OTEL_SERVICE_VERSION (default: "0.1.0")

	// ── Hot cache (BadgerDB embedded layer) ───────────────────────────────────
	//
	//   HOT_CACHE_ENABLED=true  → BadgerDB opened, S3 skipped on pointer hits.
	//   HOT_CACHE_ENABLED=false → nothing opened, zero overhead. THIS IS DEFAULT.
	HotCacheEnabled    bool          // HOT_CACHE_ENABLED     (default: false)
	HotCacheDir        string        // HOT_CACHE_DIR         (default: "/data/hotcache")
	HotCacheMaxRecords int64         // HOT_CACHE_MAX_RECORDS (default: 10_000_000)
	HotCacheTTL        time.Duration // HOT_CACHE_TTL         (default: 24h)

	// ── Performance tuning ────────────────────────────────────────────────────
	MaxConcurrentQueries int // MAX_CONCURRENT_QUERIES  (default: 256)
	S3MaxConns           int // S3_MAX_CONNECTIONS      (default: 256)
	S3BatchConcurrency   int // S3_BATCH_CONCURRENCY    (default: 64)
	InMemCacheSize       int // IN_MEM_CACHE_SIZE       (default: 100_000)
}

// Load reads all configuration from environment variables.
// Required variables that are absent cause an immediate panic so the
// container exits clearly instead of failing later with a cryptic error.
func Load() (*Config, error) {
	cfg := &Config{}

	// ── HTTP ──────────────────────────────────────────────────────────────────
	cfg.HTTPPort = envOr("HTTP_PORT", "8080")

	// ── S3 ────────────────────────────────────────────────────────────────────
	cfg.S3Endpoint = envOr("S3_ENDPOINT", "http://minio:9000")
	cfg.S3Region = envOr("S3_REGION", "us-east-1")
	cfg.S3Bucket = requireEnv("S3_BUCKET")
	cfg.S3AccessKey = requireEnv("S3_ACCESS_KEY")
	cfg.S3SecretKey = requireEnv("S3_SECRET_KEY")

	// ── Shards ────────────────────────────────────────────────────────────────
	for _, u := range strings.Split(requireEnv("SHARD_URLS"), ",") {
		if u = strings.TrimSpace(u); u != "" {
			cfg.ShardURLs = append(cfg.ShardURLs, u)
		}
	}
	if len(cfg.ShardURLs) == 0 {
		return nil, fmt.Errorf("SHARD_URLS must contain at least one address")
	}

	// ── OTEL ──────────────────────────────────────────────────────────────────
	cfg.OTELEndpoint = envOr("OTEL_ENDPOINT", "")
	cfg.OTELServiceName = envOr("OTEL_SERVICE_NAME", "read-orchestrator")
	cfg.OTELServiceVersion = envOr("OTEL_SERVICE_VERSION", "0.1.0")

	// ── Hot cache ─────────────────────────────────────────────────────────────
	// parseBool returns true ONLY for "true" or "1".
	// Absent / empty / "false" / "0" all map to false — cache is OFF by default.
	cfg.HotCacheEnabled = parseBool(os.Getenv("HOT_CACHE_ENABLED"))
	cfg.HotCacheDir = envOr("HOT_CACHE_DIR", "/data/hotcache")
	cfg.HotCacheMaxRecords = parseInt64(os.Getenv("HOT_CACHE_MAX_RECORDS"), 10_000_000)
	cfg.HotCacheTTL = parseDuration(os.Getenv("HOT_CACHE_TTL"), 24*time.Hour)

	// ── Performance tuning ────────────────────────────────────────────────────
	cfg.MaxConcurrentQueries = parseInt(os.Getenv("MAX_CONCURRENT_QUERIES"), 256)
	cfg.S3MaxConns = parseInt(os.Getenv("S3_MAX_CONNECTIONS"), 256)
	cfg.S3BatchConcurrency = parseInt(os.Getenv("S3_BATCH_CONCURRENCY"), 64)
	cfg.InMemCacheSize = parseInt(os.Getenv("IN_MEM_CACHE_SIZE"), 100_000)

	return cfg, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func requireEnv(key string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	panic(fmt.Sprintf("required environment variable %q is not set", key))
}

// parseBool returns true only for "true" or "1" (case-insensitive).
// Every other value — including the empty string — returns false.
func parseBool(s string) bool {
	s = strings.TrimSpace(strings.ToLower(s))
	return s == "true" || s == "1"
}

func parseInt64(s string, fallback int64) int64 {
	if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil && n > 0 {
		return n
	}
	return fallback
}

func parseDuration(s string, fallback time.Duration) time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
		return d
	}
	return fallback
}

func parseInt(s string, fallback int) int {
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 0 {
		return n
	}
	return fallback
}