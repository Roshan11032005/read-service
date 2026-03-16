// Package config loads runtime configuration from .env (or real env vars).
// Priority: existing environment variables > .env file entries.
package config

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

// Config is the fully resolved runtime configuration for the read-orchestrator.
type Config struct {
	// S3
	S3Bucket    string
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string

	// Shards — ordered slice of gRPC addresses, one per shard.
	// Order must match the index-builder so the consistent ring routes identically.
	ShardURLs []string

	// HTTP
	HTTPPort string
}

// Load reads .env from the working directory, merges with real env vars,
// and returns a validated Config.
func Load() (*Config, error) {
	loadDotEnv(".env")

	shardURLs, err := parseShardURLs(mustEnv("SHARD_URLS"))
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		S3Bucket:    mustEnv("S3_BUCKET"),
		S3Endpoint:  mustEnv("S3_ENDPOINT"),
		S3AccessKey: mustEnv("S3_ACCESS_KEY"),
		S3SecretKey: mustEnv("S3_SECRET_KEY"),
		S3Region:    envOr("S3_REGION", "us-east-1"),
		ShardURLs:   shardURLs,
		HTTPPort:    envOr("HTTP_PORT", "8088"),
	}

	log.Printf("[config] S3 bucket=%s endpoint=%s", cfg.S3Bucket, cfg.S3Endpoint)
	log.Printf("[config] %d shard(s)", len(cfg.ShardURLs))
	for i, u := range cfg.ShardURLs {
		log.Printf("[config]   shard %d → %s", i, u)
	}
	log.Printf("[config] HTTP port=%s", cfg.HTTPPort)

	return cfg, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("[config] required env var %q is not set", key)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseShardURLs(raw string) ([]string, error) {
	var urls []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			urls = append(urls, part)
		}
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("config: SHARD_URLS is empty")
	}
	return urls, nil
}

// loadDotEnv reads key=value pairs from path and sets them as environment
// variables, skipping keys that are already set.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Printf("[config] .env: %v", err)
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for line := 1; s.Scan(); line++ {
		text := strings.TrimSpace(s.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		k, v, ok := strings.Cut(text, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
}