package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"read-orchestrator/config"
	"read-orchestrator/internal/hotcache"
	"read-orchestrator/internal/reader"
	"read-orchestrator/internal/ring"
	"read-orchestrator/internal/s3store"
	"read-orchestrator/internal/shard"
	"read-orchestrator/internal/telemetry"
)

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// ── S3 / MinIO client ─────────────────────────────────────────────────────
	s3Client, err := s3store.New(cfg)
	if err != nil {
		log.Fatalf("s3store: %v", err)
	}
	log.Printf("[main] S3 ready  bucket=%s  endpoint=%s", cfg.S3Bucket, cfg.S3Endpoint)

	// ── RocksDB shard pool ────────────────────────────────────────────────────
	pool, err := shard.New(cfg.ShardURLs)
	if err != nil {
		log.Fatalf("shard pool: %v", err)
	}
	defer pool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := pool.PingAll(pingCtx); err != nil {
		log.Printf("[main] ⚠️  shard ping warning: %v", err)
	}
	pingCancel()

	// ── Consistent hash ring ──────────────────────────────────────────────────
	shardIDs := make([]int, pool.Len())
	for i := range shardIDs {
		shardIDs[i] = i
	}
	r := ring.New(shardIDs, 0) // 0 → ring.DefaultVNodes (150)
	log.Printf("[main] consistent ring ready  shards=%d  vnodes=%d",
		pool.Len(), ring.DefaultVNodes)

	// ── Service options accumulator ───────────────────────────────────────────
	var svcOpts []reader.Option

	// ── Hot cache ─────────────────────────────────────────────────────────────
	//
	//  HOT_CACHE_ENABLED=true  → BadgerDB opened; S3 is skipped on pointer hits.
	//  HOT_CACHE_ENABLED=false → this entire block is skipped; zero overhead;
	//                            reader.Service.cache stays nil.
	//
	if !cfg.HotCacheEnabled {
		log.Printf("[main] hot cache OFF  (set HOT_CACHE_ENABLED=true to enable)")
	} else {
		hc, hcErr := hotcache.New(hotcache.Options{
			Dir:        cfg.HotCacheDir,
			MaxRecords: cfg.HotCacheMaxRecords,
			TTL:        cfg.HotCacheTTL,
			InMemSize:  cfg.InMemCacheSize,
		})
		if hcErr != nil {
			// Init failure is non-fatal — service starts without the cache.
			log.Printf("[main] ⚠️  hot cache init failed (continuing without cache): %v", hcErr)
		} else {
			svcOpts = append(svcOpts, reader.WithCache(hc))

			defer func() {
				if err := hc.Close(); err != nil {
					log.Printf("[main] hot cache close: %v", err)
				}
			}()

			// Background GC — runs every 5 minutes to reclaim space from
			// expired TTL entries and re-sync the in-memory record counter.
			go func() {
				ticker := time.NewTicker(5 * time.Minute)
				defer ticker.Stop()
				for range ticker.C {
					hc.RunGC()
				}
			}()

			log.Printf("[main] hot cache ON  dir=%s  max=%d  ttl=%s  l1_size=%d",
				cfg.HotCacheDir, cfg.HotCacheMaxRecords, cfg.HotCacheTTL, cfg.InMemCacheSize)
		}
	}

	// ── OpenTelemetry ─────────────────────────────────────────────────────────
	otelShutdown, metrics, otelErr := telemetry.Init(
		cfg.OTELEndpoint, cfg.OTELServiceName, cfg.OTELServiceVersion,
	)
	if otelErr != nil {
		log.Printf("[main] ⚠️  OTEL init failed (running without telemetry): %v", otelErr)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelShutdown(ctx); err != nil {
				log.Printf("[main] OTEL shutdown: %v", err)
			}
		}()
		svcOpts = append(svcOpts, reader.WithMetrics(metrics))
	}

	// ── Reader service ────────────────────────────────────────────────────────
	svcOpts = append(svcOpts,
		reader.WithMaxConcurrency(cfg.MaxConcurrentQueries),
		reader.WithS3BatchConcurrency(cfg.S3BatchConcurrency),
	)
	svc := reader.New(r, pool, s3Client, svcOpts...)

	// ── HTTP server ───────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           reader.NewHTTP(svc).Handler(),
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      60 * time.Second,  // must exceed longest query
		IdleTimeout:       120 * time.Second,  // keep connections alive for reuse
		MaxHeaderBytes:    1 << 20,            // 1 MB
	}
	go func() {
		log.Printf("[main] HTTP listening on :%s", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] HTTP server error: %v", err)
		}
	}()

	log.Printf("🚀  read-orchestrator started  shards=%d  bucket=%s  http=:%s  cache=%v",
		pool.Len(), cfg.S3Bucket, cfg.HTTPPort, cfg.HotCacheEnabled)

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("[main] shutting down…")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("[main] HTTP shutdown error: %v", err)
	}
	log.Printf("[main] stopped")
}