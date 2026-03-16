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
	"read-orchestrator/internal/reader"
	"read-orchestrator/internal/ring"
	"read-orchestrator/internal/s3store"
	"read-orchestrator/internal/shard"
)

func main() {
	// ── Config ─────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// ── S3 client ──────────────────────────────────────────────────────────────
	s3Client, err := s3store.New(cfg)
	if err != nil {
		log.Fatalf("s3store: %v", err)
	}
	log.Printf("[main] S3 ready: bucket=%s endpoint=%s", cfg.S3Bucket, cfg.S3Endpoint)

	// ── Shard pool ─────────────────────────────────────────────────────────────
	pool, err := shard.New(cfg.ShardURLs)
	if err != nil {
		log.Fatalf("shard pool: %v", err)
	}
	defer pool.Close()

	// Connectivity check — non-fatal so a single unhealthy shard doesn't
	// prevent the service from starting and serving the healthy ones.
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := pool.PingAll(pingCtx); err != nil {
		log.Printf("[main] ⚠️  shard ping warning: %v", err)
	}
	pingCancel()

	// ── Consistent ring ────────────────────────────────────────────────────────
	// Build with the same parameters as the index-builder so "ref:{refID}"
	// always resolves to the same shard on both services.
	shardIDs := make([]int, pool.Len())
	for i := range shardIDs {
		shardIDs[i] = i
	}
	r := ring.New(shardIDs, 0) // 0 → ring.DefaultVNodes (150)
	log.Printf("[main] consistent ring ready: %d shard(s), %d vnodes each",
		pool.Len(), ring.DefaultVNodes)

	// ── Reader service ─────────────────────────────────────────────────────────
	svc := reader.New(r, pool, s3Client)

	// ── HTTP server ────────────────────────────────────────────────────────────
	httpHandler := reader.NewHTTP(svc)
	srv := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      httpHandler.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[main] HTTP server listening on :%s", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] HTTP server: %v", err)
		}
	}()

	log.Printf("🚀  read-orchestrator started")
	log.Printf("    shards=%d  bucket=%s  http=:%s",
		pool.Len(), cfg.S3Bucket, cfg.HTTPPort)

	// ── Graceful shutdown ──────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Printf("[main] shutting down…")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("[main] HTTP shutdown: %v", err)
	}
	log.Printf("[main] stopped")
}