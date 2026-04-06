// Package hotcache is the embedded hot-data layer for the read-orchestrator.
//
// # Where it sits in the read path
//
//	RocksDB RangeQuery  →  hot cache lookup (BadgerDB)
//	                            │
//	                    hit ────┘   (return cached bytes, skip S3 entirely)
//	                    miss ───→  S3 GetRange  →  store in cache  →  return
//
// # Cache key / signature
//
// The key is the canonical S3 pointer: "bucket|objectKey|startByte|endByte".
// This string is produced by hotcache.Pointer() and must match exactly what
// parsePointer() in reader.go resolves from RocksDB metadata. A key match
// means the byte range we cached is byte-for-byte identical to what S3 would
// return — safe to skip the network call.
//
// # Capacity and eviction
//
// Up to MaxRecords entries are held (default 10 million). Each entry carries
// a per-key TTL (default 24 h) managed natively by BadgerDB. When the counter
// reaches MaxRecords new writes are silently dropped. Call RunGC periodically
// (e.g. every 5 min) to reclaim disk space and re-sync the counter.
//
// # Thread safety
//
// All exported methods are safe for concurrent use.
package hotcache

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// ─── defaults ────────────────────────────────────────────────────────────────

const (
	DefaultMaxRecords int64         = 10_000_000
	DefaultTTL        time.Duration = 24 * time.Hour
)

// ─── Cache ───────────────────────────────────────────────────────────────────

// Cache is a TTL-aware, capacity-capped hot-data store backed by BadgerDB
// with an in-memory LRU sitting in front for the hottest entries.
type Cache struct {
	db         *badger.DB
	l1         *InMemLRU    // in-memory L1; nil-safe (disabled when InMemSize=0)
	count      atomic.Int64 // live-record estimate; re-synced on each RunGC call
	maxRecords int64
	ttl        time.Duration
}

// Options configures a Cache.
type Options struct {
	// Dir is the directory where BadgerDB persists its files.
	// It is created automatically if it does not exist.
	Dir string

	// MaxRecords caps the number of cached entries.
	// Writes are silently dropped when the limit is reached.
	// <= 0 → DefaultMaxRecords (10 million).
	MaxRecords int64

	// TTL is how long each entry lives before BadgerDB expires it.
	// <= 0 → DefaultTTL (24 h).
	TTL time.Duration

	// MemTableSizeMB is the in-memory write-buffer (default 64 MB).
	MemTableSizeMB int64

	// ValueLogFileSizeMB caps each BadgerDB value-log segment (default 512 MB).
	ValueLogFileSizeMB int64

	// InMemSize is the number of entries to hold in the in-memory L1 LRU.
	// 0 disables the L1 layer (all reads go straight to BadgerDB).
	InMemSize int
}

// New opens (or creates) a BadgerDB store at opts.Dir and returns a Cache.
// Call Close() on shutdown.
func New(opts Options) (*Cache, error) {
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = DefaultMaxRecords
	}
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	memMB := opts.MemTableSizeMB
	if memMB <= 0 {
		memMB = 64
	}
	vlogMB := opts.ValueLogFileSizeMB
	if vlogMB <= 0 {
		vlogMB = 512
	}

	bopts := badger.DefaultOptions(opts.Dir).
		WithLogger(nil).
		WithMemTableSize(int64(memMB) << 20).
		WithValueLogFileSize(int64(vlogMB) << 20).
		WithCompression(1) // Snappy — event JSON compresses well

	db, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("hotcache: open %q: %w", opts.Dir, err)
	}

	c := &Cache{
		db:         db,
		l1:         NewInMemLRU(opts.InMemSize),
		maxRecords: opts.MaxRecords,
		ttl:        opts.TTL,
	}

	// Count pre-existing entries in the background so startup is non-blocking.
	go func() {
		n := c.countAll()
		c.count.Store(n)
		log.Printf("[hotcache] ready  dir=%s  existing=%d  max=%d  ttl=%s",
			opts.Dir, n, opts.MaxRecords, opts.TTL)
	}()

	return c, nil
}

// ─── Key builder ─────────────────────────────────────────────────────────────

// Pointer builds the canonical cache key from the four fields that
// parsePointer() in reader.go resolves from RocksDB metadata.
// Both sides MUST use this function to guarantee key consistency.
func Pointer(bucket, key string, startByte, endByte int64) string {
	return fmt.Sprintf("%s|%s|%d|%d", bucket, key, startByte, endByte)
}

// refKey builds a secondary index key that allows prefix-scanning all cached
// events for a given refID. Format: "r|{refID}|{pointer}".
func refKey(refID, pointer string) []byte {
	return []byte("r|" + refID + "|" + pointer)
}

// refPrefix returns the byte prefix for scanning all entries of a refID.
func refPrefix(refID string) []byte {
	return []byte("r|" + refID + "|")
}

// ─── Read / Write ─────────────────────────────────────────────────────────────

// Get looks up the raw event bytes stored under pointer.
//
//	(bytes, nil) → cache hit
//	(nil,   nil) → cache miss — not an error
//	(nil,   err) → real BadgerDB error
func (c *Cache) Get(pointer string) ([]byte, error) {
	// L1: in-memory LRU (sub-µs).
	if val, ok := c.l1.Get(pointer); ok {
		return val, nil
	}

	// L2: BadgerDB (disk).
	var val []byte
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pointer))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	switch err {
	case nil:
		c.l1.Set(pointer, val) // promote to L1
		return val, nil
	case badger.ErrKeyNotFound:
		return nil, nil // miss — not an error
	default:
		return nil, fmt.Errorf("hotcache: get %q: %w", pointer, err)
	}
}

// Set stores raw event bytes under pointer with the configured TTL.
// Writes are silently dropped (no error) when the cache is at capacity.
func (c *Cache) Set(pointer string, raw []byte) error {
	if c.count.Load() >= c.maxRecords {
		return nil // at capacity — drop gracefully
	}
	err := c.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry([]byte(pointer), raw).WithTTL(c.ttl))
	})
	if err != nil {
		return fmt.Errorf("hotcache: set %q: %w", pointer, err)
	}
	c.count.Add(1)
	c.l1.Set(pointer, raw)
	return nil
}

// SetWithRef stores raw bytes under both the pointer key and a ref-indexed
// key ("r|{refID}|{pointer}"). The ref index enables GetCachedForRef to
// prefix-scan all cached events for a refID without knowing the pointers
// up-front — this lets the cache scan run in parallel with RocksDB.
func (c *Cache) SetWithRef(refID, pointer string, raw []byte) error {
	if c.count.Load() >= c.maxRecords {
		return nil
	}
	err := c.db.Update(func(txn *badger.Txn) error {
		// Primary key: pointer → raw bytes.
		if err := txn.SetEntry(badger.NewEntry([]byte(pointer), raw).WithTTL(c.ttl)); err != nil {
			return err
		}
		// Secondary ref-index key: r|{refID}|{pointer} → raw bytes.
		return txn.SetEntry(badger.NewEntry(refKey(refID, pointer), raw).WithTTL(c.ttl))
	})
	if err != nil {
		return fmt.Errorf("hotcache: setWithRef %q ref=%s: %w", pointer, refID, err)
	}
	c.count.Add(2) // two keys written
	c.l1.Set(pointer, raw)
	return nil
}

// GetCachedForRef returns all cached event bytes for a refID by prefix-
// scanning the ref index. The returned map is keyed by the canonical pointer
// string. An empty (non-nil) map means "scan succeeded, nothing cached".
// This is designed to run concurrently with the RocksDB range query so the
// caller already knows what's cached by the time metadata arrives.
func (c *Cache) GetCachedForRef(refID string) (map[string][]byte, error) {
	prefix := refPrefix(refID)
	result := make(map[string][]byte)

	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			// Key format: "r|{refID}|{pointer}" — extract pointer.
			fullKey := item.Key()
			ptr := string(fullKey[len(prefix):])
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[ptr] = val
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hotcache: getCachedForRef %q: %w", refID, err)
	}
	return result, nil
}

// ─── Maintenance ─────────────────────────────────────────────────────────────

// RunGC reclaims space from value-log segments that are ≥ 50 % dead
// (expired TTL entries) and re-syncs the in-memory record counter.
// Call this on a regular ticker, e.g. every 5 minutes.
func (c *Cache) RunGC() {
	for {
		if err := c.db.RunValueLogGC(0.5); err != nil {
			break // badger.ErrNoRewrite means nothing left to collect
		}
	}
	live := c.countAll()
	c.count.Store(live)
	log.Printf("[hotcache] GC complete  live=%d  max=%d", live, c.maxRecords)
}

// Stats returns a snapshot of cache counters (useful for health/metrics).
func (c *Cache) Stats() Stats {
	return Stats{
		LiveEntries: c.count.Load(),
		MaxEntries:  c.maxRecords,
	}
}

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	LiveEntries int64
	MaxEntries  int64
}

// Close flushes pending writes and closes the underlying BadgerDB cleanly.
// Always call this during graceful shutdown.
func (c *Cache) Close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("hotcache: close: %w", err)
	}
	return nil
}

// ─── internal ────────────────────────────────────────────────────────────────

// countAll does a key-only scan and returns the number of live (non-expired)
// entries. O(N) — only called from background goroutines.
func (c *Cache) countAll() int64 {
	var n int64
	_ = c.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	return n
}