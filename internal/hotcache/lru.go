package hotcache

import (
	"container/list"
	"sync"
)

// ─── Sharded in-memory LRU ──────────────────────────────────────────────────
//
// This sits in front of BadgerDB as an L1 cache. At 1000+ QPS each BadgerDB
// Get does disk I/O (even with OS page cache); this LRU holds the hottest
// pointers entirely in RAM with sub-microsecond lookups.
//
// The LRU is split into 64 shards to minimise lock contention. Each shard
// holds up to (maxSize / 64) entries. Total memory is bounded by
// maxSize × average-event-size (typically ~500 B → ~50 MB for 100k entries).

const lruShards = 64

// InMemLRU is a sharded, thread-safe, bounded in-memory LRU cache.
type InMemLRU struct {
	shards  [lruShards]lruShard
	maxPer  int // max entries per shard
	enabled bool
}

type lruEntry struct {
	key string
	val []byte
}

type lruShard struct {
	mu    sync.Mutex
	items map[string]*list.Element
	order *list.List
	max   int
}

// NewInMemLRU creates an LRU with the given total capacity.
// Pass 0 to disable (Get always misses, Set is a no-op).
func NewInMemLRU(maxSize int) *InMemLRU {
	if maxSize <= 0 {
		return &InMemLRU{enabled: false}
	}
	perShard := maxSize / lruShards
	if perShard < 1 {
		perShard = 1
	}
	l := &InMemLRU{maxPer: perShard, enabled: true}
	for i := range l.shards {
		l.shards[i] = lruShard{
			items: make(map[string]*list.Element, perShard),
			order: list.New(),
			max:   perShard,
		}
	}
	return l
}

// Get returns the cached value and true on hit, or nil/false on miss.
func (l *InMemLRU) Get(key string) ([]byte, bool) {
	if !l.enabled {
		return nil, false
	}
	s := &l.shards[shardIdx(key)]
	s.mu.Lock()
	elem, ok := s.items[key]
	if ok {
		s.order.MoveToFront(elem)
		val := elem.Value.(*lruEntry).val
		s.mu.Unlock()
		return val, true
	}
	s.mu.Unlock()
	return nil, false
}

// Set inserts or updates a key. Evicts the LRU entry if at capacity.
func (l *InMemLRU) Set(key string, val []byte) {
	if !l.enabled {
		return
	}
	s := &l.shards[shardIdx(key)]
	s.mu.Lock()
	if elem, ok := s.items[key]; ok {
		s.order.MoveToFront(elem)
		elem.Value.(*lruEntry).val = val
		s.mu.Unlock()
		return
	}
	elem := s.order.PushFront(&lruEntry{key: key, val: val})
	s.items[key] = elem
	if s.order.Len() > s.max {
		back := s.order.Back()
		s.order.Remove(back)
		delete(s.items, back.Value.(*lruEntry).key)
	}
	s.mu.Unlock()
}

func shardIdx(key string) uint64 {
	// FNV-1a inspired fast hash; only used for shard selection.
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return h & (lruShards - 1)
}
