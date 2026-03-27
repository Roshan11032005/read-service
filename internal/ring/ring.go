// Package ring implements a consistent hash ring for shard routing.
// Parameters must be identical to the index-builder so the same ref_id always
// maps to the same shard on both read and write paths.
package ring

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// DefaultVNodes is the number of virtual nodes per shard.
// Must match the index-builder value.
const DefaultVNodes = 150

type point struct {
	hash    uint64
	shardID int
}

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	points []point
}

// New builds a ring for the given shard IDs.
// Pass vnodes=0 to use DefaultVNodes.
func New(shardIDs []int, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	r := &Ring{vnodes: vnodes}
	for _, id := range shardIDs {
		r.insert(id)
	}
	r.sort()
	return r
}

// Get returns the shard ID responsible for key.
func (r *Ring) Get(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		panic("ring: Get called on empty ring")
	}
	h := xxhash.Sum64String(key)
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	if idx == len(r.points) {
		idx = 0
	}
	return r.points[idx].shardID
}

func (r *Ring) insert(shardID int) {
	for i := 0; i < r.vnodes; i++ {
		vkey := fmt.Sprintf("%d#%d", shardID, i)
		r.points = append(r.points, point{
			hash:    xxhash.Sum64String(vkey),
			shardID: shardID,
		})
	}
}

func (r *Ring) sort() {
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
}