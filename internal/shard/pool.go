// Package shard manages gRPC connections to the RocksDB nodes.
package shard

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "read-orchestrator/proto/rocksdb"
)

// Pool holds one gRPC connection per shard, keyed by shard index.
type Pool struct {
	mu      sync.RWMutex
	clients map[int]pb.RocksDBServiceClient
	conns   map[int]*grpc.ClientConn
}

// New dials all shard addresses and returns a ready Pool.
// addrs is indexed by shard ID (0, 1, 2 …) — same order as SHARD_URLS.
func New(addrs []string) (*Pool, error) {
	p := &Pool{
		clients: make(map[int]pb.RocksDBServiceClient, len(addrs)),
		conns:   make(map[int]*grpc.ClientConn, len(addrs)),
	}
	for id, addr := range addrs {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16*1024*1024)),
			grpc.WithInitialWindowSize(1<<20),     // 1 MB stream window
			grpc.WithInitialConnWindowSize(1<<20), // 1 MB connection window
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                20 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("shard %d (%s): dial: %w", id, addr, err)
		}
		p.conns[id] = conn
		p.clients[id] = pb.NewRocksDBServiceClient(conn)
		log.Printf("[shard] registered shard %d at %s", id, addr)
	}
	return p, nil
}

// Close releases all connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, c := range p.conns {
		if err := c.Close(); err != nil {
			log.Printf("[shard] close shard %d: %v", id, err)
		}
	}
}

// Len returns the number of shards.
func (p *Pool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// PingAll pings every shard and returns a combined error for any failures.
func (p *Pool) PingAll(ctx context.Context) error {
	p.mu.RLock()
	ids := make([]int, 0, len(p.clients))
	for id := range p.clients {
		ids = append(ids, id)
	}
	p.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(ids))
	for _, id := range ids {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			c, err := p.client(shardID)
			if err != nil {
				errCh <- err
				return
			}
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if _, err := c.Ping(pingCtx, &pb.PingRequest{}); err != nil {
				errCh <- fmt.Errorf("shard %d: ping: %w", shardID, err)
			} else {
				log.Printf("[shard] ✓ shard %d healthy", shardID)
			}
		}(id)
	}
	wg.Wait()
	close(errCh)

	var errs []string
	for e := range errCh {
		errs = append(errs, e.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("unhealthy shards: %v", errs)
	}
	return nil
}

// RangeQuery fetches all index entries for refID from shardID, optionally
// filtered by startTime / endTime (RFC3339 strings, pass "" for no filter).
func (p *Pool) RangeQuery(ctx context.Context, shardID int, refID, startTime, endTime string) ([]*pb.Metadata, error) {
	c, err := p.client(shardID)
	if err != nil {
		return nil, err
	}
	resp, err := c.RangeQuery(ctx, &pb.RangeQueryRequest{
		RefId:     refID,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		return nil, fmt.Errorf("shard %d range_query ref=%s: %w", shardID, refID, err)
	}
	return resp.Results, nil
}

func (p *Pool) client(shardID int) (pb.RocksDBServiceClient, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.clients[shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not in pool", shardID)
	}
	return c, nil
}