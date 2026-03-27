// Package s3store wraps the AWS S3 SDK for the read-orchestrator.
package s3store

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"read-orchestrator/config"
)

// Client is an S3 client bound to one bucket.
type Client struct {
	bucket string
	s3     *s3.Client
}

// New creates a Client from config with a connection-pooled HTTP transport
// tuned for high-throughput S3 access.
func New(cfg *config.Config) (*Client, error) {
	// Custom transport: the default MaxIdleConnsPerHost=2 is a severe
	// bottleneck — 30 of 32 concurrent fetches would create new TCP
	// connections on every batch. This transport keeps connections warm.
	maxConns := cfg.S3MaxConns
	if maxConns <= 0 {
		maxConns = 256
	}
	transport := &http.Transport{
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		MaxConnsPerHost:     maxConns,
		IdleConnTimeout:     120 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		DisableCompression:  true, // byte-range fetches are already exactly sized
		WriteBufferSize:     64 * 1024,
		ReadBufferSize:      64 * 1024,
	}
	httpClient := &http.Client{Transport: transport}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
		awsconfig.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("s3store: load config: %w", err)
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		//nolint:staticcheck
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(_ string, _ s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.S3Endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.S3Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	return &Client{bucket: cfg.S3Bucket, s3: cli}, nil
}

// GetRange fetches bytes [startByte, endByte) from the given object key.
// The builder stores endByte as the exclusive end (startByte + lineLen), so
// we request bytes=startByte-(endByte-1) to get exactly one NDJSON line.
// If bucket is empty, the configured default bucket is used.
func (c *Client) GetRange(ctx context.Context, bucket, key string, startByte, endByte int64) ([]byte, error) {
	if endByte <= startByte {
		return nil, fmt.Errorf("s3store: invalid range [%d,%d) for key %q", startByte, endByte, key)
	}
	if bucket == "" {
		bucket = c.bucket
	}
	rangeHdr := fmt.Sprintf("bytes=%d-%d", startByte, endByte-1)

	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHdr),
	})
	if err != nil {
		return nil, fmt.Errorf("s3store: get range %s %q (bucket=%s): %w", rangeHdr, key, bucket, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ─── Batch fetching ──────────────────────────────────────────────────────────

// RangeRequest describes a single byte-range to fetch from S3.
type RangeRequest struct {
	Bucket    string
	Key       string
	StartByte int64
	EndByte   int64 // exclusive
}

// RangeResult holds the fetched bytes (or error) for one RangeRequest.
type RangeResult struct {
	Data []byte
	Err  error
}

// BatchStats reports how efficient the batch was.
type BatchStats struct {
	S3Calls   int           // actual GetObject calls made (after merging)
	TotalTime time.Duration // wall-clock time for the entire batch
}

// GetRangeBatch fetches multiple byte ranges efficiently by merging requests
// that target the same S3 object into fewer, larger GetObject calls.
//
// Ranges within the same (bucket, key) that are within mergeGap bytes of each
// other are coalesced into a single fetch. The returned []RangeResult is
// positionally aligned with reqs. Concurrency limits the number of in-flight
// S3 calls.
func (c *Client) GetRangeBatch(ctx context.Context, reqs []RangeRequest, concurrency int) ([]RangeResult, BatchStats) {
	results := make([]RangeResult, len(reqs))
	if len(reqs) == 0 {
		return results, BatchStats{}
	}
	if concurrency <= 0 {
		concurrency = 32
	}

	start := time.Now()

	// ── Group by (bucket, key) ────────────────────────────────────────────
	type objKey struct{ bucket, key string }
	type indexedRange struct {
		idx        int // position in the original reqs slice
		start, end int64
	}
	groups := make(map[objKey][]indexedRange)
	for i, r := range reqs {
		b := r.Bucket
		if b == "" {
			b = c.bucket
		}
		k := objKey{b, r.Key}
		groups[k] = append(groups[k], indexedRange{idx: i, start: r.StartByte, end: r.EndByte})
	}

	// ── Build merged fetch jobs ───────────────────────────────────────────
	const mergeGap int64 = 32 * 1024 // merge ranges ≤32 KB apart

	type fetchJob struct {
		bucket, key string
		start, end  int64
		members     []indexedRange
	}
	var jobs []fetchJob

	for obj, ranges := range groups {
		sort.Slice(ranges, func(i, j int) bool {
			return ranges[i].start < ranges[j].start
		})
		cur := fetchJob{
			bucket:  obj.bucket,
			key:     obj.key,
			start:   ranges[0].start,
			end:     ranges[0].end,
			members: []indexedRange{ranges[0]},
		}
		for _, r := range ranges[1:] {
			if r.start <= cur.end+mergeGap {
				if r.end > cur.end {
					cur.end = r.end
				}
				cur.members = append(cur.members, r)
			} else {
				jobs = append(jobs, cur)
				cur = fetchJob{
					bucket:  obj.bucket,
					key:     obj.key,
					start:   r.start,
					end:     r.end,
					members: []indexedRange{r},
				}
			}
		}
		jobs = append(jobs, cur)
	}

	// ── Execute fetch jobs concurrently ───────────────────────────────────
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(j fetchJob) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			raw, err := c.GetRange(ctx, j.bucket, j.key, j.start, j.end)
			if err != nil {
				for _, m := range j.members {
					results[m.idx] = RangeResult{Err: err}
				}
				return
			}

			for _, m := range j.members {
				offset := m.start - j.start
				length := m.end - m.start
				if offset+length > int64(len(raw)) {
					results[m.idx] = RangeResult{
						Err: fmt.Errorf("s3store: merged fetch too short for [%d,%d) in key %q", m.start, m.end, j.key),
					}
					continue
				}
				sub := make([]byte, length)
				copy(sub, raw[offset:offset+length])
				results[m.idx] = RangeResult{Data: sub}
			}
		}(job)
	}
	wg.Wait()

	return results, BatchStats{
		S3Calls:   len(jobs),
		TotalTime: time.Since(start),
	}
}