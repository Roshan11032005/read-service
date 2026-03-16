// Package s3store wraps the AWS S3 SDK for the read-orchestrator.
package s3store

import (
	"context"
	"fmt"
	"io"

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

// New creates a Client from config.
func New(cfg *config.Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
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
func (c *Client) GetRange(ctx context.Context, key string, startByte, endByte int64) ([]byte, error) {
	if endByte <= startByte {
		return nil, fmt.Errorf("s3store: invalid range [%d,%d) for key %q", startByte, endByte, key)
	}
	rangeHdr := fmt.Sprintf("bytes=%d-%d", startByte, endByte-1)

	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHdr),
	})
	if err != nil {
		return nil, fmt.Errorf("s3store: get range %s %q: %w", rangeHdr, key, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}