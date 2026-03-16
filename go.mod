module read-orchestrator

go 1.23

require (
	github.com/aws/aws-sdk-go-v2 v1.26.1
	github.com/aws/aws-sdk-go-v2/config v1.27.11
	github.com/aws/aws-sdk-go-v2/credentials v1.17.11
	github.com/aws/aws-sdk-go-v2/service/s3 v1.53.1
	github.com/cespare/xxhash/v2 v2.3.0

	go.opentelemetry.io/otel v1.20.0
	go.opentelemetry.io/otel/metric v1.20.0
	go.opentelemetry.io/otel/trace v1.20.0
)