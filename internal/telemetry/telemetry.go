// Package telemetry initialises OpenTelemetry metrics + traces and exposes
// the metric instruments used throughout the read-orchestrator.
package telemetry

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// Metrics holds every OTEL metric instrument the service records.
type Metrics struct {
	QueriesTotal         metric.Int64Counter
	QueriesSuccess       metric.Int64Counter
	QueriesFailure       metric.Int64Counter
	RefIDsQueried        metric.Int64Counter
	RocksDBQueriesTotal  metric.Int64Counter
	RocksDBQueriesFailed metric.Int64Counter
	S3FetchesTotal       metric.Int64Counter
	S3FetchesFailed      metric.Int64Counter
	EventsReturned       metric.Int64Counter
	BytesRead            metric.Int64Counter

	QueryDuration          metric.Float64Histogram
	RocksDBQueryDuration   metric.Float64Histogram
	S3FetchDuration        metric.Float64Histogram
	EventParseDuration     metric.Float64Histogram
	MetadataLookupDuration metric.Float64Histogram
}

// Init sets up trace and metric providers, registers all instruments, and
// returns a shutdown function that should be deferred in main.
func Init(endpoint, serviceName, serviceVersion string) (shutdown func(context.Context) error, m *Metrics, err error) {
	ctx := context.Background()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otel resource: %w", err)
	}

	// ── Traces ──────────────────────────────────────────────────────────────
	traceExp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otel trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	// ── Metrics ─────────────────────────────────────────────────────────────
	metricExp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otel metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(10*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	meter := mp.Meter(serviceName)
	m = &Metrics{}

	must := func(name string, fn func() error) {
		if e := fn(); e != nil {
			log.Printf("[telemetry] warning: %s: %v", name, e)
		}
	}

	must("queries.total", func() error { m.QueriesTotal, err = meter.Int64Counter("queries.total"); return err })
	must("queries.success", func() error { m.QueriesSuccess, err = meter.Int64Counter("queries.success"); return err })
	must("queries.failure", func() error { m.QueriesFailure, err = meter.Int64Counter("queries.failure"); return err })
	must("queries.ref_ids.total", func() error { m.RefIDsQueried, err = meter.Int64Counter("queries.ref_ids.total"); return err })
	must("rocksdb.queries.total", func() error { m.RocksDBQueriesTotal, err = meter.Int64Counter("rocksdb.queries.total"); return err })
	must("rocksdb.queries.failed", func() error { m.RocksDBQueriesFailed, err = meter.Int64Counter("rocksdb.queries.failed"); return err })
	must("s3.fetches.total", func() error { m.S3FetchesTotal, err = meter.Int64Counter("s3.fetches.total"); return err })
	must("s3.fetches.failed", func() error { m.S3FetchesFailed, err = meter.Int64Counter("s3.fetches.failed"); return err })
	must("events.returned.total", func() error { m.EventsReturned, err = meter.Int64Counter("events.returned.total"); return err })
	must("bytes.read.total", func() error { m.BytesRead, err = meter.Int64Counter("bytes.read.total"); return err })

	must("query.duration", func() error {
		m.QueryDuration, err = meter.Float64Histogram("query.duration", metric.WithUnit("s"))
		return err
	})
	must("rocksdb.query.duration", func() error {
		m.RocksDBQueryDuration, err = meter.Float64Histogram("rocksdb.query.duration", metric.WithUnit("s"))
		return err
	})
	must("s3.fetch.duration", func() error {
		m.S3FetchDuration, err = meter.Float64Histogram("s3.fetch.duration", metric.WithUnit("s"))
		return err
	})
	must("event.parse.duration", func() error {
		m.EventParseDuration, err = meter.Float64Histogram("event.parse.duration", metric.WithUnit("s"))
		return err
	})
	must("metadata.lookup.duration", func() error {
		m.MetadataLookupDuration, err = meter.Float64Histogram("metadata.lookup.duration", metric.WithUnit("s"))
		return err
	})

	shutdown = func(ctx context.Context) error {
		if e := tp.Shutdown(ctx); e != nil {
			return fmt.Errorf("tracer shutdown: %w", e)
		}
		if e := mp.Shutdown(ctx); e != nil {
			return fmt.Errorf("meter shutdown: %w", e)
		}
		return nil
	}

	log.Printf("[telemetry] OTEL initialised — endpoint=%s service=%s", endpoint, serviceName)
	return shutdown, m, nil
}
