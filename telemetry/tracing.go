package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SetupTracing installs a global OpenTelemetry tracer provider that batches
// spans to the OTLP gRPC endpoint defined by the standard OTEL_* env vars
// (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE, ...).
//
// When OTEL_EXPORTER_OTLP_ENDPOINT is empty the tracer provider is left as
// the SDK default (no-op), so `go run` outside the cluster doesn't fail.
//
// The returned shutdown must be deferred by the caller so in-flight spans
// are flushed before exit.
func SetupTracing(ctx context.Context, serviceName, serviceVersion string) (func(context.Context) error, error) {
	if !exporterConfigured("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
		return noopShutdown, nil
	}

	exporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient())
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}

	res, err := newResource(serviceName, serviceVersion)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
