package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// SetupMetrics installs a global meter provider exporting over OTLP gRPC on
// the same OTEL_* configuration as SetupTracing, plus the Go runtime metrics
// (heap, goroutines, GC pauses) — on a single-VM box those are the first
// numbers an operator needs when the API turns slow, and nothing else on the
// machine is collecting them.
//
// Installing it is what turns on the instrumentation that the libraries
// already in the request path report for free: otelconnect's RPC duration and
// message-size histograms, and otelhttp's server and client metrics.
//
// With no endpoint configured the provider stays the SDK no-op, exactly as
// tracing does. The returned shutdown flushes the last collection interval.
func SetupMetrics(ctx context.Context, serviceName, serviceVersion string) (func(context.Context) error, error) {
	if !exporterConfigured("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
		return noopShutdown, nil
	}

	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("otlp metric exporter: %w", err)
	}

	res, err := newResource(serviceName, serviceVersion)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(longRunningDurationView()),
	)
	otel.SetMeterProvider(mp)

	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		_ = mp.Shutdown(ctx)
		return nil, fmt.Errorf("start runtime metrics: %w", err)
	}

	return mp.Shutdown, nil
}

// longRunningDurationView rebuckets the run-duration histograms. A data load
// or a dbt build is measured in minutes; the SDK's default boundaries stop at
// 10s, which would collapse every real run into the overflow bucket and make
// the percentiles useless. The boundaries below run from "the worker failed
// immediately" to "this has been going for an hour".
func longRunningDurationView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: "workspace.*.run.duration"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
		}},
	)
}
