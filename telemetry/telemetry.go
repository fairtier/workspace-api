// Package telemetry wires this service's OpenTelemetry providers: traces to an
// OTLP collector, metrics to the same, and the small helpers adapters use to
// instrument their outbound calls.
//
// Everything here is opt-in on the standard OTEL_* environment: with no
// endpoint configured the global providers are left as the SDK's no-ops, so a
// self-hoster who runs the box without a collector pays nothing and sees no
// errors. That is deliberate — observability must never be a precondition for
// the workspace plane to run.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// Setup installs both the tracer and the meter provider and returns a single
// shutdown that flushes them, in-flight spans first so a metric export cannot
// eat the shutdown budget before the trace one runs.
func Setup(ctx context.Context, serviceName, serviceVersion string) (func(context.Context) error, error) {
	shutdownTracing, err := SetupTracing(ctx, serviceName, serviceVersion)
	if err != nil {
		return nil, err
	}
	shutdownMetrics, err := SetupMetrics(ctx, serviceName, serviceVersion)
	if err != nil {
		_ = shutdownTracing(ctx)
		return nil, err
	}
	return func(ctx context.Context) error {
		return errors.Join(shutdownTracing(ctx), shutdownMetrics(ctx))
	}, nil
}

// noopShutdown is what every Setup* returns when the signal is not configured.
func noopShutdown(context.Context) error { return nil }

// exporterConfigured reports whether an OTLP endpoint is set for a signal,
// either signal-specific or through the shared OTEL_EXPORTER_OTLP_ENDPOINT.
func exporterConfigured(signalEnv string) bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv(signalEnv) != ""
}

// newResource describes this process to the collector. resource.Default()
// contributes the SDK attributes and honours OTEL_RESOURCE_ATTRIBUTES, which
// is how a deployment adds its own dimensions (deployment.environment, the
// box's host) without this module knowing about them.
//
// NewSchemaless, NOT NewWithAttributes(semconv.SchemaURL, …). resource.Merge
// refuses to merge two resources with different schema URLs, and
// resource.Default() carries whatever semconv version the SDK was built
// against — so pinning a schema here silently couples this function to the SDK
// minor version and breaks the process at startup the first time they diverge.
// They did: an SDK bump to 1.45 moved the default to semconv 1.43 while this
// import said 1.41, and every consumer with an OTLP endpoint configured died
// with `conflicting Schema URL`. Unit tests do not catch it — Setup* is a no-op
// when no endpoint is set, so the merge never runs outside a deployed process.
//
// The two attributes below are service.name and service.version, stable across
// every semconv version anyone will use, so the schema URL adds nothing here
// worth that coupling.
func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}
	return res, nil
}
