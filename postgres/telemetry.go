package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// ObserveDBStats publishes the connection pool's state as OTel gauges,
// collected on demand from sql.DB.Stats.
//
// This is the cheapest useful database signal there is, and on a box it is the
// one that explains the failure mode nothing else shows: the pool is a fixed
// resource shared by the Console's requests, the worker's poll, and the
// background sweeps, so when it saturates every one of them slows down at once
// and no single handler looks guilty. `wait_count` rising is that story.
//
// Registration is idempotent from the caller's side only in the sense that it
// should happen once per *sql.DB, at startup; calling it twice would double
// every measurement. It is a no-op when no meter provider is installed.
func ObserveDBStats(db *sql.DB) error {
	meter := otel.Meter("github.com/fairtier/workspace-api/postgres")

	connections, err := meter.Int64ObservableUpDownCounter("db.client.connection.count",
		metric.WithDescription("Connections in the pool, by state."),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("db connection gauge: %w", err)
	}
	waits, err := meter.Int64ObservableCounter("db.client.connection.wait.count",
		metric.WithDescription("Times a caller had to wait for a free connection."),
		metric.WithUnit("{request}"))
	if err != nil {
		return fmt.Errorf("db wait counter: %w", err)
	}
	waitTime, err := meter.Float64ObservableCounter("db.client.connection.wait_time",
		metric.WithDescription("Total time callers spent waiting for a free connection."),
		metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("db wait time counter: %w", err)
	}
	maxOpen, err := meter.Int64ObservableUpDownCounter("db.client.connection.max",
		metric.WithDescription("Configured maximum pool size; 0 means unlimited."),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("db max gauge: %w", err)
	}

	used := metric.WithAttributes(semconv.DBClientConnectionStateUsed)
	idle := metric.WithAttributes(semconv.DBClientConnectionStateIdle)

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := db.Stats()
		o.ObserveInt64(connections, int64(s.InUse), used)
		o.ObserveInt64(connections, int64(s.Idle), idle)
		o.ObserveInt64(waits, s.WaitCount)
		o.ObserveFloat64(waitTime, s.WaitDuration.Seconds())
		o.ObserveInt64(maxOpen, int64(s.MaxOpenConnections))
		return nil
	}, connections, waits, waitTime, maxOpen)
	if err != nil {
		return fmt.Errorf("register db stats callback: %w", err)
	}
	return nil
}
