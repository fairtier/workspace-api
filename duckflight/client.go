// Package duckflight is a thin FlightSQL client for the per-customer
// DuckFlight query engine. It exists so the QueryService handler can execute
// SQL over gRPC without touching Arrow types directly (and so tests can mock
// the whole engine behind [Executor]).
package duckflight

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Column describes one result column.
type Column struct {
	Name string
	Type string
}

// Result is a fully materialized, row-capped query result. Row values are
// JSON-marshalable Go values as produced by rowValue.
type Result struct {
	Columns   []Column
	Rows      [][]any
	Truncated bool
}

// Executor runs a SQL statement against a DuckFlight endpoint. Implemented by
// [Client]; mocked in handler tests.
type Executor interface {
	Execute(ctx context.Context, endpoint, token, sql string, maxRows int) (*Result, error)
}

// Client is the real FlightSQL-backed Executor. It dials per request: the
// TLS+HTTP/2 handshake is negligible next to interactive query latency, and a
// fresh dial can never hold a stale endpoint/token after a box is
// re-provisioned. If dial latency ever matters, replace with a per-customer
// client pool evicted on token change.
type Client struct{}

// NewClient returns a per-request-dialing Executor.
func NewClient() *Client { return &Client{} }

// bearerCreds injects the DuckFlight static bearer token into every RPC
// (DuckFlight validates `authorization: Bearer <token>` per call; the Flight
// Handshake flow is only for its basic-auth backend).
type bearerCreds struct{ token string }

func (b bearerCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (bearerCreds) RequireTransportSecurity() bool { return true }

// Execute runs sql and reads at most maxRows rows, setting Result.Truncated
// when more were available.
func (c *Client) Execute(ctx context.Context, endpoint, token, sql string, maxRows int) (*Result, error) {
	fc, err := flightsql.NewClientCtx(ctx, normalizeAddr(endpoint), nil, nil,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
		grpc.WithPerRPCCredentials(bearerCreds{token: token}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial duckflight: %w", err)
	}
	defer func() { _ = fc.Close() }()

	info, err := fc.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}

	res := &Result{}
	for _, ep := range info.Endpoint {
		if err := c.readEndpoint(ctx, fc, ep.Ticket, maxRows, res); err != nil {
			return nil, err
		}
		if res.Truncated {
			return res, nil
		}
	}
	return res, nil
}

// readEndpoint drains one Flight endpoint into res, populating columns on the
// first endpoint and stopping early (setting res.Truncated) once maxRows rows
// have been collected across all endpoints.
func (c *Client) readEndpoint(ctx context.Context, fc *flightsql.Client, ticket *flight.Ticket, maxRows int, res *Result) error {
	rdr, err := fc.DoGet(ctx, ticket)
	if err != nil {
		return err
	}
	defer rdr.Release()

	if res.Columns == nil {
		for _, f := range rdr.Schema().Fields() {
			res.Columns = append(res.Columns, Column{Name: f.Name, Type: f.Type.String()})
		}
	}
	for rdr.Next() {
		if appendRecord(rdr.RecordBatch(), maxRows, res) {
			res.Truncated = true
			return nil
		}
	}
	return rdr.Err()
}

// appendRecord appends rec's rows to res, returning true if maxRows was hit
// (leaving the remaining rows unread).
func appendRecord(rec arrow.RecordBatch, maxRows int, res *Result) bool {
	for row := 0; row < int(rec.NumRows()); row++ {
		if len(res.Rows) >= maxRows {
			return true
		}
		vals := make([]any, rec.NumCols())
		for col := 0; col < int(rec.NumCols()); col++ {
			vals[col] = rowValue(rec.Column(col), row)
		}
		res.Rows = append(res.Rows, vals)
	}
	return false
}

// normalizeAddr turns the stored DuckFlight URL into a gRPC dial target.
// Provisioning emits two shapes: `grpc://host:443` (shared substrate) and
// `https://host` (dedicated box) — both are TLS at the edge (Traefik/Envoy
// terminates, backend is h2c).
func normalizeAddr(endpoint string) string {
	addr := endpoint
	for _, p := range []string{"grpc://", "grpc+tls://", "https://", "http://"} {
		if rest, ok := strings.CutPrefix(addr, p); ok {
			addr = rest
			break
		}
	}
	addr = strings.TrimSuffix(strings.TrimSpace(addr), "/")
	if !strings.Contains(addr, ":") {
		addr += ":443"
	}
	return addr
}
