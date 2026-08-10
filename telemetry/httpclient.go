package telemetry

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// InstrumentHTTPClient returns c with its transport wrapped so every request
// it sends becomes a client span, carries the trace context to the callee, and
// feeds the http.client.* metrics.
//
// Adapters call this on the client they build rather than being handed an
// instrumented one, so an outbound call is traced no matter which constructor
// produced the client — including the zero-value defaults the tests use.
// A nil client means "http.DefaultClient", which is copied rather than mutated:
// instrumenting the process-wide default from a library constructor would be a
// side effect no caller asked for.
func InstrumentHTTPClient(c *http.Client) *http.Client {
	if c == nil {
		c = http.DefaultClient
	}
	instrumented := *c
	instrumented.Transport = InstrumentTransport(c.Transport)
	return &instrumented
}

// InstrumentTransport wraps a round tripper (nil = http.DefaultTransport) in
// the OTel one. Wrapping twice would double every span, so an already-wrapped
// transport is returned unchanged.
func InstrumentTransport(rt http.RoundTripper) http.RoundTripper {
	if _, already := rt.(*otelhttp.Transport); already {
		return rt
	}
	return otelhttp.NewTransport(rt)
}
