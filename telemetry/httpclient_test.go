package telemetry

import (
	"net/http"
	"testing"
	"time"
)

func TestInstrumentHTTPClientKeepsSettings(t *testing.T) {
	t.Parallel()

	base := &http.Client{Timeout: 42 * time.Second}
	got := InstrumentHTTPClient(base)

	if got == base {
		t.Fatal("InstrumentHTTPClient mutated the client it was given; it must return a copy")
	}
	if got.Timeout != base.Timeout {
		t.Errorf("timeout = %v, want %v", got.Timeout, base.Timeout)
	}
	if base.Transport != nil {
		t.Error("the original client's transport was replaced")
	}
}

// A nil client means http.DefaultClient — which must be copied, not
// instrumented in place: doing that from a library constructor would change
// the behaviour of every unrelated caller in the process.
func TestInstrumentHTTPClientLeavesDefaultAlone(t *testing.T) {
	t.Parallel()

	got := InstrumentHTTPClient(nil)

	if got == http.DefaultClient {
		t.Fatal("InstrumentHTTPClient(nil) returned http.DefaultClient itself")
	}
	if http.DefaultClient.Transport != nil {
		t.Error("http.DefaultClient was instrumented in place")
	}
	if got.Transport == nil {
		t.Error("the returned client is not instrumented")
	}
}

// Adapters wrap at construction and callers may wrap again; a double wrap
// would report every request as two nested client spans.
func TestInstrumentTransportDoesNotDoubleWrap(t *testing.T) {
	t.Parallel()

	once := InstrumentTransport(nil)
	twice := InstrumentTransport(once)

	if twice != once {
		t.Error("InstrumentTransport wrapped an already-instrumented transport")
	}
}
