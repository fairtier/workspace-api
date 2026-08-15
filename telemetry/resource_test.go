package telemetry

import "testing"

// Regression: resource.Merge fails on conflicting schema URLs, which only
// surfaced in a deployed process because Setup* short-circuits without an
// endpoint. This calls the merge directly so an SDK bump cannot break startup
// again without turning a test red.
func TestNewResourceMergesAgainstSDKDefault(t *testing.T) {
	res, err := newResource("svc", "1.2.3")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	var name, version bool
	for _, a := range res.Attributes() {
		switch string(a.Key) {
		case "service.name":
			name = a.Value.AsString() == "svc"
		case "service.version":
			version = a.Value.AsString() == "1.2.3"
		}
	}
	if !name || !version {
		t.Errorf("service.name=%v service.version=%v in %v", name, version, res.Attributes())
	}
}
