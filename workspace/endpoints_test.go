package workspace

import "testing"

// TestLakekeeperServiceURL covers the three-way choice the catalog dial makes,
// including the case the split exists for: a box (no Namespace) whose dial URL
// differs from what it advertises.
func TestLakekeeperServiceURL(t *testing.T) {
	tests := []struct {
		name string
		ws   Workspace
		want string
	}{
		{
			name: "shared substrate prefers the in-cluster Service",
			ws: Workspace{
				Namespace:     "customer-acme",
				LakekeeperURL: "https://lakekeeper.customer-acme.fairtier.com",
			},
			want: "http://lakekeeper.customer-acme.svc:8181",
		},
		{
			name: "box with no override dials what it advertises",
			ws:   Workspace{LakekeeperURL: "https://lakekeeper.customer-acme.fairtier.com"},
			want: "https://lakekeeper.customer-acme.fairtier.com",
		},
		{
			name: "box override wins over the advertised URL",
			ws: Workspace{
				LakekeeperURL:     "https://lakekeeper.customer-acme.fairtier.com",
				LakekeeperDialURL: "http://lakekeeper:8181",
			},
			want: "http://lakekeeper:8181",
		},
		{
			name: "an explicit override beats even the in-cluster derivation",
			ws: Workspace{
				Namespace:         "customer-acme",
				LakekeeperURL:     "https://lakekeeper.customer-acme.fairtier.com",
				LakekeeperDialURL: "http://lakekeeper.elsewhere.svc:8181",
			},
			want: "http://lakekeeper.elsewhere.svc:8181",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ws.LakekeeperServiceURL(); got != tt.want {
				t.Errorf("LakekeeperServiceURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDuckFlightServiceURL(t *testing.T) {
	tests := []struct {
		name string
		ws   Workspace
		want string
	}{
		{
			name: "no override dials what it advertises",
			ws:   Workspace{DuckFlightURL: "https://duckflight.customer-acme.fairtier.com"},
			want: "https://duckflight.customer-acme.fairtier.com",
		},
		{
			name: "override wins",
			ws: Workspace{
				DuckFlightURL:     "https://duckflight.customer-acme.fairtier.com",
				DuckFlightDialURL: "http://duckflight:31337",
			},
			want: "http://duckflight:31337",
		},
		{
			// Nothing to dial is distinct from "advertises nothing": the query
			// service refuses on this, not on DuckFlightURL.
			name: "neither set is empty",
			ws:   Workspace{},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ws.DuckFlightServiceURL(); got != tt.want {
				t.Errorf("DuckFlightServiceURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDialURLsDoNotChangeWhatIsAdvertised is the whole point of the split: a
// box pointed at its own in-cluster Services must still hand the customer's
// browser the public addresses. The bootstrap document is asserted separately
// (TestBootstrapLeaksNoSecret already fails any new field that reaches it);
// this pins the fields the document is built from.
func TestDialURLsDoNotChangeWhatIsAdvertised(t *testing.T) {
	ws := Workspace{
		CustomerDomain:    "customer-acme.fairtier.com",
		LakekeeperURL:     "https://lakekeeper.customer-acme.fairtier.com",
		DuckFlightURL:     "https://duckflight.customer-acme.fairtier.com",
		LakekeeperDialURL: "http://lakekeeper:8181",
		DuckFlightDialURL: "http://duckflight:31337",
	}
	if got, want := ws.LakekeeperURL, "https://lakekeeper.customer-acme.fairtier.com"; got != want {
		t.Errorf("advertised LakekeeperURL = %q, want %q", got, want)
	}
	if got, want := ws.DuckFlightURL, "https://duckflight.customer-acme.fairtier.com"; got != want {
		t.Errorf("advertised DuckFlightURL = %q, want %q", got, want)
	}
	if ws.LakekeeperServiceURL() == ws.LakekeeperURL {
		t.Error("the catalog dial URL should differ from the advertised one once overridden")
	}
	if ws.DuckFlightServiceURL() == ws.DuckFlightURL {
		t.Error("the engine dial URL should differ from the advertised one once overridden")
	}
}
