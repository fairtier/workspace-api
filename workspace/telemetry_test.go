package workspace

import (
	"errors"
	"testing"
	"time"
)

func TestSyncOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		err       error
		converged bool
		want      string
	}{
		{"converged", nil, true, outcomeOK},
		// Out of mirror scope: nothing failed, but nothing converged either,
		// and reporting that as success would hide a mirror that is wired but
		// never doing anything.
		{"out of scope", nil, false, outcomeSkipped},
		{"failed", errors.New("boom"), true, outcomeError},
		{"failed before scope was known", errors.New("boom"), false, outcomeError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := syncOutcome(tt.err, tt.converged); got != tt.want {
				t.Errorf("syncOutcome(%v, %v) = %q, want %q", tt.err, tt.converged, got, tt.want)
			}
		})
	}
}

func TestDraftOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, outcomeOK},
		{"feature not configured", ErrDraftNotConfigured, outcomeUnavailable},
		{"caller over the limit", ErrDraftRateLimited, outcomeRateLimited},
		// The service wraps both of these, so matching has to survive it.
		{"wrapped sentinel", errors.Join(errors.New("draft pipeline"), ErrDraftRateLimited), outcomeRateLimited},
		{"model output rejected", &ErrInvalidSourceConfig{Field: "name", Msg: "missing"}, outcomeInvalidDraft},
		{"provider failed", errors.New("deepseek: status 503"), outcomeError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := draftOutcome(tt.err); got != tt.want {
				t.Errorf("draftOutcome(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestRunDurationSeconds(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(90 * time.Second)
	before := start.Add(-time.Second)

	tests := []struct {
		name      string
		startedAt *time.Time
		completed *time.Time
		want      float64
		wantOK    bool
	}{
		{"complete run", &start, &end, 90, true},
		// A worker that died mid-load reports no timestamps. Recording a zero
		// would put the runs that matter most in the fastest bucket.
		{"never started", nil, &end, 0, false},
		{"never completed", &start, nil, 0, false},
		{"neither", nil, nil, 0, false},
		// Clock skew between the box worker and this process.
		{"completed before it started", &end, &before, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := runDurationSeconds(tt.startedAt, tt.completed)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("runDurationSeconds() = (%v, %v), want (%v, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestRepoFileKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want string
	}{
		{"pipelines/orders.credentials.age", "credential"},
		{"pipelines/orders.yaml", "definition"},
		{"transformations/daily.yaml", "definition"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			if got := repoFileKind(tt.path); got != tt.want {
				t.Errorf("repoFileKind(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
