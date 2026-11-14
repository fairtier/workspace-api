package workspace_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

type fakeStuckRunStore struct {
	gotSlug      string
	gotOlderThan time.Duration
	calls        int
	swept        int64
	err          error
}

func (s *fakeStuckRunStore) FailStuckRunningRuns(_ context.Context, customerSlug string, olderThan time.Duration) (int64, error) {
	s.calls++
	s.gotSlug = customerSlug
	s.gotOlderThan = olderThan
	return s.swept, s.err
}

func TestStuckRunSweepPassesTimeout(t *testing.T) {
	store := &fakeStuckRunStore{swept: 2}
	s := &workspace.StuckRunSweeper{
		Store:   store,
		Slug:    "acme",
		Timeout: 30 * time.Minute,
		Logger:  slog.New(slog.DiscardHandler),
	}

	if err := s.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if store.calls != 1 {
		t.Errorf("FailStuckRunningRuns calls = %d, want 1", store.calls)
	}
	if store.gotSlug != "acme" {
		t.Errorf("customerSlug = %q, want acme", store.gotSlug)
	}
	if store.gotOlderThan != 30*time.Minute {
		t.Errorf("olderThan = %s, want 30m", store.gotOlderThan)
	}
}

func TestStuckRunSweepDefaultTimeout(t *testing.T) {
	store := &fakeStuckRunStore{}
	s := &workspace.StuckRunSweeper{Store: store, Logger: slog.New(slog.DiscardHandler)}

	if err := s.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if store.gotOlderThan != workspace.DefaultStuckRunTimeout {
		t.Errorf("olderThan = %s, want default %s", store.gotOlderThan, workspace.DefaultStuckRunTimeout)
	}
	if store.gotSlug != "" {
		t.Errorf("customerSlug = %q, want empty (all workspaces)", store.gotSlug)
	}
}

func TestStuckRunSweepPropagatesError(t *testing.T) {
	store := &fakeStuckRunStore{err: errors.New("boom")}
	s := &workspace.StuckRunSweeper{Store: store, Logger: slog.New(slog.DiscardHandler)}

	if err := s.SweepOnce(context.Background()); err == nil {
		t.Fatal("SweepOnce: want error, got nil")
	}
}

// The sweep and the worker write the same pipeline_runs rows on a box. A run
// is one row across all of its attempts (the worker reuses one run id), and
// staleness is measured from that row's started_at — so the default must
// outlast every attempt the worker is allowed by default, plus the backoff
// between them. A 1h default against a 6h-per-attempt worker failed live
// runs.
func TestDefaultStuckRunTimeoutOutlastsDefaultWorkerRun(t *testing.T) {
	const workerDefaultMaxRun = 3*6*time.Hour + (30+60)*time.Second // 3 attempts x 6h + backoff
	if workspace.DefaultStuckRunTimeout <= workerDefaultMaxRun {
		t.Fatalf("DefaultStuckRunTimeout = %s, want > %s (the worker's default worst-case run)",
			workspace.DefaultStuckRunTimeout, workerDefaultMaxRun)
	}
}

func TestWorkerRunLimitsMaxRunDuration(t *testing.T) {
	tests := []struct {
		name   string
		limits workspace.WorkerRunLimits
		want   time.Duration
		wantOK bool
	}{
		{name: "worker defaults", limits: workspace.DefaultWorkerRunLimits(), want: 18*time.Hour + 90*time.Second, wantOK: true},
		{name: "no retries", limits: workspace.WorkerRunLimits{AttemptTimeout: 2 * time.Hour}, want: 2 * time.Hour, wantOK: true},
		{
			name: "backoff doubles", limits: workspace.WorkerRunLimits{AttemptTimeout: time.Hour, MaxRetries: 3, RetryBaseDelay: 10 * time.Second},
			want: 4*time.Hour + (10+20+40)*time.Second, wantOK: true,
		},
		{name: "negative retries is one attempt", limits: workspace.WorkerRunLimits{AttemptTimeout: time.Hour, MaxRetries: -1}, want: time.Hour, wantOK: true},
		{name: "no attempt limit is unbounded", limits: workspace.WorkerRunLimits{MaxRetries: 2, RetryBaseDelay: 30 * time.Second}},
		{name: "absurd retries is unbounded", limits: workspace.WorkerRunLimits{AttemptTimeout: time.Hour, MaxRetries: 1000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.limits.MaxRunDuration()
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("MaxRunDuration() = %s, %v; want %s, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestDefaultStuckRunTimeoutIsDerivedFromWorkerDefaults(t *testing.T) {
	got, warning := workspace.StuckRunTimeoutFor(workspace.DefaultWorkerRunLimits(), 0)
	if got != workspace.DefaultStuckRunTimeout || warning != "" {
		t.Errorf("StuckRunTimeoutFor(defaults, 0) = %s, %q; want %s, no warning", got, warning, workspace.DefaultStuckRunTimeout)
	}
}

func TestStuckRunTimeoutFor(t *testing.T) {
	twoHourWorker := workspace.WorkerRunLimits{AttemptTimeout: 2 * time.Hour} // floor 3h
	tests := []struct {
		name        string
		limits      workspace.WorkerRunLimits
		override    time.Duration
		want        time.Duration
		wantWarning bool
	}{
		{name: "derived when unset", limits: twoHourWorker, want: 2*time.Hour + workspace.StuckRunGrace},
		{name: "override above floor kept", limits: twoHourWorker, override: 5 * time.Hour, want: 5 * time.Hour},
		{name: "override below floor raised", limits: twoHourWorker, override: time.Hour, want: 2*time.Hour + workspace.StuckRunGrace, wantWarning: true},
		{
			name: "the old 1h against the default worker is raised", limits: workspace.DefaultWorkerRunLimits(), override: time.Hour,
			want: workspace.DefaultStuckRunTimeout, wantWarning: true,
		},
		{name: "unbounded worker disables the sweep", limits: workspace.WorkerRunLimits{}, want: 0, wantWarning: true},
		{
			name: "unbounded worker honours an explicit timeout", limits: workspace.WorkerRunLimits{}, override: 48 * time.Hour,
			want: 48 * time.Hour, wantWarning: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, warning := workspace.StuckRunTimeoutFor(tt.limits, tt.override)
			if got != tt.want {
				t.Errorf("timeout = %s, want %s", got, tt.want)
			}
			if (warning != "") != tt.wantWarning {
				t.Errorf("warning = %q, want warning: %v", warning, tt.wantWarning)
			}
		})
	}
}
