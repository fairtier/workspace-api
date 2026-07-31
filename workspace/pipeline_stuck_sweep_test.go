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
