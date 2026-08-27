package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/fairtier/workspace-api/core"
)

// A source test is the answer to "can this thing read my X?", asked before a
// pipeline is saved rather than discovered from the first scheduled run hours
// later.
//
// It is a queued job and not a call, for one reason: the probe has to run where
// extraction runs. The worker has the drivers, the baked DuckDB extensions, the
// box's own network path and — most of it — the subprocess isolation that keeps
// a wedged connection attempt from taking anything else down with it. This
// process has none of that, and running the probe here would be testing a
// different thing than the one that will run.
//
// So the flow is: the Console queues a test, the worker claims it on its next
// poll, probes, and reports; the Console polls for the outcome. The cost is
// latency measured in the worker's test-poll interval rather than milliseconds,
// which is why the Console says the test runs on the box.
const (
	SourceTestPending = "pending"
	SourceTestRunning = "running"
	SourceTestSuccess = "success"
	SourceTestFailed  = "failed"
)

// sourceTestTTL bounds how long a test row lives, whatever happens to it. It
// is also the answer to a worker that claims a test and then dies: nothing
// hangs, the row simply expires, and the Console reports a test that never
// came back rather than a spinner with no end.
const sourceTestTTL = 15 * time.Minute

// SourceTestMaxDetails caps the per-table lines a report may carry. A source
// with 300 tables must not turn one probe into a 300-row response, and the
// point of the detail list is the first few failures anyway.
const SourceTestMaxDetails = 25

// sourceTestMaxMessage bounds one reported message. A driver error can be
// pages long (a JVM stack, a full SQL statement); the row is read by a toast.
const sourceTestMaxMessage = 2000

var (
	// ErrSourceTestNotFound covers both "no such id" and "not yours": a test
	// belonging to another workspace must be indistinguishable from one that
	// never existed.
	ErrSourceTestNotFound = errors.New("source test not found or expired")

	// ErrSourceTestUnsupported is a precondition, not a failure — the Console
	// only offers the button for types the box can actually probe, and this
	// is what a client that asked anyway gets.
	ErrSourceTestUnsupported = errors.New("this source type cannot be tested on this box")
)

// SourceTest is one queued or completed probe.
type SourceTest struct {
	ID           string
	CustomerSlug string
	SourceType   string
	// The config and credentials to probe with. Credentials are encrypted at
	// rest by the store, and are never returned to a user-facing caller.
	SourceConfig      json.RawMessage
	SourceCredentials json.RawMessage
	Status            string
	Message           string
	Details           []string
	CreatedAt         time.Time
	CompletedAt       *time.Time
	ExpiresAt         time.Time
}

// SourceTestStore persists source tests. Every method is tenant-scoped by
// argument rather than by the row's own slug: a test id is a bearer of nothing.
type SourceTestStore interface {
	CreateSourceTest(ctx context.Context, t *SourceTest) error

	// GetSourceTest returns a non-expired test of this customer, or
	// ErrSourceTestNotFound.
	GetSourceTest(ctx context.Context, id, customerSlug string) (*SourceTest, error)

	// ClaimPendingSourceTests flips this customer's pending, unexpired tests to
	// running and returns them. Claiming in the same statement as the read is
	// what keeps two worker polls (or two workers) from probing one test twice.
	ClaimPendingSourceTests(ctx context.Context, customerSlug string) ([]SourceTest, error)

	// CompleteSourceTest records an outcome. It matches on the customer too,
	// so a worker can only ever finish its own tenant's test.
	CompleteSourceTest(ctx context.Context, id, customerSlug, status, message string, details []string) error

	// DeleteExpiredSourceTests sweeps rows past their TTL, credentials
	// included. Called on the same schedule as the other short-lived sweeps.
	DeleteExpiredSourceTests(ctx context.Context) (int64, error)
}

// SourceTestService queues source tests and serves them to the worker.
type SourceTestService struct {
	Workspaces Resolver
	Tests      SourceTestStore
	// Pipelines resolves the credentials an edit form left blank ("leave empty
	// to keep"). Optional: without it, a test must carry its own.
	Pipelines PipelineRepository
	// OAuthClients and Connections resolve a Google credential the same way
	// the worker poll does, so a Drive pipeline can be tested with the
	// connection it references rather than with a token nobody has.
	OAuthClients OAuthClientStore
	Connections  ConnectionStore
	Logger       *slog.Logger
}

// StartSourceTest queues a probe for the caller's workspace.
func (s *SourceTestService) StartSourceTest(ctx context.Context, callerID core.UserID, in *SourceTest) (*SourceTest, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	if !SourceTestSupported(in.SourceType) {
		return nil, ErrSourceTestUnsupported
	}
	// The same validation a save runs. A test is a slower, remoter way to
	// discover a malformed config, and the immediate refusal is the better one.
	if err := ValidateSourceConfig(in.SourceType, in.SourceConfig); err != nil {
		return nil, err
	}

	now := time.Now()
	t := &SourceTest{
		ID:                uuid.NewString(),
		CustomerSlug:      ws.Slug,
		SourceType:        in.SourceType,
		SourceConfig:      in.SourceConfig,
		SourceCredentials: in.SourceCredentials,
		Status:            SourceTestPending,
		CreatedAt:         now,
		ExpiresAt:         now.Add(sourceTestTTL),
	}
	if err := ValidateSourceCredentials(t.SourceType, t.SourceConfig, t.SourceCredentials); err != nil {
		return nil, err
	}
	if err := s.Tests.CreateSourceTest(ctx, t); err != nil {
		return nil, fmt.Errorf("create source test: %w", err)
	}
	t.SourceCredentials = nil
	return t, nil
}

// PipelineCredentials returns a pipeline's stored source credentials, for a
// test whose form left them blank. It is a separate step from StartSourceTest
// so the ownership check lives with the pipeline it protects: a pipeline id
// from another workspace resolves to nothing rather than to its credentials.
func (s *SourceTestService) PipelineCredentials(ctx context.Context, callerID core.UserID, id PipelineID) (json.RawMessage, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	if s.Pipelines == nil {
		return nil, ErrPipelineNotFound
	}
	p, err := s.Pipelines.GetPipeline(ctx, id)
	if err != nil {
		return nil, err
	}
	if p.CustomerSlug != ws.Slug {
		return nil, ErrPipelineNotFound
	}
	return p.SourceCredentials, nil
}

// GetSourceTest returns one test of the caller's workspace, without its
// credentials — nothing that went in comes back out.
func (s *SourceTestService) GetSourceTest(ctx context.Context, callerID core.UserID, id string) (*SourceTest, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	t, err := s.Tests.GetSourceTest(ctx, id, ws.Slug)
	if err != nil {
		return nil, err
	}
	t.SourceConfig = nil
	t.SourceCredentials = nil
	return t, nil
}

// ClaimSourceTests hands the worker its tenant's queued tests, with Google
// credentials resolved exactly as GetEnabledPipelines resolves them — a test
// of a pipeline that references a workspace Connection must exercise that
// connection, not a shape only the run path knows how to fill in.
func (s *SourceTestService) ClaimSourceTests(ctx context.Context, customerSlug string) ([]SourceTest, error) {
	tests, err := s.Tests.ClaimPendingSourceTests(ctx, customerSlug)
	if err != nil {
		return nil, fmt.Errorf("claim source tests: %w", err)
	}
	if len(tests) == 0 {
		return nil, nil
	}
	oauth := newOAuthClientResolver(s.OAuthClients, s.Connections, customerSlug)
	for i := range tests {
		p := Pipeline{SourceType: tests[i].SourceType, SourceCredentials: tests[i].SourceCredentials}
		if injected, ok := oauth.inject(ctx, &p); ok {
			tests[i].SourceCredentials = injected
		}
	}
	return tests, nil
}

// ReportSourceTest records the worker's verdict.
func (s *SourceTestService) ReportSourceTest(ctx context.Context, customerSlug, id, status, message string, details []string) error {
	switch status {
	case SourceTestSuccess, SourceTestFailed:
	default:
		return fmt.Errorf("invalid source test status %q", status)
	}
	if len(details) > SourceTestMaxDetails {
		details = details[:SourceTestMaxDetails]
	}
	message = truncateRunes(message, sourceTestMaxMessage)
	if err := s.Tests.CompleteSourceTest(ctx, id, customerSlug, status, message, details); err != nil {
		return err
	}
	return nil
}

// SweepExpiredSourceTests deletes timed-out rows on a loop, once immediately
// and then every interval, until ctx is done — the same shape as
// SweepExpiredGrants, and for the same reason: an answered test is finished
// with its credentials, and an unanswered one still holds them.
func SweepExpiredSourceTests(ctx context.Context, tests SourceTestStore, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		n, err := tests.DeleteExpiredSourceTests(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			logger.Warn("source test sweep failed", "error", err)
		case n > 0:
			logger.Info("source test sweep removed expired tests", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// testableSourceTypes are the source types the dlt-worker knows how to probe.
//
// A fourth cross-repo list is exactly what the duckdb work spent a phase
// avoiding, so this one is SERVED rather than copied: the bootstrap document
// carries it, and the Console shows a Test button for the intersection of what
// it has a form for and what this box says it can probe. Adding a probe is
// then a worker change plus this line, and never a Console release.
var testableSourceTypes = []string{SourceTypeDuckDB, "sql_database"}

// SourceTestSupported reports whether the worker has a probe for a source type.
func SourceTestSupported(sourceType string) bool {
	for _, t := range testableSourceTypes {
		if t == sourceType {
			return true
		}
	}
	return false
}

// TestableSourceTypes is the served list, for the bootstrap document.
func TestableSourceTypes() []string {
	out := make([]string, len(testableSourceTypes))
	copy(out, testableSourceTypes)
	return out
}

// truncateRunes bounds a string by RUNES, not bytes: a driver error can be in
// any language, and cutting one mid-rune stores a broken character. (Distinct
// from assist_explain.go's byte-wise truncate, which bounds an LLM prompt.)
func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// SanitizeSourceTestDetails trims and bounds the worker's detail lines. The
// worker scrubs credentials out of them; this only keeps a report from being
// unreasonably large.
func SanitizeSourceTestDetails(details []string) []string {
	out := make([]string, 0, len(details))
	for _, d := range details {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		d = truncateRunes(d, 500)
		out = append(out, d)
		if len(out) == SourceTestMaxDetails {
			break
		}
	}
	return out
}
