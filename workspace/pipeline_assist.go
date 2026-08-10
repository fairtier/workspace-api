package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// PipelineDraft is the LLM-produced draft of a pipeline configuration. It maps
// onto the non-sensitive fields of a CreatePipeline request — never
// credentials, which the user always supplies themselves.
type PipelineDraft struct {
	Name             string
	SourceType       string
	DatasetName      string
	Schedule         string
	WriteDisposition string
	MergeStrategy    string
	SourceConfig     json.RawMessage
	// Notes is a short human-readable explanation of the draft / assumptions,
	// surfaced to the user above the pre-filled form.
	Notes string
}

// PipelineDrafter turns a natural-language prompt into a draft pipeline
// configuration via an LLM. Implementations live outside the domain (e.g. the
// llm package) so the domain stays free of any LLM SDK dependency.
type PipelineDrafter interface {
	DraftPipeline(ctx context.Context, prompt string) (*PipelineDraft, error)
}

// RateLimiter gates how often a given key (e.g. a caller id) may perform an
// action. Allow returns false when the key is over its limit.
type RateLimiter interface {
	Allow(key string) bool
}

// PipelineAssistService drafts pipeline configurations from natural language.
// It reuses the same source-config validator as the manual CreatePipeline path
// so the AI draft and a hand-written request converge on one error surface.
type PipelineAssistService struct {
	// Drafter performs the LLM call. When nil, DraftPipeline returns
	// ErrDraftNotConfigured (the feature is unconfigured on this server).
	Drafter PipelineDrafter
	// Customers resolves the caller's tenant; the draft RPC is tenant-scoped so
	// only provisioned customers can use it.
	Workspaces Resolver
	// Limiter optionally rate-limits draft requests per caller. When nil, no
	// limiting is applied.
	Limiter RateLimiter
	Logger  *slog.Logger
}

// DraftPipeline turns prompt into a validated PipelineDraft for the caller's
// tenant. The drafted source_config is validated with ValidateSourceConfig, so
// a malformed draft fails exactly like a malformed manual request.
func (s *PipelineAssistService) DraftPipeline(ctx context.Context, callerID core.UserID, prompt string) (_ *PipelineDraft, err error) {
	defer func() { recordDraft(ctx, "pipeline", err) }()

	if s.Drafter == nil {
		return nil, ErrDraftNotConfigured
	}
	if strings.TrimSpace(prompt) == "" {
		return nil, &ErrInvalidSourceConfig{Field: "prompt", Msg: "prompt is required"}
	}

	// Tenant-scope the call (same discipline as the rest of the pipeline RPCs).
	if _, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID); err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	if s.Limiter != nil && !s.Limiter.Allow(string(callerID)) {
		return nil, ErrDraftRateLimited
	}

	draft, err := s.Drafter.DraftPipeline(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("draft pipeline: %w", err)
	}

	// The model must never produce credentials; defend against it regardless of
	// the prompt by validating only the (non-sensitive) source_config here.
	if err := ValidateSourceConfig(draft.SourceType, draft.SourceConfig); err != nil {
		return nil, err
	}
	if draft.WriteDisposition == "" {
		draft.WriteDisposition = "append"
	}

	return draft, nil
}

// MemoryRateLimiter is a simple per-key fixed-window rate limiter kept in
// process memory. It is sufficient for guarding an abusable-but-cheap RPC on a
// single replica; a shared store would be needed for multi-replica precision.
type MemoryRateLimiter struct {
	max    int           // max events allowed per window
	window time.Duration // window length

	mu      sync.Mutex
	buckets map[string]*rateBucket
	now     func() time.Time // injectable clock for tests
}

type rateBucket struct {
	windowStart time.Time
	count       int
}

// NewMemoryRateLimiter returns a limiter allowing at most max events per window
// per key. A non-positive max disables limiting (Allow always returns true).
func NewMemoryRateLimiter(max int, window time.Duration) *MemoryRateLimiter {
	return &MemoryRateLimiter{
		max:     max,
		window:  window,
		buckets: make(map[string]*rateBucket),
		now:     time.Now,
	}
}

// Allow reports whether key may perform another event now, recording it if so.
func (l *MemoryRateLimiter) Allow(key string) bool {
	if l.max <= 0 {
		return true
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok || now.Sub(b.windowStart) >= l.window {
		// Amortized eviction: drop every expired bucket whenever a new window
		// starts, so the map stays bounded by the number of active keys
		// instead of growing one entry per caller forever.
		for k, old := range l.buckets {
			if now.Sub(old.windowStart) >= l.window {
				delete(l.buckets, k)
			}
		}
		l.buckets[key] = &rateBucket{windowStart: now, count: 1}
		return true
	}
	if b.count >= l.max {
		return false
	}
	b.count++
	return true
}
