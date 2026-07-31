package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/fairtier/workspace-api/demo"

	"github.com/fairtier/workspace-api/core"
)

// The "NYC Taxi Pulse" starter demo.
// DemoService seeds and removes it by reusing the existing pipeline,
// file-drop, box-repo, and transformation services — it owns no new box-side
// machinery. The embedded assets (dbt/Rill files, the zone CSV) and the sizing
// tiers live in the leaf `demo` package.

// Demo errors, mapped to Connect codes by the server.
var (
	// ErrDemoNotConfigured means the server has no demo-bucket credential, so
	// the demo cannot be loaded (Console hides the button).
	ErrDemoNotConfigured = errors.New("the demo dataset is not configured on this server")
	// ErrDemoAlreadyLoaded means a demo is already seeded in this workspace.
	ErrDemoAlreadyLoaded = errors.New("the demo project is already loaded in this workspace")
	// ErrDemoNotLoaded means no demo has been seeded in this workspace.
	ErrDemoNotLoaded = errors.New("no demo project is loaded in this workspace")
)

// DemoBucket is the platform-held, read-only S3 credential for the shared
// demo-datasets bucket, injected server-side as the demo filesystem pipeline's
// credential so the customer never sees the token. Sourced
// from DEMO_R2_* env in main.go.
type DemoBucket struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Bucket          string
	Region          string
}

// Configured reports whether the demo bucket credential is fully set.
func (b DemoBucket) Configured() bool {
	return b.AccessKeyID != "" && b.SecretAccessKey != "" && b.Endpoint != "" && b.Bucket != ""
}

// DemoRepoFile records one seed-committed box-repo file, so teardown can prune
// exactly what the load created (keyed by repo + path).
type DemoRepoFile struct {
	Repo string `json:"repo"`
	Path string `json:"path"`
}

// Demo load lifecycle states, stored in demo_seeds.status.
const (
	// DemoStatusLoading: the background load is running (or orphaned if stale).
	DemoStatusLoading = "loading"
	// DemoStatusReady: the demo is fully seeded.
	DemoStatusReady = "ready"
	// DemoStatusRemoving: the background teardown is running (or orphaned if
	// stale). Like the load, removal does serial box I/O (a mirror sync per
	// pipeline delete + a commit per pruned repo file) that far exceeds an HTTP
	// request budget — a synchronous teardown 504'd behind Cloudflare against a
	// slow box — so it runs in the background and the Console polls.
	DemoStatusRemoving = "removing"
)

// demoLoadTimeout bounds the background load worker (box I/O can be slow, but
// not this slow). demoLoadingStale marks a "loading" row abandoned — e.g. the
// pod restarted mid-load — so GetDemoStatus/LoadDemoProject can offer a retry
// instead of wedging on it forever.
const (
	demoLoadTimeout  = 12 * time.Minute
	demoLoadingStale = 15 * time.Minute
	demoCleanupGrace = 60 * time.Second
)

// DemoSeed is the bookkeeping record for a loaded demo, keyed by customer slug.
type DemoSeed struct {
	CustomerSlug     string
	Tier             string
	Status           string
	TripsPipelineID  PipelineID
	ZonesPipelineID  PipelineID
	TransformationID TransformationID
	RepoFiles        []DemoRepoFile
	CreatedAt        time.Time
}

// DemoSeedStore persists the per-workspace demo bookkeeping record.
type DemoSeedStore interface {
	// SaveDemoSeed inserts or replaces the seed record for a slug.
	SaveDemoSeed(ctx context.Context, seed *DemoSeed) error
	// GetDemoSeed returns the seed record for a slug, or ErrDemoNotLoaded.
	GetDemoSeed(ctx context.Context, customerSlug string) (*DemoSeed, error)
	// DeleteDemoSeed removes the seed record for a slug (no error if absent).
	DeleteDemoSeed(ctx context.Context, customerSlug string) error
}

// The narrow slices of the existing services DemoService drives. The concrete
// *PipelineService / *FileDropService / *TransformationService /
// *BoxRepoService satisfy these, and they keep the demo orchestration unit-
// testable with fakes.

// DemoPipelineOps is the pipeline surface the demo needs.
type DemoPipelineOps interface {
	CreatePipeline(ctx context.Context, callerID core.UserID, p *Pipeline) (*Pipeline, error)
	TriggerPipeline(ctx context.Context, callerID core.UserID, id PipelineID) (*PipelineRun, error)
	DeletePipeline(ctx context.Context, callerID core.UserID, id PipelineID) error
}

// DemoTransformationOps is the transformation surface the demo needs.
type DemoTransformationOps interface {
	CreateTransformation(ctx context.Context, callerID core.UserID, t *Transformation) (*Transformation, error)
	DeleteTransformation(ctx context.Context, callerID core.UserID, id TransformationID) error
}

// DemoBoxRepoOps is the box-repo surface the demo needs (commit + prune).
type DemoBoxRepoOps interface {
	GetFile(ctx context.Context, callerID core.UserID, repo, filePath string) (content, sha string, err error)
	PutFile(ctx context.Context, callerID core.UserID, repo, filePath, content, sha, message string) (string, error)
	DeleteFile(ctx context.Context, callerID core.UserID, repo, filePath, sha, message string) error
}

// DemoService orchestrates loading and removing the starter demo.
type DemoService struct {
	Workspaces      Resolver
	Seeds           DemoSeedStore
	Pipelines       DemoPipelineOps
	Transformations DemoTransformationOps
	BoxRepo         DemoBoxRepoOps
	Bucket          DemoBucket
	// Runner executes the background load worker. Nil (production) spawns a
	// goroutine; tests inject a synchronous runner. The load does a lot of
	// serial box I/O (a mirror sync per pipeline + a commit per dbt/Rill
	// file) that far exceeds an HTTP request budget, so LoadDemoProject
	// returns immediately and the work happens here.
	Runner func(func())
	Logger *slog.Logger
}

const demoCommitMessage = "Seed NYC Taxi Pulse demo project"

// run executes fn in the background (goroutine) unless a Runner is injected.
func (s *DemoService) run(fn func()) {
	if s.Runner != nil {
		s.Runner(fn)
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil && s.Logger != nil {
				s.Logger.Error("demo load worker panicked", "recover", r)
			}
		}()
		fn()
	}()
}

// DemoStatus is the load state of a workspace (mirrors the proto message).
type DemoStatus struct {
	Available        bool
	Loaded           bool
	Loading          bool
	Tier             string
	TripsPipelineID  string
	ZonesPipelineID  string
	TransformationID string
	LoadedAt         *time.Time
}

// GetDemoStatus reports whether the demo can be loaded here, and whether it is
// currently loading or already loaded. A "loading" row past demoLoadingStale
// (e.g. a load whose pod restarted) is reported as neither loaded nor loading,
// so the workspace can retry.
func (s *DemoService) GetDemoStatus(ctx context.Context, callerID core.UserID) (*DemoStatus, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}
	status := &DemoStatus{Available: s.available(ws)}

	seed, err := s.Seeds.GetDemoSeed(ctx, ws.Slug)
	if errors.Is(err, ErrDemoNotLoaded) {
		return status, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get demo seed: %w", err)
	}
	switch {
	case seed.Status == DemoStatusReady:
		fillStatusFromSeed(status, seed)
	case (seed.Status == DemoStatusLoading || seed.Status == DemoStatusRemoving) && !s.loadingStale(seed):
		// Both loading and removing are reported as busy (Loading) so the
		// Console shows a progress bar and keeps polling until the seed
		// settles (ready, or gone after a removal).
		status.Loading = true
		status.Tier = seed.Tier
	}
	return status, nil
}

// available reports whether the demo can be loaded for this customer: a demo
// bucket is configured and the workspace runs on a VM box (whose Gitea repos
// the dbt/Rill files are committed to).
func (s *DemoService) available(ws *Workspace) bool {
	return s.Bucket.Configured() && ws.OnVM
}

// loadingStale reports whether a "loading" seed is old enough to be considered
// abandoned (its worker died without finishing).
func (s *DemoService) loadingStale(seed *DemoSeed) bool {
	return time.Since(seed.CreatedAt) > demoLoadingStale
}

func fillStatusFromSeed(status *DemoStatus, seed *DemoSeed) {
	status.Loaded = true
	status.Tier = seed.Tier
	status.TripsPipelineID = string(seed.TripsPipelineID)
	status.ZonesPipelineID = string(seed.ZonesPipelineID)
	status.TransformationID = string(seed.TransformationID)
	createdAt := seed.CreatedAt
	status.LoadedAt = &createdAt
}

// LoadDemoProject claims the load and kicks the actual work off in the
// background (a synchronous request can't afford the serial box I/O — that is
// what 504'd behind Cloudflare). It returns a "loading" status immediately;
// the Console polls GetDemoStatus. Idempotent: a second call while a load is in
// flight returns the in-progress status; a completed demo returns
// ErrDemoAlreadyLoaded; a failed/abandoned attempt is cleaned up and retried.
func (s *DemoService) LoadDemoProject(ctx context.Context, callerID core.UserID, tierName string) (*DemoStatus, error) {
	if !s.Bucket.Configured() {
		return nil, ErrDemoNotConfigured
	}
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get customer: %w", err)
	}

	tier := demo.TierOrDefault(tierName)
	if status, err := s.resolveExistingSeed(ctx, callerID, ws.Slug); status != nil || err != nil {
		return status, err
	}

	// Claim the load synchronously (idempotency + progress), then run the heavy
	// work detached from the request.
	claim := &DemoSeed{CustomerSlug: ws.Slug, Tier: tier.Name, Status: DemoStatusLoading, CreatedAt: time.Now().UTC()}
	if err := s.Seeds.SaveDemoSeed(ctx, claim); err != nil {
		return nil, fmt.Errorf("claim demo load: %w", err)
	}
	s.run(func() { s.loadWorker(callerID, ws.Slug, tier) })

	return &DemoStatus{Available: true, Loading: true, Tier: tier.Name}, nil
}

// resolveExistingSeed inspects any prior demo seed for the customer and decides
// what LoadDemoProject should do next. A non-nil status or error means the
// caller must return it immediately (already loaded, load in flight, or a
// store error); (nil, nil) means it is clear to load afresh — a failed or
// abandoned attempt is torn down and its seed row cleared before proceeding.
func (s *DemoService) resolveExistingSeed(ctx context.Context, callerID core.UserID, customerSlug string) (*DemoStatus, error) {
	existing, err := s.Seeds.GetDemoSeed(ctx, customerSlug)
	switch {
	case err == nil && existing.Status == DemoStatusReady:
		return nil, ErrDemoAlreadyLoaded
	case err == nil && (existing.Status == DemoStatusLoading || existing.Status == DemoStatusRemoving) && !s.loadingStale(existing):
		// Already in progress — idempotent, report the running load/removal as
		// busy rather than racing a fresh load against a background teardown.
		return &DemoStatus{Available: true, Loading: true, Tier: existing.Tier}, nil
	case err == nil:
		// Failed or abandoned attempt — clean it up, then load afresh.
		return nil, s.cleanupAbandoned(ctx, callerID, existing)
	case !errors.Is(err, ErrDemoNotLoaded):
		return nil, fmt.Errorf("get demo seed: %w", err)
	}
	return nil, nil
}

// cleanupAbandoned tears down a failed or abandoned demo attempt and clears its
// seed row so a fresh load can proceed. teardown is best-effort (logged); a
// failure to clear the seed row is fatal to the retry and returned.
func (s *DemoService) cleanupAbandoned(ctx context.Context, callerID core.UserID, existing *DemoSeed) error {
	if terr := s.teardown(ctx, callerID, existing); terr != nil && s.Logger != nil {
		s.Logger.Error("demo retry: teardown of previous attempt", "customer", existing.CustomerSlug, "err", terr)
	}
	if derr := s.Seeds.DeleteDemoSeed(ctx, existing.CustomerSlug); derr != nil {
		return fmt.Errorf("clear previous demo seed: %w", derr)
	}
	return nil
}

// loadWorker performs the actual seeding off the request path. On success it
// flips the seed row to "ready"; on any failure it rolls back every artifact
// it created and clears the seed row so the workspace is left clean to retry.
func (s *DemoService) loadWorker(callerID core.UserID, customerSlug string, tier demo.Tier) {
	ctx, cancel := context.WithTimeout(context.Background(), demoLoadTimeout)
	defer cancel()

	seed := &DemoSeed{CustomerSlug: customerSlug, Tier: tier.Name, Status: DemoStatusLoading, CreatedAt: time.Now().UTC()}
	var err error
	defer func() {
		if err != nil {
			s.failLoad(callerID, seed, err)
		}
	}()

	if err = s.createPipelines(ctx, callerID, tier, seed); err != nil {
		return
	}
	if err = s.commitRepoFiles(ctx, callerID, seed); err != nil {
		return
	}
	if err = s.createTransformation(ctx, callerID, seed); err != nil {
		return
	}
	if err = s.kickOff(ctx, callerID, seed); err != nil {
		return
	}
	seed.Status = DemoStatusReady
	if err = s.Seeds.SaveDemoSeed(ctx, seed); err != nil {
		return
	}
}

// failLoad rolls back a failed background load: it tears down whatever the
// worker managed to create and deletes the seed row (a clean slate to retry).
// Uses a fresh, bounded context — the worker's may be cancelled/expired.
func (s *DemoService) failLoad(callerID core.UserID, seed *DemoSeed, cause error) {
	if s.Logger != nil {
		s.Logger.Error("demo load failed; rolling back", "customer", seed.CustomerSlug, "err", cause)
	}
	ctx, cancel := context.WithTimeout(context.Background(), demoCleanupGrace)
	defer cancel()
	if terr := s.teardown(ctx, callerID, seed); terr != nil && s.Logger != nil {
		s.Logger.Error("demo load rollback: teardown", "customer", seed.CustomerSlug, "err", terr)
	}
	if err := s.Seeds.DeleteDemoSeed(ctx, seed.CustomerSlug); err != nil && s.Logger != nil {
		s.Logger.Error("demo load rollback: delete seed", "customer", seed.CustomerSlug, "err", err)
	}
}

// createPipelines creates the two filesystem pipelines that read the shared
// demo bucket: the trips backfill (globbing the tier's parquet) and the zone
// lookup. Both inject the platform-held demo-bucket credential server-side, so
// the customer never sees the token — and the public demo data is never copied
// into the customer's own bucket.
func (s *DemoService) createPipelines(ctx context.Context, callerID core.UserID, tier demo.Tier, seed *DemoSeed) error {
	trips := &Pipeline{
		Name:              "NYC Taxi Trips",
		SourceType:        "filesystem",
		SourceConfig:      s.demoSourceConfig(demo.TripsTable, tier.Glob),
		SourceCredentials: s.demoCredentials(),
		DatasetName:       demo.DatasetName,
		WriteDisposition:  "append",
		// Schedule is deliberately empty (manual "Run now"): whether the
		// filesystem source tracks already-processed files was settled by
		// measurement. Manual runs are safe either way.
		Schedule: "",
	}
	created, err := s.Pipelines.CreatePipeline(ctx, callerID, trips)
	if err != nil {
		return fmt.Errorf("create trips pipeline: %w", err)
	}
	seed.TripsPipelineID = created.ID

	zones := &Pipeline{
		Name:              "Taxi Zones",
		SourceType:        "filesystem",
		SourceConfig:      s.demoSourceConfig(demo.ZonesTable, demo.ZonesObject),
		SourceCredentials: s.demoCredentials(),
		DatasetName:       demo.DatasetName,
		WriteDisposition:  "replace",
	}
	createdZones, err := s.Pipelines.CreatePipeline(ctx, callerID, zones)
	if err != nil {
		return fmt.Errorf("create zones pipeline: %w", err)
	}
	seed.ZonesPipelineID = createdZones.ID
	return nil
}

// demoSourceConfig builds a filesystem source_config over the demo bucket's
// nyc-taxi/ prefix with one table globbing the given files. Extra `tables`
// beyond the schema's bucket_url are passed through to the worker (dlt-worker
// ≥0.0.6 contract), the same shape a file_upload pipeline resolves to.
func (s *DemoService) demoSourceConfig(table, glob string) json.RawMessage {
	raw, _ := json.Marshal(struct {
		filesystemConfig
		Tables []filesystemTable `json:"tables"`
	}{
		filesystemConfig: filesystemConfig{BucketURL: "s3://" + s.Bucket.Bucket + "/" + demo.BucketPrefix},
		Tables:           []filesystemTable{{Name: table, FileGlob: glob}},
	})
	return raw
}

// demoCredentials builds the filesystem source credentials from the
// platform-held demo bucket token, shared by both demo pipelines.
func (s *DemoService) demoCredentials() json.RawMessage {
	raw, _ := json.Marshal(filesystemCreds{
		AccessKeyID:     s.Bucket.AccessKeyID,
		SecretAccessKey: s.Bucket.SecretAccessKey,
		EndpointURL:     s.Bucket.Endpoint,
		Region:          s.Bucket.Region,
	})
	return raw
}

// commitRepoFiles commits every embedded dbt/Rill file into its box repo,
// recording each for teardown. Existing files (e.g. a re-load, or the seed's
// own sources.yml) are updated in place via their current blob sha.
func (s *DemoService) commitRepoFiles(ctx context.Context, callerID core.UserID, seed *DemoSeed) error {
	files, err := demo.RepoFiles()
	if err != nil {
		return fmt.Errorf("load demo repo files: %w", err)
	}
	for _, f := range files {
		// A present file yields its current sha (update); a missing one yields
		// "" (create). Any lookup error is treated as "create" and surfaced by
		// PutFile if it was in fact a real failure.
		_, sha, _ := s.BoxRepo.GetFile(ctx, callerID, f.Repo, f.Path)
		if _, err := s.BoxRepo.PutFile(ctx, callerID, f.Repo, f.Path, f.Content, sha, demoCommitMessage); err != nil {
			return fmt.Errorf("commit %s/%s: %w", f.Repo, f.Path, err)
		}
		seed.RepoFiles = append(seed.RepoFiles, DemoRepoFile{Repo: f.Repo, Path: f.Path})
	}
	return nil
}

// createTransformation creates the dbt transformation chained to run after the
// trips ingest succeeds (box-hosted transformations repo).
func (s *DemoService) createTransformation(ctx context.Context, callerID core.UserID, seed *DemoSeed) error {
	t := &Transformation{
		Name:                   "NYC Taxi Pulse",
		RepoURL:                "", // box-hosted transformations repo
		TriggerAfterPipelineID: seed.TripsPipelineID,
	}
	created, err := s.Transformations.CreateTransformation(ctx, callerID, t)
	if err != nil {
		return fmt.Errorf("create transformation: %w", err)
	}
	seed.TransformationID = created.ID
	return nil
}

// kickOff triggers the zone load (fast) and the trips backfill; the dbt run is
// chained to the trips pipeline's success inside the dlt-worker.
func (s *DemoService) kickOff(ctx context.Context, callerID core.UserID, seed *DemoSeed) error {
	if _, err := s.Pipelines.TriggerPipeline(ctx, callerID, seed.ZonesPipelineID); err != nil {
		return fmt.Errorf("trigger zones pipeline: %w", err)
	}
	if _, err := s.Pipelines.TriggerPipeline(ctx, callerID, seed.TripsPipelineID); err != nil {
		return fmt.Errorf("trigger trips pipeline: %w", err)
	}
	return nil
}

// RemoveDemoProject tears the demo down in the background and returns
// immediately: deleting the pipelines, transformation, and seed commits does
// serial box I/O (a mirror sync per pipeline, a commit per pruned repo file)
// that exceeds an HTTP request budget — a synchronous teardown 504'd behind
// Cloudflare against a slow box. It marks the seed "removing" (the Console
// polls GetDemoStatus, which reports it as busy) and does the work detached
// from the request. Idempotent: a removal already in flight is a no-op.
// Warehouse tables are left in place (see the proto doc).
func (s *DemoService) RemoveDemoProject(ctx context.Context, callerID core.UserID) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get customer: %w", err)
	}
	seed, err := s.Seeds.GetDemoSeed(ctx, ws.Slug)
	if err != nil {
		return err // ErrDemoNotLoaded or a real store error
	}
	// Already tearing down — idempotent, nothing more to kick off.
	if seed.Status == DemoStatusRemoving && !s.loadingStale(seed) {
		return nil
	}

	// Claim the removal (CreatedAt reset so staleness measures the teardown,
	// not the original load), then run the heavy work detached from the request.
	seed.Status = DemoStatusRemoving
	seed.CreatedAt = time.Now().UTC()
	if err := s.Seeds.SaveDemoSeed(ctx, seed); err != nil {
		return fmt.Errorf("claim demo removal: %w", err)
	}
	s.run(func() { s.removeWorker(callerID, seed) })
	return nil
}

// removeWorker performs the teardown off the request path. On success it
// deletes the seed row; on failure it flips the seed back to "ready" so the
// workspace can retry (teardown is idempotent, so a retry converges).
func (s *DemoService) removeWorker(callerID core.UserID, seed *DemoSeed) {
	ctx, cancel := context.WithTimeout(context.Background(), demoLoadTimeout)
	defer cancel()

	if err := s.teardown(ctx, callerID, seed); err != nil {
		if s.Logger != nil {
			s.Logger.Error("demo removal failed; will allow retry", "customer", seed.CustomerSlug, "err", err)
		}
		seed.Status = DemoStatusReady
		if serr := s.Seeds.SaveDemoSeed(context.Background(), seed); serr != nil && s.Logger != nil {
			s.Logger.Error("demo removal: restore seed status", "customer", seed.CustomerSlug, "err", serr)
		}
		return
	}
	if err := s.Seeds.DeleteDemoSeed(context.Background(), seed.CustomerSlug); err != nil && s.Logger != nil {
		s.Logger.Error("demo removal: delete seed", "customer", seed.CustomerSlug, "err", err)
	}
}

// teardown deletes the artifacts recorded in seed, best-effort per artifact.
// It returns the first hard error (so a caller can keep the seed row and
// retry) after attempting every deletion. Used both by RemoveDemoProject and
// by LoadDemoProject's rollback (where the returned error is ignored).
func (s *DemoService) teardown(ctx context.Context, callerID core.UserID, seed *DemoSeed) error {
	var firstErr error
	fail := func(what string, err error) {
		// Already-gone is success — teardown is idempotent, so a retry (or a
		// seed pointing at artifacts a prior partial teardown deleted) converges
		// instead of wedging on "not found" and never clearing the seed.
		if err == nil || errors.Is(err, ErrPipelineNotFound) || errors.Is(err, ErrTransformationNotFound) {
			return
		}
		if s.Logger != nil {
			s.Logger.WarnContext(ctx, "demo teardown step failed", "step", what, "customer", seed.CustomerSlug, "err", err)
		}
		if firstErr == nil {
			firstErr = err
		}
	}

	if seed.TransformationID != "" {
		fail("delete transformation", s.Transformations.DeleteTransformation(ctx, callerID, seed.TransformationID))
	}
	if seed.TripsPipelineID != "" {
		fail("delete trips pipeline", s.Pipelines.DeletePipeline(ctx, callerID, seed.TripsPipelineID))
	}
	if seed.ZonesPipelineID != "" {
		fail("delete zones pipeline", s.Pipelines.DeletePipeline(ctx, callerID, seed.ZonesPipelineID))
	}
	for _, f := range seed.RepoFiles {
		fail("delete "+f.Repo+"/"+f.Path, s.deleteRepoFile(ctx, callerID, f))
	}
	return firstErr
}

// deleteRepoFile prunes one seed-committed file. A file already gone (no sha)
// is a no-op success — teardown must be idempotent.
func (s *DemoService) deleteRepoFile(ctx context.Context, callerID core.UserID, f DemoRepoFile) error {
	_, sha, err := s.BoxRepo.GetFile(ctx, callerID, f.Repo, f.Path)
	if err != nil || sha == "" {
		return nil // already absent (or unreadable) — nothing to prune
	}
	return s.BoxRepo.DeleteFile(ctx, callerID, f.Repo, f.Path, sha, "Remove NYC Taxi Pulse demo project")
}
