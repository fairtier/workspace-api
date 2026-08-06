package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/demo"
	"github.com/fairtier/workspace-api/workspace"
)

// --- Fakes for the demo ops interfaces ---

type fakeDemoPipelines struct {
	created   []*workspace.Pipeline
	triggered []workspace.PipelineID
	deleted   []workspace.PipelineID
	createErr error
}

func (f *fakeDemoPipelines) CreatePipeline(_ context.Context, _ core.UserID, p *workspace.Pipeline) (*workspace.Pipeline, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	// Both demo pipelines are now "filesystem", so key the id by creation
	// order rather than source type to keep trips and zones distinct.
	p.ID = workspace.PipelineID(fmt.Sprintf("pipe-%d", len(f.created)+1))
	f.created = append(f.created, p)
	return p, nil
}

func (f *fakeDemoPipelines) TriggerPipeline(_ context.Context, _ core.UserID, id workspace.PipelineID) (*workspace.PipelineRun, error) {
	f.triggered = append(f.triggered, id)
	return &workspace.PipelineRun{PipelineID: id, Status: "pending"}, nil
}

func (f *fakeDemoPipelines) DeletePipeline(_ context.Context, _ core.UserID, id workspace.PipelineID) error {
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeDemoTransformations struct {
	created   []*workspace.Transformation
	deleted   []workspace.TransformationID
	createErr error
	deleteErr error
}

func (f *fakeDemoTransformations) CreateTransformation(_ context.Context, _ core.UserID, t *workspace.Transformation) (*workspace.Transformation, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	t.ID = "transformation-id"
	f.created = append(f.created, t)
	return t, nil
}

func (f *fakeDemoTransformations) DeleteTransformation(_ context.Context, _ core.UserID, id workspace.TransformationID) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeDemoBoxRepo struct {
	put     []string          // "repo/path"
	deleted []string          // "repo/path"
	present map[string]string // "repo/path" -> sha (for get)
}

func (f *fakeDemoBoxRepo) GetFile(_ context.Context, _ core.UserID, repo, filePath string) (string, string, error) {
	if sha, ok := f.present[repo+"/"+filePath]; ok {
		return "content", sha, nil
	}
	return "", "", errors.New("not found")
}

func (f *fakeDemoBoxRepo) PutFile(_ context.Context, _ core.UserID, repo, filePath, _, _, _ string) (string, error) {
	f.put = append(f.put, repo+"/"+filePath)
	if f.present == nil {
		f.present = map[string]string{}
	}
	f.present[repo+"/"+filePath] = "newsha" // now readable (so teardown can prune it)
	return "newsha", nil
}

func (f *fakeDemoBoxRepo) DeleteFile(_ context.Context, _ core.UserID, repo, filePath, _, _ string) error {
	f.deleted = append(f.deleted, repo+"/"+filePath)
	return nil
}

type fakeDemoSeedStore struct {
	seed *workspace.DemoSeed
}

func (f *fakeDemoSeedStore) SaveDemoSeed(_ context.Context, seed *workspace.DemoSeed) error {
	f.seed = seed
	return nil
}

func (f *fakeDemoSeedStore) GetDemoSeed(_ context.Context, _ string) (*workspace.DemoSeed, error) {
	if f.seed == nil {
		return nil, workspace.ErrDemoNotLoaded
	}
	return f.seed, nil
}

func (f *fakeDemoSeedStore) DeleteDemoSeed(_ context.Context, _ string) error {
	f.seed = nil
	return nil
}

// --- Helpers ---

func vmCustomerReader() *mockCustomerReader {
	return &mockCustomerReader{
		getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
			return &workspace.Workspace{
				Slug: "acme",
				OnVM: true,
			}, nil
		},
	}
}

func configuredBucket() workspace.DemoBucket {
	return workspace.DemoBucket{
		AccessKeyID:     "ak",
		SecretAccessKey: "sk",
		Endpoint:        "https://acct.r2.cloudflarestorage.com",
		Bucket:          "fairtier-demo-datasets",
		Region:          "auto",
	}
}

func newDemoService(pipes *fakeDemoPipelines, tf *fakeDemoTransformations, repo *fakeDemoBoxRepo, seeds *fakeDemoSeedStore) *workspace.DemoService {
	return &workspace.DemoService{
		Workspaces:      vmCustomerReader(),
		Seeds:           seeds,
		Pipelines:       pipes,
		Transformations: tf,
		BoxRepo:         repo,
		Bucket:          configuredBucket(),
		// Run the background load worker synchronously so tests observe the
		// finished state without waiting on a goroutine.
		Runner: func(fn func()) { fn() },
		Logger: slog.Default(),
	}
}

// --- Tests ---

func TestDemoService_LoadDemoProject_HappyPath(t *testing.T) {
	pipes := &fakeDemoPipelines{}
	tf := &fakeDemoTransformations{}
	repo := &fakeDemoBoxRepo{}
	seeds := &fakeDemoSeedStore{}
	svc := newDemoService(pipes, tf, repo, seeds)

	status, err := svc.LoadDemoProject(context.Background(), "user-1", "")
	if err != nil {
		t.Fatalf("LoadDemoProject() error = %v", err)
	}

	// Two filesystem pipelines, both reading the shared demo bucket: trips + zones.
	if len(pipes.created) != 2 {
		t.Fatalf("created %d pipelines, want 2", len(pipes.created))
	}
	trips, zones := pipes.created[0], pipes.created[1]
	if trips.SourceType != "filesystem" {
		t.Errorf("first pipeline source_type = %q, want filesystem", trips.SourceType)
	}
	if !strings.Contains(string(trips.SourceConfig), "fairtier-demo-datasets/nyc-taxi/") {
		t.Errorf("trips bucket_url not set: %s", trips.SourceConfig)
	}
	if !strings.Contains(string(trips.SourceConfig), "yellow_tripdata_sample.parquet") {
		t.Errorf("trips glob should be the sample (default) tier: %s", trips.SourceConfig)
	}
	if !strings.Contains(string(trips.SourceCredentials), "\"access_key_id\":\"ak\"") {
		t.Errorf("trips credentials not injected: %s", trips.SourceCredentials)
	}

	// Zones is a second filesystem source over the same demo bucket, reading
	// the mirrored zone lookup into the taxi_zones table (no file drop).
	if zones.SourceType != "filesystem" {
		t.Errorf("zones source_type = %q, want filesystem", zones.SourceType)
	}
	if !strings.Contains(string(zones.SourceConfig), "taxi_zone_lookup.csv") {
		t.Errorf("zones glob not set: %s", zones.SourceConfig)
	}
	if !strings.Contains(string(zones.SourceConfig), "\"name\":\"taxi_zones\"") {
		t.Errorf("zones table should be taxi_zones: %s", zones.SourceConfig)
	}
	if !strings.Contains(string(zones.SourceCredentials), "\"access_key_id\":\"ak\"") {
		t.Errorf("zones credentials not injected: %s", zones.SourceCredentials)
	}

	// Repo files committed to both repos.
	var nTransform, nRill int
	for _, p := range repo.put {
		switch {
		case strings.HasPrefix(p, "transformations/"):
			nTransform++
		case strings.HasPrefix(p, "rill/"):
			nRill++
		default:
			t.Errorf("unexpected repo file %q", p)
		}
	}
	if nTransform == 0 || nRill == 0 {
		t.Errorf("expected files in both repos, got transformations=%d rill=%d", nTransform, nRill)
	}

	// Transformation chained to the trips pipeline.
	if len(tf.created) != 1 {
		t.Fatalf("created %d transformations, want 1", len(tf.created))
	}
	if tf.created[0].TriggerAfterPipelineID != trips.ID {
		t.Errorf("transformation trigger_after = %q, want %q", tf.created[0].TriggerAfterPipelineID, trips.ID)
	}

	// Both pipelines triggered.
	if len(pipes.triggered) != 2 {
		t.Errorf("triggered %d pipelines, want 2", len(pipes.triggered))
	}

	// Seed persisted as ready with the default (sample) tier (Runner ran the
	// worker synchronously).
	if seeds.seed == nil {
		t.Fatal("seed not saved")
	}
	if seeds.seed.Status != "ready" {
		t.Errorf("seed status = %q, want ready", seeds.seed.Status)
	}
	if seeds.seed.Tier != "sample" {
		t.Errorf("seed tier = %q, want sample", seeds.seed.Tier)
	}
	if len(seeds.seed.RepoFiles) != len(repo.put) {
		t.Errorf("seed recorded %d repo files, committed %d", len(seeds.seed.RepoFiles), len(repo.put))
	}
	// LoadDemoProject returns immediately with a loading status; the Console
	// polls GetDemoStatus for completion.
	if !status.Loading || status.Tier != "sample" {
		t.Errorf("status = %+v, want loading sample", status)
	}
}

func TestDemoService_LoadDemoProject_AlreadyLoaded(t *testing.T) {
	seeds := &fakeDemoSeedStore{seed: &workspace.DemoSeed{CustomerSlug: "acme", Tier: "minimal", Status: workspace.DemoStatusReady}}
	svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, seeds)

	if _, err := svc.LoadDemoProject(context.Background(), "user-1", ""); !errors.Is(err, workspace.ErrDemoAlreadyLoaded) {
		t.Fatalf("error = %v, want ErrDemoAlreadyLoaded", err)
	}
}

func TestDemoService_LoadDemoProject_NotConfigured(t *testing.T) {
	svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
	svc.Bucket = workspace.DemoBucket{} // clear

	if _, err := svc.LoadDemoProject(context.Background(), "user-1", ""); !errors.Is(err, workspace.ErrDemoNotConfigured) {
		t.Fatalf("error = %v, want ErrDemoNotConfigured", err)
	}
}

func TestDemoService_LoadDemoProject_UnwindsOnFailure(t *testing.T) {
	pipes := &fakeDemoPipelines{}
	repo := &fakeDemoBoxRepo{}
	tf := &fakeDemoTransformations{createErr: errors.New("boom")}
	seeds := &fakeDemoSeedStore{}
	svc := newDemoService(pipes, tf, repo, seeds)

	// LoadDemoProject returns immediately (nil error); the worker (synchronous
	// here) fails at createTransformation and rolls everything back.
	if _, err := svc.LoadDemoProject(context.Background(), "user-1", ""); err != nil {
		t.Fatalf("LoadDemoProject() error = %v", err)
	}
	// Rollback: both pipelines deleted, all committed files deleted, seed cleared.
	if len(pipes.deleted) != 2 {
		t.Errorf("deleted %d pipelines on unwind, want 2", len(pipes.deleted))
	}
	if len(repo.deleted) != len(repo.put) {
		t.Errorf("unwind deleted %d repo files, committed %d", len(repo.deleted), len(repo.put))
	}
	if seeds.seed != nil {
		t.Error("seed row should be cleared after a failed load")
	}
}

func TestDemoService_RemoveDemoProject(t *testing.T) {
	seed := &workspace.DemoSeed{
		CustomerSlug:     "acme",
		Tier:             "minimal",
		TripsPipelineID:  "trips-id",
		ZonesPipelineID:  "zones-id",
		TransformationID: "transformation-id",
		RepoFiles: []workspace.DemoRepoFile{
			{Repo: "transformations", Path: "models/sources.yml"},
			{Repo: "rill", Path: "models/daily_zone.sql"},
		},
	}
	pipes := &fakeDemoPipelines{}
	tf := &fakeDemoTransformations{}
	repo := &fakeDemoBoxRepo{present: map[string]string{
		"transformations/models/sources.yml": "sha1",
		"rill/models/daily_zone.sql":         "sha2",
	}}
	seeds := &fakeDemoSeedStore{seed: seed}
	svc := newDemoService(pipes, tf, repo, seeds)

	if err := svc.RemoveDemoProject(context.Background(), "user-1"); err != nil {
		t.Fatalf("RemoveDemoProject() error = %v", err)
	}
	if len(pipes.deleted) != 2 {
		t.Errorf("deleted %d pipelines, want 2", len(pipes.deleted))
	}
	if len(tf.deleted) != 1 {
		t.Errorf("deleted %d transformations, want 1", len(tf.deleted))
	}
	if len(repo.deleted) != 2 {
		t.Errorf("deleted %d repo files, want 2", len(repo.deleted))
	}
	if seeds.seed != nil {
		t.Error("seed record should be deleted")
	}
}

func TestDemoService_RemoveDemoProject_NotLoaded(t *testing.T) {
	svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
	if err := svc.RemoveDemoProject(context.Background(), "user-1"); !errors.Is(err, workspace.ErrDemoNotLoaded) {
		t.Fatalf("error = %v, want ErrDemoNotLoaded", err)
	}
}

// Teardown must treat already-gone artifacts as success — a seed pointing at a
// transformation a prior partial teardown deleted must still clear the seed,
// not wedge on "not found".
func TestDemoService_RemoveDemoProject_IdempotentOnMissingArtifacts(t *testing.T) {
	seed := &workspace.DemoSeed{
		CustomerSlug:     "acme",
		TripsPipelineID:  "trips-id",
		TransformationID: "transformation-id",
	}
	tf := &fakeDemoTransformations{deleteErr: workspace.ErrTransformationNotFound}
	pipes := &fakeDemoPipelines{}
	seeds := &fakeDemoSeedStore{seed: seed}
	svc := newDemoService(pipes, tf, &fakeDemoBoxRepo{}, seeds)

	if err := svc.RemoveDemoProject(context.Background(), "user-1"); err != nil {
		t.Fatalf("RemoveDemoProject() error = %v", err)
	}
	if seeds.seed != nil {
		t.Error("seed must be cleared even when an artifact was already gone")
	}
}

func TestDemoService_GetDemoStatus(t *testing.T) {
	t.Run("available and not loaded", func(t *testing.T) {
		svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
		status, err := svc.GetDemoStatus(context.Background(), "user-1")
		if err != nil {
			t.Fatalf("GetDemoStatus() error = %v", err)
		}
		if !status.Available || status.Loaded {
			t.Errorf("status = %+v, want available && !loaded", status)
		}
	})

	t.Run("unavailable when bucket unset", func(t *testing.T) {
		svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
		svc.Bucket = workspace.DemoBucket{}
		status, _ := svc.GetDemoStatus(context.Background(), "user-1")
		if status.Available {
			t.Error("status.Available should be false with no bucket")
		}
	})

	t.Run("loaded reflects seed", func(t *testing.T) {
		seeds := &fakeDemoSeedStore{seed: &workspace.DemoSeed{Tier: "default", Status: workspace.DemoStatusReady, TripsPipelineID: "t", TransformationID: "x"}}
		svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, seeds)
		status, _ := svc.GetDemoStatus(context.Background(), "user-1")
		if !status.Loaded || status.Tier != "default" || status.TripsPipelineID != "t" {
			t.Errorf("status = %+v, want loaded default", status)
		}
	})

	t.Run("loading reflects in-progress seed", func(t *testing.T) {
		seeds := &fakeDemoSeedStore{seed: &workspace.DemoSeed{Tier: "minimal", Status: workspace.DemoStatusLoading, CreatedAt: time.Now()}}
		svc := newDemoService(&fakeDemoPipelines{}, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, seeds)
		status, _ := svc.GetDemoStatus(context.Background(), "user-1")
		if status.Loaded || !status.Loading {
			t.Errorf("status = %+v, want loading not loaded", status)
		}
	})
}

// The demo bucket is public-domain data, and a deployment that has to be
// handed a credential to read it is one that cannot seed the demo alone.
// Over a public origin the pipelines must therefore carry NO credential —
// and must name their files, because a public object store serves by key and
// will not list a directory, so a glob there matches nothing and loads zero
// rows without failing.
func TestDemoService_PublicBucketNeedsNoCredential(t *testing.T) {
	pipes := &fakeDemoPipelines{}
	svc := newDemoService(pipes, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
	svc.Bucket = workspace.DemoBucket{PublicBaseURL: "https://demo-data.example.com/"}

	if _, err := svc.LoadDemoProject(context.Background(), "user-1", "sample"); err != nil {
		t.Fatalf("LoadDemoProject() error = %v", err)
	}
	if len(pipes.created) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(pipes.created))
	}

	for _, p := range pipes.created {
		var creds map[string]any
		if err := json.Unmarshal(p.SourceCredentials, &creds); err != nil {
			t.Fatalf("%s: credentials are not JSON: %v", p.Name, err)
		}
		if len(creds) != 0 {
			t.Errorf("%s: expected no credentials over a public origin, got %v", p.Name, creds)
		}

		var cfg struct {
			BucketURL string `json:"bucket_url"`
			Tables    []struct {
				Name     string   `json:"name"`
				FileGlob string   `json:"file_glob"`
				Files    []string `json:"files"`
			} `json:"tables"`
		}
		if err := json.Unmarshal(p.SourceConfig, &cfg); err != nil {
			t.Fatalf("%s: config is not JSON: %v", p.Name, err)
		}
		// No trailing slash doubled into the object URLs.
		if cfg.BucketURL != "https://demo-data.example.com" {
			t.Errorf("%s: bucket_url = %q", p.Name, cfg.BucketURL)
		}
		if len(cfg.Tables) != 1 {
			t.Fatalf("%s: expected 1 table, got %d", p.Name, len(cfg.Tables))
		}
		table := cfg.Tables[0]
		if table.FileGlob != "" {
			t.Errorf("%s: a public origin cannot be globbed, got file_glob %q", p.Name, table.FileGlob)
		}
		if len(table.Files) == 0 {
			t.Fatalf("%s: expected explicit files", p.Name)
		}
		for _, f := range table.Files {
			if !strings.HasPrefix(f, "nyc-taxi/") {
				t.Errorf("%s: file %q is missing the bucket prefix", p.Name, f)
			}
		}
	}
}

// The S3 path is unchanged: a credential is still injected, and the same
// files are expressed as a glob, which S3 can list for.
func TestDemoService_S3BucketStillInjectsCredential(t *testing.T) {
	pipes := &fakeDemoPipelines{}
	svc := newDemoService(pipes, &fakeDemoTransformations{}, &fakeDemoBoxRepo{}, &fakeDemoSeedStore{})
	svc.Bucket = configuredBucket()

	if _, err := svc.LoadDemoProject(context.Background(), "user-1", "minimal"); err != nil {
		t.Fatalf("LoadDemoProject() error = %v", err)
	}

	trips := pipes.created[0]
	var creds map[string]any
	if err := json.Unmarshal(trips.SourceCredentials, &creds); err != nil {
		t.Fatalf("credentials are not JSON: %v", err)
	}
	if creds["access_key_id"] != "ak" {
		t.Errorf("expected the platform credential injected, got %v", creds)
	}

	var cfg struct {
		BucketURL string `json:"bucket_url"`
		Tables    []struct {
			FileGlob string `json:"file_glob"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(trips.SourceConfig, &cfg); err != nil {
		t.Fatalf("config is not JSON: %v", err)
	}
	if !strings.HasPrefix(cfg.BucketURL, "s3://") {
		t.Errorf("bucket_url = %q, want an s3:// bucket", cfg.BucketURL)
	}
	// The monthly files of the "minimal" tier, as one brace alternation. Read
	// off the tier rather than named here: the tiers are a rolling window that
	// moves as newer TLC months are mirrored, and a year hardcoded here only
	// tells us which year it was written in.
	glob := cfg.Tables[0].FileGlob
	if !strings.HasPrefix(glob, "{") || !strings.HasSuffix(glob, "}") {
		t.Fatalf("file_glob = %q, want a brace alternation", glob)
	}
	globbed := strings.Split(strings.Trim(glob, "{}"), ",")
	if want := demo.Tiers["minimal"].Files; !slices.Equal(globbed, want) {
		t.Errorf("file_glob = %v, want the minimal tier's files %v", globbed, want)
	}
}
