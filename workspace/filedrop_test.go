package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// --- Mocks ---

type mockObjectStore struct {
	putFn    func(ctx context.Context, cfg core.S3Config, key string, size int64, body io.Reader) error
	deleteFn func(ctx context.Context, cfg core.S3Config, key string) error
	headFn   func(ctx context.Context, cfg core.S3Config, key string) (bool, error)
}

func (m *mockObjectStore) Put(ctx context.Context, cfg core.S3Config, key string, size int64, body io.Reader) error {
	if m.putFn == nil {
		return nil
	}
	return m.putFn(ctx, cfg, key, size, body)
}

func (m *mockObjectStore) Delete(ctx context.Context, cfg core.S3Config, key string) error {
	if m.deleteFn == nil {
		return nil
	}
	return m.deleteFn(ctx, cfg, key)
}

func (m *mockObjectStore) Head(ctx context.Context, cfg core.S3Config, key string) (bool, error) {
	if m.headFn == nil {
		return true, nil // default: every recorded object exists
	}
	return m.headFn(ctx, cfg, key)
}

func provisionedCustomer(slug string) *workspace.Workspace {
	c := &workspace.Workspace{Slug: slug}
	c.EffectiveS3 = core.S3Config{
		Bucket:          "ft-" + slug,
		Endpoint:        "https://acct.r2.cloudflarestorage.com",
		Region:          "auto",
		AccessKeyID:     "AKIA",
		SecretAccessKey: "SECRET",
	}
	return c
}

// fileDropFixture wires a FileDropService around one pipeline owned by
// "acme". The service mutates the pipeline in place (UpdatePipeline receives
// the same pointer), so tests assert on pipeline.SourceConfig afterwards.
func fileDropFixture(pipeline *workspace.Pipeline, store *mockObjectStore) *workspace.FileDropService {
	return &workspace.FileDropService{
		Workspaces: &mockCustomerReader{
			getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
				return provisionedCustomer("acme"), nil
			},
		},
		Pipelines: &mockPipelineRepo{
			getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
				return pipeline, nil
			},
			updatePipelineFn: func(context.Context, *workspace.Pipeline) error {
				return nil
			},
		},
		Store: store,
	}
}

// --- Table name + filename validation ---

func TestUploadTableName(t *testing.T) {
	cases := map[string]string{
		"orders.csv":            "orders",
		"Daily Orders-2026.csv": "daily_orders_2026",
		"events.jsonl":          "events",
		"data.parquet":          "data",
		"2026-report.csv":       "t_2026_report",
	}
	for in, want := range cases {
		if got := workspace.UploadTableName(in); got != want {
			t.Errorf("UploadTableName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateUploadFilename(t *testing.T) {
	valid := []string{"orders.csv", "a.parquet", "report 2026.tsv", "x.jsonl", "d.ndjson"}
	for _, name := range valid {
		if err := workspace.ValidateUploadFilename(name); err != nil {
			t.Errorf("ValidateUploadFilename(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{
		"",                                // empty
		"orders.xlsx",                     // unsupported type
		"orders.csv.gz",                   // compressed (not supported yet)
		"orders",                          // no extension
		".csv",                            // empty stem
		"../escape.csv",                   // path traversal
		"a/b.csv",                         // path separator
		".hidden.csv",                     // leading dot
		strings.Repeat("a", 250) + ".csv", // too long
	}
	for _, name := range invalid {
		if err := workspace.ValidateUploadFilename(name); err == nil {
			t.Errorf("ValidateUploadFilename(%q) = nil, want error", name)
		}
	}
}

// --- Schema validation ---

func TestFileUploadSourceValidation(t *testing.T) {
	if err := workspace.ValidateSourceConfig("file_upload", nil); err != nil {
		t.Errorf("empty config: %v, want nil", err)
	}
	ok := json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv"}]}`)
	if err := workspace.ValidateSourceConfig("file_upload", ok); err != nil {
		t.Errorf("valid config: %v, want nil", err)
	}
	bad := json.RawMessage(`{"files":[{"name":"","file":"orders.csv"}]}`)
	if err := workspace.ValidateSourceConfig("file_upload", bad); err == nil {
		t.Error("missing table name accepted")
	}
	badFile := json.RawMessage(`{"files":[{"name":"x","file":"../../etc/passwd"}]}`)
	if err := workspace.ValidateSourceConfig("file_upload", badFile); err == nil {
		t.Error("path-traversal filename accepted")
	}

	if err := workspace.ValidateSourceCredentials("file_upload", nil, nil); err != nil {
		t.Errorf("empty credentials: %v, want nil", err)
	}
	if err := workspace.ValidateSourceCredentials("file_upload", nil, json.RawMessage(`{"access_key_id":"x"}`)); err == nil {
		t.Error("customer-supplied credentials accepted")
	}
}

// --- Upload / Delete ---

func TestFileDropUpload(t *testing.T) {
	t.Run("streams to the pipeline prefix and records the file", func(t *testing.T) {
		var putKey string
		var putBody string
		pipeline := &workspace.Pipeline{
			ID:           "pid-1",
			CustomerSlug: "acme",
			SourceType:   workspace.SourceTypeFileUpload,
			SourceConfig: json.RawMessage(`{}`),
		}
		store := &mockObjectStore{
			putFn: func(_ context.Context, cfg core.S3Config, key string, size int64, body io.Reader) error {
				putKey = key
				b, _ := io.ReadAll(body)
				putBody = string(b)
				if cfg.Bucket != "ft-acme" {
					t.Errorf("bucket = %q", cfg.Bucket)
				}
				return nil
			},
		}
		svc := fileDropFixture(pipeline, store)

		file, err := svc.Upload(context.Background(), "u1", "pid-1", "Orders 2026.csv", 8, strings.NewReader("a,b\n1,2\n"))
		if err != nil {
			t.Fatalf("Upload: %v", err)
		}
		if putKey != "uploads/pid-1/Orders 2026.csv" {
			t.Errorf("key = %q", putKey)
		}
		if putBody != "a,b\n1,2\n" {
			t.Errorf("body = %q", putBody)
		}
		if file.Name != "orders_2026" {
			t.Errorf("table name = %q", file.Name)
		}

		var cfg struct {
			Files []workspace.UploadedFile `json:"files"`
		}
		if err := json.Unmarshal(pipeline.SourceConfig, &cfg); err != nil {
			t.Fatalf("parse updated config: %v", err)
		}
		if len(cfg.Files) != 1 || cfg.Files[0].File != "Orders 2026.csv" || cfg.Files[0].SizeBytes != 8 {
			t.Errorf("recorded files = %+v", cfg.Files)
		}
	})

	t.Run("re-upload replaces the config entry", func(t *testing.T) {
		pipeline := &workspace.Pipeline{
			ID:           "pid-1",
			CustomerSlug: "acme",
			SourceType:   workspace.SourceTypeFileUpload,
			SourceConfig: json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv","size_bytes":5}]}`),
		}
		svc := fileDropFixture(pipeline, &mockObjectStore{})

		if _, err := svc.Upload(context.Background(), "u1", "pid-1", "orders.csv", 9, strings.NewReader("refreshed")); err != nil {
			t.Fatalf("Upload: %v", err)
		}
		var cfg struct {
			Files []workspace.UploadedFile `json:"files"`
		}
		_ = json.Unmarshal(pipeline.SourceConfig, &cfg)
		if len(cfg.Files) != 1 || cfg.Files[0].SizeBytes != 9 {
			t.Errorf("files after re-upload = %+v", cfg.Files)
		}
	})

	t.Run("rejects wrong source type", func(t *testing.T) {
		pipeline := &workspace.Pipeline{ID: "pid-1", CustomerSlug: "acme", SourceType: "rest_api"}
		svc := fileDropFixture(pipeline, &mockObjectStore{})
		_, err := svc.Upload(context.Background(), "u1", "pid-1", "a.csv", 1, strings.NewReader("x"))
		if !errors.Is(err, workspace.ErrNotFileUploadPipeline) {
			t.Fatalf("err = %v, want ErrNotFileUploadPipeline", err)
		}
	})

	t.Run("rejects foreign pipeline as not found", func(t *testing.T) {
		pipeline := &workspace.Pipeline{ID: "pid-1", CustomerSlug: "someone-else", SourceType: workspace.SourceTypeFileUpload}
		svc := fileDropFixture(pipeline, &mockObjectStore{})
		_, err := svc.Upload(context.Background(), "u1", "pid-1", "a.csv", 1, strings.NewReader("x"))
		if !errors.Is(err, workspace.ErrPipelineNotFound) {
			t.Fatalf("err = %v, want ErrPipelineNotFound", err)
		}
	})

	t.Run("rejects oversized upload", func(t *testing.T) {
		pipeline := &workspace.Pipeline{ID: "pid-1", CustomerSlug: "acme", SourceType: workspace.SourceTypeFileUpload}
		svc := fileDropFixture(pipeline, &mockObjectStore{})
		svc.MaxBytes = 10
		_, err := svc.Upload(context.Background(), "u1", "pid-1", "a.csv", 11, strings.NewReader("x"))
		var invalid *workspace.ErrInvalidUploadFile
		if !errors.As(err, &invalid) {
			t.Fatalf("err = %v, want ErrInvalidUploadFile", err)
		}
	})

	t.Run("rejects unprovisioned customer", func(t *testing.T) {
		pipeline := &workspace.Pipeline{ID: "pid-1", CustomerSlug: "acme", SourceType: workspace.SourceTypeFileUpload}
		svc := &workspace.FileDropService{
			Workspaces: &mockCustomerReader{
				getByUserIDFn: func(context.Context, core.UserID) (*workspace.Workspace, error) {
					return &workspace.Workspace{Slug: "acme"}, nil // no EffectiveS3
				},
			},
			Pipelines: &mockPipelineRepo{
				getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
					return pipeline, nil
				},
			},
			Store: &mockObjectStore{},
		}
		_, err := svc.Upload(context.Background(), "u1", "pid-1", "a.csv", 1, strings.NewReader("x"))
		if !errors.Is(err, workspace.ErrCustomerNotProvisioned) {
			t.Fatalf("err = %v, want ErrCustomerNotProvisioned", err)
		}
	})
}

func TestFileDropDelete(t *testing.T) {
	var deletedKey string
	pipeline := &workspace.Pipeline{
		ID:           "pid-1",
		CustomerSlug: "acme",
		SourceType:   workspace.SourceTypeFileUpload,
		SourceConfig: json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv"},{"name":"users","file":"users.parquet"}]}`),
	}
	store := &mockObjectStore{
		deleteFn: func(_ context.Context, _ core.S3Config, key string) error {
			deletedKey = key
			return nil
		},
	}
	svc := fileDropFixture(pipeline, store)

	if err := svc.Delete(context.Background(), "u1", "pid-1", "orders.csv"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deletedKey != "uploads/pid-1/orders.csv" {
		t.Errorf("deleted key = %q", deletedKey)
	}
	var cfg struct {
		Files []workspace.UploadedFile `json:"files"`
	}
	_ = json.Unmarshal(pipeline.SourceConfig, &cfg)
	if len(cfg.Files) != 1 || cfg.Files[0].File != "users.parquet" {
		t.Errorf("files after delete = %+v", cfg.Files)
	}
}

func TestFileDropListFlagsMissing(t *testing.T) {
	pipeline := &workspace.Pipeline{
		ID:           "pid-1",
		CustomerSlug: "acme",
		SourceType:   workspace.SourceTypeFileUpload,
		SourceConfig: json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv"},{"name":"gone","file":"gone.csv"}]}`),
	}

	t.Run("flags files whose object is absent", func(t *testing.T) {
		store := &mockObjectStore{
			headFn: func(_ context.Context, _ core.S3Config, key string) (bool, error) {
				return key == "uploads/pid-1/orders.csv", nil // gone.csv absent
			},
		}
		files, err := fileDropFixture(pipeline, store).List(context.Background(), "u1", "pid-1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(files) != 2 {
			t.Fatalf("files = %+v", files)
		}
		if files[0].Missing {
			t.Errorf("orders.csv flagged missing")
		}
		if !files[1].Missing {
			t.Errorf("gone.csv not flagged missing")
		}
		// Missing is transient: it must never be persisted into source_config.
		if strings.Contains(string(pipeline.SourceConfig), "missing") {
			t.Errorf("Missing leaked into stored config: %s", pipeline.SourceConfig)
		}
	})

	t.Run("does not cry wolf on a Head error", func(t *testing.T) {
		store := &mockObjectStore{
			headFn: func(context.Context, core.S3Config, string) (bool, error) {
				return false, errors.New("network blip")
			},
		}
		files, err := fileDropFixture(pipeline, store).List(context.Background(), "u1", "pid-1")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, f := range files {
			if f.Missing {
				t.Errorf("%s flagged missing on Head error", f.File)
			}
		}
	})
}

// --- Worker-facing rewrite ---

func TestGetEnabledPipelinesRewritesFileUpload(t *testing.T) {
	fileUpload := workspace.Pipeline{
		ID:           "pid-1",
		CustomerSlug: "acme",
		SourceType:   workspace.SourceTypeFileUpload,
		SourceConfig: json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv"},{"name":"events","file":"events.jsonl"}]}`),
	}
	empty := workspace.Pipeline{
		ID:           "pid-2",
		CustomerSlug: "acme",
		SourceType:   workspace.SourceTypeFileUpload,
		SourceConfig: json.RawMessage(`{}`),
	}
	rest := workspace.Pipeline{
		ID:           "pid-3",
		CustomerSlug: "acme",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://x"}`),
	}

	newSvc := func(ws *workspace.Workspace, customerErr error) *workspace.PipelineService {
		return &workspace.PipelineService{
			Workspaces: &mockCustomerReader{
				getBySlugFn: func(context.Context, string) (*workspace.Workspace, error) {
					return ws, customerErr
				},
			},
			Pipelines: &mockPipelineRepo{
				getEnabledPipelinesFn: func(context.Context, string) ([]workspace.Pipeline, error) {
					return []workspace.Pipeline{fileUpload, empty, rest}, nil
				},
			},
		}
	}

	t.Run("provisioned customer gets filesystem rewrite", func(t *testing.T) {
		out, err := newSvc(provisionedCustomer("acme"), nil).GetEnabledPipelines(context.Background(), "acme")
		if err != nil {
			t.Fatalf("GetEnabledPipelines: %v", err)
		}
		// The empty file_upload pipeline is omitted; rest_api passes through.
		if len(out) != 2 {
			t.Fatalf("got %d pipelines, want 2: %+v", len(out), out)
		}
		p := out[0]
		if p.SourceType != "filesystem" {
			t.Fatalf("source type = %q, want filesystem", p.SourceType)
		}
		var cfg struct {
			BucketURL string `json:"bucket_url"`
			Tables    []struct {
				Name     string `json:"name"`
				FileGlob string `json:"file_glob"`
			} `json:"tables"`
		}
		if err := json.Unmarshal(p.SourceConfig, &cfg); err != nil {
			t.Fatalf("parse rewritten config: %v", err)
		}
		if cfg.BucketURL != "s3://ft-acme/uploads/pid-1/" {
			t.Errorf("bucket_url = %q", cfg.BucketURL)
		}
		if len(cfg.Tables) != 2 || cfg.Tables[0].Name != "orders" || cfg.Tables[0].FileGlob != "orders.csv" {
			t.Errorf("tables = %+v", cfg.Tables)
		}
		var creds struct {
			AccessKeyID     string `json:"access_key_id"`
			SecretAccessKey string `json:"secret_access_key"`
			EndpointURL     string `json:"endpoint_url"`
			Region          string `json:"region"`
		}
		if err := json.Unmarshal(p.SourceCredentials, &creds); err != nil {
			t.Fatalf("parse rewritten credentials: %v", err)
		}
		if creds.AccessKeyID != "AKIA" || creds.SecretAccessKey != "SECRET" || creds.Region != "auto" {
			t.Errorf("credentials = %+v", creds)
		}
		if out[1].SourceType != "rest_api" {
			t.Errorf("passthrough pipeline = %+v", out[1])
		}
	})

	t.Run("unprovisioned customer file_upload pipelines are omitted", func(t *testing.T) {
		out, err := newSvc(&workspace.Workspace{Slug: "acme"}, nil).GetEnabledPipelines(context.Background(), "acme")
		if err != nil {
			t.Fatalf("GetEnabledPipelines: %v", err)
		}
		if len(out) != 1 || out[0].SourceType != "rest_api" {
			t.Fatalf("got %+v, want only the rest_api pipeline", out)
		}
	})
}

// The pipelines-as-files Phase 3 kill-switch strips row-backed credentials
// from the worker poll (the worker decrypts them from its checkout) but
// must NOT touch the storage credentials injected into synthesized
// file_upload pipelines — those are never rendered as .age files.
func TestGetEnabledPipelines_StripPollCredentials(t *testing.T) {
	fileUpload := workspace.Pipeline{
		ID:           "pid-1",
		CustomerSlug: "acme",
		SourceType:   workspace.SourceTypeFileUpload,
		SourceConfig: json.RawMessage(`{"files":[{"name":"orders","file":"orders.csv"}]}`),
	}
	rest := workspace.Pipeline{
		ID:                "pid-2",
		CustomerSlug:      "acme",
		SourceType:        "rest_api",
		SourceConfig:      json.RawMessage(`{"base_url":"https://x"}`),
		SourceCredentials: json.RawMessage(`{"api_key":"row-backed-secret"}`),
	}
	svc := &workspace.PipelineService{
		Workspaces: &mockCustomerReader{
			getBySlugFn: func(context.Context, string) (*workspace.Workspace, error) {
				return provisionedCustomer("acme"), nil
			},
		},
		Pipelines: &mockPipelineRepo{
			getEnabledPipelinesFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return []workspace.Pipeline{fileUpload, rest}, nil
			},
		},
		StripPollCredentials: true,
	}

	out, err := svc.GetEnabledPipelines(context.Background(), "acme")
	if err != nil {
		t.Fatalf("GetEnabledPipelines: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d pipelines, want 2: %+v", len(out), out)
	}
	if fs := out[0]; fs.SourceType != "filesystem" || len(fs.SourceCredentials) == 0 {
		t.Fatalf("file_upload storage credentials must survive the strip: %+v", fs)
	}
	if got := out[1].SourceCredentials; len(got) != 0 {
		t.Fatalf("row-backed credentials must be stripped, got %s", got)
	}
}
