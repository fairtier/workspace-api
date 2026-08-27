package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// fakeSourceTests is an in-memory SourceTestStore with the two properties the
// real one gets from SQL: a claim is a state change, and every read is scoped
// to a customer.
type fakeSourceTests struct {
	tests map[string]*SourceTest
}

func (f *fakeSourceTests) CreateSourceTest(_ context.Context, t *SourceTest) error {
	if f.tests == nil {
		f.tests = map[string]*SourceTest{}
	}
	cp := *t
	f.tests[t.ID] = &cp
	return nil
}

func (f *fakeSourceTests) GetSourceTest(_ context.Context, id, slug string) (*SourceTest, error) {
	t, ok := f.tests[id]
	if !ok || t.CustomerSlug != slug {
		return nil, ErrSourceTestNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *fakeSourceTests) ClaimPendingSourceTests(_ context.Context, slug string) ([]SourceTest, error) {
	var out []SourceTest
	for _, t := range f.tests {
		if t.CustomerSlug == slug && t.Status == SourceTestPending {
			t.Status = SourceTestRunning
			out = append(out, *t)
		}
	}
	return out, nil
}

func (f *fakeSourceTests) CompleteSourceTest(_ context.Context, id, slug, status, message string, details []string) error {
	t, ok := f.tests[id]
	if !ok || t.CustomerSlug != slug {
		return ErrSourceTestNotFound
	}
	now := time.Now()
	t.Status, t.Message, t.Details, t.CompletedAt = status, message, details, &now
	t.SourceCredentials = nil
	return nil
}

func (f *fakeSourceTests) DeleteExpiredSourceTests(context.Context) (int64, error) { return 0, nil }

type fakeSourceTestPipelines struct {
	PipelineRepository
	pipeline *Pipeline
}

func (f *fakeSourceTestPipelines) GetPipeline(_ context.Context, id PipelineID) (*Pipeline, error) {
	if f.pipeline == nil || f.pipeline.ID != id {
		return nil, ErrPipelineNotFound
	}
	return f.pipeline, nil
}

func sourceTestService(store *fakeSourceTests) *SourceTestService {
	return &SourceTestService{
		Workspaces: &StaticResolver{Workspace: Workspace{Slug: "acme"}},
		Tests:      store,
	}
}

const duckConfig = `{"extension":"mysql","attach":"host=db.internal port=3306 user=readonly database=shop password={password}","tables":[{"name":"orders"}]}`

func TestStartSourceTestQueues(t *testing.T) {
	store := &fakeSourceTests{}
	svc := sourceTestService(store)

	got, err := svc.StartSourceTest(context.Background(), core.UserID("u1"), &SourceTest{
		SourceType:        SourceTypeDuckDB,
		SourceConfig:      json.RawMessage(duckConfig),
		SourceCredentials: json.RawMessage(`{"attach_params":{"password":"s3cret"}}`),
	})
	if err != nil {
		t.Fatalf("StartSourceTest() error = %v", err)
	}
	if got.Status != SourceTestPending {
		t.Errorf("status = %q, want pending", got.Status)
	}
	// Nothing that went in comes back out: the response is what the Console
	// polls with, and it has no business carrying a password.
	if len(got.SourceCredentials) != 0 {
		t.Errorf("the queued test returned its credentials: %s", got.SourceCredentials)
	}
	if store.tests[got.ID].CustomerSlug != "acme" {
		t.Errorf("stored slug = %q, want acme", store.tests[got.ID].CustomerSlug)
	}
}

func TestStartSourceTestRefusesUnsupportedType(t *testing.T) {
	// rest_api has no probe on the worker, so queueing one would produce a
	// test nothing ever claims — a spinner until it expires.
	svc := sourceTestService(&fakeSourceTests{})
	_, err := svc.StartSourceTest(context.Background(), core.UserID("u1"), &SourceTest{
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"x","endpoint":"/x"}]}`),
	})
	if !errors.Is(err, ErrSourceTestUnsupported) {
		t.Fatalf("error = %v, want ErrSourceTestUnsupported", err)
	}
}

func TestStartSourceTestValidatesTheConfig(t *testing.T) {
	// A malformed config is refused here, where the answer is immediate,
	// rather than by a probe on the box a poll interval later.
	svc := sourceTestService(&fakeSourceTests{})
	_, err := svc.StartSourceTest(context.Background(), core.UserID("u1"), &SourceTest{
		SourceType:   SourceTypeDuckDB,
		SourceConfig: json.RawMessage(`{"extension":"nope","tables":[{"name":"t","query":"SELECT 1"}]}`),
	})
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("error = %v, want the extension refusal", err)
	}
}

func TestGetSourceTestIsTenantScoped(t *testing.T) {
	store := &fakeSourceTests{tests: map[string]*SourceTest{
		"t1": {ID: "t1", CustomerSlug: "other", Status: SourceTestSuccess},
	}}
	svc := sourceTestService(store)
	if _, err := svc.GetSourceTest(context.Background(), core.UserID("u1"), "t1"); !errors.Is(err, ErrSourceTestNotFound) {
		t.Fatalf("error = %v, want ErrSourceTestNotFound", err)
	}
}

func TestClaimSourceTestsResolvesAGoogleConnection(t *testing.T) {
	// A Drive test must exercise the connection the pipeline references, not
	// a shape only the run path knows how to fill in.
	store := &fakeSourceTests{tests: map[string]*SourceTest{
		"t1": {
			ID:                "t1",
			CustomerSlug:      "acme",
			SourceType:        SourceTypeDuckDB,
			SourceConfig:      json.RawMessage(`{"extension":"gdrive","tables":[{"name":"t","query":"SELECT 1"}]}`),
			SourceCredentials: json.RawMessage(`{"oauth":{"connection_id":"c1"}}`),
			Status:            SourceTestPending,
		},
	}}
	svc := sourceTestService(store)
	svc.OAuthClients = &fakeOAuthClients{client: &OAuthClient{ClientID: "cid", ClientSecret: "sec"}}
	svc.Connections = &fakeConnectionStore{conns: map[string]*Connection{
		"c1": googleConn("c1", "acme", "rt-1", "user@example.com", "cid"),
	}}

	claimed, err := svc.ClaimSourceTests(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ClaimSourceTests() error = %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d tests, want 1", len(claimed))
	}
	got := string(claimed[0].SourceCredentials)
	for _, want := range []string{`"REFRESH_TOKEN":"rt-1"`, `"CLIENT_SECRET":"sec"`, `"type":"gdrive"`} {
		if !strings.Contains(got, want) {
			t.Errorf("claimed credential missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, `"oauth"`) {
		t.Errorf("the oauth member must not reach the worker: %s", got)
	}
	// Claimed once: a second poll must not probe the same source again.
	again, err := svc.ClaimSourceTests(context.Background(), "acme")
	if err != nil {
		t.Fatalf("second ClaimSourceTests() error = %v", err)
	}
	if len(again) != 0 {
		t.Errorf("a claimed test was handed out twice")
	}
}

func TestReportSourceTest(t *testing.T) {
	store := &fakeSourceTests{tests: map[string]*SourceTest{
		"t1": {ID: "t1", CustomerSlug: "acme", Status: SourceTestRunning},
	}}
	svc := sourceTestService(store)

	if err := svc.ReportSourceTest(context.Background(), "acme", "t1", "maybe", "", nil); err == nil {
		t.Error("an invented status was accepted")
	}
	if err := svc.ReportSourceTest(context.Background(), "other", "t1", SourceTestSuccess, "ok", nil); !errors.Is(err, ErrSourceTestNotFound) {
		t.Errorf("a foreign tenant completed the test: %v", err)
	}
	details := make([]string, SourceTestMaxDetails+10)
	for i := range details {
		details[i] = "orders: 8 columns"
	}
	if err := svc.ReportSourceTest(context.Background(), "acme", "t1", SourceTestSuccess, strings.Repeat("x", 5000), details); err != nil {
		t.Fatalf("ReportSourceTest() error = %v", err)
	}
	stored := store.tests["t1"]
	if stored.Status != SourceTestSuccess || stored.CompletedAt == nil {
		t.Errorf("test not completed: %+v", stored)
	}
	if len(stored.Details) != SourceTestMaxDetails {
		t.Errorf("details = %d lines, want %d", len(stored.Details), SourceTestMaxDetails)
	}
	if n := len([]rune(stored.Message)); n > sourceTestMaxMessage+1 {
		t.Errorf("message not bounded: %d runes", n)
	}
}

func TestPipelineCredentialsIsTenantScoped(t *testing.T) {
	// "Leave empty to keep" must not become "leave empty to read someone
	// else's password".
	svc := sourceTestService(&fakeSourceTests{})
	svc.Pipelines = &fakeSourceTestPipelines{pipeline: &Pipeline{
		ID: "p1", CustomerSlug: "other", SourceCredentials: json.RawMessage(`{"attach_params":{"password":"s3cret"}}`),
	}}
	if _, err := svc.PipelineCredentials(context.Background(), core.UserID("u1"), "p1"); !errors.Is(err, ErrPipelineNotFound) {
		t.Fatalf("error = %v, want ErrPipelineNotFound", err)
	}
}

func TestTestableSourceTypesIsACopy(t *testing.T) {
	got := TestableSourceTypes()
	got[0] = "mutated"
	if TestableSourceTypes()[0] == "mutated" {
		t.Error("TestableSourceTypes hands out its own backing array")
	}
	if !SourceTestSupported(SourceTypeDuckDB) || SourceTestSupported("rest_api") {
		t.Error("SourceTestSupported disagrees with the served list")
	}
}
