package workspace_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

type mockSqlDrafter struct {
	gotPrompt, gotCurrentSQL, gotSchema string
	draft                               *workspace.SqlDraft
	err                                 error
}

func (m *mockSqlDrafter) DraftSql(_ context.Context, prompt, currentSQL, schemaContext string) (*workspace.SqlDraft, error) {
	m.gotPrompt, m.gotCurrentSQL, m.gotSchema = prompt, currentSQL, schemaContext
	if m.err != nil {
		return nil, m.err
	}
	return m.draft, nil
}

type mockExplainer struct {
	gotContext string
	err        error
}

func (m *mockExplainer) ExplainError(_ context.Context, contextText string) (*workspace.ErrorExplanation, error) {
	m.gotContext = contextText
	if m.err != nil {
		return nil, m.err
	}
	return &workspace.ErrorExplanation{Explanation: "it broke", LikelyCause: "x", SuggestedFix: "y"}, nil
}

type mockSchemaSource struct {
	mu         sync.Mutex
	tables     []workspace.TableRef
	tablesErr  error
	columns    map[workspace.TableRef][]workspace.ColumnSchema
	described  []workspace.TableRef
	explainErr error
}

func (m *mockSchemaSource) Tables(context.Context, core.UserID) ([]workspace.TableRef, error) {
	return m.tables, m.tablesErr
}

func (m *mockSchemaSource) Columns(_ context.Context, _ core.UserID, ref workspace.TableRef) ([]workspace.ColumnSchema, error) {
	m.mu.Lock()
	m.described = append(m.described, ref)
	m.mu.Unlock()
	if cols, ok := m.columns[ref]; ok {
		return cols, nil
	}
	return nil, errors.New("describe failed")
}

func (m *mockSchemaSource) Explain(context.Context, core.UserID, string) error {
	return m.explainErr
}

type mockPipelineLookup struct {
	p    *workspace.Pipeline
	runs []workspace.PipelineRun
	err  error
}

func (m *mockPipelineLookup) GetPipeline(context.Context, core.UserID, workspace.PipelineID) (*workspace.Pipeline, []workspace.PipelineRun, error) {
	return m.p, m.runs, m.err
}

type mockTransformationLookup struct {
	t    *workspace.Transformation
	runs []workspace.TransformationRun
	err  error
}

func (m *mockTransformationLookup) GetTransformation(context.Context, core.UserID, workspace.TransformationID) (*workspace.Transformation, []workspace.TransformationRun, error) {
	return m.t, m.runs, m.err
}

func sqlAssistService(drafter *mockSqlDrafter, schema *mockSchemaSource) *workspace.AssistService {
	return &workspace.AssistService{
		Workspaces: acmeReader(),
		Sql:        drafter,
		Schema:     schema,
	}
}

func TestAssistService_DraftSql(t *testing.T) {
	orders := workspace.TableRef{Namespace: "marts", Name: "orders"}

	t.Run("not configured when drafter or schema nil", func(t *testing.T) {
		svc := &workspace.AssistService{Workspaces: acmeReader(), Sql: &mockSqlDrafter{}}
		if _, err := svc.DraftSql(context.Background(), "u1", "top orders", ""); !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("schema context carries tables and columns", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "SELECT 1", Notes: "ok"}}
		schema := &mockSchemaSource{
			tables: []workspace.TableRef{orders},
			columns: map[workspace.TableRef][]workspace.ColumnSchema{
				orders: {{Name: "id", Type: "BIGINT"}, {Name: "amount", Type: "DOUBLE"}},
			},
		}
		svc := sqlAssistService(drafter, schema)

		draft, err := svc.DraftSql(context.Background(), "u1", "total order amount", "SELECT 2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if draft.SQL != "SELECT 1" {
			t.Errorf("draft SQL = %q", draft.SQL)
		}
		if !strings.Contains(drafter.gotSchema, "marts.orders") || !strings.Contains(drafter.gotSchema, "amount DOUBLE") {
			t.Errorf("schema context missing table/columns: %q", drafter.gotSchema)
		}
		if drafter.gotCurrentSQL != "SELECT 2" {
			t.Errorf("current SQL not passed: %q", drafter.gotCurrentSQL)
		}
	})

	t.Run("failed describe degrades to name-only", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "SELECT 1"}}
		schema := &mockSchemaSource{tables: []workspace.TableRef{orders}} // no columns → describe fails
		svc := sqlAssistService(drafter, schema)

		if _, err := svc.DraftSql(context.Background(), "u1", "orders", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(drafter.gotSchema, "marts.orders") {
			t.Errorf("table name missing from context: %q", drafter.gotSchema)
		}
	})

	t.Run("cannot-answer: explicit refusal passes through, skipping EXPLAIN", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{NoRelevantData: true, Notes: "The warehouse has no CRM data; ingest it first."}}
		schema := &mockSchemaSource{
			tables:     []workspace.TableRef{orders},
			explainErr: errors.New("EXPLAIN must not run on a refusal"),
		}
		svc := sqlAssistService(drafter, schema)

		draft, err := svc.DraftSql(context.Background(), "u1", "salesforce churn", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !draft.NoRelevantData || draft.SQL != "" {
			t.Errorf("refusal not passed through: %+v", draft)
		}
		if strings.Contains(draft.Notes, "could not validate") {
			t.Errorf("EXPLAIN annotation on a refusal: %q", draft.Notes)
		}
	})

	t.Run("refusal without a reason is malformed", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{NoRelevantData: true, Notes: "  "}}
		svc := sqlAssistService(drafter, &mockSchemaSource{tables: []workspace.TableRef{orders}})

		_, err := svc.DraftSql(context.Background(), "u1", "anything", "")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "notes" {
			t.Fatalf("want notes validation error, got %v", err)
		}
	})

	t.Run("empty sql without the refusal code is still malformed", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "", Notes: "chatty but no code"}}
		svc := sqlAssistService(drafter, &mockSchemaSource{tables: []workspace.TableRef{orders}})

		_, err := svc.DraftSql(context.Background(), "u1", "anything", "")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) || invalid.Field != "sql" {
			t.Fatalf("want sql validation error, got %v", err)
		}
	})

	t.Run("prompt-matching tables get described first when many", func(t *testing.T) {
		var tables []workspace.TableRef
		for i := range 30 {
			tables = append(tables, workspace.TableRef{Namespace: "raw", Name: fmt.Sprintf("t%02d", i)})
		}
		tables = append(tables, orders)
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "SELECT 1"}}
		schema := &mockSchemaSource{tables: tables, columns: map[workspace.TableRef][]workspace.ColumnSchema{}}
		svc := sqlAssistService(drafter, schema)

		if _, err := svc.DraftSql(context.Background(), "u1", "sum of orders by day", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		found := false
		for _, d := range schema.described {
			if d == orders {
				found = true
			}
		}
		if !found {
			t.Errorf("orders not among described tables: %v", schema.described)
		}
		if len(schema.described) > 20 {
			t.Errorf("described %d tables, cap is 20", len(schema.described))
		}
	})

	t.Run("empty draft SQL rejected", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "  "}}
		svc := sqlAssistService(drafter, &mockSchemaSource{tables: []workspace.TableRef{orders}})
		_, err := svc.DraftSql(context.Background(), "u1", "orders", "")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) {
			t.Fatalf("want invalid draft, got %v", err)
		}
	})

	t.Run("EXPLAIN failure lands in notes, not as an error", func(t *testing.T) {
		drafter := &mockSqlDrafter{draft: &workspace.SqlDraft{SQL: "SELECT nope", Notes: "draft"}}
		schema := &mockSchemaSource{
			tables:     []workspace.TableRef{orders},
			explainErr: errors.New(`column "nope" not found`),
		}
		svc := sqlAssistService(drafter, schema)

		draft, err := svc.DraftSql(context.Background(), "u1", "orders", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(draft.Notes, `column "nope" not found`) {
			t.Errorf("engine message missing from notes: %q", draft.Notes)
		}
	})
}

func failedPipelineFixture() (*workspace.Pipeline, workspace.PipelineRun) {
	started := time.Date(2026, 8, 24, 6, 0, 0, 0, time.UTC)
	p := &workspace.Pipeline{
		Name:       "Stripe",
		SourceType: "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://api.stripe.com",` +
			`"auth":{"api_key":"sk_live_XYZ"},"resources":[{"name":"charges","endpoint":"/charges"}],"primary_key":"id"}`),
		SourceCredentials: json.RawMessage(`{"token":"tok_secret"}`),
		DatasetName:       "stripe",
	}
	run := workspace.PipelineRun{
		ID: "run-1", Status: "failed", StartedAt: &started,
		ErrorMessage: "HTTP 401 from https://api.stripe.com/charges",
	}
	return p, run
}

func TestAssistService_ExplainPipelineRun(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		svc := &workspace.AssistService{Workspaces: acmeReader()}
		_, err := svc.ExplainPipelineRun(context.Background(), "u1", "p1", "run-1")
		if !errors.Is(err, workspace.ErrDraftNotConfigured) {
			t.Fatalf("want ErrDraftNotConfigured, got %v", err)
		}
	})

	t.Run("run must be among recent runs", func(t *testing.T) {
		p, run := failedPipelineFixture()
		svc := &workspace.AssistService{
			Workspaces:   acmeReader(),
			Explainer:    &mockExplainer{},
			PipelineRuns: &mockPipelineLookup{p: p, runs: []workspace.PipelineRun{run}},
		}
		_, err := svc.ExplainPipelineRun(context.Background(), "u1", "p1", "other-run")
		if !errors.Is(err, workspace.ErrPipelineRunNotFound) {
			t.Fatalf("want ErrPipelineRunNotFound, got %v", err)
		}
	})

	t.Run("context carries config and error, never credential values", func(t *testing.T) {
		p, run := failedPipelineFixture()
		explainer := &mockExplainer{}
		svc := &workspace.AssistService{
			Workspaces:   acmeReader(),
			Explainer:    explainer,
			PipelineRuns: &mockPipelineLookup{p: p, runs: []workspace.PipelineRun{run}},
		}
		ex, err := svc.ExplainPipelineRun(context.Background(), "u1", "p1", "run-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ex.Explanation == "" {
			t.Error("empty explanation")
		}
		ctxText := explainer.gotContext
		for _, want := range []string{"rest_api", "https://api.stripe.com", "HTTP 401", "primary_key"} {
			if !strings.Contains(ctxText, want) {
				t.Errorf("context missing %q: %s", want, ctxText)
			}
		}
		// The credential value and the credential-shaped config value must
		// never reach the prompt; the key survives as [redacted].
		for _, banned := range []string{"sk_live_XYZ", "tok_secret"} {
			if strings.Contains(ctxText, banned) {
				t.Errorf("credential %q leaked into context: %s", banned, ctxText)
			}
		}
		if !strings.Contains(ctxText, "[redacted]") {
			t.Errorf("api_key not redacted: %s", ctxText)
		}
	})

	t.Run("rate limited via the shared limiter", func(t *testing.T) {
		p, run := failedPipelineFixture()
		svc := &workspace.AssistService{
			Workspaces:   acmeReader(),
			Explainer:    &mockExplainer{},
			PipelineRuns: &mockPipelineLookup{p: p, runs: []workspace.PipelineRun{run}},
			Limiter:      workspace.NewMemoryRateLimiter(1, time.Minute),
		}
		if _, err := svc.ExplainPipelineRun(context.Background(), "u1", "p1", "run-1"); err != nil {
			t.Fatalf("first call should pass: %v", err)
		}
		_, err := svc.ExplainPipelineRun(context.Background(), "u1", "p1", "run-1")
		if !errors.Is(err, workspace.ErrDraftRateLimited) {
			t.Fatalf("want ErrDraftRateLimited, got %v", err)
		}
	})
}

func TestAssistService_ExplainTransformationRun(t *testing.T) {
	tr := &workspace.Transformation{Name: "marts", DBTSelector: "tag:daily", RepoRef: "main"}
	run := workspace.TransformationRun{
		ID: "run-9", Status: "failed", ModelsTotal: 4, ModelsFailed: 1, TestsTotal: 2, TestsFailed: 1,
		ModelResults: json.RawMessage(`[
			{"name":"orders_daily","status":"error","message":"Binder Error: column x not found"},
			{"name":"stg_orders","status":"success","message":""}
		]`),
		ErrorMessage: "dbt build failed: 1 models / 1 tests failed",
	}

	explainer := &mockExplainer{}
	svc := &workspace.AssistService{
		Workspaces:         acmeReader(),
		Explainer:          explainer,
		TransformationRuns: &mockTransformationLookup{t: tr, runs: []workspace.TransformationRun{run}},
	}
	if _, err := svc.ExplainTransformationRun(context.Background(), "u1", "t1", "run-9"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{"tag:daily", "orders_daily", "Binder Error", "1 failed of 4"} {
		if !strings.Contains(explainer.gotContext, want) {
			t.Errorf("context missing %q: %s", want, explainer.gotContext)
		}
	}
	if strings.Contains(explainer.gotContext, "stg_orders") {
		t.Errorf("successful node should not be listed: %s", explainer.gotContext)
	}
}

func TestAssistService_ExplainSqlError(t *testing.T) {
	t.Run("requires sql and error", func(t *testing.T) {
		svc := &workspace.AssistService{Workspaces: acmeReader(), Explainer: &mockExplainer{}}
		_, err := svc.ExplainSqlError(context.Background(), "u1", "SELECT 1", " ")
		var invalid *workspace.ErrInvalidSourceConfig
		if !errors.As(err, &invalid) {
			t.Fatalf("want invalid argument, got %v", err)
		}
	})

	t.Run("context carries sql, error, and the table list", func(t *testing.T) {
		explainer := &mockExplainer{}
		svc := &workspace.AssistService{
			Workspaces: acmeReader(),
			Explainer:  explainer,
			Schema:     &mockSchemaSource{tables: []workspace.TableRef{{Namespace: "marts", Name: "orders"}}},
		}
		if _, err := svc.ExplainSqlError(context.Background(), "u1", "SELECT nope FROM marts.orderz", `Table "orderz" not found`); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, want := range []string{"SELECT nope", `"orderz" not found`, "marts.orders"} {
			if !strings.Contains(explainer.gotContext, want) {
				t.Errorf("context missing %q: %s", want, explainer.gotContext)
			}
		}
	})
}
