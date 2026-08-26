package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// fakeCaller returns canned JSON and records the request; guards the
// AnthropicDrafter → Drafter/StructuredCaller refactor.
type fakeCaller struct {
	raw string
	got StructuredRequest
	err error
}

func (f *fakeCaller) Complete(_ context.Context, req StructuredRequest) (Result, error) {
	f.got = req
	if f.err != nil {
		return Result{}, f.err
	}
	return Result{JSON: json.RawMessage(f.raw)}, nil
}

func TestDrafter_DraftPipeline(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"name": "Stripe", "source_type": "rest_api", "dataset_name": "stripe",
		"schedule": "0 3 * * *", "write_disposition": "append", "merge_strategy": "",
		"source_config": "{\"base_url\":\"https://api.stripe.com\"}",
		"notes": "Provide your API key."
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftPipeline(context.Background(), "load stripe")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if draft.Name != "Stripe" || draft.SourceType != "rest_api" || draft.Schedule != "0 3 * * *" {
		t.Fatalf("bad mapping: %+v", draft)
	}
	if string(draft.SourceConfig) != `{"base_url":"https://api.stripe.com"}` {
		t.Fatalf("source config not passed through raw: %s", draft.SourceConfig)
	}
	if caller.got.Schema == nil || caller.got.System == "" || caller.got.Prompt != "load stripe" {
		t.Fatalf("request not populated: %+v", caller.got)
	}
}

func TestDrafter_DraftPipelineUnsupported(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"name": "", "source_type": "unsupported", "dataset_name": "",
		"schedule": "", "write_disposition": "", "merge_strategy": "",
		"source_config": "",
		"notes": "Oracle is not reachable from this platform.",
		"unsupported_reason": "sql_database supports PostgreSQL only; there is no Oracle driver."
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftPipeline(context.Background(), "extract from our Oracle DB")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if draft.SourceType != "unsupported" || draft.UnsupportedReason == "" {
		t.Fatalf("refusal not mapped: %+v", draft)
	}

	// The schema must actually offer the refusal path: "unsupported" in the
	// source_type enum and a required unsupported_reason — otherwise a closed
	// forced-choice schema shoehorns infeasible requests into the nearest
	// supported source.
	props := caller.got.Schema["properties"].(map[string]any)
	enum := props["source_type"].(map[string]any)["enum"].([]string)
	found := false
	for _, v := range enum {
		if v == "unsupported" {
			found = true
		}
	}
	if !found {
		t.Fatalf("source_type enum lacks the refusal value: %v", enum)
	}
	if _, ok := props["unsupported_reason"]; !ok {
		t.Fatal("schema lacks unsupported_reason")
	}
	required := caller.got.Schema["required"].([]string)
	reqFound := false
	for _, v := range required {
		if v == "unsupported_reason" {
			reqFound = true
		}
	}
	if !reqFound {
		t.Fatalf("unsupported_reason not required: %v", required)
	}
	if !strings.Contains(caller.got.System, "PostgreSQL ONLY") {
		t.Fatal("system prompt lost the capability envelope")
	}
}

func TestDrafter_DraftTransformation(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"name": "Revenue mart", "schedule": "", "dbt_selector": "tag:daily",
		"files": [
			{"path": "models/marts/revenue.sql", "content": "SELECT 1"},
			{"path": "models/marts/schema.yml", "content": "version: 2"}
		],
		"notes": "assumes stripe source"
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftTransformation(context.Background(), "revenue mart", "Warehouse schema:\n- stripe.charges (amount BIGINT)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if draft.Name != "Revenue mart" || draft.DBTSelector != "tag:daily" || len(draft.Files) != 2 {
		t.Fatalf("bad mapping: %+v", draft)
	}
	if draft.Files[0].Path != "models/marts/revenue.sql" || draft.Files[0].Content != "SELECT 1" {
		t.Fatalf("bad file mapping: %+v", draft.Files[0])
	}
	// The schema context must ground the prompt.
	if !strings.Contains(caller.got.Prompt, "stripe.charges") {
		t.Fatalf("schema context missing from prompt: %s", caller.got.Prompt)
	}
}

func TestDrafter_DraftRillDashboard(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"files": [{"path": "metrics/revenue.yaml", "content": "type: metrics_view"}],
		"notes": "ok"
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftRillDashboard(context.Background(), "revenue dashboard", []string{"models/orders.sql"}, "Warehouse schema:\n- marts.orders (amount DOUBLE)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(draft.Files) != 1 || draft.Files[0].Path != "metrics/revenue.yaml" {
		t.Fatalf("bad mapping: %+v", draft)
	}
	// Existing paths and the schema context must reach the model in the user
	// prompt.
	if !strings.Contains(caller.got.Prompt, "models/orders.sql") {
		t.Fatalf("existing paths missing from prompt: %s", caller.got.Prompt)
	}
	if !strings.Contains(caller.got.Prompt, "marts.orders") {
		t.Fatalf("schema context missing from prompt: %s", caller.got.Prompt)
	}
}

func TestDrafter_BadJSON(t *testing.T) {
	d := NewDrafter(&fakeCaller{raw: `not json`}, nil)
	if _, err := d.DraftTransformation(context.Background(), "p", ""); err == nil || !strings.Contains(err.Error(), "parse model output") {
		t.Fatalf("want parse error, got %v", err)
	}
}

// TestDrafter_DraftPipelineGDrivePDF pins the cross-layer agreement for the
// Drive-PDF path: what the drafter is told to emit for a gdrive source must be
// what ValidateSourceConfig accepts. The prompt naming the capability and the
// validator's allowlist live in different packages and have drifted before.
func TestDrafter_DraftPipelineGDrivePDF(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"name": "Drive invoices", "source_type": "duckdb", "dataset_name": "invoices",
		"schedule": "0 6 * * *", "write_disposition": "append", "merge_strategy": "",
		"source_config": "{\"extension\":\"gdrive\",\"tables\":[{\"name\":\"invoices\",\"query\":\"SELECT page, text FROM read_pdf('gdrive://Reports/monthly.pdf')\"}]}",
		"notes": "Connect Google to supply the refresh token."
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftPipeline(context.Background(), "load the monthly invoice PDFs from my Google Drive Reports folder")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if draft.SourceType != "duckdb" {
		t.Fatalf("source type = %q, want duckdb", draft.SourceType)
	}
	if err := workspace.ValidateSourceConfig(draft.SourceType, draft.SourceConfig); err != nil {
		t.Fatalf("drafted gdrive config rejected by the validator: %v", err)
	}
}

// TestPipelineDraftPromptAdvertisesGDrive guards the gap that made the Drive
// path unreachable: the schema's source_config description named gdrive, but
// the capability list the model reads to CHOOSE a source type did not, so a
// Drive request could be routed to unsupported or file_upload instead.
func TestPipelineDraftPromptAdvertisesGDrive(t *testing.T) {
	sourceType, ok := pipelineDraftSchema["properties"].(map[string]any)["source_type"].(map[string]any)
	if !ok {
		t.Fatal("source_type property missing from pipelineDraftSchema")
	}
	for _, tc := range []struct{ name, text string }{
		{"system prompt capability list", pipelineDraftSystemPrompt},
		{"source_type description", sourceType["description"].(string)},
	} {
		if !strings.Contains(tc.text, "gdrive") {
			t.Errorf("%s does not mention the gdrive extension", tc.name)
		}
		if !strings.Contains(tc.text, "Google Drive") {
			t.Errorf("%s does not mention Google Drive", tc.name)
		}
	}
}
