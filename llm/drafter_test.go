package llm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
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

	draft, err := d.DraftTransformation(context.Background(), "revenue mart")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if draft.Name != "Revenue mart" || draft.DBTSelector != "tag:daily" || len(draft.Files) != 2 {
		t.Fatalf("bad mapping: %+v", draft)
	}
	if draft.Files[0].Path != "models/marts/revenue.sql" || draft.Files[0].Content != "SELECT 1" {
		t.Fatalf("bad file mapping: %+v", draft.Files[0])
	}
}

func TestDrafter_DraftRillDashboard(t *testing.T) {
	caller := &fakeCaller{raw: `{
		"files": [{"path": "metrics/revenue.yaml", "content": "type: metrics_view"}],
		"notes": "ok"
	}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftRillDashboard(context.Background(), "revenue dashboard", []string{"models/orders.sql"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(draft.Files) != 1 || draft.Files[0].Path != "metrics/revenue.yaml" {
		t.Fatalf("bad mapping: %+v", draft)
	}
	// Existing paths must reach the model as context in the user prompt.
	if !strings.Contains(caller.got.Prompt, "models/orders.sql") {
		t.Fatalf("existing paths missing from prompt: %s", caller.got.Prompt)
	}
}

func TestDrafter_BadJSON(t *testing.T) {
	d := NewDrafter(&fakeCaller{raw: `not json`}, nil)
	if _, err := d.DraftTransformation(context.Background(), "p"); err == nil || !strings.Contains(err.Error(), "parse model output") {
		t.Fatalf("want parse error, got %v", err)
	}
}
