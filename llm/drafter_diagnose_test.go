package llm

import (
	"context"
	"strings"
	"testing"
)

func TestDrafter_DraftSql(t *testing.T) {
	caller := &fakeCaller{raw: `{"status":"ok","sql":"SELECT day, sum(amount) FROM \"marts\".\"orders\" GROUP BY 1 LIMIT 200","notes":"Assumed amount is revenue."}`}
	d := NewDrafter(caller, nil)

	draft, err := d.DraftSql(context.Background(), "revenue by day", "SELECT 1", "Warehouse schema:\n- marts.orders (day DATE, amount DOUBLE)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(draft.SQL, "GROUP BY 1") || draft.Notes == "" {
		t.Fatalf("bad mapping: %+v", draft)
	}
	if caller.got.Kind != "sql" {
		t.Errorf("kind = %q, want sql", caller.got.Kind)
	}
	if !strings.Contains(caller.got.Prompt, "marts.orders (day DATE") {
		t.Errorf("schema context missing from prompt: %q", caller.got.Prompt)
	}
	if !strings.Contains(caller.got.Prompt, "current SQL in the editor") || !strings.Contains(caller.got.Prompt, "SELECT 1") {
		t.Errorf("current SQL missing from prompt: %q", caller.got.Prompt)
	}
	if !strings.Contains(caller.got.System, "NEVER emit") && !strings.Contains(caller.got.System, "NEVER include credentials") {
		t.Errorf("system prompt rules missing: %q", caller.got.System)
	}
}

func TestDrafter_DraftSql_BadJSON(t *testing.T) {
	d := NewDrafter(&fakeCaller{raw: `{"sql": 42}`}, nil)
	if _, err := d.DraftSql(context.Background(), "x", "", ""); err == nil {
		t.Fatal("want a parse error for schema drift")
	}
}

func TestDrafter_DraftSql_StatusCodes(t *testing.T) {
	t.Run("no_relevant_data maps to the explicit refusal", func(t *testing.T) {
		d := NewDrafter(&fakeCaller{raw: `{"status":"no_relevant_data","sql":"","notes":"No CRM data ingested."}`}, nil)
		draft, err := d.DraftSql(context.Background(), "salesforce churn", "", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !draft.NoRelevantData || draft.SQL != "" || draft.Notes == "" {
			t.Fatalf("refusal not mapped: %+v", draft)
		}
	})

	t.Run("unknown status is off-contract", func(t *testing.T) {
		d := NewDrafter(&fakeCaller{raw: `{"status":"maybe","sql":"SELECT 1","notes":"x"}`}, nil)
		if _, err := d.DraftSql(context.Background(), "x", "", ""); err == nil || !strings.Contains(err.Error(), `unknown status "maybe"`) {
			t.Fatalf("want unknown-status error, got %v", err)
		}
	})

	t.Run("ok with empty sql is off-contract", func(t *testing.T) {
		d := NewDrafter(&fakeCaller{raw: `{"status":"ok","sql":"  ","notes":"x"}`}, nil)
		if _, err := d.DraftSql(context.Background(), "x", "", ""); err == nil || !strings.Contains(err.Error(), "empty sql") {
			t.Fatalf("want empty-sql error, got %v", err)
		}
	})
}

func TestDrafter_ExplainError(t *testing.T) {
	caller := &fakeCaller{raw: `{"explanation":"The source rejected the request.","likely_cause":"expired key","suggested_fix":"rotate it","suggested_snippet":""}`}
	d := NewDrafter(caller, nil)

	ex, err := d.ExplainError(context.Background(), "A FairTier data pipeline run failed.\nError: HTTP 401")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ex.Explanation == "" || ex.LikelyCause != "expired key" {
		t.Fatalf("bad mapping: %+v", ex)
	}
	if caller.got.Kind != "explain_error" {
		t.Errorf("kind = %q, want explain_error", caller.got.Kind)
	}
	if !strings.Contains(caller.got.Prompt, "HTTP 401") {
		t.Errorf("context missing from prompt: %q", caller.got.Prompt)
	}
	if !strings.Contains(caller.got.System, "NEVER output credentials") {
		t.Errorf("credential rule missing from system prompt: %q", caller.got.System)
	}
}

// TestRillSystemComposition guards the vendored-skills composition: the
// FairTier rules must survive verbatim AHEAD of the reference block, and the
// whole system prompt must stay within the intended budget envelope.
func TestRillSystemComposition(t *testing.T) {
	sys := rillDraftSystem
	for _, rule := range []string{
		"lk.<namespace>.<table>",
		"Never emit rill.yaml, duckdb.yaml or .env",
		"NEVER invent or include credentials",
	} {
		if !strings.Contains(sys, rule) {
			t.Errorf("FairTier rule %q missing from composed system prompt", rule)
		}
	}
	refAt := strings.Index(sys, "Reference documentation")
	if refAt < 0 {
		t.Fatal("reference block missing — did the embed break?")
	}
	if rulesAt := strings.Index(sys, "lk.<namespace>.<table>"); rulesAt > refAt {
		t.Error("FairTier rules must come BEFORE the reference block")
	}
	if len(sys) > len(rillDraftSystemPrompt)+rillSkillsBudget+256 {
		t.Errorf("composed system prompt is %d bytes, over the budget envelope", len(sys))
	}
	// The reference must carry the concrete syntax the drafts depend on.
	for _, want := range []string{"metrics_view", "type: explore", "timeseries"} {
		if !strings.Contains(sys, want) {
			t.Errorf("reference block missing %q", want)
		}
	}
}
