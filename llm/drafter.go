package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/fairtier/workspace-api/workspace"
)

// Drafter implements the domain draft interfaces (workspace.PipelineDrafter,
// workspace.TransformationDrafter, workspace.RillDrafter) on top of any
// StructuredCaller, keeping the prompts and output schemas provider-neutral.
type Drafter struct {
	Caller StructuredCaller
	Logger *slog.Logger
}

// NewDrafter constructs a Drafter over caller.
func NewDrafter(caller StructuredCaller, logger *slog.Logger) *Drafter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Drafter{Caller: caller, Logger: logger}
}

// pipelineDraftSchema is the JSON schema the model output is constrained to.
// source_config is a JSON object encoded as a string so it can carry any of
// the supported dlt source shapes; it is validated against the real source
// schema by the domain.
var pipelineDraftSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required": []string{
		"name", "source_type", "dataset_name", "schedule",
		"write_disposition", "merge_strategy", "source_config", "notes",
	},
	"properties": map[string]any{
		"name": map[string]any{
			"type":        "string",
			"description": "Short human-readable pipeline name.",
		},
		"source_type": map[string]any{
			"type": "string",
			"enum": []string{"rest_api", "sql_database", "filesystem", "google_sheets", "file_upload"},
			"description": "The dlt source type best matching the user's description. " +
				"Use file_upload when the user has a local CSV/TSV/Parquet/JSONL file (or spreadsheet export) to drop in — not filesystem, which is for an existing S3/GCS bucket the user already owns.",
		},
		"dataset_name": map[string]any{
			"type":        "string",
			"description": "Target Iceberg dataset (schema) name; lowercase, snake_case.",
		},
		"schedule": map[string]any{
			"type":        "string",
			"description": "Cron expression for the schedule, or empty string for manual-only.",
		},
		"write_disposition": map[string]any{
			"type":        "string",
			"enum":        []string{"append", "replace", "merge"},
			"description": "How loaded data is written. Default to append unless the user implies upserts (merge) or full refresh (replace).",
		},
		"merge_strategy": map[string]any{
			"type":        "string",
			"description": "Empty string, or \"upsert\" when write_disposition is merge.",
		},
		"source_config": map[string]any{
			"type": "string",
			"description": "A JSON object (encoded as a string) holding the non-sensitive dlt source config for source_type. " +
				"rest_api requires base_url and a resources array of {name, endpoint}. " +
				"sql_database may set tables or tables_config. " +
				"filesystem requires bucket_url and may set file_glob. " +
				"google_sheets requires spreadsheet_url_or_id and may set range_names (sheet tabs, A1 ranges, or named ranges; omit to load every tab). " +
				"NEVER include credentials, API keys, passwords, connection strings, or access keys here.",
		},
		"notes": map[string]any{
			"type":        "string",
			"description": "One or two sentences explaining the draft and any assumptions, plus which credentials the user still needs to provide.",
		},
	},
}

const pipelineDraftSystemPrompt = `You are a data-engineering assistant for FairTier, a simple Iceberg data platform.
Given a user's natural-language description, draft a single dlt (data load tool) pipeline configuration.

Rules:
- Choose exactly one source_type: rest_api, sql_database, filesystem, google_sheets, or file_upload.
- Prefer file_upload when the user has a local file (CSV/TSV/Parquet/JSONL) or a spreadsheet export to drop in; it needs no source_config or credentials (the files are uploaded in the next step). Use filesystem only for a bucket the user already owns.
- Fill source_config with the NON-SENSITIVE configuration only.
- NEVER invent or include credentials of any kind (API keys, tokens, passwords, connection strings, access keys). The user supplies those separately.
- Prefer sensible defaults: write_disposition "append" unless the user implies upserts or a full refresh.
- Keep dataset_name lowercase snake_case.
- If the description is ambiguous, make a reasonable choice and explain it in notes.`

// pipelineDraftOutput mirrors pipelineDraftSchema for unmarshalling the
// model's JSON response.
type pipelineDraftOutput struct {
	Name             string `json:"name"`
	SourceType       string `json:"source_type"`
	DatasetName      string `json:"dataset_name"`
	Schedule         string `json:"schedule"`
	WriteDisposition string `json:"write_disposition"`
	MergeStrategy    string `json:"merge_strategy"`
	SourceConfig     string `json:"source_config"`
	Notes            string `json:"notes"`
}

// DraftPipeline calls the model and returns a structured draft. The returned
// SourceConfig is the raw JSON the model produced; the domain validates it.
func (d *Drafter) DraftPipeline(ctx context.Context, prompt string) (*workspace.PipelineDraft, error) {
	raw, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    pipelineDraftSystemPrompt,
		Prompt:    prompt,
		Schema:    pipelineDraftSchema,
		MaxTokens: 2048,
	})
	if err != nil {
		return nil, err
	}

	var out pipelineDraftOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	cfg := out.SourceConfig
	if cfg == "" {
		cfg = "{}"
	}

	return &workspace.PipelineDraft{
		Name:             out.Name,
		SourceType:       out.SourceType,
		DatasetName:      out.DatasetName,
		Schedule:         out.Schedule,
		WriteDisposition: out.WriteDisposition,
		MergeStrategy:    out.MergeStrategy,
		SourceConfig:     json.RawMessage(cfg),
		Notes:            out.Notes,
	}, nil
}

// draftFileSchema describes one generated file in a draft output.
var draftFileSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"path", "content"},
	"properties": map[string]any{
		"path":    map[string]any{"type": "string", "description": "File path relative to the repo root."},
		"content": map[string]any{"type": "string", "description": "Full file content."},
	},
}

var transformationDraftSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"name", "schedule", "dbt_selector", "files", "notes"},
	"properties": map[string]any{
		"name": map[string]any{
			"type":        "string",
			"description": "Short human-readable transformation name.",
		},
		"schedule": map[string]any{
			"type":        "string",
			"description": "Cron expression for the schedule, or empty string for manual-only.",
		},
		"dbt_selector": map[string]any{
			"type":        "string",
			"description": "Optional dbt node selector passed as --select (e.g. \"tag:daily\"), or empty string to run everything.",
		},
		"files": map[string]any{
			"type":        "array",
			"description": "Starter dbt files under models/ — one or two .sql models plus one schema.yml. Paths like models/staging/stg_orders.sql, models/marts/orders_daily.sql, models/marts/schema.yml.",
			"items":       draftFileSchema,
		},
		"notes": map[string]any{
			"type":        "string",
			"description": "One or two sentences explaining the draft, any assumptions, and which source tables the user must confirm exist.",
		},
	},
}

const transformationDraftSystemPrompt = `You are a data-engineering assistant for FairTier, a simple Iceberg data platform.
Given a user's natural-language description, draft a dbt transformation: its config plus starter model files
for the customer's hosted dbt project (a seeded repo whose dbt_project.yml targets the "lake" catalog,
with a staging -> marts layout and sources defined for the ingested datasets).

Rules:
- Output one or two .sql models under models/ (staging and/or marts) and one schema.yml alongside them.
- Models select from ingested tables via {{ source('<dataset>', '<table>') }} and from each other via {{ ref('<model>') }}.
- Use DuckDB SQL. Keep model and column names lowercase snake_case.
- NEVER invent or include credentials, repo URLs, or tokens of any kind. Leave repository choice to the platform.
- schedule is a cron expression, or empty for manual runs. Prefer empty unless the user asks for a schedule.
- If the description is ambiguous (unknown source tables, unclear grain), make a reasonable choice and say so in notes.`

type transformationDraftOutput struct {
	Name        string `json:"name"`
	Schedule    string `json:"schedule"`
	DBTSelector string `json:"dbt_selector"`
	Files       []struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	} `json:"files"`
	Notes string `json:"notes"`
}

// DraftTransformation calls the model and returns a structured dbt draft. The
// domain validates the drafted files (paths, extensions, sizes).
func (d *Drafter) DraftTransformation(ctx context.Context, prompt string) (*workspace.TransformationDraft, error) {
	raw, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    transformationDraftSystemPrompt,
		Prompt:    prompt,
		Schema:    transformationDraftSchema,
		MaxTokens: 4096,
	})
	if err != nil {
		return nil, err
	}

	var out transformationDraftOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	draft := &workspace.TransformationDraft{
		Name:        out.Name,
		Schedule:    out.Schedule,
		DBTSelector: out.DBTSelector,
		Notes:       out.Notes,
	}
	for _, f := range out.Files {
		draft.Files = append(draft.Files, workspace.DraftFile{Path: f.Path, Content: f.Content})
	}
	return draft, nil
}

var rillDraftSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"files", "notes"},
	"properties": map[string]any{
		"files": map[string]any{
			"type":        "array",
			"description": "Rill project files: metrics/<name>.yaml (metrics view), dashboards/<name>.yaml (explore dashboard), and optionally models/<name>.sql.",
			"items":       draftFileSchema,
		},
		"notes": map[string]any{
			"type":        "string",
			"description": "One or two sentences explaining the draft and any assumptions about the underlying tables.",
		},
	},
}

const rillDraftSystemPrompt = `You are a BI assistant for FairTier, a simple Iceberg data platform.
Given a user's natural-language description, draft Rill project files: a metrics view and an explore
dashboard, plus an optional SQL model when the data needs shaping first.

The project queries the customer's Iceberg warehouse through an attached DuckDB catalog named "lk".
Tables MUST be fully qualified as lk.<namespace>.<table> (there is no default schema).

File kinds:
- models/<name>.sql — a DuckDB SQL model, e.g. SELECT ... FROM lk.<namespace>.<table>.
- metrics/<name>.yaml — "type: metrics_view" with either "model: <sql model name>" or "table: lk.<ns>.<table>",
  a "timeseries:" column when one exists, "dimensions:" ([{column, label}] entries) and
  "measures:" ([{name, expression, label}] entries, aggregate SQL expressions like SUM(amount)).
- dashboards/<name>.yaml — "type: explore" with "metrics_view: <metrics view name>" and optional
  "dimensions: '*'" / "measures: '*'".

Rules:
- NEVER invent or include credentials of any kind.
- Never emit rill.yaml, duckdb.yaml or .env — those are platform-managed.
- Keep file and field names lowercase snake_case.
- If the user's description references tables you cannot see in the provided repo paths, make a
  reasonable assumption and flag it in notes.`

type rillDraftOutput struct {
	Files []struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	} `json:"files"`
	Notes string `json:"notes"`
}

// DraftRillDashboard calls the model and returns drafted Rill files. The
// domain validates paths and YAML syntax.
func (d *Drafter) DraftRillDashboard(ctx context.Context, prompt string, existingPaths []string) (*workspace.RillDraft, error) {
	userPrompt := prompt
	if len(existingPaths) > 0 {
		userPrompt += "\n\nExisting files in the Rill project (reference these models/sources instead of inventing new ones where possible):\n- " +
			strings.Join(existingPaths, "\n- ")
	}

	raw, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    rillDraftSystemPrompt,
		Prompt:    userPrompt,
		Schema:    rillDraftSchema,
		MaxTokens: 4096,
	})
	if err != nil {
		return nil, err
	}

	var out rillDraftOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	draft := &workspace.RillDraft{Notes: out.Notes}
	for _, f := range out.Files {
		draft.Files = append(draft.Files, workspace.DraftFile{Path: f.Path, Content: f.Content})
	}
	return draft, nil
}
