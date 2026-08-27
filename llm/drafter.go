package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/fairtier/workspace-api/llm/rillskills"
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
		"unsupported_reason",
	},
	"properties": map[string]any{
		"name": map[string]any{
			"type":        "string",
			"description": "Short human-readable pipeline name.",
		},
		"source_type": map[string]any{
			"type": "string",
			"enum": []string{"rest_api", "sql_database", "filesystem", "google_sheets", "file_upload", "duckdb", "unsupported"},
			"description": "The dlt source type best matching the user's description, or \"unsupported\" when the request needs a capability the platform does not have. " +
				"Use file_upload when the user has a local CSV/TSV/Parquet/JSONL file (or spreadsheet export) to drop in — not filesystem, which is for an existing S3/GCS bucket the user already owns. " +
				"Use duckdb for anything a supported DuckDB extension can read: the MySQL and SQL Server database engines, PDF documents, HTML/XML pages, and files kept in Google Drive — the gdrive extension exposes Drive as gdrive:// URLs the other readers accept, so a PDF in the user's Drive is duckdb. A Google Sheet the user points at directly stays google_sheets; sql_database stays the PostgreSQL path.",
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
			"enum":        []string{"append", "replace", "merge", ""},
			"description": "How loaded data is written. Default to append unless the user implies upserts (merge) or full refresh (replace). Empty string only when source_type is \"unsupported\".",
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
				"duckdb requires extension (one of mysql, mssql, pdf, webbed, gdrive, httpfs), tables (an array of {name, query?, cursor_column?, primary_key?}; query defaults to reading the source table of the same name). Database extensions (mysql, mssql) also need attach — an ATTACH template where every secret part is a {placeholder} the user fills in the credentials step, e.g. \"host={host} user={user} password={password} database=shop\". Reader extensions (pdf, webbed, httpfs) take no attach; give each table an explicit query over the reader function, e.g. \"SELECT page, text FROM read_pdf('https://example.com/report.pdf')\" or \"SELECT * FROM read_html('https://example.com/prices')\". The gdrive extension registers a gdrive:// filesystem for the same readers. Address a Drive file BY ID — gdrive://id:<file id>, the id in the file's Drive URL — e.g. \"SELECT page, text FROM read_pdf('gdrive://id:1a2b3c')\" or a native Google Sheet via \"SELECT * FROM read_csv('gdrive://id:1a2b3c')\". Never a folder path like gdrive://Reports/monthly.pdf: the access the user grants covers the individual files they pick, not the folders around them, so path lookup finds nothing. If the user has not given a file id, put the gdrive://id: form in the query with a placeholder id and say in notes which file id to paste in. Its credentials come from connecting Google under Integrations — the pipeline then references that connection and the server fills the token in — so notes should tell the user to connect Google rather than to paste anything. " +
				"NEVER include credentials, API keys, passwords, connection strings, or access keys here — in a duckdb attach template that is what the {placeholder}s are for.",
		},
		"notes": map[string]any{
			"type":        "string",
			"description": "One or two sentences explaining the draft and any assumptions, plus which credentials the user still needs to provide.",
		},
		"unsupported_reason": map[string]any{
			"type":        "string",
			"description": "Empty string when the request is feasible. When source_type is \"unsupported\": one or two sentences naming the missing capability and, when one exists, a genuinely equivalent alternative (e.g. exporting to CSV and using file_upload). Never suggest a supported source as if it could reach the unsupported system.",
		},
	},
}

const pipelineDraftSystemPrompt = `You are a data-engineering assistant for FairTier, a simple Iceberg data platform.
Given a user's natural-language description, draft a single dlt (data load tool) pipeline configuration.

The platform's COMPLETE ingestion capabilities — there are no others:
- rest_api: any HTTP API returning JSON (including SaaS products reachable over their REST API).
- sql_database: PostgreSQL ONLY (this path has no other database driver).
- duckdb: read through a DuckDB extension — the MySQL and SQL Server (mssql)
  database engines; PDF documents (pdf: read_pdf, read_pdf_tables); HTML/XML web pages
  (webbed: read_html, read_xml, html_extract_tables); remote csv/parquet/json files by
  URL (httpfs); and files kept in Google Drive (gdrive: a gdrive:// filesystem the readers
  above accept, so Drive documents are read in place, addressed by file id — e.g.
  read_pdf('gdrive://id:1a2b3c')). No other database engine yet — not MariaDB,
  Oracle, Firebird, MongoDB, SQLite, Snowflake, BigQuery, or anything else.
- filesystem: files in an S3-compatible object-storage bucket the user already owns.
- google_sheets: a Google Sheets spreadsheet.
- file_upload: a local CSV/TSV/Parquet/JSONL file (or spreadsheet export) the user drops in.

Rules:
- Choose exactly one source_type from the list above, or "unsupported".
- Feasibility vs ambiguity are different things. An AMBIGUOUS request (unclear table names,
  vague schedule) gets a reasonable choice explained in notes. An INFEASIBLE request — one
  needing a capability outside the list above (an unsupported database engine, CDC/streaming,
  a SaaS system with no REST API, ...) — gets source_type "unsupported" and an
  unsupported_reason naming exactly what is missing. NEVER map an unsupported system onto
  the nearest supported source_type: a draft that cannot run is worse than a clear no.
- With source_type "unsupported", set unsupported_reason and notes, and leave every other
  string field an empty string ("" — including source_config and write_disposition).
- Prefer file_upload when the user has a local file (CSV/TSV/Parquet/JSONL) or a spreadsheet export to drop in; it needs no source_config or credentials (the files are uploaded in the next step). Use filesystem only for a bucket the user already owns.
- Documents are duckdb, not file_upload: file_upload takes tabular files only, so PDFs and
  web pages go to duckdb with the pdf or webbed extension. When those documents live in the
  user's Google Drive, use duckdb with extension "gdrive" and read them at gdrive://id:<file
  id> — never a gdrive:// folder path, which the granted Drive access cannot resolve. A
  Google Sheet the user names directly is still google_sheets.
- Fill source_config with the NON-SENSITIVE configuration only.
- NEVER invent or include credentials of any kind (API keys, tokens, passwords, connection strings, access keys). The user supplies those separately.
- Prefer sensible defaults: write_disposition "append" unless the user implies upserts or a full refresh.
- Keep dataset_name lowercase snake_case.
- If the description is ambiguous, make a reasonable choice and explain it in notes.
- Otherwise leave unsupported_reason an empty string.`

// pipelineDraftOutput mirrors pipelineDraftSchema for unmarshalling the
// model's JSON response.
type pipelineDraftOutput struct {
	Name              string `json:"name"`
	SourceType        string `json:"source_type"`
	DatasetName       string `json:"dataset_name"`
	Schedule          string `json:"schedule"`
	WriteDisposition  string `json:"write_disposition"`
	MergeStrategy     string `json:"merge_strategy"`
	SourceConfig      string `json:"source_config"`
	Notes             string `json:"notes"`
	UnsupportedReason string `json:"unsupported_reason"`
}

// DraftPipeline calls the model and returns a structured draft. The returned
// SourceConfig is the raw JSON the model produced; the domain validates it.
func (d *Drafter) DraftPipeline(ctx context.Context, prompt string) (*workspace.PipelineDraft, error) {
	res, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    pipelineDraftSystemPrompt,
		Prompt:    prompt,
		Schema:    pipelineDraftSchema,
		MaxTokens: 2048,
		Kind:      "pipeline",
	})
	if err != nil {
		return nil, err
	}

	var out pipelineDraftOutput
	if err := json.Unmarshal(res.JSON, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	cfg := out.SourceConfig
	if cfg == "" {
		cfg = "{}"
	}

	return &workspace.PipelineDraft{
		Name:              out.Name,
		SourceType:        out.SourceType,
		DatasetName:       out.DatasetName,
		Schedule:          out.Schedule,
		WriteDisposition:  out.WriteDisposition,
		MergeStrategy:     out.MergeStrategy,
		SourceConfig:      json.RawMessage(cfg),
		Notes:             out.Notes,
		UnsupportedReason: out.UnsupportedReason,
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
- When a "Warehouse schema" listing is appended to the request, it is the complete set of ingested
  tables: a listed namespace.table is referenced as {{ source('<namespace>', '<table>') }}, and you
  must NOT reference tables absent from the listing. If the data the user describes is not in the
  listing, build on the closest listed tables only when that genuinely serves the request, and say
  clearly in notes what is missing.
- Without a schema listing, source tables are unverified: flag every assumed table in notes.
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
// schemaContext (the server-built warehouse listing) grounds the source()
// references; empty means drafting blind, which the prompt tells the model.
func (d *Drafter) DraftTransformation(ctx context.Context, prompt, schemaContext string) (*workspace.TransformationDraft, error) {
	userPrompt := prompt
	if schemaContext != "" {
		userPrompt += "\n\n" + schemaContext
	}
	res, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    transformationDraftSystemPrompt,
		Prompt:    userPrompt,
		Schema:    transformationDraftSchema,
		MaxTokens: 4096,
		Kind:      "transformation",
	})
	if err != nil {
		return nil, err
	}

	var out transformationDraftOutput
	if err := json.Unmarshal(res.JSON, &out); err != nil {
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
- When a "Warehouse schema" listing is appended to the request, it is the complete warehouse: a
  listed namespace.table is referenced as lk.<namespace>.<table>, and you must NOT reference tables
  absent from the listing (existing repo models remain fair game via their model names). If the data
  the user describes is not in the listing, build on the closest listed tables only when that
  genuinely serves the request, and say clearly in notes what is missing.
- Without a schema listing, if the user's description references tables you cannot see in the
  provided repo paths, make a reasonable assumption and flag it in notes.`

// rillSkillsBudget caps how much vendored reference documentation rides in
// the Rill system prompt. Whole documents are dropped, never truncated —
// rillskills.Reference trims from the back of its priority order.
const rillSkillsBudget = 24 * 1024

// rillDraftSystem is the composed system prompt: FairTier's own rules first
// (they must win any conflict with upstream docs — the reference block, for
// example, documents external-connector table references FairTier does not
// support), then the vendored Rill syntax reference.
var rillDraftSystem = composeRillSystem()

func composeRillSystem() string {
	ref, _ := rillskills.Reference(rillSkillsBudget)
	if ref == "" {
		return rillDraftSystemPrompt
	}
	return rillDraftSystemPrompt +
		"\n\n# Reference documentation (adapted from Rill's agent skills, Apache-2.0)\n" +
		"The FairTier rules above take precedence over anything below.\n\n" +
		ref
}

type rillDraftOutput struct {
	Files []struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	} `json:"files"`
	Notes string `json:"notes"`
}

// DraftRillDashboard calls the model and returns drafted Rill files. The
// domain validates paths and YAML syntax.
// schemaContext (the server-built warehouse listing) grounds the
// lk.<namespace>.<table> references; empty means drafting blind.
func (d *Drafter) DraftRillDashboard(ctx context.Context, prompt string, existingPaths []string, schemaContext string) (*workspace.RillDraft, error) {
	userPrompt := prompt
	if len(existingPaths) > 0 {
		userPrompt += "\n\nExisting files in the Rill project (reference these models/sources instead of inventing new ones where possible):\n- " +
			strings.Join(existingPaths, "\n- ")
	}
	if schemaContext != "" {
		userPrompt += "\n\n" + schemaContext
	}

	res, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    rillDraftSystem,
		Prompt:    userPrompt,
		Schema:    rillDraftSchema,
		MaxTokens: 4096,
		Kind:      "rill_dashboard",
	})
	if err != nil {
		return nil, err
	}

	var out rillDraftOutput
	if err := json.Unmarshal(res.JSON, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	draft := &workspace.RillDraft{Notes: out.Notes}
	for _, f := range out.Files {
		draft.Files = append(draft.Files, workspace.DraftFile{Path: f.Path, Content: f.Content})
	}
	return draft, nil
}

var sqlDraftSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"status", "sql", "notes"},
	"properties": map[string]any{
		"status": map[string]any{
			"type": "string",
			"enum": []string{"ok", "no_relevant_data"},
			"description": "ok: sql contains a query answering the request from the listed tables. " +
				"no_relevant_data: no listed table contains the thing the user asked about — sql is empty and notes explain what is missing.",
		},
		"sql": map[string]any{
			"type":        "string",
			"description": "One DuckDB SELECT statement (a WITH...SELECT is fine) answering the request against the listed tables. No DDL/DML, no multiple statements. Empty string when status is no_relevant_data.",
		},
		"notes": map[string]any{
			"type":        "string",
			"description": "One or two sentences explaining the query and any assumptions (ambiguous column choice, date-range defaults, ...). When status is no_relevant_data: what the warehouse lacks and what data would need to be ingested first.",
		},
	},
}

const sqlDraftSystemPrompt = `You are a SQL assistant for FairTier, a simple Iceberg data platform.
Given a user's natural-language request and their warehouse schema, write one DuckDB SQL query.

Rules:
- Output exactly ONE read-only statement: a SELECT, optionally with WITH (CTEs). NEVER emit
  INSERT/UPDATE/DELETE/MERGE, CREATE/DROP/ALTER, ATTACH/DETACH, COPY, PRAGMA, SET, or CALL,
  and never more than one statement.
- Reference tables exactly as they appear in the provided schema listing ("namespace"."table",
  quoted when needed). Never invent tables or columns that are not listed; if the schema listing
  omits a table's columns, prefer tables whose columns are listed, or say in notes what you assumed.
- Use DuckDB's SQL dialect (FILTER clauses, date_trunc, list/struct functions are available).
- End a row-returning query with a LIMIT (200 unless the user asks otherwise); pure aggregates
  with a small result need none.
- NEVER include credentials of any kind.
- If the request is ambiguous, make a reasonable choice and explain it in notes.
- If the schema listing holds NOTHING relevant to the request (the user asks about data that was
  never ingested), set status to "no_relevant_data" with an empty sql, and use notes to say what
  is missing and what would need to be ingested first. A fabricated query against unrelated
  tables is worse than no query.
- Ambiguity is about WHICH listed table answers the user's actual subject; it never licenses
  changing the subject. If answering would mean reinterpreting the request as being about
  different data than the user named (e.g. "Salesforce churn" answered from taxi-trip tables),
  that is not an assumption — it is "no_relevant_data". Apply this test before writing any SQL:
  does some listed table actually contain the thing the user asked about? If no listed table
  does, refuse with status "no_relevant_data".`

type sqlDraftOutput struct {
	Status string `json:"status"`
	SQL    string `json:"sql"`
	Notes  string `json:"notes"`
}

// DraftSql calls the model with the tenant's schema context and returns a
// structured SQL draft. The domain layer validates the statement (and may
// annotate notes with an EXPLAIN result); the query is never executed on the
// user's behalf.
func (d *Drafter) DraftSql(ctx context.Context, prompt, currentSQL, schemaContext string) (*workspace.SqlDraft, error) {
	userPrompt := prompt
	if strings.TrimSpace(currentSQL) != "" {
		userPrompt += "\n\nThe user's current SQL in the editor (modify it if the request refers to it):\n" + currentSQL
	}
	if schemaContext != "" {
		userPrompt += "\n\n" + schemaContext
	}

	res, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    sqlDraftSystemPrompt,
		Prompt:    userPrompt,
		Schema:    sqlDraftSchema,
		MaxTokens: 2048,
		Kind:      "sql",
	})
	if err != nil {
		return nil, err
	}

	var out sqlDraftOutput
	if err := json.Unmarshal(res.JSON, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}

	// The status is an explicit machine-readable code, not a sentinel sniffed
	// from empty fields; anything off-contract is treated like any other
	// malformed model output (the caller surfaces it as a failed draft).
	switch out.Status {
	case "no_relevant_data":
		return &workspace.SqlDraft{NoRelevantData: true, Notes: out.Notes}, nil
	case "ok":
		if strings.TrimSpace(out.SQL) == "" {
			return nil, fmt.Errorf("parse model output: status %q with empty sql", out.Status)
		}
		return &workspace.SqlDraft{SQL: out.SQL, Notes: out.Notes}, nil
	default:
		return nil, fmt.Errorf("parse model output: unknown status %q", out.Status)
	}
}

var explainErrorSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required":             []string{"explanation", "likely_cause", "suggested_fix", "suggested_snippet"},
	"properties": map[string]any{
		"explanation": map[string]any{
			"type":        "string",
			"description": "Two or three plain-language sentences saying what failed, for a reader who is not a data engineer.",
		},
		"likely_cause": map[string]any{
			"type":        "string",
			"description": "The single most likely root cause, one sentence.",
		},
		"suggested_fix": map[string]any{
			"type":        "string",
			"description": "What the user should do next, concretely (which setting to change, what to check with their source provider, ...).",
		},
		"suggested_snippet": map[string]any{
			"type":        "string",
			"description": "A corrected SQL statement or config fragment, ONLY when the failure context contains enough to be confident; empty string otherwise. Never invent identifiers or credentials.",
		},
	},
}

const explainErrorSystemPrompt = `You are a support assistant for FairTier, a simple Iceberg data platform.
You are given the failure context of one pipeline run, dbt transformation run, or SQL query, and you
explain what went wrong to a user who is usually NOT a data engineer.

Rules:
- Ground every statement in the provided context; when the context is not enough to be sure, say what
  is most likely and what to check, rather than guessing confidently.
- suggested_snippet only when the context clearly determines the fix (e.g. a misspelled column in the
  provided SQL); otherwise leave it an empty string. Never invent table or column names.
- NEVER output credentials, tokens, passwords, or connection strings, even placeholders that look real.
- Credential-shaped values in the context arrive already redacted; treat a redaction marker as "a
  credential was here", not as the literal value.
- Be brief. No headings, no bullet lists inside fields, no restating the raw error verbatim.`

type explainErrorOutput struct {
	Explanation      string `json:"explanation"`
	LikelyCause      string `json:"likely_cause"`
	SuggestedFix     string `json:"suggested_fix"`
	SuggestedSnippet string `json:"suggested_snippet"`
}

// ExplainError calls the model with server-assembled failure context and
// returns a structured explanation. kind labels the failing surface for
// metering ("explain_error" covers all three targets today).
func (d *Drafter) ExplainError(ctx context.Context, contextText string) (*workspace.ErrorExplanation, error) {
	res, err := d.Caller.Complete(ctx, StructuredRequest{
		System:    explainErrorSystemPrompt,
		Prompt:    contextText,
		Schema:    explainErrorSchema,
		MaxTokens: 2048,
		Kind:      "explain_error",
	})
	if err != nil {
		return nil, err
	}

	var out explainErrorOutput
	if err := json.Unmarshal(res.JSON, &out); err != nil {
		return nil, fmt.Errorf("parse model output: %w", err)
	}
	return &workspace.ErrorExplanation{
		Explanation:      out.Explanation,
		LikelyCause:      out.LikelyCause,
		SuggestedFix:     out.SuggestedFix,
		SuggestedSnippet: out.SuggestedSnippet,
	}, nil
}
