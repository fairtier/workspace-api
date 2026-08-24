package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// Caps on the assembled failure context. error_message is scrubbed of
// credentials by the worker before it is stored, but it can be long
// (SQLAlchemy dumps whole statements); the model needs the head, not all of
// it.
const (
	maxErrorContextBytes = 8 * 1024
	maxFailedModelLines  = 20
)

// ExplainPipelineRun explains one failed (or otherwise puzzling) pipeline
// run. The context is assembled server-side from the caller's own pipeline
// and run rows — the client sends only ids, so nothing it says becomes
// trusted prompt content. Credentials never enter the context:
// source_credentials is a separate field that is simply never read here, and
// source_config additionally passes a key-name redactor as defense in depth
// (a rest_api config can carry auth headers).
func (s *AssistService) ExplainPipelineRun(ctx context.Context, callerID core.UserID, pipelineID PipelineID, runID string) (_ *ErrorExplanation, err error) {
	defer func() { recordDraft(ctx, "explain_error", err) }()

	if s.Explainer == nil || s.PipelineRuns == nil {
		return nil, ErrDraftNotConfigured
	}
	if err := s.scope(ctx, callerID); err != nil {
		return nil, err
	}

	p, runs, err := s.PipelineRuns.GetPipeline(ctx, callerID, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	run, ok := findPipelineRun(runs, runID)
	if !ok {
		return nil, ErrPipelineRunNotFound
	}

	return s.Explainer.ExplainError(ctx, buildPipelineErrorContext(p, run))
}

// ExplainTransformationRun mirrors ExplainPipelineRun for dbt runs.
func (s *AssistService) ExplainTransformationRun(ctx context.Context, callerID core.UserID, transformationID TransformationID, runID string) (_ *ErrorExplanation, err error) {
	defer func() { recordDraft(ctx, "explain_error", err) }()

	if s.Explainer == nil || s.TransformationRuns == nil {
		return nil, ErrDraftNotConfigured
	}
	if err := s.scope(ctx, callerID); err != nil {
		return nil, err
	}

	t, runs, err := s.TransformationRuns.GetTransformation(ctx, callerID, transformationID)
	if err != nil {
		return nil, fmt.Errorf("get transformation: %w", err)
	}
	run, ok := findTransformationRun(runs, runID)
	if !ok {
		return nil, ErrTransformationRunNotFound
	}

	return s.Explainer.ExplainError(ctx, buildTransformationErrorContext(t, run))
}

// ExplainSqlError explains a failed editor query. Unlike the run targets the
// SQL and error text are client-supplied — the server holds no query history
// — so the context carries only what the caller already knows, plus their own
// table listing when available.
func (s *AssistService) ExplainSqlError(ctx context.Context, callerID core.UserID, sql, errorMessage string) (_ *ErrorExplanation, err error) {
	defer func() { recordDraft(ctx, "explain_error", err) }()

	if s.Explainer == nil {
		return nil, ErrDraftNotConfigured
	}
	if strings.TrimSpace(sql) == "" || strings.TrimSpace(errorMessage) == "" {
		return nil, &ErrInvalidSourceConfig{Field: "sql", Msg: "sql and error_message are both required"}
	}
	if err := s.scope(ctx, callerID); err != nil {
		return nil, err
	}

	var b strings.Builder
	b.WriteString("A SQL query in the FairTier SQL editor failed.\n\nSQL:\n")
	b.WriteString(truncate(sql, maxErrorContextBytes/2))
	b.WriteString("\n\nEngine error:\n")
	b.WriteString(truncate(errorMessage, maxErrorContextBytes/2))
	// The table listing turns "column not found" into "did you mean" —
	// best-effort, a broken engine must not break the explanation.
	if s.Schema != nil {
		if tables, terr := s.Schema.Tables(ctx, callerID); terr == nil && len(tables) > 0 {
			b.WriteString("\n\nTables in the warehouse:\n")
			for _, t := range tables {
				if b.Len() >= maxErrorContextBytes {
					break
				}
				fmt.Fprintf(&b, "- %s.%s\n", t.Namespace, t.Name)
			}
		}
	}

	return s.Explainer.ExplainError(ctx, b.String())
}

func findPipelineRun(runs []PipelineRun, id string) (PipelineRun, bool) {
	for _, r := range runs {
		if r.ID == id {
			return r, true
		}
	}
	return PipelineRun{}, false
}

func findTransformationRun(runs []TransformationRun, id string) (TransformationRun, bool) {
	for _, r := range runs {
		if r.ID == id {
			return r, true
		}
	}
	return TransformationRun{}, false
}

// buildPipelineErrorContext renders the trusted failure context for one
// pipeline run. SourceCredentials is deliberately never touched.
func buildPipelineErrorContext(p *Pipeline, run PipelineRun) string {
	var b strings.Builder
	b.WriteString("A FairTier data pipeline run failed.\n\nPipeline configuration:\n")
	fmt.Fprintf(&b, "- name: %s\n- source_type: %s\n- dataset_name: %s\n", p.Name, p.SourceType, p.DatasetName)
	if p.Schedule != "" {
		fmt.Fprintf(&b, "- schedule: %s\n", p.Schedule)
	}
	if p.WriteDisposition != "" {
		fmt.Fprintf(&b, "- write_disposition: %s", p.WriteDisposition)
		if p.MergeStrategy != "" {
			fmt.Fprintf(&b, " (%s)", p.MergeStrategy)
		}
		b.WriteString("\n")
	}
	if cfg := redactConfigKeys(p.SourceConfig); cfg != "" {
		fmt.Fprintf(&b, "- source_config (credential-shaped values redacted): %s\n", cfg)
	}

	b.WriteString("\nRun:\n")
	writeRunTimes(&b, run.Status, run.StartedAt, run.CompletedAt)
	fmt.Fprintf(&b, "- rows_loaded: %d\n", run.RowsLoaded)
	if run.ErrorMessage != "" {
		b.WriteString("\nError (credentials already scrubbed):\n")
		b.WriteString(truncate(run.ErrorMessage, maxErrorContextBytes))
	}
	return b.String()
}

// buildTransformationErrorContext renders the trusted failure context for one
// dbt run: config, counts, the failed nodes' own messages.
func buildTransformationErrorContext(t *Transformation, run TransformationRun) string {
	var b strings.Builder
	b.WriteString("A FairTier dbt transformation run failed.\n\nTransformation configuration:\n")
	fmt.Fprintf(&b, "- name: %s\n", t.Name)
	if t.DBTSelector != "" {
		fmt.Fprintf(&b, "- dbt_selector: %s\n", t.DBTSelector)
	}
	if t.Schedule != "" {
		fmt.Fprintf(&b, "- schedule: %s\n", t.Schedule)
	}
	if t.RepoRef != "" {
		fmt.Fprintf(&b, "- repo_ref: %s\n", t.RepoRef)
	}

	b.WriteString("\nRun:\n")
	writeRunTimes(&b, run.Status, run.StartedAt, run.CompletedAt)
	fmt.Fprintf(&b, "- models: %d failed of %d; tests: %d failed of %d\n",
		run.ModelsFailed, run.ModelsTotal, run.TestsFailed, run.TestsTotal)
	if failed := failedModelLines(run.ModelResults); len(failed) > 0 {
		b.WriteString("\nFailed nodes:\n")
		for _, line := range failed {
			b.WriteString(line)
			b.WriteString("\n")
			if b.Len() >= maxErrorContextBytes {
				break
			}
		}
	}
	if run.ErrorMessage != "" && b.Len() < maxErrorContextBytes {
		b.WriteString("\nError:\n")
		b.WriteString(truncate(run.ErrorMessage, maxErrorContextBytes-b.Len()))
	}
	return b.String()
}

func writeRunTimes(b *strings.Builder, status string, startedAt, completedAt *time.Time) {
	fmt.Fprintf(b, "- status: %s\n", status)
	if startedAt != nil {
		fmt.Fprintf(b, "- started_at: %s\n", startedAt.UTC().Format(time.RFC3339))
	}
	if completedAt != nil {
		fmt.Fprintf(b, "- completed_at: %s\n", completedAt.UTC().Format(time.RFC3339))
	}
}

// failedModelLines extracts the non-success nodes from a run's model_results
// JSON ([{"name","resource_type","status","execution_time","message"}, ...]).
func failedModelLines(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var nodes []struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil
	}
	var out []string
	for _, n := range nodes {
		switch strings.ToLower(n.Status) {
		case "success", "pass", "skipped":
			continue
		}
		out = append(out, fmt.Sprintf("- %s [%s]: %s", n.Name, n.Status, truncate(n.Message, 512)))
		if len(out) >= maxFailedModelLines {
			break
		}
	}
	return out
}

// redactedKey matches JSON keys whose values must not reach a prompt even
// though source_config is nominally non-sensitive — a rest_api config can
// carry auth headers. primary_key is the one legitimate *_key config field.
var redactedKey = regexp.MustCompile(`(?i)(secret|token|password|passwd|credential|connection|dsn|auth|key)`)

// redactConfigKeys re-renders a config JSON object with credential-shaped
// values replaced by "[redacted]", recursively. Unparseable input yields ""
// (drop the config entirely rather than pass through unredacted bytes).
func redactConfigKeys(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return ""
	}
	out, err := json.Marshal(redactValue(doc))
	if err != nil {
		return ""
	}
	return string(out)
}

func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if strings.EqualFold(k, "primary_key") {
				out[k] = redactValue(val)
				continue
			}
			if redactedKey.MatchString(k) {
				out[k] = "[redacted]"
				continue
			}
			out[k] = redactValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = redactValue(val)
		}
		return out
	default:
		return v
	}
}

// truncate cuts s to at most n bytes, marking the cut.
func truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n... (truncated)"
}
