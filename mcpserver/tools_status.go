package mcpserver

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"

	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
	workflowv1 "github.com/fairtier/workspace-api/proto/workflow/v1"
	"github.com/fairtier/workspace-api/workspace"
)

const workspaceStatusDesc = `Answer "what is broken or stale right now?" in one call: every pipeline, transformation and workflow with whether it is enabled, its schedule (empty = manual only), its last run's status and time, and — when that run failed — its error message (shortened). attention lists what most likely needs a look: failed last runs, and enabled schedules that have never run. Compare last_run_at with now and the schedule to spot stale data (a run stuck in "pending" for minutes means the worker is not picking work up). Start here, then drill in with get_pipeline / get_transformation / get_workflow. agent_sql says what keeps run_query read-only: "engine" (the query engine's read-only session, behind this server's statement check), "guard_only" (this server's statement check alone), or "refused" (run_query is unavailable until the engine has read-only sessions).`

// statusErrorLen caps each error message in workspace_status: enough to
// recognise the failure, short enough that a workspace with many failing
// jobs still fits. The get_* tools return up to maxErrorMessageLen.
const statusErrorLen = 300

// maxStatusDetailCalls bounds the per-item get calls made to fetch error
// messages, so a workspace with hundreds of failing jobs costs bounded work.
const maxStatusDetailCalls = 50

type statusItem struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Enabled       bool   `json:"enabled"`
	Schedule      string `json:"schedule,omitempty"`
	AfterPipeline string `json:"after_pipeline_id,omitempty"`
	LastRunStatus string `json:"last_run_status,omitempty"`
	LastRunAt     string `json:"last_run_at,omitempty"`
	LastError     string `json:"last_error,omitempty"`
}

type workspaceStatusOut struct {
	Now             string       `json:"now"`
	Attention       []string     `json:"attention"`
	Pipelines       []statusItem `json:"pipelines"`
	Transformations []statusItem `json:"transformations"`
	Workflows       []statusItem `json:"workflows"`
	// AgentSQL says what keeps run_query read-only right now.
	AgentSQL SQLSafety `json:"agent_sql"`
}

// workspaceStatus is built from the list handlers, plus one get per item
// whose last run failed (the lists carry no error message).
func workspaceStatus(ctx context.Context, b Backends, _ struct{}) (workspaceStatusOut, error) {
	st := &statusBuilder{budget: maxStatusDetailCalls, attention: []string{}}
	out := workspaceStatusOut{Now: time.Now().UTC().Format(time.RFC3339)}
	var err error
	if out.Pipelines, err = st.pipelines(ctx, b.Pipelines); err != nil {
		return out, err
	}
	if out.Transformations, err = st.transformations(ctx, b.Transformations); err != nil {
		return out, err
	}
	if out.Workflows, err = st.workflows(ctx, b.Workflows); err != nil {
		return out, err
	}
	out.Attention = st.attention
	out.AgentSQL = agentSQL(ctx, b)
	return out, nil
}

// agentSQL reports the SQL safety mode, its detail redacted and shortened
// like any other error text (it can carry an engine error message).
func agentSQL(ctx context.Context, b Backends) SQLSafety {
	if b.SQLSafety == nil {
		return SQLSafety{EnforcedBy: "unknown"}
	}
	s := b.SQLSafety(ctx)
	s.Detail = workspace.ErrorSummary(redactString(s.Detail), statusErrorLen)
	return s
}

type statusBuilder struct {
	budget    int // get calls left for error messages
	attention []string
}

// add fills in the last error of a failed item (lastError is called only
// then, and only while the budget lasts), redacts and shortens it, and notes
// the item in attention when it needs a look.
func (st *statusBuilder) add(items []statusItem, kind string, it statusItem, lastError func() string) []statusItem {
	if it.LastRunStatus == runFailed && st.budget > 0 {
		st.budget--
		// Redact before cutting, so a cut cannot split a credential from
		// the "@" that identifies it; the summary is the line naming the
		// cause, the rest is usually a preamble or a traceback.
		it.LastError = workspace.ErrorSummary(redactString(lastError()), statusErrorLen)
	}
	switch {
	case it.LastRunStatus == runFailed:
		st.attention = append(st.attention, fmt.Sprintf("%s %q (%s): last run failed", kind, it.Name, it.ID))
	case it.Enabled && it.Schedule != "" && it.LastRunAt == "":
		st.attention = append(st.attention, fmt.Sprintf("%s %q (%s): scheduled %q but has never run", kind, it.Name, it.ID, it.Schedule))
	}
	return append(items, it)
}

func (st *statusBuilder) pipelines(ctx context.Context, b PipelineBackend) ([]statusItem, error) {
	resp, err := b.ListPipelines(ctx, connect.NewRequest(&pipelinev1.ListPipelinesRequest{}))
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	items := make([]statusItem, 0, len(resp.Msg.Pipelines))
	for _, p := range resp.Msg.Pipelines {
		it := statusItem{ID: p.Id, Name: p.Name, Enabled: p.Enabled, Schedule: p.Schedule, LastRunStatus: p.LastRunStatus, LastRunAt: p.LastRunAt}
		items = st.add(items, "pipeline", it, func() string {
			r, err := b.GetPipeline(ctx, connect.NewRequest(&pipelinev1.GetPipelineRequest{Id: p.Id}))
			if err != nil || len(r.Msg.RecentRuns) == 0 {
				return ""
			}
			return r.Msg.RecentRuns[0].ErrorMessage
		})
	}
	return items, nil
}

func (st *statusBuilder) transformations(ctx context.Context, b TransformationBackend) ([]statusItem, error) {
	resp, err := b.ListTransformations(ctx, connect.NewRequest(&transformationv1.ListTransformationsRequest{}))
	if err != nil {
		return nil, fmt.Errorf("list transformations: %w", err)
	}
	items := make([]statusItem, 0, len(resp.Msg.Transformations))
	for _, t := range resp.Msg.Transformations {
		it := statusItem{
			ID: t.Id, Name: t.Name, Enabled: t.Enabled, Schedule: t.Schedule, AfterPipeline: t.TriggerAfterPipelineId,
			LastRunStatus: t.LastRunStatus, LastRunAt: t.LastRunAt,
		}
		items = st.add(items, "transformation", it, func() string {
			r, err := b.GetTransformation(ctx, connect.NewRequest(&transformationv1.GetTransformationRequest{Id: t.Id}))
			if err != nil || len(r.Msg.RecentRuns) == 0 {
				return ""
			}
			return r.Msg.RecentRuns[0].ErrorMessage
		})
	}
	return items, nil
}

func (st *statusBuilder) workflows(ctx context.Context, b WorkflowBackend) ([]statusItem, error) {
	resp, err := b.ListWorkflows(ctx, connect.NewRequest(&workflowv1.ListWorkflowsRequest{}))
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	items := make([]statusItem, 0, len(resp.Msg.Workflows))
	for _, w := range resp.Msg.Workflows {
		it := statusItem{ID: w.Id, Name: w.Name, Enabled: w.Enabled, Schedule: w.Schedule, LastRunStatus: w.LastRunStatus, LastRunAt: w.LastRunAt}
		items = st.add(items, "workflow", it, func() string {
			r, err := b.GetWorkflow(ctx, connect.NewRequest(&workflowv1.GetWorkflowRequest{Id: w.Id}))
			if err != nil || len(r.Msg.RecentRuns) == 0 {
				return ""
			}
			return workflowRunError(r.Msg.RecentRuns[0])
		})
	}
	return items, nil
}

const runFailed = "failed"

// workflowRunError names the failed step, which says more than the run's own
// message alone. The step's message is summarised (redacted first) before
// the prefix goes on: dlt's first line is a preamble ending in "with
// exception:", and summarising the prefixed text would skip the prefix with
// it. add summarises again, which leaves a summary unchanged.
func workflowRunError(r *workflowv1.WorkflowRun) string {
	for _, sr := range r.StepRuns {
		if sr.Status == runFailed {
			msg := sr.ErrorMessage
			if msg == "" {
				msg = r.ErrorMessage
			}
			msg = workspace.ErrorSummary(redactString(msg), statusErrorLen)
			return fmt.Sprintf("step %d (%s %q) failed: %s", sr.Position+1, sr.Kind, sr.Name, msg)
		}
	}
	return r.ErrorMessage
}
