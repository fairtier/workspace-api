package mcpserver

import (
	"context"

	"connectrpc.com/connect"

	pipelinev1 "github.com/fairtier/workspace-api/proto/pipeline/v1"
	transformationv1 "github.com/fairtier/workspace-api/proto/transformation/v1"
)

const listPipelinesDesc = `List the workspace's pipelines: declarative dlt ingestion jobs that load a source (REST API, SQL database, files, Google Sheets, ...) into Iceberg tables in the namespace named by dataset_name. Shows each pipeline's schedule (cron, empty = manual only), whether it is enabled, and its last run's status.`

const getPipelineDesc = `Get one pipeline by id: its full definition (source type, source configuration with secrets redacted, dataset, schedule, write disposition) and its most recent runs with status, rows loaded and error messages. Use this to diagnose a failing pipeline. Credentials are never returned.`

const triggerPipelineDesc = `Queue a run of one pipeline now, outside its schedule. The run is picked up by the workspace's worker within seconds and executes asynchronously: this returns the queued run immediately; poll get_pipeline to follow it. A run fetches from the pipeline's source and writes to its tables (with write_disposition "replace", replacing their contents).`

const listTransformationsDesc = `List the workspace's transformations: dbt projects (kept in the "transformations" git repository unless a repo_url is set) that build models — more Iceberg tables — from the loaded data. Shows schedule, the pipeline whose success triggers it (trigger_after_pipeline_id), the dbt selector, and the last run's status.`

const getTransformationDesc = `Get one transformation by id and its most recent runs: status, the git commit each run built from, model and test counts (total / failed), and error messages. Use this to diagnose a failing dbt run; read the models with read_repo_file in the "transformations" repo.`

const triggerTransformationDesc = `Queue a run of one transformation now (dbt build of its selector at the repository's current commit). Runs asynchronously: this returns the queued run immediately; poll get_transformation to follow it. A run already waiting to start is returned instead of queueing another; a disabled transformation is refused (enable it first). Rebuilds the models' tables.`

type idIn struct {
	ID string `json:"id" jsonschema:"the id, as the list tool reports it"`
}

type pipelineSummary struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	SourceType    string `json:"source_type"`
	DatasetName   string `json:"dataset_name"`
	Schedule      string `json:"schedule,omitempty"`
	Enabled       bool   `json:"enabled"`
	LastRunStatus string `json:"last_run_status,omitempty"`
	LastRunAt     string `json:"last_run_at,omitempty"`
}

type pipelineDetail struct {
	pipelineSummary
	SourceConfig     any    `json:"source_config"`
	WriteDisposition string `json:"write_disposition,omitempty"`
	MergeStrategy    string `json:"merge_strategy,omitempty"`
	ConnectionID     string `json:"connection_id,omitempty"`
	HasCredentials   bool   `json:"has_credentials"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
}

type pipelineRun struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	CreatedAt    string `json:"created_at"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	RowsLoaded   int64  `json:"rows_loaded"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type listPipelinesOut struct {
	Pipelines []pipelineSummary `json:"pipelines"`
}

type getPipelineOut struct {
	Pipeline   pipelineDetail `json:"pipeline"`
	RecentRuns []pipelineRun  `json:"recent_runs"`
}

type triggerPipelineOut struct {
	Run pipelineRun `json:"run"`
}

func pipelineSummaryOf(p *pipelinev1.Pipeline) pipelineSummary {
	return pipelineSummary{
		ID: p.Id, Name: p.Name, SourceType: p.SourceType, DatasetName: p.DatasetName,
		Schedule: p.Schedule, Enabled: p.Enabled, LastRunStatus: p.LastRunStatus, LastRunAt: p.LastRunAt,
	}
}

func pipelineRunOf(r *pipelinev1.PipelineRun) pipelineRun {
	if r == nil {
		return pipelineRun{}
	}
	return pipelineRun{
		ID: r.Id, Status: r.Status, CreatedAt: r.CreatedAt, StartedAt: r.StartedAt, CompletedAt: r.CompletedAt,
		RowsLoaded: r.RowsLoaded, ErrorMessage: truncate(redactString(r.ErrorMessage), maxErrorMessageLen),
	}
}

func listPipelines(ctx context.Context, b Backends, _ struct{}) (listPipelinesOut, error) {
	resp, err := b.Pipelines.ListPipelines(ctx, connect.NewRequest(&pipelinev1.ListPipelinesRequest{}))
	if err != nil {
		return listPipelinesOut{}, err
	}
	out := listPipelinesOut{Pipelines: make([]pipelineSummary, 0, len(resp.Msg.Pipelines))}
	for _, p := range resp.Msg.Pipelines {
		out.Pipelines = append(out.Pipelines, pipelineSummaryOf(p))
	}
	return out, nil
}

func getPipeline(ctx context.Context, b Backends, in idIn) (getPipelineOut, error) {
	resp, err := b.Pipelines.GetPipeline(ctx, connect.NewRequest(&pipelinev1.GetPipelineRequest{Id: in.ID}))
	if err != nil {
		return getPipelineOut{}, err
	}
	p := resp.Msg.Pipeline
	out := getPipelineOut{
		Pipeline: pipelineDetail{
			pipelineSummary:  pipelineSummaryOf(p),
			SourceConfig:     redactJSON(p.SourceConfig),
			WriteDisposition: p.WriteDisposition,
			MergeStrategy:    p.MergeStrategy,
			ConnectionID:     resp.Msg.ConnectionId,
			HasCredentials:   resp.Msg.HasCredentials,
			CreatedAt:        p.CreatedAt,
			UpdatedAt:        p.UpdatedAt,
		},
		RecentRuns: make([]pipelineRun, 0, len(resp.Msg.RecentRuns)),
	}
	for _, r := range resp.Msg.RecentRuns {
		out.RecentRuns = append(out.RecentRuns, pipelineRunOf(r))
	}
	return out, nil
}

func triggerPipeline(ctx context.Context, b Backends, in idIn) (triggerPipelineOut, error) {
	resp, err := b.Pipelines.TriggerPipeline(ctx, connect.NewRequest(&pipelinev1.TriggerPipelineRequest{Id: in.ID}))
	if err != nil {
		return triggerPipelineOut{}, err
	}
	return triggerPipelineOut{Run: pipelineRunOf(resp.Msg.Run)}, nil
}

type transformationSummary struct {
	ID                     string `json:"id"`
	Name                   string `json:"name"`
	RepoURL                string `json:"repo_url,omitempty"`
	RepoRef                string `json:"repo_ref,omitempty"`
	Schedule               string `json:"schedule,omitempty"`
	TriggerAfterPipelineID string `json:"trigger_after_pipeline_id,omitempty"`
	DbtSelector            string `json:"dbt_selector,omitempty"`
	Enabled                bool   `json:"enabled"`
	LastRunStatus          string `json:"last_run_status,omitempty"`
	LastRunAt              string `json:"last_run_at,omitempty"`
}

type transformationRun struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	CommitSHA    string `json:"commit_sha,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	ModelsTotal  int32  `json:"models_total"`
	ModelsFailed int32  `json:"models_failed"`
	TestsTotal   int32  `json:"tests_total"`
	TestsFailed  int32  `json:"tests_failed"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type listTransformationsOut struct {
	Transformations []transformationSummary `json:"transformations"`
}

type getTransformationOut struct {
	Transformation transformationSummary `json:"transformation"`
	RecentRuns     []transformationRun   `json:"recent_runs"`
}

type triggerTransformationOut struct {
	Run transformationRun `json:"run"`
}

func transformationSummaryOf(t *transformationv1.Transformation) transformationSummary {
	return transformationSummary{
		ID: t.Id, Name: t.Name, RepoURL: redactString(t.RepoUrl), RepoRef: t.RepoRef, Schedule: t.Schedule,
		TriggerAfterPipelineID: t.TriggerAfterPipelineId, DbtSelector: t.DbtSelector, Enabled: t.Enabled,
		LastRunStatus: t.LastRunStatus, LastRunAt: t.LastRunAt,
	}
}

func transformationRunOf(r *transformationv1.TransformationRun) transformationRun {
	if r == nil {
		return transformationRun{}
	}
	return transformationRun{
		ID: r.Id, Status: r.Status, CommitSHA: r.CommitSha, StartedAt: r.StartedAt, CompletedAt: r.CompletedAt,
		ModelsTotal: r.ModelsTotal, ModelsFailed: r.ModelsFailed, TestsTotal: r.TestsTotal, TestsFailed: r.TestsFailed,
		ErrorMessage: truncate(redactString(r.ErrorMessage), maxErrorMessageLen),
	}
}

func listTransformations(ctx context.Context, b Backends, _ struct{}) (listTransformationsOut, error) {
	resp, err := b.Transformations.ListTransformations(ctx, connect.NewRequest(&transformationv1.ListTransformationsRequest{}))
	if err != nil {
		return listTransformationsOut{}, err
	}
	out := listTransformationsOut{Transformations: make([]transformationSummary, 0, len(resp.Msg.Transformations))}
	for _, t := range resp.Msg.Transformations {
		out.Transformations = append(out.Transformations, transformationSummaryOf(t))
	}
	return out, nil
}

func getTransformation(ctx context.Context, b Backends, in idIn) (getTransformationOut, error) {
	resp, err := b.Transformations.GetTransformation(ctx, connect.NewRequest(&transformationv1.GetTransformationRequest{Id: in.ID}))
	if err != nil {
		return getTransformationOut{}, err
	}
	out := getTransformationOut{
		Transformation: transformationSummaryOf(resp.Msg.Transformation),
		RecentRuns:     make([]transformationRun, 0, len(resp.Msg.RecentRuns)),
	}
	for _, r := range resp.Msg.RecentRuns {
		out.RecentRuns = append(out.RecentRuns, transformationRunOf(r))
	}
	return out, nil
}

func triggerTransformation(ctx context.Context, b Backends, in idIn) (triggerTransformationOut, error) {
	resp, err := b.Transformations.TriggerTransformation(ctx, connect.NewRequest(&transformationv1.TriggerTransformationRequest{Id: in.ID}))
	if err != nil {
		return triggerTransformationOut{}, err
	}
	return triggerTransformationOut{Run: transformationRunOf(resp.Msg.Run)}, nil
}
