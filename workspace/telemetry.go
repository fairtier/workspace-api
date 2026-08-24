package workspace

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// scopeName is the instrumentation scope every span and metric this package
// emits is attributed to. It is the import path by convention, so a collector
// can tell workspace-plane telemetry apart from the libraries' own.
const scopeName = "github.com/fairtier/workspace-api/workspace"

// The global providers are no-ops until cmd/workspace_api installs the real
// ones; the OTel API delegates to whatever is registered later, so taking the
// tracer and meter at package init is safe and keeps the call sites terse.
//
// This is the one piece of infrastructure the workspace plane reaches for
// without a port. OTel's API package is a facade with a no-op default, not an
// adapter: nothing here talks to a collector, decides an endpoint, or fails
// when none exists — that is all cmd's. Routing observability through ports
// would mean threading a recorder into every service for no gain in
// substitutability.
var (
	tracer = otel.Tracer(scopeName)
	meter  = otel.Meter(scopeName)
)

// Attribute keys for the dimensions this plane reasons about. They are
// namespaced under workspace.* so they never collide with semconv, and are
// deliberately low-cardinality where they appear on metrics — ids go on spans
// only, where one series per pipeline is not a cost.
const (
	attrSlug            = attribute.Key("workspace.slug")
	attrPipelineID      = attribute.Key("workspace.pipeline.id")
	attrSourceType      = attribute.Key("workspace.pipeline.source_type")
	attrTransformID     = attribute.Key("workspace.transformation.id")
	attrRunStatus       = attribute.Key("workspace.run.status")
	attrRepoPath        = attribute.Key("workspace.repo.path")
	attrRepoPlane       = attribute.Key("workspace.repo.plane")
	attrRepoFileKind    = attribute.Key("workspace.repo.file_kind")
	attrRepoOperation   = attribute.Key("workspace.repo.operation")
	attrOutcome         = attribute.Key("workspace.outcome")
	attrReason          = attribute.Key("workspace.reason")
	attrNotificationTyp = attribute.Key("workspace.notification.type")
	attrDraftKind       = attribute.Key("workspace.draft.kind")
	attrFileFormat      = attribute.Key("workspace.file.format")
)

// The two mirrored planes, as a metric dimension.
const (
	planePipelines       = "pipelines"
	planeTransformations = "transformations"
)

// Outcomes shared by the converge and adopt instruments. "skipped" is not a
// failure: a customer outside mirror scope, or a nil optional collaborator.
const (
	outcomeOK           = "ok"
	outcomeError        = "error"
	outcomeSkipped      = "skipped"
	outcomeAdopted      = "adopted"
	outcomeRefused      = "refused"
	outcomeImported     = "imported"
	outcomeExternal     = "flagged_external"
	outcomeRateLimited  = "rate_limited"
	outcomeUnavailable  = "unavailable"
	outcomeInvalidDraft = "invalid_draft"
)

// The domain instruments. Creation only fails on a malformed instrument name,
// and the API hands back a working no-op instrument when it does, so there is
// nothing useful to do with the error at package scope.
var (
	// pipelineRuns counts every run result the box worker reports back.
	// Together with the duration histogram this is the plane's core health
	// signal: failure ratio and how long loads take.
	pipelineRuns, _ = meter.Int64Counter("workspace.pipeline.runs",
		metric.WithDescription("Pipeline runs reported terminal by the worker."),
		metric.WithUnit("{run}"))
	pipelineRunDuration, _ = meter.Float64Histogram("workspace.pipeline.run.duration",
		metric.WithDescription("Wall-clock duration of a reported pipeline run."),
		metric.WithUnit("s"))
	pipelineRunRows, _ = meter.Int64Counter("workspace.pipeline.run.rows",
		metric.WithDescription("Rows loaded by reported pipeline runs."),
		metric.WithUnit("{row}"))

	// pipelineRunsStuck counts runs the sweep declared dead. A non-zero rate
	// here means box workers are dying mid-load, which no other signal shows:
	// a worker that vanishes reports nothing at all.
	pipelineRunsStuck, _ = meter.Int64Counter("workspace.pipeline.runs.stuck_failed",
		metric.WithDescription("Runs failed by the sweep after sitting in 'running' past the timeout."),
		metric.WithUnit("{run}"))

	transformationRuns, _ = meter.Int64Counter("workspace.transformation.runs",
		metric.WithDescription("Transformation runs reported terminal by the worker."),
		metric.WithUnit("{run}"))
	transformationRunDuration, _ = meter.Float64Histogram("workspace.transformation.run.duration",
		metric.WithDescription("Wall-clock duration of a reported transformation run."),
		metric.WithUnit("s"))

	// repoSyncDuration times a full converge. This sits on the request path
	// of every save, so its tail latency is the Console's.
	repoSyncDuration, _ = meter.Float64Histogram("workspace.repo.sync.duration",
		metric.WithDescription("Duration of one converge of a workspace repo to the desired file set."),
		metric.WithUnit("s"))
	// repoCommits counts writes the mirror makes. A converge that keeps
	// committing on every pass is the signature of a render that never
	// reaches a fixed point.
	repoCommits, _ = meter.Int64Counter("workspace.repo.commits",
		metric.WithDescription("Commits the mirror made to a workspace repo."),
		metric.WithUnit("{commit}"))
	// repoAdoptions counts what the pull half decided about out-of-band edits.
	repoAdoptions, _ = meter.Int64Counter("workspace.repo.adoptions",
		metric.WithDescription("Out-of-band repo edits classified by the adopt pass."),
		metric.WithUnit("{file}"))

	notificationsPublished, _ = meter.Int64Counter("workspace.notifications.published",
		metric.WithDescription("In-app notifications raised."),
		metric.WithUnit("{notification}"))
	// notificationSubscribers tracks open Console streams — the box's live
	// connection count, and the thing to look at when the broker's per-client
	// buffers start dropping.
	notificationSubscribers, _ = meter.Int64UpDownCounter("workspace.notifications.subscribers",
		metric.WithDescription("Console notification streams currently subscribed."),
		metric.WithUnit("{subscription}"))

	fileDropUploads, _ = meter.Int64Counter("workspace.filedrop.uploads",
		metric.WithDescription("Files accepted by the drop-a-file upload path."),
		metric.WithUnit("{file}"))
	fileDropUploadSize, _ = meter.Int64Histogram("workspace.filedrop.upload.size",
		metric.WithDescription("Size of files accepted by the upload path."),
		metric.WithUnit("By"))

	// assistDrafts separates a model failure from a rate-limited caller, which
	// look identical from the RPC error alone.
	assistDrafts, _ = meter.Int64Counter("workspace.assist.drafts",
		metric.WithDescription("AI draft requests by kind and outcome."),
		metric.WithUnit("{draft}"))
)

// endSpan records err on span and closes it. A nil error leaves the span
// Unset rather than marking it Ok: Ok is for an explicit success assertion,
// and blanket-setting it would hide a child span's failure from the UI.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// syncOutcome maps a converge's result to the outcome dimension. skipped is
// the "customer out of mirror scope" case, which must not read as success —
// nothing was converged.
func syncOutcome(err error, converged bool) string {
	switch {
	case err != nil:
		return outcomeError
	case !converged:
		return outcomeSkipped
	}
	return outcomeOK
}

// recordSync closes out one converge: the duration histogram plus the span
// status. Call it deferred, with the outcome resolved by the caller.
func recordSync(ctx context.Context, span trace.Span, plane string, started time.Time, err error, converged bool) {
	repoSyncDuration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(
		attrRepoPlane.String(plane),
		attrOutcome.String(syncOutcome(err, converged)),
	))
	endSpan(span, err)
}

// recordCommit counts one mirror write and notes it on the current span, so a
// converge span shows which files it actually touched rather than only how
// long the whole pass took. plane doubles as the repo name — they are the same
// string by construction, which is what lets the generic helpers report it.
func recordCommit(ctx context.Context, plane, operation, filePath string) {
	repoCommits.Add(ctx, 1, metric.WithAttributes(
		attrRepoPlane.String(plane),
		attrRepoFileKind.String(repoFileKind(filePath)),
		attrRepoOperation.String(operation),
	))
	trace.SpanFromContext(ctx).AddEvent("workspace.repo.commit", trace.WithAttributes(
		attrRepoPath.String(filePath),
		attrRepoOperation.String(operation),
	))
}

// repoFileKind splits the two managed file sets apart by path rather than by
// call site: a credential file is the armored .age ciphertext, everything the
// mirror writes otherwise is a definition. Reading it off the path keeps the
// generic delete helpers from having to be told which set they were handed.
func repoFileKind(filePath string) string {
	if strings.HasSuffix(filePath, ".age") {
		return "credential"
	}
	return "definition"
}

// recordAdoption counts one adopt decision and leaves an event carrying the
// detail — path, id, and for a refusal the user-facing reason. The decision is
// per file and rare, so an event on the pass's span beats a span each.
func recordAdoption(ctx context.Context, plane, outcome string, attrs ...attribute.KeyValue) {
	repoAdoptions.Add(ctx, 1, metric.WithAttributes(
		attrRepoPlane.String(plane),
		attrOutcome.String(outcome),
	))
	trace.SpanFromContext(ctx).AddEvent("workspace.repo."+outcome,
		trace.WithAttributes(append(attrs, attrRepoPlane.String(plane))...))
}

// recordDraft counts one AI draft attempt. The four outcomes are the four
// things an operator would otherwise have to read the logs to tell apart: the
// feature is not configured on this box at all, the caller ran out of budget,
// the model failed, or the model answered with something the validator
// rejected — the last being the one that means the prompts need work rather
// than the deployment.
func recordDraft(ctx context.Context, kind string, err error) {
	assistDrafts.Add(ctx, 1, metric.WithAttributes(
		attrDraftKind.String(kind),
		attrOutcome.String(draftOutcome(err)),
	))
}

func draftOutcome(err error) string {
	var invalid *ErrInvalidSourceConfig
	switch {
	case err == nil:
		return outcomeOK
	case errors.Is(err, ErrDraftNotConfigured):
		return outcomeUnavailable
	case errors.Is(err, ErrDraftRateLimited):
		return outcomeRateLimited
	case errors.As(err, &invalid):
		return outcomeInvalidDraft
	}
	return outcomeError
}

// runDurationSeconds is how long a run took, and whether that is knowable: a
// worker that died mid-load reports no timestamps, and a zero would poison the
// histogram with a bucket-0 sample for the runs that matter most.
func runDurationSeconds(startedAt, completedAt *time.Time) (float64, bool) {
	if startedAt == nil || completedAt == nil {
		return 0, false
	}
	d := completedAt.Sub(*startedAt)
	if d < 0 {
		return 0, false
	}
	return d.Seconds(), true
}
