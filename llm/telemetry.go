package llm

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const scopeName = "github.com/fairtier/workspace-api/llm"

var (
	tracer = otel.Tracer(scopeName)
	meter  = otel.Meter(scopeName)
)

// The OTel semantic conventions for generative AI. They are still marked
// experimental upstream, so the keys are spelled out here rather than taken
// from the semconv package: that way a convention rename is a change in this
// file, not a build break, and the names stay the ones every LLM dashboard
// already groups by.
const (
	attrOperation    = attribute.Key("gen_ai.operation.name")
	attrSystem       = attribute.Key("gen_ai.system")
	attrRequestModel = attribute.Key("gen_ai.request.model")
	attrMaxTokens    = attribute.Key("gen_ai.request.max_tokens")
	attrFinishReason = attribute.Key("gen_ai.response.finish_reasons")
	attrTokenType    = attribute.Key("gen_ai.token.type")
)

var (
	// tokenUsage is the one that costs money. Split by input/output because
	// they are priced differently, and by model because a deployment that
	// switches models wants the before and after apart.
	tokenUsage, _ = meter.Int64Counter("gen_ai.client.token.usage",
		metric.WithDescription("Tokens used by structured-output completions."),
		metric.WithUnit("{token}"))
	callDuration, _ = meter.Float64Histogram("gen_ai.client.operation.duration",
		metric.WithDescription("Duration of one structured-output completion."),
		metric.WithUnit("s"))
)

// usage is what a provider reports back about one call. Both fields are
// optional: a provider that omits usage records no token counts rather than
// zeros, which would read as "this call was free".
type usage struct {
	inputTokens  int64
	outputTokens int64
	finishReason string
}

// call is the shared instrumentation wrapper around one provider request.
//
// The prompt and the completion never touch the span. They are the customer's
// data — their schema names, their business description — and a trace backend
// is not where that belongs, whatever the gen_ai conventions permit as an
// opt-in elsewhere. Everything recorded here is metadata about the call.
//
// The span name follows the convention "<operation> <model>", which is what
// makes traces from different models comparable in one view.
func call(ctx context.Context, system, model string, maxTokens int, do func(context.Context) (usage, error)) error {
	ctx, span := tracer.Start(ctx, "chat "+model, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attrOperation.String("chat"),
			attrSystem.String(system),
			attrRequestModel.String(model),
			attrMaxTokens.Int(maxTokens),
		))
	defer span.End()

	started := time.Now()
	u, err := do(ctx)
	attrs := metric.WithAttributes(attrSystem.String(system), attrRequestModel.String(model))
	callDuration.Record(ctx, time.Since(started).Seconds(), attrs)

	if u.finishReason != "" {
		span.SetAttributes(attrFinishReason.StringSlice([]string{u.finishReason}))
	}
	recordTokens(ctx, system, model, u)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// recordTokens reports whatever usage the provider gave us. Tokens are billed
// even for a call that then failed validation, so this runs regardless of the
// error.
func recordTokens(ctx context.Context, system, model string, u usage) {
	span := trace.SpanFromContext(ctx)
	for _, t := range []struct {
		kind  string
		count int64
	}{{"input", u.inputTokens}, {"output", u.outputTokens}} {
		if t.count <= 0 {
			continue
		}
		tokenUsage.Add(ctx, t.count, metric.WithAttributes(
			attrSystem.String(system),
			attrRequestModel.String(model),
			attrTokenType.String(t.kind),
		))
		span.SetAttributes(attribute.Int64("gen_ai.usage."+t.kind+"_tokens", t.count))
	}
}
