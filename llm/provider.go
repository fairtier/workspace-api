package llm

import (
	"context"
	"encoding/json"
)

// StructuredCaller performs one single-shot structured-output LLM call:
// system + user prompt + JSON schema in, raw JSON conforming to the schema
// out. Providers differ in how strictly the schema is enforced (Anthropic
// enforces it server-side, DeepSeek only guarantees valid JSON), so callers
// must still validate the decoded output.
type StructuredCaller interface {
	Complete(ctx context.Context, req StructuredRequest) (Result, error)
}

// StructuredRequest describes a single structured-output completion.
type StructuredRequest struct {
	// System is the system prompt (role/rules).
	System string
	// Prompt is the user's message.
	Prompt string
	// Schema is the JSON Schema the output object must conform to.
	Schema map[string]any
	// MaxTokens caps the response length; 0 uses the provider default.
	MaxTokens int
	// Kind labels what is being drafted (e.g. "pipeline", "rill_dashboard")
	// for metering and telemetry. Local providers ignore it; the FairTier API
	// relay uses it to attribute spend per draft surface.
	Kind string
}

// Result is one completed structured-output call.
type Result struct {
	// JSON is the model's output, a single JSON object.
	JSON json.RawMessage
	// Usage is what the provider reported about the call. Zero values mean
	// the provider omitted usage, not that the call was free.
	Usage Usage
}

// Usage is the token spend a provider reported for one call.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}
