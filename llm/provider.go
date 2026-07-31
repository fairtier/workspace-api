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
	Complete(ctx context.Context, req StructuredRequest) (json.RawMessage, error)
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
}
