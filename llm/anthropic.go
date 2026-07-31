// Package llm holds the LLM-backed implementations of the workspace plane's
// drafting ports. It is the only package that imports an LLM SDK, keeping the
// workspace domain free of that dependency.
package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// AnthropicCaller implements StructuredCaller using the Anthropic Messages API
// with structured outputs: the response is constrained server-side to the
// request's JSON schema, and refusals surface as errors.
type AnthropicCaller struct {
	client anthropic.Client
	model  anthropic.Model
	effort anthropic.OutputConfigEffort
	logger *slog.Logger
}

// NewAnthropicCaller constructs a caller. model defaults to Claude Opus 4.8
// when empty. The API key is read from apiKey (or, if empty, the standard
// ANTHROPIC_API_KEY env var via the SDK default).
func NewAnthropicCaller(apiKey, model string, logger *slog.Logger) *AnthropicCaller {
	var opts []option.RequestOption
	if apiKey != "" {
		opts = append(opts, option.WithAPIKey(apiKey))
	}
	m := anthropic.Model(model)
	if model == "" {
		m = anthropic.ModelClaudeOpus4_8
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &AnthropicCaller{
		client: anthropic.NewClient(opts...),
		model:  m,
		effort: anthropic.OutputConfigEffortMedium,
		logger: logger,
	}
}

// Complete runs one schema-constrained request/response call (no agent loop).
func (c *AnthropicCaller) Complete(ctx context.Context, req StructuredRequest) (json.RawMessage, error) {
	maxTokens := int64(req.MaxTokens)
	if maxTokens <= 0 {
		maxTokens = 2048
	}
	resp, err := c.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     c.model,
		MaxTokens: maxTokens,
		Thinking:  anthropic.ThinkingConfigParamUnion{OfAdaptive: &anthropic.ThinkingConfigAdaptiveParam{}},
		OutputConfig: anthropic.OutputConfigParam{
			Effort: c.effort,
			Format: anthropic.JSONOutputFormatParam{Schema: req.Schema},
		},
		System: []anthropic.TextBlockParam{{Text: req.System}},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(req.Prompt)),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("anthropic messages: %w", err)
	}

	if resp.StopReason == anthropic.StopReasonRefusal {
		return nil, fmt.Errorf("model declined the request (%s)", resp.StopDetails.Category)
	}

	raw := firstText(resp)
	if raw == "" {
		return nil, fmt.Errorf("empty model response")
	}
	return json.RawMessage(raw), nil
}

// firstText returns the text of the first text block in the response.
func firstText(resp *anthropic.Message) string {
	for _, block := range resp.Content {
		if t, ok := block.AsAny().(anthropic.TextBlock); ok {
			return t.Text
		}
	}
	return ""
}
