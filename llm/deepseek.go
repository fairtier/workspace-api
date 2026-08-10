package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/fairtier/workspace-api/telemetry"
)

// DeepSeekCaller implements StructuredCaller against the DeepSeek
// chat-completions API (OpenAI-compatible). DeepSeek has no strict
// schema-enforced output mode, so the schema is embedded in the system prompt
// and json_object mode only guarantees syntactically valid JSON — callers
// validate the decoded output (which the domain layer does anyway).
type DeepSeekCaller struct {
	// APIKey authenticates requests (Bearer).
	APIKey string
	// Model defaults to "deepseek-chat" when empty.
	Model string
	// BaseURL overrides the default https://api.deepseek.com (tests).
	BaseURL string
	// HTTPClient overrides the default client (tests).
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// NewDeepSeekCaller constructs a caller with sane defaults.
func NewDeepSeekCaller(apiKey, model string, logger *slog.Logger) *DeepSeekCaller {
	if model == "" {
		model = "deepseek-chat"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DeepSeekCaller{
		APIKey: apiKey,
		Model:  model,
		Logger: logger,
	}
}

func (c *DeepSeekCaller) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return "https://api.deepseek.com"
}

func (c *DeepSeekCaller) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return tracedClient
}

// tracedClient is the default client, built once. Drafts are single
// synchronous calls from an RPC handler; the generous timeout still bounds a
// hung upstream, and the instrumented transport puts the provider round trip
// under the completion span.
var tracedClient = telemetry.InstrumentHTTPClient(&http.Client{Timeout: 90 * time.Second})

type deepseekMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type deepseekRequest struct {
	Model          string            `json:"model"`
	Messages       []deepseekMessage `json:"messages"`
	MaxTokens      int               `json:"max_tokens"`
	ResponseFormat struct {
		Type string `json:"type"`
	} `json:"response_format"`
}

type deepseekResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	// Usage is decoded purely for the token metrics. The API always sends it;
	// a response that somehow omits it decodes to zeros, which recordTokens
	// skips rather than reporting as a free call.
	Usage struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

// Complete runs one json_object-mode chat completion. The JSON schema is
// appended to the system message (which also satisfies DeepSeek's requirement
// that the word "json" appear in the prompt when using json_object mode).
func (c *DeepSeekCaller) Complete(ctx context.Context, req StructuredRequest) (json.RawMessage, error) {
	payload, err := c.buildPayload(req)
	if err != nil {
		return nil, err
	}

	var out json.RawMessage
	err = call(ctx, "deepseek", c.Model, req.MaxTokens, func(ctx context.Context) (usage, error) {
		raw, u, err := c.post(ctx, payload)
		out = raw
		return u, err
	})
	return out, err
}

// post sends the prepared body and decodes the response. The HTTP call itself
// is traced by the instrumented transport, so this span's child is the request
// — which is how a slow draft is attributed to the provider rather than to us.
func (c *DeepSeekCaller) post(ctx context.Context, payload []byte) (json.RawMessage, usage, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL()+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, usage{}, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, usage{}, fmt.Errorf("deepseek chat completions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, usage{}, fmt.Errorf("deepseek chat completions: status %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}

	return decodeDeepSeekResponse(resp.Body)
}

// buildPayload assembles and marshals the chat-completion request body,
// appending the JSON schema to the system message and clamping max_tokens.
func (c *DeepSeekCaller) buildPayload(req StructuredRequest) ([]byte, error) {
	schemaJSON, err := json.Marshal(req.Schema)
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}
	system := req.System +
		"\n\nRespond with a single JSON object that validates against this JSON Schema:\n" +
		string(schemaJSON)

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	if maxTokens > 8192 {
		maxTokens = 8192
	}

	body := deepseekRequest{
		Model: c.Model,
		Messages: []deepseekMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: req.Prompt},
		},
		MaxTokens: maxTokens,
	}
	body.ResponseFormat.Type = "json_object"

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	return payload, nil
}

// decodeDeepSeekResponse decodes a successful response body and returns the
// first choice's content, erroring on an empty response. The usage is returned
// alongside the error too: an empty completion still consumed input tokens.
func decodeDeepSeekResponse(r io.Reader) (json.RawMessage, usage, error) {
	var out deepseekResponse
	if err := json.NewDecoder(r).Decode(&out); err != nil {
		return nil, usage{}, fmt.Errorf("decode deepseek response: %w", err)
	}
	u := usage{
		inputTokens:  out.Usage.PromptTokens,
		outputTokens: out.Usage.CompletionTokens,
	}
	if len(out.Choices) == 0 || out.Choices[0].Message.Content == "" {
		return nil, u, fmt.Errorf("empty model response")
	}
	u.finishReason = out.Choices[0].FinishReason
	return json.RawMessage(out.Choices[0].Message.Content), u, nil
}
