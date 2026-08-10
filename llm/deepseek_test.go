package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeepSeekCaller_Complete(t *testing.T) {
	req := StructuredRequest{
		System: "You are a test assistant.",
		Prompt: "draft something",
		Schema: map[string]any{"type": "object", "required": []string{"name"}},
	}

	t.Run("happy path sends json_object mode with schema in system message", func(t *testing.T) {
		var got deepseekRequest
		var gotAuth string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			if r.URL.Path != "/chat/completions" {
				t.Errorf("unexpected path %s", r.URL.Path)
			}
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode request: %v", err)
			}
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"name\":\"drafted\"}"}}]}`))
		}))
		defer srv.Close()

		c := NewDeepSeekCaller("sk-test", "", nil)
		c.BaseURL = srv.URL
		c.HTTPClient = srv.Client()

		raw, err := c.Complete(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var out struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &out); err != nil || out.Name != "drafted" {
			t.Fatalf("unexpected output %s (err %v)", raw, err)
		}

		if gotAuth != "Bearer sk-test" {
			t.Errorf("want bearer auth, got %q", gotAuth)
		}
		if got.Model != "deepseek-chat" {
			t.Errorf("want default model deepseek-chat, got %q", got.Model)
		}
		if got.ResponseFormat.Type != "json_object" {
			t.Errorf("want response_format json_object, got %q", got.ResponseFormat.Type)
		}
		if len(got.Messages) != 2 || got.Messages[0].Role != "system" || got.Messages[1].Role != "user" {
			t.Fatalf("want [system, user] messages, got %+v", got.Messages)
		}
		// The schema must ride in the system message (DeepSeek has no strict
		// schema mode), which also satisfies the "json in prompt" requirement.
		if !strings.Contains(got.Messages[0].Content, `"required":["name"]`) {
			t.Errorf("schema JSON missing from system message: %s", got.Messages[0].Content)
		}
		if !strings.Contains(strings.ToLower(got.Messages[0].Content), "json") {
			t.Errorf("system message must mention json for json_object mode")
		}
	})

	t.Run("error status surfaces status and body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
		}))
		defer srv.Close()

		c := NewDeepSeekCaller("sk-test", "", nil)
		c.BaseURL = srv.URL
		c.HTTPClient = srv.Client()

		_, err := c.Complete(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "429") || !strings.Contains(err.Error(), "rate limited") {
			t.Fatalf("want 429 error with body, got %v", err)
		}
	})

	t.Run("empty choices is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"choices":[]}`))
		}))
		defer srv.Close()

		c := NewDeepSeekCaller("sk-test", "", nil)
		c.BaseURL = srv.URL
		c.HTTPClient = srv.Client()

		_, err := c.Complete(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "empty model response") {
			t.Fatalf("want empty-response error, got %v", err)
		}
	})

	t.Run("max tokens defaulted and capped", func(t *testing.T) {
		var got deepseekRequest
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&got)
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{}"}}]}`))
		}))
		defer srv.Close()

		c := NewDeepSeekCaller("sk-test", "custom-model", nil)
		c.BaseURL = srv.URL
		c.HTTPClient = srv.Client()

		if _, err := c.Complete(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if got.MaxTokens != 4096 {
			t.Errorf("want default max_tokens 4096, got %d", got.MaxTokens)
		}
		if got.Model != "custom-model" {
			t.Errorf("want model override, got %q", got.Model)
		}

		big := req
		big.MaxTokens = 100000
		if _, err := c.Complete(context.Background(), big); err != nil {
			t.Fatal(err)
		}
		if got.MaxTokens != 8192 {
			t.Errorf("want max_tokens capped at 8192, got %d", got.MaxTokens)
		}
	})
}

// The token counters are the only place LLM spend shows up, so the usage block
// has to survive decoding — including on the failure path, where the input
// tokens were still billed for a response we then reject.
func TestDecodeDeepSeekResponseUsage(t *testing.T) {
	t.Run("successful completion", func(t *testing.T) {
		body := `{"choices":[{"message":{"content":"{}"},"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":1200,"completion_tokens":340}}`

		_, u, err := decodeDeepSeekResponse(strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if u.inputTokens != 1200 || u.outputTokens != 340 {
			t.Errorf("usage = %+v, want 1200 in / 340 out", u)
		}
		if u.finishReason != "stop" {
			t.Errorf("finish reason = %q, want %q", u.finishReason, "stop")
		}
	})

	t.Run("empty completion still reports what it cost", func(t *testing.T) {
		body := `{"choices":[],"usage":{"prompt_tokens":1200,"completion_tokens":0}}`

		_, u, err := decodeDeepSeekResponse(strings.NewReader(body))
		if err == nil {
			t.Fatal("want an error for an empty model response")
		}
		if u.inputTokens != 1200 {
			t.Errorf("input tokens = %d, want 1200 reported despite the error", u.inputTokens)
		}
	})

	t.Run("missing usage block reports nothing rather than zero", func(t *testing.T) {
		body := `{"choices":[{"message":{"content":"{}"}}]}`

		_, u, err := decodeDeepSeekResponse(strings.NewReader(body))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if u.inputTokens != 0 || u.outputTokens != 0 {
			t.Errorf("usage = %+v, want zeroes (recordTokens skips them)", u)
		}
	})
}
