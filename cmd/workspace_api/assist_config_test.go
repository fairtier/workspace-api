package main

import (
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/llm"
)

// TestChooseStructuredCaller pins the provider ladder: explicit self-hoster
// keys beat the generic OpenAI-compatible endpoint, which beats the FairTier
// API relay (which only the hosted box chart sets); half-configured entries
// disable drafting rather than falling through to a weaker backend.
func TestChooseStructuredCaller(t *testing.T) {
	logger := slog.Default()
	cases := []struct {
		name string
		env  map[string]string
		want string // "" means nil caller
	}{
		{name: "nothing set", env: nil, want: ""},
		{
			name: "deepseek wins over everything",
			env: map[string]string{
				"DEEPSEEK_API_KEY":  "k",
				"ANTHROPIC_API_KEY": "k",
				"LLM_BASE_URL":      "http://x", "LLM_API_KEY": "k", "LLM_MODEL": "m",
				"FAIRTIER_ASSIST_URL": "http://x",
			},
			want: "deepseek",
		},
		{
			name: "anthropic beats openai-compat and relay",
			env: map[string]string{
				"ANTHROPIC_API_KEY": "k",
				"LLM_BASE_URL":      "http://x", "LLM_API_KEY": "k", "LLM_MODEL": "m",
				"FAIRTIER_ASSIST_URL": "http://x",
			},
			want: "anthropic",
		},
		{
			name: "openai-compat beats relay",
			env: map[string]string{
				"LLM_BASE_URL": "http://x", "LLM_API_KEY": "k", "LLM_MODEL": "m",
				"FAIRTIER_ASSIST_URL": "http://x",
			},
			want: "openai_compat",
		},
		{
			name: "openai-compat without a model disables drafting",
			env:  map[string]string{"LLM_BASE_URL": "http://x", "LLM_API_KEY": "k"},
			want: "",
		},
		{
			name: "relay with the full env",
			env: map[string]string{
				"FAIRTIER_ASSIST_URL":                "http://central",
				"FAIRTIER_ASSIST_TOKEN_URL":          "http://casdoor:8000/api/login/oauth/access_token",
				"FAIRTIER_ASSIST_OIDC_CLIENT_ID":     "id",
				"FAIRTIER_ASSIST_OIDC_CLIENT_SECRET": "secret",
			},
			want: "fairtier_relay",
		},
		{
			name: "relay without the client pair disables drafting",
			env:  map[string]string{"FAIRTIER_ASSIST_URL": "http://central"},
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := func(k string) string { return tc.env[k] }
			got := chooseStructuredCaller(getenv, logger)

			var name string
			switch c := got.(type) {
			case nil:
			case *llm.OpenAICompatCaller:
				name = c.Provider
			case *llm.AnthropicCaller:
				name = "anthropic"
			case *llm.RemoteCaller:
				name = "fairtier_relay"
			default:
				t.Fatalf("unexpected caller type %T", got)
			}
			if name != tc.want {
				t.Fatalf("provider = %q, want %q", name, tc.want)
			}
		})
	}
}
