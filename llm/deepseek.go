package llm

import (
	"log/slog"
)

// NewDeepSeekCaller constructs an OpenAI-compatible caller preset for the
// DeepSeek chat-completions API. model defaults to "deepseek-chat" when empty.
func NewDeepSeekCaller(apiKey, model string, logger *slog.Logger) *OpenAICompatCaller {
	if model == "" {
		model = "deepseek-chat"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OpenAICompatCaller{
		APIKey:   apiKey,
		Model:    model,
		BaseURL:  "https://api.deepseek.com",
		Provider: "deepseek",
		Logger:   logger,
	}
}
