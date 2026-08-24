package llm

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// RemoteCaller implements StructuredCaller against the FairTier API's assist
// relay: this deployment holds no LLM provider key at all. The prepared
// request is sent to the FairTier API, which holds the provider keys
// centrally, performs the completion, and returns the raw JSON. The call is
// authenticated with a client-credentials token from this box's own Casdoor —
// the same channel the box-secrets sync uses — so the FairTier API attributes
// the spend to this tenant from the token's issuer.
//
// The relay wire contract (assistrelay.v1.AssistRelayService/Complete,
// Connect JSON) is mirrored by hand here: this module is deliberately free of
// any dependency on the FairTier API's generated protos.
type RemoteCaller struct {
	// BaseURL is the FairTier API root, e.g. https://worker-api.example.com.
	BaseURL string
	// TokenURL is the OAuth client-credentials token endpoint of this box's
	// Casdoor (in-cluster; the minted token still carries the public issuer).
	TokenURL string
	// ClientID / ClientSecret are the box's OAuth client pair.
	ClientID     string
	ClientSecret string
	// HTTPClient overrides the default client (tests).
	HTTPClient *http.Client
	Logger     *slog.Logger

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

// NewRemoteCaller constructs a relay caller.
func NewRemoteCaller(baseURL, tokenURL, clientID, clientSecret string, logger *slog.Logger) *RemoteCaller {
	if logger == nil {
		logger = slog.Default()
	}
	return &RemoteCaller{
		BaseURL:      strings.TrimSuffix(baseURL, "/"),
		TokenURL:     tokenURL,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Logger:       logger,
	}
}

func (c *RemoteCaller) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return tracedClient
}

// relayRequest mirrors assistrelay.v1.CompleteRequest in proto3 JSON
// (lowerCamel field names, which Connect servers accept and emit).
type relayRequest struct {
	System     string `json:"system"`
	Prompt     string `json:"prompt"`
	SchemaJSON string `json:"schemaJson"`
	MaxTokens  int    `json:"maxTokens"`
	Kind       string `json:"kind,omitempty"`
}

// relayResponse mirrors assistrelay.v1.CompleteResponse. proto3 JSON encodes
// int64 as a string, so the token counts decode through flexInt64.
type relayResponse struct {
	OutputJSON       string    `json:"outputJson"`
	PromptTokens     flexInt64 `json:"promptTokens"`
	CompletionTokens flexInt64 `json:"completionTokens"`
}

// relayError mirrors the Connect JSON error envelope.
type relayError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// flexInt64 decodes a JSON number or a proto3-JSON string-encoded int64.
type flexInt64 int64

func (f *flexInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("parse int64 %q: %w", s, err)
	}
	*f = flexInt64(n)
	return nil
}

// Complete relays one structured completion through the FairTier API.
func (c *RemoteCaller) Complete(ctx context.Context, req StructuredRequest) (Result, error) {
	schemaJSON, err := json.Marshal(req.Schema)
	if err != nil {
		return Result{}, fmt.Errorf("marshal schema: %w", err)
	}
	payload, err := json.Marshal(relayRequest{
		System:     req.System,
		Prompt:     req.Prompt,
		SchemaJSON: string(schemaJSON),
		MaxTokens:  req.MaxTokens,
		Kind:       req.Kind,
	})
	if err != nil {
		return Result{}, fmt.Errorf("marshal relay request: %w", err)
	}

	// The provider and model behind the relay are the FairTier API's choice;
	// box-side telemetry records the relay round trip and whatever usage it
	// reported, labeled as its own gen_ai system.
	var res Result
	err = call(ctx, "fairtier_relay", "central", req.MaxTokens, func(ctx context.Context) (usage, error) {
		out, u, err := c.post(ctx, payload)
		res = Result{JSON: out, Usage: Usage{InputTokens: u.inputTokens, OutputTokens: u.outputTokens}}
		return u, err
	})
	return res, err
}

// post sends the relay request, refreshing the cached token once on a 401.
func (c *RemoteCaller) post(ctx context.Context, payload []byte) (json.RawMessage, usage, error) {
	for attempt := 0; ; attempt++ {
		token, err := c.bearer(ctx, attempt > 0)
		if err != nil {
			return nil, usage{}, err
		}
		out, u, unauthenticated, err := c.doPost(ctx, payload, token)
		if unauthenticated && attempt == 0 {
			continue // the cached token expired server-side; retry once fresh
		}
		return out, u, err
	}
}

// doPost performs one relay POST. unauthenticated reports a 401-class reply so
// post can refresh the token; every other error is final.
func (c *RemoteCaller) doPost(ctx context.Context, payload []byte, token string) (json.RawMessage, usage, bool, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.BaseURL+"/assistrelay.v1.AssistRelayService/Complete", bytes.NewReader(payload))
	if err != nil {
		return nil, usage{}, false, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, usage{}, false, fmt.Errorf("FairTier API assist relay: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := relayStatusError(resp.StatusCode, body)
		return nil, usage{}, resp.StatusCode == http.StatusUnauthorized, err
	}

	var out relayResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, usage{}, false, fmt.Errorf("decode relay response: %w", err)
	}
	u := usage{
		inputTokens:  int64(out.PromptTokens),
		outputTokens: int64(out.CompletionTokens),
	}
	if out.OutputJSON == "" {
		return nil, u, false, fmt.Errorf("empty relay response")
	}
	return json.RawMessage(out.OutputJSON), u, false, nil
}

// relayStatusError turns a non-2xx Connect JSON reply into an error, mapping
// the codes the domain layer gives special treatment: an unconfigured central
// relay must surface exactly like a locally unconfigured drafter, and a
// central rate limit exactly like the local one — that is what keeps the
// Console's "coming soon" and "slow down" behaviors working unchanged.
func relayStatusError(status int, body []byte) error {
	var ce relayError
	_ = json.Unmarshal(body, &ce)
	switch ce.Code {
	case "unimplemented":
		return fmt.Errorf("assist relay: %w", workspace.ErrDraftNotConfigured)
	case "resource_exhausted":
		return fmt.Errorf("assist relay: %w", workspace.ErrDraftRateLimited)
	}
	msg := ce.Message
	if msg == "" {
		msg = string(bytes.TrimSpace(body))
	}
	return fmt.Errorf("FairTier API assist relay: status %d (%s): %s", status, ce.Code, msg)
}

// bearer returns a cached token, minting a fresh one when missing, near
// expiry, or when force is set (after a 401).
func (c *RemoteCaller) bearer(ctx context.Context, force bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !force && c.token != "" && time.Now().Before(c.tokenExp.Add(-60*time.Second)) {
		return c.token, nil
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.ClientID},
		"client_secret": {c.ClientSecret},
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.TokenURL,
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("mint box token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("mint box token: status %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}

	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("mint box token: empty access_token")
	}

	c.token = out.AccessToken
	c.tokenExp = tokenExpiry(out.AccessToken, out.ExpiresIn)
	return c.token, nil
}

// tokenExpiry picks the token's lifetime: expires_in when given, else the
// JWT's own exp claim (decoded without verification — this is a cache hint,
// not an authentication decision), else a conservative five minutes.
func tokenExpiry(token string, expiresIn int64) time.Time {
	if expiresIn > 0 {
		return time.Now().Add(time.Duration(expiresIn) * time.Second)
	}
	if parts := strings.Split(token, "."); len(parts) == 3 {
		if payload, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil {
			var claims struct {
				Exp int64 `json:"exp"`
			}
			if json.Unmarshal(payload, &claims) == nil && claims.Exp > 0 {
				return time.Unix(claims.Exp, 0)
			}
		}
	}
	return time.Now().Add(5 * time.Minute)
}
