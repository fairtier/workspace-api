package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// alertPath is the relay RPC, Connect JSON. Mirrored by hand from the FairTier
// API's alertrelay.v1; fields are added there, never renamed.
const alertPath = "/alertrelay.v1.AlertRelayService/ReportPipelineFailure"

// alertTimeout bounds one report end to end, token mint included. The
// pipeline service calls this synchronously, after the run is already
// recorded, so this bound is time the worker's report call can still wait:
// the dlt-worker's report client allows 30s for the whole call, and 10s here
// sits well inside that.
const alertTimeout = 10 * time.Second

// AlertClient implements workspace.PipelineAlerter against the FairTier API's
// alert relay: this workspace has the event (its own worker reported a failed
// run) and none of what an email needs — the owner's address, their
// preference, a sender — which the FairTier API has. So the event goes up,
// and the email comes from there.
//
// Best-effort by the port's contract: no retry queue, no persistence. A
// relay that is unreachable costs the email, never the run report, and the
// in-app notification is raised regardless.
type AlertClient struct {
	// BaseURL is the FairTier API root, e.g. https://worker-api.example.com.
	BaseURL string
	Tokens  *TokenSource
	// HTTPClient overrides the default client (tests).
	HTTPClient *http.Client
	Logger     *slog.Logger
}

// NewAlertClient builds a client for the relay at baseURL.
func NewAlertClient(baseURL string, tokens *TokenSource, logger *slog.Logger) *AlertClient {
	if logger == nil {
		logger = slog.Default()
	}
	return &AlertClient{BaseURL: strings.TrimSuffix(baseURL, "/"), Tokens: tokens, Logger: logger}
}

func (c *AlertClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return defaultClient
}

// alertRequest mirrors alertrelay.v1.ReportPipelineFailureRequest in proto3
// JSON (lowerCamel field names, which Connect servers accept and emit).
type alertRequest struct {
	PipelineName string `json:"pipelineName"`
	ErrorMessage string `json:"errorMessage"`
}

// alertResponse mirrors alertrelay.v1.ReportPipelineFailureResponse.
type alertResponse struct {
	Suppressed bool `json:"suppressed"`
}

// connectError mirrors the Connect JSON error envelope.
type connectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AlertPipelineFailure reports one failed run. The slug argument is ignored on
// the wire: a workspace has one tenant, and the FairTier API binds it from the
// token's issuer rather than trusting the body. Returns nil when the relay
// suppressed the email on purpose (rate-limited, owner opted out) and when the
// relay is not configured centrally (UNIMPLEMENTED) — neither is something
// this side should warn about on every failed run.
func (c *AlertClient) AlertPipelineFailure(ctx context.Context, _ string, pipelineName, errorMessage string) error {
	ctx, cancel := context.WithTimeout(ctx, alertTimeout)
	defer cancel()

	payload, err := json.Marshal(alertRequest{PipelineName: pipelineName, ErrorMessage: errorMessage})
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}
	for attempt := 0; ; attempt++ {
		token, err := c.Tokens.Bearer(ctx, attempt > 0)
		if err != nil {
			return fmt.Errorf("alert relay: %w", err)
		}
		outcome, unauthenticated, err := c.post(ctx, payload, token)
		if unauthenticated && attempt == 0 {
			continue // the cached token expired server-side; retry once fresh
		}
		if err != nil {
			return err
		}
		switch outcome {
		case outcomeSuppressed:
			c.Logger.DebugContext(ctx, "alert relay: email suppressed", "pipeline", pipelineName)
		case outcomeUnconfigured:
			c.Logger.DebugContext(ctx, "alert relay: not configured centrally", "pipeline", pipelineName)
		}
		return nil
	}
}

type alertOutcome int

const (
	outcomeSent alertOutcome = iota
	outcomeSuppressed
	outcomeUnconfigured
)

// post performs one relay POST. unauthenticated reports a 401 so the caller
// can refresh the token; every other non-2xx is final, except UNIMPLEMENTED,
// which is an outcome rather than an error.
func (c *AlertClient) post(ctx context.Context, payload []byte, token string) (alertOutcome, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+alertPath, bytes.NewReader(payload))
	if err != nil {
		return 0, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("FairTier API alert relay: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out alertResponse
		if err := json.Unmarshal(body, &out); err != nil {
			return 0, false, fmt.Errorf("decode alert relay response: %w", err)
		}
		if out.Suppressed {
			return outcomeSuppressed, false, nil
		}
		return outcomeSent, false, nil
	}

	var ce connectError
	_ = json.Unmarshal(body, &ce)
	if ce.Code == "unimplemented" {
		return outcomeUnconfigured, false, nil
	}
	msg := ce.Message
	if msg == "" {
		msg = string(bytes.TrimSpace(body))
	}
	return 0, resp.StatusCode == http.StatusUnauthorized,
		fmt.Errorf("FairTier API alert relay: status %d (%s): %s", resp.StatusCode, ce.Code, msg)
}
