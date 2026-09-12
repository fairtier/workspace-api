// Package relay holds the box's clients for the FairTier API's box-facing
// relays — assist (AI drafting) and pipeline-failure alerts. Both are
// hosted-deployment conveniences: a self-hosted workspace runs without them,
// and an explicit self-hoster setting always wins over a relay.
//
// Every relay authenticates the same way, with a client-credentials token
// from this workspace's OWN Casdoor, which is why the token source lives here
// once. The wire contracts are mirrored by hand in proto3 JSON; this module
// deliberately imports none of the FairTier API's generated stubs, so a relay
// change there is a breaking change here and fields are added, never renamed.
package relay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TokenSource mints and caches the OAuth client-credentials token every
// FairTier API relay presents.
type TokenSource struct {
	// TokenURL is the client-credentials token endpoint of this workspace's
	// Casdoor (in-cluster; the minted token still carries the public issuer).
	TokenURL string
	// ClientID / ClientSecret are the workspace's own OAuth client pair.
	ClientID     string
	ClientSecret string
	// HTTPClient overrides the default (tests, tracing).
	HTTPClient *http.Client

	mu    sync.Mutex
	token string
	exp   time.Time
}

var defaultClient = &http.Client{Timeout: 10 * time.Second}

func (s *TokenSource) httpClient() *http.Client {
	if s.HTTPClient != nil {
		return s.HTTPClient
	}
	return defaultClient
}

// Bearer returns a cached token, minting a fresh one when missing, within a
// minute of expiry, or when force is set (a relay answered 401).
func (s *TokenSource) Bearer(ctx context.Context, force bool) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !force && s.token != "" && time.Now().Before(s.exp.Add(-60*time.Second)) {
		return s.token, nil
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {s.ClientID},
		"client_secret": {s.ClientSecret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("mint workspace token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("mint workspace token: status %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}

	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("mint workspace token: empty access_token")
	}

	s.token = out.AccessToken
	s.exp = tokenExpiry(out.AccessToken, out.ExpiresIn)
	return s.token, nil
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
