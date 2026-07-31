package casdoor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// TokenProvider implements core.TokenProvider using Casdoor's OAuth2
// client_credentials grant.
type TokenProvider struct {
	// Endpoint is the Casdoor base URL (e.g. "https://auth.fairtier.com").
	Endpoint string

	// Client is the HTTP client used for token requests.
	// If nil, http.DefaultClient is used.
	Client *http.Client
}

func (p *TokenProvider) httpClient() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return http.DefaultClient
}

// tokenURL returns the Casdoor OAuth2 token endpoint for the given base URL,
// falling back to the provider's configured Endpoint when base is empty.
func (p *TokenProvider) tokenURL(base string) string {
	if base == "" {
		base = p.Endpoint
	}
	return strings.TrimRight(base, "/") + "/api/login/oauth/access_token"
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type tokenError struct {
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

// GetClientToken obtains an access token using the OAuth2 client_credentials
// grant. issuer overrides the configured Endpoint (empty = central Casdoor);
// VM boxes pass their on-box Casdoor URL.
func (p *TokenProvider) GetClientToken(ctx context.Context, issuer, clientID, clientSecret string) (string, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL(issuer), strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var te tokenError
		_ = json.NewDecoder(resp.Body).Decode(&te)
		return "", fmt.Errorf("token request failed (%d): %s: %s", resp.StatusCode, te.Error, te.Description)
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	if tr.AccessToken == "" {
		return "", fmt.Errorf("empty access token in response")
	}

	return tr.AccessToken, nil
}
