package mcpserver

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIsSecretPath(t *testing.T) {
	secret := []string{
		".env", ".env.local", "prod.env", "sub/.env",
		"keys/deploy.pem", "tls.key", "store.p12", "creds.age", "x.gpg",
		"profiles.yml", "dbt/profiles.yaml", "PROFILES.YML",
		"secrets.toml", "config/secrets.yaml", ".dlt/secrets.toml", ".dlt/config.toml",
		"credentials.json", "gcp/service_account.json", ".ssh/id_rsa", "id_ed25519",
		".git/config",
	}
	for _, p := range secret {
		if !isSecretPath(p) {
			t.Errorf("isSecretPath(%q) = false", p)
		}
	}
	plain := []string{
		"models/orders.sql", "dbt_project.yml", "models/schema.yml", "dashboards/sales.yaml",
		"README.md", "macros/keys_helper.sql", "seeds/environments.csv", "models/token_usage.sql",
	}
	for _, p := range plain {
		if isSecretPath(p) {
			t.Errorf("isSecretPath(%q) = true", p)
		}
	}
}

func TestRedactJSON(t *testing.T) {
	in := `{
		"base_url": "https://bob:hunter2@api.example.test/v1",
		"headers": {"Authorization": "Bearer abc", "X-Api-Key": "k1", "Accept": "json"},
		"auth": {"type": "bearer", "token": "t0k"},
		"params": {"api_key": "k2", "limit": 100},
		"client_secret": "cs",
		"use_token": true,
		"password": "",
		"primary_key": "id",
		"resources": [{"name": "orders", "endpoint": "/orders", "dsn": "postgresql://u:p@h/db"}]
	}`
	out, err := json.Marshal(redactJSON(in))
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	for _, leak := range []string{"hunter2", "Bearer abc", "k1", "t0k", "k2", "cs\"", "u:p@"} {
		if strings.Contains(got, leak) {
			t.Errorf("leaked %q: %s", leak, got)
		}
	}
	for _, keep := range []string{`"Accept":"json"`, `"limit":100`, `"primary_key":"id"`, `"endpoint":"/orders"`, `"use_token":true`, `api.example.test/v1`} {
		if !strings.Contains(got, keep) {
			t.Errorf("dropped %q: %s", keep, got)
		}
	}
	if v := redactJSON("not json"); v != redacted {
		t.Errorf("unparseable config = %v, want it redacted whole", v)
	}
}

func TestRedactString(t *testing.T) {
	for in, want := range map[string]string{
		"connect postgresql://app:s3cret@db.example.test:5432/x failed": "connect postgresql://[redacted]@db.example.test:5432/x failed",
		"https://token@github.example.test/org/repo.git":                "https://[redacted]@github.example.test/org/repo.git",
		"plain https://example.test/a@b":                                "plain https://example.test/a@b",
		"no url here":                                                   "no url here",
	} {
		if got := redactString(in); got != want {
			t.Errorf("redactString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("short", 10); got != "short" {
		t.Errorf("got %q", got)
	}
	got := truncate("héllo wörld", 2) // cut inside the two-byte é
	if !strings.HasPrefix(got, "h…") {
		t.Errorf("truncate split a rune: %q", got)
	}
}

func TestRedactCutURL(t *testing.T) {
	for in, want := range map[string]string{
		"connect postgresql://app:s3cr…":           "connect postgresql://[redacted]…",
		"connect postgresql://[redacted]@db/x: no": "connect postgresql://[redacted]@db/x: no",
		"plain error…":                             "plain error…",
	} {
		if got := redactCutURL(in); got != want {
			t.Errorf("redactCutURL(%q) = %q, want %q", in, got, want)
		}
	}
}
