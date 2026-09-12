package main

import (
	"log/slog"
	"testing"

	"github.com/fairtier/workspace-api/relay"
)

func envFunc(env map[string]string) func(string) string {
	return func(k string) string { return env[k] }
}

func TestRelayIdentity(t *testing.T) {
	t.Run("generic names win", func(t *testing.T) {
		u, id, sec := relayIdentity(envFunc(map[string]string{
			"FAIRTIER_RELAY_TOKEN_URL":           "http://casdoor/token",
			"FAIRTIER_RELAY_OIDC_CLIENT_ID":      "gid",
			"FAIRTIER_RELAY_OIDC_CLIENT_SECRET":  "gsec",
			"FAIRTIER_ASSIST_TOKEN_URL":          "http://old/token",
			"FAIRTIER_ASSIST_OIDC_CLIENT_ID":     "oid",
			"FAIRTIER_ASSIST_OIDC_CLIENT_SECRET": "osec",
		}))
		if u != "http://casdoor/token" || id != "gid" || sec != "gsec" {
			t.Errorf("got %q %q %q", u, id, sec)
		}
	})
	t.Run("assist names are the fallback", func(t *testing.T) {
		u, id, sec := relayIdentity(envFunc(map[string]string{
			"FAIRTIER_ASSIST_TOKEN_URL":          "http://old/token",
			"FAIRTIER_ASSIST_OIDC_CLIENT_ID":     "oid",
			"FAIRTIER_ASSIST_OIDC_CLIENT_SECRET": "osec",
		}))
		if u != "http://old/token" || id != "oid" || sec != "osec" {
			t.Errorf("got %q %q %q", u, id, sec)
		}
	})
}

func TestBuildAlertRelay(t *testing.T) {
	logger := slog.Default()
	identity := map[string]string{
		"FAIRTIER_RELAY_TOKEN_URL":          "http://casdoor/token",
		"FAIRTIER_RELAY_OIDC_CLIENT_ID":     "id",
		"FAIRTIER_RELAY_OIDC_CLIENT_SECRET": "sec",
	}

	t.Run("unset URL keeps alerts off", func(t *testing.T) {
		if got := buildAlertRelay(envFunc(identity), logger); got != nil {
			t.Errorf("want nil, got %T", got)
		}
	})
	t.Run("URL without an identity keeps alerts off", func(t *testing.T) {
		if got := buildAlertRelay(envFunc(map[string]string{"FAIRTIER_ALERT_RELAY_URL": "https://api"}), logger); got != nil {
			t.Errorf("want nil, got %T", got)
		}
	})
	t.Run("URL plus identity builds the relay client", func(t *testing.T) {
		env := map[string]string{"FAIRTIER_ALERT_RELAY_URL": "https://api/"}
		for k, v := range identity {
			env[k] = v
		}
		got := buildAlertRelay(envFunc(env), logger)
		c, ok := got.(*relay.AlertClient)
		if !ok {
			t.Fatalf("want *relay.AlertClient, got %T", got)
		}
		if c.BaseURL != "https://api" || c.Tokens.TokenURL != "http://casdoor/token" {
			t.Errorf("client = %+v tokens = %+v", c, c.Tokens)
		}
	})
}
