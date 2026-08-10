package postgres

import (
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/crypto"
)

func TestWhereClause(t *testing.T) {
	tests := []struct {
		name    string
		columns []string
		want    string
	}{
		{"single", []string{"id"}, "id = $2"},
		{"composite", []string{"customer_slug", "provider"}, "customer_slug = $2 AND provider = $3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := whereClause(tt.columns, 2); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct{ in, want string }{
		{"pipelines", `"pipelines"`},
		{"weird name", `"weird name"`},
		{`quo"te`, `"quo""te"`},
	}
	for _, tt := range tests {
		if got := quoteIdentifier(tt.in); got != tt.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// The inventory drives destructive UPDATEs, so a malformed entry must not be
// discovered against a live database.
func TestWorkspaceEncryptedColumns_WellFormed(t *testing.T) {
	seen := map[string]bool{}

	for _, c := range WorkspaceEncryptedColumns() {
		if c.Table == "" || c.Column == "" || len(c.KeyColumns) == 0 {
			t.Errorf("incomplete entry: %+v", c)
			continue
		}
		key := c.Table + "." + c.Column
		if seen[key] {
			t.Errorf("%s listed twice — it would be rewrapped twice", key)
		}
		seen[key] = true
	}

	// The four columns the workspace plane encrypts today. A new one is meant
	// to fail here first, so that adding it to the inventory is part of the
	// same change rather than a step remembered at the next rotation.
	for _, want := range []string{
		"pipelines.source_credentials",
		"transformations.git_credentials",
		"google_oauth_grants.refresh_token",
		"customer_oauth_clients.client_secret",
	} {
		if !seen[want] {
			t.Errorf("%s missing from the encrypted-column inventory", want)
		}
	}
}

// A NoOpEncryptor (local dev, no key) has no key to rotate to, and must not be
// mistaken for one that does.
func TestRewrapEncrypted_SkipsKeylessEncryptors(t *testing.T) {
	n, err := RewrapEncrypted(nil, nil, WorkspaceEncryptedColumns())
	if err != nil || n != 0 {
		t.Errorf("nil encryptor: got (%d, %v), want (0, nil)", n, err)
	}

	n, err = RewrapEncrypted(nil, crypto.NoOpEncryptor{}, WorkspaceEncryptedColumns())
	if err != nil || n != 0 {
		t.Errorf("NoOpEncryptor: got (%d, %v), want (0, nil)", n, err)
	}
}

// The pattern both the rewrap selection and the audit negate. It must anchor
// on the id AND its trailing colon: without the colon a key id that prefixes
// another would let one key's ciphertext pass as the other's.
func TestPrimaryEnvelopePattern(t *testing.T) {
	if got, want := primaryEnvelopePattern("0123abcd"), "enc:0123abcd:%"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// What the pattern is matched against is what Encrypt writes, so the two
	// have to agree on the envelope down to the separator.
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	ring, err := crypto.NewKeyring(key)
	if err != nil {
		t.Fatal(err)
	}
	written, err := ring.Encrypt([]byte(`{"a":"b"}`))
	if err != nil {
		t.Fatal(err)
	}

	prefix := strings.TrimSuffix(primaryEnvelopePattern(ring.PrimaryKeyID()), "%")
	if !strings.HasPrefix(written, prefix) {
		t.Errorf("Encrypt wrote %q, which the sweep's pattern %q would treat as stale", written, prefix+"%")
	}
}
