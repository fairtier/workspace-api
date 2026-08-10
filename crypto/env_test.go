package crypto

import (
	"encoding/base64"
	"strings"
	"testing"
)

func encodeKey(t *testing.T, key []byte) string {
	t.Helper()
	return base64.StdEncoding.EncodeToString(key)
}

func TestEncryptorFromEnv_UnsetMeansNoEncryption(t *testing.T) {
	t.Setenv(KeyEnvVar, "")
	t.Setenv(PreviousKeysEnvVar, "")

	enc, err := EncryptorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	// A typed nil pointer here would read as a non-nil Encryptor and turn
	// every `enc == nil` guard downstream into a nil-pointer panic.
	if enc != nil {
		t.Errorf("got %#v, want a nil Encryptor", enc)
	}
}

func TestEncryptorFromEnv_BuildsTheRing(t *testing.T) {
	primary, oldA, oldB := validKey(t), validKey(t), validKey(t)

	t.Setenv(KeyEnvVar, encodeKey(t, primary))
	t.Setenv(PreviousKeysEnvVar, encodeKey(t, oldA)+","+encodeKey(t, oldB))

	enc, err := EncryptorFromEnv()
	if err != nil {
		t.Fatal(err)
	}

	ring, ok := enc.(*Keyring)
	if !ok {
		t.Fatalf("got %T, want *Keyring", enc)
	}
	if ring.PrimaryKeyID() != DeriveKeyID(primary) {
		t.Errorf("primary %q, want %q", ring.PrimaryKeyID(), DeriveKeyID(primary))
	}

	want := []string{DeriveKeyID(primary), DeriveKeyID(oldA), DeriveKeyID(oldB)}
	got := ring.KeyIDs()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("KeyIDs %v, want %v", got, want)
	}
}

func TestEncryptorFromEnv_TolerantParsing(t *testing.T) {
	primary, old := validKey(t), validKey(t)

	// Whitespace and a trailing comma are what a multi-line Kubernetes Secret
	// or a hand-edited tfvars produces; neither should fail a rotation.
	t.Setenv(KeyEnvVar, " "+encodeKey(t, primary)+" ")
	t.Setenv(PreviousKeysEnvVar, encodeKey(t, old)+", ,")

	enc, err := EncryptorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(enc.(*Keyring).KeyIDs()); got != 2 {
		t.Errorf("ring holds %d keys, want 2", got)
	}
}

func TestEncryptorFromEnv_NamesTheOffendingVariable(t *testing.T) {
	t.Run("primary", func(t *testing.T) {
		t.Setenv(KeyEnvVar, "not-base64!!")
		t.Setenv(PreviousKeysEnvVar, "")

		_, err := EncryptorFromEnv()
		if err == nil || !strings.Contains(err.Error(), KeyEnvVar) {
			t.Errorf("error %v does not name %s", err, KeyEnvVar)
		}
	})

	t.Run("previous", func(t *testing.T) {
		t.Setenv(KeyEnvVar, encodeKey(t, validKey(t)))
		t.Setenv(PreviousKeysEnvVar, "not-base64!!")

		_, err := EncryptorFromEnv()
		if err == nil || !strings.Contains(err.Error(), PreviousKeysEnvVar) {
			t.Errorf("error %v does not name %s", err, PreviousKeysEnvVar)
		}
	})

	t.Run("wrong length", func(t *testing.T) {
		t.Setenv(KeyEnvVar, base64.StdEncoding.EncodeToString(make([]byte, 16)))
		t.Setenv(PreviousKeysEnvVar, "")

		if _, err := EncryptorFromEnv(); err == nil {
			t.Error("expected an error for a 16-byte key")
		}
	})
}
