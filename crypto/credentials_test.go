package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

func validKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestAESEncryptor_RoundTrip(t *testing.T) {
	enc, err := NewAESEncryptor(validKey(t))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		plaintext string
	}{
		{"json object", `{"api_key":"secret123","password":"hunter2"}`},
		{"empty object", `{}`},
		{"nested", `{"a":{"b":"c"}}`},
		{"empty bytes", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, err := enc.Encrypt([]byte(tt.plaintext))
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}

			decrypted, err := enc.Decrypt(encrypted)
			if err != nil {
				t.Fatalf("decrypt: %v", err)
			}

			if string(decrypted) != tt.plaintext {
				t.Errorf("got %q, want %q", decrypted, tt.plaintext)
			}
		})
	}
}

func TestAESEncryptor_PrefixAndRandomness(t *testing.T) {
	enc, err := NewAESEncryptor(validKey(t))
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte(`{"key":"value"}`)

	a, _ := enc.Encrypt(plaintext)
	b, _ := enc.Encrypt(plaintext)

	if !strings.HasPrefix(a, "enc:") {
		t.Errorf("expected enc: prefix, got %q", a)
	}

	if a == b {
		t.Error("two encryptions of same plaintext should differ (random nonce)")
	}
}

// legacyEncrypt writes the pre-key-id envelope ("enc:" + base64, no id), so
// the rest of the suite can assert that rows written before this package
// learned about key ids still read.
func legacyEncrypt(t *testing.T, key, plaintext []byte) string {
	t.Helper()

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	return "enc:" + base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, plaintext, nil))
}

func TestKeyring_EnvelopeCarriesPrimaryKeyID(t *testing.T) {
	key := validKey(t)
	ring, err := NewKeyring(key)
	if err != nil {
		t.Fatal(err)
	}

	stored, err := ring.Encrypt([]byte(`{"key":"value"}`))
	if err != nil {
		t.Fatal(err)
	}

	want := "enc:" + DeriveKeyID(key) + ":"
	if !strings.HasPrefix(stored, want) {
		t.Errorf("got %q, want prefix %q", stored, want)
	}
	if ring.PrimaryKeyID() != DeriveKeyID(key) {
		t.Errorf("PrimaryKeyID %q, want %q", ring.PrimaryKeyID(), DeriveKeyID(key))
	}
}

func TestDeriveKeyID_StableAndDistinct(t *testing.T) {
	a, b := validKey(t), validKey(t)

	// Determinism is the property the whole scheme rests on: the id is never
	// stored beside the key, so a key must always answer to the same name.
	sameKey := append([]byte(nil), a...)
	if DeriveKeyID(a) != DeriveKeyID(sameKey) {
		t.Error("key id must be deterministic for the same key")
	}
	if DeriveKeyID(a) == DeriveKeyID(b) {
		t.Error("distinct keys must get distinct ids")
	}
	if got := len(DeriveKeyID(a)); got != keyIDBytes*2 {
		t.Errorf("key id length %d, want %d hex chars", got, keyIDBytes*2)
	}
	// The id must not be a prefix of the key itself, under any encoding.
	if strings.Contains(hex.EncodeToString(a), DeriveKeyID(a)) {
		t.Error("key id appears verbatim in the key material")
	}
}

// A rotation in progress: the new key writes, the retired key still reads.
func TestKeyring_ReadsPreviousKey(t *testing.T) {
	oldKey, newKey := validKey(t), validKey(t)
	plaintext := []byte(`{"api_key":"secret123"}`)

	retired, err := NewKeyring(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	underOldKey, err := retired.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	rotating, err := NewKeyring(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}

	got, err := rotating.Decrypt(underOldKey)
	if err != nil {
		t.Fatalf("decrypt under retained key: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Errorf("got %q, want %q", got, plaintext)
	}

	// ...and it writes under the new one, which is what lets the rewrap
	// sweep find what is still outstanding.
	rewritten, err := rotating.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rewritten, "enc:"+DeriveKeyID(newKey)+":") {
		t.Errorf("rewritten value %q not under the new key", rewritten)
	}
}

// Once the old key leaves the ring, its ciphertext must fail loudly and name
// the key it wants — not fall back to plaintext or to a wrong key.
func TestKeyring_UnknownKeyIDFailsLoudly(t *testing.T) {
	oldKey, newKey := validKey(t), validKey(t)

	retired, err := NewKeyring(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	underOldKey, err := retired.Encrypt([]byte(`{"a":"b"}`))
	if err != nil {
		t.Fatal(err)
	}

	rotated, err := NewKeyring(newKey)
	if err != nil {
		t.Fatal(err)
	}

	_, err = rotated.Decrypt(underOldKey)
	if err == nil {
		t.Fatal("expected an error for a key that is not in the ring")
	}
	if !strings.Contains(err.Error(), DeriveKeyID(oldKey)) {
		t.Errorf("error %q does not name the missing key %s", err, DeriveKeyID(oldKey))
	}
}

func TestKeyring_ReadsLegacyUntaggedCiphertext(t *testing.T) {
	oldKey, newKey := validKey(t), validKey(t)
	plaintext := []byte(`{"password":"hunter2"}`)

	t.Run("single key", func(t *testing.T) {
		ring, err := NewKeyring(oldKey)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ring.Decrypt(legacyEncrypt(t, oldKey, plaintext))
		if err != nil {
			t.Fatalf("decrypt legacy: %v", err)
		}
		if string(got) != string(plaintext) {
			t.Errorf("got %q, want %q", got, plaintext)
		}
	})

	// A legacy value carries no id, so mid-rotation it can only be found by
	// trying the ring — including the key that is no longer primary.
	t.Run("under a previous key", func(t *testing.T) {
		ring, err := NewKeyring(newKey, oldKey)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ring.Decrypt(legacyEncrypt(t, oldKey, plaintext))
		if err != nil {
			t.Fatalf("decrypt legacy: %v", err)
		}
		if string(got) != string(plaintext) {
			t.Errorf("got %q, want %q", got, plaintext)
		}
	})

	t.Run("under no key in the ring", func(t *testing.T) {
		ring, err := NewKeyring(newKey)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ring.Decrypt(legacyEncrypt(t, oldKey, plaintext)); err == nil {
			t.Error("expected an error when no key opens the ciphertext")
		}
	})
}

func TestNewKeyring_RejectsDuplicateAndInvalidKeys(t *testing.T) {
	key := validKey(t)

	if _, err := NewKeyring(key, key); err == nil {
		t.Error("expected an error when the same key is both primary and previous")
	}
	if _, err := NewKeyring(key, make([]byte, 16)); err == nil {
		t.Error("expected an error for a short previous key")
	}
}

func TestKeyring_KeyIDs(t *testing.T) {
	a, b := validKey(t), validKey(t)
	ring, err := NewKeyring(a, b)
	if err != nil {
		t.Fatal(err)
	}

	got := ring.KeyIDs()
	want := []string{DeriveKeyID(a), DeriveKeyID(b)}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("KeyIDs()[%d] = %q, want %q (primary first)", i, got[i], want[i])
		}
	}
}

func TestAESEncryptor_InvalidKeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 24, 31, 33, 64} {
		_, err := NewAESEncryptor(make([]byte, n))
		if err == nil {
			t.Errorf("expected error for key length %d", n)
		}
	}
}

func TestAESEncryptor_CorruptedCiphertext(t *testing.T) {
	enc, err := NewAESEncryptor(validKey(t))
	if err != nil {
		t.Fatal(err)
	}

	_, err = enc.Decrypt("enc:bm90LXZhbGlkLWNpcGhlcnRleHQ=")
	if err == nil {
		t.Error("expected error for corrupted ciphertext")
	}
}

func TestAESEncryptor_PlaintextFallback(t *testing.T) {
	enc, err := NewAESEncryptor(validKey(t))
	if err != nil {
		t.Fatal(err)
	}

	// Data without enc: prefix should be returned as-is
	plain := `{"api_key":"test"}`
	got, err := enc.Decrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != plain {
		t.Errorf("got %q, want %q", got, plain)
	}
}

func TestNoOpEncryptor(t *testing.T) {
	var enc NoOpEncryptor
	plaintext := `{"key":"value"}`

	encrypted, err := enc.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	if encrypted != plaintext {
		t.Errorf("NoOp encrypt should pass through, got %q", encrypted)
	}

	decrypted, err := enc.Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != plaintext {
		t.Errorf("NoOp decrypt should pass through, got %q", decrypted)
	}
}
