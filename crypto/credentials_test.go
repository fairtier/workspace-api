package crypto

import (
	"crypto/rand"
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
