package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

const encPrefix = "enc:"

// Encryptor encrypts and decrypts credential data at rest.
type Encryptor interface {
	Encrypt(plaintext []byte) (string, error)
	Decrypt(stored string) ([]byte, error)
}

// AESEncryptor implements Encryptor using AES-256-GCM.
type AESEncryptor struct {
	gcm cipher.AEAD
}

// NewAESEncryptor creates an AESEncryptor from a 32-byte key.
func NewAESEncryptor(key []byte) (*AESEncryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: new cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: new gcm: %w", err)
	}

	return &AESEncryptor{gcm: gcm}, nil
}

// Encrypt returns "enc:" + base64(nonce + ciphertext).
func (e *AESEncryptor) Encrypt(plaintext []byte) (string, error) {
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("crypto: generate nonce: %w", err)
	}

	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	return encPrefix + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt strips the "enc:" prefix, base64-decodes, splits nonce from ciphertext,
// and returns the plaintext. If the stored value has no "enc:" prefix, it is
// returned as-is (plaintext backward compatibility).
func (e *AESEncryptor) Decrypt(stored string) ([]byte, error) {
	if !strings.HasPrefix(stored, encPrefix) {
		return []byte(stored), nil
	}

	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, encPrefix))
	if err != nil {
		return nil, fmt.Errorf("crypto: base64 decode: %w", err)
	}

	nonceSize := e.gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("crypto: ciphertext too short")
	}

	plaintext, err := e.gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt: %w", err)
	}

	return plaintext, nil
}

// NoOpEncryptor passes data through without encryption. Useful for tests and
// local development without CREDENTIAL_ENCRYPTION_KEY.
type NoOpEncryptor struct{}

func (NoOpEncryptor) Encrypt(plaintext []byte) (string, error) {
	return string(plaintext), nil
}

func (NoOpEncryptor) Decrypt(stored string) ([]byte, error) {
	return []byte(stored), nil
}
