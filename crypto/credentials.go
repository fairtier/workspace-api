package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

const encPrefix = "enc:"

// keyIDLabel domain-separates the key-id derivation from every other use of
// the same key material (AES-GCM encryption, the HMAC fingerprinter).
const keyIDLabel = "fairtier/credential-key-id/v1"

// keyIDBytes is how much of the derived digest becomes the id. Four bytes (8
// hex chars) is short enough to read in a log line and long enough that the
// handful of keys a ring ever holds cannot collide by accident — and
// NewKeyring rejects a collision outright rather than guessing.
const keyIDBytes = 4

// Encryptor encrypts and decrypts credential data at rest.
type Encryptor interface {
	Encrypt(plaintext []byte) (string, error)
	Decrypt(stored string) ([]byte, error)
}

// KeyIdentified is implemented by encryptors that stamp a key id into what
// they write. Callers that need to know which key a value would be written
// under — the rewrap sweeps, the startup log line — type-assert for it, so
// NoOpEncryptor and any future keyless implementation stay usable as plain
// Encryptors.
type KeyIdentified interface {
	PrimaryKeyID() string
}

// Keyring implements Encryptor with AES-256-GCM under one write key and any
// number of read-only keys retained for decryption.
//
// The stored form is:
//
//	enc:<key id>:<base64(nonce+ciphertext)>   — written by this package
//	enc:<base64(nonce+ciphertext)>            — legacy, written before key ids
//	<anything else>                           — plaintext, returned as-is
//
// Base64's alphabet contains no ":", so the separator distinguishes the two
// encrypted forms unambiguously at any payload length.
//
// A legacy value carries no evidence of which key produced it, so Decrypt
// tries every key in the ring (GCM's tag makes a wrong key a clean failure,
// never a wrong plaintext). A tagged value is looked up by id and fails loudly
// when that key is not in the ring — which is what makes "no ciphertext is
// still under the retired key" a question SQL can answer, instead of one that
// needs trial decryption of every row.
type Keyring struct {
	primary keyEntry
	byID    map[string]keyEntry
	order   []keyEntry // primary first, then previous — legacy decrypt order
}

// keyEntry is one key in the ring, with its derived id.
type keyEntry struct {
	id  string
	gcm cipher.AEAD
}

// NewAESEncryptor creates a single-key Keyring from a 32-byte key. Equivalent
// to NewKeyring with no previous keys.
func NewAESEncryptor(key []byte) (*Keyring, error) {
	return NewKeyring(key)
}

// NewKeyring builds a ring that writes under primary and can read anything
// written under primary or any of previous. Every key must be 32 bytes, and no
// two may derive the same id (which in practice means: the same key twice).
func NewKeyring(primary []byte, previous ...[]byte) (*Keyring, error) {
	primaryEntry, err := newKeyEntry(primary)
	if err != nil {
		return nil, fmt.Errorf("crypto: primary key: %w", err)
	}

	ring := &Keyring{
		primary: primaryEntry,
		byID:    map[string]keyEntry{primaryEntry.id: primaryEntry},
		order:   []keyEntry{primaryEntry},
	}

	for i, key := range previous {
		entry, err := newKeyEntry(key)
		if err != nil {
			return nil, fmt.Errorf("crypto: previous key %d: %w", i, err)
		}
		if _, dup := ring.byID[entry.id]; dup {
			return nil, fmt.Errorf("crypto: previous key %d duplicates key %s already in the ring", i, entry.id)
		}
		ring.byID[entry.id] = entry
		ring.order = append(ring.order, entry)
	}

	return ring, nil
}

// newKeyEntry validates a key, derives its id, and prepares its AEAD.
func newKeyEntry(key []byte) (keyEntry, error) {
	if len(key) != 32 {
		return keyEntry{}, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return keyEntry{}, fmt.Errorf("new cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return keyEntry{}, fmt.Errorf("new gcm: %w", err)
	}

	return keyEntry{id: DeriveKeyID(key), gcm: gcm}, nil
}

// DeriveKeyID names a key by a truncated HMAC of the key under a fixed label.
// Deterministic, so the id needs no bookkeeping — the same key always answers
// to the same name — and one-way, so an id in a log line or a database column
// reveals nothing about the key.
func DeriveKeyID(key []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(keyIDLabel))
	return hex.EncodeToString(mac.Sum(nil)[:keyIDBytes])
}

// PrimaryKeyID is the id stamped into everything Encrypt writes.
func (k *Keyring) PrimaryKeyID() string { return k.primary.id }

// KeyIDs lists every key the ring can read, primary first.
func (k *Keyring) KeyIDs() []string {
	ids := make([]string, 0, len(k.order))
	for _, e := range k.order {
		ids = append(ids, e.id)
	}
	return ids
}

// Encrypt returns "enc:<primary key id>:" + base64(nonce + ciphertext).
func (k *Keyring) Encrypt(plaintext []byte) (string, error) {
	nonce := make([]byte, k.primary.gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("crypto: generate nonce: %w", err)
	}

	ciphertext := k.primary.gcm.Seal(nonce, nonce, plaintext, nil)
	return encPrefix + k.primary.id + ":" + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt reverses Encrypt, and also reads the legacy untagged form and
// plaintext (a value with no "enc:" prefix is returned as-is).
func (k *Keyring) Decrypt(stored string) ([]byte, error) {
	if !strings.HasPrefix(stored, encPrefix) {
		return []byte(stored), nil
	}

	keyID, payload := splitEnvelope(strings.TrimPrefix(stored, encPrefix))

	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("crypto: base64 decode: %w", err)
	}

	if keyID == "" {
		return k.openUntagged(data)
	}

	entry, ok := k.byID[keyID]
	if !ok {
		return nil, fmt.Errorf("crypto: ciphertext is under key %s, which is not in the ring (have %s)",
			keyID, strings.Join(k.KeyIDs(), ", "))
	}

	plaintext, err := open(entry, data)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt under key %s: %w", keyID, err)
	}
	return plaintext, nil
}

// splitEnvelope separates an optional key id from the base64 payload. Legacy
// values have no id, and return "".
func splitEnvelope(body string) (keyID, payload string) {
	if before, after, ok := strings.Cut(body, ":"); ok {
		return before, after
	}
	return "", body
}

// openUntagged tries every key in the ring against a legacy (untagged)
// ciphertext.
func (k *Keyring) openUntagged(data []byte) ([]byte, error) {
	for _, entry := range k.order {
		if plaintext, err := open(entry, data); err == nil {
			return plaintext, nil
		}
	}
	return nil, fmt.Errorf("crypto: decrypt: untagged ciphertext opened under none of the ring's keys (%s)",
		strings.Join(k.KeyIDs(), ", "))
}

// open splits the nonce from the ciphertext and authenticates it under one key.
func open(entry keyEntry, data []byte) ([]byte, error) {
	nonceSize := entry.gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	return entry.gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
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
