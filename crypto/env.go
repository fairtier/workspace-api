package crypto

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

// The environment contract for at-rest credential encryption. Every binary
// that touches the same database must build the same ring: if one held only
// the new key while another still wrote under the old, each would produce rows
// the other cannot read. That is why this lives here rather than being parsed
// again in each main().
const (
	// KeyEnvVar holds the base64 32-byte key everything is written under.
	KeyEnvVar = "CREDENTIAL_ENCRYPTION_KEY"

	// PreviousKeysEnvVar holds keys that are no longer written under but must
	// still be readable — comma-separated base64, set only while a rotation is
	// in flight.
	PreviousKeysEnvVar = "CREDENTIAL_ENCRYPTION_KEYS_PREVIOUS"
)

// EncryptorFromEnv builds the at-rest encryptor from the environment, or
// returns a nil Encryptor when KeyEnvVar is unset (local development, where
// values are stored in plaintext by rule).
//
// The nil is a genuine nil interface, not a typed nil pointer, so the
// `enc == nil` checks the repositories and sweeps make keep working.
func EncryptorFromEnv() (Encryptor, error) {
	primary, err := decodeKey(KeyEnvVar, os.Getenv(KeyEnvVar))
	if err != nil {
		return nil, err
	}
	if primary == nil {
		return nil, nil
	}

	previous, err := decodePreviousKeys()
	if err != nil {
		return nil, err
	}

	ring, err := NewKeyring(primary, previous...)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

// decodePreviousKeys parses the comma-separated retired keys, ignoring empty
// entries so a trailing comma or an unset-but-present variable is not an error.
func decodePreviousKeys() ([][]byte, error) {
	raw := os.Getenv(PreviousKeysEnvVar)
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var keys [][]byte
	for i, field := range strings.Split(raw, ",") {
		key, err := decodeKey(fmt.Sprintf("%s[%d]", PreviousKeysEnvVar, i), field)
		if err != nil {
			return nil, err
		}
		if key != nil {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// decodeKey base64-decodes one key, naming the variable it came from on
// failure — an operator pasting a key into the wrong slot should not have to
// guess which one.
func decodeKey(name, value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	key, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("crypto: decode %s: %w", name, err)
	}
	return key, nil
}
