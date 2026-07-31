package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

// HMACFingerprinter produces deterministic keyed fingerprints
// (HMAC-SHA256). Used by the pipeline mirror to decide whether a committed
// age-encrypted credential file is current without decrypting it (age
// output is non-deterministic). Keyed so that someone holding only the
// fingerprint table cannot brute-force low-entropy credentials against it.
type HMACFingerprinter struct {
	key []byte
}

// NewHMACFingerprinter keys the fingerprinter (any non-empty key; in
// practice the decoded CREDENTIAL_ENCRYPTION_KEY — reusing it across
// AES-GCM and HMAC-SHA256 is safe, the algorithms are unrelated).
func NewHMACFingerprinter(key []byte) *HMACFingerprinter {
	return &HMACFingerprinter{key: key}
}

func (f *HMACFingerprinter) Fingerprint(parts ...[]byte) string {
	return fingerprint(hmac.New(sha256.New, f.key), parts)
}

// SHA256Fingerprinter is the keyless fallback (local dev without
// CREDENTIAL_ENCRYPTION_KEY — no real credentials there by rule).
type SHA256Fingerprinter struct{}

func (SHA256Fingerprinter) Fingerprint(parts ...[]byte) string {
	return fingerprint(sha256.New(), parts)
}

// fingerprint hashes length-prefixed parts, so part boundaries are
// unambiguous ("ab","c" never collides with "a","bc").
func fingerprint(h hash.Hash, parts [][]byte) string {
	var lenBuf [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(p)))
		h.Write(lenBuf[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}
