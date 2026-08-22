package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// testJWKS returns a Keyfunc over a fresh ed25519 key and a signer for it.
//
// A twin of core's helper of the same name: the plain-HTTP handlers here
// authenticate through core.UserAuth and so need real signed tokens, and a
// test helper is not worth exporting from the shared kernel to share.
func testJWKS(t *testing.T) (keyfunc.Keyfunc, func(claims jwt.MapClaims) string) {
	t.Helper()
	ctx := context.Background()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwk, err := jwkset.NewJWKFromKey(pub, jwkset.JWKOptions{
		Metadata: jwkset.JWKMetadataOptions{KID: "test-key", ALG: jwkset.AlgEdDSA, USE: jwkset.UseSig},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := jwkset.NewMemoryStorage()
	if err := store.KeyWrite(ctx, jwk); err != nil {
		t.Fatal(err)
	}
	jwks, err := keyfunc.New(keyfunc.Options{Ctx: ctx, Storage: store})
	if err != nil {
		t.Fatal(err)
	}

	sign := func(claims jwt.MapClaims) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		token.Header["kid"] = "test-key"
		s, err := token.SignedString(priv)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	return jwks, sign
}
