package core

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTokenFromHeader(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		want    string
		wantErr bool
	}{
		{name: "valid bearer", header: "Bearer abc123", want: "abc123"},
		{name: "valid with long token", header: "Bearer eyJhbGciOiJSUzI1NiJ9.test.sig", want: "eyJhbGciOiJSUzI1NiJ9.test.sig"},
		{name: "empty header", header: "", wantErr: true},
		{name: "no bearer prefix", header: "abc123", wantErr: true},
		{name: "basic auth", header: "Basic abc123", wantErr: true},
		{name: "bearer lowercase", header: "bearer abc123", wantErr: true},
		{name: "bearer only", header: "Bearer ", wantErr: true},
		{name: "bearer no space", header: "Bearerabc123", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tokenFromHeader(tt.header)
			if (err != nil) != tt.wantErr {
				t.Errorf("tokenFromHeader(%q) error = %v, wantErr %v", tt.header, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("tokenFromHeader(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

// testJWKS returns a Keyfunc over a fresh ed25519 key and a signer for it.
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

func TestNewInternalAuthInterceptor(t *testing.T) {
	jwks, sign := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()

	tests := []struct {
		name       string
		authHeader string
		wantCode   connect.Code // 0 = expect success
		wantCaller InternalCaller
	}{
		{
			name:       "valid dlt-worker token",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "admin/dlt-worker-acme", "exp": exp}),
			wantCaller: InternalCaller{App: "dlt-worker-acme"},
		},
		{
			name:     "missing header",
			wantCode: connect.CodeUnauthenticated,
		},
		{
			name:       "expired token",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "admin/dlt-worker-acme", "exp": time.Now().Add(-time.Hour).Unix()}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "token without exp",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "admin/dlt-worker-acme"}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name: "user token is not a service account",
			// Only "admin/<app>" subjects are service accounts; a
			// human's subject is their own Casdoor user id.
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "customer-acme/alice", "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "missing subject",
			authHeader: "Bearer " + sign(jwt.MapClaims{"exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "garbage token",
			authHeader: "Bearer not.a.jwt",
			wantCode:   connect.CodeUnauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Box trust nil: the central path must behave identically with
			// box-issuer trust disabled.
			interceptor := NewInternalAuthInterceptor(jwks, nil, slog.Default())

			var gotCaller InternalCaller
			next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
				gotCaller = InternalCallerFromContext(ctx)
				return connect.NewResponse(&emptypb.Empty{}), nil
			}

			req := connect.NewRequest(&emptypb.Empty{})
			if tt.authHeader != "" {
				req.Header().Set("Authorization", tt.authHeader)
			}

			_, err := interceptor(next)(context.Background(), req)
			if tt.wantCode != 0 {
				if connect.CodeOf(err) != tt.wantCode {
					t.Fatalf("interceptor error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("interceptor error = %v", err)
			}
			if gotCaller != tt.wantCaller {
				t.Errorf("caller = %+v, want %+v", gotCaller, tt.wantCaller)
			}
		})
	}
}

// fakeCustomerChecker gates box-issuer JWKS fetches in tests.
type fakeCustomerChecker struct {
	vmSlugs map[string]bool
}

func (f *fakeCustomerChecker) IsVMCustomer(_ context.Context, slug string) (bool, error) {
	return f.vmSlugs[slug], nil
}

// testBoxTrust builds a BoxIssuerTrust whose keyfunc construction is stubbed
// to return boxJWKS (no network). It reports how many times a keyfunc was
// constructed via the returned counter.
func testBoxTrust(t *testing.T, baseDomain string, checker CustomerChecker, boxJWKS keyfunc.Keyfunc) (*BoxIssuerTrust, *int) {
	t.Helper()
	trust := NewBoxIssuerTrust(context.Background(), baseDomain, checker)
	if trust == nil {
		t.Fatal("NewBoxIssuerTrust returned nil for non-empty base domain")
	}
	fetches := 0
	trust.newKeyfunc = func(_ context.Context, _ string) (keyfunc.Keyfunc, error) {
		fetches++
		return boxJWKS, nil
	}
	return trust, &fetches
}

func TestNewInternalAuthInterceptorBoxIssuer(t *testing.T) {
	centralJWKS, signCentral := testJWKS(t)
	boxJWKS, signBox := testJWKS(t)
	_, signOther := testJWKS(t) // a key trusted by nobody
	exp := time.Now().Add(time.Hour).Unix()

	const acmeIssuer = "https://auth.customer-acme.fairtier.com"

	tests := []struct {
		name        string
		vmSlugs     map[string]bool
		authHeader  string
		wantCode    connect.Code // 0 = expect success
		wantCaller  InternalCaller
		wantFetches int
	}{
		{
			name:        "valid box token binds slug from issuer",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": exp}),
			wantCaller:  InternalCaller{App: "dlt-worker", Slug: "acme", Issuer: acmeIssuer},
			wantFetches: 1,
		},
		{
			name:    "box-shaped issuer signed with the central key",
			vmSlugs: map[string]bool{"acme": true},
			// The issuer routes to the box trust anchor, whose key does not
			// verify a central-signed token — no silent fallback to central.
			authHeader:  "Bearer " + signCentral(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 1,
		},
		{
			name:        "box-shaped issuer signed with an untrusted key",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signOther(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 1,
		},
		{
			name:    "unknown slug never fetches a JWKS",
			vmSlugs: map[string]bool{},
			authHeader: "Bearer " + signBox(jwt.MapClaims{
				"sub": "admin/dlt-worker", "iss": "https://auth.customer-evil.fairtier.com", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:    "issuer suffix trick falls through to central and fails",
			vmSlugs: map[string]bool{"acme": true},
			authHeader: "Bearer " + signBox(jwt.MapClaims{
				"sub": "admin/dlt-worker", "iss": "https://auth.customer-acme.fairtier.com.evil.com", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:    "issuer with explicit port is not box-shaped",
			vmSlugs: map[string]bool{"acme": true},
			authHeader: "Bearer " + signBox(jwt.MapClaims{
				"sub": "admin/dlt-worker", "iss": acmeIssuer + ":443", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:    "uppercase issuer host is not box-shaped",
			vmSlugs: map[string]bool{"acme": true},
			authHeader: "Bearer " + signBox(jwt.MapClaims{
				"sub": "admin/dlt-worker", "iss": "https://auth.customer-ACME.fairtier.com", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:    "empty slug is not box-shaped",
			vmSlugs: map[string]bool{"acme": true},
			authHeader: "Bearer " + signBox(jwt.MapClaims{
				"sub": "admin/dlt-worker", "iss": "https://auth.customer-.fairtier.com", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:        "box token without issuer routes to central and fails",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 0,
		},
		{
			name:        "expired box token",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": time.Now().Add(-time.Hour).Unix()}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 1,
		},
		{
			name:        "box user token is not a service account",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signBox(jwt.MapClaims{"sub": "acme/alice", "iss": acmeIssuer, "exp": exp}),
			wantCode:    connect.CodeUnauthenticated,
			wantFetches: 1,
		},
		{
			name:        "central token still authenticates with box trust enabled",
			vmSlugs:     map[string]bool{"acme": true},
			authHeader:  "Bearer " + signCentral(jwt.MapClaims{"sub": "admin/dlt-worker-acme", "exp": exp}),
			wantCaller:  InternalCaller{App: "dlt-worker-acme"},
			wantFetches: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trust, fetches := testBoxTrust(t, "fairtier.com", &fakeCustomerChecker{vmSlugs: tt.vmSlugs}, boxJWKS)
			interceptor := NewInternalAuthInterceptor(centralJWKS, trust, slog.Default())

			var gotCaller InternalCaller
			next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
				gotCaller = InternalCallerFromContext(ctx)
				return connect.NewResponse(&emptypb.Empty{}), nil
			}

			req := connect.NewRequest(&emptypb.Empty{})
			req.Header().Set("Authorization", tt.authHeader)

			_, err := interceptor(next)(context.Background(), req)
			if *fetches != tt.wantFetches {
				t.Errorf("JWKS keyfunc constructions = %d, want %d", *fetches, tt.wantFetches)
			}
			if tt.wantCode != 0 {
				if connect.CodeOf(err) != tt.wantCode {
					t.Fatalf("interceptor error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("interceptor error = %v", err)
			}
			if gotCaller != tt.wantCaller {
				t.Errorf("caller = %+v, want %+v", gotCaller, tt.wantCaller)
			}
		})
	}
}

func TestBoxIssuerTrustCachesKeyfunc(t *testing.T) {
	boxJWKS, signBox := testJWKS(t)
	centralJWKS, _ := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()
	const iss = "https://auth.customer-acme.fairtier.com"

	trust, fetches := testBoxTrust(t, "fairtier.com", &fakeCustomerChecker{vmSlugs: map[string]bool{"acme": true}}, boxJWKS)
	interceptor := NewInternalAuthInterceptor(centralJWKS, trust, slog.Default())
	next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		return connect.NewResponse(&emptypb.Empty{}), nil
	}

	for range 3 {
		req := connect.NewRequest(&emptypb.Empty{})
		req.Header().Set("Authorization", "Bearer "+signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": iss, "exp": exp}))
		if _, err := interceptor(next)(context.Background(), req); err != nil {
			t.Fatalf("interceptor error = %v", err)
		}
	}
	if *fetches != 1 {
		t.Errorf("JWKS keyfunc constructions = %d, want 1 (cached per issuer)", *fetches)
	}
}

// TestNewInternalAuthInterceptorPinnedBox covers the box-side trust anchor: a
// single pinned issuer, no regex, no CustomerChecker. The load-bearing case is
// the first — the box's own dlt-worker subject is "admin/dlt-worker" (no
// "-<slug>" suffix), so the slug can only come from the issuer match, exactly
// as the regex-based BoxIssuerTrust does centrally.
func TestNewInternalAuthInterceptorPinnedBox(t *testing.T) {
	boxJWKS, signBox := testJWKS(t)
	fallthroughJWKS, _ := testJWKS(t) // stands in for the box's own jwks on the central path
	_, signOther := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()

	const acmeIssuer = "https://auth.customer-acme.fairtier.com"

	tests := []struct {
		name       string
		authHeader string
		wantCode   connect.Code // 0 = expect success
		wantCaller InternalCaller
	}{
		{
			name:       "box worker token binds the pinned slug",
			authHeader: "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": exp}),
			wantCaller: InternalCaller{App: "dlt-worker", Slug: "acme", Issuer: acmeIssuer},
		},
		{
			name: "foreign issuer does not take the box path",
			// Signed by the box key but a different issuer: routes to the
			// fallthrough anchor, which does not verify it.
			authHeader: "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": "https://auth.customer-other.fairtier.com", "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "pinned issuer signed with an untrusted key",
			authHeader: "Bearer " + signOther(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "box user token is not a service account",
			authHeader: "Bearer " + signBox(jwt.MapClaims{"sub": "acme/alice", "iss": acmeIssuer, "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "expired box token",
			authHeader: "Bearer " + signBox(jwt.MapClaims{"sub": "admin/dlt-worker", "iss": acmeIssuer, "exp": time.Now().Add(-time.Hour).Unix()}),
			wantCode:   connect.CodeUnauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trust := NewPinnedBoxTrust(acmeIssuer, "acme", boxJWKS)
			interceptor := NewInternalAuthInterceptor(fallthroughJWKS, trust, slog.Default())

			var gotCaller InternalCaller
			next := func(ctx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
				gotCaller = InternalCallerFromContext(ctx)
				return connect.NewResponse(&emptypb.Empty{}), nil
			}

			req := connect.NewRequest(&emptypb.Empty{})
			req.Header().Set("Authorization", tt.authHeader)

			_, err := interceptor(next)(context.Background(), req)
			if tt.wantCode != 0 {
				if connect.CodeOf(err) != tt.wantCode {
					t.Fatalf("interceptor error = %v, want code %v", err, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("interceptor error = %v", err)
			}
			if gotCaller != tt.wantCaller {
				t.Errorf("caller = %+v, want %+v", gotCaller, tt.wantCaller)
			}
		})
	}
}

// fakeStreamConn is a minimal connect.StreamingHandlerConn carrying only a
// request header — enough to drive the auth interceptor's streaming path.
type fakeStreamConn struct {
	header http.Header
}

func (c *fakeStreamConn) Spec() connect.Spec           { return connect.Spec{} }
func (c *fakeStreamConn) Peer() connect.Peer           { return connect.Peer{} }
func (c *fakeStreamConn) Receive(any) error            { return nil }
func (c *fakeStreamConn) RequestHeader() http.Header   { return c.header }
func (c *fakeStreamConn) Send(any) error               { return nil }
func (c *fakeStreamConn) ResponseHeader() http.Header  { return http.Header{} }
func (c *fakeStreamConn) ResponseTrailer() http.Header { return http.Header{} }

// TestAuthInterceptorStreamingHandler is the regression guard for the
// StreamNotifications auth bug: the interceptor MUST authenticate streaming
// handlers (not just unary), or every stream aborts with "authentication
// required". A connect.UnaryInterceptorFunc silently passes streaming handlers
// through, so this test fails if the interceptor is downgraded to one.
func TestAuthInterceptorStreamingHandler(t *testing.T) {
	jwks, sign := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()
	const iss = "https://auth.customer-acme.example.com"

	tests := []struct {
		name       string
		authHeader string
		wantCode   connect.Code // 0 = expect success
		wantUserID UserID
	}{
		{
			name:       "valid token reaches handler with user in context",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "user-123", "iss": iss, "exp": exp}),
			wantUserID: "user-123",
		},
		{name: "missing header", wantCode: connect.CodeUnauthenticated},
		{
			name:       "expired token",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "user-123", "iss": iss, "exp": time.Now().Add(-time.Hour).Unix()}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "wrong issuer",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "user-123", "iss": "https://auth.customer-evil.example.com", "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{
			name:       "missing issuer",
			authHeader: "Bearer " + sign(jwt.MapClaims{"sub": "user-123", "exp": exp}),
			wantCode:   connect.CodeUnauthenticated,
		},
		{name: "garbage token", authHeader: "Bearer not.a.jwt", wantCode: connect.CodeUnauthenticated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := NewAuthInterceptor(UserAuth{JWKS: jwks, Issuer: iss})

			var gotUserID UserID
			called := false
			next := func(ctx context.Context, _ connect.StreamingHandlerConn) error {
				called = true
				gotUserID = UserIDFromContext(ctx)
				return nil
			}

			conn := &fakeStreamConn{header: http.Header{}}
			if tt.authHeader != "" {
				conn.header.Set("Authorization", tt.authHeader)
			}

			err := interceptor.WrapStreamingHandler(next)(context.Background(), conn)
			if tt.wantCode != 0 {
				if connect.CodeOf(err) != tt.wantCode {
					t.Fatalf("WrapStreamingHandler error = %v, want code %v", err, tt.wantCode)
				}
				if called {
					t.Error("handler should not run on auth failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("WrapStreamingHandler error = %v", err)
			}
			if !called {
				t.Fatal("handler was not called on valid token")
			}
			if gotUserID != tt.wantUserID {
				t.Errorf("user ID in context = %q, want %q", gotUserID, tt.wantUserID)
			}
		})
	}
}

func TestUserIDFromContext(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want UserID
	}{
		{
			name: "with user ID",
			ctx:  context.WithValue(context.Background(), userIDKey, UserID("user-123")),
			want: "user-123",
		},
		{
			name: "without user ID",
			ctx:  context.Background(),
			want: "",
		},
		{
			name: "wrong type in context",
			ctx:  context.WithValue(context.Background(), userIDKey, "not-a-UserID"),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UserIDFromContext(tt.ctx); got != tt.want {
				t.Errorf("UserIDFromContext() = %q, want %q", got, tt.want)
			}
		})
	}
}

// A Casdoor client-credentials token is a valid signature from a trusted
// issuer, but it identifies a machine — and on a box those credentials are
// handed to end users (read-only Lakekeeper warehouse access). The user
// surfaces must not accept one as a workspace member.
func TestUserIDFromBearerRejectsServiceAccounts(t *testing.T) {
	jwks, sign := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()
	const iss = "https://auth.customer-acme.example.com"
	auth := UserAuth{JWKS: jwks, Issuer: iss}

	tests := []struct {
		name    string
		sub     string
		wantErr bool
	}{
		{name: "human subject", sub: "9f0b2c1e-6a4d-4c8f-8d2a-0e1b3c4d5e6f"},
		{name: "lakekeeper service account", sub: "admin/lk-acme-reader", wantErr: true},
		{name: "dlt-worker service account", sub: "admin/dlt-worker-acme", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := auth.UserIDFromBearer(context.Background(),
				"Bearer "+sign(jwt.MapClaims{"sub": tt.sub, "iss": iss, "exp": exp}))
			if (err != nil) != tt.wantErr {
				t.Fatalf("UserIDFromBearer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != UserID(tt.sub) {
				t.Errorf("UserIDFromBearer() = %q, want %q", got, tt.sub)
			}
		})
	}
}

// TestUserAuthAudience covers the opt-in aud pinning: with Audiences set the
// token must name one of the expected OIDC client IDs; with it empty the
// claim is ignored (which app the Console logs in with is deployment config).
func TestUserAuthAudience(t *testing.T) {
	jwks, sign := testJWKS(t)
	exp := time.Now().Add(time.Hour).Unix()
	const iss = "https://auth.customer-acme.example.com"

	tests := []struct {
		name      string
		audiences []string
		claims    jwt.MapClaims
		wantErr   bool
	}{
		{
			name:      "matching audience",
			audiences: []string{"console-client"},
			claims:    jwt.MapClaims{"sub": "user-123", "iss": iss, "aud": "console-client", "exp": exp},
		},
		{
			name:      "any of several audiences",
			audiences: []string{"console-client", "legacy-client"},
			claims:    jwt.MapClaims{"sub": "user-123", "iss": iss, "aud": "legacy-client", "exp": exp},
		},
		{
			name:      "wrong audience",
			audiences: []string{"console-client"},
			claims:    jwt.MapClaims{"sub": "user-123", "iss": iss, "aud": "other-app", "exp": exp},
			wantErr:   true,
		},
		{
			name:      "missing audience claim",
			audiences: []string{"console-client"},
			claims:    jwt.MapClaims{"sub": "user-123", "iss": iss, "exp": exp},
			wantErr:   true,
		},
		{
			name:   "no expected audiences skips the check",
			claims: jwt.MapClaims{"sub": "user-123", "iss": iss, "aud": "whatever", "exp": exp},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			auth := UserAuth{JWKS: jwks, Issuer: iss, Audiences: tt.audiences}
			_, err := auth.UserIDFromBearer(context.Background(), "Bearer "+sign(tt.claims))
			if (err != nil) != tt.wantErr {
				t.Fatalf("UserIDFromBearer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestTokenProfileClaimFormats pins the mapping the whole box-side attribution
// path rests on. The two Casdoor token formats disagree about "name" and the
// standard one is the counter-intuitive direction, so reading it wrong yields
// commits authored by a workspace's admin login instead of a person's name —
// wrong but plausible, i.e. the kind of bug nobody reports.
func TestTokenProfileClaimFormats(t *testing.T) {
	tests := []struct {
		name   string
		claims jwt.MapClaims
		want   TokenProfile
	}{
		{
			// tokenFormat "JWT-Standard" (Casdoor UserStandard): the username
			// is preferred_username and "name" is the DISPLAY name.
			name: "standard format",
			claims: jwt.MapClaims{
				"sub":                "u-1",
				"preferred_username": "rich",
				"name":               "Tomáš Procházka",
				"email":              "rich@example.com",
			},
			want: TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Tomáš Procházka", Email: "rich@example.com"},
		},
		{
			// tokenFormat "JWT" embeds the user object: "name" is the username
			// and the display name is its own claim.
			name: "full user-object format",
			claims: jwt.MapClaims{
				"sub":         "u-2",
				"name":        "rich",
				"displayName": "Tomáš Procházka",
				"email":       "rich@example.com",
			},
			want: TokenProfile{Subject: "u-2", Name: "rich", DisplayName: "Tomáš Procházka", Email: "rich@example.com"},
		},
		{
			// Every standard-format field is omitempty, so a user with no
			// display name simply has no "name" claim.
			name:   "standard format without a display name",
			claims: jwt.MapClaims{"sub": "u-3", "preferred_username": "rich", "email": "rich@example.com"},
			want:   TokenProfile{Subject: "u-3", Name: "rich", Email: "rich@example.com"},
		},
		{
			name:   "no profile claims at all",
			claims: jwt.MapClaims{"sub": "u-4"},
			want:   TokenProfile{Subject: "u-4"},
		},
		{
			// A claim of the wrong JSON type must not panic the auth path.
			name:   "non-string claims are ignored",
			claims: jwt.MapClaims{"sub": "u-5", "email": 42, "name": []string{"x"}},
			want:   TokenProfile{Subject: "u-5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenProfile(tt.want.Subject, tt.claims); got != tt.want {
				t.Errorf("tokenProfile() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestAuthInterceptorSetsTokenProfile proves the claims survive the actual
// interceptor, not just the parser: this is the wiring commit attribution
// depends on, and a context value is exactly the kind of thing a refactor
// drops silently.
func TestAuthInterceptorSetsTokenProfile(t *testing.T) {
	jwks, sign := testJWKS(t)
	auth := UserAuth{JWKS: jwks, Issuer: "https://auth.example.com"}
	token := sign(jwt.MapClaims{
		"sub":                "u-1",
		"iss":                "https://auth.example.com",
		"exp":                time.Now().Add(time.Hour).Unix(),
		"preferred_username": "rich",
		"name":               "Rich",
		"email":              "rich@example.com",
	})

	ctx, err := auth.authenticateBearer(t.Context(), "Bearer "+token)
	if err != nil {
		t.Fatalf("authenticateBearer() error = %v", err)
	}
	if got := UserIDFromContext(ctx); got != UserID("u-1") {
		t.Errorf("UserIDFromContext() = %q, want u-1", got)
	}
	got, ok := TokenProfileFromContext(ctx)
	if !ok {
		t.Fatal("TokenProfileFromContext() not set")
	}
	want := TokenProfile{Subject: "u-1", Name: "rich", DisplayName: "Rich", Email: "rich@example.com"}
	if got != want {
		t.Errorf("TokenProfileFromContext() = %+v, want %+v", got, want)
	}
}
