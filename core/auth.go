// Caller identity and the interceptors that establish it. This lives in the
// shared kernel because both planes authenticate the same tokens from the same
// Casdoor, and because the control plane's own handlers read the caller out of
// the context — obtaining central's identity plumbing from the workspace
// plane's server package would make a control-plane auth change a release of
// the public workspace module.
package core

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey int

const (
	userIDKey contextKey = iota
	internalCallerKey
	tokenProfileKey
)

// TokenProfile is the profile the caller's own verified token carries. It is
// best-effort by nature — every field but Subject is optional in both Casdoor
// token formats — and exists so a deployment with no users table (the box)
// can still attribute a git commit to the person who made it. Anything that
// must be trustworthy belongs in a lookup, not here: these claims are only as
// good as the issuer, which is the same issuer that authenticated the call.
type TokenProfile struct {
	// Subject is the token's sub, i.e. the same value UserIDFromContext
	// returns. Consumers compare it against the caller they were handed
	// rather than assuming the context belongs to them.
	Subject     string
	Name        string
	DisplayName string
	Email       string
}

// TokenProfileFromContext returns the profile claims of the token that
// authenticated this request, if the request was authenticated at all.
func TokenProfileFromContext(ctx context.Context) (TokenProfile, bool) {
	v, ok := ctx.Value(tokenProfileKey).(TokenProfile)
	return v, ok
}

// ContextWithTokenProfile returns a context carrying the given profile as if
// the auth interceptor had set it. For handler tests and embedders that
// authenticate outside the interceptor.
func ContextWithTokenProfile(ctx context.Context, p TokenProfile) context.Context {
	return context.WithValue(ctx, tokenProfileKey, p)
}

// UserIDFromContext returns the authenticated user ID from the context.
func UserIDFromContext(ctx context.Context) UserID {
	v, _ := ctx.Value(userIDKey).(UserID)
	return v
}

// ContextWithUserID returns a context carrying the given user ID as if the
// auth interceptor had set it. For handler tests (both planes') and embedders
// that authenticate outside the interceptor.
func ContextWithUserID(ctx context.Context, id UserID) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

// InternalCaller identifies the authenticated internal-mux caller.
type InternalCaller struct {
	// App is the Casdoor application name from the token subject
	// "admin/<app>": "dlt-worker-<slug>" for central (shared-substrate)
	// tokens, the base app name (e.g. "dlt-worker") for box tokens.
	App string
	// Slug is the tenant slug bound from a trusted box issuer host. Empty
	// for central-JWKS tokens, whose tenant binding lives in the App name.
	Slug string
	// Issuer is the verified token issuer for box tokens; empty for central.
	Issuer string
}

// InternalCallerFromContext returns the authenticated internal caller. The
// zero value means the request carried no valid service token, which the
// internal auth interceptor never lets through — handlers treat it as a
// denial.
func InternalCallerFromContext(ctx context.Context) InternalCaller {
	v, _ := ctx.Value(internalCallerKey).(InternalCaller)
	return v
}

// ContextWithInternalCaller returns a context carrying the given caller as if
// the internal auth interceptor had set it. For handler tests, which live in
// the packages that serve the internal mux rather than in this one.
func ContextWithInternalCaller(ctx context.Context, c InternalCaller) context.Context {
	return context.WithValue(ctx, internalCallerKey, c)
}

// UserAuth validates Console-user bearer tokens: signature via the trusted
// JWKS, expiry, and a pinned issuer — the JWKS URL may point at an in-cluster
// alias of the issuer, so the iss claim is checked against the canonical
// issuer explicitly, exactly as the internal box path re-pins it with
// jwt.WithIssuer. Audiences, when non-empty, additionally requires the aud
// claim to name one of the listed OIDC client IDs, so a token minted by the
// same Casdoor for a different application is rejected; empty skips the
// check (Casdoor sets aud to the issuing app's client ID, and which app the
// Console logs in with is deployment config, not something this module can
// assume).
type UserAuth struct {
	JWKS      keyfunc.Keyfunc
	Issuer    string
	Audiences []string
}

// validSigningMethods pins every token verification in this package to
// asymmetric algorithms. The trusted keys all come from a JWKS and are
// asymmetric anyway — golang-jwt's HMAC verifier rejects a non-[]byte key —
// but an explicit allowlist keeps algorithm confusion impossible by
// construction rather than by that implementation detail.
var validSigningMethods = []string{
	"RS256", "RS384", "RS512",
	"PS256", "PS384", "PS512",
	"ES256", "ES384", "ES512",
	"EdDSA",
}

func (a UserAuth) parserOptions() []jwt.ParserOption {
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(validSigningMethods),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(a.Issuer),
	}
	if len(a.Audiences) > 0 {
		opts = append(opts, jwt.WithAudience(a.Audiences...))
	}
	return opts
}

// UserIDFromBearer validates an Authorization header and returns the token's
// subject. Shared by the RPC auth interceptors and the plain-HTTP endpoints
// (file-drop upload, Google OAuth start) that live outside ConnectRPC.
func (a UserAuth) UserIDFromBearer(ctx context.Context, authHeader string) (UserID, error) {
	token, err := tokenFromHeader(authHeader)
	if err != nil {
		return "", err
	}
	return a.userIDFromToken(ctx, token)
}

// authenticateBearer validates an Authorization header and returns a context
// carrying both the caller and the profile claims their token supplied. The
// interceptors use this rather than UserIDFromBearer so commit attribution
// works on a deployment that cannot look the caller up (see TokenProfile);
// UserIDFromBearer stays for the plain-HTTP endpoints, which only need the
// identity.
func (a UserAuth) authenticateBearer(ctx context.Context, authHeader string) (context.Context, error) {
	token, err := tokenFromHeader(authHeader)
	if err != nil {
		return nil, err
	}
	return a.authenticateToken(ctx, token)
}

// authenticateToken verifies a token and returns a context carrying the
// caller plus their profile claims.
func (a UserAuth) authenticateToken(ctx context.Context, token string) (context.Context, error) {
	userID, profile, err := a.parseToken(ctx, token)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, userIDKey, userID)
	return context.WithValue(ctx, tokenProfileKey, profile), nil
}

// userIDFromToken verifies a bearer token and returns the human user it
// authenticates. Service-account subjects are rejected outright: the user
// surfaces of both planes are for people, and a machine identity that
// reaches them acts as a full workspace member (UserID.IsServiceAccount
// explains what that would buy an attacker on a box). Centrally no service
// account has a users row either, so this only moves the denial from the
// lookup to the token — the two planes answer the same way.
func (a UserAuth) userIDFromToken(ctx context.Context, token string) (UserID, error) {
	userID, _, err := a.parseToken(ctx, token)
	return userID, err
}

// parseToken verifies a bearer token and returns the caller together with the
// profile claims it carries.
func (a UserAuth) parseToken(ctx context.Context, token string) (UserID, TokenProfile, error) {
	parsed, err := jwt.Parse(token, a.JWKS.KeyfuncCtx(ctx), a.parserOptions()...)
	if err != nil {
		return "", TokenProfile{}, errors.New("invalid token")
	}

	sub, err := parsed.Claims.GetSubject()
	if err != nil || sub == "" {
		return "", TokenProfile{}, errors.New("missing subject")
	}
	if UserID(sub).IsServiceAccount() {
		return "", TokenProfile{}, errors.New("service-account token is not a user identity")
	}
	return UserID(sub), tokenProfile(sub, parsed.Claims), nil
}

// tokenProfile reads the optional profile claims out of a verified token.
//
// The two Casdoor token formats in play disagree about what "name" means, and
// the mapping is the reverse of the intuitive one — verified against Casdoor's
// UserStandard struct tags rather than assumed:
//
//	tokenFormat "JWT"          embeds the user object: name = username,
//	                           displayName = display name.
//	tokenFormat "JWT-Standard" is OIDC UserStandard: preferred_username =
//	                           username, name = *display* name, picture =
//	                           avatar.
//
// Every field is omitempty in the standard format, so any of them may be
// absent. preferred_username is the discriminator: only the standard format
// emits it. Boxes issue JWT-Standard (the full format bloats the session
// cookie past the HPACK table); central issues JWT.
func tokenProfile(sub string, claims jwt.Claims) TokenProfile {
	m, ok := claims.(jwt.MapClaims)
	if !ok {
		return TokenProfile{Subject: sub}
	}
	profile := TokenProfile{Subject: sub, Email: stringClaim(m, "email")}
	if username := stringClaim(m, "preferred_username"); username != "" {
		profile.Name = username
		profile.DisplayName = stringClaim(m, "name")
		return profile
	}
	profile.Name = stringClaim(m, "name")
	profile.DisplayName = stringClaim(m, "displayName")
	return profile
}

func stringClaim(m jwt.MapClaims, name string) string {
	s, _ := m[name].(string)
	return s
}

// authInterceptor validates JWT bearer tokens and puts the token subject in
// the context for both unary and server-streaming handlers.
//
// It is a full connect.Interceptor rather than a connect.UnaryInterceptorFunc
// on purpose: the unary-only helper leaves streaming handlers unauthenticated
// (its WrapStreamingHandler is a passthrough), so UserIDFromContext returns ""
// inside a stream and the handler rejects every client. That is exactly what
// broke StreamNotifications — it aborted with "authentication required" on
// every connection, leaving F5 (the unary ListNotifications) as the only way
// to see notification state.
type authInterceptor struct {
	auth UserAuth
}

// NewAuthInterceptor creates a ConnectRPC interceptor that validates JWT
// tokens against the given UserAuth policy. It authenticates unary and
// server-streaming handlers alike.
func NewAuthInterceptor(auth UserAuth) connect.Interceptor {
	return authInterceptor{auth: auth}
}

func (i authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		authed, err := i.auth.authenticateBearer(ctx, req.Header().Get("Authorization"))
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, err)
		}
		return next(authed, req)
	}
}

// WrapStreamingClient is a passthrough: this server serves streams, it never
// calls one as a client.
func (i authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		authed, err := i.auth.authenticateBearer(ctx, conn.RequestHeader().Get("Authorization"))
		if err != nil {
			return connect.NewError(connect.CodeUnauthenticated, err)
		}
		return next(authed, conn)
	}
}

// NewOptionalAuthInterceptor is like NewAuthInterceptor but does not fail
// when the Authorization header is missing. If a valid token is present it
// sets the user ID in context; otherwise the request proceeds unauthenticated.
// Individual handlers decide whether to require auth via UserIDFromContext.
func NewOptionalAuthInterceptor(auth UserAuth) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			token, err := tokenFromHeader(req.Header().Get("Authorization"))
			if err != nil {
				// No token — proceed without setting user ID.
				return next(ctx, req)
			}

			authed, err := auth.authenticateToken(ctx, token)
			if err != nil {
				return nil, connect.NewError(connect.CodeUnauthenticated, err)
			}

			return next(authed, req)
		}
	}
}

// CustomerChecker gates per-box JWKS fetches: only slugs of existing
// dedicated-VM customers get a trust anchor. Without it, any box-shaped
// issuer would trigger an outbound JWKS request —
// auth.customer-<anything>.<baseDomain> resolves via the wildcard DNS record,
// so forged issuers are a request amplifier, not a 4xx.
type CustomerChecker interface {
	IsVMCustomer(ctx context.Context, slug string) (bool, error)
}

// BoxIssuerTrust accepts tokens minted by a dedicated-VM box's own Casdoor
// (issuer https://auth.customer-<slug>.<baseDomain>) and binds the tenant
// slug from the issuer host. The box is wholly customer-controlled, so any
// token it issues can only ever bind to that customer's own slug — the
// issuer host, not the app name, is the security boundary.
type BoxIssuerTrust struct {
	ctx       context.Context // long-lived server ctx: keyfunc refresh goroutines outlive requests
	pattern   *regexp.Regexp
	customers CustomerChecker
	// newKeyfunc is a seam for tests; production uses keyfunc.NewDefaultCtx.
	newKeyfunc func(ctx context.Context, jwksURL string) (keyfunc.Keyfunc, error)

	mu    sync.Mutex
	cache map[string]keyfunc.Keyfunc // issuer -> keyfunc; positive entries only
}

// NewBoxIssuerTrust builds the box trust anchor for the given base domain
// (e.g. "fairtier.com"). An empty baseDomain returns nil, which disables
// box-issuer trust entirely.
func NewBoxIssuerTrust(ctx context.Context, baseDomain string, customers CustomerChecker) *BoxIssuerTrust {
	if baseDomain == "" {
		return nil
	}
	// Anchored, lowercase-only, no port: exactly the Casdoor origin the box
	// chart configures. The slug charset mirrors DNS-label rules.
	pattern := regexp.MustCompile(`^https://auth\.customer-([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)\.` + regexp.QuoteMeta(baseDomain) + `$`)
	return &BoxIssuerTrust{
		ctx:       ctx,
		pattern:   pattern,
		customers: customers,
		newKeyfunc: func(ctx context.Context, jwksURL string) (keyfunc.Keyfunc, error) {
			return keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
		},
		cache: map[string]keyfunc.Keyfunc{},
	}
}

// slugForIssuer returns the tenant slug encoded in a box-shaped issuer, or
// "" when the issuer does not match the trusted pattern (nil-safe).
func (b *BoxIssuerTrust) slugForIssuer(iss string) string {
	if b == nil || iss == "" {
		return ""
	}
	m := b.pattern.FindStringSubmatch(iss)
	if m == nil {
		return ""
	}
	return m[1]
}

// keyfuncFor returns the (cached) keyfunc validating tokens from the given
// issuer. The CustomerChecker gate runs before any JWKS fetch; failures are
// not cached so a box that was just provisioned heals on the next call.
func (b *BoxIssuerTrust) keyfuncFor(ctx context.Context, iss, slug string) (keyfunc.Keyfunc, error) {
	b.mu.Lock()
	kf, ok := b.cache[iss]
	b.mu.Unlock()
	if ok {
		return kf, nil
	}

	isVM, err := b.customers.IsVMCustomer(ctx, slug)
	if err != nil {
		return nil, err
	}
	if !isVM {
		return nil, errors.New("unknown box issuer")
	}

	kf, err = b.newKeyfunc(b.ctx, iss+"/.well-known/jwks")
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, ok := b.cache[iss]; ok {
		return existing, nil
	}
	b.cache[iss] = kf
	return kf, nil
}

// boxTrust is the trust anchor for box-issued internal tokens: it recognizes a
// box issuer and yields the keyfunc plus the tenant slug to bind. Both
// *BoxIssuerTrust (control plane: many boxes, discriminated by regex) and
// *pinnedBoxTrust (a single box: one known issuer) implement it.
type boxTrust interface {
	slugForIssuer(iss string) string
	keyfuncFor(ctx context.Context, iss, slug string) (keyfunc.Keyfunc, error)
}

// pinnedBoxTrust is the single-tenant box's trust anchor: it accepts exactly
// one issuer (the box's own Casdoor) and binds that box's slug. The box serves
// one tenant, so unlike the control plane's BoxIssuerTrust it needs no regex,
// no CustomerChecker, and no second JWKS fetch — the box already loaded its
// Casdoor keys and both the user and worker paths validate against them.
type pinnedBoxTrust struct {
	issuer string
	slug   string
	kf     keyfunc.Keyfunc
}

// NewPinnedBoxTrust builds the box-side trust anchor that trusts exactly the
// given issuer (binding slug), validating against the already-loaded box jwks.
func NewPinnedBoxTrust(issuer, slug string, jwks keyfunc.Keyfunc) *pinnedBoxTrust {
	return &pinnedBoxTrust{issuer: issuer, slug: slug, kf: jwks}
}

func (p *pinnedBoxTrust) slugForIssuer(iss string) string {
	if p == nil || iss == "" || iss != p.issuer {
		return ""
	}
	return p.slug
}

func (p *pinnedBoxTrust) keyfuncFor(_ context.Context, _, _ string) (keyfunc.Keyfunc, error) {
	return p.kf, nil
}

// NewInternalAuthInterceptor authenticates tenant-facing internal RPCs
// (the :8081 mux, also exposed as worker-api.<baseDomain>). It accepts two
// kinds of Casdoor client-credentials JWTs and puts an InternalCaller in the
// context for handlers to bind against the requested tenant:
//
//   - Central tokens, validated against the platform Casdoor JWKS. Tokens
//     carry sub = "<appOwner>/<appName>", and applications live under the
//     built-in "admin" owner (the same identity Lakekeeper principals are
//     registered as). Only the platform creates central Casdoor
//     applications, so an "admin/"-prefixed subject is a platform-issued
//     service identity whose app name ("dlt-worker-<slug>") binds the
//     tenant — tenants can never mint one via their own users, whose
//     subjects are "customer-<slug>/<name>".
//
//   - Box tokens, issued by a dedicated-VM box's own Casdoor and validated
//     against that box's JWKS via a boxTrust anchor (nil disables the path):
//     *BoxIssuerTrust centrally, *pinnedBoxTrust on the box itself. Here the
//     tenant binding is the issuer host; the app name is advisory because the
//     customer controls their box's Casdoor.
func NewInternalAuthInterceptor(jwks keyfunc.Keyfunc, box boxTrust, logger *slog.Logger) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			caller, err := internalCallerFromRequest(ctx, jwks, box, req)
			if err != nil {
				return nil, connect.NewError(connect.CodeUnauthenticated, err)
			}
			if caller.Slug != "" {
				logger.InfoContext(ctx, "box internal API caller",
					"procedure", req.Spec().Procedure, "slug", caller.Slug, "app", caller.App)
			}
			return next(context.WithValue(ctx, internalCallerKey, caller), req)
		}
	}
}

// internalCallerFromRequest validates the request's bearer token and returns
// the caller identity. The token's unverified iss claim only routes to a
// trust anchor (central JWKS vs a box's JWKS); the chosen anchor then
// verifies the signature, and box verification re-pins the issuer with
// jwt.WithIssuer so a token cannot claim one issuer and verify under another.
func internalCallerFromRequest(ctx context.Context, jwks keyfunc.Keyfunc, box boxTrust, req connect.AnyRequest) (InternalCaller, error) {
	token, err := tokenFromHeader(req.Header().Get("Authorization"))
	if err != nil {
		return InternalCaller{}, err
	}

	var routing jwt.MapClaims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &routing); err != nil {
		return InternalCaller{}, errors.New("invalid token")
	}
	iss, _ := routing.GetIssuer()

	slug := ""
	if box != nil {
		slug = box.slugForIssuer(iss)
	}
	if slug != "" {
		kf, err := box.keyfuncFor(ctx, iss, slug)
		if err != nil {
			return InternalCaller{}, errors.New("untrusted issuer")
		}
		parsed, err := jwt.Parse(token, kf.KeyfuncCtx(ctx),
			jwt.WithValidMethods(validSigningMethods),
			jwt.WithExpirationRequired(),
			jwt.WithIssuer(iss),
		)
		if err != nil {
			return InternalCaller{}, errors.New("invalid token")
		}
		app, err := serviceAppFromSubject(parsed)
		if err != nil {
			return InternalCaller{}, err
		}
		return InternalCaller{App: app, Slug: slug, Issuer: iss}, nil
	}

	parsed, err := jwt.Parse(token, jwks.KeyfuncCtx(ctx),
		jwt.WithValidMethods(validSigningMethods),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return InternalCaller{}, errors.New("invalid token")
	}
	app, err := serviceAppFromSubject(parsed)
	if err != nil {
		return InternalCaller{}, err
	}
	return InternalCaller{App: app}, nil
}

// serviceAppFromSubject extracts the Casdoor application name from a
// client-credentials token subject "admin/<app>".
func serviceAppFromSubject(parsed *jwt.Token) (string, error) {
	sub, err := parsed.Claims.GetSubject()
	if err != nil || sub == "" {
		return "", errors.New("missing subject")
	}
	app, ok := UserID(sub).ServiceAccountApp()
	if !ok {
		return "", errors.New("not a service-account token")
	}
	return app, nil
}

func tokenFromHeader(auth string) (string, error) {
	if auth == "" {
		return "", errors.New("missing authorization header")
	}
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || token == "" {
		return "", errors.New("invalid authorization header")
	}
	return token, nil
}
