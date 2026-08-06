// Command workspace_api is the box-local workspace plane: the same product
// services the FairTier control plane mounts via server.RegisterWorkspacePlane,
// wired for a dedicated-VM box that serves exactly one workspace. Tenancy comes
// from static config (workspace.StaticResolver) instead of the customers
// table, auth from the box's own Casdoor JWKS, repo credentials from local
// Secrets (workspace.StaticBoxCredentials) instead of the central deposit
// tables, and the box Gitea repos are the definitions' source of truth
// (git-primary defaults ON here). The control plane — billing, identity
// sync, provisioning — does not exist on this binary; when central
// disappears, this keeps the workspace working.
package main

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"github.com/MicahParks/keyfunc/v3"
	_ "github.com/jackc/pgx/v5/stdlib"
	rscors "github.com/rs/cors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/fairtier/workspace-api/casdoor"
	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/crypto"
	"github.com/fairtier/workspace-api/duckflight"
	"github.com/fairtier/workspace-api/gitea"
	"github.com/fairtier/workspace-api/lakekeeper"
	"github.com/fairtier/workspace-api/llm"
	"github.com/fairtier/workspace-api/objstore"
	"github.com/fairtier/workspace-api/postgres"
	"github.com/fairtier/workspace-api/proto/boxcredential/v1/boxcredentialv1connect"
	"github.com/fairtier/workspace-api/server"
	"github.com/fairtier/workspace-api/telemetry"
	"github.com/fairtier/workspace-api/version"
	"github.com/fairtier/workspace-api/workspace"
)

func main() {
	if err := run(); err != nil {
		slog.Error("failed to run", "err", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.Default()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	shutdownTracing, err := telemetry.SetupTracing(ctx, "workspace-api", version.Binary())
	if err != nil {
		return fmt.Errorf("setup tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Warn("tracing shutdown error", "err", err)
		}
	}()

	ws, err := loadStaticWorkspace()
	if err != nil {
		return err
	}
	logger.Info("workspace loaded", "slug", ws.Slug, "domain", ws.CustomerDomain)
	warnMissingOptionalConfig(logger, ws)

	db, enc, err := openDatabase(logger)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Box Casdoor JWKS: the only issuer this deployment trusts for Console
	// (public-mux) callers. Any user it authenticates belongs to the single
	// workspace — the issuer is the tenant boundary. The issuer is pinned to
	// the canonical Casdoor origin even when AUTH_JWKS_URL points at an
	// in-cluster alias; AUTH_EXPECTED_AUDIENCES (comma-separated OIDC client
	// IDs, optional) additionally binds tokens to the Console's own Casdoor
	// app so credentials minted for other apps on the same box are refused.
	jwksURL := cmp.Or(os.Getenv("AUTH_JWKS_URL"), ws.CasdoorIssuer+"/.well-known/jwks")
	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return fmt.Errorf("create JWKS keyfunc for %s: %w", jwksURL, err)
	}
	userAuth := server.UserAuth{
		JWKS:      jwks,
		Issuer:    ws.CasdoorIssuer,
		Audiences: splitCSV(os.Getenv("AUTH_EXPECTED_AUDIENCES")),
	}

	otelInterceptor, err := otelconnect.NewInterceptor(otelconnect.WithTrustRemote())
	if err != nil {
		return fmt.Errorf("create otelconnect interceptor: %w", err)
	}
	authOpts := connect.WithInterceptors(otelInterceptor, server.NewAuthInterceptor(userAuth))

	repo := &postgres.Repository{DB: db, Encryptor: enc}
	resolver := &workspace.StaticResolver{Workspace: *ws}
	boxCreds := &workspace.StaticBoxCredentials{
		Slug:          ws.Slug,
		GitUsername:   cmp.Or(os.Getenv("BOX_GIT_USERNAME"), "fairtier-admin"),
		GitToken:      os.Getenv("BOX_GIT_TOKEN"),
		SnapshotToken: os.Getenv("BOX_SNAPSHOT_TOKEN"),
		AgePublicKey:  os.Getenv("BOX_AGE_PUBLIC_KEY"),
	}
	if boxCreds.GitToken == "" {
		logger.Warn("BOX_GIT_TOKEN not set; repo-backed features (pipelines mirror, transformations, box repo editor) will report the credential as missing")
	}

	// In-app notifications: single-tenant, but the LISTEN/NOTIFY bridge is
	// kept so a future multi-replica box deployment behaves like central.
	notificationBroker := workspace.NewNotificationBroker()
	notificationSvc := &workspace.NotificationService{
		Workspaces:    resolver,
		Notifications: repo,
		Broker:        notificationBroker,
		Publisher:     repo,
	}
	go (&postgres.NotificationListener{
		DSN:    os.Getenv("PG_DSN"),
		Broker: notificationBroker,
		Logger: logger,
	}).Run(ctx)

	pipelineMirror := &workspace.PipelineMirror{
		Workspaces:          resolver,
		Credentials:         boxCreds,
		Pipelines:           repo,
		AgeKeys:             boxCreds,
		Renders:             repo,
		Fingerprint:         buildFingerprinter(),
		PipelineCredentials: repo,
		DefinitionRenders:   repo,
		Notifications:       notificationSvc,
		Ownership:           repo,
		NewClient:           gitea.NewClient,
		Logger:              logger,
	}

	// The box repo IS the source of truth here, so both git-primary levers
	// default ON (central defaults them off). "off"/"on" keep the central env
	// names working. Read once: the same flag drives the save path (services)
	// and the adopt sweep, so turning a plane off stops both halves together.
	pipelinesGitPrimary := os.Getenv("PIPELINES_GIT_PRIMARY") != "off"
	transformationsGitPrimary := os.Getenv("TRANSFORMATIONS_GIT_PRIMARY") != "off"

	// Commit attribution: central reads its users table, the box reads the
	// claims of the token the caller just presented — its database holds no
	// user rows, and the identity lives in the box's own Casdoor, which minted
	// that token. A caller whose token carries no email keeps the platform
	// identity, exactly as a failed central lookup does.
	commitUsers := server.TokenUserReader{}

	pipelineSvc := &workspace.PipelineService{
		Workspaces:    resolver,
		Pipelines:     repo,
		Notifications: notificationSvc,
		Mirror:        pipelineMirror,
		Ownership:     repo,
		Users:         commitUsers,
		Versions:      pipelineMirror,
		// The local dlt-worker decrypts credentials from the checkout, and
		// saves commit synchronously.
		StripPollCredentials: os.Getenv("POLL_SOURCE_CREDENTIALS") != "on",
		GitPrimary:           pipelinesGitPrimary,
		Logger:               logger,
	}

	fileDropSvc := &workspace.FileDropService{
		Workspaces: resolver,
		Pipelines:  repo,
		Store:      objstore.New(),
		MaxBytes:   loadFileDropMaxBytes(),
		Logger:     logger,
	}

	transformationMirror := &workspace.TransformationMirror{
		Workspaces:        resolver,
		Credentials:       boxCreds,
		Transformations:   repo,
		Pipelines:         repo,
		DefinitionRenders: repo,
		Notifications:     notificationSvc,
		NewClient:         gitea.NewClient,
		Logger:            logger,
	}
	wireRepoImport(pipelineMirror, transformationMirror, repo, logger)

	transformationSvc := &workspace.TransformationService{
		Workspaces:      resolver,
		Transformations: repo,
		Pipelines:       repo,
		Notifications:   notificationSvc,
		Mirror:          transformationMirror,
		Users:           commitUsers,
		GitPrimary:      transformationsGitPrimary,
		Logger:          logger,
	}
	transformationServer := &server.TransformationServer{Service: transformationSvc}

	boxRepoSvc := &workspace.BoxRepoService{
		Workspaces:      resolver,
		Credentials:     boxCreds,
		Users:           commitUsers,
		NewClient:       gitea.NewClient,
		NewMirrorClient: gitea.NewMirrorClient,
		Logger:          logger,
	}

	demoServer := &server.DemoServer{Service: &workspace.DemoService{
		Workspaces:      resolver,
		Seeds:           repo,
		Pipelines:       pipelineSvc,
		Transformations: transformationSvc,
		BoxRepo:         boxRepoSvc,
		Bucket:          loadDemoBucket(),
		Logger:          logger,
	}}

	// Lakekeeper user management drives the box's own Casdoor: service
	// accounts are created there via the workspace's OIDC app credential —
	// the same admin-API mechanism the central API uses for VM boxes. The
	// audience Secret converges via the on-box CronJob (OnVM skip).
	lkClient := &lakekeeper.Client{}
	tokenProvider := &casdoor.TokenProvider{Endpoint: ws.CasdoorIssuer}
	lakekeeperUserSvc := &workspace.LakekeeperUserService{
		Workspaces: resolver,
		CasdoorAppsFor: func(ws *workspace.Workspace) workspace.CasdoorAppManager {
			return &casdoor.AppManager{
				Endpoint:     ws.CasdoorIssuer,
				ClientID:     ws.OIDCClientID,
				ClientSecret: ws.OIDCClientSecret,
			}
		},
		Lakekeeper: lkClient,
		Tokens:     tokenProvider,
		Logger:     logger,
	}
	warehouseSvc := &workspace.WarehouseService{
		Workspaces: resolver,
		Lakekeeper: lkClient,
		Tokens:     tokenProvider,
		Logger:     logger,
	}

	if os.Getenv("GOOGLE_OAUTH_CLIENT_ID") != "" {
		// The broker handlers need a customer lookup this binary does not
		// have yet — box-local Google OAuth arrives with Phase 3D.
		logger.Warn("GOOGLE_OAUTH_* is set but Sign in with Google is not supported on the box binary yet; google_sheets falls back to the service-account path")
	}

	pipelineAssistServer, assistServer := buildAssistServers(resolver, logger)

	mux := http.NewServeMux()
	server.RegisterWorkspacePlane(mux, server.WorkspacePlaneServers{
		Health:          &server.HealthServer{DB: db, Logger: logger},
		LakekeeperUsers: &server.LakekeeperUserServer{Service: lakekeeperUserSvc},
		Warehouses:      &server.WarehouseServer{Service: warehouseSvc},
		Snapshots:       &server.SnapshotServer{Workspaces: resolver, Snapshots: boxCreds, HTTPClient: http.DefaultClient},
		Pipelines:       &server.PipelineServer{Service: pipelineSvc, FileDrop: fileDropSvc},
		Transformations: transformationServer,
		PipelineAssist:  pipelineAssistServer,
		Assist:          assistServer,
		BoxRepos:        &server.BoxRepoServer{Service: boxRepoSvc},
		Demo:            demoServer,
		Notifications:   &server.NotificationServer{Service: notificationSvc},
		Query:           &server.QueryServer{Workspaces: resolver, Executor: duckflight.NewClient()},
	}, authOpts)
	server.RegisterWorkspacePlainHTTP(mux, logger, userAuth, db, nil, nil, fileDropSvc, nil, firstCORSOrigin())
	// The pre-authentication discovery document.
	server.RegisterWorkspaceBootstrap(mux, logger,
		server.BootstrapFromWorkspace(ws, consoleClientID(logger), fileDropSvc != nil, false))

	// Internal mux (:8081): the local dlt-worker's poll + run reporting.
	// The worker authenticates with a token from the box's own Casdoor;
	// BoxIssuerTrust binds the tenant from the issuer host exactly as
	// central does, with the static checker pinning the one known slug.
	// No BoxCredentialService: deposits are a central-transition mechanism —
	// this binary reads the same credentials from local config.
	internalOpts, err := buildInternalOpts(ctx, logger, ws, jwks, otelInterceptor)
	if err != nil {
		return err
	}
	internalMux := http.NewServeMux()
	server.RegisterWorkspaceInternal(internalMux, server.WorkspaceInternalServers{
		Pipelines:       server.NewInternalPipelineServer(pipelineSvc),
		Transformations: server.NewInternalTransformationServer(transformationSvc),
	}, internalOpts)

	serviceNames := workspaceServiceNamesWithoutBoxCredential()
	internalMux.Handle(grpchealth.NewHandler(grpchealth.NewStaticChecker(serviceNames...)))
	internalMux.Handle(grpcreflect.NewHandlerV1(grpcreflect.NewStaticReflector(serviceNames...)))

	startBackgroundSweeps(ctx, logger, backgroundSweeps{
		Slug:                      ws.Slug,
		Repo:                      repo,
		Resolver:                  resolver,
		PipelineMirror:            pipelineMirror,
		TransformationMirror:      transformationMirror,
		PipelinesGitPrimary:       pipelinesGitPrimary,
		TransformationsGitPrimary: transformationsGitPrimary,
	})

	corsHandler := wrapPublicHandler(mux, corsAllowedOrigins(), logger)

	h2cProtocols := new(http.Protocols)
	h2cProtocols.SetHTTP1(true)
	h2cProtocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{Addr: ":8080", Handler: corsHandler, Protocols: h2cProtocols}

	internalPort := cmp.Or(os.Getenv("INTERNAL_PORT"), "8081")
	tracedInternalMux := otelhttp.NewHandler(internalMux, "workspace-api-internal",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	internalServer := &http.Server{Addr: ":" + internalPort, Handler: tracedInternalMux, Protocols: h2cProtocols}

	return serveAndShutdown(ctx, cancel, logger, httpServer, internalServer, internalPort)
}

// wireRepoImport turns on hydration (control-plane/workspace-split Phase 3B).
// This database starts empty on a new box, and adoption only ever looks at
// files it holds a render row for — so without an import pass the box would
// serve nothing until every pipeline and transformation had been saved through
// it once. Wiring the importer lets the adopt sweep read the repo's untracked
// definition files back into rows with their ids intact; the pass stays
// read-only toward the repo. WORKSPACE_IMPORT_FROM_REPO=off disables it.
// Central never wires this: there the Console is the only create path.
func wireRepoImport(pipelines *workspace.PipelineMirror, transformations *workspace.TransformationMirror, repo *postgres.Repository, logger *slog.Logger) {
	if os.Getenv("WORKSPACE_IMPORT_FROM_REPO") == "off" {
		logger.Info("repo import disabled: definitions already in the workspace repositories will not be loaded into this database")
		return
	}
	pipelines.Importer = repo
	transformations.Importer = repo
}

// backgroundSweeps carries the wiring for the periodic sweeps.
type backgroundSweeps struct {
	Slug                      string
	Repo                      *postgres.Repository
	Resolver                  *workspace.StaticResolver
	PipelineMirror            *workspace.PipelineMirror
	TransformationMirror      *workspace.TransformationMirror
	PipelinesGitPrimary       bool
	TransformationsGitPrimary bool
}

// startBackgroundSweeps launches the sweeps that run in the central
// platform-worker today; this binary is single-replica on the box, so it
// hosts them itself. Each adopt mirror is wired only while its git-primary
// flag is on (nil = that plane's adoption off, per the AdoptSweeper contract),
// so turning a flag off stops git→DB adoption together with git-first saves.
// The stuck-run sweep is scoped to the box's own slug: every other box write
// path is slug-gated in depth, and this keeps a mispointed PG_DSN from
// failing another tenant's in-flight runs.
func startBackgroundSweeps(ctx context.Context, logger *slog.Logger, s backgroundSweeps) {
	sweeper := &workspace.AdoptSweeper{Workspaces: s.Resolver, Logger: logger}
	if s.PipelinesGitPrimary {
		sweeper.Mirror = s.PipelineMirror
	}
	if s.TransformationsGitPrimary {
		sweeper.Transformations = s.TransformationMirror
	}
	if sweeper.Mirror != nil || sweeper.Transformations != nil {
		go sweeper.Run(ctx, loadDurationEnv("PIPELINE_ADOPT_SWEEP_INTERVAL", 10*time.Minute))
	} else {
		logger.Info("adopt sweep disabled: both git-primary flags are off")
	}

	stuckSweeper := &workspace.StuckRunSweeper{
		Store:   s.Repo,
		Slug:    s.Slug,
		Timeout: loadDurationEnv("PIPELINE_RUN_STUCK_TIMEOUT", 0),
		Logger:  logger,
	}
	go stuckSweeper.Run(ctx, loadDurationEnv("PIPELINE_RUN_STUCK_SWEEP_INTERVAL", 5*time.Minute))
}

// loadStaticWorkspace builds the single workspace this deployment serves from
// WORKSPACE_* env. Slug, customer domain, Casdoor org and Casdoor issuer are
// required; service endpoints default to the box's standard hostname scheme
// (<service>.<customerDomain>). The Casdoor org and issuer are supplied
// explicitly rather than reconstructed from the slug — that "customer-<slug>"
// / "auth.<domain>" naming is the control plane's, created by provisioning, so
// the box is told it and treats it as opaque.
func loadStaticWorkspace() (*workspace.Workspace, error) {
	slug := os.Getenv("WORKSPACE_SLUG")
	if slug == "" {
		return nil, errors.New("WORKSPACE_SLUG environment variable not set")
	}
	// The slug is the tenant key (customer_slug rows, the worker-token match)
	// and appears in the customer domain — a bad value fails at startup here
	// instead of as opaque errors later.
	if !dnsLabelRE.MatchString(slug) {
		return nil, fmt.Errorf("WORKSPACE_SLUG %q is not a DNS label (lowercase letters, digits, hyphens)", slug)
	}
	customerDomain := os.Getenv("WORKSPACE_CUSTOMER_DOMAIN")
	if customerDomain == "" {
		return nil, errors.New("WORKSPACE_CUSTOMER_DOMAIN environment variable not set (the box's public domain, e.g. customer-acme.fairtier.com)")
	}
	// Central workspace rows may carry the wildcard-certificate form; every
	// derived endpoint needs the bare domain, so normalize once here.
	customerDomain = strings.TrimPrefix(customerDomain, "*.")
	if err := validateCustomerDomain(customerDomain); err != nil {
		return nil, fmt.Errorf("WORKSPACE_CUSTOMER_DOMAIN %q: %w", customerDomain, err)
	}
	casdoorOrg := os.Getenv("WORKSPACE_CASDOOR_ORG")
	if casdoorOrg == "" {
		return nil, errors.New("WORKSPACE_CASDOOR_ORG environment variable not set (the Casdoor org owning this workspace's users)")
	}
	casdoorIssuer := os.Getenv("WORKSPACE_CASDOOR_ISSUER")
	if casdoorIssuer == "" {
		return nil, errors.New("WORKSPACE_CASDOOR_ISSUER environment variable not set (the box Casdoor base URL)")
	}

	return &workspace.Workspace{
		Slug:                slug,
		OnVM:                true,
		CustomerDomain:      customerDomain,
		LakekeeperURL:       cmp.Or(os.Getenv("WORKSPACE_LAKEKEEPER_URL"), "https://lakekeeper."+customerDomain),
		LakekeeperWarehouse: cmp.Or(os.Getenv("WORKSPACE_LAKEKEEPER_WAREHOUSE"), "default"),
		CasdoorIssuer:       casdoorIssuer,
		CasdoorOrg:          casdoorOrg,
		OIDCClientID:        os.Getenv("WORKSPACE_OIDC_CLIENT_ID"),
		OIDCClientSecret:    os.Getenv("WORKSPACE_OIDC_CLIENT_SECRET"),
		DuckFlightURL:       cmp.Or(os.Getenv("WORKSPACE_DUCKFLIGHT_URL"), "https://duckflight."+customerDomain),
		DuckFlightAuthToken: os.Getenv("WORKSPACE_DUCKFLIGHT_AUTH_TOKEN"),
		EffectiveS3:         loadS3Config(),
		RillEnabled:         os.Getenv("WORKSPACE_RILL_ENABLED") != "false",
		CubeEnabled:         os.Getenv("WORKSPACE_CUBE_ENABLED") == "true",
	}, nil
}

// dnsLabelRE is the DNS-label charset the slug must satisfy (it appears in the
// customer domain and is used as the tenant key).
var dnsLabelRE = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// validateCustomerDomain rejects values that are not a plain lowercase
// hostname: every WORKSPACE_* endpoint default is built as
// "https://<service>." + domain, so a scheme, path, space, or single label
// would produce valid-looking but unreachable URLs that only fail later.
func validateCustomerDomain(domain string) error {
	switch {
	case strings.Contains(domain, "://"):
		return errors.New("must be a bare hostname, not a URL")
	case strings.ContainsAny(domain, "/ "):
		return errors.New("must not contain a path or whitespace")
	case !strings.Contains(domain, "."):
		return errors.New("must be a fully qualified domain")
	case domain != strings.ToLower(domain):
		return errors.New("must be lowercase")
	}
	return nil
}

// warnMissingOptionalConfig flags env that is not required to boot but backs
// features that will otherwise fail at first use, so an operator sees the gap
// at startup instead of in a user-facing error.
func warnMissingOptionalConfig(logger *slog.Logger, ws *workspace.Workspace) {
	if ws.OIDCClientID == "" || ws.OIDCClientSecret == "" {
		logger.Warn("WORKSPACE_OIDC_CLIENT_ID/_SECRET not set; Lakekeeper user management will fail")
	}
	if s3 := ws.EffectiveS3; s3.Bucket == "" || s3.AccessKeyID == "" || s3.SecretAccessKey == "" {
		logger.Warn("WORKSPACE_S3_* incomplete; file-upload pipelines are unavailable")
	}
	if ws.DuckFlightAuthToken == "" {
		logger.Warn("WORKSPACE_DUCKFLIGHT_AUTH_TOKEN not set; the query engine is disabled")
	}
}

// loadS3Config reads the workspace bucket (the box's fairtier-storage Secret)
// from WORKSPACE_S3_* env.
func loadS3Config() core.S3Config {
	return core.S3Config{
		Bucket:          os.Getenv("WORKSPACE_S3_BUCKET"),
		KeyPrefix:       os.Getenv("WORKSPACE_S3_KEY_PREFIX"),
		Endpoint:        os.Getenv("WORKSPACE_S3_ENDPOINT"),
		Region:          cmp.Or(os.Getenv("WORKSPACE_S3_REGION"), "auto"),
		AccessKeyID:     os.Getenv("WORKSPACE_S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("WORKSPACE_S3_SECRET_ACCESS_KEY"),
	}
}

// openDatabase connects to the box Postgres (PG_DSN), runs the platform
// migration set (control-plane tables stay empty — harmless), and builds the
// at-rest credential encryptor from CREDENTIAL_ENCRYPTION_KEY (optional).
func openDatabase(logger *slog.Logger) (*sql.DB, crypto.Encryptor, error) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		return nil, nil, errors.New("PG_DSN environment variable not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to ping database: %w", err)
	}
	if err := postgres.Migrate(db); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to run migrations: %w", err)
	}
	logger.Info("database ready")

	var enc crypto.Encryptor
	if keyB64 := os.Getenv("CREDENTIAL_ENCRYPTION_KEY"); keyB64 != "" {
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("decode CREDENTIAL_ENCRYPTION_KEY: %w", err)
		}
		enc, err = crypto.NewAESEncryptor(key)
		if err != nil {
			_ = db.Close()
			return nil, nil, fmt.Errorf("init credential encryptor: %w", err)
		}
		logger.Info("credential encryption enabled")
	}
	if err := postgres.MigrateEncryptCredentials(db, enc); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("encrypt existing credentials: %w", err)
	}
	return db, enc, nil
}

// buildInternalOpts assembles the internal-mux (:8081, local dlt-worker)
// handler options: tracing + tenant-bound service auth. The box serves exactly
// one tenant, so the trust anchor is its single known Casdoor issuer
// (ws.CasdoorIssuer); a token minted there binds this box's slug. This is the
// box counterpart of the control plane's BoxIssuerTrust, which must instead
// discriminate many box issuers by regex — a distinction the box does not
// have.
func buildInternalOpts(_ context.Context, logger *slog.Logger, ws *workspace.Workspace,
	jwks keyfunc.Keyfunc, otelInterceptor connect.Interceptor,
) (connect.HandlerOption, error) {
	// The rollout-era "log" mode is gone: the internal API always enforces
	// service auth. Fail loudly rather than silently tightening a deployment
	// that still sets the variable.
	if mode := os.Getenv("INTERNAL_AUTH_MODE"); mode != "" && mode != "enforce" {
		return nil, fmt.Errorf("INTERNAL_AUTH_MODE %q is not supported: the internal API always enforces service auth; unset the variable", mode)
	}

	boxTrust := server.NewPinnedBoxTrust(ws.CasdoorIssuer, ws.Slug, jwks)
	return connect.WithInterceptors(otelInterceptor, server.NewInternalAuthInterceptor(jwks, boxTrust, logger)), nil
}

// workspaceServiceNamesWithoutBoxCredential filters the central service-name
// list down to what this binary actually mounts.
func workspaceServiceNamesWithoutBoxCredential() []string {
	names := make([]string, 0, len(server.WorkspaceServiceNames))
	for _, n := range server.WorkspaceServiceNames {
		if n == boxcredentialv1connect.BoxCredentialServiceName {
			continue
		}
		names = append(names, n)
	}
	return names
}

// buildAssistServers wires the AI-drafting servers (DEEPSEEK_API_KEY
// preferred, ANTHROPIC_API_KEY fallback — a self-hoster's own key). Nil
// caller keeps the draft RPCs on UNIMPLEMENTED.
func buildAssistServers(resolver workspace.Resolver, logger *slog.Logger) (*server.PipelineAssistServer, *server.AssistServer) {
	var structuredCaller llm.StructuredCaller
	switch {
	case os.Getenv("DEEPSEEK_API_KEY") != "":
		logger.Info("AI drafting enabled", "provider", "deepseek")
		structuredCaller = llm.NewDeepSeekCaller(os.Getenv("DEEPSEEK_API_KEY"), os.Getenv("DEEPSEEK_MODEL"), logger)
	case os.Getenv("ANTHROPIC_API_KEY") != "":
		logger.Info("AI drafting enabled", "provider", "anthropic")
		structuredCaller = llm.NewAnthropicCaller(os.Getenv("ANTHROPIC_API_KEY"), os.Getenv("ANTHROPIC_MODEL"), logger)
	}

	draftLimiter := workspace.NewMemoryRateLimiter(10, time.Minute)
	pipelineAssist := &workspace.PipelineAssistService{
		Workspaces: resolver,
		Limiter:    draftLimiter,
		Logger:     logger,
	}
	assistSvc := &workspace.AssistService{
		Workspaces: resolver,
		Limiter:    draftLimiter,
		Logger:     logger,
	}
	if structuredCaller != nil {
		drafter := llm.NewDrafter(structuredCaller, logger)
		pipelineAssist.Drafter = drafter
		assistSvc.Transformations = drafter
		assistSvc.Rill = drafter
	}
	return &server.PipelineAssistServer{Service: pipelineAssist}, &server.AssistServer{Service: assistSvc}
}

// buildFingerprinter mirrors the control-plane binary: keyed HMAC fingerprints
// when CREDENTIAL_ENCRYPTION_KEY decodes, plain SHA-256 otherwise.
func buildFingerprinter() workspace.CredentialFingerprinter {
	key, err := base64.StdEncoding.DecodeString(os.Getenv("CREDENTIAL_ENCRYPTION_KEY"))
	if err != nil || len(key) == 0 {
		return crypto.SHA256Fingerprinter{}
	}
	return crypto.NewHMACFingerprinter(key)
}

// loadFileDropMaxBytes reads FILEDROP_MAX_BYTES (default DefaultUploadMaxBytes).
func loadFileDropMaxBytes() int64 {
	maxBytes := int64(workspace.DefaultUploadMaxBytes)
	if v := os.Getenv("FILEDROP_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxBytes = n
		}
	}
	return maxBytes
}

// loadDemoBucket says where the demo datasets are read from; a zero value
// disables the demo loader.
//
// DEMO_PUBLIC_BASE_URL is the one that matters here. The datasets are public
// domain, so serving them over an unauthenticated origin means this
// deployment can seed the demo with nothing handed to it — no token to
// deliver, none to rotate, and none to land in its credential mirror. The
// DEMO_R2_* fallback is for a deployment that keeps the bucket private.
func loadDemoBucket() workspace.DemoBucket {
	return workspace.DemoBucket{
		PublicBaseURL:   os.Getenv("DEMO_PUBLIC_BASE_URL"),
		AccessKeyID:     os.Getenv("DEMO_R2_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("DEMO_R2_SECRET_ACCESS_KEY"),
		Endpoint:        os.Getenv("DEMO_R2_ENDPOINT"),
		Bucket:          os.Getenv("DEMO_R2_BUCKET"),
		Region:          cmp.Or(os.Getenv("DEMO_R2_REGION"), "auto"),
	}
}

// loadDurationEnv parses a duration env var, falling back on absence or
// parse failure.
func loadDurationEnv(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env, using fallback", "name", name, "value", v, "fallback", fallback)
		return fallback
	}
	return d
}

// splitCSV splits a comma-separated env value, trimming whitespace and
// dropping empty items; nil for an empty value.
func splitCSV(s string) []string {
	var out []string
	for item := range strings.SplitSeq(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// corsAllowedOrigins returns the origins from CORS_ALLOWED_ORIGINS. There is
// deliberately no default: the vendor SaaS Console is the wrong origin for a
// self-hosted box, and every deployment knows where its own Console lives.
func corsAllowedOrigins() []string {
	return splitCSV(os.Getenv("CORS_ALLOWED_ORIGINS"))
}

// firstCORSOrigin returns the first configured CORS origin, or "" when unset.
func firstCORSOrigin() string {
	if origins := corsAllowedOrigins(); len(origins) > 0 {
		return origins[0]
	}
	return ""
}

// consoleClientID returns the box Casdoor app the Console signs in with.
//
// It is seeded by the box's console-seed job, so it is legitimately empty on a
// box deployed before that job existed — the bootstrap document then advertises
// no sign-in and the Console reports that, rather than starting a PKCE flow
// that cannot complete.
func consoleClientID(logger *slog.Logger) string {
	id := os.Getenv("WORKSPACE_CONSOLE_CLIENT_ID")
	if id == "" {
		logger.Warn("WORKSPACE_CONSOLE_CLIENT_ID not set; the Console cannot sign in to this workspace")
	}
	return id
}

// wrapPublicHandler wraps the public mux with OTel HTTP tracing and CORS for
// browser Connect clients (same shape as the control-plane binary). With no
// configured origins the CORS middleware is skipped entirely — rs/cors treats
// an empty AllowedOrigins list as allow-all, and failing browser calls loudly
// beats silently trusting every origin.
func wrapPublicHandler(mux *http.ServeMux, origins []string, logger *slog.Logger) http.Handler {
	skipProbes := otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
	})
	tracedMux := otelhttp.NewHandler(mux, "workspace-api",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
		skipProbes,
	)
	if len(origins) == 0 {
		logger.Warn("CORS_ALLOWED_ORIGINS not set; browser cross-origin calls (the Console) will be refused")
		return tracedMux
	}
	return rscors.New(rscors.Options{
		AllowedOrigins: origins,
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: append(connectcors.AllowedHeaders(), "Authorization"),
		ExposedHeaders: connectcors.ExposedHeaders(),
	}).Handler(tracedMux)
}

// serveAndShutdown starts both HTTP servers, blocks until the context is
// cancelled or a listener fails, then gracefully shuts both down.
func serveAndShutdown(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger,
	httpServer, internalServer *http.Server, internalPort string,
) error {
	errCh := make(chan error, 2)

	go func() {
		logger.Info("server listening", "addr", ":8080")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("public server failed: %w", err)
		}
	}()
	go func() {
		logger.Info("internal server listening", "addr", ":"+internalPort)
		if err := internalServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("internal server failed: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down servers...")
	case err := <-errCh:
		logger.Error("server error, shutting down...", "err", err)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	var shutdownErr error
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		shutdownErr = fmt.Errorf("failed to shutdown public server: %w", err)
	}
	if err := internalServer.Shutdown(shutdownCtx); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("failed to shutdown internal server: %w", err))
	}
	if shutdownErr != nil {
		return shutdownErr
	}
	logger.Info("servers stopped gracefully")
	return nil
}
