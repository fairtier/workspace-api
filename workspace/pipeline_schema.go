package workspace

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ErrInvalidSourceConfig indicates that the pipeline's sourceConfig JSON
// failed schema validation for the given source type. Field is the JSON path
// of the offending field (e.g. "base_url"), used to build a FieldViolation.
type ErrInvalidSourceConfig struct {
	Field string
	Msg   string
}

func (e *ErrInvalidSourceConfig) Error() string { return e.Msg }

// ErrInvalidSourceCredentials indicates that the pipeline's sourceCredentials
// JSON failed schema validation for the given source type. Field is the JSON
// path of the offending field, used to build a FieldViolation.
type ErrInvalidSourceCredentials struct {
	Field string
	Msg   string
}

func (e *ErrInvalidSourceCredentials) Error() string { return e.Msg }

// ValidateSourceConfig validates sourceConfig JSON for the given source type.
func ValidateSourceConfig(sourceType string, raw json.RawMessage) error {
	switch sourceType {
	case "rest_api":
		return validateRestAPIConfig(raw)
	case "sql_database":
		return validateSQLDatabaseConfig(raw)
	case "filesystem":
		return validateFilesystemConfig(raw)
	case "google_sheets":
		return validateGoogleSheetsConfig(raw)
	case SourceTypeFileUpload:
		return validateFileUploadConfig(raw)
	default:
		return &ErrInvalidSourceConfig{Field: "source_type", Msg: fmt.Sprintf("unknown source type: %q", sourceType)}
	}
}

// ValidateSourceCredentials validates sourceCredentials JSON for the given
// source type.
//
// config is the pipeline's already-validated sourceConfig, because whether a
// credential is required at all can depend on it: a filesystem source over a
// public http(s) origin has nothing to authenticate to, and demanding a
// credential there would mean inventing one.
func ValidateSourceCredentials(sourceType string, config, raw json.RawMessage) error {
	switch sourceType {
	case "rest_api":
		return validateRestAPICreds(raw)
	case "sql_database":
		return validateSQLDatabaseCreds(raw)
	case "filesystem":
		return validateFilesystemCreds(config, raw)
	case "google_sheets":
		return validateGoogleSheetsCreds(raw)
	case SourceTypeFileUpload:
		return validateFileUploadCreds(raw)
	default:
		return &ErrInvalidSourceCredentials{Field: "source_type", Msg: fmt.Sprintf("unknown source type: %q", sourceType)}
	}
}

// isEmptyJSON returns true when raw is nil, empty, "{}", or "null".
func isEmptyJSON(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	s := strings.TrimSpace(string(raw))
	return s == "{}" || s == "null"
}

// --- rest_api ---

type restAPIConfig struct {
	BaseURL          string             `json:"base_url"`
	Resources        []restAPIResource  `json:"resources"`
	Params           json.RawMessage    `json:"params"`
	Paginator        json.RawMessage    `json:"paginator"`
	PrimaryKey       json.RawMessage    `json:"primary_key"`
	WriteDisposition string             `json:"write_disposition"`
	Incremental      *incrementalConfig `json:"incremental"`
}

type restAPIResource struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
}

type incrementalConfig struct {
	CursorPath string `json:"cursor_path"`
}

func validateRestAPIConfig(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: "rest_api: sourceConfig is required"}
	}
	var cfg restAPIConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: fmt.Sprintf("rest_api: invalid sourceConfig JSON: %v", err)}
	}
	if cfg.BaseURL == "" {
		return &ErrInvalidSourceConfig{Field: "base_url", Msg: "rest_api: base_url is required"}
	}
	if len(cfg.Resources) == 0 {
		return &ErrInvalidSourceConfig{Field: "resources", Msg: "rest_api: resources must contain at least one entry"}
	}
	if err := validateRestAPIResources(cfg.Resources); err != nil {
		return err
	}
	if err := validateRestAPIWriteDisposition(cfg.WriteDisposition); err != nil {
		return err
	}
	if cfg.Incremental != nil && cfg.Incremental.CursorPath == "" {
		return &ErrInvalidSourceConfig{Field: "incremental.cursor_path", Msg: "rest_api: incremental.cursor_path is required when incremental is set"}
	}
	return nil
}

// validateRestAPIResources checks that every rest_api resource has a name and endpoint.
func validateRestAPIResources(resources []restAPIResource) error {
	for i, r := range resources {
		if r.Name == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("resources[%d].name", i), Msg: fmt.Sprintf("rest_api: resources[%d].name is required", i)}
		}
		if r.Endpoint == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("resources[%d].endpoint", i), Msg: fmt.Sprintf("rest_api: resources[%d].endpoint is required", i)}
		}
	}
	return nil
}

// validateRestAPIWriteDisposition checks the optional rest_api write_disposition value.
func validateRestAPIWriteDisposition(writeDisposition string) error {
	if writeDisposition == "" {
		return nil
	}
	switch writeDisposition {
	case "append", "replace", "merge":
		return nil
	default:
		return &ErrInvalidSourceConfig{Field: "write_disposition", Msg: fmt.Sprintf("rest_api: write_disposition must be append, replace, or merge; got %q", writeDisposition)}
	}
}

type restAPICreds struct {
	Auth    json.RawMessage `json:"auth"`
	APIKey  string          `json:"api_key"`
	Headers json.RawMessage `json:"headers"`
}

func validateRestAPICreds(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return nil // all fields optional
	}
	var creds restAPICreds
	if err := json.Unmarshal(raw, &creds); err != nil {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: fmt.Sprintf("rest_api: invalid sourceCredentials JSON: %v", err)}
	}
	return nil
}

// --- sql_database ---

type sqlDatabaseConfig struct {
	Tables       []string              `json:"tables"`
	TablesConfig []sqlDatabaseTableCfg `json:"tables_config"`
}

type sqlDatabaseTableCfg struct {
	Name        string             `json:"name"`
	Incremental *incrementalConfig `json:"incremental"`
}

func validateSQLDatabaseConfig(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return nil // all fields optional
	}
	var cfg sqlDatabaseConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: fmt.Sprintf("sql_database: invalid sourceConfig JSON: %v", err)}
	}
	for i, tc := range cfg.TablesConfig {
		if tc.Name == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("tables_config[%d].name", i), Msg: fmt.Sprintf("sql_database: tables_config[%d].name is required", i)}
		}
		if tc.Incremental != nil && tc.Incremental.CursorPath == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("tables_config[%d].incremental.cursor_path", i), Msg: fmt.Sprintf("sql_database: tables_config[%d].incremental.cursor_path is required when incremental is set", i)}
		}
	}
	return nil
}

type sqlDatabaseCreds struct {
	ConnectionString string `json:"connection_string"`
}

func validateSQLDatabaseCreds(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "sql_database: sourceCredentials is required"}
	}
	var creds sqlDatabaseCreds
	if err := json.Unmarshal(raw, &creds); err != nil {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: fmt.Sprintf("sql_database: invalid sourceCredentials JSON: %v", err)}
	}
	if creds.ConnectionString == "" {
		return &ErrInvalidSourceCredentials{Field: "connection_string", Msg: "sql_database: connection_string is required"}
	}
	return validateSQLDatabaseDialect(creds.ConnectionString)
}

// validateSQLDatabaseDialect rejects connection strings for database engines
// the dlt-worker cannot reach, at save time instead of as a runtime crash on
// the box. The allowlist mirrors the drivers installed in the dlt-worker
// image (its pyproject.toml: psycopg only, i.e. PostgreSQL) — the same
// cross-repo parity contract as the source-credentials shapes: adding a
// driver to the worker means extending this list in the same change, and
// teaching the pipeline-draft system prompt (llm/drafter.go) the new engine.
func validateSQLDatabaseDialect(connectionString string) error {
	scheme, _, ok := strings.Cut(connectionString, "://")
	if !ok {
		return &ErrInvalidSourceCredentials{Field: "connection_string",
			Msg: "sql_database: connection_string must be a SQLAlchemy URL like postgresql://user:password@host:5432/dbname"}
	}
	dialect, driver, _ := strings.Cut(strings.ToLower(scheme), "+")
	if dialect != "postgres" && dialect != "postgresql" {
		return &ErrInvalidSourceCredentials{Field: "connection_string",
			Msg: fmt.Sprintf("sql_database: only PostgreSQL is supported (the worker has no %q driver); the connection string must start with postgresql://", dialect)}
	}
	// The worker ships psycopg (v3) only; any other explicit driver
	// (psycopg2, asyncpg, pg8000, ...) fails on the box with a missing module.
	if driver != "" && driver != "psycopg" {
		return &ErrInvalidSourceCredentials{Field: "connection_string",
			Msg: fmt.Sprintf("sql_database: driver %q is not installed on the worker; use postgresql:// (or postgresql+psycopg://)", driver)}
	}
	return nil
}

// --- filesystem ---

type filesystemConfig struct {
	BucketURL string `json:"bucket_url"`
	FileGlob  string `json:"file_glob"`
}

func validateFilesystemConfig(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: "filesystem: sourceConfig is required"}
	}
	var cfg filesystemConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: fmt.Sprintf("filesystem: invalid sourceConfig JSON: %v", err)}
	}
	if cfg.BucketURL == "" {
		return &ErrInvalidSourceConfig{Field: "bucket_url", Msg: "filesystem: bucket_url is required"}
	}
	return nil
}

type filesystemCreds struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	EndpointURL     string `json:"endpoint_url"`
	Region          string `json:"region"`
}

// filesystemTable maps one warehouse table to the file(s) it is read from,
// relative to the source's bucket_url. Part of the dlt-worker contract
// (worker ≥0.0.6): each entry becomes a (filesystem | read_<format>) resource.
type filesystemTable struct {
	Name     string `json:"name"`
	FileGlob string `json:"file_glob,omitempty"`
	// Files names the objects to read instead of matching them. Required
	// when bucket_url is http(s): a public object store serves by key and
	// will not list a directory, so there is nothing to glob against.
	Files []string `json:"files,omitempty"`
}

// isPublicBucketURL reports whether a filesystem sourceConfig reads over an
// unauthenticated http(s) origin rather than an object-store bucket.
func isPublicBucketURL(config json.RawMessage) bool {
	if isEmptyJSON(config) {
		return false
	}
	var cfg filesystemConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return false
	}
	lower := strings.ToLower(cfg.BucketURL)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

func validateFilesystemCreds(config, raw json.RawMessage) error {
	// A public origin serves the objects to anyone; there is no credential
	// to check and none is accepted.
	if isPublicBucketURL(config) {
		return nil
	}
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "filesystem: sourceCredentials is required"}
	}
	var creds filesystemCreds
	if err := json.Unmarshal(raw, &creds); err != nil {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: fmt.Sprintf("filesystem: invalid sourceCredentials JSON: %v", err)}
	}
	if creds.AccessKeyID == "" {
		return &ErrInvalidSourceCredentials{Field: "access_key_id", Msg: "filesystem: access_key_id is required"}
	}
	if creds.SecretAccessKey == "" {
		return &ErrInvalidSourceCredentials{Field: "secret_access_key", Msg: "filesystem: secret_access_key is required"}
	}
	return nil
}

// --- google_sheets ---

type googleSheetsConfig struct {
	SpreadsheetURLOrID string   `json:"spreadsheet_url_or_id"`
	RangeNames         []string `json:"range_names"`
}

func validateGoogleSheetsConfig(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: "google_sheets: sourceConfig is required"}
	}
	var cfg googleSheetsConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: fmt.Sprintf("google_sheets: invalid sourceConfig JSON: %v", err)}
	}
	if cfg.SpreadsheetURLOrID == "" {
		return &ErrInvalidSourceConfig{Field: "spreadsheet_url_or_id", Msg: "google_sheets: spreadsheet_url_or_id is required"}
	}
	for i, r := range cfg.RangeNames {
		if strings.TrimSpace(r) == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("range_names[%d]", i), Msg: fmt.Sprintf("google_sheets: range_names[%d] must not be empty", i)}
		}
	}
	return nil
}

// --- file_upload ---

// SourceTypeFileUpload is the managed file-drop source: the customer uploads
// CSV/Parquet/JSONL files through the Console into their own bucket under
// uploads/<pipeline_id>/, and the platform serves the pipeline to the
// dlt-worker as a regular "filesystem" source with server-injected bucket URL
// and credentials (see PipelineService.GetEnabledPipelines). Customers never
// see or supply S3 credentials for this type.
const SourceTypeFileUpload = "file_upload"

// fileUploadConfig is the stored source_config for file_upload pipelines.
// The files list is platform-managed: FileDropService.Upload/Delete maintain
// it, one entry per uploaded object.
type fileUploadConfig struct {
	Files []UploadedFile `json:"files,omitempty"`
}

// UploadedFile is one file dropped into a file_upload pipeline's prefix.
// Name is the warehouse table the file loads into (derived from the
// filename); File is the object name relative to the pipeline's prefix.
type UploadedFile struct {
	Name       string `json:"name"`
	File       string `json:"file"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	UploadedAt string `json:"uploaded_at,omitempty"`
	// Missing is set by FileDropService.List when the recorded object is no
	// longer in the bucket (removed out-of-band, or a differently-named
	// re-upload left this table's glob matching nothing). It is a transient
	// view flag — json:"-" keeps it out of the stored source_config — that the
	// Console surfaces so a table loading 0 rows is never a silent dead end.
	Missing bool `json:"-"`
}

func validateFileUploadConfig(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return nil // files are added after creation via FileDropService
	}
	var cfg fileUploadConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return &ErrInvalidSourceConfig{Field: "source_config", Msg: fmt.Sprintf("file_upload: invalid sourceConfig JSON: %v", err)}
	}
	for i, f := range cfg.Files {
		if f.Name == "" {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("files[%d].name", i), Msg: fmt.Sprintf("file_upload: files[%d].name is required", i)}
		}
		if err := ValidateUploadFilename(f.File); err != nil {
			return &ErrInvalidSourceConfig{Field: fmt.Sprintf("files[%d].file", i), Msg: fmt.Sprintf("file_upload: files[%d].file: %v", i, err)}
		}
	}
	return nil
}

// googleSheetsCreds carries the customer's Google Sheets credentials. Exactly
// one of two methods must be present:
//
//   - service_account_key: the GCP service-account key JSON (the file
//     downloaded from the Google Cloud console). The spreadsheet must be shared
//     read-only with the key's client_email. The advanced/automation path.
//   - oauth: a delegated-user grant from the "Sign in with Google" flow. The
//     easy path for ordinary users — no JSON, no manual sharing.
type googleSheetsCreds struct {
	ServiceAccountKey json.RawMessage    `json:"service_account_key,omitempty"`
	OAuth             *googleSheetsOAuth `json:"oauth,omitempty"`
}

// googleSheetsOAuth is the delegated-user OAuth credential for Google Sheets.
// It takes several shapes across its lifecycle:
//
//   - Console input (create/update): either GrantID — a reference to a
//     short-lived server-side grant minted by the /oauth/google/callback flow,
//     which PipelineService swaps for the stored RefreshToken before
//     persisting so the refresh token never travels through the browser — or
//     ConnectionID, a reference to a workspace-level Connection (the "connect
//     Google once" entity) that stays a reference at rest.
//   - Stored (encrypted at rest): RefreshToken (+ Email for display, + ClientID
//     recording which of the customer's OAuth apps minted the token), OR just
//     ConnectionID for connection-referencing pipelines.
//   - Served to the worker: RefreshToken plus ClientID/ClientSecret, resolved
//     per customer from their own OAuth app (OAuthClientStore) — and, for
//     ConnectionID rows, the refresh token resolved from the Connection first —
//     by GetEnabledPipelines and by the mirror's .age render. The served shape
//     is identical for both storage forms, so the worker never learns which
//     one a pipeline uses.
//
// ClientID is stored but ClientSecret never is. Storing the id is what lets a
// stale connection be reported rather than merely failing: a refresh token is
// only refreshable by the client it was issued to, so once the customer swaps
// their Google app every earlier token is dead, and comparing the stored id
// against the current one is the only way to know that before the run.
type googleSheetsOAuth struct {
	GrantID      string `json:"grant_id,omitempty"`
	ConnectionID string `json:"connection_id,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Email        string `json:"email,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
}

type googleServiceAccountKey struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func validateGoogleSheetsCreds(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "google_sheets: sourceCredentials is required"}
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil {
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: fmt.Sprintf("google_sheets: invalid sourceCredentials JSON: %v", err)}
	}
	hasSA := !isEmptyJSON(creds.ServiceAccountKey)
	hasOAuth := creds.OAuth != nil
	switch {
	case hasSA && hasOAuth:
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "google_sheets: provide either service_account_key or oauth, not both"}
	case hasOAuth:
		return validateGoogleSheetsOAuth(creds.OAuth)
	case hasSA:
		return validateGoogleSheetsServiceAccount(creds.ServiceAccountKey)
	default:
		return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "google_sheets: service_account_key or oauth is required"}
	}
}

// validateGoogleSheetsOAuth accepts any shape the Console/store can present:
// a grant_id (one-shot Sign in with Google), a connection_id (workspace-level
// Google connection), or a refresh_token (already-stored form).
func validateGoogleSheetsOAuth(o *googleSheetsOAuth) error {
	if o.GrantID == "" && o.ConnectionID == "" && o.RefreshToken == "" {
		return &ErrInvalidSourceCredentials{Field: "oauth", Msg: "google_sheets: oauth requires grant_id (from Sign in with Google), connection_id, or refresh_token"}
	}
	return nil
}

func validateGoogleSheetsServiceAccount(serviceAccountKey json.RawMessage) error {
	// Accept the key either as the JSON object itself or as a JSON-encoded string.
	keyRaw := serviceAccountKey
	var keyStr string
	if err := json.Unmarshal(keyRaw, &keyStr); err == nil {
		keyRaw = json.RawMessage(keyStr)
	}
	var key googleServiceAccountKey
	if err := json.Unmarshal(keyRaw, &key); err != nil {
		return &ErrInvalidSourceCredentials{Field: "service_account_key", Msg: fmt.Sprintf("google_sheets: service_account_key must be a service-account key JSON object: %v", err)}
	}
	if key.ClientEmail == "" {
		return &ErrInvalidSourceCredentials{Field: "service_account_key", Msg: "google_sheets: service_account_key is missing client_email"}
	}
	if key.PrivateKey == "" {
		return &ErrInvalidSourceCredentials{Field: "service_account_key", Msg: "google_sheets: service_account_key is missing private_key"}
	}
	return nil
}

// googleSheetsGrantID returns the OAuth grant_id carried by a google_sheets
// pipeline's credentials, or ("", false) when the pipeline is not google_sheets
// or carries no oauth.grant_id (service-account or already-swapped creds).
func googleSheetsGrantID(sourceType string, raw json.RawMessage) (string, bool) {
	if sourceType != "google_sheets" || isEmptyJSON(raw) {
		return "", false
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil || creds.OAuth == nil {
		return "", false
	}
	if creds.OAuth.GrantID == "" {
		return "", false
	}
	return creds.OAuth.GrantID, true
}

// googleSheetsConnectionID returns the workspace Connection referenced by a
// google_sheets pipeline's credentials, or ("", false) when the pipeline is
// not google_sheets, carries no oauth.connection_id, or already embeds a
// refresh token (embedded wins: it is the resolved form).
func googleSheetsConnectionID(sourceType string, raw json.RawMessage) (string, bool) {
	if sourceType != "google_sheets" || isEmptyJSON(raw) {
		return "", false
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil || creds.OAuth == nil {
		return "", false
	}
	if creds.OAuth.ConnectionID == "" || creds.OAuth.RefreshToken != "" {
		return "", false
	}
	return creds.OAuth.ConnectionID, true
}

// ConnectionID returns the workspace Connection this pipeline's stored
// credentials reference, or "" when it holds its own credentials (or none).
// A reference is not credential material, so unlike the credentials
// themselves it is safe to hand back to the editor — and necessary there: an
// editor that cannot show which account is attached cannot offer to detach it.
func (p *Pipeline) ConnectionID() string {
	id, _ := googleSheetsConnectionID(p.SourceType, p.SourceCredentials)
	return id
}

// HasCredentials reports whether the pipeline has stored source credentials.
// Lets the editor distinguish "keep existing" from "there is nothing to keep".
func (p *Pipeline) HasCredentials() bool {
	return !isEmptyJSON(p.SourceCredentials)
}

// googleSheetsStoredOAuthCreds builds the persisted credential JSON for an
// OAuth grant: the refresh token, the granting email (for display), and the
// customer OAuth client that minted it.
func googleSheetsStoredOAuthCreds(refreshToken, email, clientID string) (json.RawMessage, error) {
	return json.Marshal(googleSheetsCreds{OAuth: &googleSheetsOAuth{
		RefreshToken: refreshToken,
		Email:        email,
		ClientID:     clientID,
	}})
}

// googleSheetsOAuthClientID returns the OAuth client id a stored google_sheets
// credential was minted with, and whether the credential is an OAuth one at all.
// An OAuth credential with no recorded id is reported as ("", true): that is a
// legacy row from when one shared app served every customer, and it is exactly
// as stale as one naming a different app.
func googleSheetsOAuthClientID(sourceType string, raw json.RawMessage) (string, bool) {
	if sourceType != "google_sheets" || isEmptyJSON(raw) {
		return "", false
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil || creds.OAuth == nil || creds.OAuth.RefreshToken == "" {
		return "", false
	}
	return creds.OAuth.ClientID, true
}

// injectGoogleSheetsOAuthClient adds the customer's OAuth client_id/client_secret
// to a stored google_sheets OAuth credential so the worker can refresh access
// tokens. It returns (raw, false) unchanged when the pipeline is not an OAuth
// google_sheets credential, or when the stored credential was minted by a
// different client than the one passed — refreshing that token would fail at
// Google, and shipping a credential that pairs one app's secret with another
// app's refresh token only turns a clear "reconnect" into an opaque run failure.
func injectGoogleSheetsOAuthClient(sourceType string, raw json.RawMessage, clientID, clientSecret string) (json.RawMessage, bool) {
	if sourceType != "google_sheets" || isEmptyJSON(raw) {
		return raw, false
	}
	var creds googleSheetsCreds
	if err := json.Unmarshal(raw, &creds); err != nil || creds.OAuth == nil || creds.OAuth.RefreshToken == "" {
		return raw, false
	}
	if creds.OAuth.ClientID != clientID {
		return raw, false
	}
	creds.OAuth.ClientSecret = clientSecret
	injected, err := json.Marshal(creds)
	if err != nil {
		return raw, false
	}
	return injected, true
}

// validateFileUploadCreds rejects any credentials: the platform injects the
// customer's own storage credentials when serving the pipeline to the worker.
func validateFileUploadCreds(raw json.RawMessage) error {
	if isEmptyJSON(raw) {
		return nil
	}
	return &ErrInvalidSourceCredentials{Field: "source_credentials", Msg: "file_upload: credentials are managed by the platform; leave empty"}
}
