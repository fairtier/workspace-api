package workspace

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateSourceConfig(t *testing.T) {
	tests := []struct {
		name       string
		sourceType string
		raw        json.RawMessage
		wantErr    bool
		wantCfgErr bool // expect *ErrInvalidSourceConfig
		errSubstr  string
	}{
		// --- rest_api ---
		{
			name:       "rest_api valid minimal",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"users","endpoint":"/users"}]}`),
		},
		{
			name:       "rest_api valid with optional fields",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"users","endpoint":"/users"}],"write_disposition":"merge","incremental":{"cursor_path":"updated_at"}}`),
		},
		{
			name:       "rest_api missing base_url",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"resources":[{"name":"users","endpoint":"/users"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "base_url is required",
		},
		{
			name:       "rest_api empty resources",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "at least one entry",
		},
		{
			name:       "rest_api resource missing name",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"endpoint":"/users"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "resources[0].name is required",
		},
		{
			name:       "rest_api resource missing endpoint",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"users"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "resources[0].endpoint is required",
		},
		{
			name:       "rest_api invalid write_disposition",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"u","endpoint":"/u"}],"write_disposition":"upsert"}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "write_disposition must be",
		},
		{
			name:       "rest_api incremental without cursor_path",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"base_url":"https://api.example.com","resources":[{"name":"u","endpoint":"/u"}],"incremental":{}}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "cursor_path is required",
		},
		{
			name:       "rest_api empty config",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "sourceConfig is required",
		},
		{
			name:       "rest_api malformed JSON",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{not json`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "invalid sourceConfig JSON",
		},

		// --- sql_database ---
		{
			name:       "sql_database valid empty config",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{}`),
		},
		{
			name:       "sql_database valid with tables",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"tables":["users","orders"]}`),
		},
		{
			name:       "sql_database valid with tables_config",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"tables_config":[{"name":"users","incremental":{"cursor_path":"id"}}]}`),
		},
		{
			name:       "sql_database tables_config missing name",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"tables_config":[{"incremental":{"cursor_path":"id"}}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "tables_config[0].name is required",
		},
		{
			name:       "sql_database incremental without cursor_path",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"tables_config":[{"name":"users","incremental":{}}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "cursor_path is required",
		},

		// --- filesystem ---
		{
			name:       "filesystem valid minimal",
			sourceType: "filesystem",
			raw:        json.RawMessage(`{"bucket_url":"s3://my-bucket"}`),
		},
		{
			name:       "filesystem valid with file_glob",
			sourceType: "filesystem",
			raw:        json.RawMessage(`{"bucket_url":"s3://my-bucket","file_glob":"*.csv"}`),
		},
		{
			name:       "filesystem missing bucket_url",
			sourceType: "filesystem",
			raw:        json.RawMessage(`{"file_glob":"*.csv"}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "bucket_url is required",
		},

		// --- google_sheets ---
		{
			name:       "google_sheets valid minimal",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"spreadsheet_url_or_id":"1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"}`),
		},
		{
			name:       "google_sheets valid with URL and ranges",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"spreadsheet_url_or_id":"https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0","range_names":["Orders","Customers!A1:D"]}`),
		},
		{
			name:       "google_sheets missing spreadsheet",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"range_names":["Orders"]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "spreadsheet_url_or_id is required",
		},
		{
			name:       "google_sheets empty range name",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"spreadsheet_url_or_id":"abc","range_names":["Orders",""]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "range_names[1] must not be empty",
		},
		{
			name:       "google_sheets empty config",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "sourceConfig is required",
		},

		// --- duckdb ---
		{
			name:       "duckdb valid with attach",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","attach":"host={host} user={user} password={password} database=shop","tables":[{"name":"orders"}]}`),
		},
		{
			name:       "duckdb reader extension query-only (pdf)",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"pdf","tables":[{"name":"pages","query":"SELECT page, text FROM read_pdf('https://example.com/report.pdf')"}]}`),
		},
		{
			// The Drive-backed reader shape: gdrive registers a gdrive://
			// filesystem, so the reader function is still pdf's — and pdf
			// must therefore be LOADed beside it. `extension: "gdrive"`
			// alone saved fine and then failed on the box with "Table
			// Function with name read_pdf does not exist", which is what
			// the plural form exists to stop.
			name:       "duckdb reader extension query-only (gdrive pdf)",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extensions":["gdrive","pdf"],"tables":[{"name":"invoices","query":"SELECT page, text FROM read_pdf('gdrive://id:1a2b3c')"}]}`),
		},
		{
			name:       "duckdb extensions list, one entry",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extensions":["pdf"],"tables":[{"name":"pages","query":"SELECT * FROM read_pdf('https://example.com/r.pdf')"}]}`),
		},
		{
			name:       "duckdb both extension forms refused",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"gdrive","extensions":["gdrive","pdf"],"tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "set extension or extensions, not both",
		},
		{
			name:       "duckdb extensions empty list is missing",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extensions":[],"tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "extension is required",
		},
		{
			// The allowlist applies to every member, not just the first:
			// a list is not a way around it.
			name:       "duckdb extensions member not allowlisted",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extensions":["gdrive","postgres"],"tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  `extension "postgres" is not supported`,
		},
		{
			name:       "duckdb gdrive native sheet via read_csv",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"gdrive","tables":[{"name":"budget","query":"SELECT * FROM read_csv('gdrive://Finance/Budget')"}]}`),
		},
		{
			name:       "duckdb gdrive table needs query without attach",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "query is required when no attach template is set",
		},
		{
			name:       "duckdb valid query-only with incremental",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","tables":[{"name":"orders","query":"SELECT * FROM src.orders","cursor_column":"updated_at","primary_key":"id"}]}`),
		},
		{
			name:       "duckdb config empty",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "sourceConfig is required",
		},
		{
			name:       "duckdb missing extension",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "extension is required",
		},
		{
			name:       "duckdb extension not allowlisted",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"postgres","tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "is not supported",
		},
		{
			name:       "duckdb extension not an identifier",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql; DROP","tables":[{"name":"t","query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "invalid extension name",
		},
		{
			name:       "duckdb empty tables",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","tables":[]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "at least one entry",
		},
		{
			name:       "duckdb table missing name",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","tables":[{"query":"SELECT 1"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "tables[0].name is required",
		},
		{
			name:       "duckdb table needs query without attach",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","tables":[{"name":"orders"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "query is required when no attach",
		},
		{
			name:       "duckdb cursor_column not a column name",
			sourceType: "duckdb",
			raw:        json.RawMessage(`{"extension":"mysql","attach":"database=shop","tables":[{"name":"t","cursor_column":"id; DROP"}]}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "not a plain column name",
		},

		// --- unknown source type ---
		{
			name:       "unknown source type",
			sourceType: "kafka",
			raw:        json.RawMessage(`{}`),
			wantErr:    true,
			wantCfgErr: true,
			errSubstr:  "unknown source type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSourceConfig(tt.sourceType, tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSourceConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if tt.wantCfgErr {
					if _, ok := errors.AsType[*ErrInvalidSourceConfig](err); !ok {
						t.Errorf("expected *ErrInvalidSourceConfig, got %T", err)
					}
				}
				if tt.errSubstr != "" && !contains(err.Error(), tt.errSubstr) {
					t.Errorf("expected error to contain %q, got %q", tt.errSubstr, err.Error())
				}
			}
		})
	}
}

func TestValidateSourceCredentials(t *testing.T) {
	tests := []struct {
		name         string
		sourceType   string
		config       json.RawMessage // sourceConfig; only filesystem consults it
		raw          json.RawMessage
		wantErr      bool
		wantCredsErr bool // expect *ErrInvalidSourceCredentials
		errSubstr    string
	}{
		// --- rest_api ---
		{
			name:       "rest_api creds empty is ok",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{}`),
		},
		{
			name:       "rest_api creds with api_key",
			sourceType: "rest_api",
			raw:        json.RawMessage(`{"api_key":"secret123"}`),
		},

		// --- sql_database ---
		{
			name:       "sql_database creds valid",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"connection_string":"postgres://user:pass@host/db"}`),
		},
		{
			name:         "sql_database creds missing connection_string",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"other":"val"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "connection_string is required",
		},
		{
			name:       "sql_database creds explicit psycopg driver",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"connection_string":"postgresql+psycopg://user:pass@host:5432/db"}`),
		},
		{
			name:       "sql_database creds dialect case-insensitive",
			sourceType: "sql_database",
			raw:        json.RawMessage(`{"connection_string":"PostgreSQL://user:pass@host/db"}`),
		},
		{
			name:         "sql_database creds mysql rejected",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"connection_string":"mysql://user:pass@host/db"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "only PostgreSQL is supported",
		},
		{
			name:         "sql_database creds mysql rejection points at duckdb",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"connection_string":"mysql://user:pass@host/db"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    `use the "duckdb" source type`,
		},

		// --- duckdb ---
		{
			name:       "duckdb creds fill every placeholder",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"mysql","attach":"host={host} user={user} password={password} database=shop","tables":[{"name":"orders"}]}`),
			raw:        json.RawMessage(`{"attach_params":{"host":"db.example.com","user":"u","password":"s"}}`),
		},
		{
			name:       "duckdb creds empty ok without placeholders",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"mysql","attach":"host=public.example.com user=ro database=demo","tables":[{"name":"orders"}]}`),
			raw:        nil,
		},
		{
			name:         "duckdb creds missing placeholder named",
			sourceType:   "duckdb",
			config:       json.RawMessage(`{"extension":"mysql","attach":"host={host} password={password} database=shop","tables":[{"name":"orders"}]}`),
			raw:          json.RawMessage(`{"attach_params":{"host":"h"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "attach_params missing password",
		},
		{
			name:       "duckdb creds secret only",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"mysql","tables":[{"name":"t","query":"SELECT 1"}]}`),
			raw:        json.RawMessage(`{"secret":{"token":"abc"}}`),
		},
		{
			// The shape the gdrive extension expects, pinned here because it
			// is a cross-repo contract with the worker's duckdb_source.py.
			name:       "duckdb gdrive creds oauth secret",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices","query":"SELECT * FROM read_csv('gdrive://id:1a2b3c')"}]}`),
			raw:        json.RawMessage(`{"secret":{"PROVIDER":"config","REFRESH_TOKEN":"rt","CLIENT_ID":"cid","CLIENT_SECRET":"cs"}}`),
		},
		{
			// The Connection path: one Google sign-in, referenced rather than
			// pasted. Resolved to the flattened secret at serve/render time.
			name:       "duckdb gdrive creds connection reference",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices","query":"SELECT * FROM read_csv('gdrive://id:1a2b3c')"}]}`),
			raw:        json.RawMessage(`{"oauth":{"connection_id":"c1"}}`),
		},
		{
			name:       "duckdb gdrive creds grant reference",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices","query":"SELECT * FROM read_csv('gdrive://id:1a2b3c')"}]}`),
			raw:        json.RawMessage(`{"oauth":{"grant_id":"g1"}}`),
		},
		{
			// This refusal is load-bearing: the serve and render paths read
			// "duckdb credential with an oauth member" as "gdrive" without
			// consulting source_config, which only holds if saves enforce it.
			name:         "duckdb oauth rejected when gdrive is not loaded",
			sourceType:   "duckdb",
			config:       json.RawMessage(`{"extension":"mysql","attach":"database=shop","tables":[{"name":"orders"}]}`),
			raw:          json.RawMessage(`{"oauth":{"connection_id":"c1"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    `only used by the "gdrive" extension`,
		},
		{
			// gdrive among several, not gdrive alone: a PDF in Drive is
			// still a Google-backed source.
			name:       "duckdb gdrive creds oauth with a second extension",
			sourceType: "duckdb",
			config:     json.RawMessage(`{"extensions":["gdrive","pdf"],"tables":[{"name":"invoices","query":"SELECT page, text FROM read_pdf('gdrive://id:1a2b3c')"}]}`),
			raw:        json.RawMessage(`{"oauth":{"connection_id":"c1"}}`),
		},
		{
			name:         "duckdb oauth rejected on a list without gdrive",
			sourceType:   "duckdb",
			config:       json.RawMessage(`{"extensions":["pdf","httpfs"],"tables":[{"name":"t","query":"SELECT 1"}]}`),
			raw:          json.RawMessage(`{"oauth":{"connection_id":"c1"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    `only used by the "gdrive" extension`,
		},
		{
			name:         "duckdb gdrive oauth needs a reference or token",
			sourceType:   "duckdb",
			config:       json.RawMessage(`{"extension":"gdrive","tables":[{"name":"invoices","query":"SELECT 1"}]}`),
			raw:          json.RawMessage(`{"oauth":{}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "oauth requires grant_id",
		},
		{
			name:         "sql_database creds oracle rejected",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"connection_string":"oracle+oracledb://user:pass@host:1521/orcl"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "only PostgreSQL is supported",
		},
		{
			name:         "sql_database creds uninstalled driver rejected",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"connection_string":"postgresql+psycopg2://user:pass@host/db"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    `driver "psycopg2" is not installed`,
		},
		{
			name:         "sql_database creds not a URL",
			sourceType:   "sql_database",
			raw:          json.RawMessage(`{"connection_string":"host=localhost dbname=prod"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "must be a SQLAlchemy URL",
		},
		{
			name:         "sql_database creds empty",
			sourceType:   "sql_database",
			raw:          nil,
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "sourceCredentials is required",
		},

		// --- filesystem ---
		{
			name:       "filesystem creds valid",
			sourceType: "filesystem",
			raw:        json.RawMessage(`{"access_key_id":"AK","secret_access_key":"SK"}`),
		},
		{
			name:         "filesystem creds missing access_key_id",
			sourceType:   "filesystem",
			raw:          json.RawMessage(`{"secret_access_key":"SK"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "access_key_id is required",
		},
		{
			name:         "filesystem creds missing secret_access_key",
			sourceType:   "filesystem",
			raw:          json.RawMessage(`{"access_key_id":"AK"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "secret_access_key is required",
		},
		{
			name:         "filesystem creds empty",
			sourceType:   "filesystem",
			raw:          json.RawMessage(`{}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "sourceCredentials is required",
		},

		// --- google_sheets ---
		{
			name:       "google_sheets creds valid object key",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"service_account_key":{"type":"service_account","client_email":"pipe@proj.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n"}}`),
		},
		{
			name:       "google_sheets creds valid string-encoded key",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"service_account_key":"{\"client_email\":\"pipe@proj.iam.gserviceaccount.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nMII\\n-----END PRIVATE KEY-----\\n\"}"}`),
		},
		{
			name:         "google_sheets creds missing key",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"other":"val"}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "service_account_key or oauth is required",
		},
		{
			name:       "google_sheets creds valid oauth grant_id",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"oauth":{"grant_id":"11111111-1111-1111-1111-111111111111"}}`),
		},
		{
			name:       "google_sheets creds valid oauth stored refresh_token",
			sourceType: "google_sheets",
			raw:        json.RawMessage(`{"oauth":{"refresh_token":"1//refresh","email":"user@gmail.com"}}`),
		},
		{
			name:         "google_sheets creds oauth empty",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"oauth":{}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "oauth requires grant_id",
		},
		{
			name:         "google_sheets creds both methods",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"service_account_key":{"client_email":"pipe@proj.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nMII\n-----END PRIVATE KEY-----\n"},"oauth":{"grant_id":"11111111-1111-1111-1111-111111111111"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "not both",
		},
		{
			name:         "google_sheets creds key missing client_email",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"service_account_key":{"private_key":"-----BEGIN PRIVATE KEY-----"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "missing client_email",
		},
		{
			name:         "google_sheets creds key missing private_key",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"service_account_key":{"client_email":"pipe@proj.iam.gserviceaccount.com"}}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "missing private_key",
		},
		{
			name:         "google_sheets creds key not an object",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{"service_account_key":[1,2]}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "service-account key JSON object",
		},
		{
			name:         "google_sheets creds empty",
			sourceType:   "google_sheets",
			raw:          json.RawMessage(`{}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "sourceCredentials is required",
		},

		// --- unknown ---
		{
			name:         "unknown source type creds",
			sourceType:   "kafka",
			raw:          json.RawMessage(`{}`),
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "unknown source type",
		},
		// --- filesystem over a public origin: no credential to check ---
		{
			name:       "filesystem public origin needs no creds",
			sourceType: "filesystem",
			config:     json.RawMessage(`{"bucket_url":"https://demo-data.example.com"}`),
			raw:        nil,
		},
		{
			name:       "filesystem public origin ignores partial creds",
			sourceType: "filesystem",
			config:     json.RawMessage(`{"bucket_url":"https://demo-data.example.com"}`),
			raw:        json.RawMessage(`{"access_key_id":"x"}`),
		},
		{
			name:         "filesystem s3 still requires creds",
			sourceType:   "filesystem",
			config:       json.RawMessage(`{"bucket_url":"s3://bucket/prefix/"}`),
			raw:          nil,
			wantErr:      true,
			wantCredsErr: true,
			errSubstr:    "sourceCredentials is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSourceCredentials(tt.sourceType, tt.config, tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSourceCredentials() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if tt.wantCredsErr {
					if _, ok := errors.AsType[*ErrInvalidSourceCredentials](err); !ok {
						t.Errorf("expected *ErrInvalidSourceCredentials, got %T", err)
					}
				}
				if tt.errSubstr != "" && !contains(err.Error(), tt.errSubstr) {
					t.Errorf("expected error to contain %q, got %q", tt.errSubstr, err.Error())
				}
			}
		})
	}
}

func TestIsEmptyJSON(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want bool
	}{
		{"nil", nil, true},
		{"empty", json.RawMessage(``), true},
		{"empty object", json.RawMessage(`{}`), true},
		{"null", json.RawMessage(`null`), true},
		{"non-empty", json.RawMessage(`{"key":"val"}`), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isEmptyJSON(tt.raw); got != tt.want {
				t.Errorf("isEmptyJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstr(s, substr)
}

func searchSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
