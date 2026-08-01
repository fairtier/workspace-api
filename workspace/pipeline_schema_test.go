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
					var cfgErr *ErrInvalidSourceConfig
					if !errors.As(err, &cfgErr) {
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
					var credsErr *ErrInvalidSourceCredentials
					if !errors.As(err, &credsErr) {
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
