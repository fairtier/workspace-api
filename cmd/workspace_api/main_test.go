package main

import (
	"slices"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

func TestLoadStaticWorkspaceValidation(t *testing.T) {
	const (
		validOrg    = "customer-acme"
		validIssuer = "https://auth.customer-acme.example.com"
	)
	tests := []struct {
		name    string
		slug    string
		domain  string
		org     string
		issuer  string
		wantErr bool
	}{
		{name: "valid", slug: "acme", domain: "customer-acme.example.com", org: validOrg, issuer: validIssuer},
		{name: "wildcard domain accepted after strip", slug: "acme", domain: "*.customer-acme.example.com", org: validOrg, issuer: validIssuer},
		{name: "missing slug", slug: "", domain: "customer-acme.example.com", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "uppercase slug", slug: "Acme", domain: "customer-acme.example.com", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "underscore slug", slug: "ac_me", domain: "customer-acme.example.com", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "missing domain", slug: "acme", domain: "", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "domain with scheme", slug: "acme", domain: "https://customer-acme.example.com", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "domain with path", slug: "acme", domain: "customer-acme.example.com/api", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "single-label domain", slug: "acme", domain: "localhost", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "uppercase domain", slug: "acme", domain: "customer-ACME.example.com", org: validOrg, issuer: validIssuer, wantErr: true},
		{name: "missing casdoor org", slug: "acme", domain: "customer-acme.example.com", org: "", issuer: validIssuer, wantErr: true},
		{name: "missing casdoor issuer", slug: "acme", domain: "customer-acme.example.com", org: validOrg, issuer: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WORKSPACE_SLUG", tt.slug)
			t.Setenv("WORKSPACE_CUSTOMER_DOMAIN", tt.domain)
			t.Setenv("WORKSPACE_CASDOOR_ORG", tt.org)
			t.Setenv("WORKSPACE_CASDOOR_ISSUER", tt.issuer)

			ws, err := loadStaticWorkspace()
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadStaticWorkspace() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			// The wildcard prefix must be gone from every derived endpoint.
			if ws.CustomerDomain != "customer-acme.example.com" {
				t.Errorf("CustomerDomain = %q, want customer-acme.example.com", ws.CustomerDomain)
			}
			// Casdoor org and issuer are taken verbatim from env, never derived.
			if ws.CasdoorOrg != validOrg {
				t.Errorf("CasdoorOrg = %q, want %q", ws.CasdoorOrg, validOrg)
			}
			if ws.CasdoorIssuer != validIssuer {
				t.Errorf("CasdoorIssuer = %q, want %q", ws.CasdoorIssuer, validIssuer)
			}
		})
	}
}

func TestCORSAllowedOrigins(t *testing.T) {
	tests := []struct {
		name  string
		env   string
		want  []string
		first string
	}{
		{name: "unset", env: "", want: nil, first: ""},
		{name: "single", env: "https://console.example.com", want: []string{"https://console.example.com"}, first: "https://console.example.com"},
		{
			name:  "spaces and empties trimmed",
			env:   " https://a.example.com , https://b.example.com ,,",
			want:  []string{"https://a.example.com", "https://b.example.com"},
			first: "https://a.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CORS_ALLOWED_ORIGINS", tt.env)
			if got := corsAllowedOrigins(); !slices.Equal(got, tt.want) {
				t.Errorf("corsAllowedOrigins() = %q, want %q", got, tt.want)
			}
			if got := firstCORSOrigin(); got != tt.first {
				t.Errorf("firstCORSOrigin() = %q, want %q", got, tt.first)
			}
		})
	}
}

func TestLoadFileDropMaxBytes(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want int64
	}{
		{name: "unset uses default", env: "", want: workspace.DefaultUploadMaxBytes},
		{name: "valid override", env: "1048576", want: 1048576},
		{name: "non-numeric falls back", env: "lots", want: workspace.DefaultUploadMaxBytes},
		{name: "negative falls back", env: "-1", want: workspace.DefaultUploadMaxBytes},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("FILEDROP_MAX_BYTES", tt.env)
			if got := loadFileDropMaxBytes(); got != tt.want {
				t.Errorf("loadFileDropMaxBytes() = %d, want %d", got, tt.want)
			}
		})
	}
}
