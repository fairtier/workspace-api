package casdoor

import (
	"context"
	"fmt"

	"github.com/casdoor/casdoor-go-sdk/casdoorsdk"

	"github.com/fairtier/workspace-api/core"
)

// AppManager implements core.CasdoorAppManager using the Casdoor SDK.
// It creates per-user applications for OAuth2 client_credentials authentication.
type AppManager struct {
	// Endpoint is the Casdoor base URL (e.g. "https://auth.fairtier.com").
	Endpoint string
	// ClientID is the built-in admin app client ID.
	ClientID string
	// ClientSecret is the built-in admin app client secret.
	ClientSecret string
}

func (m *AppManager) client(org string) *casdoorsdk.Client {
	return casdoorsdk.NewClient(m.Endpoint, m.ClientID, m.ClientSecret, "", org, "")
}

// AddApp creates a Casdoor application for the given organization.
// The application is configured for client_credentials only (no password grant).
// Casdoor auto-generates ClientId and ClientSecret.
//
// The "dcr" tag is load-bearing, not cosmetic. Casdoor accepts an
// application's clientId/clientSecret as API authentication on any /api/*
// path, and an untagged application's subject ("app") is allowed everything by
// the built-in policy — so the pair handed to a service-account holder would
// carry management authority over the whole Casdoor. The tag moves the subject
// to "app-dcr", whose policy covers only /api/login/oauth/*,
// /api/get-oauth-token, /api/userinfo and /api/get-application: everything a
// client-credentials service account needs to obtain and spend a token, and
// nothing that administers anything.
func (m *AppManager) AddApp(_ context.Context, org, name string) (*core.CasdoorApp, error) {
	app := &casdoorsdk.Application{
		Owner:                "admin",
		Name:                 name,
		Organization:         org,
		DisplayName:          name,
		Tags:                 []string{"dcr"},
		GrantTypes:           []string{"client_credentials"},
		TokenFormat:          "JWT",
		ExpireInHours:        168,
		RefreshExpireInHours: 336,
	}

	ok, err := m.client(org).AddApplication(app)
	if err != nil {
		return nil, fmt.Errorf("casdoor add application: %w", err)
	}
	if !ok {
		return nil, core.ErrUserAlreadyExists
	}

	// Casdoor generates ClientId and ClientSecret on creation. Fetch to get them.
	created, err := m.client(org).GetApplication(name)
	if err != nil {
		return nil, fmt.Errorf("casdoor get application after creation: %w", err)
	}
	if created == nil {
		return nil, fmt.Errorf("casdoor application %q not found after creation", name)
	}

	return &core.CasdoorApp{
		Name:         created.Name,
		ClientID:     created.ClientId,
		ClientSecret: created.ClientSecret,
	}, nil
}

// DeleteApp deletes a Casdoor application by name within the given organization.
func (m *AppManager) DeleteApp(_ context.Context, org, name string) error {
	ok, err := m.client(org).DeleteApplication(&casdoorsdk.Application{
		Owner:        "admin",
		Name:         name,
		Organization: org,
	})
	if err != nil {
		return fmt.Errorf("casdoor delete application: %w", err)
	}
	if !ok {
		return core.ErrUserNotFound
	}
	return nil
}

// ListApps returns all Casdoor applications for the given organization,
// excluding the management application (lakekeeper-*).
func (m *AppManager) ListApps(_ context.Context, org string) ([]core.CasdoorApp, error) {
	apps, err := m.client(org).GetOrganizationApplications()
	if err != nil {
		return nil, fmt.Errorf("casdoor list applications: %w", err)
	}

	result := make([]core.CasdoorApp, 0, len(apps))
	for _, a := range apps {
		// Skip the management application (created by Terraform).
		if len(a.GrantTypes) != 1 || a.GrantTypes[0] != "client_credentials" {
			continue
		}
		result = append(result, core.CasdoorApp{
			Name:         a.Name,
			ClientID:     a.ClientId,
			ClientSecret: a.ClientSecret,
		})
	}
	return result, nil
}
