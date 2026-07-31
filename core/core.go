// Package core is the shared kernel between the control plane (domain) and
// the workspace plane (workspace): plain types both planes need. It must
// stay tiny — types only, no ports, no services, no I/O, and it may import
// neither plane (enforced by depguard).
package core

import "strings"

// UserID is a strongly-typed identifier for users (Casdoor subject claim).
type UserID string

func (id UserID) String() string { return string(id) }

// serviceAccountPrefix is the Casdoor owner every application lives under.
// Applications are always created under the built-in "admin" owner (that is
// also why a Lakekeeper principal reads "oidc~admin/<app>"), and a
// client-credentials token's subject is that application id — so the prefix
// identifies a machine caller, and only the platform can mint one.
const serviceAccountPrefix = "admin/"

// ServiceAccountApp returns the Casdoor application name behind a
// client-credentials subject ("admin/<app>"), and false for a person.
func (id UserID) ServiceAccountApp() (string, bool) {
	app, ok := strings.CutPrefix(string(id), serviceAccountPrefix)
	if !ok || app == "" {
		return "", false
	}
	return app, true
}

// IsServiceAccount reports whether the subject identifies a Casdoor
// application rather than a person.
//
// End-user surfaces must reject those, because a Casdoor issuer signs more
// than its Console users: every Lakekeeper data-platform service account is a
// Casdoor application, and those client credentials are handed to end users
// for BYO-dbt/BYO-Rill with role "reader". Without this check a read-only
// warehouse credential is also a valid workspace-user token.
func (id UserID) IsServiceAccount() bool {
	_, ok := id.ServiceAccountApp()
	return ok
}
