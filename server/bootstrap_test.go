package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// TestBootstrapLeaksNoSecret is the guard behind serving this document
// unauthenticated.
//
// It fills EVERY string in a Workspace — including the nested S3 config — with
// a sentinel naming the field it came from, renders the document, and asserts
// that only sentinels from the public allowlist survive. So a field added to
// Workspace later and wired into the bootstrap fails here by default, whatever
// it is called: the test does not try to guess which names are secret, it
// requires each exposed field to be listed on purpose.
func TestBootstrapLeaksNoSecret(t *testing.T) {
	// The fields a caller with no token is allowed to learn. Every one is
	// public by construction — see the WorkspaceBootstrap doc comment.
	public := map[string]bool{
		"Slug":           true,
		"CustomerDomain": true,
		"CasdoorIssuer":  true,
		"CasdoorOrg":     true,
	}

	var ws workspace.Workspace
	sentinels := fillStrings(reflect.ValueOf(&ws).Elem(), "")

	doc := BootstrapFromWorkspace(&ws, "console-client-id", true, true)
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal bootstrap: %v", err)
	}
	body := string(encoded)

	for path, sentinel := range sentinels {
		leaked := strings.Contains(body, sentinel)
		switch {
		case public[path] && !leaked:
			t.Errorf("public field %s is missing from the bootstrap document", path)
		case !public[path] && leaked:
			t.Errorf("field %s leaks into the unauthenticated bootstrap document: %s", path, body)
		}
	}
}

// fillStrings sets every string field reachable from v to a unique sentinel
// and returns them keyed by field path. Nested structs are walked so the S3
// credentials are covered too.
func fillStrings(v reflect.Value, prefix string) map[string]string {
	out := make(map[string]string)
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.String:
			sentinel := fmt.Sprintf("SENTINEL-%s-VALUE", path)
			fv.SetString(sentinel)
			out[path] = sentinel
		case reflect.Struct:
			for k, s := range fillStrings(fv, path) {
				out[k] = s
			}
		}
	}
	return out
}

func TestBootstrapCapabilities(t *testing.T) {
	ws := &workspace.Workspace{
		Slug:          "acme",
		CasdoorIssuer: "https://auth.customer-acme.fairtier.com",
		RillEnabled:   true,
		// CubeEnabled stays false, DuckFlightURL empty.
	}

	doc := BootstrapFromWorkspace(ws, "console", false, false)

	if doc.Capabilities.ControlPlane {
		t.Error("a box must never advertise a control plane")
	}
	if !doc.Capabilities.Rill {
		t.Error("rill capability should follow RillEnabled")
	}
	if doc.Capabilities.Cube || doc.Capabilities.DuckFlight ||
		doc.Capabilities.FileDrop || doc.Capabilities.GoogleOAuth {
		t.Errorf("unconfigured surfaces must not be advertised: %+v", doc.Capabilities)
	}
}

// The Console reads this document before it can authenticate, so the handler
// must answer without a token and in the shape the Console parses.
func TestBootstrapHandlerServesJSONUnauthenticated(t *testing.T) {
	doc := BootstrapFromWorkspace(&workspace.Workspace{
		Slug:           "acme",
		CustomerDomain: "customer-acme.fairtier.com",
		CasdoorIssuer:  "https://auth.customer-acme.fairtier.com",
		CasdoorOrg:     "acme",
	}, "console", true, false)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/fairtier-workspace", nil)
	WorkspaceBootstrapHandler(nil, doc).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	for _, key := range []string{"slug", "customer_domain", "casdoor_issuer", "casdoor_org", "console_client_id", "capabilities"} {
		if _, ok := got[key]; !ok {
			t.Errorf("bootstrap document is missing %q: %s", key, rec.Body.String())
		}
	}
}
