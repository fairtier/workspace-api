package lakekeeper

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/fairtier/workspace-api/core"
)

// fakeServer is a test helper that captures HTTP requests and returns canned responses.
type fakeServer struct {
	t      *testing.T
	mu     sync.Mutex
	calls  []capturedRequest
	server *httptest.Server
	mux    *http.ServeMux
}

type capturedRequest struct {
	Method string
	Path   string
	Query  url.Values
	Body   json.RawMessage
	Header http.Header
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	fs := &fakeServer{
		t:   t,
		mux: http.NewServeMux(),
	}
	fs.server = httptest.NewServer(fs.mux)
	t.Cleanup(fs.server.Close)
	return fs
}

func (fs *fakeServer) URL() string { return fs.server.URL }

// handle registers a handler for the given method+path and records the request.
func (fs *fakeServer) handle(pattern string, status int, response any) {
	fs.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fs.mu.Lock()
		fs.calls = append(fs.calls, capturedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.Query(),
			Body:   body,
			Header: r.Header.Clone(),
		})
		fs.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if response != nil {
			if err := json.NewEncoder(w).Encode(response); err != nil {
				fs.t.Fatalf("encode response: %v", err)
			}
		}
	})
}

// lastCall returns the most recent captured request.
func (fs *fakeServer) lastCall() capturedRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.calls) == 0 {
		fs.t.Fatal("no requests captured")
	}
	return fs.calls[len(fs.calls)-1]
}

// bodyMap parses the request body as a JSON object.
func (cr capturedRequest) bodyMap() map[string]json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(cr.Body, &m); err != nil {
		panic("unmarshal body: " + err.Error())
	}
	return m
}

// --- Storage profile tests (the SDK workaround) ---

func TestCreateWarehouse_StorageProfile_VendedMode(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/warehouse", http.StatusCreated, nil)

	c := &Client{}
	s3 := core.S3Config{
		Bucket:                   "ft-test",
		Region:                   "auto",
		Endpoint:                 "https://acct.r2.cloudflarestorage.com",
		AccessKeyID:              "ak",
		SecretAccessKey:          "sk",
		CredentialDelegationMode: "vended",
		CloudflareAPIToken:       "cf-token",
		CloudflareAccountID:      "cf-acct",
	}

	name, err := c.CreateWarehouse(context.Background(), fs.URL(), "test-token", "default", s3)
	if err != nil {
		t.Fatal(err)
	}
	if name != "default" {
		t.Fatalf("expected name 'default', got %q", name)
	}

	call := fs.lastCall()
	body := call.bodyMap()

	var profile map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-profile"], &profile); err != nil {
		t.Fatal(err)
	}

	assertJSONField(t, profile, "sts-enabled", "true")
	assertJSONField(t, profile, "remote-signing-enabled", "false")
	assertJSONField(t, profile, "flavor", `"s3-compat"`)
	assertJSONField(t, profile, "bucket", `"ft-test"`)

	// Verify cloudflare-r2 credential type
	var cred map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-credential"], &cred); err != nil {
		t.Fatal(err)
	}
	assertJSONField(t, cred, "credential-type", `"cloudflare-r2"`)
	assertJSONField(t, cred, "account-id", `"cf-acct"`)
	assertJSONField(t, cred, "token", `"cf-token"`)
}

func TestCreateWarehouse_StorageProfile_RemoteSigningMode(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/warehouse", http.StatusCreated, nil)

	c := &Client{}
	s3 := core.S3Config{
		Bucket:                   "my-bucket",
		Region:                   "eu-central-1",
		Endpoint:                 "https://s3.example.com",
		AccessKeyID:              "ak",
		SecretAccessKey:          "sk",
		CredentialDelegationMode: "remote-signing",
		PathStyleAccess:          new(true),
	}

	_, err := c.CreateWarehouse(context.Background(), fs.URL(), "test-token", "wh", s3)
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var profile map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-profile"], &profile); err != nil {
		t.Fatal(err)
	}

	assertJSONField(t, profile, "sts-enabled", "false")
	assertJSONField(t, profile, "remote-signing-enabled", "true")
	assertJSONField(t, profile, "path-style-access", "true")

	// Standard access-key credential
	var cred map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-credential"], &cred); err != nil {
		t.Fatal(err)
	}
	assertJSONField(t, cred, "credential-type", `"access-key"`)
}

func TestCreateWarehouse_StorageProfile_NoneMode(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/warehouse", http.StatusCreated, nil)

	c := &Client{}
	s3 := core.S3Config{
		Bucket:                   "byos-bucket",
		Region:                   "us-east-1",
		AccessKeyID:              "ak",
		SecretAccessKey:          "sk",
		CredentialDelegationMode: "none",
	}

	_, err := c.CreateWarehouse(context.Background(), fs.URL(), "test-token", "wh", s3)
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var profile map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-profile"], &profile); err != nil {
		t.Fatal(err)
	}

	assertJSONField(t, profile, "sts-enabled", "false")
	assertJSONField(t, profile, "remote-signing-enabled", "false")
}

func TestCreateWarehouse_KeyPrefix(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/warehouse", http.StatusCreated, nil)

	c := &Client{}
	s3 := core.S3Config{
		Bucket:                   "shared",
		Region:                   "auto",
		KeyPrefix:                "tenant-42/",
		AccessKeyID:              "ak",
		SecretAccessKey:          "sk",
		CredentialDelegationMode: "none",
	}

	_, err := c.CreateWarehouse(context.Background(), fs.URL(), "test-token", "wh", s3)
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var profile map[string]json.RawMessage
	if err := json.Unmarshal(body["storage-profile"], &profile); err != nil {
		t.Fatal(err)
	}
	assertJSONField(t, profile, "key-prefix", `"tenant-42/"`)
}

// --- Bootstrap ---

func TestBootstrap_Success(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/bootstrap", http.StatusNoContent, nil)

	c := &Client{}
	err := c.Bootstrap(context.Background(), fs.URL(), "test-token")
	if err != nil {
		t.Fatal(err)
	}

	call := fs.lastCall()
	if call.Header.Get("Authorization") != "Bearer test-token" {
		t.Fatalf("expected Bearer token, got %q", call.Header.Get("Authorization"))
	}
}

func TestBootstrap_AlreadyBootstrapped(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/bootstrap", http.StatusConflict, map[string]any{
		"error": map[string]any{
			"message": "Catalog already bootstrapped",
			"type":    "CatalogAlreadyBootstrapped",
			"code":    409,
		},
	})

	c := &Client{}
	err := c.Bootstrap(context.Background(), fs.URL(), "test-token")
	if err != nil {
		t.Fatalf("expected nil error for already-bootstrapped, got: %v", err)
	}
}

// --- User lifecycle ---

func TestCreateUser_Success(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/user", http.StatusCreated, nil)

	c := &Client{}
	err := c.CreateUser(context.Background(), fs.URL(), "test-token", "oidc~admin/dlt-worker-test", "dlt-worker")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	assertJSONField(t, body, "id", `"oidc~admin/dlt-worker-test"`)
	assertJSONField(t, body, "name", `"dlt-worker"`)
	assertJSONField(t, body, "user-type", `"application"`)
}

func TestCreateUser_AlreadyExists(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/user", http.StatusConflict, map[string]any{
		"error": map[string]any{
			"message": "User already exists",
			"type":    "UserAlreadyExists",
			"code":    409,
		},
	})

	c := &Client{}
	err := c.CreateUser(context.Background(), fs.URL(), "test-token", "user-1", "name")
	if err != nil {
		t.Fatalf("expected nil error for 409, got: %v", err)
	}
}

func TestDeleteUser_Success(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("DELETE /management/v1/user/oidc~admin/dlt-worker", http.StatusNoContent, nil)

	c := &Client{}
	err := c.DeleteUser(context.Background(), fs.URL(), "test-token", "oidc~admin/dlt-worker")
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteUser_AlreadyGone(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("DELETE /management/v1/user/{id...}", http.StatusNotFound, map[string]any{
		"error": map[string]any{
			"message": "User not found",
			"type":    "UserNotFound",
			"code":    404,
		},
	})

	c := &Client{}
	err := c.DeleteUser(context.Background(), fs.URL(), "test-token", "nonexistent")
	if err != nil {
		t.Fatalf("expected nil error for 404, got: %v", err)
	}
}

// --- List operations ---

// warehouseResponse builds a valid GetWarehouseResponse JSON object. The
// generated SDK decodes responses strictly (all required fields must be
// present, unknown fields rejected), so fixtures must be complete.
func warehouseResponse(id, name string) map[string]any {
	return map[string]any{
		"id":             id,
		"warehouse-id":   id,
		"name":           name,
		"project-id":     defaultProjectID,
		"protected":      false,
		"status":         "active",
		"delete-profile": map[string]any{"type": "hard"},
		"storage-profile": map[string]any{
			"type": "s3", "bucket": "b", "region": "r", "sts-enabled": false,
		},
	}
}

func TestListWarehouses(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/warehouse", http.StatusOK, map[string]any{
		"warehouses": []map[string]any{
			warehouseResponse("id-1", "default"),
			warehouseResponse("id-2", "staging"),
		},
	})

	c := &Client{}
	whs, err := c.ListWarehouses(context.Background(), fs.URL(), "test-token")
	if err != nil {
		t.Fatal(err)
	}

	if len(whs) != 2 {
		t.Fatalf("expected 2 warehouses, got %d", len(whs))
	}
	if whs[0].ID != "id-1" || whs[0].Name != "default" {
		t.Fatalf("unexpected first warehouse: %+v", whs[0])
	}
}

func TestGetWarehouseID_Found(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/warehouse", http.StatusOK, map[string]any{
		"warehouses": []map[string]any{
			warehouseResponse("id-1", "default"),
		},
	})

	c := &Client{}
	id, err := c.GetWarehouseID(context.Background(), fs.URL(), "test-token", "default")
	if err != nil {
		t.Fatal(err)
	}
	if id != "id-1" {
		t.Fatalf("expected id-1, got %q", id)
	}
}

func TestGetWarehouseID_NotFound(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/warehouse", http.StatusOK, map[string]any{
		"warehouses": []map[string]any{},
	})

	c := &Client{}
	_, err := c.GetWarehouseID(context.Background(), fs.URL(), "test-token", "missing")
	if err == nil {
		t.Fatal("expected error for missing warehouse")
	}
}

// userResponse builds a valid User JSON object for the strict SDK decoder.
func userResponse(id, name string) map[string]any {
	return map[string]any{
		"id":                id,
		"name":              name,
		"user-type":         "application",
		"created-at":        "2026-01-01T00:00:00Z",
		"last-updated-with": "create-endpoint",
	}
}

func TestListUsers(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/user", http.StatusOK, map[string]any{
		"users": []map[string]any{
			userResponse("oidc~admin/dlt-worker-test", "dlt-worker"),
			userResponse("oidc~admin/api-user", "api-user"),
		},
	})

	c := &Client{}
	users, err := c.ListUsers(context.Background(), fs.URL(), "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].ID != "oidc~admin/dlt-worker-test" {
		t.Fatalf("unexpected first user: %+v", users[0])
	}
}

// --- Permissions ---

func TestAssignServerRole(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/permissions/server/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignServerRole(context.Background(), fs.URL(), "test-token", "oidc~admin/user-1", "admin")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var writes []json.RawMessage
	if err := json.Unmarshal(body["writes"], &writes); err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}

	var assignment map[string]string
	if err := json.Unmarshal(writes[0], &assignment); err != nil {
		t.Fatal(err)
	}
	if assignment["user"] != "oidc~admin/user-1" || assignment["type"] != "admin" {
		t.Fatalf("unexpected assignment: %v", assignment)
	}
}

// assignmentsResponse builds the GET-assignments JSON Lakekeeper returns.
func assignmentsResponse(userID string, types ...string) map[string]any {
	assignments := make([]map[string]any, 0, len(types))
	for _, t := range types {
		assignments = append(assignments, map[string]any{
			"type": t,
			"user": userID,
		})
	}
	return map[string]any{"assignments": assignments}
}

func TestAssignWarehouseRole_Reader_FromEmpty(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, assignmentsResponse("user-1"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "reader")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var writes []map[string]string
	if err := json.Unmarshal(body["writes"], &writes); err != nil {
		t.Fatal(err)
	}

	types := make(map[string]bool)
	for _, w := range writes {
		types[w["type"]] = true
		if w["user"] != "user-1" {
			t.Fatalf("unexpected user: %q", w["user"])
		}
	}
	if !types["describe"] || !types["select"] {
		t.Fatalf("reader should have describe+select, got: %v", types)
	}
	if types["create"] || types["modify"] || types["ownership"] {
		t.Fatalf("reader should not have create/modify/ownership, got: %v", types)
	}
}

func TestAssignWarehouseRole_Writer_FromEmpty(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, assignmentsResponse("user-1"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "writer")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var writes []map[string]string
	if err := json.Unmarshal(body["writes"], &writes); err != nil {
		t.Fatal(err)
	}

	types := make(map[string]bool)
	for _, w := range writes {
		types[w["type"]] = true
	}
	for _, expected := range []string{"describe", "select", "create", "modify"} {
		if !types[expected] {
			t.Fatalf("writer should have %q, got: %v", expected, types)
		}
	}
}

func TestAssignWarehouseRole_Admin_FromEmpty(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, assignmentsResponse("user-1"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "admin")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var writes []map[string]string
	if err := json.Unmarshal(body["writes"], &writes); err != nil {
		t.Fatal(err)
	}

	if len(writes) != 1 || writes[0]["type"] != "ownership" {
		t.Fatalf("admin should have exactly ownership, got: %v", writes)
	}
}

func TestAssignWarehouseRole_UnknownRole(t *testing.T) {
	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), "http://unused", "token", "wh-1", "user-1", "superuser")
	if err == nil {
		t.Fatal("expected error for unknown role")
	}
}

// Upgrade reader → writer must Write only the missing create+modify tuples
// and must NOT include describe+select (which would cause OpenFGA to roll back
// the whole batch and silently leave the role unchanged).
func TestAssignWarehouseRole_UpgradeReaderToWriter(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK,
		assignmentsResponse("user-1", "describe", "select"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "writer")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var writes []map[string]string
	if err := json.Unmarshal(body["writes"], &writes); err != nil {
		t.Fatal(err)
	}
	types := make(map[string]bool)
	for _, w := range writes {
		types[w["type"]] = true
	}
	if !types["create"] || !types["modify"] {
		t.Fatalf("upgrade should write create+modify, got: %v", types)
	}
	if types["describe"] || types["select"] {
		t.Fatalf("upgrade should NOT re-write existing describe/select, got: %v", types)
	}

	var deletes []map[string]string
	_ = json.Unmarshal(body["deletes"], &deletes)
	if len(deletes) != 0 {
		t.Fatalf("upgrade should not delete anything, got: %v", deletes)
	}
}

// Downgrade writer → reader must Delete the create+modify tuples and not Write
// anything (describe+select are already present).
func TestAssignWarehouseRole_DowngradeWriterToReader(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK,
		assignmentsResponse("user-1", "describe", "select", "create", "modify"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "reader")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var deletes []map[string]string
	if err := json.Unmarshal(body["deletes"], &deletes); err != nil {
		t.Fatal(err)
	}
	types := make(map[string]bool)
	for _, d := range deletes {
		types[d["type"]] = true
	}
	if !types["create"] || !types["modify"] {
		t.Fatalf("downgrade should delete create+modify, got: %v", types)
	}
	if types["describe"] || types["select"] {
		t.Fatalf("downgrade should keep describe+select, got deletes: %v", types)
	}

	var writes []map[string]string
	_ = json.Unmarshal(body["writes"], &writes)
	if len(writes) != 0 {
		t.Fatalf("downgrade should not write anything, got: %v", writes)
	}
}

// No-op: assigning the same role the user already has must skip the POST
// entirely (no Update call), so there's nothing for OpenFGA to roll back on.
func TestAssignWarehouseRole_NoOpWhenUnchanged(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK,
		assignmentsResponse("user-1", "describe", "select", "create", "modify"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusInternalServerError, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "writer")
	if err != nil {
		t.Fatalf("no-op upsert should not call POST, got: %v", err)
	}
}

// Role-management must not touch out-of-band relations like pass_grants_admin
// that the user may hold separately.
func TestAssignWarehouseRole_PreservesUnmanagedRelations(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK,
		assignmentsResponse("user-1", "describe", "select", "pass_grants"))
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.AssignWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1", "reader")
	if err != nil {
		t.Fatal(err)
	}

	// Reader against existing reader+pass_grants: no writes, no deletes.
	calls := 0
	for _, call := range fs.calls {
		if call.Method == "POST" {
			calls++
		}
	}
	if calls != 0 {
		t.Fatalf("no-op upsert should not POST when reader is already in place, got %d POSTs", calls)
	}
}

func TestRemoveWarehouseRole(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("POST /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, nil)

	c := &Client{}
	err := c.RemoveWarehouseRole(context.Background(), fs.URL(), "test-token", "wh-123", "user-1")
	if err != nil {
		t.Fatal(err)
	}

	body := fs.lastCall().bodyMap()
	var deletes []map[string]string
	if err := json.Unmarshal(body["deletes"], &deletes); err != nil {
		t.Fatal(err)
	}

	// All 7 relations should be deleted
	if len(deletes) != 7 {
		t.Fatalf("expected 7 deletes, got %d", len(deletes))
	}
	types := make(map[string]bool)
	for _, d := range deletes {
		types[d["type"]] = true
		if d["user"] != "user-1" {
			t.Fatalf("unexpected user: %q", d["user"])
		}
	}
	for _, rel := range []string{"ownership", "pass_grants", "manage_grants", "describe", "select", "create", "modify"} {
		if !types[rel] {
			t.Fatalf("missing delete for %q", rel)
		}
	}
}

func TestGetWarehouseAssignments(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/permissions/warehouse/{id}/assignments", http.StatusOK, map[string]any{
		"assignments": []map[string]any{
			{"user": "oidc~admin/writer", "type": "describe"},
			{"user": "oidc~admin/writer", "type": "select"},
			{"role": "some-role", "type": "describe"}, // should be filtered out
			{"user": "oidc~admin/owner", "type": "ownership"},
		},
	})

	c := &Client{}
	assignments, err := c.GetWarehouseAssignments(context.Background(), fs.URL(), "test-token", "wh-123")
	if err != nil {
		t.Fatal(err)
	}

	// Role-based assignment should be filtered out
	if len(assignments) != 3 {
		t.Fatalf("expected 3 user assignments, got %d: %+v", len(assignments), assignments)
	}

	// Verify user assignments are present
	found := make(map[string]bool)
	for _, a := range assignments {
		found[a.UserID+"/"+a.Relation] = true
	}
	for _, expected := range []string{
		"oidc~admin/writer/describe",
		"oidc~admin/writer/select",
		"oidc~admin/owner/ownership",
	} {
		if !found[expected] {
			t.Fatalf("missing assignment %q in %v", expected, found)
		}
	}
}

// --- Auth header ---

func TestAuthHeaderSent(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/warehouse", http.StatusOK, map[string]any{
		"warehouses": []map[string]any{},
	})

	c := &Client{}
	_, _ = c.ListWarehouses(context.Background(), fs.URL(), "my-secret-token")

	call := fs.lastCall()
	if got := call.Header.Get("Authorization"); got != "Bearer my-secret-token" {
		t.Fatalf("expected 'Bearer my-secret-token', got %q", got)
	}
}

// --- Project scoping ---

// The SDK scopes ListWarehouses to a project via the `projectId` query
// parameter (default project = nil UUID).
func TestProjectIDQueryParam(t *testing.T) {
	fs := newFakeServer(t)
	fs.handle("GET /management/v1/warehouse", http.StatusOK, map[string]any{
		"warehouses": []map[string]any{},
	})

	c := &Client{}
	_, _ = c.ListWarehouses(context.Background(), fs.URL(), "token")

	call := fs.lastCall()
	if got := call.Query.Get("projectId"); got != defaultProjectID {
		t.Fatalf("expected projectId query param %q, got %q", defaultProjectID, got)
	}
}

// --- Helpers ---

func assertJSONField(t *testing.T, m map[string]json.RawMessage, key, want string) {
	t.Helper()
	raw, ok := m[key]
	if !ok {
		t.Fatalf("missing key %q in %v", key, mapKeys(m))
	}
	got := string(raw)
	if got != want {
		t.Fatalf("%s: got %s, want %s", key, got, want)
	}
}

func mapKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
