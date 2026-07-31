package postgres

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/fairtier/workspace-api/workspace"
)

func TestEncodeNotification_RoundTrip(t *testing.T) {
	n := workspace.Notification{
		ID:           "n1",
		CustomerSlug: "acme",
		Type:         "pipeline_run",
		Title:        `Pipeline "sales" failed`,
		Body:         "boom: connection refused",
		Link:         "/pipelines?pipeline=p1",
		Read:         false,
		CreatedAt:    time.Unix(1_700_000_000, 0).UTC(),
	}
	raw, err := encodeNotification(n)
	if err != nil {
		t.Fatal(err)
	}
	var p notificationPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatal(err)
	}
	got := p.toDomain()
	if !got.CreatedAt.Equal(n.CreatedAt) {
		t.Fatalf("created_at: got %v want %v", got.CreatedAt, n.CreatedAt)
	}
	got.CreatedAt, n.CreatedAt = time.Time{}, time.Time{}
	if got != n {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, n)
	}
}

func TestEncodeNotification_TruncatesOversizedBody(t *testing.T) {
	// A body far larger than the payload cap (e.g. a giant stack trace) must
	// still produce a valid, under-limit, valid-UTF-8 frame.
	n := workspace.Notification{
		ID:           "n1",
		CustomerSlug: "acme",
		Type:         "pipeline_run",
		Title:        "Pipeline failed",
		Body:         strings.Repeat("é", 10_000), // multi-byte runes, ~20KB
		Link:         "/pipelines?pipeline=p1",
	}
	raw, err := encodeNotification(n)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > maxNotifyPayload {
		t.Fatalf("payload %d exceeds cap %d", len(raw), maxNotifyPayload)
	}
	var p notificationPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("truncated payload is not valid JSON: %v", err)
	}
	if !utf8.ValidString(p.Body) {
		t.Fatal("truncated body is not valid UTF-8")
	}
	if !strings.HasSuffix(p.Body, "…") {
		t.Fatalf("truncated body should end with ellipsis, got %q", p.Body[len(p.Body)-8:])
	}
	// Fields other than Body survive intact.
	if p.Title != n.Title || p.Link != n.Link || p.ID != n.ID {
		t.Fatalf("non-body fields altered by truncation: %+v", p)
	}
}
