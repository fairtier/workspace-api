package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	notificationv1 "github.com/fairtier/workspace-api/proto/notification/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// TestPumpNotificationsHeartbeat is the regression guard for the
// StreamNotifications 504: the stream MUST flush a heartbeat frame on connect
// and on a fixed interval while idle, or an idle stream sends no bytes and gets
// cut by Envoy/Cloudflare (surfacing as a 504 + spurious CORS error). It also
// checks that real notifications pass through and are not marked heartbeat.
func TestPumpNotificationsHeartbeat(t *testing.T) {
	ch := make(chan workspace.Notification, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var sent []*notificationv1.Notification
	snapshot := func() []*notificationv1.Notification {
		mu.Lock()
		defer mu.Unlock()
		return append([]*notificationv1.Notification(nil), sent...)
	}

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- pumpNotifications(ctx, ch, 20*time.Millisecond, func(n *notificationv1.Notification) error {
			mu.Lock()
			sent = append(sent, n)
			mu.Unlock()
			return nil
		})
	}()

	// An initial heartbeat must arrive promptly, before any notification.
	waitFor(t, func() bool { return len(snapshot()) >= 1 })
	if !snapshot()[0].Heartbeat {
		t.Fatalf("first frame must be a heartbeat, got %+v", snapshot()[0])
	}

	// At least one more heartbeat must arrive on the interval while idle.
	waitFor(t, func() bool { return len(snapshot()) >= 2 })

	// A real notification passes through, not flagged as a heartbeat.
	ch <- workspace.Notification{ID: "n-1", Type: "info", Title: "hi", CreatedAt: time.Now()}
	waitFor(t, func() bool {
		for _, n := range snapshot() {
			if n.Id == "n-1" {
				return true
			}
		}
		return false
	})
	for _, n := range snapshot() {
		if n.Id == "n-1" && n.Heartbeat {
			t.Fatal("real notification must not be flagged heartbeat")
		}
	}

	// Cancelling the context ends the loop with nil.
	cancel()
	select {
	case err := <-sendErr:
		if err != nil {
			t.Fatalf("pumpNotifications returned %v, want nil on cancel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pumpNotifications did not return after context cancel")
	}
}

// TestPumpNotificationsSendError propagates a send failure (client gone).
func TestPumpNotificationsSendError(t *testing.T) {
	ch := make(chan workspace.Notification)
	boom := errors.New("client gone")
	err := pumpNotifications(context.Background(), ch, time.Hour, func(*notificationv1.Notification) error {
		return boom // fails on the very first (heartbeat) send
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
