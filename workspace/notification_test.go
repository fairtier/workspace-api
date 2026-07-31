package workspace_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// mockNotificationRepo is an in-memory NotificationRepository.
type mockNotificationRepo struct {
	mu    sync.Mutex
	items []workspace.Notification
	seq   int
}

func (m *mockNotificationRepo) CreateNotification(_ context.Context, n *workspace.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	n.ID = string(rune('a' + m.seq))
	n.CreatedAt = time.Unix(int64(m.seq), 0)
	m.items = append(m.items, *n)
	return nil
}

func (m *mockNotificationRepo) ListNotifications(_ context.Context, slug string, limit int) ([]workspace.Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []workspace.Notification
	for i := len(m.items) - 1; i >= 0 && len(out) < limit; i-- {
		if m.items[i].CustomerSlug == slug {
			out = append(out, m.items[i])
		}
	}
	return out, nil
}

func (m *mockNotificationRepo) UnreadCount(_ context.Context, slug string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := 0
	for _, n := range m.items {
		if n.CustomerSlug == slug && !n.Read {
			c++
		}
	}
	return c, nil
}

func (m *mockNotificationRepo) MarkRead(_ context.Context, slug, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.items {
		if m.items[i].ID == id && m.items[i].CustomerSlug == slug {
			m.items[i].Read = true
			return nil
		}
	}
	return workspace.ErrNotificationNotFound
}

func (m *mockNotificationRepo) MarkAllRead(_ context.Context, slug string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.items {
		if m.items[i].CustomerSlug == slug {
			m.items[i].Read = true
		}
	}
	return nil
}

func TestNotificationService(t *testing.T) {
	newSvc := func() (*workspace.NotificationService, *mockNotificationRepo) {
		repo := &mockNotificationRepo{}
		return &workspace.NotificationService{
			Workspaces:    acmeReader(),
			Notifications: repo,
			Broker:        workspace.NewNotificationBroker(),
		}, repo
	}

	t.Run("notify, list, unread, mark", func(t *testing.T) {
		svc, _ := newSvc()
		ctx := context.Background()

		if err := svc.Notify(ctx, workspace.Notification{CustomerSlug: "acme", Title: "one"}); err != nil {
			t.Fatal(err)
		}
		if err := svc.Notify(ctx, workspace.Notification{CustomerSlug: "acme", Title: "two"}); err != nil {
			t.Fatal(err)
		}

		list, err := svc.List(ctx, "u1")
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 2 {
			t.Fatalf("want 2 notifications, got %d", len(list))
		}
		// Newest first.
		if list[0].Title != "two" {
			t.Fatalf("want newest first, got %q", list[0].Title)
		}
		// Notify defaults a missing type.
		if list[0].Type != "info" {
			t.Fatalf("want default type info, got %q", list[0].Type)
		}

		n, err := svc.UnreadCount(ctx, "u1")
		if err != nil || n != 2 {
			t.Fatalf("want 2 unread, got %d (err %v)", n, err)
		}

		if err := svc.MarkRead(ctx, "u1", list[0].ID); err != nil {
			t.Fatal(err)
		}
		if n, _ := svc.UnreadCount(ctx, "u1"); n != 1 {
			t.Fatalf("want 1 unread after mark, got %d", n)
		}

		if err := svc.MarkAllRead(ctx, "u1"); err != nil {
			t.Fatal(err)
		}
		if n, _ := svc.UnreadCount(ctx, "u1"); n != 0 {
			t.Fatalf("want 0 unread after mark-all, got %d", n)
		}
	})

	t.Run("mark-read unknown id is not found", func(t *testing.T) {
		svc, _ := newSvc()
		err := svc.MarkRead(context.Background(), "u1", "nope")
		if !errors.Is(err, workspace.ErrNotificationNotFound) {
			t.Fatalf("want ErrNotificationNotFound, got %v", err)
		}
	})

	t.Run("subscribe receives live notifications", func(t *testing.T) {
		svc, _ := newSvc()
		ctx := context.Background()
		ch, cancel, err := svc.Subscribe(ctx, "u1")
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := svc.Notify(ctx, workspace.Notification{CustomerSlug: "acme", Title: "live"}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-ch:
			if got.Title != "live" {
				t.Fatalf("want live notification, got %q", got.Title)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for live notification")
		}
	})
}

// recordingPublisher captures cross-replica publishes and can be told to fail.
type recordingPublisher struct {
	mu   sync.Mutex
	got  []workspace.Notification
	fail error
}

func (p *recordingPublisher) PublishNotification(_ context.Context, n workspace.Notification) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail != nil {
		return p.fail
	}
	p.got = append(p.got, n)
	return nil
}

func TestNotify_Publisher(t *testing.T) {
	ctx := context.Background()

	t.Run("hands off to publisher and skips local broker", func(t *testing.T) {
		pub := &recordingPublisher{}
		broker := workspace.NewNotificationBroker()
		svc := &workspace.NotificationService{
			Workspaces:    acmeReader(),
			Notifications: &mockNotificationRepo{},
			Broker:        broker,
			Publisher:     pub,
		}
		ch, cancel := broker.Subscribe("acme")
		defer cancel()

		if err := svc.Notify(ctx, workspace.Notification{CustomerSlug: "acme", Title: "hi"}); err != nil {
			t.Fatal(err)
		}
		// Publisher got it, with the persisted ID populated.
		if len(pub.got) != 1 || pub.got[0].Title != "hi" || pub.got[0].ID == "" {
			t.Fatalf("publisher got %+v", pub.got)
		}
		// Local broker was NOT fed directly: delivery loops back via the
		// listener (not exercised here), so no double push.
		select {
		case n := <-ch:
			t.Fatalf("unexpected local broadcast %+v; publisher owns delivery", n)
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("falls back to local broker when publisher fails", func(t *testing.T) {
		pub := &recordingPublisher{fail: errors.New("notify down")}
		broker := workspace.NewNotificationBroker()
		svc := &workspace.NotificationService{
			Workspaces:    acmeReader(),
			Notifications: &mockNotificationRepo{},
			Broker:        broker,
			Publisher:     pub,
		}
		ch, cancel := broker.Subscribe("acme")
		defer cancel()

		err := svc.Notify(ctx, workspace.Notification{CustomerSlug: "acme", Title: "fallback"})
		if err == nil {
			t.Fatal("want publish error surfaced to caller")
		}
		select {
		case n := <-ch:
			if n.Title != "fallback" {
				t.Fatalf("want fallback broadcast, got %+v", n)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for fallback local broadcast")
		}
	})
}

func TestNotificationBroker_CancelStopsDelivery(t *testing.T) {
	b := workspace.NewNotificationBroker()
	ch, cancel := b.Subscribe("acme")
	cancel()
	// Channel is closed after cancel; a receive returns the zero value, !ok.
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after cancel")
	}
}
