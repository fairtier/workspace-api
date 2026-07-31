package workspace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// ErrNotificationNotFound is returned when a notification does not exist or is
// not owned by the caller's tenant.
var ErrNotificationNotFound = errors.New("notification not found")

// Notification is a single in-app notification (the top-bar bell). It is
// tenant-scoped: CustomerSlug identifies the workspace the event belongs to.
type Notification struct {
	ID           string
	CustomerSlug string
	Type         string // "pipeline_run" | "provisioning" | "snapshot" | "info"
	Title        string
	Body         string
	Link         string // optional Console route name to deep-link to
	Read         bool
	CreatedAt    time.Time
}

// NotificationRepository persists notifications.
type NotificationRepository interface {
	// CreateNotification inserts n, populating n.ID and n.CreatedAt.
	CreateNotification(ctx context.Context, n *Notification) error
	// ListNotifications returns the tenant's notifications, newest first, capped at limit.
	ListNotifications(ctx context.Context, customerSlug string, limit int) ([]Notification, error)
	// UnreadCount returns the number of unread notifications for the tenant.
	UnreadCount(ctx context.Context, customerSlug string) (int, error)
	// MarkRead marks one notification read, scoped to the tenant; returns
	// ErrNotificationNotFound if it doesn't exist for that tenant.
	MarkRead(ctx context.Context, customerSlug, id string) error
	// MarkAllRead marks all of the tenant's notifications read.
	MarkAllRead(ctx context.Context, customerSlug string) error
}

// Notifier is the producer-side interface used by other services (e.g. pipeline
// run reporting) to raise a notification without depending on the full service.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// NotificationPublisher fans a persisted notification out to every replica for
// live delivery. The in-process broker only reaches subscribers connected to
// the same replica; the publisher (Postgres LISTEN/NOTIFY) bridges the gap so a
// run reported on one replica is streamed to a browser connected to another.
// Each replica — including the one that raised the notification — receives the
// broadcast back through its own listener, so delivery stays exactly-once per
// replica.
type NotificationPublisher interface {
	PublishNotification(ctx context.Context, n Notification) error
}

// NotificationBroker is an in-process pub/sub for live notification delivery to
// streaming subscribers. It is per-replica: a notification raised on one replica
// is streamed to subscribers connected to that replica. The persisted list (via
// the repository) is the cross-replica source of truth; the broker only powers
// the live push on top of it.
type NotificationBroker struct {
	mu   sync.Mutex
	subs map[string]map[*subscriber]struct{} // keyed by customer_slug
}

type subscriber struct {
	ch chan Notification
}

// NewNotificationBroker returns an empty broker.
func NewNotificationBroker() *NotificationBroker {
	return &NotificationBroker{subs: make(map[string]map[*subscriber]struct{})}
}

// Subscribe registers a live subscriber for a tenant. The returned channel
// receives notifications until the returned cancel func is called. Sends are
// non-blocking: a slow consumer drops messages rather than stalling producers
// (the persisted list backfills anything missed on reconnect).
func (b *NotificationBroker) Subscribe(customerSlug string) (<-chan Notification, func()) {
	s := &subscriber{ch: make(chan Notification, 16)}
	b.mu.Lock()
	if b.subs[customerSlug] == nil {
		b.subs[customerSlug] = make(map[*subscriber]struct{})
	}
	b.subs[customerSlug][s] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		if set := b.subs[customerSlug]; set != nil {
			if _, ok := set[s]; ok {
				delete(set, s)
				close(s.ch)
			}
			if len(set) == 0 {
				delete(b.subs, customerSlug)
			}
		}
		b.mu.Unlock()
	}
	return s.ch, cancel
}

// Broadcast delivers n to every live subscriber of its tenant (non-blocking).
// Called both by the local Notify fallback and by the cross-replica listener
// when a NOTIFY payload arrives from Postgres.
func (b *NotificationBroker) Broadcast(n Notification) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs[n.CustomerSlug] {
		select {
		case s.ch <- n:
		default: // drop for slow consumers; list view backfills
		}
	}
}

// NotificationService orchestrates notification CRUD + live delivery.
type NotificationService struct {
	Workspaces    Resolver
	Notifications NotificationRepository
	Broker        *NotificationBroker
	// Publisher bridges live delivery across replicas (Postgres LISTEN/NOTIFY).
	// When set, Notify hands off to it and the local broker is fed by this
	// replica's listener; when nil (tests, single-replica), Notify falls back
	// to delivering straight into the local broker.
	Publisher NotificationPublisher
}

// defaultListLimit caps the bell's history load.
const defaultListLimit = 50

// List returns the caller's tenant's recent notifications, newest first.
func (s *NotificationService) List(ctx context.Context, callerID core.UserID) ([]Notification, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, fmt.Errorf("get workspace: %w", err)
	}
	return s.Notifications.ListNotifications(ctx, ws.Slug, defaultListLimit)
}

// UnreadCount returns the caller's tenant's unread notification count.
func (s *NotificationService) UnreadCount(ctx context.Context, callerID core.UserID) (int, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return 0, fmt.Errorf("get workspace: %w", err)
	}
	return s.Notifications.UnreadCount(ctx, ws.Slug)
}

// MarkRead marks one of the caller's notifications read.
func (s *NotificationService) MarkRead(ctx context.Context, callerID core.UserID, id string) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get workspace: %w", err)
	}
	return s.Notifications.MarkRead(ctx, ws.Slug, id)
}

// MarkAllRead marks all of the caller's notifications read.
func (s *NotificationService) MarkAllRead(ctx context.Context, callerID core.UserID) error {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return fmt.Errorf("get workspace: %w", err)
	}
	return s.Notifications.MarkAllRead(ctx, ws.Slug)
}

// Subscribe resolves the caller to a tenant and returns a live channel of that
// tenant's notifications plus a cancel func. Used by the streaming RPC.
func (s *NotificationService) Subscribe(ctx context.Context, callerID core.UserID) (<-chan Notification, func(), error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, nil, fmt.Errorf("get workspace: %w", err)
	}
	if s.Broker == nil {
		return nil, nil, fmt.Errorf("broker not configured")
	}
	ch, cancel := s.Broker.Subscribe(ws.Slug)
	return ch, cancel, nil
}

// Notify persists n and pushes it to live subscribers. It implements Notifier
// so producers (e.g. pipeline run reporting) can raise notifications.
//
// Persistence populates n.ID and n.CreatedAt before the live push, so the
// broadcast frame carries the same identity the list view will show. When a
// cross-replica Publisher is configured it owns delivery — the NOTIFY loops
// back through every replica's listener, this one included — and a publish
// failure degrades to a local-only broadcast so at least same-replica
// subscribers still see it. The persisted list backfills anything missed.
func (s *NotificationService) Notify(ctx context.Context, n Notification) error {
	if n.Type == "" {
		n.Type = "info"
	}
	if err := s.Notifications.CreateNotification(ctx, &n); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}
	if s.Publisher != nil {
		if err := s.Publisher.PublishNotification(ctx, n); err != nil {
			if s.Broker != nil {
				s.Broker.Broadcast(n)
			}
			return fmt.Errorf("publish notification: %w", err)
		}
		return nil
	}
	if s.Broker != nil {
		s.Broker.Broadcast(n)
	}
	return nil
}
