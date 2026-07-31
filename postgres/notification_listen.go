package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/fairtier/workspace-api/workspace"
)

// notificationChannel is the Postgres LISTEN/NOTIFY channel that carries live
// in-app notifications between replicas. Every replica LISTENs on it; Notify
// emits a NOTIFY on it. See workspace.NotificationPublisher for why this bridge
// exists (the in-process broker only reaches same-replica subscribers).
const notificationChannel = "notifications"

// maxNotifyPayload caps the NOTIFY payload. Postgres rejects payloads over 8000
// bytes and that rejection would fail the emitting statement, so we stay well
// under it and truncate Body if needed — the persisted row keeps the full text,
// only the live-push frame is trimmed. Titles/links are always short.
const maxNotifyPayload = 7000

// notificationPayload is the wire shape carried over the NOTIFY channel. It
// mirrors the fields the stream frame needs so a listening replica can rebuild
// the workspace.Notification without re-querying.
type notificationPayload struct {
	ID           string    `json:"id"`
	CustomerSlug string    `json:"customer_slug"`
	Type         string    `json:"type"`
	Title        string    `json:"title"`
	Body         string    `json:"body"`
	Link         string    `json:"link"`
	Read         bool      `json:"read"`
	CreatedAt    time.Time `json:"created_at"`
}

func toPayload(n workspace.Notification) notificationPayload {
	return notificationPayload{
		ID:           n.ID,
		CustomerSlug: n.CustomerSlug,
		Type:         n.Type,
		Title:        n.Title,
		Body:         n.Body,
		Link:         n.Link,
		Read:         n.Read,
		CreatedAt:    n.CreatedAt,
	}
}

func (p notificationPayload) toDomain() workspace.Notification {
	return workspace.Notification{
		ID:           p.ID,
		CustomerSlug: p.CustomerSlug,
		Type:         p.Type,
		Title:        p.Title,
		Body:         p.Body,
		Link:         p.Link,
		Read:         p.Read,
		CreatedAt:    p.CreatedAt,
	}
}

// encodeNotification marshals n for the NOTIFY payload, truncating Body if the
// full frame would exceed maxNotifyPayload.
func encodeNotification(n workspace.Notification) (string, error) {
	p := toPayload(n)
	raw, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("marshal notification payload: %w", err)
	}
	if len(raw) <= maxNotifyPayload {
		return string(raw), nil
	}
	// Trim Body by the overflow plus room for the ellipsis marker, on a rune
	// boundary so the JSON stays valid UTF-8.
	const ellipsis = "…"
	overflow := len(raw) - maxNotifyPayload + len(ellipsis)
	if overflow >= len(p.Body) {
		p.Body = ""
	} else {
		keep := len(p.Body) - overflow
		for keep > 0 && !utf8RuneStart(p.Body[keep]) {
			keep--
		}
		p.Body = p.Body[:keep] + ellipsis
	}
	raw, err = json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("marshal truncated notification payload: %w", err)
	}
	return string(raw), nil
}

// utf8RuneStart reports whether b is the first byte of a UTF-8 rune (i.e. not a
// 0b10xxxxxx continuation byte).
func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

// PublishNotification emits a NOTIFY so every replica's listener delivers n into
// its local broker. It implements workspace.NotificationPublisher.
func (r *Repository) PublishNotification(ctx context.Context, n workspace.Notification) error {
	payload, err := encodeNotification(n)
	if err != nil {
		return err
	}
	// pg_notify() is the parameterized form of NOTIFY (LISTEN/NOTIFY has no
	// bind-parameter syntax of its own).
	if _, err := r.DB.ExecContext(ctx, `SELECT pg_notify($1, $2)`, notificationChannel, payload); err != nil {
		return fmt.Errorf("postgres: pg_notify notification: %w", err)
	}
	return nil
}

// NotificationListener holds a dedicated Postgres connection LISTENing for
// notification NOTIFYs and fans each one into the local broker. One runs per
// replica; it is the receive half of the cross-replica bridge that
// Repository.PublishNotification feeds.
type NotificationListener struct {
	DSN    string
	Broker *workspace.NotificationBroker
	Logger *slog.Logger
}

// Run listens until ctx is cancelled, reconnecting with backoff on any
// connection loss. It owns its own pgx connection (outside the database/sql
// pool) because a LISTEN session must stay pinned to one long-lived connection.
func (l *NotificationListener) Run(ctx context.Context) {
	const backoff = 2 * time.Second
	for ctx.Err() == nil {
		if err := l.listen(ctx); err != nil && ctx.Err() == nil {
			if l.Logger != nil {
				l.Logger.WarnContext(ctx, "notification listener: connection lost, retrying", "err", err, "backoff", backoff)
			}
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
		}
	}
}

// listen opens one connection, LISTENs, and pumps notifications until an error
// or ctx cancellation.
func (l *NotificationListener) listen(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, l.DSN)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{notificationChannel}.Sanitize()); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if l.Logger != nil {
		l.Logger.InfoContext(ctx, "notification listener: subscribed", "channel", notificationChannel)
	}

	for {
		msg, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		var p notificationPayload
		if err := json.Unmarshal([]byte(msg.Payload), &p); err != nil {
			if l.Logger != nil {
				l.Logger.WarnContext(ctx, "notification listener: bad payload, skipping", "err", err)
			}
			continue
		}
		l.Broker.Broadcast(p.toDomain())
	}
}
