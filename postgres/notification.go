package postgres

import (
	"context"
	"fmt"

	"github.com/fairtier/workspace-api/workspace"
)

func (r *Repository) CreateNotification(ctx context.Context, n *workspace.Notification) error {
	err := r.DB.QueryRowContext(ctx,
		`INSERT INTO notifications (customer_slug, type, title, body, link, read)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at`,
		n.CustomerSlug, n.Type, n.Title, n.Body, n.Link, n.Read,
	).Scan(&n.ID, &n.CreatedAt)
	if err != nil {
		return fmt.Errorf("postgres: create notification: %w", err)
	}
	return nil
}

func (r *Repository) ListNotifications(ctx context.Context, customerSlug string, limit int) ([]workspace.Notification, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, customer_slug, type, title, body, link, read, created_at
		 FROM notifications
		 WHERE customer_slug = $1
		 ORDER BY created_at DESC
		 LIMIT $2`,
		customerSlug, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: list notifications: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []workspace.Notification
	for rows.Next() {
		var n workspace.Notification
		if err := rows.Scan(&n.ID, &n.CustomerSlug, &n.Type, &n.Title, &n.Body, &n.Link, &n.Read, &n.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan notification: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: iterate notifications: %w", err)
	}
	return out, nil
}

func (r *Repository) UnreadCount(ctx context.Context, customerSlug string) (int, error) {
	var count int
	err := r.DB.QueryRowContext(ctx,
		`SELECT count(*) FROM notifications WHERE customer_slug = $1 AND NOT read`,
		customerSlug,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("postgres: unread count: %w", err)
	}
	return count, nil
}

func (r *Repository) MarkRead(ctx context.Context, customerSlug, id string) error {
	res, err := r.DB.ExecContext(ctx,
		`UPDATE notifications SET read = TRUE WHERE id = $1 AND customer_slug = $2`,
		id, customerSlug,
	)
	if err != nil {
		return fmt.Errorf("postgres: mark notification read: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: rows affected: %w", err)
	}
	if n == 0 {
		return workspace.ErrNotificationNotFound
	}
	return nil
}

func (r *Repository) MarkAllRead(ctx context.Context, customerSlug string) error {
	_, err := r.DB.ExecContext(ctx,
		`UPDATE notifications SET read = TRUE WHERE customer_slug = $1 AND NOT read`,
		customerSlug,
	)
	if err != nil {
		return fmt.Errorf("postgres: mark all notifications read: %w", err)
	}
	return nil
}
