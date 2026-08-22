package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	notificationv1 "github.com/fairtier/workspace-api/proto/notification/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// NotificationServer implements the ConnectRPC NotificationService handler.
type NotificationServer struct {
	Service *workspace.NotificationService
}

func (s *NotificationServer) ListNotifications(ctx context.Context, _ *connect.Request[notificationv1.ListNotificationsRequest]) (*connect.Response[notificationv1.ListNotificationsResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	list, err := s.Service.List(ctx, callerID)
	if err != nil {
		return nil, notificationError(err)
	}
	unread, err := s.Service.UnreadCount(ctx, callerID)
	if err != nil {
		return nil, notificationError(err)
	}

	out := make([]*notificationv1.Notification, 0, len(list))
	for i := range list {
		out = append(out, notificationToPB(&list[i]))
	}
	return connect.NewResponse(&notificationv1.ListNotificationsResponse{
		Notifications: out,
		UnreadCount:   int32(unread),
	}), nil
}

func (s *NotificationServer) GetUnreadCount(ctx context.Context, _ *connect.Request[notificationv1.GetUnreadCountRequest]) (*connect.Response[notificationv1.GetUnreadCountResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	unread, err := s.Service.UnreadCount(ctx, callerID)
	if err != nil {
		return nil, notificationError(err)
	}
	return connect.NewResponse(&notificationv1.GetUnreadCountResponse{UnreadCount: int32(unread)}), nil
}

func (s *NotificationServer) MarkRead(ctx context.Context, req *connect.Request[notificationv1.MarkReadRequest]) (*connect.Response[notificationv1.MarkReadResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if req.Msg.Id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	if err := s.Service.MarkRead(ctx, callerID, req.Msg.Id); err != nil {
		return nil, notificationError(err)
	}
	return connect.NewResponse(&notificationv1.MarkReadResponse{}), nil
}

func (s *NotificationServer) MarkAllRead(ctx context.Context, _ *connect.Request[notificationv1.MarkAllReadRequest]) (*connect.Response[notificationv1.MarkAllReadResponse], error) {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}
	if err := s.Service.MarkAllRead(ctx, callerID); err != nil {
		return nil, notificationError(err)
	}
	return connect.NewResponse(&notificationv1.MarkAllReadResponse{}), nil
}

func (s *NotificationServer) StreamNotifications(ctx context.Context, _ *connect.Request[notificationv1.StreamNotificationsRequest], stream *connect.ServerStream[notificationv1.Notification]) error {
	callerID := core.UserIDFromContext(ctx)
	if callerID == "" {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	ch, cancel, err := s.Service.Subscribe(ctx, callerID)
	if err != nil {
		return notificationError(err)
	}
	defer cancel()

	return pumpNotifications(ctx, ch, streamHeartbeatInterval, stream.Send)
}

// streamHeartbeatInterval is the gap between StreamNotifications keep-alive
// frames. It sits comfortably under Cloudflare's ~100s idle-proxy timeout so
// the stream never goes silent long enough to be cut.
const streamHeartbeatInterval = 25 * time.Second

// pumpNotifications drives the StreamNotifications send loop: an immediate
// heartbeat on connect, a heartbeat every interval while idle, and real
// notifications as they arrive off ch. It returns nil when ctx is cancelled or
// ch closes, and the send error otherwise.
//
// The heartbeats are load-bearing: an idle server-stream that flushes no bytes
// trips the Envoy route timeout and Cloudflare's ~100s proxy timeout, which
// surfaces to the browser as a 504 (and, because the 504 page carries no CORS
// header, as a spurious CORS error). The initial frame also flushes the
// response headers right away so the client's stream actually opens. Clients
// skip frames with heartbeat=true.
func pumpNotifications(ctx context.Context, ch <-chan workspace.Notification, interval time.Duration, send func(*notificationv1.Notification) error) error {
	if err := send(&notificationv1.Notification{Heartbeat: true}); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := send(&notificationv1.Notification{Heartbeat: true}); err != nil {
				return err
			}
		case n, ok := <-ch:
			if !ok {
				return nil
			}
			if err := send(notificationToPB(&n)); err != nil {
				return err
			}
		}
	}
}

// notificationError maps notification-specific domain errors onto Connect codes.
func notificationError(err error) error {
	if errors.Is(err, workspace.ErrNotificationNotFound) {
		return connect.NewError(connect.CodeNotFound, err)
	}
	return domainError(err)
}

func notificationToPB(n *workspace.Notification) *notificationv1.Notification {
	return &notificationv1.Notification{
		Id:        n.ID,
		Type:      n.Type,
		Title:     n.Title,
		Body:      n.Body,
		Link:      n.Link,
		Read:      n.Read,
		CreatedAt: n.CreatedAt.UTC().Format(time.RFC3339),
	}
}
