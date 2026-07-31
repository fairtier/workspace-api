package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	boxrepov1 "github.com/fairtier/workspace-api/proto/boxrepo/v1"
	"github.com/fairtier/workspace-api/workspace"
)

// BoxRepoServer implements the ConnectRPC BoxRepoService handler: the
// Console's read/write surface over the box's Gitea repos (Rill project,
// hosted dbt transformations).
type BoxRepoServer struct {
	Service *workspace.BoxRepoService
}

// ListFiles returns the customer-authored file tree of a box repo.
func (s *BoxRepoServer) ListFiles(ctx context.Context, req *connect.Request[boxrepov1.ListFilesRequest]) (*connect.Response[boxrepov1.ListFilesResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	entries, err := s.Service.ListFiles(ctx, callerID, req.Msg.Repo)
	if err != nil {
		return nil, boxRepoError(err)
	}

	files := make([]*boxrepov1.FileEntry, 0, len(entries))
	for _, e := range entries {
		files = append(files, &boxrepov1.FileEntry{Path: e.Path, Sha: e.SHA})
	}
	return connect.NewResponse(&boxrepov1.ListFilesResponse{Files: files}), nil
}

// GetFile returns one file's content and blob sha.
func (s *BoxRepoServer) GetFile(ctx context.Context, req *connect.Request[boxrepov1.GetFileRequest]) (*connect.Response[boxrepov1.GetFileResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	content, sha, err := s.Service.GetFile(ctx, callerID, req.Msg.Repo, req.Msg.Path)
	if err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.GetFileResponse{Content: content, Sha: sha}), nil
}

// PutFile creates or updates one file as a commit on the default branch.
func (s *BoxRepoServer) PutFile(ctx context.Context, req *connect.Request[boxrepov1.PutFileRequest]) (*connect.Response[boxrepov1.PutFileResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	sha, err := s.Service.PutFile(ctx, callerID, req.Msg.Repo, req.Msg.Path, req.Msg.Content, req.Msg.Sha, req.Msg.Message)
	if err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.PutFileResponse{Sha: sha}), nil
}

// ListFileHistory returns the newest-first commit history of one file.
func (s *BoxRepoServer) ListFileHistory(ctx context.Context, req *connect.Request[boxrepov1.ListFileHistoryRequest]) (*connect.Response[boxrepov1.ListFileHistoryResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	commits, err := s.Service.ListFileHistory(ctx, callerID, req.Msg.Repo, req.Msg.Path)
	if err != nil {
		return nil, boxRepoError(err)
	}
	out := make([]*boxrepov1.FileCommit, 0, len(commits))
	for _, c := range commits {
		out = append(out, &boxrepov1.FileCommit{
			Sha:         c.SHA,
			Message:     c.Message,
			AuthorName:  c.AuthorName,
			AuthorEmail: c.AuthorEmail,
			Date:        c.Date,
		})
	}
	return connect.NewResponse(&boxrepov1.ListFileHistoryResponse{Commits: out}), nil
}

// GetFileAtRef returns one file's content as of a commit sha.
func (s *BoxRepoServer) GetFileAtRef(ctx context.Context, req *connect.Request[boxrepov1.GetFileAtRefRequest]) (*connect.Response[boxrepov1.GetFileAtRefResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	content, err := s.Service.GetFileAtRef(ctx, callerID, req.Msg.Repo, req.Msg.Path, req.Msg.Ref)
	if err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.GetFileAtRefResponse{Content: content}), nil
}

// SetPushMirror configures (or replaces) a box repo's push mirror to the
// customer's own remote. The remote credential passes through to the box's
// Gitea and is never persisted centrally.
func (s *BoxRepoServer) SetPushMirror(ctx context.Context, req *connect.Request[boxrepov1.SetPushMirrorRequest]) (*connect.Response[boxrepov1.SetPushMirrorResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if err := s.Service.SetPushMirror(ctx, callerID, req.Msg.Repo, req.Msg.RemoteUrl, req.Msg.RemoteUsername, req.Msg.RemotePassword); err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.SetPushMirrorResponse{}), nil
}

// GetPushMirror returns a box repo's mirror status (credential-stripped).
func (s *BoxRepoServer) GetPushMirror(ctx context.Context, req *connect.Request[boxrepov1.GetPushMirrorRequest]) (*connect.Response[boxrepov1.GetPushMirrorResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	mirror, ok, err := s.Service.GetPushMirror(ctx, callerID, req.Msg.Repo)
	if err != nil {
		return nil, boxRepoError(err)
	}
	if !ok {
		return connect.NewResponse(&boxrepov1.GetPushMirrorResponse{Configured: false}), nil
	}
	return connect.NewResponse(&boxrepov1.GetPushMirrorResponse{
		Configured: true,
		RemoteUrl:  mirror.RemoteURL,
		LastUpdate: mirror.LastUpdate,
		LastError:  mirror.LastError,
	}), nil
}

// DeletePushMirror removes a box repo's push mirror.
func (s *BoxRepoServer) DeletePushMirror(ctx context.Context, req *connect.Request[boxrepov1.DeletePushMirrorRequest]) (*connect.Response[boxrepov1.DeletePushMirrorResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if err := s.Service.DeletePushMirror(ctx, callerID, req.Msg.Repo); err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.DeletePushMirrorResponse{}), nil
}

// SyncPushMirror triggers an immediate push to the mirror remote.
func (s *BoxRepoServer) SyncPushMirror(ctx context.Context, req *connect.Request[boxrepov1.SyncPushMirrorRequest]) (*connect.Response[boxrepov1.SyncPushMirrorResponse], error) {
	callerID := UserIDFromContext(ctx)
	if callerID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	if err := s.Service.SyncPushMirror(ctx, callerID, req.Msg.Repo); err != nil {
		return nil, boxRepoError(err)
	}
	return connect.NewResponse(&boxrepov1.SyncPushMirrorResponse{}), nil
}

// boxRepoError maps box-repo domain errors onto Connect codes.
func boxRepoError(err error) error {
	switch {
	case errors.Is(err, workspace.ErrBoxRepoUnavailable),
		errors.Is(err, workspace.ErrBoxCredentialNotFound):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, workspace.ErrRepoFileChanged):
		return connect.NewError(connect.CodeAborted, err)
	default:
		return domainError(err)
	}
}
