package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	boxrepov1 "github.com/fairtier/workspace-api/proto/boxrepo/v1"
	notificationv1 "github.com/fairtier/workspace-api/proto/notification/v1"
)

const reposNote = `The workspace has two editable git repositories: "transformations" (the dbt project: models/*.sql, schema .yml files, dbt_project.yml; plus the workspace's own definition files, transformations/<name>.yaml and workflows/<name>.yaml, which the workspace renders and adopts edits from) and "rill" (Rill dashboards: sources, models and dashboard .yaml files; present only when Rill is enabled). To create or change a workflow prefer create_workflow/update_workflow: they validate immediately, while an edited workflows/*.yaml file is only picked up on the next sweep (minutes) and a mistake shows up as a notification.`

const listRepoFilesDesc = `List the files in one of the workspace's git repositories, with each file's blob sha. ` + reposNote + ` Dotfiles, platform-managed files and anything that looks like a secrets file are not listed.`

const readRepoFileDesc = `Read one file from one of the workspace's git repositories. Returns the content and its blob sha — pass that sha to write_repo_file to update the file. ` + reposNote + ` Files over 256 KB and secrets files are refused.`

const writeRepoFileDesc = `Create or update one file in one of the workspace's git repositories, as a commit on the default branch. ` + reposNote + `

To update an existing file, pass the sha read_repo_file returned: the write is refused if the file changed since (read it again and merge). Omit sha only to create a new file. The workspace picks up the change on its own: transformations use the new commit on their next run; Rill reloads dashboards. Commit messages are recorded with the credential that made them.`

const listNotificationsDesc = `List the workspace's recent notifications, newest first: pipeline and transformation failures, completed loads and other events the Console shows in its bell, with the unread count. A quick way to see what went wrong recently.`

type repoIn struct {
	Repo string `json:"repo" jsonschema:"which repository: transformations or rill"`
}

type repoFile struct {
	Path string `json:"path"`
	Sha  string `json:"sha"`
}

type listRepoFilesOut struct {
	Files []repoFile `json:"files"`
}

func listRepoFiles(ctx context.Context, b Backends, in repoIn) (listRepoFilesOut, error) {
	resp, err := b.Repos.ListFiles(ctx, connect.NewRequest(&boxrepov1.ListFilesRequest{Repo: in.Repo}))
	if err != nil {
		return listRepoFilesOut{}, err
	}
	out := listRepoFilesOut{Files: make([]repoFile, 0, len(resp.Msg.Files))}
	for _, f := range resp.Msg.Files {
		if isSecretPath(f.Path) {
			continue
		}
		out.Files = append(out.Files, repoFile{Path: f.Path, Sha: f.Sha})
	}
	return out, nil
}

type readRepoFileIn struct {
	Repo string `json:"repo" jsonschema:"which repository: transformations or rill"`
	Path string `json:"path" jsonschema:"file path relative to the repository root, as list_repo_files reports it"`
}

type readRepoFileOut struct {
	Path    string `json:"path"`
	Sha     string `json:"sha"`
	Content string `json:"content"`
}

func readRepoFile(ctx context.Context, b Backends, in readRepoFileIn) (readRepoFileOut, error) {
	if isSecretPath(in.Path) {
		return readRepoFileOut{}, secretPathError(in.Path)
	}
	resp, err := b.Repos.GetFile(ctx, connect.NewRequest(&boxrepov1.GetFileRequest{Repo: in.Repo, Path: in.Path}))
	if err != nil {
		return readRepoFileOut{}, err
	}
	// Refused rather than truncated: a truncated file handed back to an
	// agent that then writes it would silently drop the tail.
	if n := len(resp.Msg.Content); n > maxFileBytes {
		return readRepoFileOut{}, fmt.Errorf("%s is %d KB; read_repo_file returns files up to %d KB", in.Path, n>>10, maxFileBytes>>10)
	}
	return readRepoFileOut{Path: in.Path, Sha: resp.Msg.Sha, Content: resp.Msg.Content}, nil
}

type writeRepoFileIn struct {
	Repo    string `json:"repo" jsonschema:"which repository: transformations or rill"`
	Path    string `json:"path" jsonschema:"file path relative to the repository root"`
	Content string `json:"content" jsonschema:"the complete new file content"`
	Message string `json:"message" jsonschema:"the commit message: what changed and why"`
	Sha     string `json:"sha,omitempty" jsonschema:"the blob sha read_repo_file returned; required to update an existing file, omitted to create a new one"`
}

type writeRepoFileOut struct {
	Path string `json:"path"`
	Sha  string `json:"sha"`
}

func writeRepoFile(ctx context.Context, b Backends, in writeRepoFileIn) (writeRepoFileOut, error) {
	if isSecretPath(in.Path) {
		return writeRepoFileOut{}, secretPathError(in.Path)
	}
	if strings.TrimSpace(in.Message) == "" {
		return writeRepoFileOut{}, errors.New("message is required: say what changed and why")
	}
	if len(in.Content) > maxWriteBytes {
		return writeRepoFileOut{}, fmt.Errorf("content is %d KB; write_repo_file accepts up to %d KB", len(in.Content)>>10, maxWriteBytes>>10)
	}
	resp, err := b.Repos.PutFile(ctx, connect.NewRequest(&boxrepov1.PutFileRequest{
		Repo: in.Repo, Path: in.Path, Content: in.Content, Sha: in.Sha,
		Message: strings.TrimSpace(in.Message) + auditTrailer(ctx),
	}))
	if err != nil {
		return writeRepoFileOut{}, err
	}
	return writeRepoFileOut{Path: in.Path, Sha: resp.Msg.Sha}, nil
}

type notification struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Body      string `json:"body,omitempty"`
	Link      string `json:"link,omitempty"`
	Read      bool   `json:"read"`
	CreatedAt string `json:"created_at"`
}

type listNotificationsOut struct {
	UnreadCount   int32          `json:"unread_count"`
	Notifications []notification `json:"notifications"`
}

func listNotifications(ctx context.Context, b Backends, _ struct{}) (listNotificationsOut, error) {
	resp, err := b.Notifications.ListNotifications(ctx, connect.NewRequest(&notificationv1.ListNotificationsRequest{}))
	if err != nil {
		return listNotificationsOut{}, err
	}
	out := listNotificationsOut{
		UnreadCount:   resp.Msg.UnreadCount,
		Notifications: make([]notification, 0, len(resp.Msg.Notifications)),
	}
	for _, n := range resp.Msg.Notifications {
		out.Notifications = append(out.Notifications, notification{
			ID: n.Id, Type: n.Type, Title: n.Title, Link: n.Link, Read: n.Read, CreatedAt: n.CreatedAt,
			Body: truncate(redactString(n.Body), maxNotificationBody),
		})
	}
	return out, nil
}
