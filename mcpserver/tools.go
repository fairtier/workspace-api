package mcpserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// A tool is one MCP tool bound to its handler. Adding one is a toolset()
// entry plus a handler: readTool(...) or writeTool(...) with an input struct
// (jsonschema tags become the argument descriptions the agent reads), an
// output struct, and a func(ctx, Backends, In) (Out, error). The ctx a
// handler receives already carries the caller as the ConnectRPC handlers
// expect it, and write tools have already had their scope checked.
type tool struct {
	name     string
	write    bool
	register func(*mcp.Server, Backends)
}

// toolset is every tool, in the order tools/list reports them.
func toolset() []tool {
	return []tool{
		// The lakehouse.
		readTool("list_tables", "List tables", listTablesDesc, listTables),
		readTool("describe_table", "Describe a table", describeTableDesc, describeTable),
		readTool("run_query", "Run a read-only SQL query", runQueryDesc, runQuery),
		// Ingestion.
		readTool("list_pipelines", "List pipelines", listPipelinesDesc, listPipelines),
		readTool("get_pipeline", "Get a pipeline and its recent runs", getPipelineDesc, getPipeline),
		writeTool("trigger_pipeline", "Run a pipeline now", triggerPipelineDesc, effects{destructive: true, openWorld: true}, triggerPipeline),
		// Transformation.
		readTool("list_transformations", "List transformations", listTransformationsDesc, listTransformations),
		readTool("get_transformation", "Get a transformation and its recent runs", getTransformationDesc, getTransformation),
		writeTool("trigger_transformation", "Run a transformation now", triggerTransformationDesc, effects{destructive: true}, triggerTransformation),
		// The workspace's git repositories.
		readTool("list_repo_files", "List repository files", listRepoFilesDesc, listRepoFiles),
		readTool("read_repo_file", "Read a repository file", readRepoFileDesc, readRepoFile),
		writeTool("write_repo_file", "Write a repository file", writeRepoFileDesc, effects{destructive: true, idempotent: true}, writeRepoFile),
		// Status.
		readTool("list_notifications", "List notifications", listNotificationsDesc, listNotifications),
	}
}

type handlerFunc[In, Out any] func(ctx context.Context, b Backends, in In) (Out, error)

// effects are a write tool's MCP annotations. Hints only — no client may base
// a security decision on them — but they are what makes a client ask before
// running a tool.
type effects struct {
	destructive bool
	idempotent  bool
	openWorld   bool
}

func readTool[In, Out any](name, title, desc string, h handlerFunc[In, Out]) tool {
	ann := &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, OpenWorldHint: new(false)}
	return newTool(name, desc, ann, false, h)
}

func writeTool[In, Out any](name, title, desc string, e effects, h handlerFunc[In, Out]) tool {
	ann := &mcp.ToolAnnotations{
		Title:           title,
		DestructiveHint: new(e.destructive),
		IdempotentHint:  e.idempotent,
		OpenWorldHint:   new(e.openWorld),
	}
	return newTool(name, desc+writeScopeNote, ann, true, h)
}

const writeScopeNote = "\n\nRequires an access token with the workspace:write scope."

func newTool[In, Out any](name, desc string, ann *mcp.ToolAnnotations, write bool, h handlerFunc[In, Out]) tool {
	return tool{
		name:  name,
		write: write,
		register: func(s *mcp.Server, b Backends) {
			mcp.AddTool(s, &mcp.Tool{Name: name, Description: desc, Annotations: ann},
				func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
					var zero Out
					ctx, err := callContext(ctx, req, write)
					if err != nil {
						return nil, zero, err
					}
					out, err := h(ctx, b, in)
					if err != nil {
						return nil, zero, err
					}
					return nil, out, nil
				})
		},
	}
}

var errWriteScope = errors.New("this tool changes the workspace and needs an access token with the workspace:write scope; this token is read-only")

type callerCtxKey struct{}

// callContext authorizes a call and returns the context the backends expect:
// the caller as core.UserIDFromContext reads it, plus their profile for
// commit attribution.
func callContext(ctx context.Context, req *mcp.CallToolRequest, write bool) (context.Context, error) {
	ti := req.Extra.TokenInfo
	caller, ok := callerFromTokenInfo(ti)
	if !ok {
		return nil, errors.New("unauthenticated")
	}
	if write && !hasScope(ti, workspace.ScopeWorkspaceWrite) {
		return nil, errWriteScope
	}
	ctx = core.ContextWithUserID(ctx, caller.UserID)
	ctx = core.ContextWithTokenProfile(ctx, caller.Profile)
	return context.WithValue(ctx, callerCtxKey{}, caller), nil
}

// auditTrailer is appended to commit messages made through MCP, so the
// repository's history says which credential made the change.
func auditTrailer(ctx context.Context) string {
	c, _ := ctx.Value(callerCtxKey{}).(Caller)
	if c.Via == "" {
		return "\n\nVia FairTier MCP"
	}
	return fmt.Sprintf("\n\nVia FairTier MCP, %s", c.Via)
}
