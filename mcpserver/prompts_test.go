package mcpserver

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fairtier/workspace-api/workspace"
)

var promptNames = []string{"diagnose_failures", "build_workflow", "profile_table", "review_transformation"}

func promptText(t *testing.T, res *mcp.GetPromptResult) string {
	t.Helper()
	if len(res.Messages) != 1 || res.Messages[0].Role != "user" {
		t.Fatalf("messages = %+v, want one user message", res.Messages)
	}
	tc, ok := res.Messages[0].Content.(*mcp.TextContent)
	if !ok {
		t.Fatalf("content = %T, want text", res.Messages[0].Content)
	}
	return tc.Text
}

func TestMCP_PromptsList(t *testing.T) {
	env := newTestEnv(t)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	res, err := env.connect(t, readPAT).ListPrompts(context.Background(), nil)
	if err != nil {
		t.Fatalf("prompts/list: %v", err)
	}
	var names []string
	args := map[string][]string{}
	for _, p := range res.Prompts {
		names = append(names, p.Name)
		if p.Description == "" || p.Title == "" {
			t.Errorf("%s lacks a title or description", p.Name)
		}
		for _, a := range p.Arguments {
			if !a.Required || a.Description == "" {
				t.Errorf("%s argument %s: required=%v, description=%q", p.Name, a.Name, a.Required, a.Description)
			}
			args[p.Name] = append(args[p.Name], a.Name)
		}
	}
	// The SDK lists prompts by name.
	want := slices.Sorted(slices.Values(promptNames))
	if !slices.Equal(names, want) {
		t.Errorf("prompts = %v, want %v", names, want)
	}
	wantArgs := map[string][]string{"build_workflow": {"goal"}, "profile_table": {"namespace", "table"}, "review_transformation": {"id"}}
	for name, a := range wantArgs {
		if !slices.Equal(args[name], a) {
			t.Errorf("%s arguments = %v, want %v", name, args[name], a)
		}
	}
}

func TestMCP_PromptsGet(t *testing.T) {
	env := newTestEnv(t)
	writePAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceWrite)
	readPAT, _ := env.mintPAT(t, workspace.ScopeWorkspaceRead)
	write, read := env.connect(t, writePAT), env.connect(t, readPAT)
	get := func(t *testing.T, s *mcp.ClientSession, name string, args map[string]string) string {
		t.Helper()
		res, err := s.GetPrompt(context.Background(), &mcp.GetPromptParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("prompts/get %s: %v", name, err)
		}
		return promptText(t, res)
	}

	t.Run("diagnose_failures starts from status and never triggers unasked", func(t *testing.T) {
		txt := get(t, write, "diagnose_failures", nil)
		for _, want := range []string{"workspace_status", "get_pipeline", "read_repo_file", "Do NOT trigger", "1000 rows"} {
			if !strings.Contains(txt, want) {
				t.Errorf("missing %q", want)
			}
		}
		if !strings.Contains(get(t, read, "diagnose_failures", nil), "cannot trigger") {
			t.Error("read-only variant should say runs cannot be triggered")
		}
	})

	t.Run("build_workflow carries the goal and the safe order", func(t *testing.T) {
		txt := get(t, write, "build_workflow", map[string]string{"goal": "refresh sales marts at 6"})
		for _, want := range []string{"refresh sales marts at 6", "list_pipelines", "double run", "enabled: false", "Do neither without a yes"} {
			if !strings.Contains(txt, want) {
				t.Errorf("missing %q", want)
			}
		}
		if strings.Contains(txt, "read-only token") {
			t.Error("write variant must not claim a read-only token")
		}
		if !strings.Contains(get(t, read, "build_workflow", map[string]string{"goal": "x"}), "read-only token") {
			t.Error("read-only variant should say the write steps are unavailable")
		}
	})

	t.Run("profile_table names the table", func(t *testing.T) {
		txt := get(t, read, "profile_table", map[string]string{"namespace": "raw", "table": "orders"})
		for _, want := range []string{"raw.orders", "describe_table", "run_query", "dbt tests"} {
			if !strings.Contains(txt, want) {
				t.Errorf("missing %q", want)
			}
		}
	})

	t.Run("review_transformation names the id", func(t *testing.T) {
		txt := get(t, write, "review_transformation", map[string]string{"id": "t-123"})
		for _, want := range []string{`get_transformation (id "t-123")`, "list_repo_files", "write_repo_file"} {
			if !strings.Contains(txt, want) {
				t.Errorf("missing %q", want)
			}
		}
	})

	t.Run("a missing required argument is an error", func(t *testing.T) {
		if _, err := write.GetPrompt(context.Background(), &mcp.GetPromptParams{Name: "profile_table", Arguments: map[string]string{"namespace": "raw"}}); err == nil {
			t.Error("want an error for the missing table argument")
		}
	})
}

// The prompts must only name tools that exist, and the SQL shapes they
// recommend must pass the read-only guard.
func TestPrompts_ReferenceRealTools(t *testing.T) {
	known := map[string]bool{}
	for _, tl := range toolset() {
		known[tl.name] = true
	}
	for _, p := range promptset() {
		args := map[string]string{"goal": "g", "namespace": "n", "table": "t", "id": "i"}
		for _, word := range strings.FieldsFunc(p.render(args, true), func(r rune) bool {
			return r != '_' && (r < 'a' || r > 'z')
		}) {
			if looksLikeTool(word) && !known[word] && !fieldNames[word] {
				t.Errorf("prompt %s names unknown tool %q", p.prompt.Name, word)
			}
		}
	}
	for _, sql := range []string{
		"SELECT count(*), approx_count_distinct(id) FROM raw.orders",
		"SELECT status, count(*) FROM raw.orders USING SAMPLE 10% GROUP BY status ORDER BY 2 DESC LIMIT 10",
	} {
		if err := CheckReadOnlySQL(sql); err != nil {
			t.Errorf("guard refuses a recommended query %q: %v", sql, err)
		}
	}
}

// fieldNames are API fields the prompts quote that share the tool-name shape.
var fieldNames = map[string]bool{"write_disposition": true, "trigger_after_pipeline_id": true}

// looksLikeTool matches the verb_noun shape of this server's tool names.
func looksLikeTool(w string) bool {
	for _, prefix := range []string{"list_", "get_", "describe_", "run_", "read_", "write_", "trigger_", "create_", "update_", "workspace_"} {
		if strings.HasPrefix(w, prefix) {
			return true
		}
	}
	return false
}
