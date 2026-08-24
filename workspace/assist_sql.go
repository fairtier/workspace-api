package workspace

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/fairtier/workspace-api/core"
)

// Bounds for the SQL draft inputs. The schema context is the big one: a
// workspace can hold hundreds of tables, and the prompt must stay a prompt,
// not a catalog dump.
const (
	// maxSchemaContextBytes caps the schema block appended to the prompt.
	maxSchemaContextBytes = 8 * 1024
	// maxDescribedTables caps how many tables get column detail (each DESCRIBE
	// is one engine round trip that also forces the iceberg bind).
	maxDescribedTables = 20
	// describeConcurrency bounds the parallel DESCRIBEs.
	describeConcurrency = 4
	// maxCurrentSQLBytes truncates the editor's current SQL in the prompt.
	maxCurrentSQLBytes = 16 * 1024
)

// DraftSql turns prompt into a draft DuckDB query for the caller's tenant.
// The server assembles the schema context from the tenant's own engine — all
// table names, column detail for the most relevant few — and the draft is
// validated with EXPLAIN best-effort: a statement the engine rejects is still
// returned (the user reviews and edits anyway), with the engine's message
// appended to notes. The draft is never executed here.
func (s *AssistService) DraftSql(ctx context.Context, callerID core.UserID, prompt, currentSQL string) (_ *SqlDraft, err error) {
	defer func() { recordDraft(ctx, "sql", err) }()

	if s.Sql == nil || s.Schema == nil {
		return nil, ErrDraftNotConfigured
	}
	if err := s.gate(ctx, callerID, prompt); err != nil {
		return nil, err
	}

	schemaContext, err := s.buildSchemaContext(ctx, callerID, prompt)
	if err != nil {
		return nil, fmt.Errorf("list warehouse schema: %w", err)
	}
	if len(currentSQL) > maxCurrentSQLBytes {
		currentSQL = currentSQL[:maxCurrentSQLBytes]
	}

	draft, err := s.Sql.DraftSql(ctx, prompt, currentSQL, schemaContext)
	if err != nil {
		return nil, fmt.Errorf("draft sql: %w", err)
	}
	if strings.TrimSpace(draft.SQL) == "" {
		return nil, &ErrInvalidSourceConfig{Field: "sql", Msg: "draft produced no SQL"}
	}

	// Best-effort validation, never fatal: EXPLAIN plans without executing,
	// and the engine's own message ("column X not found, did you mean...") is
	// worth more to the user than a rejected draft.
	if verr := s.Schema.Explain(ctx, callerID, draft.SQL); verr != nil {
		draft.Notes = strings.TrimSpace(draft.Notes +
			"\n\nThe engine could not validate this draft: " + verr.Error())
	}
	return draft, nil
}

// buildSchemaContext renders the prompt's schema block: every table name
// (cheap — one constant query), plus column detail for at most
// maxDescribedTables tables, preferring those whose name matches a prompt
// token. Column detail is per-table best-effort — a failed DESCRIBE degrades
// that table to name-only rather than failing the draft.
func (s *AssistService) buildSchemaContext(ctx context.Context, callerID core.UserID, prompt string) (string, error) {
	tables, err := s.Schema.Tables(ctx, callerID)
	if err != nil {
		return "", err
	}
	if len(tables) == 0 {
		return "The warehouse currently has no tables.", nil
	}

	described := pickDescribeTargets(tables, prompt, maxDescribedTables)
	columns := s.describeAll(ctx, callerID, described)

	var b strings.Builder
	b.WriteString("Warehouse schema (DuckDB; reference tables as \"namespace\".\"table\"):\n")
	for _, t := range tables {
		if b.Len() >= maxSchemaContextBytes {
			fmt.Fprintf(&b, "... (%d tables total; listing truncated)\n", len(tables))
			break
		}
		cols, ok := columns[t]
		if !ok || len(cols) == 0 {
			fmt.Fprintf(&b, "- %s.%s\n", t.Namespace, t.Name)
			continue
		}
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			parts = append(parts, c.Name+" "+c.Type)
		}
		fmt.Fprintf(&b, "- %s.%s (%s)\n", t.Namespace, t.Name, strings.Join(parts, ", "))
	}
	return b.String(), nil
}

// pickDescribeTargets chooses which tables get column detail: all of them
// when few, else prompt-token matches first (a request naming "orders" gets
// the orders table described), then list order as the tiebreaker.
func pickDescribeTargets(tables []TableRef, prompt string, limit int) []TableRef {
	if len(tables) <= limit {
		return tables
	}
	tokens := promptTokens(prompt)
	type scored struct {
		ref   TableRef
		score int
		idx   int
	}
	all := make([]scored, len(tables))
	for i, t := range tables {
		sc := 0
		lower := strings.ToLower(t.Namespace + "." + t.Name)
		for _, tok := range tokens {
			if strings.Contains(lower, tok) {
				sc++
			}
		}
		all[i] = scored{ref: t, score: sc, idx: i}
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return all[i].idx < all[j].idx
	})
	out := make([]TableRef, 0, limit)
	for _, s := range all[:limit] {
		out = append(out, s.ref)
	}
	return out
}

// promptTokens lowercases and splits the prompt into word tokens of 3+ chars
// (shorter ones match everything and score nothing meaningful).
func promptTokens(prompt string) []string {
	fields := strings.FieldsFunc(strings.ToLower(prompt), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_'
	})
	out := fields[:0]
	for _, f := range fields {
		if len(f) >= 3 {
			out = append(out, f)
		}
	}
	return out
}

// describeAll runs the DESCRIBEs with bounded concurrency, each best-effort.
func (s *AssistService) describeAll(ctx context.Context, callerID core.UserID, targets []TableRef) map[TableRef][]ColumnSchema {
	out := make(map[TableRef][]ColumnSchema, len(targets))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, describeConcurrency)
	for _, t := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			cols, err := s.Schema.Columns(ctx, callerID, t)
			if err != nil {
				return // name-only for this table
			}
			mu.Lock()
			out[t] = cols
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}
