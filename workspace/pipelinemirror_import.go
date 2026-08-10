package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// Hydration (control-plane/workspace-split Phase 3B): a box's workspace
// database starts empty, and the adopt pass only ever looks at files it holds
// a render row for — so a fresh box serves zero pipelines even though its
// Gitea repo carries the full set. The import pass closes that gap: every
// pipelines/<slug>.yaml the database does not track yet is read back into a
// row, keeping the id the file carries, so the box, the dlt-worker's run rows
// and the central cache all keep addressing the same pipeline.
//
// It is strictly read-only toward the repo — the property that makes the whole
// sweep safe to run while central still owns the write path — and it never
// deletes: a row whose file disappeared is left alone, because the box holds
// run history the repo does not.
//
// Turned on by wiring Importer, which the box binary does and central does
// not: centrally the Console remains the only create path (git-centric gaps
// #4), and importing there would resurrect a pipeline in the window between
// its row being deleted and its file being removed.

// PipelineImporter creates a pipeline row with a caller-supplied id. Separate
// from PipelineRepository.CreatePipeline, which lets Postgres mint the id:
// hydration must preserve the id the repo file already carries.
type PipelineImporter interface {
	ImportPipeline(ctx context.Context, p *Pipeline) error
}

// importUnrendered creates a row for every definition file the database does
// not track. A file it cannot take is left in place and reported once; only a
// repo read or an unexpected write failure aborts the pass. No importer wired
// (central) means no hydration at all.
func (m *PipelineMirror) importUnrendered(ctx context.Context, client RepoFileClient, customerSlug string, st *pipelineAdoptState) error {
	if m.Importer == nil {
		return nil
	}
	tracked := trackedPaths(st.renders, func(r PipelineDefinitionRender) string { return r.Path })
	taken := newImportedSet[PipelineID]()
	for _, p := range st.pipelines {
		taken.add(p.ID, p.Name)
	}

	imported := 0
	for _, filePath := range sortedPaths(st.yamlTree) {
		if tracked[filePath] {
			continue // this database rendered it; drift is the adopt pass's job
		}
		ok, err := m.importFile(ctx, client, customerSlug, filePath, st, taken)
		if err != nil {
			return err
		}
		if ok {
			imported++
		}
	}

	if imported > 0 && m.Logger != nil {
		m.Logger.InfoContext(ctx, "pipelines imported from repo", "customer", customerSlug, "count", imported)
	}
	return nil
}

// importFile reads one untracked definition file and creates its row,
// reporting whether it did.
func (m *PipelineMirror) importFile(ctx context.Context, client RepoFileClient, customerSlug, filePath string, st *pipelineAdoptState, taken importedSet[PipelineID]) (bool, error) {
	blobSHA := st.yamlTree[filePath]
	content, _, err := client.GetContents(ctx, pipelinesRepo, filePath)
	if err != nil {
		return false, fmt.Errorf("get %s: %w", filePath, err)
	}
	p, reason := parseImportablePipeline(content, customerSlug, taken)
	if p == nil {
		m.reportImportSkip(ctx, filePath, blobSHA, reason)
		return false, nil
	}
	// This process holds only the box's age PUBLIC key, so a credential file
	// it finds beside the definition is one it can never re-render. Marking
	// it external keeps the converge from deleting it as stale; a Console
	// credential edit reclaims ownership the usual way.
	_, hasCredentials := st.ageTree[credentialPathFor(filePath)]
	p.CredentialsExternal = hasCredentials

	if err := m.Importer.ImportPipeline(ctx, p); err != nil {
		if errors.Is(err, ErrPipelineAlreadyExists) {
			m.reportImportSkip(ctx, filePath, blobSHA, "a pipeline with that id or name already exists")
			return false, nil
		}
		return false, fmt.Errorf("import %s: %w", filePath, err)
	}
	recordAdoption(ctx, planePipelines, outcomeImported,
		attrRepoPath.String(filePath), attrPipelineID.String(string(p.ID)),
		attrSourceType.String(p.SourceType))
	m.recordDefinitionRender(ctx, p.ID, filePath, blobSHA)
	taken.add(p.ID, p.Name)
	return true, nil
}

// parseImportablePipeline strictly parses a repo file into a new pipeline row.
// It returns nil with a user-facing reason the file cannot be imported, or nil
// with an empty reason when there is simply nothing to do (the id already has
// a row, only without render bookkeeping).
func parseImportablePipeline(content, customerSlug string, taken importedSet[PipelineID]) (*Pipeline, string) {
	var f pipelineFile
	if err := yaml.Unmarshal([]byte(content), &f); err != nil {
		return nil, "the file is not valid YAML"
	}
	if uuid.Validate(f.ID) != nil {
		return nil, "the file has no valid id"
	}
	if taken.ids[PipelineID(f.ID)] {
		return nil, "" // already a row — an untracked render, not a new pipeline
	}
	if f.Name == "" {
		return nil, "the file has no name"
	}
	if taken.names[f.Name] {
		return nil, "another pipeline already uses that name"
	}
	p, err := pipelineFromFile(PipelineID(f.ID), f)
	if err != nil {
		return nil, "the file's source_config cannot be read"
	}
	restoreFileUpload(p)
	if err := ValidateSourceConfig(p.SourceType, p.SourceConfig); err != nil {
		return nil, "the file's source_config is not valid for its source type"
	}
	p.CustomerSlug = customerSlug
	// The repo carries no creation time, and reconstructing one from git
	// history would cost a call per file for a display-only field.
	p.CreatedAt = time.Now()
	p.UpdatedAt = p.CreatedAt
	return p, ""
}

// restoreFileUpload reverses the file_upload → filesystem rewrite the render
// applies (resolveFileUploadPipeline). A file_upload pipeline is rendered in
// the plain "filesystem" form the pipeline worker can load, so importing the
// file as written would silently change the pipeline's type and lose its
// file-drop identity — the upload surface disappears, and the next save would
// make that permanent.
//
// The marker is exact rather than heuristic: only that rewrite points a bucket
// URL at the pipeline's OWN file-drop prefix (…/uploads/<id>/). size_bytes and
// uploaded_at are not in the rendered file; they are display-only and
// FileDropService reads them back from the bucket.
func restoreFileUpload(p *Pipeline) {
	if p.SourceType != "filesystem" {
		return
	}
	var cfg struct {
		filesystemConfig
		Tables []filesystemTable `json:"tables"`
	}
	if err := json.Unmarshal(p.SourceConfig, &cfg); err != nil {
		return
	}
	if !strings.HasSuffix(cfg.BucketURL, "/uploads/"+string(p.ID)+"/") {
		return
	}
	files := make([]UploadedFile, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		files = append(files, UploadedFile{Name: t.Name, File: t.FileGlob})
	}
	raw, err := json.Marshal(fileUploadConfig{Files: files})
	if err != nil {
		return
	}
	p.SourceType = SourceTypeFileUpload
	p.SourceConfig = raw
}

// credentialPathFor maps a rendered definition path to the .age credential
// path beside it — the pair shares a slug so a rename moves both.
func credentialPathFor(definitionPath string) string {
	return strings.TrimSuffix(definitionPath, ".yaml") + ".credentials.age"
}

// trackedPaths collects the repo paths a render-bookkeeping table already
// accounts for, whatever the row type.
func trackedPaths[K comparable, V any](rows map[K]V, path func(V) string) map[string]bool {
	tracked := make(map[string]bool, len(rows))
	for _, row := range rows {
		tracked[path(row)] = true
	}
	return tracked
}

// importedSet holds the ids and display names an import batch must not reuse:
// the id is the primary key and (customer_slug, name) is unique, so a file
// colliding with either is refused before the insert rather than after.
type importedSet[ID comparable] struct {
	ids   map[ID]bool
	names map[string]bool
}

func newImportedSet[ID comparable]() importedSet[ID] {
	return importedSet[ID]{ids: make(map[ID]bool), names: make(map[string]bool)}
}

func (s importedSet[ID]) add(id ID, name string) {
	s.ids[id] = true
	s.names[name] = true
}

func (m *PipelineMirror) reportImportSkip(ctx context.Context, filePath, blobSHA, reason string) {
	if reason == "" || m.Logger == nil || !m.importSkips.firstReport(filePath, blobSHA) {
		return
	}
	m.Logger.WarnContext(ctx, "pipeline file not imported", "path", filePath, "reason", reason)
}

// importSkips suppresses repeat logs for a repo file the import pass cannot
// take. The sweep re-reads the same tree every interval, so an unfixable file
// would otherwise warn forever; keying on the blob sha means a new commit on
// the same path is reported again.
type importSkips struct {
	mu   sync.Mutex
	seen map[string]string // path → blob sha last reported
}

// firstReport records (path, blobSHA) and reports whether it is new.
func (s *importSkips) firstReport(path, blobSHA string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[path] == blobSHA {
		return false
	}
	if s.seen == nil {
		s.seen = make(map[string]string)
	}
	s.seen[path] = blobSHA
	return true
}
