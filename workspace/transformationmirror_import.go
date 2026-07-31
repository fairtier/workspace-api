package workspace

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// Hydration for transformations — the twin of the pipeline import pass (see
// pipelinemirror_import.go): every transformations/<slug>.yaml this database
// does not track yet becomes a row, keeping the file's id. Read-only toward
// the repo, never deletes, and enabled only by wiring Importer (the box binary
// does; central does not).
//
// Git credentials for connected external repos are never rendered, so an
// imported transformation starts with none. That is correct for the box's own
// hosted repos, which the dbt runner reaches with box-local credentials; a
// transformation pointing at a customer's external repo needs its credentials
// re-entered once the Console saves through the box (Phase 3C).

// TransformationImporter creates a transformation row with a caller-supplied
// id, preserving the id the repo file carries (CreateTransformation lets
// Postgres mint one).
type TransformationImporter interface {
	ImportTransformation(ctx context.Context, t *Transformation) error
}

// importUnrendered creates a row for every transformation file the database
// does not track. A file it cannot take is left in place and reported once;
// only a repo read or an unexpected write failure aborts the pass.
func (m *TransformationMirror) importUnrendered(ctx context.Context, client RepoFileClient, customerSlug string, st *transformationAdoptState) error {
	if m.Importer == nil {
		return nil
	}
	tracked := trackedPaths(st.renders, func(r TransformationDefinitionRender) string { return r.Path })
	taken := newImportedSet[TransformationID]()
	for _, t := range st.transformations {
		taken.add(t.ID, t.Name)
	}

	imported := 0
	for _, filePath := range sortedPaths(st.yamlTree) {
		if tracked[filePath] {
			continue // this database rendered it; drift is the adopt pass's job
		}
		ok, err := m.importFile(ctx, client, customerSlug, filePath, st.yamlTree[filePath], taken)
		if err != nil {
			return err
		}
		if ok {
			imported++
		}
	}

	if imported > 0 && m.Logger != nil {
		m.Logger.InfoContext(ctx, "transformations imported from repo", "customer", customerSlug, "count", imported)
	}
	return nil
}

// importFile reads one untracked execution config and creates its row,
// reporting whether it did.
func (m *TransformationMirror) importFile(ctx context.Context, client RepoFileClient, customerSlug, filePath, blobSHA string, taken importedSet[TransformationID]) (bool, error) {
	content, _, err := client.GetContents(ctx, transformationsRepo, filePath)
	if err != nil {
		return false, fmt.Errorf("get %s: %w", filePath, err)
	}
	t, reason, err := m.parseImportableTransformation(ctx, content, customerSlug, taken)
	if err != nil {
		return false, err
	}
	if t == nil {
		m.reportImportSkip(ctx, filePath, blobSHA, reason)
		return false, nil
	}
	if err := m.Importer.ImportTransformation(ctx, t); err != nil {
		if errors.Is(err, ErrTransformationAlreadyExists) {
			m.reportImportSkip(ctx, filePath, blobSHA, "another transformation already uses that name")
			return false, nil
		}
		return false, fmt.Errorf("import %s: %w", filePath, err)
	}
	m.recordDefinitionRender(ctx, t.ID, filePath, blobSHA)
	taken.add(t.ID, t.Name)
	return true, nil
}

// parseImportableTransformation strictly parses a repo file into a new
// transformation row. It returns nil with a user-facing reason the file cannot
// be imported, or nil with an empty reason when the id already has a row (an
// untracked render, not a new transformation). A trigger-pipeline lookup
// failure other than not-found is a real error and aborts the pass, exactly as
// in the adopt path.
func (m *TransformationMirror) parseImportableTransformation(ctx context.Context, content, customerSlug string, taken importedSet[TransformationID]) (*Transformation, string, error) {
	var f transformationFile
	if err := yaml.Unmarshal([]byte(content), &f); err != nil {
		return nil, "the file is not valid YAML", nil
	}
	if uuid.Validate(f.ID) != nil {
		return nil, "the file has no valid id", nil
	}
	if taken.ids[TransformationID(f.ID)] {
		return nil, "", nil
	}
	if f.Name == "" {
		return nil, "the file has no name", nil
	}
	if taken.names[f.Name] {
		return nil, "another transformation already uses that name", nil
	}
	trigger, reason, err := m.importableTrigger(ctx, f.TriggerAfterPipeline, customerSlug)
	if err != nil || reason != "" {
		return nil, reason, err
	}

	t := &Transformation{
		ID:                     TransformationID(f.ID),
		CustomerSlug:           customerSlug,
		Name:                   f.Name,
		RepoURL:                f.RepoURL,
		RepoRef:                cmp.Or(f.RepoRef, "main"),
		Schedule:               f.Schedule,
		TriggerAfterPipelineID: trigger,
		DBTSelector:            f.DBTSelector,
		Enabled:                f.Enabled,
	}
	// The repo carries no creation time, and reconstructing one from git
	// history would cost a call per file for a display-only field.
	t.CreatedAt = time.Now()
	t.UpdatedAt = t.CreatedAt
	return t, "", nil
}

// importableTrigger resolves the file's trigger_after_pipeline. The pipeline
// import runs first in the same sweep, so a chained pipeline is normally
// already there; a missing one would violate the FK, and a cross-tenant one is
// refused for the same reason the adopt path refuses it.
func (m *TransformationMirror) importableTrigger(ctx context.Context, id, customerSlug string) (PipelineID, string, error) {
	if id == "" {
		return "", "", nil
	}
	p, err := m.Pipelines.GetPipeline(ctx, PipelineID(id))
	if errors.Is(err, ErrPipelineNotFound) {
		return "", "the file's trigger_after_pipeline does not exist", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("get trigger pipeline: %w", err)
	}
	if p.CustomerSlug != customerSlug {
		return "", "the file's trigger_after_pipeline does not exist", nil
	}
	return PipelineID(id), "", nil
}

func (m *TransformationMirror) reportImportSkip(ctx context.Context, filePath, blobSHA, reason string) {
	if reason == "" || m.Logger == nil || !m.importSkips.firstReport(filePath, blobSHA) {
		return
	}
	m.Logger.WarnContext(ctx, "transformation file not imported", "path", filePath, "reason", reason)
}
