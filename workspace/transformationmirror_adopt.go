package workspace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Adopt-on-drift for transformations (control-plane/workspace-split Phase
// 2F, the twin of the pipeline adopt pass): the box's transformations repo
// is the source of truth for execution configs, so out-of-band commits to
// transformations/<slug>.yaml are pulled INTO the central cache instead of
// being overwritten. Strictly read-only toward the repo.
//
// Semantics per foreign commit (blob sha differs from the last recorded
// render):
//   - parses cleanly, same id, trigger pipeline (if any) belongs to the
//     same customer → the central row adopts the repo state; the user is
//     notified "updated from git".
//   - anything else → the file is left alone, the user is notified once
//     (the refused sha is recorded), and the central cache keeps its state.
//     The next Console save still overwrites — an explicit save is newer
//     intent.
//
// New files and deletions are NOT adopted: the Console remains the only
// create/delete path. Git credentials are never part of the file, so they
// are never touched.

// AdoptCustomer runs one adopt pass for a customer. Out-of-scope customers
// (shared substrate, no deposited credential) are skipped silently.
func (m *TransformationMirror) AdoptCustomer(ctx context.Context, customerSlug string) error {
	client, _, ok, err := boxMirrorClientFor(ctx, m.Workspaces, m.Credentials, m.NewClient, customerSlug)
	if err != nil || !ok {
		return err
	}
	if m.DefinitionRenders == nil {
		return nil // no bookkeeping — cannot tell foreign commits apart
	}

	st, err := m.loadAdoptState(ctx, client, customerSlug)
	if err != nil {
		return err
	}
	// Hydration first (Phase 3B): rows created here match the tree exactly,
	// so the adopt loop below has nothing left to do for them.
	if err := m.importUnrendered(ctx, client, customerSlug, st); err != nil {
		return err
	}

	for i := range st.transformations {
		if err := m.adoptDefinition(ctx, client, &st.transformations[i], st.renders, st.yamlTree); err != nil {
			return err
		}
	}
	return nil
}

// transformationAdoptState is the snapshot one adopt pass works from: the
// managed transformations/*.yaml files, and the rows they map onto.
type transformationAdoptState struct {
	transformations []Transformation
	renders         map[TransformationID]TransformationDefinitionRender
	yamlTree        map[string]string
}

func (m *TransformationMirror) loadAdoptState(ctx context.Context, client RepoFileClient, customerSlug string) (*transformationAdoptState, error) {
	tree, err := client.ListTree(ctx, transformationsRepo)
	if err != nil {
		return nil, fmt.Errorf("list %s tree: %w", transformationsRepo, err)
	}
	st := &transformationAdoptState{yamlTree: transformationTreeYAML(tree)}

	if st.transformations, err = m.Transformations.ListTransformationsByCustomer(ctx, customerSlug); err != nil {
		return nil, fmt.Errorf("list transformations: %w", err)
	}
	if st.renders, err = m.DefinitionRenders.GetTransformationDefinitionRenders(ctx, customerSlug); err != nil {
		return nil, fmt.Errorf("get definition renders: %w", err)
	}
	return st, nil
}

// adoptDefinition inspects one transformation's rendered file for a foreign
// commit and adopts or refuses it.
func (m *TransformationMirror) adoptDefinition(ctx context.Context, client RepoFileClient, t *Transformation, defRenders map[TransformationID]TransformationDefinitionRender, yamlTree map[string]string) error {
	row, tracked := defRenders[t.ID]
	if !tracked {
		return nil // never rendered — the next save-converge bootstraps it
	}
	treeSHA, inTree := yamlTree[row.Path]
	if !inTree || treeSHA == row.BlobSHA {
		return nil // deleted files are healed by the next save-converge
	}
	if treeSHA == row.RefusedBlobSHA {
		return nil // already refused and reported this exact commit
	}

	content, _, err := client.GetContents(ctx, transformationsRepo, row.Path)
	if err != nil {
		return fmt.Errorf("get %s: %w", row.Path, err)
	}
	adopted, refuseReason, err := m.parseAdoptableTransformation(ctx, content, t)
	if err != nil {
		return err
	}
	if adopted == nil {
		m.refuseAdoption(ctx, t, row.Path, treeSHA, refuseReason)
		return nil
	}

	adopted.UpdatedAt = time.Now()
	if err := m.Transformations.UpdateTransformation(ctx, adopted); err != nil {
		return fmt.Errorf("adopt %s: %w", row.Path, err)
	}
	m.recordDefinitionRender(ctx, t.ID, row.Path, treeSHA)
	m.notifyAdopted(ctx, t.CustomerSlug, row.Path)
	return nil
}

// parseAdoptableTransformation strictly parses a foreign edit against the
// current row. It returns the transformation to store, or nil with a
// user-facing reason the edit cannot be adopted. A trigger pipeline lookup
// failure other than not-found is a real error and aborts the pass.
func (m *TransformationMirror) parseAdoptableTransformation(ctx context.Context, content string, current *Transformation) (*Transformation, string, error) {
	var f transformationFile
	if err := yaml.Unmarshal([]byte(content), &f); err != nil {
		return nil, "the file is not valid YAML", nil
	}
	if f.ID != string(current.ID) {
		return nil, "the file's id field does not match this transformation", nil
	}
	trigger := PipelineID(f.TriggerAfterPipeline)
	if trigger != "" {
		p, err := m.Pipelines.GetPipeline(ctx, trigger)
		if errors.Is(err, ErrPipelineNotFound) {
			return nil, "the file's trigger_after_pipeline does not exist", nil
		}
		if err != nil {
			return nil, "", fmt.Errorf("get trigger pipeline: %w", err)
		}
		if p.CustomerSlug != current.CustomerSlug {
			return nil, "the file's trigger_after_pipeline does not exist", nil
		}
	}
	adopted := &Transformation{
		ID:                     current.ID,
		CustomerSlug:           current.CustomerSlug,
		Name:                   f.Name,
		RepoURL:                f.RepoURL,
		RepoRef:                f.RepoRef,
		GitCredentials:         current.GitCredentials,
		Schedule:               f.Schedule,
		TriggerAfterPipelineID: trigger,
		DBTSelector:            f.DBTSelector,
		Enabled:                f.Enabled,
		CreatedAt:              current.CreatedAt,
	}
	if adopted.RepoRef == "" {
		adopted.RepoRef = "main"
	}
	return adopted, "", nil
}

// refuseAdoption records the refused sha (once-per-commit notification
// guard) and tells the user their edit could not be taken over.
func (m *TransformationMirror) refuseAdoption(ctx context.Context, t *Transformation, filePath, treeSHA, reason string) {
	if err := m.DefinitionRenders.MarkTransformationDefinitionRefused(ctx, t.ID, treeSHA); err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "mark transformation refused", "transformation", t.ID, "err", err)
		}
		return
	}
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: t.CustomerSlug,
		Type:         "info",
		Title:        "Transformation file edit could not be applied",
		Body:         fmt.Sprintf("%s was edited outside the Console, but %s. The Console keeps its current configuration; fix the file or save the transformation in the Console to overwrite it.", filePath, reason),
		Link:         "transformations",
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify refused transformation adoption", "transformation", t.ID, "err", err)
	}
}

// notifyAdopted raises the "updated from git" notification. Best-effort.
func (m *TransformationMirror) notifyAdopted(ctx context.Context, customerSlug, filePath string) {
	if m.Notifications == nil {
		return
	}
	n := Notification{
		CustomerSlug: customerSlug,
		Type:         "info",
		Title:        "Transformation updated from your repo",
		Body:         fmt.Sprintf("%s was edited outside the Console; the change has been applied to the transformation.", filePath),
		Link:         "transformations",
	}
	if err := m.Notifications.Notify(ctx, n); err != nil && m.Logger != nil {
		m.Logger.WarnContext(ctx, "notify adopted transformation", "customer", customerSlug, "err", err)
	}
}
