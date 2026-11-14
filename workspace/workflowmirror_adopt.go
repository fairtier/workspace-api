package workspace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// The workflow adopt pass: the pull half of workflows-as-files. Strictly
// read-only toward the repo; it writes only rows.
//
// Per workflows/*.yaml file:
//   - a rendered file with a foreign commit (blob sha differs from the last
//     render) → parsed strictly, its step keys resolved, and validated with
//     the API's own validator; valid → the row adopts it ("updated from your
//     repo"), anything else → the row is left alone and the user is notified
//     once per commit. The next Console save still overwrites, as for
//     transformations.
//   - a rendered file that moved (a file elsewhere carries its id, the old
//     path is gone) → a rename: adopted like an edit, at the new path.
//   - a file with no render row and an id this database does not know, or no
//     id → a new workflow (Importer only, i.e. on the box): created with the
//     file's id, or a fresh one for a hand-written file without one. Refused
//     files are reported once per commit.
//   - a rendered file that is gone and was not moved → the workflow is
//     deleted (Importer only). Never on an empty repo listing, which is a
//     reset or unreadable repo rather than anyone's intent.
//
// Additions and deletions go further than the pipeline and transformation
// passes, which leave them to the Console. A workflow is pure configuration —
// no credentials, no data, only references — so on the box, where the repo
// is the source of truth, a file in git is allowed to be the whole of it.
// Deleting the file deletes the workflow's run history with it, exactly as
// DeleteWorkflow does.
//
// Loop safety: every write the mirror makes, and every commit this pass
// adopts, is recorded as the row's blob sha, so the next pass sees nothing
// foreign. A re-render after an adoption (the file normalised to the
// rendered form) is the mirror's own write and raises no drift.

// AdoptCustomer runs one workflow adopt pass for a customer. Run it after the
// pipeline and transformation passes of the same sweep: a step can only
// resolve against targets that exist.
func (m *WorkflowMirror) AdoptCustomer(ctx context.Context, customerSlug string) (err error) {
	ctx, span := tracer.Start(ctx, "WorkflowMirror.AdoptCustomer", trace.WithAttributes(
		attrSlug.String(customerSlug),
		attrRepoPlane.String(planeWorkflows),
	))
	defer func() { endSpan(span, err) }()

	client, _, ok, err := boxMirrorClientFor(ctx, m.Workspaces, m.Credentials, m.NewClient, customerSlug, m.Logger)
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
	span.SetAttributes(
		attribute.Int("workspace.repo.definition_files", len(st.yamlTree)),
		attribute.Int("workspace.workflows", len(st.workflows)),
	)

	moved, err := m.adoptUntracked(ctx, client, customerSlug, st)
	if err != nil {
		return err
	}
	for i := range st.workflows {
		w := &st.workflows[i]
		if moved[w.ID] {
			continue
		}
		if err := m.adoptTracked(ctx, client, w, st); err != nil {
			return err
		}
	}
	return nil
}

// workflowAdoptState is the snapshot one adopt pass works from.
type workflowAdoptState struct {
	workflows []Workflow
	byID      map[WorkflowID]*Workflow
	renders   map[WorkflowID]WorkflowDefinitionRender
	tracked   map[string]WorkflowID // render path → workflow
	yamlTree  map[string]string
	// repoEmpty is a listing with no files at all: a reset or unreadable
	// repo, never a reason to delete anything.
	repoEmpty bool
	keys      *workflowTargetKeys
}

func (m *WorkflowMirror) loadAdoptState(ctx context.Context, client RepoFileClient, customerSlug string) (*workflowAdoptState, error) {
	tree, err := client.ListTree(ctx, transformationsRepo)
	if err != nil {
		return nil, fmt.Errorf("list %s tree: %w", transformationsRepo, err)
	}
	st := &workflowAdoptState{yamlTree: workflowTreeYAML(tree), repoEmpty: len(tree) == 0}
	if st.keys, err = m.loadTargetKeys(ctx, customerSlug); err != nil {
		return nil, err
	}
	if st.workflows, err = m.Workflows.ListWorkflowsByCustomer(ctx, customerSlug); err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	if st.renders, err = m.DefinitionRenders.GetWorkflowDefinitionRenders(ctx, customerSlug); err != nil {
		return nil, fmt.Errorf("get definition renders: %w", err)
	}
	st.byID = make(map[WorkflowID]*Workflow, len(st.workflows))
	for i := range st.workflows {
		st.byID[st.workflows[i].ID] = &st.workflows[i]
	}
	st.tracked = make(map[string]WorkflowID, len(st.renders))
	for id, r := range st.renders {
		st.tracked[r.Path] = id
	}
	return st, nil
}

// adoptUntracked handles every file no render row points at: renames of
// rendered files, and (box only) new workflows. It returns the workflows it
// adopted at a new path, which the tracked pass must not treat as deleted.
func (m *WorkflowMirror) adoptUntracked(ctx context.Context, client RepoFileClient, customerSlug string, st *workflowAdoptState) (map[WorkflowID]bool, error) {
	moved := map[WorkflowID]bool{}
	for _, filePath := range sortedPaths(st.yamlTree) {
		if _, ok := st.tracked[filePath]; ok {
			continue
		}
		if err := m.adoptUntrackedFile(ctx, client, customerSlug, filePath, st, moved); err != nil {
			return nil, err
		}
	}
	return moved, nil
}

// adoptUntrackedFile handles one file no render row points at, adding the
// workflow to moved when it adopts a rename.
func (m *WorkflowMirror) adoptUntrackedFile(ctx context.Context, client RepoFileClient, customerSlug, filePath string, st *workflowAdoptState, moved map[WorkflowID]bool) error {
	blobSHA := st.yamlTree[filePath]
	content, _, err := client.GetContents(ctx, transformationsRepo, filePath)
	if err != nil {
		return fmt.Errorf("get %s: %w", filePath, err)
	}
	f, reason := parseWorkflowFile(content)
	if f == nil {
		if m.Importer != nil {
			m.refuseFile(ctx, customerSlug, filePath, blobSHA, reason)
		}
		return nil
	}
	if w, known := st.byID[WorkflowID(f.ID)]; known {
		ok, err := m.adoptKnownID(ctx, customerSlug, filePath, blobSHA, f, w, st)
		if ok {
			moved[w.ID] = true
		}
		return err
	}
	if m.Importer == nil {
		return nil
	}
	return m.importFile(ctx, customerSlug, filePath, blobSHA, f, st)
}

// adoptKnownID handles an untracked file carrying the id of an existing
// workflow: a rename when that workflow's rendered file is gone, a duplicate
// when it is still there. It reports whether it adopted a rename.
func (m *WorkflowMirror) adoptKnownID(ctx context.Context, customerSlug, filePath, blobSHA string, f *workflowFile, w *Workflow, st *workflowAdoptState) (bool, error) {
	row, tracked := st.renders[w.ID]
	if !tracked {
		// A workflow never rendered here: the next converge writes its file
		// and collects this copy (its id is ours).
		return false, nil
	}
	if _, stillThere := st.yamlTree[row.Path]; stillThere {
		m.refuseFile(ctx, customerSlug, filePath, blobSHA,
			fmt.Sprintf("its id is the id of the workflow %q, which already has its file at %s; remove the id line to create a new workflow from this file", w.Name, row.Path))
		return false, nil
	}
	adopted, reason, err := m.workflowFromFile(ctx, f, w.ID, customerSlug, st.keys)
	if err != nil {
		return false, err
	}
	if adopted == nil {
		m.refuseFile(ctx, customerSlug, filePath, blobSHA, reason)
		return true, nil // moved, if not valid: it must not read as deleted
	}
	if reason, err := m.storeAdopted(ctx, adopted, w); err != nil || reason != "" {
		if reason != "" {
			m.refuseFile(ctx, customerSlug, filePath, blobSHA, reason)
		}
		return true, err
	}
	recordAdoption(ctx, planeWorkflows, outcomeAdopted, attrRepoPath.String(filePath), attrWorkflowID.String(string(w.ID)))
	m.recordDefinitionRender(ctx, w.ID, filePath, blobSHA)
	// A second copy later in this pass is then a duplicate, not a rename.
	st.renders[w.ID] = WorkflowDefinitionRender{WorkflowID: w.ID, Path: filePath, BlobSHA: blobSHA}
	m.notify(ctx, customerSlug, "Workflow updated from your repo",
		fmt.Sprintf("%s was moved to %s outside the Console; the workflow now follows the new file.", row.Path, filePath),
		workflowLink(w.ID))
	return true, nil
}

// importFile creates a workflow from a new file (box only).
func (m *WorkflowMirror) importFile(ctx context.Context, customerSlug, filePath, blobSHA string, f *workflowFile, st *workflowAdoptState) error {
	id := WorkflowID(f.ID)
	if id == "" {
		id = WorkflowID(uuid.NewString())
	} else if uuid.Validate(f.ID) != nil {
		m.refuseFile(ctx, customerSlug, filePath, blobSHA, "its id is not a UUID (leave the id line out of a new workflow; one is assigned)")
		return nil
	}
	w, reason, err := m.workflowFromFile(ctx, f, id, customerSlug, st.keys)
	if err != nil {
		return err
	}
	if w == nil {
		m.refuseFile(ctx, customerSlug, filePath, blobSHA, reason)
		return nil
	}
	w.CreatedAt = time.Now()
	w.UpdatedAt = w.CreatedAt
	if err := m.Importer.ImportWorkflow(ctx, w); err != nil {
		if errors.Is(err, ErrWorkflowAlreadyExists) {
			m.refuseFile(ctx, customerSlug, filePath, blobSHA, "another workflow already uses that name")
			return nil
		}
		return fmt.Errorf("import %s: %w", filePath, err)
	}
	recordAdoption(ctx, planeWorkflows, outcomeImported, attrRepoPath.String(filePath), attrWorkflowID.String(string(id)))
	m.recordDefinitionRender(ctx, id, filePath, blobSHA)
	st.byID[id] = w
	m.notify(ctx, customerSlug, "Workflow added from your repo",
		fmt.Sprintf("%s was added outside the Console; the workflow %q has been created from it.", filePath, w.Name),
		workflowLink(id))
	return nil
}

// adoptTracked inspects one rendered workflow's file for a foreign commit or
// a deletion.
func (m *WorkflowMirror) adoptTracked(ctx context.Context, client RepoFileClient, w *Workflow, st *workflowAdoptState) error {
	row, tracked := st.renders[w.ID]
	if !tracked {
		return nil // never rendered — the next converge bootstraps it
	}
	treeSHA, inTree := st.yamlTree[row.Path]
	if !inTree {
		return m.adoptDeletion(ctx, w, row.Path, st)
	}
	if treeSHA == row.BlobSHA || treeSHA == row.RefusedBlobSHA {
		return nil
	}
	content, _, err := client.GetContents(ctx, transformationsRepo, row.Path)
	if err != nil {
		return fmt.Errorf("get %s: %w", row.Path, err)
	}
	reason, err := m.applyEdit(ctx, content, w, st)
	if err != nil {
		return err
	}
	if reason != "" {
		m.refuseTracked(ctx, w, row.Path, treeSHA, reason)
		return nil
	}
	recordAdoption(ctx, planeWorkflows, outcomeAdopted, attrRepoPath.String(row.Path), attrWorkflowID.String(string(w.ID)))
	m.recordDefinitionRender(ctx, w.ID, row.Path, treeSHA)
	m.notify(ctx, w.CustomerSlug, "Workflow updated from your repo",
		fmt.Sprintf("%s was edited outside the Console; the change has been applied to the workflow.", row.Path),
		workflowLink(w.ID))
	return nil
}

// applyEdit parses, resolves and validates a rendered file's new content and
// stores it over w. It returns the user-facing reason when the edit cannot
// be taken.
func (m *WorkflowMirror) applyEdit(ctx context.Context, content string, w *Workflow, st *workflowAdoptState) (string, error) {
	f, reason := parseWorkflowFile(content)
	if f == nil {
		return reason, nil
	}
	if f.ID != "" && f.ID != string(w.ID) {
		return "the file's id field does not match this workflow", nil
	}
	adopted, reason, err := m.workflowFromFile(ctx, f, w.ID, w.CustomerSlug, st.keys)
	if err != nil || adopted == nil {
		return reason, err
	}
	return m.storeAdopted(ctx, adopted, w)
}

// storeAdopted writes an adopted definition over the current row. A name
// another workflow holds is a refusal, not an error.
func (m *WorkflowMirror) storeAdopted(ctx context.Context, adopted, current *Workflow) (string, error) {
	adopted.CreatedAt = current.CreatedAt
	adopted.UpdatedAt = time.Now()
	err := m.Workflows.UpdateWorkflow(ctx, adopted)
	if errors.Is(err, ErrWorkflowAlreadyExists) {
		return "another workflow already uses that name", nil
	}
	if err != nil {
		return "", fmt.Errorf("adopt workflow %s: %w", current.ID, err)
	}
	return "", nil
}

// adoptDeletion deletes a workflow whose rendered file was removed from the
// repo (box only, never on an empty listing).
func (m *WorkflowMirror) adoptDeletion(ctx context.Context, w *Workflow, filePath string, st *workflowAdoptState) error {
	if m.Importer == nil || st.repoEmpty {
		return nil
	}
	if err := m.Workflows.DeleteWorkflow(ctx, w.ID); err != nil {
		if errors.Is(err, ErrWorkflowNotFound) {
			return nil
		}
		return fmt.Errorf("adopt deletion of %s: %w", filePath, err)
	}
	recordAdoption(ctx, planeWorkflows, outcomeDeleted, attrRepoPath.String(filePath), attrWorkflowID.String(string(w.ID)))
	m.notify(ctx, w.CustomerSlug, "Workflow deleted from your repo",
		fmt.Sprintf("%s was removed outside the Console, so the workflow %q and its run history have been deleted. Restore the file from git history to bring the definition back.", filePath, w.Name),
		"/workflows")
	return nil
}

// refuseTracked records the refused sha on the render row (once-per-commit
// guard) and tells the user.
func (m *WorkflowMirror) refuseTracked(ctx context.Context, w *Workflow, filePath, treeSHA, reason string) {
	if err := m.DefinitionRenders.MarkWorkflowDefinitionRefused(ctx, w.ID, treeSHA); err != nil {
		if m.Logger != nil {
			m.Logger.WarnContext(ctx, "mark workflow refused", "workflow", w.ID, "err", err)
		}
		return
	}
	recordAdoption(ctx, planeWorkflows, outcomeRefused,
		attrRepoPath.String(filePath), attrWorkflowID.String(string(w.ID)), attrReason.String(reason))
	m.notify(ctx, w.CustomerSlug, "Workflow file edit could not be applied",
		fmt.Sprintf("%s was edited outside the Console, but %s. The Console keeps its current configuration; fix the file or save the workflow in the Console to overwrite it.", filePath, reason),
		workflowLink(w.ID))
}

// refuseFile reports a file that has no render row to carry a refused sha:
// once per (path, blob sha) for the life of the process.
func (m *WorkflowMirror) refuseFile(ctx context.Context, customerSlug, filePath, blobSHA, reason string) {
	if !m.skips.firstReport(filePath, blobSHA) {
		return
	}
	recordAdoption(ctx, planeWorkflows, outcomeRefused, attrRepoPath.String(filePath), attrReason.String(reason))
	if m.Logger != nil {
		m.Logger.WarnContext(ctx, "workflow file not applied", "path", filePath, "reason", reason)
	}
	m.notify(ctx, customerSlug, "Workflow file could not be applied",
		fmt.Sprintf("%s in your workspace repo could not be applied: %s. Nothing was changed; fix the file and commit again.", filePath, reason),
		"/workflows")
}

func workflowLink(id WorkflowID) string { return "/workflows?workflow=" + string(id) }
