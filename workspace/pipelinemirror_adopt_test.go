package workspace_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/fairtier/workspace-api/workspace"
)

// fakeOwnershipStore records credential-ownership flips.
type fakeOwnershipStore struct {
	external map[workspace.PipelineID]bool
	dropped  []workspace.PipelineID
}

func newFakeOwnershipStore() *fakeOwnershipStore {
	return &fakeOwnershipStore{external: map[workspace.PipelineID]bool{}}
}

func (f *fakeOwnershipStore) SetPipelineCredentialsExternal(_ context.Context, id workspace.PipelineID, external bool) error {
	f.external[id] = external
	return nil
}

func (f *fakeOwnershipStore) DeletePipelineCredentialRender(_ context.Context, id workspace.PipelineID) error {
	f.dropped = append(f.dropped, id)
	return nil
}

// cancellingLister cancels the surrounding context from inside its first
// ListVMWorkspaceSlugs call, so AdoptSweeper.Run exits after exactly one pass.
type cancellingLister struct {
	cancel context.CancelFunc
	calls  int
}

func (l *cancellingLister) ListVMWorkspaceSlugs(context.Context) ([]string, error) {
	l.calls++
	l.cancel()
	return nil, nil
}

// TestAdoptSweeperSweepsImmediately guards the first-pass behavior: Run must
// sweep before waiting on the ticker (a one-hour interval would otherwise
// time the test out), matching StuckRunSweeper.Run.
func TestAdoptSweeperSweepsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lister := &cancellingLister{cancel: cancel}
	s := &workspace.AdoptSweeper{
		Workspaces: lister,
		Logger:     slog.New(slog.DiscardHandler),
	}

	s.Run(ctx, time.Hour)

	if lister.calls != 1 {
		t.Errorf("ListVMWorkspaceSlugs calls = %d, want 1 immediate sweep", lister.calls)
	}
}

func TestPipelineMirror_AdoptCustomer(t *testing.T) {
	pipe := workspace.Pipeline{
		ID:           "11111111-aaaa",
		Name:         "Orders",
		SourceType:   "rest_api",
		SourceConfig: json.RawMessage(`{"base_url":"https://example.com","resources":[{"name":"users","endpoint":"/users"}]}`),
		DatasetName:  "raw",
		Enabled:      true,
	}
	const path = "pipelines/orders.yaml"

	// setup renders the initial state, then rewires the pipeline repo mock so
	// the adopt pass can read/update the row.
	setup := func(t *testing.T) (*fakeMirrorRepo, *fakeDefRenderStore, *fakeNotifier, *fakeOwnershipStore, *workspace.PipelineMirror, *[]workspace.Pipeline) {
		t.Helper()
		repo := newFakeMirrorRepo(nil)
		store := newFakeDefRenderStore()
		notifier := &fakeNotifier{}
		ownership := newFakeOwnershipStore()
		m := mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe})
		m.DefinitionRenders = store
		m.Notifications = notifier
		m.Ownership = ownership
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		notifier.notes = nil

		var updated []workspace.Pipeline
		current := pipe
		m.Pipelines = &mockPipelineRepo{
			listPipelinesByCustomerFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return []workspace.Pipeline{current}, nil
			},
			getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
				c := current
				return &c, nil
			},
			updatePipelineFn: func(_ context.Context, p *workspace.Pipeline) error {
				updated = append(updated, *p)
				return nil
			},
		}
		return repo, store, notifier, ownership, m, &updated
	}

	t.Run("valid foreign edit is adopted into the cache", func(t *testing.T) {
		repo, store, notifier, _, m, updated := setup(t)
		edited := strings.Replace(repo.files[path], "schedule: \"\"", `schedule: "0 6 * * *"`, 1)
		edited = strings.Replace(edited, "enabled: true", "enabled: true", 1)
		repo.tamper(path, edited)
		tamperedSHA := repo.shas[path]

		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if len(*updated) != 1 {
			t.Fatalf("updates = %d, want 1 (adoption writes the cache row)", len(*updated))
		}
		if repo.files[path] != edited {
			t.Fatal("adopt pass must never write the repo")
		}
		if store.rows[pipe.ID].BlobSHA != tamperedSHA {
			t.Fatalf("render row must adopt the foreign sha, got %q want %q", store.rows[pipe.ID].BlobSHA, tamperedSHA)
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "updated from your repo") {
			t.Fatalf("want one 'updated from repo' notification, got %+v", notifier.notes)
		}
	})

	t.Run("unparseable foreign edit is refused once", func(t *testing.T) {
		repo, store, notifier, _, m, updated := setup(t)
		repo.tamper(path, "{{{ not yaml")
		tamperedSHA := repo.shas[path]

		for range 2 { // second pass must not re-notify
			if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
				t.Fatal(err)
			}
		}
		if len(*updated) != 0 {
			t.Fatal("refused edit must not touch the cache")
		}
		if repo.files[path] != "{{{ not yaml" {
			t.Fatal("refused edit must stay in the repo untouched")
		}
		if store.rows[pipe.ID].RefusedBlobSHA != tamperedSHA {
			t.Fatalf("refused sha not recorded: %+v", store.rows[pipe.ID])
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "could not be applied") {
			t.Fatalf("want exactly one refusal notification, got %+v", notifier.notes)
		}
	})

	t.Run("source-type change is refused", func(t *testing.T) {
		repo, _, notifier, _, m, updated := setup(t)
		edited := strings.Replace(repo.files[path], "source_type: rest_api", "source_type: filesystem", 1)
		repo.tamper(path, edited)

		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if len(*updated) != 0 {
			t.Fatal("a type-changing edit must not be adopted")
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Body, "source type") {
			t.Fatalf("want a source-type refusal, got %+v", notifier.notes)
		}
	})

	t.Run("foreign age edit flips credentials to externally-managed", func(t *testing.T) {
		repo := newFakeMirrorRepo(nil)
		store := newFakeDefRenderStore()
		notifier := &fakeNotifier{}
		ownership := newFakeOwnershipStore()
		renders := &fakeRenderStore{}
		m := withAge(mirrorFor(boxCustomer(), repo, []workspace.Pipeline{pipe}),
			testRecipient(t), fakeCredReader{pipe.ID: json.RawMessage(`{"api_key":"s3cr3t"}`)}, renders)
		m.DefinitionRenders = store
		m.Notifications = notifier
		m.Ownership = ownership
		if err := m.SyncCustomer(context.Background(), "acme", nil); err != nil {
			t.Fatal(err)
		}
		notifier.notes = nil
		m.Pipelines = &mockPipelineRepo{
			listPipelinesByCustomerFn: func(context.Context, string) ([]workspace.Pipeline, error) {
				return []workspace.Pipeline{pipe}, nil
			},
			getPipelineFn: func(context.Context, workspace.PipelineID) (*workspace.Pipeline, error) {
				c := pipe
				return &c, nil
			},
		}

		const agePath = "pipelines/orders.credentials.age"
		repo.tamper(agePath, "-----BEGIN AGE ENCRYPTED FILE----- box-managed -----END AGE ENCRYPTED FILE-----")
		if err := m.AdoptCustomer(context.Background(), "acme"); err != nil {
			t.Fatal(err)
		}
		if !ownership.external[pipe.ID] {
			t.Fatal("foreign .age commit must flip the pipeline to externally-managed")
		}
		if len(notifier.notes) != 1 || !strings.Contains(notifier.notes[0].Title, "managed in your repo") {
			t.Fatalf("want the externally-managed notification, got %+v", notifier.notes)
		}
	})
}

// testRecipient returns a throwaway age recipient (the tests never decrypt).
func testRecipient(t *testing.T) string {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	return identity.Recipient().String()
}
