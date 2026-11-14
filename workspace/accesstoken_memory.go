package workspace

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// MemoryAccessTokenStore is an in-process AccessTokenStore for tests (in this
// module and in embedders') and for throwaway local runs. Tokens do not
// survive a restart; production uses the postgres store.
type MemoryAccessTokenStore struct {
	mu     sync.Mutex
	tokens []AccessToken
	seq    int
}

var _ AccessTokenStore = (*MemoryAccessTokenStore)(nil)

func (m *MemoryAccessTokenStore) CreateAccessToken(_ context.Context, t *AccessToken) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	t.ID = fmt.Sprintf("00000000-0000-4000-8000-%012d", m.seq)
	t.CreatedAt = time.Now().UTC().Add(time.Duration(m.seq) * time.Microsecond)
	m.tokens = append(m.tokens, cloneAccessToken(*t))
	return nil
}

func (m *MemoryAccessTokenStore) ListAccessTokens(_ context.Context, customerSlug string, userSub core.UserID) ([]AccessToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []AccessToken
	for _, t := range slices.Backward(m.tokens) {
		if t.CustomerSlug == customerSlug && t.UserSub == userSub {
			out = append(out, cloneAccessToken(t))
		}
	}
	return out, nil
}

func (m *MemoryAccessTokenStore) GetAccessTokenByHash(_ context.Context, hash []byte) (*AccessToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.tokens {
		if bytes.Equal(t.Hash, hash) {
			c := cloneAccessToken(t)
			return &c, nil
		}
	}
	return nil, ErrAccessTokenNotFound
}

func (m *MemoryAccessTokenStore) RevokeAccessToken(_ context.Context, customerSlug string, userSub core.UserID, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.tokens {
		t := &m.tokens[i]
		if t.ID != id || t.CustomerSlug != customerSlug || t.UserSub != userSub {
			continue
		}
		if t.RevokedAt == nil {
			t.RevokedAt = &at
		}
		return nil
	}
	return ErrAccessTokenNotFound
}

func (m *MemoryAccessTokenStore) TouchAccessToken(_ context.Context, id string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.tokens {
		if m.tokens[i].ID == id {
			m.tokens[i].LastUsedAt = &at
		}
	}
	return nil
}

func cloneAccessToken(t AccessToken) AccessToken {
	t.Scopes = slices.Clone(t.Scopes)
	t.Hash = bytes.Clone(t.Hash)
	return t
}
