package server

import (
	"fmt"
	"testing"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/workspace"
)

func TestBoxRepoErrorCodes(t *testing.T) {
	for err, want := range map[error]connect.Code{
		fmt.Errorf("%w: gitea GET …: status 404", workspace.ErrRepoFileNotFound): connect.CodeNotFound,
		workspace.ErrRepoFileChanged:          connect.CodeAborted,
		workspace.ErrBoxRepoUnavailable:       connect.CodeFailedPrecondition,
		fmt.Errorf("gitea GET …: status 500"): connect.CodeInternal,
	} {
		if got := connect.CodeOf(boxRepoError(err)); got != want {
			t.Errorf("boxRepoError(%v) = %v, want %v", err, got, want)
		}
	}
}
