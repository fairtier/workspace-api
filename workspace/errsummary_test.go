package workspace_test

import (
	"strings"
	"testing"

	"github.com/fairtier/workspace-api/workspace"
)

// dltStepFailed is the error_message a real dlt-worker reported for a
// rest_api source answering 400: dlt's preamble first, the cause below it.
const dltStepFailed = "Pipeline execution failed at `step=extract` when processing package with `load_id=1794624568.341378` with exception:\n\n" +
	"<class 'dlt.extract.exceptions.ResourceExtractionError'>\n" +
	"In processing pipe `badsource`: extraction of resource `badsource` in `generator` `paginate_resource` caused an exception: 400 Client Error: Bad Request for url: http://127.0.0.1:8000/fail"

const pyTraceback = `Traceback (most recent call last):
  File "/app/run.py", line 3, in <module>
    connect()
  File "/app/db.py", line 9, in connect
    raise OSError("connection refused")
OSError: connection refused

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/app/run.py", line 5, in <module>
    raise RuntimeError("load failed") from e
RuntimeError: load failed`

func TestErrorSummary(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"empty", "", ""},
		{"one line", "boom", "boom"},
		{"message then traceback", "\n  boom  \nTraceback (most recent call last):\n  File \"x\", line 1\nValueError: boom", "boom"},
		{"dlt preamble", dltStepFailed, "In processing pipe `badsource`: extraction of resource `badsource` in `generator` `paginate_resource` caused an exception: 400 Client Error: Bad Request for url: http://127.0.0.1:8000/fail"},
		{"traceback only: innermost exception", pyTraceback, "OSError: connection refused"},
		{"only preamble falls back to it", "Pipeline failed with exception:\n\n", "Pipeline failed with exception:"},
		{"cut", strings.Repeat("é", 205), strings.Repeat("é", 199) + "…"},
	} {
		if got := workspace.ErrorSummary(tc.in, 200); got != tc.want {
			t.Errorf("%s: ErrorSummary = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Summarising a summary changes nothing, so a caller can prefix one and
// summarise again (the MCP status does).
func TestErrorSummary_Idempotent(t *testing.T) {
	for _, in := range []string{dltStepFailed, pyTraceback, "boom", "only a header:"} {
		once := workspace.ErrorSummary(in, 200)
		if twice := workspace.ErrorSummary(once, 200); twice != once {
			t.Errorf("ErrorSummary(%q) = %q, then %q", in, once, twice)
		}
	}
}
