package workspace

import "strings"

// tracebackHeader opens a Python traceback; the indented lines after it are
// frames, and the first unindented one is the exception.
const tracebackHeader = "Traceback (most recent call last):"

// ErrorSummary returns the one line of a possibly multi-line error message
// that names its cause, trimmed and cut to at most n runes (marked with "…").
//
// It is the first non-empty line that is not merely an introduction:
// lines ending in ":" (dlt's "… with exception:", Python's traceback header
// and "The above exception was the direct cause of the following
// exception:"), a bare exception class repr ("<class '…'>"), and the
// indented frame lines of a traceback are skipped. A chained Python
// traceback prints the innermost exception first, so that is the one taken.
// When every line is skipped the first non-empty one is used.
//
// A single line that is not skipped maps to itself, so summarising a
// summary changes nothing. Callers that redact do so before calling: a cut
// must not split a credential from what identifies it.
func ErrorSummary(s string, n int) string {
	first := ""
	inTraceback := false
	for raw := range strings.SplitSeq(s, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if first == "" {
			first = line
		}
		if inTraceback && (raw[0] == ' ' || raw[0] == '\t') {
			continue // a frame
		}
		inTraceback = line == tracebackHeader
		if strings.HasSuffix(line, ":") || isClassRepr(line) {
			continue
		}
		return cutRunes(line, n)
	}
	return cutRunes(first, n)
}

// isClassRepr reports whether line is only an exception class, as dlt
// prints it between its preamble and the message: "<class 'pkg.Error'>".
func isClassRepr(line string) bool {
	return strings.HasPrefix(line, "<class '") && strings.HasSuffix(line, "'>")
}

// cutRunes cuts s to at most n runes, the last one "…" when cut.
func cutRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
