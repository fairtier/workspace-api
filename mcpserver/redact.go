package mcpserver

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
)

// Secret hygiene for what an agent is shown. An agent's context window is not
// a safe place: it is sent to a model provider, logged by the client, and
// readable by any later prompt-injected instruction. So the tools hold back
// what has no business there even though the person behind the token could
// see it in the Console.

// Output caps. An agent pays for every byte in its context, and an unbounded
// result is how a single call blows it.
const (
	maxFileBytes        = 256 << 10 // read_repo_file
	maxWriteBytes       = 1 << 20   // write_repo_file
	maxRowBytes         = 256 << 10 // run_query, all rows together
	maxErrorMessageLen  = 2000      // a run's error message
	maxNotificationBody = 1000
)

// secretPathError explains a denied path without echoing more than the name.
func secretPathError(p string) error {
	return fmt.Errorf("%s looks like a secrets file; it is not available over MCP", p)
}

// isSecretPath reports whether a repository path looks like it holds
// credentials. It is a denylist on top of the repo editor's own rules (which
// already hide dotfiles and platform-managed files), applied to reads, writes
// and listings alike: an agent has no reason to read a key and no business
// writing one.
func isSecretPath(p string) bool {
	base := strings.ToLower(path.Base(p))
	if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "secrets.") || strings.HasPrefix(base, "credentials") {
		return true
	}
	switch base {
	case "profiles.yml", "profiles.yaml", "id_rsa", "id_ed25519", "id_ecdsa", "service_account.json", "service-account.json":
		return true
	}
	switch path.Ext(base) {
	case ".age", ".pem", ".key", ".p12", ".pfx", ".jks", ".keystore", ".env", ".kdbx", ".gpg", ".asc":
		return true
	}
	// Any segment that is a dot directory (.dlt/secrets.toml, .ssh/…).
	for seg := range strings.SplitSeq(strings.ToLower(p), "/") {
		if strings.HasPrefix(seg, ".") && seg != "." && seg != ".." {
			return true
		}
	}
	return false
}

// secretKeyRE matches config keys whose value is a credential by name.
var secretKeyRE = regexp.MustCompile(`(?i)(pass(word|wd)?|secret|token|api[_-]?key|apikey|private[_-]?key|credential|auth(orization)?$|bearer|connection[_-]?string|dsn|access[_-]?key|client[_-]?secret|cookie|session)`)

const redacted = "[redacted]"

// redactJSON parses raw JSON and returns it with secret-named keys replaced
// and credentials stripped from URLs in string values. Pipeline source_config
// is non-secret by contract — credentials live in the encrypted
// source_credentials, which no tool returns — but a person can still paste
// a token into a header or query parameter of a REST source's config, and
// this is the last place to catch it. Unparseable input is replaced whole.
func redactJSON(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return redacted
	}
	return redactValue(v)
}

func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if secretKeyRE.MatchString(k) && !isEmptyValue(val) {
				t[k] = redacted
				continue
			}
			t[k] = redactValue(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = redactValue(t[i])
		}
		return t
	case string:
		return redactString(t)
	default:
		return v
	}
}

func isEmptyValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case bool:
		return true // a flag ("use_token": true) is not a secret
	default:
		return false
	}
}

// urlUserinfoRE matches the userinfo of a URL (scheme://user:pass@host).
var urlUserinfoRE = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]+@`)

// redactString strips URL credentials from free text (an error message, a
// repository URL, a config value).
func redactString(s string) string {
	return urlUserinfoRE.ReplaceAllString(s, "${1}"+redacted+"@")
}

// cutURLRE matches a URL that was cut before its host — at the end of a
// server-shortened string ("…"), where the "@" that urlUserinfoRE needs may
// have been cut off with the rest.
var cutURLRE = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]*…$`)

// redactCutURL redacts what may be a credential at the cut end of a string
// the server already shortened (Workflow.last_run_error).
func redactCutURL(s string) string {
	return cutURLRE.ReplaceAllString(s, "${1}"+redacted+"…")
}

// truncate caps s at n bytes on a rune boundary, marking the cut.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "… [truncated]"
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
