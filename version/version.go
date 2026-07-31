// Package version reports the build-time identity of the running binary.
package version

import "runtime/debug"

// Binary returns the VCS revision the binary was built from, as recorded
// by `go build` in runtime/debug.BuildInfo when the build root is inside
// a git checkout. Returns "dev" when unavailable (e.g. `go run` from a
// dirty tree, or builds with -buildvcs=false).
//
// The customer module revision is NOT derived from this — it's supplied
// at runtime via the MODULE_REVISION env var (mounted from a ConfigMap
// updated by CI). Binary identity and module identity are decoupled.
func Binary() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" {
			return s.Value
		}
	}
	return "dev"
}
