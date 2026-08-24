package rillskills

import (
	"strings"
	"testing"
)

func TestReference(t *testing.T) {
	t.Run("full budget keeps all sections", func(t *testing.T) {
		ref, dropped := Reference(1 << 20)
		if len(dropped) != 0 {
			t.Fatalf("nothing should drop at a huge budget, dropped %v", dropped)
		}
		for _, want := range []string{"metrics view", "explore dashboard", "model"} {
			if !strings.Contains(strings.ToLower(ref), want) {
				t.Errorf("reference missing %q", want)
			}
		}
	})

	t.Run("tight budget drops whole trailing sections, never truncates", func(t *testing.T) {
		full, _ := Reference(1 << 20)
		small, dropped := Reference(len(full) - 1)
		if len(dropped) == 0 {
			t.Fatal("expected at least one dropped section")
		}
		if dropped[0] != "explore" {
			t.Errorf("lowest-priority section should drop first, got %v", dropped)
		}
		if len(small) >= len(full) {
			t.Error("smaller budget did not shrink the reference")
		}
		// Whatever remains must be whole documents: the kept prefix of the
		// priority order, concatenated exactly as Reference builds it.
		kept := sections()[:len(sections())-len(dropped)]
		var want strings.Builder
		for _, s := range kept {
			want.WriteString(s.Content)
			want.WriteString("\n")
		}
		if small != want.String() {
			t.Error("remaining content is not the untruncated concatenation of the kept sections")
		}
	})

	t.Run("zero budget yields empty reference", func(t *testing.T) {
		ref, dropped := Reference(0)
		if ref != "" || len(dropped) != 3 {
			t.Fatalf("want everything dropped, got %d bytes, dropped %v", len(ref), dropped)
		}
	})

	// The vendored excerpts must never smuggle in content the FairTier
	// drafts cannot use — a canvas draft would fail server-side validation,
	// and upstream doc links invite the model to cite dead ends.
	t.Run("curation invariants", func(t *testing.T) {
		ref, _ := Reference(1 << 20)
		for _, banned := range []string{"localhost:9009", "rill start", "docs.rilldata.com"} {
			if strings.Contains(strings.ToLower(ref), banned) {
				t.Errorf("vendored content contains %q — re-curate the excerpt", banned)
			}
		}
	})
}
