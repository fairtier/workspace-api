// Package rillskills embeds curated excerpts of Rill's agent skills
// (https://github.com/rilldata/agent-skills, Apache-2.0) as reference
// documentation for the Rill dashboard drafter's system prompt. See README.md
// in this directory for provenance and curation notes.
package rillskills

import (
	_ "embed"
)

// The three files, embedded individually so README/LICENSE stay out of the
// prompt.
var (
	//go:embed metrics_view.md
	metricsView string
	//go:embed model.md
	model string
	//go:embed explore.md
	explore string
)

// Section is one reference document, named for the trim log.
type Section struct {
	Name    string
	Content string
}

// sections lists the documents in priority order: when the budget forces a
// trim, the LAST entries drop first. Metrics views carry the most syntax the
// model gets wrong (dimensions/measures/format presets), so they survive
// longest.
func sections() []Section {
	return []Section{
		{Name: "metrics_view", Content: metricsView},
		{Name: "model", Content: model},
		{Name: "explore", Content: explore},
	}
}

// Reference returns the concatenated reference block, dropping whole trailing
// sections (never truncating mid-document — a half example teaches wrong
// syntax) until the result fits budget bytes. Returns the kept text and the
// names of dropped sections.
func Reference(budget int) (string, []string) {
	kept := sections()
	var dropped []string
	for len(kept) > 0 && totalLen(kept) > budget {
		last := kept[len(kept)-1]
		dropped = append(dropped, last.Name)
		kept = kept[:len(kept)-1]
	}
	var b []byte
	for _, s := range kept {
		b = append(b, s.Content...)
		b = append(b, '\n')
	}
	return string(b), dropped
}

func totalLen(ss []Section) int {
	n := 0
	for _, s := range ss {
		n += len(s.Content) + 1
	}
	return n
}
