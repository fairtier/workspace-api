# Vendored Rill agent-skills excerpts

The `*.md` files in this directory are **curated excerpts** of Rill's agent
skills, vendored as reference documentation for the Rill dashboard drafter's
system prompt (`llm/drafter.go`).

- Source: <https://github.com/rilldata/agent-skills>
- Commit: `e58a668dcd48a6cd6fc6d5f552ba8cd59cdfeaa1` (2026-08)
- License: Apache-2.0 (see [LICENSE](./LICENSE) in this directory, and the
  repo-root `NOTICE`)

| File | Source skill | What was kept |
|---|---|---|
| `metrics_view.md` | `skills/rill-metrics-view/SKILL.md` | Core concepts (table source, timeseries, dimensions, measures, best practices, inline explore), the full example, the DuckDB dialect notes |
| `model.md` | `skills/rill-model/SKILL.md` | Introduction/categories/performance, materialization, referencing other models, refresh schedules, DuckDB dialect notes |
| `explore.md` | `skills/rill-explore/SKILL.md` | Introduction, development approach, inline explores, annotated + minimal examples |

Deliberately **dropped** at vendor time: YAML frontmatter, security policies,
canvas dashboards, incremental/partitioned models, staging connectors,
ClickHouse/Druid dialect notes, links to the Rill docs site, and anything
assuming a running Rill Developer server — none of it applies to drafting
files for a FairTier box's hosted Rill project, and prompt bytes are budgeted
(`rillSkillsBudget` in `llm/drafter.go`).

To refresh: re-extract from a newer upstream commit, re-apply the table
above, update the commit hash here, and re-run `TestRillSkillsReference`
(which guards the byte budget).

FairTier-specific rules (the `lk.<namespace>.<table>` qualification, the
platform-managed files the model must never emit) do NOT belong here — they
live in the system prompt itself, ahead of this reference block, and the
drafter's unit test asserts they survive composition verbatim.
