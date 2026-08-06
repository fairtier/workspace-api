// Package demo holds the embedded "NYC Taxi Pulse" starter-demo assets and a
// manifest describing how they seed a workspace: two filesystem pipelines
// reading the shared demo bucket (a backfill of the public TLC trips parquet +
// the zone lookup CSV), the dbt models committed to the box `transformations`
// repo, and the Rill files committed to the box `rill` repo.
//
// It is a leaf package: it imports nothing from the domain packages (they
// import it), so the manifest speaks in plain strings, not domain types. The
// runtime demo bucket URL and the sizing-tier glob are filled in by the domain
// layer.
//
// Column names in the embedded SQL/YAML are validated against a real load and
// corrected here — editing an embedded file is a normal code change.
package demo

import (
	"embed"
	"fmt"
	"io/fs"
	"strings"
)

//go:embed all:dbt all:rill
var repoFS embed.FS

// Warehouse namespace and object names the demo lands into. Keep in sync with
// the dbt sources.yml and the Rill model SQL.
const (
	// DatasetName is the dlt dataset both pipelines write to → the warehouse
	// namespace `lake.nyc_taxi`.
	DatasetName = "nyc_taxi"
	// TripsTable is the raw trips table produced by the trips pipeline.
	TripsTable = "yellow_trips"
	// ZonesTable is the raw zone-lookup table produced by the zones pipeline.
	// It must stay "taxi_zones" — that is the table the dbt sources.yml
	// references.
	ZonesTable = "taxi_zones"
	// ZonesObject is the zone-lookup object name in the demo bucket (the
	// public TLC filename), mirrored there by the operator. The zones
	// pipeline reads it via a filesystem source, same as the trips parquet —
	// so the shared public demo data never has to be copied into a customer's
	// own bucket.
	ZonesObject = "taxi_zone_lookup.csv"
	// BucketPrefix is the key prefix under the demo bucket the mirrored demo
	// datasets are written to.
	BucketPrefix = "nyc-taxi/"
)

// Tier is one sizing option, selected purely by which trips files it names.
//
// The files are enumerated rather than globbed, because the demo bucket is
// public and a public object-storage bucket serves objects by key and
// refuses to list a directory — there is nothing for a glob to match
// against. That is not a limitation to work around: the TLC filenames are
// deterministic, so naming them is exact where a glob was only ever an
// approximation of the same list.
type Tier struct {
	Name string
	// Files are object names under BucketPrefix, in load order.
	Files []string
	Rows  string // human hint, informational only
}

// latestYear/latestMonth is the most recent month mirrored into the demo
// bucket, and the end of every range-based tier below. TLC publishes about two
// months behind, one month at a time, so this moves — bumping it is the second
// half of an operator running scripts/mirror-demo-datasets.sh in the platform
// repo, whose LATEST_YM must hold the same month. Nothing discovers it at
// runtime: the tiers name their files (see Tier), so the mirror and the loader
// have to agree on a fixed end month rather than each finding its own.
const (
	latestYear  = 2026
	latestMonth = 5
)

// tripFile names the TLC trip file for one month.
func tripFile(year, month int) string {
	return fmt.Sprintf("yellow_tripdata_%d-%02d.parquet", year, month)
}

// tripFiles enumerates the monthly TLC trip files from one month to another,
// inclusive, in load order. A range rather than whole years because the newest
// year is always partial — a tier that stops at the last December available
// would be a year and a half stale for most of its life.
func tripFiles(fromYear, fromMonth, toYear, toMonth int) []string {
	var files []string
	for y, m := fromYear, fromMonth; y < toYear || (y == toYear && m <= toMonth); {
		files = append(files, tripFile(y, m))
		if m == 12 {
			y, m = y+1, 1
		} else {
			m++
		}
	}
	return files
}

// Tiers, keyed by name. "sample" is the default for one-click seeding of a
// fresh workspace. Measured on a live box: a standard box is a small VM
// (2 cores / ~3.8GB) running the whole platform stack (k3s,
// Lakekeeper, Postgres, Casdoor, OpenFGA, Gitea, Rill, dlt-worker) — the stack
// alone uses ~2.9GB, leaving under ~1GB of headroom. A month of trips (~3M
// rows) drove the dlt-worker to its 1GB limit on the PyIceberg write and pushed
// the node into OOM/thrash. A ~200k-row pre-built sample loads in seconds and
// fits the headroom. The larger tiers are opt-in "make it yours" upgrades and
// assume a box sized to ingest millions of rows (a one-field edit on the
// pipeline).
//
// The trip tiers are windows ending at the latest published month rather than
// at whole years, so "the demo" means recent data for as long as someone keeps
// running the mirror. `full` starts in 2019 for the story, not the size: it is
// the only tier that contains the COVID cliff.
var Tiers = map[string]Tier{
	"sample":  {Name: "sample", Files: []string{"yellow_tripdata_sample.parquet"}, Rows: "~200k"},
	"tiny":    {Name: "tiny", Files: []string{tripFile(latestYear, latestMonth)}, Rows: "~4M"},
	"minimal": {Name: "minimal", Files: tripFiles(2025, 1, 2025, 12), Rows: "~48M"},
	"default": {Name: "default", Files: tripFiles(2024, 1, latestYear, latestMonth), Rows: "~110M"},
	"full":    {Name: "full", Files: tripFiles(2019, 1, latestYear, latestMonth), Rows: "~330M"},
}

// DefaultTier is used when the caller does not pick one. Set to "sample" from
// the live measurement above: a standard box has no memory headroom to ingest
// millions of rows while hosting the full stack, so the onboarding demo uses a
// small pre-built sample.
const DefaultTier = "sample"

// TierOrDefault returns the named tier, falling back to DefaultTier for an
// empty or unknown name.
func TierOrDefault(name string) Tier {
	if t, ok := Tiers[name]; ok {
		return t
	}
	return Tiers[DefaultTier]
}

// RepoFile is one file to commit into a box repo.
type RepoFile struct {
	// Repo is the box Gitea repo: "transformations" or "rill".
	Repo string
	// Path is repo-relative (e.g. "models/staging/stg_trips.sql").
	Path string
	// Content is the file body.
	Content string
}

// repoForDir maps an embedded top-level directory to the box repo it seeds.
var repoForDir = map[string]string{
	"dbt":  "transformations",
	"rill": "rill",
}

// RepoFiles returns every embedded dbt/Rill file as a RepoFile, in a stable
// (lexical) order so seed commits — and their recorded SHAs — are
// deterministic. The leading "dbt/"/"rill/" segment is stripped to yield the
// repo-relative path.
func RepoFiles() ([]RepoFile, error) {
	var files []RepoFile
	err := fs.WalkDir(repoFS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		top, rest, ok := strings.Cut(p, "/")
		if !ok {
			return nil
		}
		repo, ok := repoForDir[top]
		if !ok {
			return nil
		}
		content, err := repoFS.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read embedded %s: %w", p, err)
		}
		files = append(files, RepoFile{Repo: repo, Path: rest, Content: string(content)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
