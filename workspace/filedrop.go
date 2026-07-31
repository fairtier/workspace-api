package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/fairtier/workspace-api/core"
)

// File drop: customers upload CSV/Parquet/JSONL files through the Console
// into their own bucket (managed R2 or BYOS) under uploads/<pipeline_id>/,
// and a file_upload pipeline loads them into the warehouse. The customer
// never sees storage credentials: uploads go through the platform using the
// EffectiveS3 credentials cached in ActualConfig, and the dlt-worker receives
// the pipeline rewritten as a plain "filesystem" source with the same
// credentials injected (see PipelineService.GetEnabledPipelines).

// ErrNotFileUploadPipeline indicates a file-drop operation against a pipeline
// of a different source type.
var ErrNotFileUploadPipeline = errors.New("pipeline is not a file_upload pipeline")

// ErrInvalidUploadFile indicates a rejected upload (filename, extension, or
// size). The message is safe to surface to the caller.
type ErrInvalidUploadFile struct{ Msg string }

func (e *ErrInvalidUploadFile) Error() string { return e.Msg }

// ObjectStore writes and deletes objects in a customer's S3-compatible
// storage using the per-customer credentials resolved by Terraform
// (ActualConfig.EffectiveS3).
type ObjectStore interface {
	// Put streams body (exactly size bytes) to key in cfg's bucket.
	Put(ctx context.Context, cfg core.S3Config, key string, size int64, body io.Reader) error
	// Delete removes key from cfg's bucket. Deleting a missing key is not an
	// error (the goal — key gone — is already met).
	Delete(ctx context.Context, cfg core.S3Config, key string) error
	// Head reports whether key exists in cfg's bucket. The error is non-nil
	// only when existence could not be determined (transport/permission error),
	// so callers can tell "definitely absent" (false, nil) from "unknown".
	Head(ctx context.Context, cfg core.S3Config, key string) (bool, error)
}

// uploadExtensions maps accepted file extensions to the dlt reader format the
// worker selects for the table (see _reader_for in the dlt-worker). Matched
// case-insensitively against the filename suffix. Compressed files are not
// accepted: dlt's readers get file objects, so gzip inference-by-name does
// not apply — revisit once validated on the canary box.
var uploadExtensions = map[string]string{
	".csv":     "csv",
	".tsv":     "csv",
	".parquet": "parquet",
	".jsonl":   "jsonl",
	".ndjson":  "jsonl",
}

// uploadFilenamePattern is the allowed shape of an uploaded object name: no
// path separators, no leading dot, conservative charset. The extension
// allowlist is checked separately.
var uploadFilenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._ -]{0,199}$`)

// ValidateUploadFilename checks the object-name and extension rules shared by
// the upload path and stored-config validation.
func ValidateUploadFilename(name string) error {
	if name == "" {
		return &ErrInvalidUploadFile{Msg: "filename is required"}
	}
	if !uploadFilenamePattern.MatchString(name) {
		return &ErrInvalidUploadFile{Msg: fmt.Sprintf("invalid filename %q: use letters, digits, spaces, dots, dashes and underscores", name)}
	}
	if uploadFormat(name) == "" {
		return &ErrInvalidUploadFile{Msg: fmt.Sprintf("unsupported file type %q: accepted are .csv, .tsv, .parquet, .jsonl, .ndjson", name)}
	}
	return nil
}

// uploadFormat returns the reader format for a filename ("" = unsupported).
func uploadFormat(name string) string {
	lower := strings.ToLower(name)
	format, ext := "", ""
	for e, f := range uploadExtensions {
		if strings.HasSuffix(lower, e) && len(e) > len(ext) {
			format, ext = f, e
		}
	}
	// The stem (everything before the extension) must be non-empty.
	if format != "" && len(name) == len(ext) {
		return ""
	}
	return format
}

// tableNameSanitizer collapses every non-alphanumeric run to one underscore.
var tableNameSanitizer = regexp.MustCompile(`[^a-z0-9]+`)

// UploadTableName derives the warehouse table name from an uploaded filename:
// the extension is stripped and the stem is lowercased with non-alphanumeric
// runs collapsed to "_" ("Daily Orders-2026.csv" → "daily_orders_2026").
// dlt normalizes identifiers the same direction, so what the customer sees in
// the Console matches what lands in the catalog.
func UploadTableName(filename string) string {
	stem := longestStem(strings.ToLower(filename))
	name := strings.Trim(tableNameSanitizer.ReplaceAllString(stem, "_"), "_")
	if name == "" || name[0] >= '0' && name[0] <= '9' {
		name = "t_" + name
	}
	return name
}

// longestStem strips the longest matching upload extension from lower.
func longestStem(lower string) string {
	best := lower
	for e := range uploadExtensions {
		if strings.HasSuffix(lower, e) && len(lower)-len(e) < len(best) {
			best = lower[:len(lower)-len(e)]
		}
	}
	return best
}

// fileDropKey is the object key for an uploaded file: the customer's
// configured key prefix (BYOS), then uploads/<pipeline_id>/<filename>.
func fileDropKey(s3 core.S3Config, pipelineID PipelineID, filename string) string {
	return path.Join(s3.KeyPrefix, "uploads", string(pipelineID), filename)
}

// fileDropBucketURL is the s3:// URL of the pipeline's upload prefix, the
// bucket_url the rewritten filesystem source reads from.
func fileDropBucketURL(s3 core.S3Config, pipelineID PipelineID) string {
	return "s3://" + path.Join(s3.Bucket, s3.KeyPrefix, "uploads", string(pipelineID)) + "/"
}

// FileDropService manages the files of file_upload pipelines. All methods
// verify the caller owns the pipeline and that it is a file_upload pipeline.
type FileDropService struct {
	Workspaces Resolver
	Pipelines  PipelineRepository
	Store      ObjectStore
	// MaxBytes caps a single uploaded file. Zero means the default 512 MiB.
	MaxBytes int64
	Logger   *slog.Logger
}

// DefaultUploadMaxBytes caps a single uploaded file when FileDropService
// .MaxBytes is unset. Generous for "normal-sized data" while keeping one
// request from monopolizing an API replica for very long.
const DefaultUploadMaxBytes = 512 << 20

func (s *FileDropService) maxBytes() int64 {
	if s.MaxBytes > 0 {
		return s.MaxBytes
	}
	return DefaultUploadMaxBytes
}

// Upload streams one file into the pipeline's upload prefix and records it in
// the pipeline's source_config. Re-uploading the same filename overwrites the
// object and keeps a single config entry (that is how customers refresh
// data: re-drop the file, run the pipeline again).
func (s *FileDropService) Upload(ctx context.Context, callerID core.UserID, pipelineID PipelineID, filename string, size int64, body io.Reader) (*UploadedFile, error) {
	if err := ValidateUploadFilename(filename); err != nil {
		return nil, err
	}
	if size <= 0 {
		return nil, &ErrInvalidUploadFile{Msg: "upload requires a known, non-zero Content-Length"}
	}
	if size > s.maxBytes() {
		return nil, &ErrInvalidUploadFile{Msg: fmt.Sprintf("file exceeds the %d MiB upload limit", s.maxBytes()>>20)}
	}

	ws, pipeline, err := s.ownedFileUploadPipeline(ctx, callerID, pipelineID)
	if err != nil {
		return nil, err
	}
	s3, err := uploadStorage(ws)
	if err != nil {
		return nil, err
	}

	key := fileDropKey(s3, pipeline.ID, filename)
	if err := s.Store.Put(ctx, s3, key, size, body); err != nil {
		return nil, fmt.Errorf("store upload: %w", err)
	}

	file := UploadedFile{
		Name:       UploadTableName(filename),
		File:       filename,
		SizeBytes:  size,
		UploadedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := s.updateFiles(ctx, pipeline, func(files []UploadedFile) []UploadedFile {
		out := files[:0]
		for _, f := range files {
			if f.File != file.File {
				out = append(out, f)
			}
		}
		return append(out, file)
	}); err != nil {
		return nil, err
	}
	return &file, nil
}

// List returns the pipeline's uploaded files as recorded in source_config,
// flagging any whose object is no longer in the bucket. Each file maps to one
// warehouse table via its glob (the object name), so a table whose object is
// gone loads 0 rows with no error — the silent dead end this flag surfaces.
// The check is best-effort: if existence cannot be determined (unprovisioned
// ws, or a storage error) files are returned unflagged rather than
// crying wolf.
func (s *FileDropService) List(ctx context.Context, callerID core.UserID, pipelineID PipelineID) ([]UploadedFile, error) {
	ws, pipeline, err := s.ownedFileUploadPipeline(ctx, callerID, pipelineID)
	if err != nil {
		return nil, err
	}
	cfg, err := parseFileUploadConfig(pipeline.SourceConfig)
	if err != nil {
		return nil, err
	}
	s.flagMissingFiles(ctx, ws, pipeline.ID, cfg.Files)
	return cfg.Files, nil
}

// flagMissingFiles sets Missing on any file whose object is absent from the
// bucket. Best-effort: an unprovisioned customer or a Head error leaves files
// unflagged (Missing stays false) so a transient storage blip never mislabels
// a healthy table as broken.
func (s *FileDropService) flagMissingFiles(ctx context.Context, ws *Workspace, pipelineID PipelineID, files []UploadedFile) {
	s3, err := uploadStorage(ws)
	if err != nil {
		return
	}
	for i := range files {
		exists, err := s.Store.Head(ctx, s3, fileDropKey(s3, pipelineID, files[i].File))
		if err != nil {
			if s.Logger != nil {
				s.Logger.WarnContext(ctx, "file drop: could not verify upload existence",
					"pipeline_id", pipelineID, "file", files[i].File, "err", err)
			}
			continue
		}
		files[i].Missing = !exists
	}
}

// Delete removes one uploaded file from storage and from source_config.
func (s *FileDropService) Delete(ctx context.Context, callerID core.UserID, pipelineID PipelineID, filename string) error {
	if err := ValidateUploadFilename(filename); err != nil {
		return err
	}
	ws, pipeline, err := s.ownedFileUploadPipeline(ctx, callerID, pipelineID)
	if err != nil {
		return err
	}
	s3, err := uploadStorage(ws)
	if err != nil {
		return err
	}

	if err := s.Store.Delete(ctx, s3, fileDropKey(s3, pipeline.ID, filename)); err != nil {
		return fmt.Errorf("delete upload: %w", err)
	}
	return s.updateFiles(ctx, pipeline, func(files []UploadedFile) []UploadedFile {
		out := files[:0]
		for _, f := range files {
			if f.File != filename {
				out = append(out, f)
			}
		}
		return out
	})
}

func (s *FileDropService) ownedFileUploadPipeline(ctx context.Context, callerID core.UserID, pipelineID PipelineID) (*Workspace, *Pipeline, error) {
	ws, err := s.Workspaces.GetWorkspaceByUser(ctx, callerID)
	if err != nil {
		return nil, nil, fmt.Errorf("get customer: %w", err)
	}
	pipeline, err := s.Pipelines.GetPipeline(ctx, pipelineID)
	if err != nil {
		return nil, nil, fmt.Errorf("get pipeline: %w", err)
	}
	if pipeline.CustomerSlug != ws.Slug {
		return nil, nil, ErrPipelineNotFound
	}
	if pipeline.SourceType != SourceTypeFileUpload {
		return nil, nil, ErrNotFileUploadPipeline
	}
	return ws, pipeline, nil
}

// updateFiles rewrites the pipeline's files list through mutate and persists
// it. It re-reads the pipeline immediately before the read-modify-write to
// narrow (not eliminate) the last-writer-wins window: the caller resolved the
// pipeline before streaming an upload to storage, so a concurrent upload could
// have committed a files entry in between. A full fix needs optimistic
// locking; the Console uploads sequentially, so overlap is one customer
// editing the same pipeline in two tabs.
func (s *FileDropService) updateFiles(ctx context.Context, pipeline *Pipeline, mutate func([]UploadedFile) []UploadedFile) error {
	if fresh, err := s.Pipelines.GetPipeline(ctx, pipeline.ID); err == nil {
		pipeline = fresh
	}
	cfg, err := parseFileUploadConfig(pipeline.SourceConfig)
	if err != nil {
		return err
	}
	cfg.Files = mutate(cfg.Files)
	raw, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal source config: %w", err)
	}
	pipeline.SourceConfig = raw
	pipeline.UpdatedAt = time.Now()
	if err := s.Pipelines.UpdatePipeline(ctx, pipeline); err != nil {
		return fmt.Errorf("update pipeline: %w", err)
	}
	return nil
}

func parseFileUploadConfig(raw json.RawMessage) (*fileUploadConfig, error) {
	cfg := &fileUploadConfig{}
	if isEmptyJSON(raw) {
		return cfg, nil
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse file_upload source config: %w", err)
	}
	return cfg, nil
}

// uploadStorage returns the customer's effective storage for uploads, or
// ErrCustomerNotProvisioned when the workspace has no usable bucket yet.
func uploadStorage(ws *Workspace) (core.S3Config, error) {
	s3 := ws.EffectiveS3
	if s3.Bucket == "" || s3.AccessKeyID == "" || s3.SecretAccessKey == "" || s3.Endpoint == "" {
		return core.S3Config{}, ErrCustomerNotProvisioned
	}
	return s3, nil
}

// resolveFileUploadPipeline rewrites a file_upload pipeline into the plain
// "filesystem" source the dlt-worker understands, injecting the customer's
// storage credentials. Returns false when the pipeline has no files yet —
// there is nothing to load, so the worker should not see it.
func resolveFileUploadPipeline(p *Pipeline, s3 core.S3Config) (bool, error) {
	cfg, err := parseFileUploadConfig(p.SourceConfig)
	if err != nil {
		return false, err
	}
	if len(cfg.Files) == 0 {
		return false, nil
	}

	tables := make([]filesystemTable, 0, len(cfg.Files))
	for _, f := range cfg.Files {
		tables = append(tables, filesystemTable{Name: f.Name, FileGlob: f.File})
	}
	srcCfg, err := json.Marshal(struct {
		filesystemConfig
		Tables []filesystemTable `json:"tables"`
	}{
		filesystemConfig: filesystemConfig{BucketURL: fileDropBucketURL(s3, p.ID)},
		Tables:           tables,
	})
	if err != nil {
		return false, fmt.Errorf("marshal filesystem config: %w", err)
	}
	creds, err := json.Marshal(filesystemCreds{
		AccessKeyID:     s3.AccessKeyID,
		SecretAccessKey: s3.SecretAccessKey,
		EndpointURL:     s3.Endpoint,
		Region:          s3.Region,
	})
	if err != nil {
		return false, fmt.Errorf("marshal filesystem credentials: %w", err)
	}

	p.SourceType = "filesystem"
	p.SourceConfig = srcCfg
	p.SourceCredentials = creds
	return true, nil
}
