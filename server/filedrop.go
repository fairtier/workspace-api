package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	"github.com/fairtier/workspace-api/core"
	"github.com/fairtier/workspace-api/workspace"
)

// FileDropUploadHandler accepts one raw-body file upload for a file_upload
// pipeline: POST /filedrop/{pipelineID}/{filename}. It lives outside
// ConnectRPC because browsers cannot client-stream over connect-web and the
// body should stream through to the customer's bucket without buffering.
//
// Auth mirrors the public RPC mux: a Casdoor JWT bearer validated under the
// same core.UserAuth policy. Responses are JSON: the recorded UploadedFile on
// success, or {"error": "..."} with a matching HTTP status.
func FileDropUploadHandler(logger *slog.Logger, auth core.UserAuth, svc *workspace.FileDropService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		userID, err := auth.UserIDFromBearer(ctx, r.Header.Get("Authorization"))
		if err != nil {
			writeFileDropError(w, http.StatusUnauthorized, "authentication required")
			return
		}

		pipelineID := r.PathValue("pipelineID")
		filename := r.PathValue("filename")
		if pipelineID == "" || filename == "" {
			writeFileDropError(w, http.StatusBadRequest, "pipeline id and filename are required")
			return
		}
		if r.ContentLength <= 0 {
			// Streaming to S3 needs the exact size up front; browsers always
			// send Content-Length for File/Blob bodies.
			writeFileDropError(w, http.StatusLengthRequired, "Content-Length is required")
			return
		}

		file, err := svc.Upload(ctx, userID, workspace.PipelineID(pipelineID), filename, r.ContentLength, r.Body)
		if err != nil {
			status, msg := fileDropStatus(err)
			if status == http.StatusInternalServerError {
				logger.ErrorContext(ctx, "file drop upload failed", "pipeline_id", pipelineID, "file", filename, "err", err)
				msg = "upload failed"
			}
			writeFileDropError(w, status, msg)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(file)
	}
}

// fileDropStatus maps domain errors to HTTP statuses, reusing the RPC
// mapping (domainError) so both surfaces classify identically.
func fileDropStatus(err error) (int, string) {
	var connectErr *connect.Error
	if !errors.As(domainError(err), &connectErr) {
		return http.StatusInternalServerError, ""
	}
	switch connectErr.Code() {
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest, connectErr.Message()
	case connect.CodeNotFound:
		return http.StatusNotFound, connectErr.Message()
	case connect.CodeFailedPrecondition:
		return http.StatusConflict, connectErr.Message()
	default:
		return http.StatusInternalServerError, ""
	}
}

func writeFileDropError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
