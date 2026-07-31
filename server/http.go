package server

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// LivenessHandler returns 200 if the process is alive.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}

// ReadinessHandler returns 200 if the database is reachable. The ping error
// is logged, never echoed: the endpoint is unauthenticated, and pgx connect
// errors spell out the DB user, database name, and host.
func ReadinessHandler(logger *slog.Logger, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			logger.ErrorContext(ctx, "readiness check: database ping failed", "err", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("database not ready"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}
