package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/fairtier/workspace-api/workspace"
)

// The sweep's timeout follows the worker's own variables, so a deployment
// that sets them once for both containers can never sweep a live run.
func TestStuckRunTimeoutFromWorkerEnv(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	tests := []struct {
		name string
		env  map[string]string
		want time.Duration
	}{
		{name: "nothing set: worker defaults", want: workspace.DefaultStuckRunTimeout},
		{
			name: "worker timeout raised", env: map[string]string{"PIPELINE_RUN_TIMEOUT_SECONDS": "43200"},
			want: 3*12*time.Hour + 90*time.Second + workspace.StuckRunGrace,
		},
		{
			name: "worker timeout lowered", env: map[string]string{"PIPELINE_RUN_TIMEOUT_SECONDS": "3600", "PIPELINE_MAX_RETRIES": "0"},
			want: time.Hour + workspace.StuckRunGrace,
		},
		{
			name: "retry backoff counted", env: map[string]string{"PIPELINE_RUN_TIMEOUT_SECONDS": "3600", "PIPELINE_MAX_RETRIES": "1", "PIPELINE_RETRY_BASE_DELAY": "600"},
			want: 2*time.Hour + 10*time.Minute + workspace.StuckRunGrace,
		},
		{
			name: "explicit override below the worker is raised", env: map[string]string{"PIPELINE_RUN_STUCK_TIMEOUT": "1h"},
			want: workspace.DefaultStuckRunTimeout,
		},
		{
			name: "explicit override above the worker is kept", env: map[string]string{"PIPELINE_RUN_STUCK_TIMEOUT": "30h"},
			want: 30 * time.Hour,
		},
		{name: "unbounded worker: sweep off", env: map[string]string{"PIPELINE_RUN_TIMEOUT_SECONDS": "0"}, want: 0},
		{name: "in-process worker has no limit: sweep off", env: map[string]string{"PIPELINE_SUBPROCESS": "false"}, want: 0},
		{
			name: "bad integer falls back to the worker default", env: map[string]string{"PIPELINE_RUN_TIMEOUT_SECONDS": "six hours"},
			want: workspace.DefaultStuckRunTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, name := range []string{
				"PIPELINE_RUN_TIMEOUT_SECONDS", "PIPELINE_MAX_RETRIES", "PIPELINE_RETRY_BASE_DELAY",
				"PIPELINE_SUBPROCESS", "PIPELINE_RUN_STUCK_TIMEOUT",
			} {
				t.Setenv(name, tt.env[name])
			}
			if got := stuckRunTimeout(logger); got != tt.want {
				t.Errorf("stuckRunTimeout() = %s, want %s", got, tt.want)
			}
		})
	}
}

// The workflow sweep's running-step rule uses the same derived timeout, its
// own override can only lengthen it, and the idle grace includes one sweep
// interval.
func TestWorkflowStuckPolicyFromWorkerEnv(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	tests := []struct {
		name string
		env  map[string]string
		want time.Duration
	}{
		{name: "worker defaults", want: workspace.DefaultStuckRunTimeout},
		{name: "the old 8h override is raised", env: map[string]string{"WORKFLOW_RUN_STUCK_TIMEOUT": "8h"}, want: workspace.DefaultStuckRunTimeout},
		{name: "a longer override is kept", env: map[string]string{"WORKFLOW_RUN_STUCK_TIMEOUT": "48h"}, want: 48 * time.Hour},
		{name: "unbounded worker: running-step rule off", env: map[string]string{"PIPELINE_SUBPROCESS": "false"}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, name := range []string{
				"PIPELINE_RUN_TIMEOUT_SECONDS", "PIPELINE_MAX_RETRIES", "PIPELINE_RETRY_BASE_DELAY",
				"PIPELINE_SUBPROCESS", "WORKFLOW_RUN_STUCK_TIMEOUT",
			} {
				t.Setenv(name, tt.env[name])
			}
			p := workflowStuckPolicy(logger, 5*time.Minute)
			if p.StepTimeout != tt.want {
				t.Errorf("StepTimeout = %s, want %s", p.StepTimeout, tt.want)
			}
			if p.IdleGrace != workspace.DefaultWorkflowIdleGrace+5*time.Minute {
				t.Errorf("IdleGrace = %s", p.IdleGrace)
			}
		})
	}
}
