package emulator

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 by definition, not a security use.
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// spawnCtrlNamespace is the state namespace for spore.host spawn control-plane
// (seed) data.
const spawnCtrlNamespace = "spawn-ctrl"

// spawnTaskCompletion is a seeded spore.host `spawn task run` completion record.
// Substrate never executes the task; a seed lets a test set the observable
// completion outcome that a GET of tasks/<task_id>/completion.json returns,
// gated on the simulated clock. It mirrors taskproto.CompletionRecord's wire
// form (see #360). The minimal five fields below are what the spawn CLI's
// fetchCompletion reads; cost_estimate/logs/retry_class are omitempty on the
// consumer side and are not modeled here.
type spawnTaskCompletion struct {
	// TaskID is the spawn task identifier (the middle segment of the object key).
	TaskID string `json:"task_id"`

	// ExitCode is the task's exit code (0 for the nominal success path).
	ExitCode int `json:"exit_code"`

	// State is the terminal task state: "completed" or "failed".
	State string `json:"state"`

	// StartedAt / EndedAt are RFC3339 timestamps. A poll before EndedAt (on the
	// simulated clock) observes the record as not-yet-present (404 NoSuchKey);
	// at or after EndedAt it is served.
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at"`
}

// spawnCompletionKey returns the state key for a seeded task-completion record.
func spawnCompletionKey(taskID string) string {
	return "completion:" + taskID
}

// taskCompletionID returns the task id embedded in a completion-record object
// key of the form "tasks/<task_id>/completion.json", and true when the key
// matches that shape. Any other key returns ("", false) so a bare GET of a
// non-existent key stays a normal 404.
func taskCompletionID(key string) (string, bool) {
	const prefix = "tasks/"
	const suffix = "/completion.json"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	if id == "" {
		return "", false
	}
	return id, true
}

// resolveTaskCompletion serves a spore.host task-completion record for a GET of
// tasks/<task_id>/completion.json when no real object exists at the key. It
// returns (resp, true) when it handled the request — either a synthesized 200
// carrying the completion JSON, or a 404 NoSuchKey when a seeded record's
// EndedAt has not yet been reached on the simulated clock ("still running").
// It returns (nil, false) when the key is not a completion-record key, so the
// caller falls through to its normal NoSuchKey response.
//
// Absent a seed, a matching key resolves to the nominal success record
// (exit_code 0, state "completed") so the happy path needs no seeding, matching
// the seeding philosophy. Substrate does not run the task; this is the seedable
// completion observation only.
func (p *S3Plugin) resolveTaskCompletion(ctx context.Context, _ string, key string) (*AWSResponse, bool) {
	taskID, ok := taskCompletionID(key)
	if !ok {
		return nil, false
	}

	data, err := p.state.Get(ctx, spawnCtrlNamespace, spawnCompletionKey(taskID))
	if err != nil {
		return nil, false
	}

	var rec spawnTaskCompletion
	if data == nil {
		// No seed: nominal success path.
		now := p.tc.Now().UTC().Format(time.RFC3339)
		rec = spawnTaskCompletion{TaskID: taskID, ExitCode: 0, State: "completed", StartedAt: now, EndedAt: now}
	} else {
		if err := json.Unmarshal(data, &rec); err != nil {
			return nil, false
		}
		rec.TaskID = taskID
		if rec.State == "" {
			rec.State = "completed"
		}
		// Clock gate: before EndedAt the record is not yet present.
		if ended, perr := time.Parse(time.RFC3339, rec.EndedAt); perr == nil {
			if p.tc.Now().Before(ended) {
				return s3ErrorResponse("NoSuchKey", "The specified key does not exist.", http.StatusNotFound), true
			}
		}
	}

	body, err := json.Marshal(rec)
	if err != nil {
		return nil, false
	}
	hash := md5.Sum(body) //nolint:gosec // S3 ETags are MD5 by definition.
	headers := map[string]string{
		"Content-Type":   "application/json",
		"ETag":           fmt.Sprintf("%q", fmt.Sprintf("%x", hash)),
		"Last-Modified":  p.tc.Now().UTC().Format(http.TimeFormat),
		"Content-Length": strconv.Itoa(len(body)),
	}
	return &AWSResponse{StatusCode: http.StatusOK, Headers: headers, Body: body}, true
}

// handleSpawnSeedTaskCompletion handles POST /v1/spawn/task-completion. It seeds
// the completion record a GET of tasks/<task_id>/completion.json returns, gated
// on EndedAt over the simulated clock. Substrate does not execute the task; this
// sets the observable completion outcome.
// Body: {"task_id","exit_code","state","started_at","ended_at"}.
func (s *Server) handleSpawnSeedTaskCompletion(w http.ResponseWriter, r *http.Request) {
	var rec spawnTaskCompletion
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if rec.TaskID == "" {
		http.Error(w, `{"error":"task_id is required"}`, http.StatusBadRequest)
		return
	}
	if rec.State == "" {
		rec.State = "completed"
	}
	data, err := json.Marshal(rec)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), spawnCtrlNamespace, spawnCompletionKey(rec.TaskID), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "taskId": rec.TaskID})
}

// handleSpawnClearTaskCompletion handles DELETE /v1/spawn/task-completion.
// With ?taskId=... it removes that seed; without it removes all.
func (s *Server) handleSpawnClearTaskCompletion(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("taskId"); id != "" {
		if err := s.state.Delete(r.Context(), spawnCtrlNamespace, spawnCompletionKey(id)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), spawnCtrlNamespace, "completion:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), spawnCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
