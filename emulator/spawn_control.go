package emulator

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 by definition, not a security use.
	"encoding/json"
	"fmt"
	"net/http"
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

// resolveTaskCompletion synthesizes a spore.host task-completion record for a GET
// of tasks/<task_id>/completion.json when no real object exists at the key. It
// returns (obj, body, true) when it handled the request, letting the caller serve
// the record through its normal object-response path — so conditional and ranged
// reads apply to it as they do to any other object.
//
// It returns handled=false both when the key is not a completion-record key and
// when a seeded record's EndedAt has not yet been reached on the simulated clock
// ("still running"); in either case the caller's NoSuchKey response is correct.
//
// Absent a seed, a matching key resolves to the nominal success record
// (exit_code 0, state "completed") so the happy path needs no seeding, matching
// the seeding philosophy. Substrate does not run the task; this is the seedable
// completion observation only.
func (p *S3Plugin) resolveTaskCompletion(ctx context.Context, key string) (S3Object, []byte, bool) {
	taskID, ok := taskCompletionID(key)
	if !ok {
		return S3Object{}, nil, false
	}

	data, err := p.state.Get(ctx, spawnCtrlNamespace, spawnCompletionKey(taskID))
	if err != nil {
		return S3Object{}, nil, false
	}

	var rec spawnTaskCompletion
	if data == nil {
		// No seed: nominal success path.
		now := p.tc.Now().UTC().Format(time.RFC3339)
		rec = spawnTaskCompletion{TaskID: taskID, ExitCode: 0, State: "completed", StartedAt: now, EndedAt: now}
	} else {
		if err := json.Unmarshal(data, &rec); err != nil {
			return S3Object{}, nil, false
		}
		rec.TaskID = taskID
		if rec.State == "" {
			rec.State = "completed"
		}
		// Clock gate: before EndedAt the record is not yet present.
		if ended, perr := time.Parse(time.RFC3339, rec.EndedAt); perr == nil {
			if p.tc.Now().Before(ended) {
				return S3Object{}, nil, false
			}
		}
	}

	body, err := json.Marshal(rec)
	if err != nil {
		return S3Object{}, nil, false
	}
	hash := md5.Sum(body) //nolint:gosec // S3 ETags are MD5 by definition.
	return S3Object{
		Key:          key,
		Size:         int64(len(body)),
		ContentType:  "application/json",
		ETag:         fmt.Sprintf("%q", fmt.Sprintf("%x", hash)),
		LastModified: p.tc.Now().UTC(),
	}, body, true
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
