package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// spawnSeedCompletion posts a seeded task-completion record to the control endpoint.
func spawnSeedCompletion(t *testing.T, srv *emulator.Server, body map[string]any) {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	w := s3Request(t, srv, http.MethodPost, "/v1/spawn/task-completion", b, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

func TestSpawn_TaskCompletion_NominalSuccess(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// No seed: a matching completion key resolves to the nominal success record.
	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-1/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var rec map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
	assert.Equal(t, "task-1", rec["task_id"])
	assert.Equal(t, float64(0), rec["exit_code"])
	assert.Equal(t, "completed", rec["state"])
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestSpawn_TaskCompletion_SeededFailure(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// ended_at is in the past relative to the fixed test clock (2026-01-01 12:00),
	// so the record is immediately observable.
	spawnSeedCompletion(t, srv, map[string]any{
		"task_id":    "task-fail",
		"exit_code":  4,
		"state":      "failed",
		"started_at": "2026-01-01T11:00:00Z",
		"ended_at":   "2026-01-01T11:05:00Z",
	})

	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-fail/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var rec map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
	assert.Equal(t, float64(4), rec["exit_code"])
	assert.Equal(t, "failed", rec["state"])
	assert.Equal(t, "2026-01-01T11:05:00Z", rec["ended_at"])
}

func TestSpawn_TaskCompletion_ClockGate(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// ended_at is far in the future relative to the fixed test clock, so a poll
	// observes the record as not-yet-present ("still running").
	spawnSeedCompletion(t, srv, map[string]any{
		"task_id":    "task-slow",
		"exit_code":  0,
		"state":      "completed",
		"started_at": "2026-01-01T11:59:00Z",
		"ended_at":   "2030-01-01T00:00:00Z",
	})

	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-slow/completion.json", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "NoSuchKey")
}

func TestSpawn_TaskCompletion_RealObjectWins(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// A real staged object at the completion key (the interim `aws s3 cp` path)
	// is served verbatim, not overridden by the seed resolver.
	staged := `{"task_id":"task-staged","exit_code":7,"state":"failed","started_at":"x","ended_at":"y"}`
	pw := s3Request(t, srv, http.MethodPut, "/results/tasks/task-staged/completion.json", []byte(staged), nil)
	require.Equal(t, http.StatusOK, pw.Code)

	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-staged/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, staged, w.Body.String())
}

func TestSpawn_TaskCompletion_NonMatchingKeyStays404(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// A bare GET of a non-existent, non-completion key must still 404 — the
	// resolver only synthesizes for the tasks/*/completion.json shape.
	for _, key := range []string{
		"/results/tasks/task-1/other.json",
		"/results/some/other/object.txt",
		"/results/tasks//completion.json",
	} {
		w := s3Request(t, srv, http.MethodGet, key, nil, nil)
		assert.Equal(t, http.StatusNotFound, w.Code, key)
		assert.Contains(t, w.Body.String(), "NoSuchKey", key)
	}
}

func TestSpawn_TaskCompletion_SeedAndClear(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	spawnSeedCompletion(t, srv, map[string]any{
		"task_id": "task-c", "exit_code": 2, "state": "failed",
		"started_at": "2026-01-01T11:00:00Z", "ended_at": "2026-01-01T11:01:00Z",
	})

	// Seeded outcome is observed.
	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-c/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	var rec map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
	assert.Equal(t, float64(2), rec["exit_code"])

	// Targeted clear removes the seed; the key falls back to the nominal success.
	dw := s3Request(t, srv, http.MethodDelete, "/v1/spawn/task-completion?taskId=task-c", nil, nil)
	require.Equal(t, http.StatusOK, dw.Code)

	w2 := s3Request(t, srv, http.MethodGet, "/results/tasks/task-c/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w2.Code)
	require.NoError(t, json.Unmarshal(w2.Body.Bytes(), &rec))
	assert.Equal(t, float64(0), rec["exit_code"])
	assert.Equal(t, "completed", rec["state"])

	// Missing task_id on seed is a 400.
	bw := s3Request(t, srv, http.MethodPost, "/v1/spawn/task-completion", []byte(`{"exit_code":1}`), nil)
	assert.Equal(t, http.StatusBadRequest, bw.Code)

	// Clear-all also succeeds.
	spawnSeedCompletion(t, srv, map[string]any{"task_id": "task-d", "state": "completed", "ended_at": "2026-01-01T11:00:00Z"})
	caw := s3Request(t, srv, http.MethodDelete, "/v1/spawn/task-completion", nil, nil)
	assert.Equal(t, http.StatusOK, caw.Code)
}
