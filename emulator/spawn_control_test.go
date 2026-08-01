package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestSpawn_TaskCompletion_SeedNoEndedAt(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/results", nil, nil)

	// A seed without ended_at (unparseable) is served immediately, and an empty
	// state defaults to "completed".
	spawnSeedCompletion(t, srv, map[string]any{"task_id": "task-now", "exit_code": 1})

	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-now/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var rec map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &rec))
	assert.Equal(t, float64(1), rec["exit_code"])
	assert.Equal(t, "completed", rec["state"])
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

// spawnCompletionHeadGet issues a HEAD and a GET of the same completion key and
// returns both recorders, so a test can assert the pair agrees rather than
// asserting each in isolation. The disagreement in #457 was invisible to any test
// that only exercised one verb.
func spawnCompletionHeadGet(t *testing.T, srv *emulator.Server, path string) (head, get *httptest.ResponseRecorder) {
	t.Helper()
	return s3Request(t, srv, http.MethodHead, path, nil, nil),
		s3Request(t, srv, http.MethodGet, path, nil, nil)
}

// TestSpawn_TaskCompletion_HeadMatchesGet is #457: getObject consulted the
// completion resolver and headObject did not, so HEAD reported NoSuchKey for a key
// GET served with a 200 body. Real S3 never disagrees with itself that way.
//
// The practical failure was that `aws s3 cp` — the command spawn prints for users —
// could not read a synthesized record at all, because the CLI HEADs before it GETs
// and never reached the working GET. An SDK HeadObject existence poll had the same
// problem, and in the worse direction: absence reads as "still running", so the
// loop never terminates rather than failing visibly.
//
// Every resolver behavior is asserted through both verbs together, since the whole
// defect was one verb's view of the resolver drifting from the other's.
func TestSpawn_TaskCompletion_HeadMatchesGet(t *testing.T) {
	tests := []struct {
		name     string
		seed     map[string]any
		stage    string
		key      string
		wantCode int
	}{
		{
			name:     "nominal success, no seed",
			key:      "task-h1",
			wantCode: http.StatusOK,
		},
		{
			name: "seeded failure",
			seed: map[string]any{
				"task_id": "task-h2", "exit_code": 9, "state": "failed",
				"started_at": "2026-01-01T11:00:00Z", "ended_at": "2026-01-01T11:05:00Z",
			},
			key:      "task-h2",
			wantCode: http.StatusOK,
		},
		{
			// The clock gate has to gate both verbs identically: a HEAD before the
			// simulated completion time is the "still running" observation a poll
			// loop depends on, and a HEAD that answered 200 early would report a
			// task complete that GET still reports as absent.
			name: "clock-gated, not yet complete",
			seed: map[string]any{
				"task_id": "task-h3", "exit_code": 0, "state": "completed",
				"started_at": "2026-01-01T11:59:00Z", "ended_at": "2030-01-01T00:00:00Z",
			},
			key:      "task-h3",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "real staged object wins",
			stage:    `{"task_id":"task-h4","exit_code":7,"state":"failed","started_at":"x","ended_at":"y"}`,
			key:      "task-h4",
			wantCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

			if tt.seed != nil {
				spawnSeedCompletion(t, srv, tt.seed)
			}
			path := "/results/tasks/" + tt.key + "/completion.json"
			if tt.stage != "" {
				require.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, path, []byte(tt.stage), nil).Code)
			}

			head, get := spawnCompletionHeadGet(t, srv, path)

			assert.Equal(t, tt.wantCode, get.Code, "GET: %s", get.Body.String())
			assert.Equal(t, tt.wantCode, head.Code, "HEAD: %s", head.Body.String())
			assert.Equal(t, get.Code, head.Code, "HEAD and GET disagree about the same key")

			if tt.wantCode != http.StatusOK {
				assert.Contains(t, head.Body.String(), "NoSuchKey")
				return
			}

			// The metadata a HEAD exists to report has to describe the body a GET
			// would actually return — a HEAD promising a different length or ETag is
			// its own wrong answer, and Content-Length is what `aws s3 cp` sizes its
			// download from.
			assert.Empty(t, head.Body.String(), "HEAD returns no body")
			for _, h := range []string{"Content-Length", "ETag", "Content-Type", "Last-Modified", "Accept-Ranges"} {
				assert.Equal(t, get.Header().Get(h), head.Header().Get(h), h)
			}
			assert.Equal(t, strconv.Itoa(get.Body.Len()), head.Header().Get("Content-Length"),
				"HEAD's Content-Length must match the bytes GET returns")
		})
	}
}

// TestSpawn_TaskCompletion_HeadNonMatchingKeyStays404 is the counterpart to
// [TestSpawn_TaskCompletion_NonMatchingKeyStays404] for HEAD. Routing the resolver
// into headObject must not make HEAD answer for keys the resolver does not own; a
// HEAD of any absent key outside the tasks/*/completion.json shape is still
// NoSuchKey.
func TestSpawn_TaskCompletion_HeadNonMatchingKeyStays404(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

	for _, key := range []string{
		"/results/tasks/task-1/other.json",
		"/results/some/other/object.txt",
		"/results/tasks//completion.json",
		"/results/tasks/task-1/completion.json.bak",
	} {
		w := s3Request(t, srv, http.MethodHead, key, nil, nil)
		assert.Equal(t, http.StatusNotFound, w.Code, key)
		assert.Contains(t, w.Body.String(), "NoSuchKey", key)
	}
}

// TestSpawn_TaskCompletion_HeadVersionedIsNot404Synthesized pins that a HEAD naming
// an explicit versionId does not reach the resolver, matching getObject: a
// synthesized record has no version history, so answering for a named version would
// invent one.
func TestSpawn_TaskCompletion_HeadVersionedIsNot404Synthesized(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

	const path = "/results/tasks/task-v/completion.json"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodHead, path, nil, nil).Code)

	for _, verb := range []string{http.MethodHead, http.MethodGet} {
		w := s3Request(t, srv, verb, path+"?versionId=null", nil, nil)
		assert.Equal(t, http.StatusNotFound, w.Code, verb)
		assert.Contains(t, w.Body.String(), "NoSuchKey", verb)
	}
}

// TestSpawn_TaskCompletion_HeadRange covers HEAD's Range handling on a synthesized
// record. headObject honors Range as GET does, and now that it serves the resolver's
// object the two must agree on both the satisfiable and unsatisfiable cases.
func TestSpawn_TaskCompletion_HeadRange(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

	const path = "/results/tasks/task-hr/completion.json"
	full := s3Request(t, srv, http.MethodGet, path, nil, nil)
	require.Equal(t, http.StatusOK, full.Code)

	part := s3Request(t, srv, http.MethodHead, path, nil, map[string]string{"Range": "bytes=0-3"})
	require.Equal(t, http.StatusPartialContent, part.Code)
	assert.Equal(t, "4", part.Header().Get("Content-Length"))
	assert.Equal(t, "bytes 0-3/"+strconv.Itoa(full.Body.Len()), part.Header().Get("Content-Range"))
	assert.Empty(t, part.Body.String(), "HEAD returns no body even for a range")

	bad := s3Request(t, srv, http.MethodHead, path, nil,
		map[string]string{"Range": "bytes=" + strconv.Itoa(full.Body.Len()+10) + "-"})
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, bad.Code)
}

// TestSpawn_TaskCompletion_HeadPreconditions covers conditional HEADs against a
// synthesized record. The resolver supplies a real ETag, so If-None-Match must
// produce a 304 — which is what makes a cheap "has it changed?" poll work — and a
// non-matching If-Match a 412.
func TestSpawn_TaskCompletion_HeadPreconditions(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

	const path = "/results/tasks/task-hp/completion.json"
	first := s3Request(t, srv, http.MethodHead, path, nil, nil)
	require.Equal(t, http.StatusOK, first.Code)
	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag, "the resolver must supply an ETag for conditionals to work")

	same := s3Request(t, srv, http.MethodHead, path, nil, map[string]string{"If-None-Match": etag})
	assert.Equal(t, http.StatusNotModified, same.Code)

	mismatch := s3Request(t, srv, http.MethodHead, path, nil, map[string]string{"If-Match": `"deadbeef"`})
	assert.Equal(t, http.StatusPreconditionFailed, mismatch.Code)
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

	// Malformed JSON body on seed is a 400.
	mw := s3Request(t, srv, http.MethodPost, "/v1/spawn/task-completion", []byte(`{not json`), nil)
	assert.Equal(t, http.StatusBadRequest, mw.Code)

	// Clear-all also succeeds.
	spawnSeedCompletion(t, srv, map[string]any{"task_id": "task-d", "state": "completed", "ended_at": "2026-01-01T11:00:00Z"})
	caw := s3Request(t, srv, http.MethodDelete, "/v1/spawn/task-completion", nil, nil)
	assert.Equal(t, http.StatusOK, caw.Code)
}
