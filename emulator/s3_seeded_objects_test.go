package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The S3 test server's clock reads 2026-01-01T12:00:00Z.
const (
	seededObjectPast   = "2026-01-01T11:00:00Z"
	seededObjectFuture = "2030-01-01T00:00:00Z"
)

// seedS3Object posts a seeded object to the control plane and requires it accepted.
func seedS3Object(t *testing.T, srv *emulator.Server, seed map[string]any) {
	t.Helper()
	b, err := json.Marshal(seed)
	require.NoError(t, err)
	w := s3Request(t, srv, http.MethodPost, "/v1/s3/seeded-objects", b, nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// TestS3SeededObject_Served covers what a GET of a seeded key returns: the body in
// each of its three spellings, the Content-Type, an MD5 ETag, and a Last-Modified
// of availableAt. HEAD reports the same metadata with no body.
func TestS3SeededObject_Served(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		seed     map[string]any
		wantBody string
		wantType string
	}{
		{
			name: "a JSON object body is served as its JSON text",
			seed: map[string]any{
				"bucket": "results", "key": "tasks/t1/completion.json",
				"body":        map[string]any{"task_id": "t1", "exit_code": 4, "state": "failed"},
				"contentType": "application/json", "availableAt": seededObjectPast,
			},
			wantBody: `{"exit_code":4,"state":"failed","task_id":"t1"}`,
			wantType: "application/json",
		},
		{
			name:     "a JSON string body is served as its text",
			seed:     map[string]any{"bucket": "results", "key": "out.txt", "body": "line one\n"},
			wantBody: "line one\n",
			wantType: "application/octet-stream",
		},
		{
			name: "a base64 body is served as its bytes",
			seed: map[string]any{
				"bucket": "results", "key": "out.bin",
				"bodyBase64": base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 0xff}),
			},
			wantBody: string([]byte{0, 1, 2, 0xff}),
			wantType: "application/octet-stream",
		},
		{
			name:     "no body is an empty object",
			seed:     map[string]any{"bucket": "results", "key": "marker"},
			wantBody: "",
			wantType: "application/octet-stream",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			seedS3Object(t, srv, tt.seed)

			path := "/results/" + tt.seed["key"].(string)
			get := s3Request(t, srv, http.MethodGet, path, nil, nil)
			require.Equal(t, http.StatusOK, get.Code, get.Body.String())
			if tt.seed["body"] != nil && tt.wantType == "application/json" {
				assert.JSONEq(t, tt.wantBody, get.Body.String())
			} else {
				assert.Equal(t, tt.wantBody, get.Body.String())
			}
			assert.Equal(t, tt.wantType, get.Header().Get("Content-Type"))
			assert.Equal(t, strconv.Itoa(get.Body.Len()), get.Header().Get("Content-Length"))
			assert.NotEmpty(t, get.Header().Get("ETag"))
			if at, ok := tt.seed["availableAt"].(string); ok {
				want, err := time.Parse(time.RFC3339, at)
				require.NoError(t, err)
				assert.Equal(t, want.Format(http.TimeFormat), get.Header().Get("Last-Modified"))
			}

			head := s3Request(t, srv, http.MethodHead, path, nil, nil)
			require.Equal(t, http.StatusOK, head.Code)
			assert.Empty(t, head.Body.String())
			for _, h := range []string{"Content-Length", "ETag", "Content-Type", "Last-Modified"} {
				assert.Equal(t, get.Header().Get(h), head.Header().Get(h), h)
			}
		})
	}
}

// TestS3SeededObject_NotServed covers every case a seed does not answer: an unseeded
// key (including the tasks/<id>/completion.json shape, which is no longer synthesized),
// a seed whose availableAt is still ahead of the clock, a key in a bucket that does not
// exist, and a GET naming a version.
func TestS3SeededObject_NotServed(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "slow.json", "body": "x", "availableAt": seededObjectFuture})
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "versioned.json", "body": "x"})
	seedS3Object(t, srv, map[string]any{"key": "wild.json", "body": "x"})

	for _, tt := range []struct {
		path, code string
	}{
		{"/results/tasks/t1/completion.json", "NoSuchKey"},
		{"/results/slow.json", "NoSuchKey"},
		{"/results/versioned.json?versionId=null", "NoSuchKey"},
		{"/missing/wild.json", "NoSuchBucket"},
	} {
		for _, verb := range []string{http.MethodGet, http.MethodHead} {
			w := s3Request(t, srv, verb, tt.path, nil, nil)
			assert.Equal(t, http.StatusNotFound, w.Code, "%s %s", verb, tt.path)
			if verb == http.MethodGet {
				assert.Contains(t, w.Body.String(), tt.code, tt.path)
			}
		}
	}
}

// TestS3SeededObject_AvailableAtFollowsTheClock pins the poll-loop case the seed exists
// for: a key is absent until the simulated clock reaches availableAt, then present.
func TestS3SeededObject_AvailableAtFollowsTheClock(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ts.FreezeTimeAt(start)
	cpCreateBucket(t, ts, "jobs")

	status, body := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/s3/seeded-objects", []byte(
		`{"bucket":"jobs","key":"done.json","body":{"ok":true},"availableAt":"`+start.Add(time.Minute).Format(time.RFC3339)+`"}`))
	require.Equal(t, http.StatusOK, status, body)
	assert.JSONEq(t, `{"ok":true,"bucket":"jobs","key":"done.json"}`, body)

	assert.Equal(t, http.StatusNotFound, cpS3(t, ts, http.MethodGet, "/jobs/done.json", nil, nil))
	ts.FreezeTimeAt(start.Add(time.Minute))
	assert.Equal(t, http.StatusOK, cpS3(t, ts, http.MethodGet, "/jobs/done.json", nil, nil))
}

// TestS3SeededObject_Precedence covers resolution order: a real object beats any seed,
// and an exact-bucket seed beats the wildcard.
func TestS3SeededObject_Precedence(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	for _, b := range []string{"/aaa", "/bbb"} {
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, b, nil, nil).Code)
	}
	seedS3Object(t, srv, map[string]any{"bucket": "*", "key": "k", "body": "wild"})
	seedS3Object(t, srv, map[string]any{"bucket": "aaa", "key": "k", "body": "exact"})
	seedS3Object(t, srv, map[string]any{"bucket": "aaa", "key": "real", "body": "seeded"})
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/aaa/real", []byte("written"), nil).Code)

	assert.Equal(t, "exact", s3Request(t, srv, http.MethodGet, "/aaa/k", nil, nil).Body.String())
	assert.Equal(t, "wild", s3Request(t, srv, http.MethodGet, "/bbb/k", nil, nil).Body.String())
	assert.Equal(t, "written", s3Request(t, srv, http.MethodGet, "/aaa/real", nil, nil).Body.String())
}

// TestS3SeededObject_RangeAndConditionals pins that a seed is served through the
// normal object path, so a ranged read and the preconditions apply to it.
func TestS3SeededObject_RangeAndConditionals(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "r.txt", "body": "0123456789"})

	part := s3Request(t, srv, http.MethodGet, "/results/r.txt", nil, map[string]string{"Range": "bytes=2-4"})
	require.Equal(t, http.StatusPartialContent, part.Code)
	assert.Equal(t, "234", part.Body.String())
	assert.Equal(t, "bytes 2-4/10", part.Header().Get("Content-Range"))

	bad := s3Request(t, srv, http.MethodGet, "/results/r.txt", nil, map[string]string{"Range": "bytes=20-"})
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, bad.Code)

	etag := s3Request(t, srv, http.MethodHead, "/results/r.txt", nil, nil).Header().Get("ETag")
	require.NotEmpty(t, etag)
	assert.Equal(t, http.StatusNotModified,
		s3Request(t, srv, http.MethodGet, "/results/r.txt", nil, map[string]string{"If-None-Match": etag}).Code)
	assert.Equal(t, http.StatusPreconditionFailed,
		s3Request(t, srv, http.MethodHead, "/results/r.txt", nil, map[string]string{"If-Match": `"deadbeef"`}).Code)
}

// TestS3SeededObject_Clear covers the three DELETE forms: one bucket and key, a key
// alone (the wildcard), and neither (everything).
func TestS3SeededObject_Clear(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "one", "body": "1"})
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "two", "body": "2"})
	seedS3Object(t, srv, map[string]any{"key": "three", "body": "3"})

	status := func(key string) int { return s3Request(t, srv, http.MethodGet, "/results/"+key, nil, nil).Code }

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects?bucket=results&key=one", nil, nil).Code)
	assert.Equal(t, http.StatusNotFound, status("one"))
	assert.Equal(t, http.StatusOK, status("two"))

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects?key=three", nil, nil).Code)
	assert.Equal(t, http.StatusNotFound, status("three"))

	assert.Equal(t, http.StatusBadRequest, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects?bucket=results", nil, nil).Code)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects", nil, nil).Code)
	assert.Equal(t, http.StatusNotFound, status("two"))
}

// TestS3SeededObject_RejectsBadSeeds covers every refusal, each of which would
// otherwise store a seed that could never be served as written.
func TestS3SeededObject_RejectsBadSeeds(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	for _, tt := range []struct{ name, body, want string }{
		{"malformed JSON", `{not json`, "invalid"},
		{"no key", `{"bucket":"b","body":"x"}`, "key is required"},
		{"both bodies", `{"key":"k","body":"x","bodyBase64":"eA=="}`, "not both"},
		{"bad base64", `{"key":"k","bodyBase64":"%%%"}`, "bodyBase64"},
		{"bad availableAt", `{"key":"k","availableAt":"tomorrow"}`, "availableAt"},
	} {
		w := s3Request(t, srv, http.MethodPost, "/v1/s3/seeded-objects", []byte(tt.body), nil)
		assert.Equal(t, http.StatusBadRequest, w.Code, tt.name)
		assert.Contains(t, w.Body.String(), tt.want, tt.name)
	}
}

// TestReplay_ASeededObjectReplays pins that the seed is a recorded control-plane write:
// a replay re-applies it in position, so the GET it answered replays as the same 200
// and body rather than a 404.
func TestReplay_ASeededObjectReplays(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	cpCreateBucket(t, ts, "replayed")
	status, body := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/s3/seeded-objects",
		[]byte(`{"bucket":"replayed","key":"out.json","body":{"n":1}}`))
	require.Equal(t, http.StatusOK, status, body)
	require.Equal(t, http.StatusOK, cpS3(t, ts, http.MethodGet, "/replayed/out.json", nil, nil))

	seedEvents := cpControlPlaneEvents(t, ts)
	require.Len(t, seedEvents, 1)
	assert.Equal(t, "POST /v1/s3/seeded-objects", seedEvents[0].Operation)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.SkippedEvents)
	assert.Empty(t, results.Differences, replayDifferenceSummary(results))
}

// TestS3SeededObject_StoreFaults pins that a store failure on the seed's record is a
// 500 rather than a silent miss, on every path that touches it.
func TestS3SeededObject_StoreFaults(t *testing.T) {
	t.Parallel()
	const record = "seeded-object:"
	for _, tt := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		path   string
		body   string
	}{
		{"GET, seed read", func(m *cfFaultStateManager) { m.failGet = record }, http.MethodGet, "/results/k", ""},
		{"HEAD, seed read", func(m *cfFaultStateManager) { m.failGet = record }, http.MethodHead, "/results/k", ""},
		{"GET, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = record }, http.MethodGet, "/results/k", ""},
		{"POST, seed write", func(m *cfFaultStateManager) { m.failPut = record }, http.MethodPost, "/v1/s3/seeded-objects", `{"key":"k2"}`},
		{"DELETE one", func(m *cfFaultStateManager) { m.failDelete = record }, http.MethodDelete, "/v1/s3/seeded-objects?key=k", ""},
		{"DELETE all", func(m *cfFaultStateManager) { m.failDelete = record }, http.MethodDelete, "/v1/s3/seeded-objects", ""},
		{"GET, tombstone read", func(m *cfFaultStateManager) { m.failGet = "seeded-object-superseded:" }, http.MethodGet, "/results/k", ""},
		{"DeleteObject, tombstone write", func(m *cfFaultStateManager) { m.failPut = "seeded-object-superseded:" }, http.MethodDelete, "/results/k", ""},
		{"PutObject, seed read", func(m *cfFaultStateManager) { m.failGet = record }, http.MethodPut, "/results/k", "x"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, state)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			seedS3Object(t, srv, map[string]any{"key": "k", "body": "x"})

			tt.arm(state)
			var body []byte
			if tt.body != "" {
				body = []byte(tt.body)
			}
			w := s3Request(t, srv, tt.method, tt.path, body, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
		})
	}
}

// TestS3SeededObject_TombstoneStoreFaults is the store-failure counterpart for the
// tombstone a delete of a seeded key leaves: reading a corrupt one, and failing to clear
// one when a seed is posted or cleared, are each a 500.
func TestS3SeededObject_TombstoneStoreFaults(t *testing.T) {
	t.Parallel()
	const tombstone = "seeded-object-superseded:"
	for _, tt := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		path   string
		body   string
	}{
		{"GET, corrupt tombstone", func(m *cfFaultStateManager) { m.corruptGet = tombstone }, http.MethodGet, "/results/k", ""},
		{"POST, tombstone clear", func(m *cfFaultStateManager) { m.failDelete = tombstone }, http.MethodPost, "/v1/s3/seeded-objects", `{"key":"k"}`},
		{"DELETE one, tombstone clear", func(m *cfFaultStateManager) { m.failDelete = tombstone }, http.MethodDelete, "/v1/s3/seeded-objects?key=k", ""},
		{"DELETE all, tombstone clear", func(m *cfFaultStateManager) { m.failDelete = tombstone }, http.MethodDelete, "/v1/s3/seeded-objects", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, state)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			seedS3Object(t, srv, map[string]any{"key": "k", "body": "x"})
			require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/results/k", nil, nil).Code)

			tt.arm(state)
			var body []byte
			if tt.body != "" {
				body = []byte(tt.body)
			}
			w := s3Request(t, srv, tt.method, tt.path, body, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
		})
	}
}

// TestS3SeededObject_CopySourceStoreFaults covers the copy source loader's store
// failures: the source record unreadable or corrupt, and the seed behind an absent
// source unreadable. Each is a 500 and writes no destination.
func TestS3SeededObject_CopySourceStoreFaults(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		source string
	}{
		{"source record read", func(m *cfFaultStateManager) { m.failGet = "object:results/real" }, "/results/real"},
		{"corrupt source record", func(m *cfFaultStateManager) { m.corruptGet = "object:results/real" }, "/results/real"},
		{"seed read behind an absent source", func(m *cfFaultStateManager) { m.failGet = "seeded-object:" }, "/results/seeded"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, state)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results/real", []byte("r"), nil).Code)
			seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "seeded", "body": "s"})

			tt.arm(state)
			w := s3Request(t, srv, http.MethodPut, "/results/dst", nil, map[string]string{"X-Amz-Copy-Source": tt.source})
			assert.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
		})
	}
}

// TestS3SeededObject_SupersedeStoreFaults covers the write paths that supersede a seed
// and are not in [TestS3SeededObject_StoreFaults]: a copy onto the seeded key and a
// delete in a versioned bucket. A failed seed read on either is a 500.
func TestS3SeededObject_SupersedeStoreFaults(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		versioned bool
		do        func(t *testing.T, srv *emulator.Server) int
	}{
		{"CopyObject onto the key", false, func(t *testing.T, srv *emulator.Server) int {
			return s3Request(t, srv, http.MethodPut, "/results/k", nil,
				map[string]string{"X-Amz-Copy-Source": "/results/real"}).Code
		}},
		{"DeleteObject, versioned", true, func(t *testing.T, srv *emulator.Server) int {
			return s3Request(t, srv, http.MethodDelete, "/results/k", nil, nil).Code
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, state)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			if tt.versioned {
				s3EnableVersioning(t, srv, "results")
			}
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results/real", []byte("r"), nil).Code)
			seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "k", "body": "s"})

			state.failGet = "seeded-object:results/k"
			assert.Equal(t, http.StatusInternalServerError, tt.do(t, srv))
		})
	}
}

// s3EnableVersioning turns versioning on for bucket.
func s3EnableVersioning(t *testing.T, srv *emulator.Server, bucket string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?versioning", []byte(
		`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// TestS3SeededObject_AWriteOrDeleteSupersedesTheSeed pins that a seed stands for one
// object, which a delete or an overwrite removes: once the key has been written or
// deleted, the seed is not served again, in an unversioned or a versioned bucket.
func TestS3SeededObject_AWriteOrDeleteSupersedesTheSeed(t *testing.T) {
	t.Parallel()
	const path = "/results/out.json"
	tests := []struct {
		name      string
		versioned bool
		wildcard  bool
		steps     func(t *testing.T, srv *emulator.Server)
		wantBody  string // "" means 404 NoSuchKey
	}{
		{
			name: "DeleteObject, unversioned",
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
			},
		},
		{
			name:     "DeleteObject of a wildcard seed, unversioned",
			wildcard: true,
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
			},
		},
		{
			name: "DeleteObjects, unversioned",
			steps: func(t *testing.T, srv *emulator.Server) {
				w := s3Request(t, srv, http.MethodPost, "/results?delete",
					[]byte(`<Delete><Object><Key>out.json</Key></Object></Delete>`), nil)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			},
		},
		{
			name: "PutObject then DeleteObject, unversioned",
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, path, []byte("written"), nil).Code)
				require.Equal(t, "written", s3Request(t, srv, http.MethodGet, path, nil, nil).Body.String())
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
			},
		},
		{
			name: "CopyObject onto the key then DeleteObject, unversioned",
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results/src", []byte("copied"), nil).Code)
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, path, nil,
					map[string]string{"X-Amz-Copy-Source": "/results/src"}).Code)
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
			},
		},
		{
			name:      "DeleteObject then delete the marker, versioned",
			versioned: true,
			steps: func(t *testing.T, srv *emulator.Server) {
				del := s3Request(t, srv, http.MethodDelete, path, nil, nil)
				require.Equal(t, http.StatusNoContent, del.Code)
				require.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodGet, path, nil, nil).Code)
				marker := del.Header().Get("x-amz-version-id")
				require.NotEmpty(t, marker)
				require.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, path+"?versionId="+marker, nil, nil).Code)
			},
		},
		{
			name:      "PutObject then delete its version, versioned",
			versioned: true,
			steps: func(t *testing.T, srv *emulator.Server) {
				put := s3Request(t, srv, http.MethodPut, path, []byte("written"), nil)
				require.Equal(t, http.StatusOK, put.Code)
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
					path+"?versionId="+put.Header().Get("x-amz-version-id"), nil, nil).Code)
			},
		},
		{
			name: "posting the seed again restores it",
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
				seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "out.json", "body": "again"})
			},
			wantBody: "again",
		},
		{
			name:     "posting a wildcard seed again restores it",
			wildcard: true,
			steps: func(t *testing.T, srv *emulator.Server) {
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
				seedS3Object(t, srv, map[string]any{"key": "out.json", "body": "again"})
			},
			wantBody: "again",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/other", nil, nil).Code)
			if tt.versioned {
				s3EnableVersioning(t, srv, "results")
			}
			seed := map[string]any{"bucket": "results", "key": "out.json", "body": "seeded"}
			if tt.wildcard {
				seed["bucket"] = "*"
			}
			seedS3Object(t, srv, seed)
			require.Equal(t, "seeded", s3Request(t, srv, http.MethodGet, path, nil, nil).Body.String())

			tt.steps(t, srv)

			get := s3Request(t, srv, http.MethodGet, path, nil, nil)
			if tt.wantBody == "" {
				assert.Equal(t, http.StatusNotFound, get.Code, get.Body.String())
				assert.Contains(t, get.Body.String(), "NoSuchKey")
				assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodHead, path, nil, nil).Code)
			} else {
				assert.Equal(t, tt.wantBody, get.Body.String())
			}
			if tt.wildcard && tt.wantBody == "" {
				assert.Equal(t, "seeded", s3Request(t, srv, http.MethodGet, "/other/out.json", nil, nil).Body.String(),
					"a wildcard seed superseded in one bucket still answers in another")
			}
		})
	}
}

// TestS3SeededObject_ADeleteBeforeAvailableAtDeletesNothing pins the other half: a seed
// still ahead of the clock describes a write that has not happened, so deleting the key
// first leaves it to appear on time. Clearing the seed clears a tombstone with it.
func TestS3SeededObject_ADeleteBeforeAvailableAtDeletesNothing(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "late", "body": "late", "availableAt": "2026-06-01T00:00:00Z"})
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "now", "body": "now"})

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/results/late", nil, nil).Code)
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/results/now", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPost, "/v1/control/time",
		[]byte(`{"time":"2026-06-01T00:00:00Z"}`), nil).Code)
	assert.Equal(t, "late", s3Request(t, srv, http.MethodGet, "/results/late", nil, nil).Body.String())
	assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodGet, "/results/now", nil, nil).Code)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects?bucket=results&key=now", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "now", "body": "fresh"})
	assert.Equal(t, "fresh", s3Request(t, srv, http.MethodGet, "/results/now", nil, nil).Body.String())

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/results/now", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete, "/v1/s3/seeded-objects", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "now", "body": "after clear-all"})
	assert.Equal(t, "after clear-all", s3Request(t, srv, http.MethodGet, "/results/now", nil, nil).Body.String())
}

// TestS3SeededObject_CopySource pins that CopyObject reads a seeded source the way
// GetObject does, body and all.
func TestS3SeededObject_CopySource(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
	seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "seeded.txt", "body": "from a seed"})

	w := s3Request(t, srv, http.MethodPut, "/results/copy.txt", nil,
		map[string]string{"X-Amz-Copy-Source": "/results/seeded.txt"})
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, "from a seed", s3Request(t, srv, http.MethodGet, "/results/copy.txt", nil, nil).Body.String())

	missing := s3Request(t, srv, http.MethodPut, "/results/copy2.txt", nil,
		map[string]string{"X-Amz-Copy-Source": "/results/absent.txt"})
	assert.Equal(t, http.StatusNotFound, missing.Code)
	assert.Contains(t, missing.Body.String(), "NoSuchKey")

	// A "*" seed does not make a bucket exist: copying from it in a bucket that does
	// not is NoSuchBucket, as a GET of it is.
	seedS3Object(t, srv, map[string]any{"bucket": "*", "key": "wild.txt", "body": "anywhere"})
	noBucket := s3Request(t, srv, http.MethodPut, "/results/copy3.txt", nil,
		map[string]string{"X-Amz-Copy-Source": "/no-such-bucket/wild.txt"})
	assert.Equal(t, http.StatusNotFound, noBucket.Code)
	assert.Contains(t, noBucket.Body.String(), "NoSuchBucket")
	assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodGet, "/results/copy3.txt", nil, nil).Code,
		"a refused copy writes no destination")
}

// TestS3SeededObject_TombstonesStayInTheirScope pins that a tombstone suppresses only the
// seed it superseded. Posting a "*" seed must not revive a bucket's own deleted seed, and
// clearing a bucket's own seed must not revive the "*" seed deleted there.
func TestS3SeededObject_TombstonesStayInTheirScope(t *testing.T) {
	t.Parallel()
	const path = "/results/k"
	get := func(t *testing.T, srv *emulator.Server) (int, string) {
		t.Helper()
		w := s3Request(t, srv, http.MethodGet, path, nil, nil)
		return w.Code, w.Body.String()
	}

	t.Run("a wildcard seed posted after a bucket seed was deleted", func(t *testing.T) {
		t.Parallel()
		srv, _ := newS3TestServer(t)
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
		seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "k", "body": "exact"})
		require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)

		seedS3Object(t, srv, map[string]any{"bucket": "*", "key": "k", "body": "wild"})
		code, _ := get(t, srv)
		assert.Equal(t, http.StatusNotFound, code,
			"the bucket's own deleted seed still decides the key; the wildcard does not revive it")
	})

	t.Run("a bucket seed cleared after the wildcard seed was deleted there", func(t *testing.T) {
		t.Parallel()
		srv, _ := newS3TestServer(t)
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
		seedS3Object(t, srv, map[string]any{"bucket": "*", "key": "k", "body": "wild"})
		require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)

		seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "k", "body": "exact"})
		code, body := get(t, srv)
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "exact", body, "a new seed of the bucket's own scope is served")

		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete,
			"/v1/s3/seeded-objects?bucket=results&key=k", nil, nil).Code)
		code, _ = get(t, srv)
		assert.Equal(t, http.StatusNotFound, code, "the wildcard seed deleted here stays deleted")
	})

	t.Run("deleting a bucket seed keeps the wildcard's tombstone", func(t *testing.T) {
		t.Parallel()
		srv, _ := newS3TestServer(t)
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)
		seedS3Object(t, srv, map[string]any{"bucket": "*", "key": "k", "body": "wild"})
		require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)
		seedS3Object(t, srv, map[string]any{"bucket": "results", "key": "k", "body": "exact"})
		require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, path, nil, nil).Code)

		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodDelete,
			"/v1/s3/seeded-objects?bucket=results&key=k", nil, nil).Code)
		code, _ := get(t, srv)
		assert.Equal(t, http.StatusNotFound, code, "superseding the bucket seed did not erase the wildcard's record")
	})
}

// TestReplay_ASeedWrittenOnARunningClockReplaysToTheSameState pins that a seed which
// stores the time it was written stores the instant its event records. The clock runs
// during the recording, so without the hold the handler's reading and the event's
// timestamp would differ, and the replayed state would not match the live one.
func TestReplay_ASeedWrittenOnARunningClockReplaysToTheSameState(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	require.False(t, ts.TimeController().Frozen(), "the point is a running clock")
	cpCreateBucket(t, ts, "running")
	status, body := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/s3/seeded-objects",
		[]byte(`{"bucket":"running","key":"k","body":"x"}`))
	require.Equal(t, http.StatusOK, status, body)

	sm, ok := ts.StateManager().(emulator.SnapshotableStateManager)
	require.True(t, ok)
	live, err := sm.Snapshot(t.Context())
	require.NoError(t, err)
	at, err := ts.ReplaySandbox(t).StateAt(t.Context(), emulator.DefaultStreamID, 100)
	require.NoError(t, err)
	assert.JSONEq(t, string(live), string(at.State), "the replayed seed stores the recorded seededAt")
}
