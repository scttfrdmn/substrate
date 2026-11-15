package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// osFixedTime is the simulated clock's start for in-process OpenSearch tests, so no test reads the
// wall clock.
var osFixedTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// errOSInjected is the failure osFaultState injects.
var errOSInjected = errors.New("injected state failure")

// osFaultState is a StateManager that fails one chosen call — the failAt-th Get, Put, Delete or List
// after a reset of its counter — and delegates every other call.
type osFaultState struct {
	emulator.StateManager
	mu     sync.Mutex
	calls  int
	failAt int
}

func (s *osFaultState) tick() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failAt > 0 && s.calls == s.failAt {
		return errOSInjected
	}
	return nil
}

func (s *osFaultState) arm(failAt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls, s.failAt = 0, failAt
}

func (s *osFaultState) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *osFaultState) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if err := s.tick(); err != nil {
		return nil, err
	}
	return s.StateManager.Get(ctx, ns, key)
}

func (s *osFaultState) Put(ctx context.Context, ns, key string, v []byte) error {
	if err := s.tick(); err != nil {
		return err
	}
	return s.StateManager.Put(ctx, ns, key, v)
}

func (s *osFaultState) Delete(ctx context.Context, ns, key string) error {
	if err := s.tick(); err != nil {
		return err
	}
	return s.StateManager.Delete(ctx, ns, key)
}

func (s *osFaultState) List(ctx context.Context, ns, prefix string) ([]string, error) {
	if err := s.tick(); err != nil {
		return nil, err
	}
	return s.StateManager.List(ctx, ns, prefix)
}

// osOp is one in-process OpenSearch request.
type osOp struct {
	method, path, body string
	params             map[string]string
}

// osDirect builds an OpenSearchPlugin over state and returns a function issuing one request to it.
// Each request's ID mint is seeded from its position, so a run is reproducible.
func osDirect(t *testing.T, state emulator.StateManager) func(osOp) (*emulator.AWSResponse, error) {
	t.Helper()
	p := &emulator.OpenSearchPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  emulator.NewDefaultLogger(0, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(osFixedTime)},
	}))
	n := 0
	return func(op osOp) (*emulator.AWSResponse, error) {
		n++
		seed := fmt.Sprintf("req-os-%d", n)
		ctx := &emulator.RequestContext{
			RequestID: seed, AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint(seed), Metadata: map[string]interface{}{},
		}
		params := op.params
		if params == nil {
			params = map[string]string{}
		}
		return p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "opensearch", Operation: op.method, HTTPMethod: op.method, Path: op.path,
			Body: []byte(op.body), Params: params, Headers: map[string]string{},
		})
	}
}

// osBulkBody joins NDJSON lines into a bulk body.
func osBulkBody(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

// TestOpenSearch_EveryStateFailureIsReported sweeps each operation: it counts the state calls the
// operation makes, then fails each one in turn and asserts the operation returns the error rather
// than answering as though the call had succeeded.
func TestOpenSearch_EveryStateFailureIsReported(t *testing.T) {
	t.Parallel()
	seed := []osOp{
		{method: "PUT", path: "/i", body: `{"mappings":{"properties":{"a":{"type":"keyword"}}}}`},
		{method: "PUT", path: "/i/_doc/1", body: `{"a":"x","n":1}`},
		{method: "PUT", path: "/i/_doc/2", body: `{"a":"y","n":2}`},
		{method: "PUT", path: "/i/_doc/3", body: `{"a":"z","n":3}`},
	}
	cases := []struct {
		name string
		call osOp
	}{
		{"create index", osOp{method: "PUT", path: "/new", body: `{"settings":{"number_of_shards":2}}`}},
		{"get index", osOp{method: "GET", path: "/i"}},
		{"head index", osOp{method: "HEAD", path: "/i"}},
		{"delete index", osOp{method: "DELETE", path: "/i"}},
		{"get mapping", osOp{method: "GET", path: "/i/_mapping"}},
		{"put mapping", osOp{method: "PUT", path: "/i/_mapping", body: `{"properties":{"b":{"type":"text"}}}`}},
		{"index new", osOp{method: "PUT", path: "/i/_doc/9", body: `{"a":"q"}`}},
		{"index auto-create", osOp{method: "POST", path: "/fresh/_doc", body: `{"a":"q"}`}},
		{"get doc", osOp{method: "GET", path: "/i/_doc/1"}},
		{"head doc", osOp{method: "HEAD", path: "/i/_doc/1"}},
		{"delete doc", osOp{method: "DELETE", path: "/i/_doc/1"}},
		{"search", osOp{method: "POST", path: "/i/_search", body: `{"query":{"match_all":{}}}`, params: map[string]string{"scroll": "1m"}}},
		{"bulk", osOp{method: "POST", path: "/_bulk", body: osBulkBody(
			`{"index":{"_index":"i","_id":"4"}}`, `{"a":"w"}`,
			`{"create":{"_index":"i","_id":"5"}}`, `{"a":"v"}`,
			`{"update":{"_index":"i","_id":"1"}}`, `{"doc":{"n":10}}`,
			`{"update":{"_index":"i","_id":"6"}}`, `{"doc":{"n":6},"doc_as_upsert":true}`,
			`{"delete":{"_index":"i","_id":"2"}}`,
			`{"delete":{"_index":"absent","_id":"2"}}`,
			`{"index":{"_index":"other","_id":"1"}}`, `{"a":"u"}`,
		)}},
		{"clear scroll", osOp{method: "DELETE", path: "/_search/scroll", body: `{"scroll_id":["s1","s2"]}`}},
		{"bulk update reaching an index first", osOp{method: "POST", path: "/_bulk", body: osBulkBody(
			`{"update":{"_index":"i","_id":"3"}}`, `{"doc":{"n":30}}`,
			`{"update":{"_index":"later","_id":"1"}}`, `{"upsert":{"n":1}}`,
		)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			counter := &osFaultState{StateManager: emulator.NewMemoryStateManager()}
			do := osDirect(t, counter)
			for _, op := range seed {
				_, err := do(op)
				require.NoError(t, err)
			}
			counter.arm(0)
			_, err := do(tc.call)
			require.NoError(t, err, "the operation succeeds when nothing fails")
			calls := counter.count()
			require.Positive(t, calls)

			for i := 1; i <= calls; i++ {
				state := &osFaultState{StateManager: emulator.NewMemoryStateManager()}
				do := osDirect(t, state)
				for _, op := range seed {
					_, err := do(op)
					require.NoError(t, err)
				}
				state.arm(i)
				_, err := do(tc.call)
				assert.ErrorIsf(t, err, errOSInjected, "state call %d of %d failing is reported", i, calls)
			}
		})
	}
}

// TestOpenSearch_ScrollStateFailures covers a scroll continuation's state calls and a stored context
// that will not decode.
func TestOpenSearch_ScrollStateFailures(t *testing.T) {
	t.Parallel()
	open := func(t *testing.T, state *osFaultState) (func(osOp) (*emulator.AWSResponse, error), string) {
		do := osDirect(t, state)
		_, err := do(osOp{method: "PUT", path: "/i/_doc/1", body: `{"a":1}`})
		require.NoError(t, err)
		_, err = do(osOp{method: "PUT", path: "/i/_doc/2", body: `{"a":2}`})
		require.NoError(t, err)
		resp, err := do(osOp{method: "POST", path: "/i/_search", body: `{"size":1}`, params: map[string]string{"scroll": "1m"}})
		require.NoError(t, err)
		var got struct {
			ScrollID string `json:"_scroll_id"`
		}
		require.NoError(t, json.Unmarshal(resp.Body, &got))
		return do, got.ScrollID
	}

	counter := &osFaultState{StateManager: emulator.NewMemoryStateManager()}
	do, id := open(t, counter)
	counter.arm(0)
	_, err := do(osOp{method: "GET", path: "/_search/scroll", params: map[string]string{"scroll_id": id}})
	require.NoError(t, err)
	calls := counter.count()
	for i := 1; i <= calls; i++ {
		state := &osFaultState{StateManager: emulator.NewMemoryStateManager()}
		do, id := open(t, state)
		state.arm(i)
		_, err := do(osOp{method: "GET", path: "/_search/scroll", params: map[string]string{"scroll_id": id}})
		assert.ErrorIsf(t, err, errOSInjected, "scroll state call %d of %d", i, calls)
	}

	// Clearing a live context reports a failed delete.
	clearState := &osFaultState{StateManager: emulator.NewMemoryStateManager()}
	do, id = open(t, clearState)
	clearState.arm(2) // the Get finds the context; the Delete fails
	_, err = do(osOp{method: "DELETE", path: "/_search/scroll", body: `{"scroll_id":"` + id + `"}`})
	assert.ErrorIs(t, err, errOSInjected)

	// A document deleted after the search is skipped; one whose record is corrupted is an error.
	mem := emulator.NewMemoryStateManager()
	do, id = open(t, &osFaultState{StateManager: mem})
	_, err = do(osOp{method: "DELETE", path: "/i/_doc/2"})
	require.NoError(t, err)
	resp, err := do(osOp{method: "GET", path: "/_search/scroll", params: map[string]string{"scroll_id": id}})
	require.NoError(t, err)
	assert.Contains(t, string(resp.Body), `"hits":[]`, "the deleted document is skipped")

	mem = emulator.NewMemoryStateManager()
	do, id = open(t, &osFaultState{StateManager: mem})
	require.NoError(t, mem.Put(t.Context(), "opensearch", "doc:123456789012/us-east-1/_default/i/2", []byte(`{"_source":[1]}`)))
	_, err = do(osOp{method: "GET", path: "/_search/scroll", params: map[string]string{"scroll_id": id}})
	assert.Error(t, err, "a source that will not decode is an error")

	// A stored context that will not decode is the engine's internal_error at 500.
	mem = emulator.NewMemoryStateManager()
	require.NoError(t, mem.Put(t.Context(), "opensearch", "scroll:123456789012/us-east-1/_default/broken", []byte("{")))
	resp, err = osDirect(t, mem)(osOp{method: "POST", path: "/_search/scroll", body: `{"scroll_id":"broken"}`})
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "internal_error")
}

// TestOpenSearch_CorruptRecordsAreErrors asserts a stored index or document record that will not
// decode is an error, not an absent record, and that an index record with no name reads back under
// the name its key carries.
func TestOpenSearch_CorruptRecordsAreErrors(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mem := emulator.NewMemoryStateManager()
	require.NoError(t, mem.Put(ctx, "opensearch", "index:123456789012/us-east-1/_default/badidx", []byte("{")))
	require.NoError(t, mem.Put(ctx, "opensearch", "index:123456789012/us-east-1/_default/good", []byte(`{"uuid":"u1"}`)))
	require.NoError(t, mem.Put(ctx, "opensearch", "doc:123456789012/us-east-1/_default/good/bad", []byte("{")))
	require.NoError(t, mem.Put(ctx, "opensearch", "index:123456789012/us-east-1/_default/src", []byte(`{"uuid":"u2"}`)))
	require.NoError(t, mem.Put(ctx, "opensearch", "doc:123456789012/us-east-1/_default/src/1", []byte(`{"_source":[1],"_version":1}`)))
	do := osDirect(t, mem)

	_, err := do(osOp{method: "GET", path: "/badidx"})
	assert.Error(t, err)
	_, err = do(osOp{method: "GET", path: "/good/_doc/bad"})
	assert.Error(t, err)
	_, err = do(osOp{method: "POST", path: "/src/_search"})
	assert.Error(t, err, "a stored source that is not an object will not decode")
	_, err = do(osOp{method: "GET", path: "/src/_doc/1"})
	assert.Error(t, err)
	_, err = do(osOp{method: "POST", path: "/src/_bulk", body: osBulkBody(`{"update":{"_id":"1"}}`, `{"doc":{"a":1}}`)})
	assert.Error(t, err)

	resp, err := do(osOp{method: "GET", path: "/good"})
	require.NoError(t, err)
	assert.Contains(t, string(resp.Body), `"provided_name":"good"`)
}

// TestOpenSearch_DirectRequestReadsTheVerbFromOperation covers an in-process request that sets only
// Operation, as an internal caller does, and a path with no index.
func TestOpenSearch_DirectRequestReadsTheVerbFromOperation(t *testing.T) {
	t.Parallel()
	p := &emulator.OpenSearchPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State: emulator.NewMemoryStateManager(), Logger: emulator.NewDefaultLogger(0, false),
	}))
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", IDs: emulator.NewIDMint("r")}
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{Operation: "PUT", Path: "/idx/_doc/1", Body: []byte(`{"a":1}`)})
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	resp, err = p.HandleRequest(ctx, &emulator.AWSRequest{Operation: "GET", Path: "/"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "index_not_specified")

	// An operation that is not a verb reaches no handler, and is named in the refusal.
	resp, err = p.HandleRequest(ctx, &emulator.AWSRequest{Operation: "Bogus", Path: "/idx"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "no handler for Bogus /idx")
}
