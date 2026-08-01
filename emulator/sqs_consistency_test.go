package emulator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// seedSQSConsistency posts a seeded create→lookup consistency window to the
// control plane.
func seedSQSConsistency(t *testing.T, srv *emulator.Server, seed map[string]any) {
	t.Helper()
	body, err := json.Marshal(seed)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/v1/sqs/consistency", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "seed failed: %s", w.Body.String())
}

// clearSQSConsistency deletes seeds via the control plane. An empty queueName
// clears all of them.
func clearSQSConsistency(t *testing.T, srv *emulator.Server, queueName string) {
	t.Helper()
	path := "/v1/sqs/consistency"
	if queueName != "" {
		path += "?queueName=" + queueName
	}
	r := httptest.NewRequest(http.MethodDelete, path, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "clear failed: %s", w.Body.String())
}

// createSQSQueue creates a queue and returns its URL.
func createSQSQueue(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	resp := sqsRequest(t, srv, map[string]string{"Action": "CreateQueue", "QueueName": name})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return "http://sqs.us-east-1.localhost/000000000000/" + name
}

// sqsErrorCode returns the response status and the error code from an SQS error
// document. SQS errors are always JSON here — errorProtocolFor resolves the
// protocol per service and sqs is JSON-RPC — so this reads __type for both
// request protocols.
func sqsErrorCode(t *testing.T, resp *http.Response) (int, string) {
	t.Helper()
	if resp.StatusCode == http.StatusOK {
		resp.Body.Close() //nolint:errcheck
		return resp.StatusCode, ""
	}
	body := readJSONBody(t, resp)
	code, _ := body["__type"].(string)
	return resp.StatusCode, code
}

// getQueueURL calls GetQueueUrl by name under the given protocol.
func getQueueURL(t *testing.T, srv *emulator.Server, name string, jsonProto bool) (int, string) {
	t.Helper()
	if jsonProto {
		return sqsErrorCode(t, sqsJSONRequest(t, srv, "GetQueueUrl", map[string]interface{}{"QueueName": name}))
	}
	return sqsErrorCode(t, sqsRequest(t, srv, map[string]string{"Action": "GetQueueUrl", "QueueName": name}))
}

// getQueueAttributes calls GetQueueAttributes by URL under the given protocol.
func getQueueAttributes(t *testing.T, srv *emulator.Server, queueURL string, jsonProto bool) (int, string) {
	t.Helper()
	if jsonProto {
		return sqsErrorCode(t, sqsJSONRequest(t, srv, "GetQueueAttributes", map[string]interface{}{"QueueUrl": queueURL}))
	}
	return sqsErrorCode(t, sqsRequest(t, srv, map[string]string{"Action": "GetQueueAttributes", "QueueUrl": queueURL}))
}

// bothProtocols runs fn for the query and JSON protocols. SQS is dual-protocol and
// the reporter reaches it through an SDK, so a seed that worked under only one
// would still leave their retry loop untestable.
func bothProtocols(t *testing.T, fn func(t *testing.T, jsonProto bool)) {
	t.Helper()
	t.Run("query", func(t *testing.T) { fn(t, false) })
	t.Run("json", func(t *testing.T) { fn(t, true) })
}

// TestSQS_GetQueueUrl_SeededConsistencyWindow is the core of #413. AWS documents
// that a caller must wait at least a second after CreateQueue before the queue is
// usable, so a create→lookup→retry loop can legitimately see QueueDoesNotExist for
// a queue that exists. Substrate resolves instantly, which makes that retry path
// unreachable and any test of it vacuous.
func TestSQS_GetQueueUrl_SeededConsistencyWindow(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		createSQSQueue(t, srv, "run-q")
		seedSQSConsistency(t, srv, map[string]any{"queueName": "run-q", "getUrlMisses": 2})

		for i := 1; i <= 2; i++ {
			status, code := getQueueURL(t, srv, "run-q", jsonProto)
			assert.Equal(t, http.StatusBadRequest, status, "call %d must miss", i)
			assert.Equal(t, "QueueDoesNotExist", code, "call %d", i)
		}

		// Budget exhausted: the queue resolves.
		status, _ := getQueueURL(t, srv, "run-q", jsonProto)
		assert.Equal(t, http.StatusOK, status, "call 3 must succeed once the window closes")

		// And stays resolved — the seed counts down, it does not re-arm.
		status, _ = getQueueURL(t, srv, "run-q", jsonProto)
		assert.Equal(t, http.StatusOK, status)
	})
}

// TestSQS_GetQueueAttributes_SeededConsistencyWindow covers the second operation.
// AWS documents QueueDoesNotExist on both, and a consumer that polls attributes
// rather than the URL needs the same window.
func TestSQS_GetQueueAttributes_SeededConsistencyWindow(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSQSQueue(t, srv, "attr-q")
		seedSQSConsistency(t, srv, map[string]any{"queueName": "attr-q", "getAttributesMisses": 1})

		status, code := getQueueAttributes(t, srv, queueURL, jsonProto)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "QueueDoesNotExist", code)

		status, _ = getQueueAttributes(t, srv, queueURL, jsonProto)
		assert.Equal(t, http.StatusOK, status)
	})
}

// TestSQS_Consistency_CountersAreIndependent pins that seeding one operation does
// not perturb the other, so a test can exercise one retry loop in isolation.
func TestSQS_Consistency_CountersAreIndependent(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createSQSQueue(t, srv, "indep-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "indep-q", "getUrlMisses": 1})

	// GetQueueAttributes was not seeded, so it resolves immediately...
	status, _ := getQueueAttributes(t, srv, queueURL, false)
	assert.Equal(t, http.StatusOK, status, "unseeded GetQueueAttributes must not consume the GetQueueUrl budget")

	// ...and the GetQueueUrl budget is still intact.
	status, code := getQueueURL(t, srv, "indep-q", false)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "QueueDoesNotExist", code)

	status, _ = getQueueURL(t, srv, "indep-q", false)
	assert.Equal(t, http.StatusOK, status)
}

// TestSQS_GetQueueUrl_UnseededIsInstant is the "unseeded behavior is unchanged"
// criterion: without a seed, a queue is resolvable the moment it is created.
func TestSQS_GetQueueUrl_UnseededIsInstant(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSQSQueue(t, srv, "instant-q")

		status, _ := getQueueURL(t, srv, "instant-q", jsonProto)
		assert.Equal(t, http.StatusOK, status)

		status, _ = getQueueAttributes(t, srv, queueURL, jsonProto)
		assert.Equal(t, http.StatusOK, status)
	})
}

// TestSQS_Consistency_AbsentQueueDoesNotConsumeMisses pins the ordering rule: a
// miss is consumed only when the queue actually exists. Otherwise a seed set
// before CreateQueue would be silently spent by the lookups that precede it, and
// the retry assertion the seed exists for would pass without ever exercising the
// window.
func TestSQS_Consistency_AbsentQueueDoesNotConsumeMisses(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	// Seed before the queue exists — the natural order for a harness setting up a
	// create→lookup→retry test.
	seedSQSConsistency(t, srv, map[string]any{"queueName": "later-q", "getUrlMisses": 1})

	// Genuinely absent: fails, but must not spend the budget.
	status, code := getQueueURL(t, srv, "later-q", false)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "QueueDoesNotExist", code)

	createSQSQueue(t, srv, "later-q")

	// Now the seeded window applies.
	status, code = getQueueURL(t, srv, "later-q", false)
	assert.Equal(t, http.StatusBadRequest, status, "the seeded miss must survive the absent-queue lookup")
	assert.Equal(t, "QueueDoesNotExist", code)

	status, _ = getQueueURL(t, srv, "later-q", false)
	assert.Equal(t, http.StatusOK, status)
}

// TestSQS_Consistency_DoesNotRearmOnCreateQueue pins that a seed counts down the
// next N misses rather than re-arming per create. createQueue is unconditionally
// idempotent — it early-returns the existing URL — so "after its CreateQueue" is
// ambiguous when create runs twice, and re-arming would make the data path write
// control-plane state.
func TestSQS_Consistency_DoesNotRearmOnCreateQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "rearm-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "rearm-q", "getUrlMisses": 1})

	status, _ := getQueueURL(t, srv, "rearm-q", false)
	require.Equal(t, http.StatusBadRequest, status)

	// A second CreateQueue must not restore the budget.
	createSQSQueue(t, srv, "rearm-q")

	status, _ = getQueueURL(t, srv, "rearm-q", false)
	assert.Equal(t, http.StatusOK, status, "CreateQueue must not re-arm a spent seed")
}

// TestSQS_Consistency_Wildcard covers a seed with no queueName, which applies to
// any queue — useful for a harness that wants every lookup in a suite to face the
// window without naming each queue.
func TestSQS_Consistency_Wildcard(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "wild-a")
	createSQSQueue(t, srv, "wild-b")
	seedSQSConsistency(t, srv, map[string]any{"getUrlMisses": 2})

	// The budget is on the wildcard seed itself, so the two queues share it.
	status, code := getQueueURL(t, srv, "wild-a", false)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "QueueDoesNotExist", code)

	status, _ = getQueueURL(t, srv, "wild-b", false)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = getQueueURL(t, srv, "wild-a", false)
	assert.Equal(t, http.StatusOK, status)
}

// TestSQS_Consistency_NameTakesPrecedenceOverWildcard pins the resolution order:
// an exact name match is consulted before the wildcard.
func TestSQS_Consistency_NameTakesPrecedenceOverWildcard(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "named-q")
	createSQSQueue(t, srv, "other-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "named-q", "getUrlMisses": 1})
	seedSQSConsistency(t, srv, map[string]any{"getUrlMisses": 1})

	// named-q spends its own seed, leaving the wildcard budget untouched.
	status, _ := getQueueURL(t, srv, "named-q", false)
	assert.Equal(t, http.StatusBadRequest, status)

	// A second named-q lookup falls through to the wildcard, which still has one.
	status, _ = getQueueURL(t, srv, "named-q", false)
	assert.Equal(t, http.StatusBadRequest, status,
		"an exhausted name-scoped seed must not mask a wildcard seed that still has budget")

	status, _ = getQueueURL(t, srv, "named-q", false)
	assert.Equal(t, http.StatusOK, status)

	// The wildcard is now spent, so an unrelated queue is unaffected.
	status, _ = getQueueURL(t, srv, "other-q", false)
	assert.Equal(t, http.StatusOK, status)
}

// TestSQS_Consistency_Clear covers DELETE with and without ?queueName.
func TestSQS_Consistency_Clear(t *testing.T) {
	t.Run("by name", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		createSQSQueue(t, srv, "clear-q")
		seedSQSConsistency(t, srv, map[string]any{"queueName": "clear-q", "getUrlMisses": 5})
		clearSQSConsistency(t, srv, "clear-q")

		status, _ := getQueueURL(t, srv, "clear-q", false)
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("all", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		createSQSQueue(t, srv, "clear-a")
		createSQSQueue(t, srv, "clear-b")
		seedSQSConsistency(t, srv, map[string]any{"queueName": "clear-a", "getUrlMisses": 5})
		seedSQSConsistency(t, srv, map[string]any{"queueName": "clear-b", "getUrlMisses": 5})
		seedSQSConsistency(t, srv, map[string]any{"getUrlMisses": 5})
		clearSQSConsistency(t, srv, "")

		for _, name := range []string{"clear-a", "clear-b"} {
			status, _ := getQueueURL(t, srv, name, false)
			assert.Equal(t, http.StatusOK, status, name)
		}
	})
}

// TestSQS_Consistency_RejectsNegative covers control-plane validation. A negative
// count would otherwise sit in the store forever answering "no misses left",
// looking like a seed that silently did nothing.
func TestSQS_Consistency_RejectsNegative(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{"negative getUrlMisses", map[string]any{"queueName": "neg-q", "getUrlMisses": -1}},
		{"negative getAttributesMisses", map[string]any{"queueName": "neg-q", "getAttributesMisses": -3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			body, err := json.Marshal(tt.body)
			require.NoError(t, err)
			r := httptest.NewRequest(http.MethodPost, "/v1/sqs/consistency", bytes.NewReader(body))
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, r)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

// TestSQS_Consistency_ConcurrentGetQueueUrl is the test that fails without the
// striped mutex in sqsQueueMutex. Consuming a miss is a read-modify-write and
// StateManager has no compare-and-swap, so concurrent lookups could each read the
// same count and each write count-1, consuming one miss where several were seeded.
//
// Run with -race. If this ever passes with the lock removed, it is not driving
// enough concurrency — check that before trusting it.
func TestSQS_Consistency_ConcurrentGetQueueUrl(t *testing.T) {
	const n = 32

	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "conc-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "conc-q", "getUrlMisses": n})

	var (
		mu       sync.Mutex
		misses   int
		start    = make(chan struct{})
		wg       sync.WaitGroup
		statuses = make([]int, n)
	)
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // maximize the overlap on the read-modify-write
			resp := sqsRequest(t, srv, map[string]string{"Action": "GetQueueUrl", "QueueName": "conc-q"})
			resp.Body.Close() //nolint:errcheck
			statuses[idx] = resp.StatusCode
			if resp.StatusCode == http.StatusBadRequest {
				mu.Lock()
				misses++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// Exactly the seeded number of misses: no double-spend, and none lost.
	assert.Equal(t, n, misses, "every seeded miss must be consumed exactly once")

	// The budget is now spent, so the next lookup resolves.
	status, _ := getQueueURL(t, srv, "conc-q", false)
	assert.Equal(t, http.StatusOK, status)
}

// TestSQS_Consistency_ResetClearsSeeds pins that seeds live in the state store and
// are therefore wiped by POST /v1/state/reset, which is what a harness expects
// between test cases.
func TestSQS_Consistency_ResetClearsSeeds(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "reset-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "reset-q", "getUrlMisses": 3})

	r := httptest.NewRequest(http.MethodPost, "/v1/state/reset", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "reset failed: %s", w.Body.String())

	// The queue is gone too, so recreate it and confirm no window remains.
	createSQSQueue(t, srv, "reset-q")
	status, _ := getQueueURL(t, srv, "reset-q", false)
	assert.Equal(t, http.StatusOK, status)
}
