package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// createQueueWithAttrs calls CreateQueue with attributes under the given protocol,
// returning the status and the error code (empty on success).
func createQueueWithAttrs(t *testing.T, srv *emulator.Server, name string, attrs map[string]string, jsonProto bool) (int, string) {
	t.Helper()
	if jsonProto {
		body := map[string]interface{}{"QueueName": name}
		if attrs != nil {
			body["Attributes"] = attrs
		}
		return sqsErrorCode(t, sqsJSONRequest(t, srv, "CreateQueue", body))
	}

	params := map[string]string{"Action": "CreateQueue", "QueueName": name}
	// The query protocol carries attributes as ordered Attribute.N.Name/Value
	// pairs. Sorted so the wire form is reproducible across runs rather than
	// varying with map iteration order.
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)
	for i, k := range names {
		idx := strconv.Itoa(i + 1)
		params["Attribute."+idx+".Name"] = k
		params["Attribute."+idx+".Value"] = attrs[k]
	}
	return sqsErrorCode(t, sqsRequest(t, srv, params))
}

// postSQSSeedRaw posts a raw seed body to the control plane and returns the status,
// so a test can exercise validation the typed helper would not produce.
func postSQSSeedRaw(t *testing.T, srv *emulator.Server, body string) int {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/sqs/consistency", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Code
}

// TestSQS_CreateQueue_QueueNameExists is the deterministic half of #429. AWS raises
// QueueNameExists when a name is reused with attribute values differing from the
// existing queue's; substrate returned the existing URL regardless, so a consumer's
// conflict handler was unreachable and any test of it passed vacuously.
//
// This half needs no seed because the condition is observable from state, which is
// what makes it the more valuable one: it fires on the real mistake — two stacks or
// two test cases claiming one queue name with different settings — rather than only
// when a harness remembers to arm it.
func TestSQS_CreateQueue_QueueNameExists(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)

		status, _ := createQueueWithAttrs(t, srv, "conflict-q",
			map[string]string{"VisibilityTimeout": "45"}, jsonProto)
		require.Equal(t, http.StatusOK, status, "first create must succeed")

		status, code := createQueueWithAttrs(t, srv, "conflict-q",
			map[string]string{"VisibilityTimeout": "90"}, jsonProto)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "QueueNameExists", code)
	})
}

// TestSQS_CreateQueue_IdempotentWhenAttributesMatch is the regression guard the
// issue asks for by name. The existing behavior is correct for this case and must
// stay that way: AWS documents that a request naming the same values returns the
// existing URL, and substrate's own CloudFormation re-deploy path depends on it.
func TestSQS_CreateQueue_IdempotentWhenAttributesMatch(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		tests := []struct {
			name   string
			first  map[string]string
			second map[string]string
		}{
			{
				name:   "no attributes either time",
				first:  nil,
				second: nil,
			},
			{
				name:   "identical explicit attributes",
				first:  map[string]string{"VisibilityTimeout": "45", "DelaySeconds": "5"},
				second: map[string]string{"VisibilityTimeout": "45", "DelaySeconds": "5"},
			},
			{
				// The re-create asks for what the queue observably has, even though
				// its stored attribute map is empty. GetQueueAttributes reports
				// VisibilityTimeout=30 for a queue created bare, so a request naming
				// 30 asks for no change — comparing raw map entries would wrongly
				// reject this.
				name:   "explicit value equal to the default",
				first:  nil,
				second: map[string]string{"VisibilityTimeout": "30"},
			},
			{
				// The reverse direction: created with the default named explicitly,
				// re-created without naming it.
				name:   "default omitted on the second call",
				first:  map[string]string{"VisibilityTimeout": "30"},
				second: nil,
			},
			{
				// An omitted attribute is "no opinion", not an assertion of its
				// default — so dropping one between calls is not a conflict. This is
				// what keeps CFN working: deploySQSQueue forwards only declared
				// properties, so a template that drops one sends a strict subset.
				name:   "subset of the original attributes",
				first:  map[string]string{"VisibilityTimeout": "45", "DelaySeconds": "5"},
				second: map[string]string{"VisibilityTimeout": "45"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				srv, _ := newSQSTestServer(t)

				status, _ := createQueueWithAttrs(t, srv, "idem-q", tt.first, jsonProto)
				require.Equal(t, http.StatusOK, status, "first create")

				status, code := createQueueWithAttrs(t, srv, "idem-q", tt.second, jsonProto)
				assert.Equal(t, http.StatusOK, status, "re-create must stay idempotent")
				assert.Empty(t, code)
			})
		}
	})
}

// TestSQS_CreateQueue_ConflictCases covers which differences count, the acceptance
// criterion asking for that decision to be pinned rather than left implicit.
func TestSQS_CreateQueue_ConflictCases(t *testing.T) {
	tests := []struct {
		name         string
		first        map[string]string
		second       map[string]string
		wantConflict bool
	}{
		{
			name:         "value differs from an explicit original",
			first:        map[string]string{"DelaySeconds": "5"},
			second:       map[string]string{"DelaySeconds": "10"},
			wantConflict: true,
		},
		{
			// Created bare, re-created naming a non-default value: the queue's
			// effective DelaySeconds is 0, so asking for 10 is a real change.
			name:         "value differs from a defaulted original",
			first:        nil,
			second:       map[string]string{"DelaySeconds": "10"},
			wantConflict: true,
		},
		{
			// A second attribute the first call never set, whose requested value is
			// not the default either.
			name:         "new attribute with a non-default value",
			first:        map[string]string{"VisibilityTimeout": "45"},
			second:       map[string]string{"VisibilityTimeout": "45", "MessageRetentionPeriod": "60"},
			wantConflict: true,
		},
		{
			// An attribute substrate has no default for at all: the queue has no
			// value, so any requested value differs.
			name:         "attribute with no substrate default",
			first:        nil,
			second:       map[string]string{"KmsMasterKeyId": "alias/aws/sqs"},
			wantConflict: true,
		},
		{
			// Values are compared as strings, so an equivalent-but-differently-spelled
			// number is a conflict. Documented rather than normalized: an SDK or
			// template re-sending its own serialization always matches, which is the
			// case that has to work.
			name:         "numerically equal but textually different",
			first:        map[string]string{"DelaySeconds": "5"},
			second:       map[string]string{"DelaySeconds": "05"},
			wantConflict: true,
		},
		{
			// Policy is compared as text for the same reason.
			name:         "policy differing only in whitespace",
			first:        map[string]string{"Policy": `{"Version":"2012-10-17"}`},
			second:       map[string]string{"Policy": `{ "Version": "2012-10-17" }`},
			wantConflict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)

			status, _ := createQueueWithAttrs(t, srv, "cases-q", tt.first, false)
			require.Equal(t, http.StatusOK, status, "first create")

			status, code := createQueueWithAttrs(t, srv, "cases-q", tt.second, false)
			if tt.wantConflict {
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "QueueNameExists", code)
				return
			}
			assert.Equal(t, http.StatusOK, status)
			assert.Empty(t, code)
		})
	}
}

// TestSQS_CreateQueue_FifoQueueAttributeIsNotAConflict pins that re-creating a FIFO
// queue with FifoQueue=true — what every SDK and CloudFormation template sends —
// stays idempotent. Substrate derives the flag from the ".fifo" suffix and never
// copies it into the attribute map, so a naive comparison would see "true" against
// an absent value and reject the most ordinary FIFO re-create there is.
func TestSQS_CreateQueue_FifoQueueAttributeIsNotAConflict(t *testing.T) {
	// The first create names no attributes, so nothing lands in the queue's map and
	// the flag is derived from the suffix alone. That is the case the special-casing
	// exists for: a create that *does* pass FifoQueue stores it, and would compare
	// fine from the map without any of this.
	t.Run("derived from the name suffix", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		require.Equal(t, http.StatusOK, mustCreate(t, srv, "orders.fifo", nil), "first create")

		status, code := createQueueWithAttrs(t, srv, "orders.fifo",
			map[string]string{"FifoQueue": "true"}, false)
		assert.Equal(t, http.StatusOK, status, "re-creating a FIFO queue must stay idempotent")
		assert.Empty(t, code)

		// Claiming a FIFO queue is standard is a genuine conflict, and one AWS calls
		// out as impossible to change after creation.
		status, code = createQueueWithAttrs(t, srv, "orders.fifo",
			map[string]string{"FifoQueue": "false"}, false)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "QueueNameExists", code)
	})

	// The mirror image: a standard queue must not accept a FifoQueue=true claim,
	// which needs the false branch of the same derivation.
	t.Run("standard queue claimed as FIFO", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		require.Equal(t, http.StatusOK, mustCreate(t, srv, "plain-q", nil), "first create")

		status, code := createQueueWithAttrs(t, srv, "plain-q",
			map[string]string{"FifoQueue": "false"}, false)
		assert.Equal(t, http.StatusOK, status, "FifoQueue=false on a standard queue is no change")
		assert.Empty(t, code)

		status, code = createQueueWithAttrs(t, srv, "plain-q",
			map[string]string{"FifoQueue": "true"}, false)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "QueueNameExists", code)
	})

	// And the ordinary SDK shape, where the attribute is passed both times.
	t.Run("attribute passed on both creates", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		require.Equal(t, http.StatusOK,
			mustCreate(t, srv, "events.fifo", map[string]string{"FifoQueue": "true"}))

		status, code := createQueueWithAttrs(t, srv, "events.fifo",
			map[string]string{"FifoQueue": "true"}, false)
		assert.Equal(t, http.StatusOK, status)
		assert.Empty(t, code)
	})
}

// TestSQS_CreateQueue_ConflictNamesTheAttribute asserts the message identifies which
// attribute differs. AWS's own text does not, and without it the error is nearly
// undiagnosable for a caller holding a large attribute set — the documented prefix
// is still present, so a consumer matching on either the code or that text is
// unaffected.
func TestSQS_CreateQueue_ConflictNamesTheAttribute(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	status, _ := createQueueWithAttrs(t, srv, "named-conflict-q",
		map[string]string{"VisibilityTimeout": "45", "DelaySeconds": "5"}, false)
	require.Equal(t, http.StatusOK, status)

	resp := sqsRequest(t, srv, map[string]string{
		"Action": "CreateQueue", "QueueName": "named-conflict-q",
		"Attribute.1.Name": "DelaySeconds", "Attribute.1.Value": "9",
	})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body := readJSONBody(t, resp)
	assert.Equal(t, "QueueNameExists", body["__type"])
	message, _ := body["message"].(string)
	if message == "" {
		message, _ = body["Message"].(string)
	}
	assert.Contains(t, message, "A queue with this name already exists",
		"AWS's documented wording must survive")
	assert.Contains(t, message, "DelaySeconds", "the offending attribute must be named")
}

// TestSQS_CreateQueue_ConflictIsDeterministic pins that the attribute named when
// several differ is stable. A message that varied with map iteration order would be
// irreproducible across runs, which is the opposite of what this emulator exists
// for — a consumer asserting on it would see a flaky test.
func TestSQS_CreateQueue_ConflictIsDeterministic(t *testing.T) {
	const runs = 20
	for range runs {
		srv, _ := newSQSTestServer(t)
		require.Equal(t, http.StatusOK, mustCreate(t, srv, "det-q", map[string]string{
			"VisibilityTimeout": "45", "DelaySeconds": "5", "MessageRetentionPeriod": "600",
		}))

		resp := sqsRequest(t, srv, map[string]string{
			"Action": "CreateQueue", "QueueName": "det-q",
			"Attribute.1.Name": "VisibilityTimeout", "Attribute.1.Value": "99",
			"Attribute.2.Name": "DelaySeconds", "Attribute.2.Value": "99",
			"Attribute.3.Name": "MessageRetentionPeriod", "Attribute.3.Value": "99",
		})
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		message, _ := readJSONBody(t, resp)["message"].(string)
		// Sorted order, so the first differing attribute is always DelaySeconds.
		assert.Contains(t, message, "DelaySeconds")
	}
}

// mustCreate creates a queue with attributes and returns the status.
func mustCreate(t *testing.T, srv *emulator.Server, name string, attrs map[string]string) int {
	t.Helper()
	status, _ := createQueueWithAttrs(t, srv, name, attrs, false)
	return status
}

// TestSQS_CreateQueue_ConflictCreatesNothing asserts a rejected create leaves the
// existing queue untouched. A consumer that catches QueueNameExists and inspects the
// queue has to find the original settings, not a half-applied merge.
func TestSQS_CreateQueue_ConflictCreatesNothing(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createSQSQueue(t, srv, "intact-q")

	require.Equal(t, http.StatusOK, mustCreate(t, srv, "intact-q",
		map[string]string{"VisibilityTimeout": "30"}))

	status, code := createQueueWithAttrs(t, srv, "intact-q",
		map[string]string{"VisibilityTimeout": "77"}, false)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "QueueNameExists", code)

	resp := sqsRequest(t, srv, map[string]string{
		"Action": "GetQueueAttributes", "QueueUrl": queueURL,
		"AttributeName.1": "All",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, body, "<Value>30</Value>", "the original attributes must survive")
	assert.NotContains(t, body, "<Value>77</Value>", "a rejected create must not apply anything")
}

// TestSQS_CreateQueue_SeededQueueDeletedRecently is the seeded half of #429. AWS
// requires a 60-second wait after DeleteQueue before the name can be reused, so a
// consumer's delete→recreate→retry loop has a real error branch. Substrate keeps no
// memory of a delete, which made that branch unreachable.
//
// The budget is counted rather than timed for the same reason #413's is:
// TimeController.Now advances with wall time, so a duration window would expire
// partway through a test and make the assertion depend on how long the rest of the
// test took.
func TestSQS_CreateQueue_SeededQueueDeletedRecently(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSQSQueue(t, srv, "recreate-q")

		resp := sqsRequest(t, srv, map[string]string{"Action": "DeleteQueue", "QueueUrl": queueURL})
		resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode, "delete queue")

		seedSQSConsistency(t, srv, map[string]any{
			"queueName": "recreate-q", "deletedRecentlyMisses": 2,
		})

		for i := 1; i <= 2; i++ {
			status, code := createQueueWithAttrs(t, srv, "recreate-q", nil, jsonProto)
			assert.Equal(t, http.StatusBadRequest, status, "create %d must be refused", i)
			assert.Equal(t, "QueueDeletedRecently", code, "create %d", i)
		}

		// Budget exhausted: the name is reusable, and the queue really is created.
		status, _ := createQueueWithAttrs(t, srv, "recreate-q", nil, jsonProto)
		require.Equal(t, http.StatusOK, status, "create 3 must succeed once the window closes")

		status, _ = getQueueURL(t, srv, "recreate-q", jsonProto)
		assert.Equal(t, http.StatusOK, status, "the queue must exist after the successful create")
	})
}

// TestSQS_CreateQueue_DeletedRecentlyDoesNotApplyToExistingQueue pins that the seed
// only fires while the name is free. AWS raises QueueDeletedRecently for a name too
// recently freed, so an existing queue is the one case it cannot describe — and
// spending the budget on an idempotent re-create would leave the delete→recreate
// assertion the seed exists for meaningless.
func TestSQS_CreateQueue_DeletedRecentlyDoesNotApplyToExistingQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createSQSQueue(t, srv, "live-q")
	seedSQSConsistency(t, srv, map[string]any{"queueName": "live-q", "deletedRecentlyMisses": 1})

	// The queue exists, so this is an idempotent hit and must not consume the seed.
	status, code := createQueueWithAttrs(t, srv, "live-q", nil, false)
	require.Equal(t, http.StatusOK, status, "an existing queue must not be refused")
	require.Empty(t, code)

	resp := sqsRequest(t, srv, map[string]string{"Action": "DeleteQueue", "QueueUrl": queueURL})
	resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Now the name is free and the budget is still intact.
	status, code = createQueueWithAttrs(t, srv, "live-q", nil, false)
	assert.Equal(t, http.StatusBadRequest, status, "the seeded miss must have survived")
	assert.Equal(t, "QueueDeletedRecently", code)
}

// TestSQS_CreateQueue_UnseededRecreateIsInstant is the "unseeded behavior unchanged"
// criterion: without a seed, a deleted name is immediately reusable.
func TestSQS_CreateQueue_UnseededRecreateIsInstant(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSQSQueue(t, srv, "fast-q")

		resp := sqsRequest(t, srv, map[string]string{"Action": "DeleteQueue", "QueueUrl": queueURL})
		resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode)

		status, code := createQueueWithAttrs(t, srv, "fast-q", nil, jsonProto)
		assert.Equal(t, http.StatusOK, status)
		assert.Empty(t, code)
	})
}

// TestSQS_CreateQueue_DeletedRecentlyCounterIsIndependent pins that the new counter
// does not disturb #413's two, so a test can seed a recreate window without also
// perturbing its lookups.
func TestSQS_CreateQueue_DeletedRecentlyCounterIsIndependent(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	createSQSQueue(t, srv, "indep-create-q")
	seedSQSConsistency(t, srv, map[string]any{
		"queueName": "indep-create-q", "deletedRecentlyMisses": 1,
	})

	// The lookup counters were not seeded, so lookups resolve normally...
	status, _ := getQueueURL(t, srv, "indep-create-q", false)
	assert.Equal(t, http.StatusOK, status, "an unseeded lookup must not consume the create budget")

	// ...and the create budget is still intact once the name is free.
	queueURL := "http://sqs.us-east-1.localhost/123456789012/indep-create-q"
	resp := sqsRequest(t, srv, map[string]string{"Action": "DeleteQueue", "QueueUrl": queueURL})
	resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	status, code := createQueueWithAttrs(t, srv, "indep-create-q", nil, false)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "QueueDeletedRecently", code)
}

// TestSQS_CreateQueue_DeletedRecentlyWildcard covers a seed with no queueName, for a
// harness that wants every recreate in a suite to face the window.
func TestSQS_CreateQueue_DeletedRecentlyWildcard(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	seedSQSConsistency(t, srv, map[string]any{"deletedRecentlyMisses": 1})

	// No queue of this name has ever existed, so the wildcard applies to a first
	// create too — the emulator cannot know whether a name was "recently deleted",
	// which is exactly why the condition is seeded rather than inferred.
	status, code := createQueueWithAttrs(t, srv, "wild-create-q", nil, false)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "QueueDeletedRecently", code)

	status, _ = createQueueWithAttrs(t, srv, "wild-create-q", nil, false)
	assert.Equal(t, http.StatusOK, status, "the budget is spent, so the create proceeds")
}

// TestSQS_CreateQueue_RejectsNegativeDeletedRecently covers control-plane validation
// on the new counter. A negative value would sit in the store answering "no misses
// left" forever, looking like a seed that silently did nothing.
func TestSQS_CreateQueue_RejectsNegativeDeletedRecently(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	status := postSQSSeedRaw(t, srv, `{"queueName":"neg-q","deletedRecentlyMisses":-1}`)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestSQS_CreateQueue_DeletedRecentlyStateFailure asserts a store failure during the
// seed lookup propagates rather than collapsing into a successful create. Creating
// the queue anyway would hide the fault and hand back a URL for a queue whose seed
// state is unknown.
func TestSQS_CreateQueue_DeletedRecentlyStateFailure(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	srv, _ := newSQSTestServerWithState(t, state)
	require.NoError(t, emulator.SQSPutRawSeedForTest(state, "corrupt-create-q", []byte("{not json")))

	status, code := createQueueWithAttrs(t, srv, "corrupt-create-q", nil, false)
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.NotEqual(t, "QueueDeletedRecently", code)
}

// sqsQueueAttribute reads one attribute from GetQueueAttributes under either
// protocol, so a default can be asserted against the exact value rather than mere
// presence.
func sqsQueueAttribute(t *testing.T, srv *emulator.Server, name, attr string, jsonProto bool) string {
	t.Helper()
	queueURL := "http://sqs.us-east-1.localhost/123456789012/" + name
	if jsonProto {
		resp := sqsJSONRequest(t, srv, "GetQueueAttributes", map[string]interface{}{
			"QueueUrl":       queueURL,
			"AttributeNames": []string{"All"},
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		attrs, ok := readJSONBody(t, resp)["Attributes"].(map[string]interface{})
		require.True(t, ok, "response carries an Attributes object")
		v, _ := attrs[attr].(string)
		return v
	}

	resp := sqsRequest(t, srv, map[string]string{"Action": "GetQueueAttributes", "QueueUrl": queueURL})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Attributes []struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &out))
	for _, a := range out.Attributes {
		if a.Name == attr {
			return a.Value
		}
	}
	return ""
}

// TestSQS_CreateQueue_MaximumMessageSizeDefault is #439. GetQueueAttributes reported
// 262144 (256 KiB) for a queue created without the attribute; the current CreateQueue
// reference documents 1,048,576 bytes (1 MiB) as the default, and 256 KiB is the
// historical limit. A consumer sizing a payload against what substrate reported was
// working from a number a real queue would not give it.
//
// This covers the reported default only. Enforcement of it on SendMessage came later
// (#454) and is covered by TestSQS_SendMessage_EffectiveLimitResolvesThroughDefault,
// which asserts the enforced limit is the value this test pins.
func TestSQS_CreateQueue_MaximumMessageSizeDefault(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		t.Run("a bare queue reports 1 MiB", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			status, _ := createQueueWithAttrs(t, srv, "mms-default", nil, jsonProto)
			require.Equal(t, http.StatusOK, status)

			assert.Equal(t, "1048576", sqsQueueAttribute(t, srv, "mms-default", "MaximumMessageSize", jsonProto),
				"the documented CreateQueue default")
		})

		t.Run("an explicit value is still honored", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			status, _ := createQueueWithAttrs(t, srv, "mms-explicit",
				map[string]string{"MaximumMessageSize": "262144"}, jsonProto)
			require.Equal(t, http.StatusOK, status)

			// Correcting the default must not start overriding a caller's own value —
			// 256 KiB is still a legal size, it is simply no longer the default.
			assert.Equal(t, "262144", sqsQueueAttribute(t, srv, "mms-explicit", "MaximumMessageSize", jsonProto))
		})

		// This is the case that fails if only one of the two default sites moves.
		// CreateQueue's conflict check (#429) resolves an existing queue's unset
		// attributes through sqsAttributeDefaults, while GetQueueAttributes reads its
		// own inline fallback. If they disagree, a request naming exactly the value a
		// caller just read back is rejected as QueueNameExists.
		t.Run("re-creating with the reported default stays idempotent", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			status, _ := createQueueWithAttrs(t, srv, "mms-idem", nil, jsonProto)
			require.Equal(t, http.StatusOK, status)

			reported := sqsQueueAttribute(t, srv, "mms-idem", "MaximumMessageSize", jsonProto)
			status, code := createQueueWithAttrs(t, srv, "mms-idem",
				map[string]string{"MaximumMessageSize": reported}, jsonProto)
			assert.Equal(t, http.StatusOK, status,
				"re-creating with the value GetQueueAttributes reported must be idempotent")
			assert.Empty(t, code)
		})
	})
}
