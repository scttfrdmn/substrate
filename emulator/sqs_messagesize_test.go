package emulator_test

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// sqsMessageAttr is one message attribute in a send request, in the form each
// protocol needs it.
type sqsMessageAttr struct {
	name        string
	dataType    string
	stringValue string
	binaryValue []byte
}

// sendMessageWithAttrs sends a message under either protocol and returns the status
// and error code, so a size rejection can be asserted on the code rather than merely
// on failure.
func sendMessageWithAttrs(t *testing.T, srv *emulator.Server, queueURL, body string,
	attrs []sqsMessageAttr, jsonProto bool,
) (int, string) {
	t.Helper()
	if jsonProto {
		in := map[string]interface{}{"QueueUrl": queueURL, "MessageBody": body}
		if len(attrs) > 0 {
			m := make(map[string]interface{}, len(attrs))
			for _, a := range attrs {
				v := map[string]interface{}{"DataType": a.dataType}
				if a.stringValue != "" {
					v["StringValue"] = a.stringValue
				}
				if a.binaryValue != nil {
					// encoding/json marshals []byte as base64, which is the wire form,
					// so the emulator's []byte field round-trips without help here.
					v["BinaryValue"] = a.binaryValue
				}
				m[a.name] = v
			}
			in["MessageAttributes"] = m
		}
		return sqsErrorCode(t, sqsJSONRequest(t, srv, "SendMessage", in))
	}

	params := map[string]string{"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": body}
	for i, a := range attrs {
		base := "MessageAttribute." + strconv.Itoa(i+1) + "."
		params[base+"Name"] = a.name
		params[base+"Value.DataType"] = a.dataType
		if a.stringValue != "" {
			params[base+"Value.StringValue"] = a.stringValue
		}
		if a.binaryValue != nil {
			params[base+"Value.BinaryValue"] = base64.StdEncoding.EncodeToString(a.binaryValue)
		}
	}
	return sqsErrorCode(t, sqsRequest(t, srv, params))
}

// sendMessage sends a body with no attributes.
func sendMessage(t *testing.T, srv *emulator.Server, queueURL, body string, jsonProto bool) (int, string) {
	t.Helper()
	return sendMessageWithAttrs(t, srv, queueURL, body, nil, jsonProto)
}

// sqsBatchEntry is one SendMessageBatch entry.
type sqsBatchEntry struct {
	id    string
	body  string
	attrs []sqsMessageAttr
}

// sendMessageBatch sends a batch under either protocol.
func sendMessageBatch(t *testing.T, srv *emulator.Server, queueURL string,
	entries []sqsBatchEntry, jsonProto bool,
) (int, string) {
	t.Helper()
	if jsonProto {
		list := make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			entry := map[string]interface{}{"Id": e.id, "MessageBody": e.body}
			if len(e.attrs) > 0 {
				m := make(map[string]interface{}, len(e.attrs))
				for _, a := range e.attrs {
					m[a.name] = map[string]interface{}{
						"DataType": a.dataType, "StringValue": a.stringValue,
					}
				}
				entry["MessageAttributes"] = m
			}
			list = append(list, entry)
		}
		return sqsErrorCode(t, sqsJSONRequest(t, srv, "SendMessageBatch",
			map[string]interface{}{"QueueUrl": queueURL, "Entries": list}))
	}

	params := map[string]string{"Action": "SendMessageBatch", "QueueUrl": queueURL}
	for i, e := range entries {
		base := "SendMessageBatchRequestEntry." + strconv.Itoa(i+1) + "."
		params[base+"Id"] = e.id
		params[base+"MessageBody"] = e.body
		for j, a := range e.attrs {
			ab := base + "MessageAttribute." + strconv.Itoa(j+1) + "."
			params[ab+"Name"] = a.name
			params[ab+"Value.DataType"] = a.dataType
			params[ab+"Value.StringValue"] = a.stringValue
		}
	}
	return sqsErrorCode(t, sqsRequest(t, srv, params))
}

// sqsQueueDepth counts the messages a queue will deliver, by receiving up to max and
// returning how many came back. Used to assert that a rejected send enqueued nothing
// — a 400 that still stored the message would leave the queue in a state real AWS
// never produces, and asserting only on the status code would not notice.
func sqsQueueDepth(t *testing.T, srv *emulator.Server, queueURL string) int {
	t.Helper()
	resp := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return strings.Count(readBody(t, resp), "<MessageId>")
}

// createSizedQueue creates a queue with an explicit MaximumMessageSize and returns
// its URL.
func createSizedQueue(t *testing.T, srv *emulator.Server, name, maxSize string) string {
	t.Helper()
	status, code := createQueueWithAttrs(t, srv, name,
		map[string]string{"MaximumMessageSize": maxSize}, false)
	require.Equal(t, http.StatusOK, status, "create failed: %s", code)
	return "http://sqs.us-east-1.localhost/000000000000/" + name
}

// TestSQS_SendMessage_EnforcesMaximumMessageSize is #454. No length check existed
// anywhere in the send path, so a body of any size was accepted and delivered. Real
// SQS rejects one over the queue's MaximumMessageSize with InvalidParameterValue,
// HTTP 400 — which made a consumer's too-large-payload branch (chunk, compress, spill
// to S3) unreachable, and any test of it green while verifying nothing.
//
// The error code is not derivable from the API model: SendMessage declares no
// oversized-message error, its InvalidMessageContents is a character-set error, and
// BatchRequestTooLong is declared only on SendMessageBatch. The code asserted here is
// the one a captured real-AWS SDK error carries.
func TestSQS_SendMessage_EnforcesMaximumMessageSize(t *testing.T) {
	const limit = 4096

	tests := []struct {
		name     string
		bodyLen  int
		wantCode string
	}{
		{
			// The inclusive boundary. AWS's wording is "must be shorter than N bytes",
			// but N is the documented maximum *size*, so exactly N is legal — an
			// exclusive reading would reject the largest legal message and send a
			// consumer chunking a payload that needs no chunking.
			name:    "exactly at the limit is accepted",
			bodyLen: limit,
		},
		{
			name:     "one byte over is rejected",
			bodyLen:  limit + 1,
			wantCode: "InvalidParameterValue",
		},
		{
			name:    "one byte under is accepted",
			bodyLen: limit - 1,
		},
		{
			name:     "far over is rejected",
			bodyLen:  limit * 4,
			wantCode: "InvalidParameterValue",
		},
		{
			// A body of "" is legal here even though AWS documents a one-character
			// minimum: that is a separate validation from the size cap, and folding it
			// in would make this change reject sends #454 says nothing about.
			name:    "empty body is not a size failure",
			bodyLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				srv, _ := newSQSTestServer(t)
				queueURL := createSizedQueue(t, srv, "sized-q", strconv.Itoa(limit))

				status, code := sendMessage(t, srv, queueURL, strings.Repeat("x", tt.bodyLen), jsonProto)

				if tt.wantCode == "" {
					assert.Equal(t, http.StatusOK, status)
					assert.Empty(t, code)
					assert.Equal(t, 1, sqsQueueDepth(t, srv, queueURL), "an accepted message is enqueued")
					return
				}

				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, tt.wantCode, code)
				assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL),
					"a rejected message must not be enqueued")
			})
		})
	}
}

// TestSQS_SendMessage_EffectiveLimitResolvesThroughDefault pins that the enforced
// limit is the *effective* one: a queue created without the attribute enforces the
// 1 MiB default (#439), not no limit at all and not a second hardcoded copy.
//
// Reading through the same default the read path uses is what keeps the number
// GetQueueAttributes reports identical to the number SendMessage enforces. A consumer
// that sizes payloads from the reported attribute would otherwise have them rejected
// anyway.
func TestSQS_SendMessage_EffectiveLimitResolvesThroughDefault(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSQSQueue(t, srv, "bare-q")

		reported := sqsQueueAttribute(t, srv, "bare-q", "MaximumMessageSize", jsonProto)
		require.Equal(t, "1048576", reported, "the #439 default")
		limit, err := strconv.Atoi(reported)
		require.NoError(t, err)

		status, code := sendMessage(t, srv, queueURL, strings.Repeat("x", limit), jsonProto)
		assert.Equal(t, http.StatusOK, status, "exactly the reported limit must be accepted")
		assert.Empty(t, code)

		status, code = sendMessage(t, srv, queueURL, strings.Repeat("x", limit+1), jsonProto)
		assert.Equal(t, http.StatusBadRequest, status,
			"a bare queue enforces the default rather than no limit")
		assert.Equal(t, "InvalidParameterValue", code)
	})
}

// TestSQS_SendMessage_ExplicitSmallerLimitIsEnforced covers the case that separates
// enforcing the queue's attribute from enforcing a constant: a body legal under the
// 1 MiB default must still be rejected by a queue configured smaller. A constant-based
// check passes every other test in this file and fails this one.
func TestSQS_SendMessage_ExplicitSmallerLimitIsEnforced(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		small := createSizedQueue(t, srv, "small-q", "1024")
		big := createSQSQueue(t, srv, "big-q")

		body := strings.Repeat("x", 2048) // legal at 1 MiB, over the 1 KiB attribute

		status, code := sendMessage(t, srv, small, body, jsonProto)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)

		status, code = sendMessage(t, srv, big, body, jsonProto)
		assert.Equal(t, http.StatusOK, status, "the same body is legal on a default queue")
		assert.Empty(t, code)
	})
}

// TestSQS_SendMessage_ErrorMessageNamesTheLimit pins that the message interpolates the
// limit that actually applied. It is the only place a caller learns which limit they
// hit, and reporting 1 MiB for a queue that rejected at 1 KiB would send them looking
// for a bug in their own sizing.
func TestSQS_SendMessage_ErrorMessageNamesTheLimit(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createSizedQueue(t, srv, "msg-q", "1024")

	resp := sqsRequest(t, srv, map[string]string{
		"Action": "SendMessage", "QueueUrl": queueURL,
		"MessageBody": strings.Repeat("x", 2048),
	})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body := readBody(t, resp)

	assert.Contains(t, body, "Message must be shorter than 1024 bytes",
		"the observed real-AWS wording, naming the queue's own limit")
	assert.NotContains(t, body, "1048576", "the default must not be reported for a 1 KiB queue")
}

// TestSQS_SendMessage_AttributesCountTowardTheLimit is the acceptance criterion that
// size is measured the way AWS measures it. The developer guide is explicit: "All
// components of a message attribute are included in the 1 MiB message size
// restriction", and the per-component breakdown — name, data type, and value — is the
// one AWS's own Extended Client Library uses to decide whether a payload needs
// offloading to S3.
//
// Measuring the body alone is wrong in the direction that matters: a caller packing
// most of the budget into metadata is over the limit in AWS and under it here, so the
// payload shape that motivates a chunking branch is the one substrate would accept.
func TestSQS_SendMessage_AttributesCountTowardTheLimit(t *testing.T) {
	const limit = 1024

	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		t.Run("a body under the limit plus attributes over it is rejected", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "attr-q", strconv.Itoa(limit))

			// The body alone fits comfortably; body + attribute does not.
			status, code := sendMessageWithAttrs(t, srv, queueURL, strings.Repeat("x", 600),
				[]sqsMessageAttr{{name: "meta", dataType: "String", stringValue: strings.Repeat("y", 600)}},
				jsonProto)

			assert.Equal(t, http.StatusBadRequest, status,
				"attributes count toward the total, so this is over the limit")
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))
		})

		t.Run("body plus attributes exactly at the limit is accepted", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "attr-exact-q", strconv.Itoa(limit))

			// name(4) + dataType(6) + value = the attribute's contribution, so the
			// body takes the remainder. Computed rather than written as a literal so
			// the arithmetic is visible: this is the inclusive boundary *with*
			// attributes, which is where an off-by-one in the size formula shows up.
			const attrName, attrType = "meta", "String"
			attrValue := strings.Repeat("y", 100)
			attrSize := len(attrName) + len(attrType) + len(attrValue)
			body := strings.Repeat("x", limit-attrSize)

			status, code := sendMessageWithAttrs(t, srv, queueURL, body,
				[]sqsMessageAttr{{name: attrName, dataType: attrType, stringValue: attrValue}},
				jsonProto)

			assert.Equal(t, http.StatusOK, status, "exactly at the limit is legal: %s", code)
			assert.Empty(t, code)

			// One more body byte crosses it, which proves the boundary above was the
			// real edge rather than merely somewhere under it.
			status, code = sendMessageWithAttrs(t, srv, queueURL, body+"x",
				[]sqsMessageAttr{{name: attrName, dataType: attrType, stringValue: attrValue}},
				jsonProto)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})

		t.Run("multiple attributes accumulate", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "attr-multi-q", strconv.Itoa(limit))

			// Each attribute alone leaves room — 400 bytes of a 1024 limit — but three
			// together do not. A formula that measured only the first, or that stopped
			// at the first index, would accept this.
			attrs := []sqsMessageAttr{
				{name: "a1", dataType: "String", stringValue: strings.Repeat("y", 400)},
				{name: "a2", dataType: "String", stringValue: strings.Repeat("y", 400)},
				{name: "a3", dataType: "String", stringValue: strings.Repeat("y", 400)},
			}
			for _, a := range attrs {
				require.Less(t, len(a.name)+len(a.dataType)+len(a.stringValue), limit,
					"each attribute alone must fit, or this proves nothing about accumulation")
			}

			status, code := sendMessageWithAttrs(t, srv, queueURL, "small", attrs, jsonProto)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	})
}

// TestSQS_SendMessage_BinaryAttributeCountsRawBytes pins that a binary attribute is
// measured as its decoded length, per AWS's size calculation
// ("binaryVal.asByteArray().length"), not as its base64 transport encoding — which is
// ~33% larger and would reject messages AWS accepts.
func TestSQS_SendMessage_BinaryAttributeCountsRawBytes(t *testing.T) {
	const limit = 1024

	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSizedQueue(t, srv, "bin-q", strconv.Itoa(limit))

		// 780 raw bytes encode to 1040 base64 characters, so the two readings
		// straddle the 1024 limit: legal measured raw, over it measured encoded.
		const attrName, attrType, body = "blob", "Binary", "small"
		overhead := len(attrName) + len(attrType) + len(body)
		raw := make([]byte, 780)
		for i := range raw {
			raw[i] = byte(i % 251)
		}

		// Both bounds are asserted, because either one alone leaves the test unable to
		// fail for the right reason: without the first it might pass by being small
		// enough either way, and without the second the encoded reading would not
		// actually cross the limit.
		require.LessOrEqual(t, len(raw)+overhead, limit,
			"measured raw, this message must be legal")
		require.Greater(t, len(base64.StdEncoding.EncodeToString(raw))+overhead, limit,
			"measured base64-encoded, it must exceed the limit — otherwise the two readings agree")

		status, code := sendMessageWithAttrs(t, srv, queueURL, body,
			[]sqsMessageAttr{{name: attrName, dataType: attrType, binaryValue: raw}}, jsonProto)

		assert.Equal(t, http.StatusOK, status, "raw bytes are what count: %s", code)
		assert.Empty(t, code)
	})
}

// TestSQS_SendMessage_FifoQueueEnforcesBeforeDedup covers the ordering the size check
// deliberately takes: ahead of the FIFO branch.
//
// A FIFO send that recorded its deduplication ID and *then* failed would poison the
// dedup window — the corrected retry, carrying the same MessageDeduplicationId with a
// smaller body, would be swallowed as a duplicate and silently never delivered. That
// is a worse failure than the one being fixed, because it looks like success.
func TestSQS_SendMessage_FifoQueueEnforcesBeforeDedup(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	status, code := createQueueWithAttrs(t, srv, "size-q.fifo", map[string]string{
		"FifoQueue": "true", "MaximumMessageSize": "1024",
	}, false)
	require.Equal(t, http.StatusOK, status, code)
	queueURL := "http://sqs.us-east-1.localhost/000000000000/size-q.fifo"

	send := func(body string) (int, string) {
		return sqsErrorCode(t, sqsRequest(t, srv, map[string]string{
			"Action": "SendMessage", "QueueUrl": queueURL,
			"MessageBody": body, "MessageGroupId": "g1", "MessageDeduplicationId": "d1",
		}))
	}

	status, code = send(strings.Repeat("x", 2048))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValue", code)

	// The retry reuses the dedup ID, as a real client would after fixing the payload.
	status, code = send("ok")
	assert.Equal(t, http.StatusOK, status, code)
	assert.Equal(t, 1, sqsQueueDepth(t, srv, queueURL),
		"the rejected send must not have consumed the deduplication ID")
}

// TestSQS_SendMessageBatch_EnforcesSizes covers the batch limits. AWS states that "the
// maximum allowed individual message size and the maximum total payload size (the sum
// of the individual lengths of all of the batched messages) are both 1 MiB", so both
// apply and both are covered here.
//
// The codes differ by which limit is breached, and the ordering matters: because the
// two limits are equal on a default queue, a single oversized entry breaches the total
// as well, and real AWS reports BatchRequestTooLong for that case rather than the
// per-message error.
func TestSQS_SendMessageBatch_EnforcesSizes(t *testing.T) {
	t.Run("batch total over the cap is BatchRequestTooLong", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSQSQueue(t, srv, "batch-total-q")

			// Ten entries, each legal on its own at 1 MiB, summing to over 1 MiB. This
			// is the case a per-message-only check accepts.
			entries := make([]sqsBatchEntry, 0, 10)
			for i := 1; i <= 10; i++ {
				entries = append(entries, sqsBatchEntry{
					id:   "e" + strconv.Itoa(i),
					body: strings.Repeat("x", 200_000),
				})
			}

			status, code := sendMessageBatch(t, srv, queueURL, entries, jsonProto)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "BatchRequestTooLong", code)
			assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL),
				"a rejected batch must enqueue nothing, not a prefix of its entries")
		})
	})

	t.Run("one oversized entry on a default queue is BatchRequestTooLong", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSQSQueue(t, srv, "batch-one-big-q")

			status, code := sendMessageBatch(t, srv, queueURL, []sqsBatchEntry{
				{id: "small", body: "ok"},
				{id: "big", body: strings.Repeat("x", 1_048_577)},
			}, jsonProto)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "BatchRequestTooLong", code,
				"the equal limits mean the total is breached too, and AWS reports this code")
			assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))
		})
	})

	t.Run("per-entry limit from the queue attribute", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "batch-per-entry-q", "1024")

			// Well under the 1 MiB request cap, so only the per-message limit can
			// reject this. That is what makes both checks independently reachable.
			status, code := sendMessageBatch(t, srv, queueURL, []sqsBatchEntry{
				{id: "ok", body: "small"},
				{id: "over", body: strings.Repeat("x", 2048)},
			}, jsonProto)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))
		})
	})

	t.Run("a small queue attribute does not lower the request cap", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "batch-small-attr-q", "1024")

			// Ten entries at 1 KiB each: every entry is exactly at the per-message
			// limit and the total is ~10 KiB, far under the 1 MiB request cap. AWS
			// documents MaximumMessageSize as a per-message attribute, so this must be
			// accepted — a batch check that compared the *total* against the queue
			// attribute would reject it.
			entries := make([]sqsBatchEntry, 0, 10)
			for i := 1; i <= 10; i++ {
				entries = append(entries, sqsBatchEntry{
					id:   "e" + strconv.Itoa(i),
					body: strings.Repeat("x", 1024),
				})
			}

			status, code := sendMessageBatch(t, srv, queueURL, entries, jsonProto)
			assert.Equal(t, http.StatusOK, status, code)
			assert.Empty(t, code)
			assert.Equal(t, 10, sqsQueueDepth(t, srv, queueURL))
		})
	})

	t.Run("entry attributes count toward its size", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSizedQueue(t, srv, "batch-attr-q", "1024")

			status, code := sendMessageBatch(t, srv, queueURL, []sqsBatchEntry{
				{id: "ok", body: "small"},
				{
					id:   "over",
					body: strings.Repeat("x", 600),
					attrs: []sqsMessageAttr{
						{name: "meta", dataType: "String", stringValue: strings.Repeat("y", 600)},
					},
				},
			}, jsonProto)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	})

	t.Run("a legal batch still succeeds", func(t *testing.T) {
		bothProtocols(t, func(t *testing.T, jsonProto bool) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSQSQueue(t, srv, "batch-ok-q")

			status, code := sendMessageBatch(t, srv, queueURL, []sqsBatchEntry{
				{id: "a", body: "one"},
				{id: "b", body: "two", attrs: []sqsMessageAttr{
					{name: "k", dataType: "String", stringValue: "v"},
				}},
			}, jsonProto)

			assert.Equal(t, http.StatusOK, status, code)
			assert.Empty(t, code)
			assert.Equal(t, 2, sqsQueueDepth(t, srv, queueURL),
				"adding the size check must not drop legal entries")
		})
	})
}

// TestSQS_SendMessage_MalformedAttributeFallsBackToDefault pins that an unparseable
// MaximumMessageSize resolves to the default rather than to zero. Zero would reject
// every message including an empty one, turning a bad attribute value into a queue
// that accepts nothing — CreateQueue is where an invalid value belongs, not every
// subsequent send.
//
// The attribute is written through SetQueueAttributes because CreateQueue is the
// validating path; this reaches the state a corrupted or hand-seeded queue would be in.
func TestSQS_SendMessage_MalformedAttributeFallsBackToDefault(t *testing.T) {
	for _, bad := range []string{"not-a-number", "0", "-1", ""} {
		t.Run("value "+strconv.Quote(bad), func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createSQSQueue(t, srv, "bad-attr-q")

			resp := sqsRequest(t, srv, map[string]string{
				"Action": "SetQueueAttributes", "QueueUrl": queueURL,
				"Attribute.1.Name": "MaximumMessageSize", "Attribute.1.Value": bad,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)

			status, code := sendMessage(t, srv, queueURL, "a modest body", false)
			assert.Equal(t, http.StatusOK, status, "a bad attribute must not reject everything: %s", code)
			assert.Equal(t, 1, sqsQueueDepth(t, srv, queueURL))

			// The default still applies, so enforcement is not simply switched off.
			status, code = sendMessage(t, srv, queueURL, strings.Repeat("x", 1_048_577), false)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	}
}
