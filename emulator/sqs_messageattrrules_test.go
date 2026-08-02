package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// batchFailure is one BatchResultErrorEntry as a test observes it, normalized across
// the two protocols.
type batchFailure struct {
	id          string
	code        string
	message     string
	senderFault bool
}

// sendBatchWithResults sends a batch and returns the successful entry IDs and the
// failures, plus the HTTP status.
//
// The status is returned rather than asserted here because the whole point of #472's
// batch half is that a partially-failing batch is a 200 — a helper that required 200
// could not express the request-level size failure the same operation still has.
func sendBatchWithResults(t *testing.T, srv *emulator.Server, queueURL string,
	entries []sqsBatchEntry, jsonProto bool,
) (int, []string, []batchFailure) {
	t.Helper()

	if jsonProto {
		list := make([]map[string]interface{}, 0, len(entries))
		for _, e := range entries {
			entry := map[string]interface{}{"Id": e.id, "MessageBody": e.body}
			if len(e.attrs) > 0 {
				entry["MessageAttributes"] = jsonAttrMap(e.attrs)
			}
			list = append(list, entry)
		}
		resp := sqsJSONRequest(t, srv, "SendMessageBatch",
			map[string]interface{}{"QueueUrl": queueURL, "Entries": list})
		if resp.StatusCode != http.StatusOK {
			status, _ := sqsErrorCode(t, resp)
			return status, nil, nil
		}
		body := readJSONBody(t, resp)

		var ids []string
		if raw, ok := body["Successful"].([]interface{}); ok {
			for _, item := range raw {
				m, isObj := item.(map[string]interface{})
				require.True(t, isObj, "Successful entry is not an object: %v", item)
				id, _ := m["Id"].(string)
				ids = append(ids, id)
			}
		}
		var failures []batchFailure
		raw, present := body["Failed"].([]interface{})
		require.True(t, present, "Failed must be present even when empty: %v", body)
		for _, item := range raw {
			m, isObj := item.(map[string]interface{})
			require.True(t, isObj, "Failed entry is not an object: %v", item)
			f := batchFailure{}
			f.id, _ = m["Id"].(string)
			f.code, _ = m["Code"].(string)
			f.message, _ = m["Message"].(string)
			f.senderFault, _ = m["SenderFault"].(bool)
			failures = append(failures, f)
		}
		return resp.StatusCode, ids, failures
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
			if a.stringValue != "" {
				params[ab+"Value.StringValue"] = a.stringValue
			}
		}
	}
	resp := sqsRequest(t, srv, params)
	if resp.StatusCode != http.StatusOK {
		status, _ := sqsErrorCode(t, resp)
		return status, nil, nil
	}

	var parsed struct {
		Result struct {
			Entry []struct {
				ID string `xml:"Id"`
			} `xml:"SendMessageBatchResultEntry"`
			Failed []struct {
				ID          string `xml:"Id"`
				SenderFault bool   `xml:"SenderFault"`
				Code        string `xml:"Code"`
				Message     string `xml:"Message"`
			} `xml:"BatchResultErrorEntry"`
		} `xml:"SendMessageBatchResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &parsed))

	var ids []string
	for _, e := range parsed.Result.Entry {
		ids = append(ids, e.ID)
	}
	var failures []batchFailure
	for _, f := range parsed.Result.Failed {
		failures = append(failures, batchFailure{
			id: f.ID, code: f.Code, message: f.Message, senderFault: f.SenderFault,
		})
	}
	return resp.StatusCode, ids, failures
}

// sendAttrError sends a single-attribute message and returns the status, code and
// message, so the wire text can be asserted and not merely the code.
func sendAttrError(t *testing.T, srv *emulator.Server, queueURL string,
	attr sqsMessageAttr, jsonProto bool,
) (int, string, string) {
	t.Helper()
	if jsonProto {
		resp := sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
			"QueueUrl":          queueURL,
			"MessageBody":       "body",
			"MessageAttributes": jsonAttrMap([]sqsMessageAttr{attr}),
		})
		if resp.StatusCode == http.StatusOK {
			resp.Body.Close() //nolint:errcheck
			return resp.StatusCode, "", ""
		}
		body := readJSONBody(t, resp)
		code, _ := body["__type"].(string)
		message, _ := body["message"].(string)
		return resp.StatusCode, code, message
	}

	params := map[string]string{
		"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": "body",
		"MessageAttribute.1.Name":           attr.name,
		"MessageAttribute.1.Value.DataType": attr.dataType,
	}
	// Set only when non-empty, so the empty-value case sends no StringValue at all
	// rather than an empty one — the query protocol cannot distinguish them, and the
	// rule under test is about the value being absent or empty either way.
	if attr.stringValue != "" {
		params["MessageAttribute.1.Value.StringValue"] = attr.stringValue
	}
	resp := sqsRequest(t, srv, params)
	if resp.StatusCode == http.StatusOK {
		resp.Body.Close() //nolint:errcheck
		return resp.StatusCode, "", ""
	}
	body := readJSONBody(t, resp)
	code, _ := body["__type"].(string)
	message, _ := body["message"].(string)
	return resp.StatusCode, code, message
}

// TestSQS_MessageAttributes_CountIsEnforced is #472's first criterion: an eleventh
// attribute was accepted, so a caller near the documented maximum — a tracing agent
// adding headers to a message that already carries nine of its own is the reported
// case — saw substrate deliver a message real SQS refuses.
//
// The message is a real-AWS capture, quoted in the test rather than only in the code so
// that changing one without the other fails here. The count and the limit are both
// interpolated, which is the shape the captured response has.
func TestSQS_MessageAttributes_CountIsEnforced(t *testing.T) {
	attrs := func(n int) []sqsMessageAttr {
		out := make([]sqsMessageAttr, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, sqsMessageAttr{
				name: "attr" + strconv.Itoa(i), dataType: "String",
				stringValue: "v" + strconv.Itoa(i),
			})
		}
		return out
	}

	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		t.Run("ten are accepted", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createAttrQueue(t, srv, "count-ten")

			status, code := sendMessageWithAttrs(t, srv, queueURL, "body", attrs(10), jsonProto)
			require.Equal(t, http.StatusOK, status, "ten is the boundary, not a rejection: %s", code)
			assert.Equal(t, 1, sqsQueueDepth(t, srv, queueURL))
		})

		t.Run("eleven are rejected and nothing is enqueued", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createAttrQueue(t, srv, "count-eleven")

			status, code := sendMessageWithAttrs(t, srv, queueURL, "body", attrs(11), jsonProto)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL),
				"a rejected send must enqueue nothing")
		})
	})
}

// TestSQS_MessageAttributes_CountErrorNamesBothNumbers pins the captured wording,
// including that the offending count is reported alongside the limit.
//
// A caller learns from this message alone how far over they are. Two counts are asserted
// rather than one, and that is the point of the test: one of the reimplementations that
// transcribed this string hardcoded the count as `[12]` while checking for more than ten,
// which a single-count assertion cannot distinguish from a correctly interpolated one.
func TestSQS_MessageAttributes_CountErrorNamesBothNumbers(t *testing.T) {
	for _, count := range []int{11, 12, 25} {
		t.Run(strconv.Itoa(count)+" attributes", func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createAttrQueue(t, srv, "count-message")

			attrs := make([]sqsMessageAttr, 0, count)
			for i := 0; i < count; i++ {
				attrs = append(attrs, sqsMessageAttr{
					name: "attr" + strconv.Itoa(i), dataType: "String", stringValue: "v",
				})
			}

			resp := sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
				"QueueUrl": queueURL, "MessageBody": "body",
				"MessageAttributes": jsonAttrMap(attrs),
			})
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			body := readJSONBody(t, resp)
			assert.Equal(t, "Number of message attributes ["+strconv.Itoa(count)+
				"] exceeds the allowed maximum [10].", body["message"])
		})
	}
}

// TestSQS_MessageAttributes_NameRules covers #472's name criteria in one table.
//
// The four casing rows are the point of the table. The prefix rule is
// case-**insensitive** — the guide says "or any casing variations" — which is the
// opposite of EC2's aws: tag prefix, where AWS:foo is a legal key (#452). A check
// copied from the tag code passes the AWS.foo row and fails aws.foo and AwS.foo, which
// is exactly what these rows exist to catch.
//
// AWSfoo and amazonfoo are legal and asserted so: the reserved form is the prefix
// *with* its period, so a name that merely starts with those letters is not reserved.
func TestSQS_MessageAttributes_NameRules(t *testing.T) {
	tests := []struct {
		name      string
		attrName  string
		wantOK    bool
		wantInMsg string
	}{
		{name: "AWS. prefix", attrName: "AWS.trace", wantInMsg: "reserved for internal use"},
		{name: "Amazon. prefix", attrName: "Amazon.trace", wantInMsg: "reserved for internal use"},
		{name: "lowercase aws. prefix", attrName: "aws.trace", wantInMsg: "reserved for internal use"},
		{name: "mixed-case AwS. prefix", attrName: "AwS.trace", wantInMsg: "reserved for internal use"},
		{name: "uppercase AMAZON. prefix", attrName: "AMAZON.trace", wantInMsg: "reserved for internal use"},
		{name: "leading period", attrName: ".trace", wantInMsg: "cannot start or end with '.'"},
		{name: "trailing period", attrName: "trace.", wantInMsg: "cannot start or end with '.'"},
		{name: "sequential periods", attrName: "trace..id", wantInMsg: "successive '.' characters"},
		{name: "space", attrName: "trace id", wantInMsg: "can only contain"},
		{name: "at sign", attrName: "trace@id", wantInMsg: "can only contain"},
		{name: "colon", attrName: "trace:id", wantInMsg: "can only contain"},
		{name: "non-ASCII", attrName: "traceÀid", wantInMsg: "can only contain"},
		{name: "256 bytes", attrName: strings.Repeat("a", 256), wantInMsg: "shorter than 256 Bytes"},
		{name: "257 bytes", attrName: strings.Repeat("a", 257), wantInMsg: "shorter than 256 Bytes"},

		{name: "AWSfoo without a period", attrName: "AWSfoo", wantOK: true},
		{name: "amazonfoo without a period", attrName: "amazonfoo", wantOK: true},
		{name: "aws-foo", attrName: "aws-foo", wantOK: true},
		{name: "an interior period", attrName: "trace.id", wantOK: true},
		{name: "mixed legal characters", attrName: "Foo_Bar-1.2", wantOK: true},
		{name: "255 bytes", attrName: strings.Repeat("a", 255), wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				srv, _ := newSQSTestServer(t)
				queueURL := createAttrQueue(t, srv, "name-rules")

				status, code, message := sendAttrError(t, srv, queueURL,
					sqsMessageAttr{name: tt.attrName, dataType: "String", stringValue: "v"},
					jsonProto)

				if tt.wantOK {
					require.Equal(t, http.StatusOK, status, "should be legal: %s %s", code, message)
					assert.Equal(t, 1, sqsQueueDepth(t, srv, queueURL))
					return
				}
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
				assert.Contains(t, message, tt.wantInMsg)
				assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL),
					"a rejected send must enqueue nothing")
			})
		})
	}
}

// TestSQS_MessageAttributes_NameCharacterMessageIsVerbatim pins the observed wording
// exactly, oddities included: "upper and lower score characters" is AWS's own phrasing
// and the string ends with a space.
//
// Asserted with Equal rather than Contains because both quirks are the kind a reader
// tidies in passing, and a tidied string is no longer the one a consumer sees from real
// SQS. This test is the reason not to.
func TestSQS_MessageAttributes_NameCharacterMessageIsVerbatim(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "name-verbatim")

	_, _, message := sendAttrError(t, srv, queueURL,
		sqsMessageAttr{name: "trace id", dataType: "String", stringValue: "v"}, false)
	assert.Equal(t, "Message (user) attributes name can only contain upper and lower "+
		"score characters, digits, periods, hyphens and underscores. ", message)
}

// TestSQS_MessageAttributes_TypeRules covers the DataType criteria.
//
// The three base types and their custom-suffixed forms are legal; anything else is not.
// The "string" row pins that the match is case-sensitive, which the guide states
// directly ("Is case-sensitive") and which is the opposite of the name prefix rule in
// the same request — a single check reused for both would be wrong for one of them.
func TestSQS_MessageAttributes_TypeRules(t *testing.T) {
	tests := []struct {
		name      string
		dataType  string
		value     string
		wantOK    bool
		wantInMsg string
	}{
		{name: "String", dataType: "String", value: "v", wantOK: true},
		{name: "Number", dataType: "Number", value: "1", wantOK: true},
		{name: "Binary", dataType: "Binary", wantOK: true},
		{name: "custom Number suffix", dataType: "Number.java.lang.Long", value: "1", wantOK: true},
		{name: "custom Binary suffix", dataType: "Binary.gif", wantOK: true},
		{name: "custom String suffix", dataType: "String.json", value: "{}", wantOK: true},

		{name: "truncated base type", dataType: "Str", value: "v", wantInMsg: "must be prefixed with"},
		{name: "unknown type", dataType: "int", value: "1", wantInMsg: "must be prefixed with"},
		{name: "lowercase string", dataType: "string", value: "v", wantInMsg: "must be prefixed with"},
		{name: "empty type", dataType: "", value: "v", wantInMsg: "Missing required parameter DataType"},
		{
			name: "256 bytes", dataType: "String." + strings.Repeat("a", 249), value: "v",
			wantInMsg: "types must be shorter than 256 Bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				srv, _ := newSQSTestServer(t)
				queueURL := createAttrQueue(t, srv, "type-rules")

				status, code, message := sendAttrError(t, srv, queueURL,
					sqsMessageAttr{name: "attr", dataType: tt.dataType, stringValue: tt.value},
					jsonProto)

				if tt.wantOK {
					require.Equal(t, http.StatusOK, status, "should be legal: %s %s", code, message)
					return
				}
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
				assert.Contains(t, message, tt.wantInMsg)
			})
		})
	}
}

// TestSQS_MessageAttributes_TypeLengthBoundIsExclusive pins that 255 bytes of type is
// legal and 256 is not.
//
// The guide says a type "can be up to 256 characters long" while the error says "must
// be shorter than 256 Bytes", and the two readings differ by exactly this case. The
// message is the more specific evidence, so 256 is refused — and the disagreement is
// pinned here so that flipping the bound is a visible decision rather than an
// off-by-one.
func TestSQS_MessageAttributes_TypeLengthBoundIsExclusive(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "type-bound")

	legal := "String." + strings.Repeat("a", 255-len("String."))
	require.Len(t, legal, 255)
	status, code, message := sendAttrError(t, srv, queueURL,
		sqsMessageAttr{name: "attr", dataType: legal, stringValue: "v"}, false)
	assert.Equal(t, http.StatusOK, status, "255 is legal: %s %s", code, message)

	status, code, _ = sendAttrError(t, srv, queueURL,
		sqsMessageAttr{name: "attr", dataType: legal + "a", stringValue: "v"}, false)
	assert.Equal(t, http.StatusBadRequest, status, "256 is not")
	assert.Equal(t, "InvalidParameterValue", code)
}

// TestSQS_MessageAttributes_EmptyStringValueRejected covers the documented rule that
// an attribute's "Name, Type, Value, and the message body must not be empty or null".
//
// The message has two independent real-AWS captures behind it — an SDK error in
// open-telemetry/opentelemetry-js-contrib#1528, and a direct `aws sqs send-message`
// against sqs.ap-northeast-1.amazonaws.com recorded in softwaremill/elasticmq#373 —
// which is why the attribute name is asserted to appear interpolated in it.
//
// A Binary attribute with an empty value is still accepted, which is deliberate: only
// the String case has a source specifying its message. That is also what keeps
// TestSQS_MessageAttributes_BinaryIsIdentifiedByDataType reachable through the API.
func TestSQS_MessageAttributes_EmptyStringValueRejected(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "empty-value")

		status, code, message := sendAttrError(t, srv, queueURL,
			sqsMessageAttr{name: "tracestate", dataType: "String"}, jsonProto)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Equal(t, "Message (user) attribute 'tracestate' must contain a "+
			"non-empty value of type 'String'.", message)
		assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))

		// The Binary case is not held to this rule; see the doc comment.
		status, code = sendMessageWithAttrs(t, srv, queueURL, "body",
			[]sqsMessageAttr{{name: "payload", dataType: "Binary"}}, jsonProto)
		assert.Equal(t, http.StatusOK, status, "an empty Binary value stays legal: %s", code)
	})
}

// TestSQS_MessageAttributes_NumberValidation is #472's Number criterion, and the
// silent-success shape it names: a Number attribute holding "abc" was accepted, so a
// consumer's parse-failure branch was unreachable.
//
// The rejection is a real-AWS capture — boto3 sending {'StringValue': 'Hello',
// 'DataType': 'Number'} to live SQS gets InvalidParameterValue with this exact sentence
// — so the attribute name is asserted interpolated into it.
//
// The range rows are a weaker claim, and their own test below says so. Scientific
// notation is accepted as the permissive reading of an unsettled detail: no capture
// shows whether real SQS takes "1e5" as a Number.
func TestSQS_MessageAttributes_NumberValidation(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantOK    bool
		wantInMsg string
	}{
		{name: "integer", value: "42", wantOK: true},
		{name: "negative", value: "-42", wantOK: true},
		{name: "explicit plus", value: "+42", wantOK: true},
		{name: "decimal", value: "3.14159", wantOK: true},
		{name: "leading zeroes", value: "007", wantOK: true},
		{name: "trailing point", value: "42.", wantOK: true},
		{name: "leading point", value: ".5", wantOK: true},
		{name: "scientific", value: "1e5", wantOK: true},
		{name: "negative exponent", value: "1.5e-10", wantOK: true},
		{name: "38 digits of precision", value: strings.Repeat("9", 38), wantOK: true},

		{name: "letters", value: "abc", wantInMsg: "to a number"},
		{name: "empty", value: "", wantInMsg: "to a number"},
		{name: "a bare sign", value: "-", wantInMsg: "to a number"},
		{name: "a bare point", value: ".", wantInMsg: "to a number"},
		{name: "two points", value: "1.2.3", wantInMsg: "to a number"},
		{name: "trailing letters", value: "42abc", wantInMsg: "to a number"},
		{name: "internal space", value: "4 2", wantInMsg: "to a number"},
		{name: "thousands separator", value: "1,000", wantInMsg: "to a number"},
		{name: "hexadecimal", value: "0x1f", wantInMsg: "to a number"},
		{name: "underscore separator", value: "1_000", wantInMsg: "to a number"},
		{name: "an exponent with no digits", value: "1e", wantInMsg: "to a number"},
		{name: "infinity", value: "Inf", wantInMsg: "to a number"},
		{name: "not a number", value: "NaN", wantInMsg: "to a number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				srv, _ := newSQSTestServer(t)
				queueURL := createAttrQueue(t, srv, "number-rules")

				status, code, message := sendAttrError(t, srv, queueURL,
					sqsMessageAttr{name: "Age", dataType: "Number", stringValue: tt.value},
					jsonProto)

				if tt.wantOK {
					require.Equal(t, http.StatusOK, status, "should be legal: %s %s", code, message)
					return
				}
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
				assert.Contains(t, message, tt.wantInMsg)
				assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))
			})
		})
	}
}

// TestSQS_MessageAttributes_NumberCastMessageNamesTheAttribute pins the captured
// wording, with the offending attribute's name interpolated as the capture shows it.
func TestSQS_MessageAttributes_NumberCastMessageNamesTheAttribute(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "number-message")

	_, _, message := sendAttrError(t, srv, queueURL,
		sqsMessageAttr{name: "Age", dataType: "Number", stringValue: "Hello"}, false)
	assert.Equal(t, "Can't cast the value of message (user) attribute 'Age' to a number.",
		message)
}

// TestSQS_MessageAttributes_NumberRangeBounds covers the documented range, "between
// 10^-128 and 10^+126".
//
// The bounds are computed as literal digit strings rather than through a float, because
// 10^126 is not representable exactly in float64 — a float64 comparison lands on the
// wrong side of the boundary for a value written out in full, which is precisely the
// case these rows exercise.
//
// A custom-suffixed Number is held to the same range: the suffix names a caller's own
// type, not a different value domain.
func TestSQS_MessageAttributes_NumberRangeBounds(t *testing.T) {
	// 10^126 exactly, and one more than it.
	max := "1" + strings.Repeat("0", 126)
	overMax := "2" + strings.Repeat("0", 126)
	underMin := "-2" + strings.Repeat("0", 128)

	tests := []struct {
		name     string
		dataType string
		value    string
		wantOK   bool
	}{
		{name: "at the maximum", dataType: "Number", value: max, wantOK: true},
		{name: "at the negative minimum", dataType: "Number", value: "-1" + strings.Repeat("0", 128), wantOK: true},
		{name: "zero", dataType: "Number", value: "0", wantOK: true},
		{name: "over the maximum", dataType: "Number", value: overMax},
		{name: "under the minimum", dataType: "Number", value: underMin},
		{name: "an overflowing exponent", dataType: "Number", value: "1e999999999"},
		{name: "a custom type is bounded too", dataType: "Number.java.lang.Long", value: overMax},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			queueURL := createAttrQueue(t, srv, "number-range")

			status, code, message := sendAttrError(t, srv, queueURL,
				sqsMessageAttr{name: "n", dataType: tt.dataType, stringValue: tt.value}, false)

			if tt.wantOK {
				require.Equal(t, http.StatusOK, status, "within range: %s %s", code, message)
				return
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "should be in range (-10**128..10**126)")
		})
	}
}

// TestSQS_MessageAttributes_FifoRejectionPrecedesDedup pins that an attribute rejection
// on a FIFO queue happens before the deduplication ID is recorded.
//
// This is #454's ordering property, re-asserted rather than assumed to have survived: a
// send that recorded its dedup ID and then failed would swallow the corrected retry as
// a duplicate and never deliver it — a failure that looks like success, which is worse
// than the one being fixed.
func TestSQS_MessageAttributes_FifoRejectionPrecedesDedup(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	status, code := createQueueWithAttrs(t, srv, "attr-rules.fifo",
		map[string]string{"FifoQueue": "true"}, false)
	require.Equal(t, http.StatusOK, status, "create failed: %s", code)
	queueURL := "http://sqs.us-east-1.localhost/000000000000/attr-rules.fifo"

	send := func(attrName string) *http.Response {
		t.Helper()
		return sqsRequest(t, srv, map[string]string{
			"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": "body",
			"MessageGroupId": "g1", "MessageDeduplicationId": "dedup-1",
			"MessageAttribute.1.Name":              attrName,
			"MessageAttribute.1.Value.DataType":    "String",
			"MessageAttribute.1.Value.StringValue": "v",
		})
	}

	rejected := send("AWS.trace")
	require.Equal(t, http.StatusBadRequest, rejected.StatusCode)
	rejected.Body.Close() //nolint:errcheck

	// The corrected retry reuses the same deduplication ID and must be delivered.
	accepted := send("trace")
	require.Equal(t, http.StatusOK, accepted.StatusCode)
	accepted.Body.Close() //nolint:errcheck

	msgs := receiveMessages(t, srv, queueURL, []string{"All"}, false)
	require.Len(t, msgs, 1, "the corrected retry must not be swallowed as a duplicate")
	assert.Equal(t, "v", msgs[0].attributes["trace"].stringValue)
}

// TestSQS_MessageAttributes_BatchReportsPerEntryFailures is #472's batch criterion, and
// the half substrate had no type for: both batch paths emitted an empty Failed list, so
// a consumer's per-entry error handling had nothing to exercise.
//
// The response is a **200** with one entry in Failed and two in Successful, which is
// what the reference describes: "you should check for batch errors even when the call
// returns an HTTP status code of 200". Exactly two messages are receivable, so the bad
// entry is refused without taking its siblings with it.
func TestSQS_MessageAttributes_BatchReportsPerEntryFailures(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "batch-per-entry")

		status, ids, failures := sendBatchWithResults(t, srv, queueURL, []sqsBatchEntry{
			{id: "good-1", body: "one", attrs: []sqsMessageAttr{
				{name: "trace", dataType: "String", stringValue: "a"},
			}},
			{id: "bad", body: "two", attrs: []sqsMessageAttr{
				{name: "AWS.trace", dataType: "String", stringValue: "b"},
			}},
			{id: "good-2", body: "three", attrs: []sqsMessageAttr{
				{name: "trace", dataType: "String", stringValue: "c"},
			}},
		}, jsonProto)

		require.Equal(t, http.StatusOK, status,
			"a partially-failing batch is a 200, not a request-level error")
		assert.ElementsMatch(t, []string{"good-1", "good-2"}, ids)

		require.Len(t, failures, 1)
		assert.Equal(t, "bad", failures[0].id)
		assert.Equal(t, "InvalidParameterValue", failures[0].code)
		assert.True(t, failures[0].senderFault,
			"a malformed attribute is the caller's fault, and a retry loop reads this")
		assert.Contains(t, failures[0].message, "reserved for internal use")

		assert.Equal(t, 2, sqsQueueDepth(t, srv, queueURL),
			"the failed entry must not enqueue, and its siblings must")
	})
}

// TestSQS_MessageAttributes_BatchFailedIsPresentWhenEmpty pins that Failed is an empty
// list rather than absent on a fully successful batch.
//
// The model declares Failed required, and an SDK distinguishes an empty list from a
// missing one: a consumer looping over the field would see nil where AWS sends an empty
// collection. Substrate already emitted a placeholder here, so this is a regression
// guard on replacing it with the real list.
func TestSQS_MessageAttributes_BatchFailedIsPresentWhenEmpty(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "batch-empty-failed")

		status, ids, failures := sendBatchWithResults(t, srv, queueURL, []sqsBatchEntry{
			{id: "a", body: "one"},
			{id: "b", body: "two", attrs: []sqsMessageAttr{
				{name: "trace", dataType: "String", stringValue: "v"},
			}},
		}, jsonProto)

		require.Equal(t, http.StatusOK, status)
		assert.ElementsMatch(t, []string{"a", "b"}, ids)
		assert.Empty(t, failures)
		assert.Equal(t, 2, sqsQueueDepth(t, srv, queueURL))
	})
}

// TestSQS_MessageAttributes_BatchSizeStillFailsWholeRequest pins the distinction the
// two checks now sit ten lines apart on.
//
// An attribute violation fails its entry; an oversized payload fails the request with a
// 400. BatchRequestTooLong is a documented request-level error about the aggregate, and
// a single oversized entry is observed to be reported the same way (#454), so adding
// per-entry failures must not have converted the size checks into them.
func TestSQS_MessageAttributes_BatchSizeStillFailsWholeRequest(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createSizedQueue(t, srv, "batch-size-vs-attr", "1024")

		status, ids, failures := sendBatchWithResults(t, srv, queueURL, []sqsBatchEntry{
			{id: "small", body: "one"},
			{id: "huge", body: strings.Repeat("x", 2048)},
		}, jsonProto)

		assert.Equal(t, http.StatusBadRequest, status,
			"an oversized entry is still a request-level failure, not a Failed entry")
		assert.Empty(t, ids)
		assert.Empty(t, failures)
		assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL))
	})
}

// TestSQS_MessageAttributes_BatchCountIsPerEntry pins that the ten-attribute maximum
// applies to each entry rather than to the batch's total attribute count.
//
// Three entries of nine attributes each is 27 in the request and legal, because the
// documented limit is per message. A check written over the accumulated set would
// reject this whole batch.
func TestSQS_MessageAttributes_BatchCountIsPerEntry(t *testing.T) {
	nine := make([]sqsMessageAttr, 0, 9)
	for i := 0; i < 9; i++ {
		nine = append(nine, sqsMessageAttr{
			name: "attr" + strconv.Itoa(i), dataType: "String", stringValue: "v",
		})
	}
	eleven := make([]sqsMessageAttr, 0, 11)
	for i := 0; i < 11; i++ {
		eleven = append(eleven, sqsMessageAttr{
			name: "attr" + strconv.Itoa(i), dataType: "String", stringValue: "v",
		})
	}

	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "batch-count")

		status, ids, failures := sendBatchWithResults(t, srv, queueURL, []sqsBatchEntry{
			{id: "a", body: "one", attrs: nine},
			{id: "b", body: "two", attrs: nine},
			{id: "c", body: "three", attrs: eleven},
		}, jsonProto)

		require.Equal(t, http.StatusOK, status)
		assert.ElementsMatch(t, []string{"a", "b"}, ids,
			"27 attributes across three entries is legal; the limit is per message")
		require.Len(t, failures, 1)
		assert.Equal(t, "c", failures[0].id)
		assert.Contains(t, failures[0].message, "exceeds the allowed maximum [10]")
	})
}

// TestSQS_MessageAttributes_ErrorIsDeterministicAcrossRules pins that a message
// breaking two rules always reports the same one.
//
// Attributes are visited in sorted name order for exactly this reason: Go map iteration
// is randomized, so an implementation walking the map would report one error on some
// runs and the other on others — a flaky assertion in a consumer's suite, produced by
// the emulator that exists to remove flakiness. Repeated because a single run of a
// randomized walk can agree by chance.
func TestSQS_MessageAttributes_ErrorIsDeterministicAcrossRules(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	queueURL := createAttrQueue(t, srv, "attr-determinism")

	// "AWS.first" sorts before "zz bad", so the prefix rule is the one reported.
	attrs := []sqsMessageAttr{
		{name: "zz bad", dataType: "String", stringValue: "v"},
		{name: "AWS.first", dataType: "String", stringValue: "v"},
	}

	var first string
	for i := 0; i < 20; i++ {
		resp := sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
			"QueueUrl": queueURL, "MessageBody": "body",
			"MessageAttributes": jsonAttrMap(attrs),
		})
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		message, _ := readJSONBody(t, resp)["message"].(string)
		if i == 0 {
			first = message
			assert.Contains(t, first, "reserved for internal use",
				"the alphabetically first offending attribute is the one reported")
			continue
		}
		require.Equal(t, first, message, "the reported rule must not vary between runs")
	}
}

// storeIllegalAttributes replaces a stored message's MessageAttributes with attrs,
// writing straight into the state store, and returns the message ID.
//
// Going around the API is the point rather than a shortcut: no send path stores an
// illegal attribute any more, so the only way this state exists is an event log recorded
// against a substrate that predates the rules. Reaching it through the API is impossible
// by construction, and anything weaker would not exercise the path at all.
//
// The queue must hold exactly one message, which every caller here arranges by sending
// one legal message first.
func storeIllegalAttributes(t *testing.T, state emulator.StateManager, queueName string,
	attrs map[string]interface{},
) string {
	t.Helper()
	ctx := context.Background()
	urlKey := "000000000000/" + queueName

	idsRaw, err := state.Get(ctx, "sqs", "msg_ids:"+urlKey)
	require.NoError(t, err)
	var ids []string
	require.NoError(t, json.Unmarshal(idsRaw, &ids))
	require.Len(t, ids, 1, "the helper assumes one stored message")

	msgKey := "msg:" + urlKey + ":" + ids[0]
	raw, err := state.Get(ctx, "sqs", msgKey)
	require.NoError(t, err)
	var stored map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &stored))
	stored["MessageAttributes"] = attrs
	mutated, err := json.Marshal(stored)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "sqs", msgKey, mutated))
	return ids[0]
}

// newSQSServerCapturingLogs builds an SQS test server whose logger writes to buf, so a
// warning can be asserted rather than assumed.
func newSQSServerCapturingLogs(t *testing.T, state emulator.StateManager,
	buf *bytes.Buffer,
) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := newTestLogger(buf, slog.LevelWarn)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	plugin := &emulator.SQSPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// TestSQS_MessageAttributes_StoredAttributesAreReturnedNotWithheld is #491's decision,
// and the guarantee it protects is replay rather than validation.
//
// The rules apply on send, not on receive. A message written into state before they
// existed is returned as stored, in full, because substrate's core property is that
// replaying an event log reproduces the same observations — and the message was accepted
// by the substrate that recorded it. Withholding or dropping it now would break exactly
// that, and a receive-time rejection has no AWS behavior behind it to imitate: real SQS
// never accepted the message, so there is nothing to copy.
//
// This test must fail if anyone later "fixes" receive to validate. All three violation
// families are covered, because a fix that withheld would most likely be written against
// one of them and the other two would silently follow.
func TestSQS_MessageAttributes_StoredAttributesAreReturnedNotWithheld(t *testing.T) {
	// Eleven attributes: the count rule, which is a property of the set rather than of
	// any one attribute, so it is the one a per-attribute filter would miss.
	eleven := make(map[string]interface{}, 11)
	for i := 0; i < 11; i++ {
		eleven["attr"+strconv.Itoa(i)] = map[string]interface{}{
			"DataType": "String", "StringValue": "v" + strconv.Itoa(i),
		}
	}

	tests := []struct {
		name  string
		attrs map[string]interface{}
		// want is the attribute name and value that must come back unchanged.
		wantName, wantValue string
	}{
		{
			name: "a reserved name prefix",
			attrs: map[string]interface{}{
				"AWS.trace": map[string]interface{}{"DataType": "String", "StringValue": "v"},
			},
			wantName: "AWS.trace", wantValue: "v",
		},
		{
			name:     "an eleventh attribute",
			attrs:    eleven,
			wantName: "attr10", wantValue: "v10",
		},
		{
			name: "a Number attribute holding a non-number",
			attrs: map[string]interface{}{
				"Age": map[string]interface{}{"DataType": "Number", "StringValue": "abc"},
			},
			wantName: "Age", wantValue: "abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bothProtocols(t, func(t *testing.T, jsonProto bool) {
				state := emulator.NewMemoryStateManager()
				srv, _ := newSQSTestServerWithState(t, state)
				queueName := "attr-stored"
				queueURL := createAttrQueue(t, srv, queueName)

				status, code := sendMessageWithAttrs(t, srv, queueURL, "body",
					[]sqsMessageAttr{{name: "trace", dataType: "String", stringValue: "v"}},
					jsonProto)
				require.Equal(t, http.StatusOK, status, "send failed: %s", code)
				msgID := storeIllegalAttributes(t, state, queueName, tt.attrs)

				msgs := receiveMessages(t, srv, queueURL, []string{"All"}, jsonProto)
				require.Len(t, msgs, 1, "a stored message must still be delivered")
				assert.Equal(t, msgID, msgs[0].messageID)
				assert.Equal(t, "body", msgs[0].body, "the body is returned as stored")
				assert.Len(t, msgs[0].attributes, len(tt.attrs),
					"every stored attribute is returned, none filtered out")
				assert.Equal(t, tt.wantValue, msgs[0].attributes[tt.wantName].stringValue,
					"the illegal attribute is returned as stored, not corrected or withheld")
			})
		})
	}
}

// TestSQS_MessageAttributes_StoredViolationWarns is the signal #491 chose over silence:
// an operator whose fixture predates the rules learns about it, and the observation does
// not change.
//
// The warning is asserted through a capturing logger rather than assumed, and its
// absence for a legal message is asserted too — a warning that fires on every receive
// would be noise an operator learns to ignore, which is the failure mode that would make
// the whole decision worthless.
func TestSQS_MessageAttributes_StoredViolationWarns(t *testing.T) {
	t.Run("a violation warns and names what to look at", func(t *testing.T) {
		state := emulator.NewMemoryStateManager()
		var buf bytes.Buffer
		srv := newSQSServerCapturingLogs(t, state, &buf)
		queueName := "attr-warn"
		queueURL := createAttrQueue(t, srv, queueName)

		status, code := sendMessageWithAttrs(t, srv, queueURL, "body",
			[]sqsMessageAttr{{name: "trace", dataType: "String", stringValue: "v"}}, false)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)
		msgID := storeIllegalAttributes(t, state, queueName, map[string]interface{}{
			"Age": map[string]interface{}{"DataType": "Number", "StringValue": "abc"},
		})

		msgs := receiveMessages(t, srv, queueURL, nil, false)
		require.Len(t, msgs, 1)

		logged := buf.String()
		assert.Contains(t, logged, "stored message attributes violate a current rule")
		assert.Contains(t, logged, msgID, "the message ID is what an operator greps for")
		assert.Contains(t, logged, queueName, "the queue names which fixture to look at")
		assert.Contains(t, logged, "Can't cast the value of message (user) attribute 'Age'",
			"the violation itself is reported, not merely that there was one")
	})

	t.Run("the request naming no attributes still warns", func(t *testing.T) {
		// The violation is a property of what is in state, not of what was asked for.
		// Checking only the selected subset would hide it from exactly the caller most
		// likely to be replaying an old log, since #461 returns nothing by default.
		state := emulator.NewMemoryStateManager()
		var buf bytes.Buffer
		srv := newSQSServerCapturingLogs(t, state, &buf)
		queueName := "attr-warn-unselected"
		queueURL := createAttrQueue(t, srv, queueName)

		status, code := sendMessageWithAttrs(t, srv, queueURL, "body",
			[]sqsMessageAttr{{name: "trace", dataType: "String", stringValue: "v"}}, false)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)
		storeIllegalAttributes(t, state, queueName, map[string]interface{}{
			"AWS.trace": map[string]interface{}{"DataType": "String", "StringValue": "v"},
		})

		msgs := receiveMessages(t, srv, queueURL, nil, false)
		require.Len(t, msgs, 1)
		assert.Empty(t, msgs[0].attributes, "no selector was sent, so none are returned")
		assert.Contains(t, buf.String(), "stored message attributes violate a current rule")
	})

	t.Run("a legal message logs nothing", func(t *testing.T) {
		state := emulator.NewMemoryStateManager()
		var buf bytes.Buffer
		srv := newSQSServerCapturingLogs(t, state, &buf)
		queueURL := createAttrQueue(t, srv, "attr-quiet")

		status, code := sendMessageWithAttrs(t, srv, queueURL, "body", []sqsMessageAttr{
			{name: "trace", dataType: "String", stringValue: "v"},
			{name: "Age", dataType: "Number", stringValue: "42"},
		}, false)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)

		msgs := receiveMessages(t, srv, queueURL, []string{"All"}, false)
		require.Len(t, msgs, 1)
		assert.Empty(t, buf.String(), "a legal message must produce no warning")
	})

	t.Run("a message with no attributes logs nothing", func(t *testing.T) {
		state := emulator.NewMemoryStateManager()
		var buf bytes.Buffer
		srv := newSQSServerCapturingLogs(t, state, &buf)
		queueURL := createAttrQueue(t, srv, "attr-none")

		status, code := sendMessage(t, srv, queueURL, "body", false)
		require.Equal(t, http.StatusOK, status, "send failed: %s", code)

		msgs := receiveMessages(t, srv, queueURL, []string{"All"}, false)
		require.Len(t, msgs, 1)
		assert.Empty(t, buf.String())
	})
}

// TestSQS_MessageAttributes_SendTimeRejectionIsUnchanged guards the other half of the
// decision. Warning on receive must not have softened the send path: a message with an
// illegal attribute is still refused before it is stored, so no new run can produce the
// state the warning exists to report.
func TestSQS_MessageAttributes_SendTimeRejectionIsUnchanged(t *testing.T) {
	bothProtocols(t, func(t *testing.T, jsonProto bool) {
		srv, _ := newSQSTestServer(t)
		queueURL := createAttrQueue(t, srv, "attr-send-still-refuses")

		status, code, message := sendAttrError(t, srv, queueURL,
			sqsMessageAttr{name: "AWS.trace", dataType: "String", stringValue: "v"}, jsonProto)
		require.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Contains(t, message, "reserved for internal use")
		assert.Equal(t, 0, sqsQueueDepth(t, srv, queueURL), "a refused send stores nothing")
	})
}
