package emulator_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1441 (GetQueueAttributes selection and approximate counts) and #1462
// (ChangeMessageVisibilityBatch, ReceiveMessage system attributes, per-receive
// VisibilityTimeout), each under both wire protocols.

// sqsProtocols names the two protocols every test here runs under.
var sqsProtocols = []struct {
	name string
	json bool
}{{"query", false}, {"json", true}}

// sqsReceived is one message of a ReceiveMessage response, protocol-independent.
type sqsReceived struct {
	ID         string
	Handle     string
	Body       string
	Attributes map[string]string
}

// sqsTestCreateQueue creates a queue and returns its URL.
func sqsTestCreateQueue(t *testing.T, srv *emulator.Server, name string, attrs map[string]string) string {
	t.Helper()
	params := map[string]string{"Action": "CreateQueue", "QueueName": name}
	i := 1
	for k, v := range attrs {
		params[fmt.Sprintf("Attribute.%d.Name", i)] = k
		params[fmt.Sprintf("Attribute.%d.Value", i)] = v
		i++
	}
	resp := sqsRequest(t, srv, params)
	require.Equal(t, http.StatusOK, resp.StatusCode, readBody(t, resp))
	resp.Body.Close() //nolint:errcheck
	return "http://sqs.us-east-1.localhost/123456789012/" + name
}

// sqsTestSend sends one message with the given extra query parameters.
func sqsTestSend(t *testing.T, srv *emulator.Server, queueURL, body string, extra map[string]string) {
	t.Helper()
	params := map[string]string{"Action": "SendMessage", "QueueUrl": queueURL, "MessageBody": body}
	for k, v := range extra {
		params[k] = v
	}
	resp := sqsRequest(t, srv, params)
	require.Equal(t, http.StatusOK, resp.StatusCode, readBody(t, resp))
	resp.Body.Close() //nolint:errcheck
}

// sqsTestReceive receives under one protocol. jsonBody holds JSON members beyond
// QueueUrl; query holds the query-protocol spelling of the same request.
func sqsTestReceive(t *testing.T, srv *emulator.Server, jsonProto bool, queueURL string, jsonBody map[string]any, query map[string]string) []sqsReceived {
	t.Helper()
	if jsonProto {
		body := map[string]any{"QueueUrl": queueURL, "MaxNumberOfMessages": 10}
		for k, v := range jsonBody {
			body[k] = v
		}
		resp := sqsJSONRequest(t, srv, "ReceiveMessage", body)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		out := readJSONBody(t, resp)
		raw, _ := out["Messages"].([]any)
		msgs := make([]sqsReceived, 0, len(raw))
		for _, r := range raw {
			m := r.(map[string]any)
			got := sqsReceived{ID: m["MessageId"].(string), Handle: m["ReceiptHandle"].(string), Body: m["Body"].(string)}
			if a, ok := m["Attributes"].(map[string]any); ok {
				got.Attributes = map[string]string{}
				for k, v := range a {
					got.Attributes[k] = v.(string)
				}
			}
			msgs = append(msgs, got)
		}
		return msgs
	}
	params := map[string]string{"Action": "ReceiveMessage", "QueueUrl": queueURL, "MaxNumberOfMessages": "10"}
	for k, v := range query {
		params[k] = v
	}
	resp := sqsRequest(t, srv, params)
	raw := readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, raw)
	var doc struct {
		Messages []struct {
			MessageID     string `xml:"MessageId"`
			ReceiptHandle string `xml:"ReceiptHandle"`
			Body          string `xml:"Body"`
			Attribute     []struct {
				Name  string `xml:"Name"`
				Value string `xml:"Value"`
			} `xml:"Attribute"`
		} `xml:"ReceiveMessageResult>Message"`
	}
	require.NoError(t, xml.Unmarshal([]byte(raw), &doc))
	msgs := make([]sqsReceived, 0, len(doc.Messages))
	for _, m := range doc.Messages {
		got := sqsReceived{ID: m.MessageID, Handle: m.ReceiptHandle, Body: m.Body}
		if len(m.Attribute) > 0 {
			got.Attributes = map[string]string{}
			for _, a := range m.Attribute {
				got.Attributes[a.Name] = a.Value
			}
		}
		msgs = append(msgs, got)
	}
	return msgs
}

// sqsTestQueueAttributes calls GetQueueAttributes for names and returns the status,
// error code and attributes.
func sqsTestQueueAttributes(t *testing.T, srv *emulator.Server, jsonProto bool, queueURL string, names ...string) (int, string, map[string]string) {
	t.Helper()
	if jsonProto {
		body := map[string]any{"QueueUrl": queueURL}
		if names != nil {
			body["AttributeNames"] = names
		}
		resp := sqsJSONRequest(t, srv, "GetQueueAttributes", body)
		if resp.StatusCode != http.StatusOK {
			status, code := sqsErrorCode(t, resp)
			return status, code, nil
		}
		out := readJSONBody(t, resp)
		attrs := map[string]string{}
		if a, ok := out["Attributes"].(map[string]any); ok {
			for k, v := range a {
				attrs[k] = v.(string)
			}
		}
		return http.StatusOK, "", attrs
	}
	params := map[string]string{"Action": "GetQueueAttributes", "QueueUrl": queueURL}
	for i, n := range names {
		params["AttributeName."+strconv.Itoa(i+1)] = n
	}
	resp := sqsRequest(t, srv, params)
	if resp.StatusCode != http.StatusOK {
		status, code := sqsErrorCode(t, resp)
		return status, code, nil
	}
	var doc struct {
		Attribute []struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &doc))
	attrs := map[string]string{}
	for _, a := range doc.Attribute {
		attrs[a.Name] = a.Value
	}
	return http.StatusOK, "", attrs
}

func TestSQS_GetQueueAttributes_ApproximateCounts(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, tc := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "counts-"+proto.name, nil)
			counts := func() map[string]string {
				t.Helper()
				status, code, attrs := sqsTestQueueAttributes(t, srv, proto.json, q,
					"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed")
				require.Equal(t, http.StatusOK, status, code)
				return attrs
			}
			want := func(visible, inFlight, delayed int) map[string]string {
				return map[string]string{
					"ApproximateNumberOfMessages":           strconv.Itoa(visible),
					"ApproximateNumberOfMessagesNotVisible": strconv.Itoa(inFlight),
					"ApproximateNumberOfMessagesDelayed":    strconv.Itoa(delayed),
				}
			}

			assert.Equal(t, want(0, 0, 0), counts(), "an empty queue reports zeros")

			sqsTestSend(t, srv, q, "a", nil)
			sqsTestSend(t, srv, q, "b", nil)
			sqsTestSend(t, srv, q, "c", map[string]string{"DelaySeconds": "60"})
			assert.Equal(t, want(2, 0, 1), counts())

			got := sqsTestReceive(t, srv, proto.json, q,
				map[string]any{"MaxNumberOfMessages": 1, "VisibilityTimeout": 30},
				map[string]string{"MaxNumberOfMessages": "1", "VisibilityTimeout": "30"})
			require.Len(t, got, 1)
			assert.Equal(t, want(1, 1, 1), counts(), "a received message is in flight")

			tc.SetTime(tc.Now().Add(31 * time.Second))
			assert.Equal(t, want(2, 0, 1), counts(), "an expired visibility timeout returns it")

			tc.SetTime(tc.Now().Add(30 * time.Second))
			assert.Equal(t, want(3, 0, 0), counts(), "the delay has elapsed")

			// All carries the counts alongside the static attributes.
			status, _, all := sqsTestQueueAttributes(t, srv, proto.json, q, "All")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, "3", all["ApproximateNumberOfMessages"])
			assert.Contains(t, all, "QueueArn")
			assert.Contains(t, all, "VisibilityTimeout")
		})
	}
}

func TestSQS_GetQueueAttributes_Selection(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "sel-"+proto.name, nil)

			status, _, attrs := sqsTestQueueAttributes(t, srv, proto.json, q, "VisibilityTimeout", "QueueArn")
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, []string{"QueueArn", "VisibilityTimeout"}, sortedKeys(attrs), "only the named attributes")

			status, _, attrs = sqsTestQueueAttributes(t, srv, proto.json, q, "Policy")
			require.Equal(t, http.StatusOK, status)
			assert.Empty(t, attrs, "a defined attribute the queue has no value for is absent")

			status, _, attrs = sqsTestQueueAttributes(t, srv, proto.json, q)
			require.Equal(t, http.StatusOK, status)
			assert.Empty(t, attrs, "no AttributeNames returns empty results")

			status, code, _ := sqsTestQueueAttributes(t, srv, proto.json, q, "VisibilityTimeout", "NotAnAttribute")
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidAttributeName", code)
		})
	}
}

func TestSQS_ReceiveMessage_SystemAttributes(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, tc := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "sys-"+proto.name, nil)
			sentAt := tc.Now()
			sqsTestSend(t, srv, q, "x", nil)

			// The issue's repro: AttributeNames=["All"].
			tc.SetTime(sentAt.Add(2 * time.Second))
			firstAt := tc.Now()
			got := sqsTestReceive(t, srv, proto.json, q,
				map[string]any{"AttributeNames": []string{"All"}, "VisibilityTimeout": 5},
				map[string]string{"AttributeName.1": "All", "VisibilityTimeout": "5"})
			require.Len(t, got, 1)
			assert.Equal(t, map[string]string{
				"ApproximateReceiveCount":          "1",
				"ApproximateFirstReceiveTimestamp": strconv.FormatInt(firstAt.UnixMilli(), 10),
				"SentTimestamp":                    strconv.FormatInt(sentAt.UnixMilli(), 10),
				"SenderId":                         "123456789012",
			}, got[0].Attributes)

			// The newer MessageSystemAttributeNames, naming one attribute.
			tc.SetTime(tc.Now().Add(10 * time.Second))
			got = sqsTestReceive(t, srv, proto.json, q,
				map[string]any{"MessageSystemAttributeNames": []string{"ApproximateReceiveCount"}, "VisibilityTimeout": 5},
				map[string]string{"MessageSystemAttributeName.1": "ApproximateReceiveCount", "VisibilityTimeout": "5"})
			require.Len(t, got, 1)
			assert.Equal(t, map[string]string{"ApproximateReceiveCount": "2"}, got[0].Attributes)

			// The first-receive timestamp does not move on later receives.
			tc.SetTime(tc.Now().Add(10 * time.Second))
			got = sqsTestReceive(t, srv, proto.json, q,
				map[string]any{"MessageSystemAttributeNames": []string{"All"}, "VisibilityTimeout": 5},
				map[string]string{"MessageSystemAttributeName.1": "All", "VisibilityTimeout": "5"})
			require.Len(t, got, 1)
			assert.Equal(t, "3", got[0].Attributes["ApproximateReceiveCount"])
			assert.Equal(t, strconv.FormatInt(firstAt.UnixMilli(), 10), got[0].Attributes["ApproximateFirstReceiveTimestamp"])

			// Nothing is returned unless asked for.
			tc.SetTime(tc.Now().Add(10 * time.Second))
			got = sqsTestReceive(t, srv, proto.json, q, nil, nil)
			require.Len(t, got, 1)
			assert.Nil(t, got[0].Attributes)
		})
	}
}

func TestSQS_ReceiveMessage_FIFOSystemAttributes(t *testing.T) {
	srv, _ := newSQSTestServer(t)
	q := sqsTestCreateQueue(t, srv, "sys.fifo", map[string]string{"FifoQueue": "true"})
	sqsTestSend(t, srv, q, "x", map[string]string{"MessageGroupId": "g1", "MessageDeduplicationId": "d1"})
	got := sqsTestReceive(t, srv, true, q, map[string]any{"AttributeNames": []string{"MessageGroupId", "MessageDeduplicationId"}}, nil)
	require.Len(t, got, 1)
	assert.Equal(t, map[string]string{"MessageGroupId": "g1", "MessageDeduplicationId": "d1"}, got[0].Attributes)
}

func TestSQS_ReceiveMessage_PerReceiveVisibilityTimeout(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "vis0-"+proto.name, map[string]string{"VisibilityTimeout": "300"})
			sqsTestSend(t, srv, q, "x", nil)

			zero := sqsTestReceive(t, srv, proto.json, q, map[string]any{"VisibilityTimeout": 0}, map[string]string{"VisibilityTimeout": "0"})
			require.Len(t, zero, 1)
			again := sqsTestReceive(t, srv, proto.json, q, nil, nil)
			require.Len(t, again, 1, "VisibilityTimeout=0 leaves the message visible immediately")

			none := sqsTestReceive(t, srv, proto.json, q, nil, nil)
			assert.Empty(t, none, "without VisibilityTimeout the queue's 300s default applies")
		})
	}
	t.Run("out of range", func(t *testing.T) {
		srv, _ := newSQSTestServer(t)
		q := sqsTestCreateQueue(t, srv, "vis-range", nil)
		status, code := sqsErrorCode(t, sqsJSONRequest(t, srv, "ReceiveMessage", map[string]any{"QueueUrl": q, "VisibilityTimeout": 43201}))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})
}

func TestSQS_ChangeMessageVisibility_RefusesUnknownAndNotInflight(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, tc := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "cmv-"+proto.name, nil)
			sqsTestSend(t, srv, q, "x", nil)
			got := sqsTestReceive(t, srv, proto.json, q, map[string]any{"VisibilityTimeout": 5}, map[string]string{"VisibilityTimeout": "5"})
			require.Len(t, got, 1)

			change := func(handle string, vis int) (int, string) {
				if proto.json {
					return sqsErrorCode(t, sqsJSONRequest(t, srv, "ChangeMessageVisibility", map[string]any{
						"QueueUrl": q, "ReceiptHandle": handle, "VisibilityTimeout": vis,
					}))
				}
				return sqsErrorCode(t, sqsRequest(t, srv, map[string]string{
					"Action": "ChangeMessageVisibility", "QueueUrl": q, "ReceiptHandle": handle, "VisibilityTimeout": strconv.Itoa(vis),
				}))
			}

			status, code := change("not-a-handle", 10)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ReceiptHandleIsInvalid", code)

			status, _ = change(got[0].Handle, 60)
			assert.Equal(t, http.StatusOK, status, "an in-flight message's visibility can be changed")

			status, code = change(got[0].Handle, 43201)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)

			tc.SetTime(tc.Now().Add(61 * time.Second))
			status, code = change(got[0].Handle, 10)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "MessageNotInflight", code)
		})
	}
}

// sqsTestVisibilityBatch calls ChangeMessageVisibilityBatch and returns the status,
// the error code of a refused request, the successful entry Ids and the failed
// entries by Id.
func sqsTestVisibilityBatch(t *testing.T, srv *emulator.Server, jsonProto bool, queueURL string, entries []map[string]any) (int, string, []string, map[string]string) {
	t.Helper()
	var resp *http.Response
	if jsonProto {
		resp = sqsJSONRequest(t, srv, "ChangeMessageVisibilityBatch", map[string]any{"QueueUrl": queueURL, "Entries": entries})
	} else {
		params := map[string]string{"Action": "ChangeMessageVisibilityBatch", "QueueUrl": queueURL}
		for i, e := range entries {
			prefix := fmt.Sprintf("ChangeMessageVisibilityBatchRequestEntry.%d.", i+1)
			params[prefix+"Id"] = fmt.Sprint(e["Id"])
			params[prefix+"ReceiptHandle"] = fmt.Sprint(e["ReceiptHandle"])
			params[prefix+"VisibilityTimeout"] = fmt.Sprint(e["VisibilityTimeout"])
		}
		resp = sqsRequest(t, srv, params)
	}
	if resp.StatusCode != http.StatusOK {
		status, code := sqsErrorCode(t, resp)
		return status, code, nil, nil
	}
	failed := map[string]string{}
	var ok []string
	if jsonProto {
		out := readJSONBody(t, resp)
		for _, s := range out["Successful"].([]any) {
			ok = append(ok, s.(map[string]any)["Id"].(string))
		}
		for _, f := range out["Failed"].([]any) {
			m := f.(map[string]any)
			assert.Equal(t, true, m["SenderFault"])
			failed[m["Id"].(string)] = m["Code"].(string)
		}
		require.NotNil(t, out["Failed"], "Failed is always present")
		return http.StatusOK, "", ok, failed
	}
	var doc struct {
		OK []struct {
			ID string `xml:"Id"`
		} `xml:"ChangeMessageVisibilityBatchResult>ChangeMessageVisibilityBatchResultEntry"`
		Failed []struct {
			ID          string `xml:"Id"`
			SenderFault bool   `xml:"SenderFault"`
			Code        string `xml:"Code"`
		} `xml:"ChangeMessageVisibilityBatchResult>BatchResultErrorEntry"`
	}
	require.NoError(t, xml.Unmarshal([]byte(readBody(t, resp)), &doc))
	for _, s := range doc.OK {
		ok = append(ok, s.ID)
	}
	for _, f := range doc.Failed {
		assert.True(t, f.SenderFault)
		failed[f.ID] = f.Code
	}
	return http.StatusOK, "", ok, failed
}

func TestSQS_ChangeMessageVisibilityBatch(t *testing.T) {
	for _, proto := range sqsProtocols {
		t.Run(proto.name, func(t *testing.T) {
			srv, _ := newSQSTestServer(t)
			q := sqsTestCreateQueue(t, srv, "cmvb-"+proto.name, nil)
			sqsTestSend(t, srv, q, "one", nil)
			sqsTestSend(t, srv, q, "two", nil)
			got := sqsTestReceive(t, srv, proto.json, q, map[string]any{"VisibilityTimeout": 30}, map[string]string{"VisibilityTimeout": "30"})
			require.Len(t, got, 2)

			status, _, ok, failed := sqsTestVisibilityBatch(t, srv, proto.json, q, []map[string]any{
				{"Id": "release", "ReceiptHandle": got[0].Handle, "VisibilityTimeout": 0},
				{"Id": "extend", "ReceiptHandle": got[1].Handle, "VisibilityTimeout": 600},
				{"Id": "bogus", "ReceiptHandle": "not-a-handle", "VisibilityTimeout": 10},
			})
			require.Equal(t, http.StatusOK, status)
			assert.Equal(t, []string{"release", "extend"}, ok)
			assert.Equal(t, map[string]string{"bogus": "ReceiptHandleIsInvalid"}, failed)

			// The released message is visible again; the extended one is not.
			again := sqsTestReceive(t, srv, proto.json, q, nil, nil)
			require.Len(t, again, 1)
			assert.Equal(t, got[0].Body, again[0].Body)
		})
	}

	eleven := make([]map[string]any, 11)
	for i := range eleven {
		eleven[i] = map[string]any{"Id": fmt.Sprintf("e%d", i), "ReceiptHandle": "h", "VisibilityTimeout": 1}
	}
	requestErrors := []struct {
		name    string
		entries []map[string]any
		code    string
	}{
		{"empty", nil, "EmptyBatchRequest"},
		{"too many", eleven, "TooManyEntriesInBatchRequest"},
		{"duplicate ids", []map[string]any{
			{"Id": "a", "ReceiptHandle": "h1", "VisibilityTimeout": 1},
			{"Id": "a", "ReceiptHandle": "h2", "VisibilityTimeout": 1},
		}, "BatchEntryIdsNotDistinct"},
		{"invalid id", []map[string]any{{"Id": "not valid!", "ReceiptHandle": "h", "VisibilityTimeout": 1}}, "InvalidBatchEntryId"},
	}
	for _, proto := range sqsProtocols {
		for _, tc := range requestErrors {
			t.Run(proto.name+"/"+tc.name, func(t *testing.T) {
				srv, _ := newSQSTestServer(t)
				q := sqsTestCreateQueue(t, srv, "cmvb-err", nil)
				status, code, _, _ := sqsTestVisibilityBatch(t, srv, proto.json, q, tc.entries)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, tc.code, code)
			})
		}
	}
}

func TestSQS_ChangeMessageVisibility_TimeoutParameter(t *testing.T) {
	setup := func(t *testing.T, name string) (*emulator.Server, string, string) {
		t.Helper()
		srv, _ := newSQSTestServer(t)
		q := sqsTestCreateQueue(t, srv, name, nil)
		sqsTestSend(t, srv, q, "x", nil)
		got := sqsTestReceive(t, srv, false, q, nil, map[string]string{"VisibilityTimeout": "60"})
		require.Len(t, got, 1)
		return srv, q, got[0].Handle
	}

	t.Run("query batch refuses a non-integer VisibilityTimeout", func(t *testing.T) {
		srv, q, handle := setup(t, "cmvb-nonint")
		status, code := sqsErrorCode(t, sqsRequest(t, srv, map[string]string{
			"Action": "ChangeMessageVisibilityBatch", "QueueUrl": q,
			"ChangeMessageVisibilityBatchRequestEntry.1.Id":                "e1",
			"ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle":     handle,
			"ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout": "soon",
		}))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Empty(t, sqsTestReceive(t, srv, false, q, nil, nil), "nothing was changed")
	})
	t.Run("query batch entry without VisibilityTimeout is 0, as the member is optional", func(t *testing.T) {
		srv, q, handle := setup(t, "cmvb-absent")
		resp := sqsRequest(t, srv, map[string]string{
			"Action": "ChangeMessageVisibilityBatch", "QueueUrl": q,
			"ChangeMessageVisibilityBatchRequestEntry.1.Id":            "e1",
			"ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle": handle,
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close() //nolint:errcheck
		assert.Len(t, sqsTestReceive(t, srv, false, q, nil, nil), 1)
	})
	for _, proto := range sqsProtocols {
		t.Run(proto.name+"/ChangeMessageVisibility requires VisibilityTimeout", func(t *testing.T) {
			srv, q, handle := setup(t, "cmv-missing-"+proto.name)
			var status int
			var code string
			if proto.json {
				status, code = sqsErrorCode(t, sqsJSONRequest(t, srv, "ChangeMessageVisibility", map[string]any{"QueueUrl": q, "ReceiptHandle": handle}))
			} else {
				status, code = sqsErrorCode(t, sqsRequest(t, srv, map[string]string{"Action": "ChangeMessageVisibility", "QueueUrl": q, "ReceiptHandle": handle}))
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "MissingParameter", code)
		})
	}
	t.Run("query ChangeMessageVisibility refuses a non-integer VisibilityTimeout", func(t *testing.T) {
		srv, q, handle := setup(t, "cmv-nonint")
		status, code := sqsErrorCode(t, sqsRequest(t, srv, map[string]string{
			"Action": "ChangeMessageVisibility", "QueueUrl": q, "ReceiptHandle": handle, "VisibilityTimeout": "1.5",
		}))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})
}
