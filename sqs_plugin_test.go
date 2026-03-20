package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func newSQSTestServer(t *testing.T) (*substrate.Server, *substrate.TimeController) {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(sqsPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger), tc
}

func sqsRequest(t *testing.T, srv *substrate.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "sqs.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readBody(t *testing.T, r *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return string(body)
}

func TestSQSPlugin_CreateGetDelete(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	// Create queue.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "test-queue",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, body, "test-queue")

	// Get queue URL.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":    "GetQueueUrl",
		"QueueName": "test-queue",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2 := readBody(t, resp2)
	assert.Contains(t, body2, "test-queue")

	// Delete queue (account "000000000000" = fallback when no auth header).
	queueURL := "http://sqs.us-east-1.localhost/000000000000/test-queue"
	resp3 := sqsRequest(t, srv, map[string]string{
		"Action":   "DeleteQueue",
		"QueueUrl": queueURL,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// Get URL of deleted queue → error.
	resp4 := sqsRequest(t, srv, map[string]string{
		"Action":    "GetQueueUrl",
		"QueueName": "test-queue",
	})
	assert.Equal(t, http.StatusBadRequest, resp4.StatusCode)
}

func TestSQSPlugin_SendReceiveDelete(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/msg-queue"

	// Create queue.
	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "msg-queue",
	})

	// Send message.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    queueURL,
		"MessageBody": "hello world",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Receive message.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body := readBody(t, resp2)
	assert.Contains(t, body, "hello world")

	// Extract receipt handle from XML.
	type msg struct {
		ReceiptHandle string `xml:"ReceiptHandle"`
	}
	type receiveResult struct {
		Message []msg `xml:"Message"`
	}
	type receiveResponse struct {
		XMLName              xml.Name      `xml:"ReceiveMessageResponse"`
		ReceiveMessageResult receiveResult `xml:"ReceiveMessageResult"`
	}
	var parsed receiveResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.ReceiveMessageResult.Message, 1)
	handle := parsed.ReceiveMessageResult.Message[0].ReceiptHandle
	assert.NotEmpty(t, handle)

	// Delete message.
	resp3 := sqsRequest(t, srv, map[string]string{
		"Action":        "DeleteMessage",
		"QueueUrl":      queueURL,
		"ReceiptHandle": handle,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// Receive again → empty.
	resp4 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body4 := readBody(t, resp4)
	// No messages expected.
	assert.NotContains(t, body4, "hello world")
}

func TestSQSPlugin_VisibilityTimeout(t *testing.T) {
	srv, tc := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/vis-queue"

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "vis-queue",
	})

	sqsRequest(t, srv, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    queueURL,
		"MessageBody": "timeout-test",
	})

	// Receive with 5s visibility timeout.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
		"VisibilityTimeout":   "5",
	})
	body := readBody(t, resp)
	assert.Contains(t, body, "timeout-test")

	// Receive immediately — should be empty (message invisible).
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body2 := readBody(t, resp2)
	assert.NotContains(t, body2, "timeout-test")

	// Advance time past visibility timeout.
	tc.SetTime(tc.Now().Add(10 * time.Second))

	// Receive again — message reappears.
	resp3 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body3 := readBody(t, resp3)
	assert.Contains(t, body3, "timeout-test")
}

func TestSQSPlugin_ReceiveMessage_DelaySeconds(t *testing.T) {
	srv, tc := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/delay-queue"

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "delay-queue",
	})

	// Send message with 10s delay.
	sqsRequest(t, srv, map[string]string{
		"Action":       "SendMessage",
		"QueueUrl":     queueURL,
		"MessageBody":  "delayed-message",
		"DelaySeconds": "10",
	})

	// Receive immediately — should be empty.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body := readBody(t, resp)
	assert.NotContains(t, body, "delayed-message")

	// Advance time past delay.
	tc.SetTime(tc.Now().Add(15 * time.Second))

	// Receive again — message now visible.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body2 := readBody(t, resp2)
	assert.Contains(t, body2, "delayed-message")
}

func TestSQSPlugin_ListQueues(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	for _, name := range []string{"alpha-queue", "beta-queue", "alpha-other"} {
		sqsRequest(t, srv, map[string]string{
			"Action":    "CreateQueue",
			"QueueName": name,
		})
	}

	// List all.
	resp := sqsRequest(t, srv, map[string]string{
		"Action": "ListQueues",
	})
	body := readBody(t, resp)
	assert.Contains(t, body, "alpha-queue")
	assert.Contains(t, body, "beta-queue")
	assert.Contains(t, body, "alpha-other")

	// List with prefix.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":          "ListQueues",
		"QueueNamePrefix": "alpha",
	})
	body2 := readBody(t, resp2)
	assert.Contains(t, body2, "alpha-queue")
	assert.Contains(t, body2, "alpha-other")
	assert.NotContains(t, body2, "beta-queue")
}

func TestSQSPlugin_SendMessageBatch(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/batch-queue"

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "batch-queue",
	})

	resp := sqsRequest(t, srv, map[string]string{
		"Action":                            "SendMessageBatch",
		"QueueUrl":                          queueURL,
		"SendMessageBatchRequestEntry.1.Id": "entry1",
		"SendMessageBatchRequestEntry.1.MessageBody": "message-one",
		"SendMessageBatchRequestEntry.2.Id":          "entry2",
		"SendMessageBatchRequestEntry.2.MessageBody": "message-two",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, body, "entry1")
	assert.Contains(t, body, "entry2")

	// Receive both messages.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
		"VisibilityTimeout":   "0",
	})
	body2 := readBody(t, resp2)
	assert.Contains(t, body2, "message-one")
	assert.Contains(t, body2, "message-two")
}

func TestSQSPlugin_PurgeQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/purge-queue"

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "purge-queue",
	})

	// Send 3 messages.
	for i := 1; i <= 3; i++ {
		sqsRequest(t, srv, map[string]string{
			"Action":      "SendMessage",
			"QueueUrl":    queueURL,
			"MessageBody": "msg",
		})
	}

	// Purge.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":   "PurgeQueue",
		"QueueUrl": queueURL,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Receive — empty.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
	})
	body := readBody(t, resp2)
	// No Message elements expected.
	assert.NotContains(t, body, "<Body>")
}

func TestSQSPlugin_GetQueueAttributes(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "attrs-queue",
	})

	queueURL := "http://sqs.us-east-1.localhost/000000000000/attrs-queue"
	resp := sqsRequest(t, srv, map[string]string{
		"Action":   "GetQueueAttributes",
		"QueueUrl": queueURL,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, body, "QueueArn")
	assert.Contains(t, body, "VisibilityTimeout")
}

func TestSQSPlugin_ChangeMessageVisibility(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/cmv-queue"
	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "cmv-queue",
	})
	sqsRequest(t, srv, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    queueURL,
		"MessageBody": "visibility-test",
	})

	// Receive with default visibility (30s).
	resp := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body := readBody(t, resp)

	var parsed struct {
		ReceiveMessageResult struct {
			Message []struct {
				ReceiptHandle string `xml:"ReceiptHandle"`
			} `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.ReceiveMessageResult.Message, 1)
	handle := parsed.ReceiveMessageResult.Message[0].ReceiptHandle

	// Change visibility to 0 so the message becomes immediately visible.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":            "ChangeMessageVisibility",
		"QueueUrl":          queueURL,
		"ReceiptHandle":     handle,
		"VisibilityTimeout": "0",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// Now the message should be receivable immediately.
	resp3 := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	body3 := readBody(t, resp3)
	assert.Contains(t, body3, "visibility-test")
}

func TestSQSPlugin_TagUntagListTags(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "tag-queue",
	})
	queueURL := "http://sqs.us-east-1.localhost/000000000000/tag-queue"

	// Tag the queue.
	sqsRequest(t, srv, map[string]string{
		"Action":      "TagQueue",
		"QueueUrl":    queueURL,
		"Tag.1.Key":   "env",
		"Tag.1.Value": "prod",
	})

	// List tags — must include the new tag.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":   "ListQueueTags",
		"QueueUrl": queueURL,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, body, "env")
	assert.Contains(t, body, "prod")

	// Untag.
	sqsRequest(t, srv, map[string]string{
		"Action":   "UntagQueue",
		"QueueUrl": queueURL,
		"TagKey.1": "env",
	})

	// List tags again — should no longer contain the removed tag.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":   "ListQueueTags",
		"QueueUrl": queueURL,
	})
	body2 := readBody(t, resp2)
	assert.NotContains(t, body2, "env")
}

func TestSQSPlugin_DeleteMessageBatch(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	queueURL := "http://sqs.us-east-1.localhost/000000000000/dmbatch-queue"
	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "dmbatch-queue",
	})

	// Send 2 messages.
	for i := 1; i <= 2; i++ {
		sqsRequest(t, srv, map[string]string{
			"Action":      "SendMessage",
			"QueueUrl":    queueURL,
			"MessageBody": fmt.Sprintf("msg-%d", i),
		})
	}

	// Receive both with VisibilityTimeout=0 so they can be re-received if needed.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
		"VisibilityTimeout":   "0",
	})
	body := readBody(t, resp)

	type msg struct {
		ReceiptHandle string `xml:"ReceiptHandle"`
	}
	type receiveResult struct {
		Message []msg `xml:"Message"`
	}
	type receiveResponse struct {
		ReceiveMessageResult receiveResult `xml:"ReceiveMessageResult"`
	}
	var parsed receiveResponse
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed))
	require.Len(t, parsed.ReceiveMessageResult.Message, 2)

	// Delete both messages in a single batch request.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":                              "DeleteMessageBatch",
		"QueueUrl":                            queueURL,
		"DeleteMessageBatchRequestEntry.1.Id": "e1",
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": parsed.ReceiveMessageResult.Message[0].ReceiptHandle,
		"DeleteMessageBatchRequestEntry.2.Id":            "e2",
		"DeleteMessageBatchRequestEntry.2.ReceiptHandle": parsed.ReceiveMessageResult.Message[1].ReceiptHandle,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

func TestSQSPlugin_SetQueueAttributes(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "setattr-queue",
	})
	queueURL := "http://sqs.us-east-1.localhost/000000000000/setattr-queue"

	// Set VisibilityTimeout to 60.
	resp := sqsRequest(t, srv, map[string]string{
		"Action":            "SetQueueAttributes",
		"QueueUrl":          queueURL,
		"Attribute.1.Name":  "VisibilityTimeout",
		"Attribute.1.Value": "60",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify via GetQueueAttributes.
	resp2 := sqsRequest(t, srv, map[string]string{
		"Action":   "GetQueueAttributes",
		"QueueUrl": queueURL,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body := readBody(t, resp2)
	assert.Contains(t, body, "VisibilityTimeout")
}

// --- JSON protocol helpers ---------------------------------------------------

func sqsJSONRequest(t *testing.T, srv *substrate.Server, target string, body interface{}) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	r.Host = "sqs.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	r.Header.Set("X-Amz-Target", "AmazonSQS."+target)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readJSONBody(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]interface{}
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

// --- JSON protocol tests -----------------------------------------------------

func TestSQSPlugin_JSON_CreateGetDeleteQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	// Create queue.
	resp := sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "json-queue",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readJSONBody(t, resp)
	assert.Contains(t, body["QueueUrl"], "json-queue")

	qURL := body["QueueUrl"].(string)

	// Get queue URL.
	resp2 := sqsJSONRequest(t, srv, "GetQueueUrl", map[string]interface{}{
		"QueueName": "json-queue",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2 := readJSONBody(t, resp2)
	assert.Equal(t, qURL, body2["QueueUrl"])

	// Idempotent create — returns same URL.
	resp3 := sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "json-queue",
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	body3 := readJSONBody(t, resp3)
	assert.Equal(t, qURL, body3["QueueUrl"])

	// Delete queue.
	resp4 := sqsJSONRequest(t, srv, "DeleteQueue", map[string]interface{}{
		"QueueUrl": qURL,
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Get URL of deleted queue → error.
	resp5 := sqsJSONRequest(t, srv, "GetQueueUrl", map[string]interface{}{
		"QueueName": "json-queue",
	})
	assert.Equal(t, http.StatusBadRequest, resp5.StatusCode)
}

func TestSQSPlugin_JSON_GetQueueAttributes(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "attrs-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/attrs-json-queue"

	resp := sqsJSONRequest(t, srv, "GetQueueAttributes", map[string]interface{}{
		"QueueUrl":       qURL,
		"AttributeNames": []string{"All"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readJSONBody(t, resp)
	attrs, ok := body["Attributes"].(map[string]interface{})
	require.True(t, ok)
	assert.Contains(t, attrs, "QueueArn")
	assert.Contains(t, attrs, "VisibilityTimeout")
}

func TestSQSPlugin_JSON_SetQueueAttributes(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "setattr-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/setattr-json-queue"

	resp := sqsJSONRequest(t, srv, "SetQueueAttributes", map[string]interface{}{
		"QueueUrl":   qURL,
		"Attributes": map[string]string{"VisibilityTimeout": "120"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := sqsJSONRequest(t, srv, "GetQueueAttributes", map[string]interface{}{
		"QueueUrl": qURL,
	})
	body := readJSONBody(t, resp2)
	attrs := body["Attributes"].(map[string]interface{})
	assert.Equal(t, "120", attrs["VisibilityTimeout"])
}

func TestSQSPlugin_JSON_SendReceiveDeleteMessage(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "msg-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/msg-json-queue"

	// Send message.
	resp := sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
		"QueueUrl":    qURL,
		"MessageBody": "hello json world",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	sendBody := readJSONBody(t, resp)
	assert.NotEmpty(t, sendBody["MessageId"])
	assert.NotEmpty(t, sendBody["MD5OfMessageBody"])

	// Receive message.
	resp2 := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 1,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	recvBody := readJSONBody(t, resp2)
	msgs := recvBody["Messages"].([]interface{})
	require.Len(t, msgs, 1)
	msg := msgs[0].(map[string]interface{})
	assert.Equal(t, "hello json world", msg["Body"])
	handle := msg["ReceiptHandle"].(string)

	// Delete message.
	resp3 := sqsJSONRequest(t, srv, "DeleteMessage", map[string]interface{}{
		"QueueUrl":      qURL,
		"ReceiptHandle": handle,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// Receive again — empty.
	resp4 := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 1,
	})
	recvBody4 := readJSONBody(t, resp4)
	msgs4 := recvBody4["Messages"].([]interface{})
	assert.Len(t, msgs4, 0)
}

func TestSQSPlugin_JSON_ReceiveMessage_Empty(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "empty-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/empty-json-queue"

	resp := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 10,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	b, _ := io.ReadAll(resp.Body)
	// Must be [] not null.
	assert.Contains(t, string(b), `"Messages":[]`)
}

func TestSQSPlugin_JSON_ListQueues(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	for _, name := range []string{"json-alpha", "json-beta", "json-alpha-two"} {
		sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
			"QueueName": name,
		})
	}

	// List all.
	resp := sqsJSONRequest(t, srv, "ListQueues", map[string]interface{}{})
	body := readJSONBody(t, resp)
	urls := body["QueueUrls"].([]interface{})
	names := make([]string, len(urls))
	for i, u := range urls {
		names[i] = u.(string)
	}
	assert.Len(t, names, 3)

	// List with prefix.
	resp2 := sqsJSONRequest(t, srv, "ListQueues", map[string]interface{}{
		"QueueNamePrefix": "json-alpha",
	})
	body2 := readJSONBody(t, resp2)
	urls2 := body2["QueueUrls"].([]interface{})
	assert.Len(t, urls2, 2)
	for _, u := range urls2 {
		assert.Contains(t, u.(string), "json-alpha")
	}
}

func TestSQSPlugin_JSON_PurgeQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "purge-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/purge-json-queue"

	for i := 0; i < 3; i++ {
		sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
			"QueueUrl":    qURL,
			"MessageBody": "msg",
		})
	}

	resp := sqsJSONRequest(t, srv, "PurgeQueue", map[string]interface{}{
		"QueueUrl": qURL,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 10,
	})
	body := readJSONBody(t, resp2)
	assert.Len(t, body["Messages"].([]interface{}), 0)
}

func TestSQSPlugin_JSON_SendMessageBatch(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "batch-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/batch-json-queue"

	resp := sqsJSONRequest(t, srv, "SendMessageBatch", map[string]interface{}{
		"QueueUrl": qURL,
		"Entries": []map[string]interface{}{
			{"Id": "e1", "MessageBody": "body-one"},
			{"Id": "e2", "MessageBody": "body-two"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readJSONBody(t, resp)
	successful := body["Successful"].([]interface{})
	assert.Len(t, successful, 2)

	// Receive both.
	resp2 := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 10,
	})
	body2 := readJSONBody(t, resp2)
	assert.Len(t, body2["Messages"].([]interface{}), 2)
}

func TestSQSPlugin_JSON_TagUntagListQueueTags(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "tag-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/tag-json-queue"

	// Tag the queue.
	resp := sqsJSONRequest(t, srv, "TagQueue", map[string]interface{}{
		"QueueUrl": qURL,
		"Tags":     map[string]string{"env": "prod", "team": "infra"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List tags.
	resp2 := sqsJSONRequest(t, srv, "ListQueueTags", map[string]interface{}{
		"QueueUrl": qURL,
	})
	body := readJSONBody(t, resp2)
	tags := body["Tags"].(map[string]interface{})
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	// Untag env.
	resp3 := sqsJSONRequest(t, srv, "UntagQueue", map[string]interface{}{
		"QueueUrl": qURL,
		"TagKeys":  []string{"env"},
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// List again — env gone, team remains.
	resp4 := sqsJSONRequest(t, srv, "ListQueueTags", map[string]interface{}{
		"QueueUrl": qURL,
	})
	body4 := readJSONBody(t, resp4)
	tags4 := body4["Tags"].(map[string]interface{})
	assert.NotContains(t, tags4, "env")
	assert.Equal(t, "infra", tags4["team"])
}

func TestSQSPlugin_JSON_ChangeMessageVisibility(t *testing.T) {
	srv, tc := newSQSTestServer(t)

	sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "cmv-json-queue",
	})
	qURL := "http://sqs.us-east-1.localhost/000000000000/cmv-json-queue"

	sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
		"QueueUrl":    qURL,
		"MessageBody": "cmv-test",
	})

	// Receive with default visibility.
	resp := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 1,
	})
	body := readJSONBody(t, resp)
	msgs := body["Messages"].([]interface{})
	require.Len(t, msgs, 1)
	handle := msgs[0].(map[string]interface{})["ReceiptHandle"].(string)

	// Change visibility to 0 — message immediately visible again.
	resp2 := sqsJSONRequest(t, srv, "ChangeMessageVisibility", map[string]interface{}{
		"QueueUrl":          qURL,
		"ReceiptHandle":     handle,
		"VisibilityTimeout": 0,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// Advance time slightly to ensure visibility window check passes.
	tc.SetTime(tc.Now().Add(1 * time.Second))

	resp3 := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 1,
	})
	body3 := readJSONBody(t, resp3)
	msgs3 := body3["Messages"].([]interface{})
	assert.Len(t, msgs3, 1)
}

func TestSQSPlugin_JSON_Error_NonExistentQueue(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	resp := sqsJSONRequest(t, srv, "SendMessage", map[string]interface{}{
		"QueueUrl":    "http://sqs.us-east-1.localhost/000000000000/no-such-queue",
		"MessageBody": "test",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSQSPlugin_CrossProtocol(t *testing.T) {
	srv, _ := newSQSTestServer(t)

	// Create via JSON.
	respCreate := sqsJSONRequest(t, srv, "CreateQueue", map[string]interface{}{
		"QueueName": "cross-queue",
	})
	require.Equal(t, http.StatusOK, respCreate.StatusCode)
	qURL := "http://sqs.us-east-1.localhost/000000000000/cross-queue"

	// Send via query protocol.
	sqsRequest(t, srv, map[string]string{
		"Action":      "SendMessage",
		"QueueUrl":    qURL,
		"MessageBody": "from-query-protocol",
	})

	// Receive via JSON.
	resp := sqsJSONRequest(t, srv, "ReceiveMessage", map[string]interface{}{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": 10,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readJSONBody(t, resp)
	msgs := body["Messages"].([]interface{})
	require.Len(t, msgs, 1)
	assert.Equal(t, "from-query-protocol", msgs[0].(map[string]interface{})["Body"])
}
