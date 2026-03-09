package substrate_test

import (
	"context"
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
