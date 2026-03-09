package substrate_test

import (
	"context"
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

func newSNSTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	snsPlugin := &substrate.SNSPlugin{}
	require.NoError(t, snsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(snsPlugin)

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(sqsPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func snsRequest(t *testing.T, srv *substrate.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "sns.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readSNSBody(t *testing.T, r *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return string(body)
}

func TestSNSPlugin_CreateTopic(t *testing.T) {
	srv := newSNSTestServer(t)

	// Create new topic.
	resp := snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "my-topic",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSNSBody(t, resp)
	assert.Contains(t, body, "arn:aws:sns:us-east-1:000000000000:my-topic")

	// Idempotent create — same ARN returned.
	resp2 := snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "my-topic",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2 := readSNSBody(t, resp2)
	assert.Contains(t, body2, "arn:aws:sns:us-east-1:000000000000:my-topic")
}

func TestSNSPlugin_DeleteTopic(t *testing.T) {
	srv := newSNSTestServer(t)

	// Create topic.
	resp := snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "delete-me",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Delete topic.
	resp2 := snsRequest(t, srv, map[string]string{
		"Action":   "DeleteTopic",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:delete-me",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// ListTopics should not include the deleted topic.
	resp3 := snsRequest(t, srv, map[string]string{
		"Action": "ListTopics",
	})
	body3 := readSNSBody(t, resp3)
	assert.NotContains(t, body3, "delete-me")
}

func TestSNSPlugin_Subscribe(t *testing.T) {
	srv := newSNSTestServer(t)

	// Create topic first.
	snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "sub-topic",
	})

	// Subscribe with SQS protocol.
	resp := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:sub-topic",
		"Protocol": "sqs",
		"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSNSBody(t, resp)
	assert.Contains(t, body, "SubscriptionArn")
	assert.Contains(t, body, "sub-topic")
}

func TestSNSPlugin_Unsubscribe(t *testing.T) {
	srv := newSNSTestServer(t)

	snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "unsub-topic",
	})

	subResp := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:unsub-topic",
		"Protocol": "sqs",
		"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue",
	})
	assert.Equal(t, http.StatusOK, subResp.StatusCode)
	subBody := readSNSBody(t, subResp)

	// Extract subscription ARN from response.
	start := strings.Index(subBody, "<SubscriptionArn>")
	end := strings.Index(subBody, "</SubscriptionArn>")
	require.True(t, start >= 0 && end > start, "subscription ARN not found in response")
	subARN := subBody[start+len("<SubscriptionArn>") : end]

	// Unsubscribe.
	resp := snsRequest(t, srv, map[string]string{
		"Action":          "Unsubscribe",
		"SubscriptionArn": subARN,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSNSPlugin_ListTopics(t *testing.T) {
	srv := newSNSTestServer(t)

	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		snsRequest(t, srv, map[string]string{
			"Action": "CreateTopic",
			"Name":   name,
		})
	}

	resp := snsRequest(t, srv, map[string]string{
		"Action": "ListTopics",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSNSBody(t, resp)
	assert.Contains(t, body, "topic-a")
	assert.Contains(t, body, "topic-b")
	assert.Contains(t, body, "topic-c")
}

func TestSNSPlugin_ListSubscriptions(t *testing.T) {
	srv := newSNSTestServer(t)

	snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "list-sub-topic",
	})
	snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:list-sub-topic",
		"Protocol": "sqs",
		"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/q1",
	})
	snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:list-sub-topic",
		"Protocol": "sqs",
		"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/q2",
	})

	resp := snsRequest(t, srv, map[string]string{
		"Action": "ListSubscriptions",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSNSBody(t, resp)
	assert.Contains(t, body, "list-sub-topic")
}

func TestSNSPlugin_Publish(t *testing.T) {
	srv := newSNSTestServer(t)

	snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "publish-topic",
	})

	// Publish without subscribers — should succeed.
	resp := snsRequest(t, srv, map[string]string{
		"Action":   "Publish",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:publish-topic",
		"Message":  "hello world",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSNSBody(t, resp)
	assert.Contains(t, body, "MessageId")
}

func TestSNSPlugin_TagResource_ListTagsForResource(t *testing.T) {
	srv := newSNSTestServer(t)

	snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "tagged-topic",
	})

	// Tag the topic.
	resp := snsRequest(t, srv, map[string]string{
		"Action":              "TagResource",
		"ResourceArn":         "arn:aws:sns:us-east-1:000000000000:tagged-topic",
		"Tags.member.1.Key":   "Env",
		"Tags.member.1.Value": "test",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List tags.
	resp2 := snsRequest(t, srv, map[string]string{
		"Action":      "ListTagsForResource",
		"ResourceArn": "arn:aws:sns:us-east-1:000000000000:tagged-topic",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2 := readSNSBody(t, resp2)
	assert.Contains(t, body2, "Env")
	assert.Contains(t, body2, "test")
}
