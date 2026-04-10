package substrate_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(sqsPlugin)

	snsPlugin := &substrate.SNSPlugin{}
	require.NoError(t, snsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(snsPlugin)

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

// sqsRequestViaSNSServer sends an SQS request to the combined test server.
func sqsRequestViaSNSServer(t *testing.T, srv *substrate.Server, params map[string]string) *http.Response {
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

// TestSNS_Publish_SQSEnvelope verifies that messages published to SNS are delivered
// to SQS subscribers wrapped in the standard SNS notification JSON envelope.
func TestSNS_Publish_SQSEnvelope(t *testing.T) {
	srv := newSNSTestServer(t)

	// Create topic.
	resp := snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "envelope-topic",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create SQS queue.
	resp2 := sqsRequestViaSNSServer(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "envelope-queue",
	})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	queueURL := "http://sqs.us-east-1.amazonaws.com/000000000000/envelope-queue"

	// Subscribe SQS to topic.
	resp3 := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:envelope-topic",
		"Protocol": "sqs",
		"Endpoint": queueURL,
	})
	require.Equal(t, http.StatusOK, resp3.StatusCode)

	// Publish message.
	resp4 := snsRequest(t, srv, map[string]string{
		"Action":   "Publish",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:envelope-topic",
		"Message":  "hello from sns",
		"Subject":  "Test Subject",
	})
	require.Equal(t, http.StatusOK, resp4.StatusCode)

	// Receive from SQS.
	resp5 := sqsRequestViaSNSServer(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
	})
	require.Equal(t, http.StatusOK, resp5.StatusCode)
	body := readSNSBody(t, resp5)

	// The SQS message body should be the SNS envelope JSON.
	assert.Contains(t, body, "Notification")
	assert.Contains(t, body, "hello from sns")
	assert.Contains(t, body, "envelope-topic")
}

// TestSNS_Publish_HTTPEndpoint verifies HTTP/S protocol delivery.
func TestSNS_Publish_HTTPEndpoint(t *testing.T) {
	var mu sync.Mutex
	var received []byte

	// Start HTTP endpoint to receive SNS notifications.
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer endpoint.Close()

	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	snsPlugin := &substrate.SNSPlugin{}
	require.NoError(t, snsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(snsPlugin)

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	// Create topic.
	resp := snsRequest(t, srv, map[string]string{
		"Action": "CreateTopic",
		"Name":   "http-topic",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Subscribe HTTP endpoint.
	resp2 := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:http-topic",
		"Protocol": "http",
		"Endpoint": endpoint.URL,
	})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	// Publish message.
	resp3 := snsRequest(t, srv, map[string]string{
		"Action":   "Publish",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:http-topic",
		"Message":  "hello http",
		"Subject":  "HTTP Test",
	})
	require.Equal(t, http.StatusOK, resp3.StatusCode)

	// Verify HTTP endpoint received the envelope.
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, received, "HTTP endpoint should have received a POST")

	var envelope map[string]interface{}
	require.NoError(t, json.Unmarshal(received, &envelope))
	assert.Equal(t, "Notification", envelope["Type"])
	assert.Equal(t, "hello http", envelope["Message"])
	assert.Equal(t, "HTTP Test", envelope["Subject"])
	assert.Contains(t, envelope["TopicArn"], "http-topic")
}

// TestSNS_Publish_FilterPolicy verifies that subscriptions with filter policies
// only receive matching messages.
func TestSNS_Publish_FilterPolicy(t *testing.T) {
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	snsPlugin := &substrate.SNSPlugin{}
	require.NoError(t, snsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(snsPlugin)

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(sqsPlugin)

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	// Create topic + queue.
	resp := snsRequest(t, srv, map[string]string{"Action": "CreateTopic", "Name": "filter-topic"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := sqsRequestViaSNSServer(t, srv, map[string]string{"Action": "CreateQueue", "QueueName": "filter-queue"})
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	queueURL := "http://sqs.us-east-1.amazonaws.com/000000000000/filter-queue"

	// Subscribe with filter policy. We need to seed the filter policy directly
	// into state since SetSubscriptionAttributes is not yet integrated.
	resp3 := snsRequest(t, srv, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": "arn:aws:sns:us-east-1:000000000000:filter-topic",
		"Protocol": "sqs",
		"Endpoint": queueURL,
	})
	require.Equal(t, http.StatusOK, resp3.StatusCode)

	// Seed filter policy directly into state on the subscription.
	subKeys, _ := state.List(context.Background(), "sns", "sub_ids:000000000000/us-east-1/filter-topic")
	require.NotEmpty(t, subKeys)

	// Load subscription IDs.
	subIDsData, _ := state.Get(context.Background(), "sns", "sub_ids:000000000000/us-east-1/filter-topic")
	var subIDs []string
	require.NoError(t, json.Unmarshal(subIDsData, &subIDs))
	require.Len(t, subIDs, 1)

	// Load subscription, add filter policy, save.
	subData, _ := state.Get(context.Background(), "sns", "subscription:000000000000/us-east-1/"+subIDs[0])
	var sub substrate.SNSSubscription
	require.NoError(t, json.Unmarshal(subData, &sub))
	sub.FilterPolicy = map[string]interface{}{"color": []interface{}{"red", "blue"}}
	updated, _ := json.Marshal(sub)
	require.NoError(t, state.Put(context.Background(), "sns", "subscription:000000000000/us-east-1/"+subIDs[0], updated))

	// Publish matching message (color=red) → should be delivered.
	resp4 := snsRequest(t, srv, map[string]string{
		"Action":                         "Publish",
		"TopicArn":                       "arn:aws:sns:us-east-1:000000000000:filter-topic",
		"Message":                        "red message",
		"MessageAttributes.entry.1.Name": "color",
		"MessageAttributes.entry.1.Value.DataType":    "String",
		"MessageAttributes.entry.1.Value.StringValue": "red",
	})
	require.Equal(t, http.StatusOK, resp4.StatusCode)

	// Publish non-matching message (color=green) → should NOT be delivered.
	resp5 := snsRequest(t, srv, map[string]string{
		"Action":                         "Publish",
		"TopicArn":                       "arn:aws:sns:us-east-1:000000000000:filter-topic",
		"Message":                        "green message",
		"MessageAttributes.entry.1.Name": "color",
		"MessageAttributes.entry.1.Value.DataType":    "String",
		"MessageAttributes.entry.1.Value.StringValue": "green",
	})
	require.Equal(t, http.StatusOK, resp5.StatusCode)

	// Receive from SQS — should only get the red message.
	resp6 := sqsRequestViaSNSServer(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "10",
	})
	require.Equal(t, http.StatusOK, resp6.StatusCode)
	body := readSNSBody(t, resp6)
	assert.Contains(t, body, "red message")
	assert.NotContains(t, body, "green message")
}
