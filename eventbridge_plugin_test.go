package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newEBTestServer creates a test server with the EventBridge plugin registered.
func newEBTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	plugin := &substrate.EventBridgePlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// ebRequest sends an EventBridge JSON-protocol request.
func ebRequest(t *testing.T, srv *substrate.Server, op string, body any) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "events.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "AmazonEventBridge."+op)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/events/aws4_request, SignedHeaders=host, Signature=fake")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func ebReadBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return body
}

func TestEB_RuleLifecycle(t *testing.T) {
	srv := newEBTestServer(t)

	// PutRule creates a new rule.
	resp := ebRequest(t, srv, "PutRule", map[string]string{
		"Name":         "my-rule",
		"EventPattern": `{"source": ["aws.ec2"]}`,
		"State":        "ENABLED",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var putResult struct {
		RuleArn string `json:"RuleArn"`
	}
	require.NoError(t, json.Unmarshal(body, &putResult))
	assert.Contains(t, putResult.RuleArn, "my-rule")

	// ListRules returns the created rule.
	resp = ebRequest(t, srv, "ListRules", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = ebReadBody(t, resp)
	var listResult struct {
		Rules []struct {
			Name string `json:"Name"`
		} `json:"Rules"`
	}
	require.NoError(t, json.Unmarshal(body, &listResult))
	require.Len(t, listResult.Rules, 1)
	assert.Equal(t, "my-rule", listResult.Rules[0].Name)

	// DescribeRule returns full details.
	resp = ebRequest(t, srv, "DescribeRule", map[string]string{"Name": "my-rule"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = ebReadBody(t, resp)
	var descResult struct {
		Name  string `json:"Name"`
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(body, &descResult))
	assert.Equal(t, "my-rule", descResult.Name)
	assert.Equal(t, "ENABLED", descResult.State)

	// PutRule again updates the rule.
	resp = ebRequest(t, srv, "PutRule", map[string]string{
		"Name":  "my-rule",
		"State": "DISABLED",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEB_TargetLifecycle(t *testing.T) {
	srv := newEBTestServer(t)

	// Create rule.
	ebRequest(t, srv, "PutRule", map[string]string{"Name": "rule1", "State": "ENABLED"})

	// PutTargets.
	resp := ebRequest(t, srv, "PutTargets", map[string]any{
		"Rule": "rule1",
		"Targets": []map[string]string{
			{"Id": "target1", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var putResult struct {
		FailedEntryCount int `json:"FailedEntryCount"`
	}
	require.NoError(t, json.Unmarshal(body, &putResult))
	assert.Equal(t, 0, putResult.FailedEntryCount)

	// ListTargetsByRule.
	resp = ebRequest(t, srv, "ListTargetsByRule", map[string]string{"Rule": "rule1"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = ebReadBody(t, resp)
	var listResult struct {
		Targets []struct {
			Id  string `json:"Id"` //nolint:revive
			ARN string `json:"Arn"`
		} `json:"Targets"`
	}
	require.NoError(t, json.Unmarshal(body, &listResult))
	require.Len(t, listResult.Targets, 1)
	assert.Equal(t, "target1", listResult.Targets[0].Id)

	// RemoveTargets.
	resp = ebRequest(t, srv, "RemoveTargets", map[string]any{
		"Rule": "rule1",
		"Ids":  []string{"target1"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteRule should succeed now targets are removed.
	resp = ebRequest(t, srv, "DeleteRule", map[string]string{"Name": "rule1"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEB_DeleteRuleWithTargetsFails(t *testing.T) {
	srv := newEBTestServer(t)

	ebRequest(t, srv, "PutRule", map[string]string{"Name": "rule-with-targets", "State": "ENABLED"})
	ebRequest(t, srv, "PutTargets", map[string]any{
		"Rule":    "rule-with-targets",
		"Targets": []map[string]string{{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue"}},
	})

	resp := ebRequest(t, srv, "DeleteRule", map[string]string{"Name": "rule-with-targets"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEB_PutEvents(t *testing.T) {
	srv := newEBTestServer(t)

	resp := ebRequest(t, srv, "PutEvents", map[string]any{
		"Entries": []map[string]string{
			{
				"Source":     "com.example.app",
				"DetailType": "UserCreated",
				"Detail":     `{"userId": "u1"}`,
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var result struct {
		FailedEntryCount int `json:"FailedEntryCount"`
		Entries          []struct {
			EventID string `json:"EventId"`
		} `json:"Entries"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Equal(t, 0, result.FailedEntryCount)
	require.Len(t, result.Entries, 1)
	assert.NotEmpty(t, result.Entries[0].EventID)
}

func TestEB_PutEventsInvalidEntry(t *testing.T) {
	srv := newEBTestServer(t)

	// Missing required fields.
	resp := ebRequest(t, srv, "PutEvents", map[string]any{
		"Entries": []map[string]string{
			{"Source": "com.example.app"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var result struct {
		FailedEntryCount int `json:"FailedEntryCount"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Equal(t, 1, result.FailedEntryCount)
}

func TestEB_ListEventBuses(t *testing.T) {
	srv := newEBTestServer(t)

	resp := ebRequest(t, srv, "ListEventBuses", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var result struct {
		EventBuses []struct {
			Name string `json:"Name"`
		} `json:"EventBuses"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.EventBuses, 1)
	assert.Equal(t, "default", result.EventBuses[0].Name)
}

func TestEB_DescribeRuleNotFound(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "DescribeRule", map[string]string{"Name": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEB_ListRulesPrefix(t *testing.T) {
	srv := newEBTestServer(t)

	for _, n := range []string{"prod-rule1", "prod-rule2", "dev-rule1"} {
		ebRequest(t, srv, "PutRule", map[string]string{"Name": n, "State": "ENABLED"})
	}

	resp := ebRequest(t, srv, "ListRules", map[string]string{"NamePrefix": "prod-"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := ebReadBody(t, resp)
	var result struct {
		Rules []struct {
			Name string `json:"Name"`
		} `json:"Rules"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Len(t, result.Rules, 2)
}
