package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// newSchedulerTestServer creates a test server with only the SchedulerPlugin registered.
func newSchedulerTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	plugin := &substrate.SchedulerPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// schedulerRequest sends a REST/JSON request to the scheduler service.
func schedulerRequest(t *testing.T, ts *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	} else {
		reqBody = bytes.NewBufferString("{}")
	}

	req, err := http.NewRequest(method, ts.URL+path, reqBody)
	require.NoError(t, err)
	req.Host = "scheduler.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/scheduler/aws4_request, SignedHeaders=host, Signature=fake")

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// readBody reads and returns the full response body.
func readSchedulerBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return body
}

func TestScheduler_CreateGetDelete(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Create a schedule.
	createBody := `{
		"ScheduleExpression": "rate(5 minutes)",
		"State": "ENABLED",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-fn", "RoleArn": "arn:aws:iam::000000000000:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/my-schedule", createBody)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	body := readSchedulerBody(t, resp)
	var createResp struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	require.NoError(t, json.Unmarshal(body, &createResp))
	assert.Contains(t, createResp.ScheduleArn, "arn:aws:scheduler:")
	assert.Contains(t, createResp.ScheduleArn, "my-schedule")

	// Get the schedule.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/my-schedule", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	var getResp struct {
		Name               string `json:"Name"`
		State              string `json:"State"`
		ScheduleExpression string `json:"ScheduleExpression"`
		GroupName          string `json:"GroupName"`
	}
	require.NoError(t, json.Unmarshal(body, &getResp))
	assert.Equal(t, "my-schedule", getResp.Name)
	assert.Equal(t, "ENABLED", getResp.State)
	assert.Equal(t, "rate(5 minutes)", getResp.ScheduleExpression)
	assert.Equal(t, "default", getResp.GroupName)

	// Delete the schedule.
	resp = schedulerRequest(t, ts, http.MethodDelete, "/schedules/my-schedule", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Get after delete should return 404.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/my-schedule", "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestScheduler_UpdateSchedule(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Create a schedule.
	createBody := `{
		"ScheduleExpression": "rate(5 minutes)",
		"State": "ENABLED",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:000000000000:function:my-fn", "RoleArn": "arn:aws:iam::000000000000:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/update-test", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Update the schedule expression.
	updateBody := `{"ScheduleExpression": "cron(0 12 * * ? *)"}`
	resp = schedulerRequest(t, ts, http.MethodPut, "/schedules/update-test", updateBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)
	var updateResp struct {
		ScheduleArn string `json:"ScheduleArn"`
	}
	require.NoError(t, json.Unmarshal(body, &updateResp))
	assert.Contains(t, updateResp.ScheduleArn, "update-test")

	// Get and verify the updated expression.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/update-test", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	var getResp struct {
		ScheduleExpression string `json:"ScheduleExpression"`
	}
	require.NoError(t, json.Unmarshal(body, &getResp))
	assert.Equal(t, "cron(0 12 * * ? *)", getResp.ScheduleExpression)
}

func TestScheduler_CreateDuplicate(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	createBody := `{
		"ScheduleExpression": "rate(1 hour)",
		"Target": {"Arn": "arn:aws:sqs:us-east-1:000000000000:my-queue", "RoleArn": "arn:aws:iam::000000000000:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`

	// First create should succeed.
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/dup-test", createBody)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Second create of same name should return 409 Conflict.
	resp = schedulerRequest(t, ts, http.MethodPost, "/schedules/dup-test", createBody)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestScheduler_ListSchedules(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:000000000000:function:fn", "RoleArn": "arn:aws:iam::000000000000:role/role"}`
	ftw := `{"Mode": "OFF"}`

	// Create 3 schedules: alpha-1, alpha-2, beta-1.
	for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
		require.Equal(t, http.StatusCreated, resp.StatusCode, "create %s", name)
	}

	// List all schedules — expect 3.
	resp := schedulerRequest(t, ts, http.MethodGet, "/schedules", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)
	var listResp struct {
		Schedules []struct {
			Name string `json:"Name"`
		} `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(body, &listResp))
	assert.Len(t, listResp.Schedules, 3)

	// List with namePrefix=alpha — expect 2.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?namePrefix=alpha", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &listResp))
	assert.Len(t, listResp.Schedules, 2)

	// List with state=DISABLED — expect 0 (all are ENABLED by default).
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?state=DISABLED", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &listResp))
	assert.Len(t, listResp.Schedules, 0)
}

func TestScheduler_ListPagination(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:000000000000:function:fn", "RoleArn": "arn:aws:iam::000000000000:role/role"}`
	ftw := `{"Mode": "OFF"}`

	// Create 5 schedules.
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("page-sched-%d", i)
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
		require.Equal(t, http.StatusCreated, resp.StatusCode, "create %s", name)
	}

	// First page: maxResults=2.
	resp := schedulerRequest(t, ts, http.MethodGet, "/schedules?maxResults=2", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)
	var page1 struct {
		Schedules []struct {
			Name string `json:"Name"`
		} `json:"Schedules"`
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(body, &page1))
	assert.Len(t, page1.Schedules, 2)
	assert.NotEmpty(t, page1.NextToken)

	// Second page using nextToken.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?maxResults=2&nextToken="+page1.NextToken, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	var page2 struct {
		Schedules []struct {
			Name string `json:"Name"`
		} `json:"Schedules"`
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(body, &page2))
	assert.Len(t, page2.Schedules, 2)
	assert.NotEmpty(t, page2.NextToken)

	// Third page — should have the remaining 1 item and no NextToken.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?maxResults=2&nextToken="+page2.NextToken, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	var page3 struct {
		Schedules []struct {
			Name string `json:"Name"`
		} `json:"Schedules"`
		NextToken string `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(body, &page3))
	assert.Len(t, page3.Schedules, 1)
	assert.Empty(t, page3.NextToken)
}
