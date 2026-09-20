package emulator_test

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

	"github.com/scttfrdmn/substrate/emulator"
)

// newSchedulerTestServer creates a test server with only the SchedulerPlugin registered.
func newSchedulerTestServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Now())

	plugin := &emulator.SchedulerPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
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
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn", "RoleArn": "arn:aws:iam::123456789012:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/my-schedule", createBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
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
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn", "RoleArn": "arn:aws:iam::123456789012:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/update-test", createBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Update the schedule expression. UpdateSchedule publishes the same three Required: Yes members as
	// the create, so an update naming only the expression is refused (#1008); the caller restates them.
	updateBody := `{
		"ScheduleExpression": "cron(0 12 * * ? *)",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn", "RoleArn": "arn:aws:iam::123456789012:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
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
		"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:my-queue", "RoleArn": "arn:aws:iam::123456789012:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`

	// First create should succeed.
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/dup-test", createBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Second create of same name should return 409 Conflict.
	resp = schedulerRequest(t, ts, http.MethodPost, "/schedules/dup-test", createBody)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestScheduler_ListSchedules(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"}`
	ftw := `{"Mode": "OFF"}`

	// Create 3 schedules: alpha-1, alpha-2, beta-1.
	for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", name)
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

	// List with NamePrefix=alpha — expect 2. PascalCase is what API_ListSchedules publishes (#1226).
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?NamePrefix=alpha", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &listResp))
	assert.Len(t, listResp.Schedules, 2)

	// List with State=DISABLED — expect 0 (all are ENABLED by default).
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?State=DISABLED", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)
	require.NoError(t, json.Unmarshal(body, &listResp))
	assert.Len(t, listResp.Schedules, 0)
}

// TestScheduler_ListSchedulesReadsThePublishedQueryKeys pins the spellings API_ListSchedules
// publishes, and pins that the lowerCamel spellings substrate used to read are now inert (#1226).
//
// The two halves are asserted together because the defect was invisible from either alone: the old
// tests passed on the lowerCamel form, and a real SDK can only send the PascalCase form, so nothing
// compared the two. The inert half matters as much as the working half — AWS ignores a query
// parameter its model does not carry, so accepting one would leave a call that filters against
// substrate and silently does not against AWS.
func TestScheduler_ListSchedulesReadsThePublishedQueryKeys(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"}`
	ftw := `{"Mode": "OFF"}`
	for _, name := range []string{"keys-a1", "keys-a2", "keys-b1"} {
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", name)
	}

	count := func(t *testing.T, query string) (int, string) {
		t.Helper()
		resp := schedulerRequest(t, ts, http.MethodGet, "/schedules"+query, "")
		require.Equal(t, http.StatusOK, resp.StatusCode, query)
		var out struct {
			Schedules []struct {
				Name string `json:"Name"`
			} `json:"Schedules"`
			NextToken string `json:"NextToken"`
		}
		require.NoError(t, json.Unmarshal(readSchedulerBody(t, resp), &out), query)
		return len(out.Schedules), out.NextToken
	}

	t.Run("the published spellings are read", func(t *testing.T) {
		got, _ := count(t, "?NamePrefix=keys-a")
		assert.Equal(t, 2, got, "NamePrefix")

		got, _ = count(t, "?State=DISABLED")
		assert.Equal(t, 0, got, "State")

		got, token := count(t, "?MaxResults=1")
		assert.Equal(t, 1, got, "MaxResults")
		require.NotEmpty(t, token, "MaxResults=1 must leave a cursor")

		got, _ = count(t, "?MaxResults=1&NextToken="+token)
		assert.Equal(t, 1, got, "NextToken advances the page")

		// ScheduleGroup, not GroupName: the page names the parameter GroupName and binds it to the
		// ScheduleGroup key, so only the key is observable. A group holding nothing lists nothing,
		// where the unread parameter would have listed the default group's three.
		got, _ = count(t, "?ScheduleGroup=other")
		assert.Equal(t, 0, got, "ScheduleGroup")
	})

	t.Run("the lowerCamel spellings are ignored", func(t *testing.T) {
		for _, query := range []string{
			"?namePrefix=keys-a",
			"?state=DISABLED",
			"?maxResults=1",
			"?scheduleGroup=other",
			"?groupName=other",
		} {
			got, _ := count(t, query)
			assert.Equal(t, 3, got, "%s must be ignored, as AWS ignores an unmodelled parameter", query)
		}
	})
}

// TestScheduler_ListSchedulesMaxResultsReadings pins the two page-size readings that are substrate's
// own rather than the page's, now that MaxResults is read at all (#1226).
//
// API_ListSchedules publishes a Valid Range of 1–100 and **no default**. Substrate answers 20 when the
// parameter is absent or unusable, and clamps a larger request to 100 rather than refusing it — both
// recorded in docs/services.md as divergences. They are asserted here because an unasserted reading is
// how the lowerCamel keys survived: nothing observed what the operation actually did.
//
// A clamp is only observable above the maximum, so this needs more than 100 schedules; that is why it
// is a separate test from the query-key one rather than another subtest of it.
func TestScheduler_ListSchedulesMaxResultsReadings(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"}`
	ftw := `{"Mode": "OFF"}`
	const total = 101
	for i := range total {
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, fmt.Sprintf("/schedules/cap-%03d", i), body)
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %d", i)
	}

	page := func(t *testing.T, query string) (int, string) {
		t.Helper()
		resp := schedulerRequest(t, ts, http.MethodGet, "/schedules"+query, "")
		require.Equal(t, http.StatusOK, resp.StatusCode, query)
		var out struct {
			Schedules []struct {
				Name string `json:"Name"`
			} `json:"Schedules"`
			NextToken string `json:"NextToken"`
		}
		require.NoError(t, json.Unmarshal(readSchedulerBody(t, resp), &out), query)
		return len(out.Schedules), out.NextToken
	}

	for _, tc := range []struct {
		name  string
		query string
		want  int
		why   string
	}{
		{"absent", "", 20, "the unpublished default of 20"},
		{"above the published maximum", "?MaxResults=500", 100, "clamped to the published maximum, not refused"},
		{"at the published maximum", "?MaxResults=100", 100, "the maximum is honored as given"},
		{"zero", "?MaxResults=0", 20, "outside the range 1-100 and silently ignored, not refused"},
		{"negative", "?MaxResults=-5", 20, "silently ignored, not refused"},
		{"not a number", "?MaxResults=many", 20, "unparseable and silently ignored, not refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, token := page(t, tc.query)
			assert.Equal(t, tc.want, got, tc.why)
			assert.NotEmpty(t, token, "%d of %d schedules leaves a cursor", tc.want, total)
		})
	}
}

func TestScheduler_ListPagination(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	target := `{"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"}`
	ftw := `{"Mode": "OFF"}`

	// Create 5 schedules.
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("page-sched-%d", i)
		body := fmt.Sprintf(`{"ScheduleExpression": "rate(1 hour)", "Target": %s, "FlexibleTimeWindow": %s}`, target, ftw)
		resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", name)
	}

	// First page: MaxResults=2.
	resp := schedulerRequest(t, ts, http.MethodGet, "/schedules?MaxResults=2", "")
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

	// Second page using NextToken.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?MaxResults=2&NextToken="+page1.NextToken, "")
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
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules?MaxResults=2&NextToken="+page2.NextToken, "")
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

// TestScheduler_TimestampFormat verifies that CreationDate and LastModificationDate
// are returned as Unix epoch float64 values, not RFC3339 strings (#231).
func TestScheduler_TimestampFormat(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	createBody := `{
		"ScheduleExpression": "rate(1 hour)",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/r"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/ts-test", createBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// GetSchedule — verify CreationDate is numeric.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/ts-test", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &raw))

	// CreationDate must decode as a number, not a quoted string.
	var creationDate float64
	require.NoError(t, json.Unmarshal(raw["CreationDate"], &creationDate),
		"CreationDate should be a float64 Unix timestamp, got: %s", raw["CreationDate"])
	assert.Greater(t, creationDate, float64(0))

	var lastModDate float64
	require.NoError(t, json.Unmarshal(raw["LastModificationDate"], &lastModDate),
		"LastModificationDate should be a float64 Unix timestamp, got: %s", raw["LastModificationDate"])
	assert.Greater(t, lastModDate, float64(0))

	// ListSchedules — verify the same.
	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body = readSchedulerBody(t, resp)

	var listRaw struct {
		Schedules []map[string]json.RawMessage `json:"Schedules"`
	}
	require.NoError(t, json.Unmarshal(body, &listRaw))
	require.Len(t, listRaw.Schedules, 1)

	var listCreationDate float64
	require.NoError(t, json.Unmarshal(listRaw.Schedules[0]["CreationDate"], &listCreationDate),
		"ListSchedules CreationDate should be float64, got: %s", listRaw.Schedules[0]["CreationDate"])
	assert.Greater(t, listCreationDate, float64(0))
}
