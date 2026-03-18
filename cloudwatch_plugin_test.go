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

// newCWAlarmTestServer creates a test server with the CloudWatch plugin.
func newCWAlarmTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	plugin := &substrate.CloudWatchPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// cwRequest sends a CloudWatch query-protocol request.
func cwRequest(t *testing.T, srv *substrate.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "monitoring.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=fake")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func cwReadBody(t *testing.T, r *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return string(body)
}

func TestCWAlarm_PutAndDescribe(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	// Create alarm.
	resp := cwRequest(t, srv, map[string]string{
		"Action":             "PutMetricAlarm",
		"AlarmName":          "cpu-high",
		"MetricName":         "CPUUtilization",
		"Namespace":          "AWS/EC2",
		"Statistic":          "Average",
		"ComparisonOperator": "GreaterThanThreshold",
		"Threshold":          "80",
		"EvaluationPeriods":  "2",
		"Period":             "300",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe alarms.
	resp = cwRequest(t, srv, map[string]string{"Action": "DescribeAlarms"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwReadBody(t, resp)
	assert.Contains(t, body, "cpu-high")
	assert.Contains(t, body, "INSUFFICIENT_DATA")
}

func TestCWAlarm_SetAlarmState(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	// Create alarm.
	cwRequest(t, srv, map[string]string{
		"Action":             "PutMetricAlarm",
		"AlarmName":          "my-alarm",
		"MetricName":         "Errors",
		"Namespace":          "AWS/Lambda",
		"ComparisonOperator": "GreaterThanThreshold",
		"Threshold":          "0",
		"EvaluationPeriods":  "1",
		"Period":             "60",
	})

	// Set state to ALARM.
	resp := cwRequest(t, srv, map[string]string{
		"Action":      "SetAlarmState",
		"AlarmName":   "my-alarm",
		"StateValue":  "ALARM",
		"StateReason": "testing",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe and verify state.
	resp = cwRequest(t, srv, map[string]string{
		"Action":              "DescribeAlarms",
		"AlarmNames.member.1": "my-alarm",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwReadBody(t, resp)
	assert.Contains(t, body, "ALARM")
	assert.Contains(t, body, "testing")
}

func TestCWAlarm_DeleteAlarms(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	// Create two alarms.
	for _, n := range []string{"alarm-a", "alarm-b"} {
		cwRequest(t, srv, map[string]string{
			"Action":             "PutMetricAlarm",
			"AlarmName":          n,
			"MetricName":         "Latency",
			"Namespace":          "AWS/ApiGateway",
			"ComparisonOperator": "GreaterThanThreshold",
			"Threshold":          "1000",
			"EvaluationPeriods":  "1",
			"Period":             "60",
		})
	}

	// Delete one.
	resp := cwRequest(t, srv, map[string]string{
		"Action":              "DeleteAlarms",
		"AlarmNames.member.1": "alarm-a",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Only alarm-b should remain.
	resp = cwRequest(t, srv, map[string]string{"Action": "DescribeAlarms"})
	body := cwReadBody(t, resp)
	assert.NotContains(t, body, "alarm-a")
	assert.Contains(t, body, "alarm-b")
}

func TestCWAlarm_DescribeByState(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "alarm1",
		"MetricName": "M", "Namespace": "N", "ComparisonOperator": "GreaterThanThreshold",
		"Threshold": "1", "EvaluationPeriods": "1", "Period": "60",
	})

	// Set to OK.
	cwRequest(t, srv, map[string]string{
		"Action": "SetAlarmState", "AlarmName": "alarm1",
		"StateValue": "OK", "StateReason": "test",
	})

	// Describe filtering by state OK — should return alarm1.
	resp := cwRequest(t, srv, map[string]string{
		"Action":     "DescribeAlarms",
		"StateValue": "OK",
	})
	body := cwReadBody(t, resp)
	assert.Contains(t, body, "alarm1")

	// Describe filtering by state ALARM — should not return alarm1.
	resp = cwRequest(t, srv, map[string]string{
		"Action":     "DescribeAlarms",
		"StateValue": "ALARM",
	})
	body = cwReadBody(t, resp)
	assert.NotContains(t, body, "alarm1")
}

func TestCWAlarm_DescribeAlarmsForMetric(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "cpu-alarm",
		"MetricName": "CPUUtilization", "Namespace": "AWS/EC2",
		"ComparisonOperator": "GreaterThanThreshold",
		"Threshold":          "90", "EvaluationPeriods": "1", "Period": "60",
	})

	resp := cwRequest(t, srv, map[string]string{
		"Action":     "DescribeAlarmsForMetric",
		"MetricName": "CPUUtilization",
		"Namespace":  "AWS/EC2",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwReadBody(t, resp)
	assert.Contains(t, body, "cpu-alarm")
}

func TestCWAlarm_EnableDisableActions(t *testing.T) {
	srv := newCWAlarmTestServer(t)

	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "actions-alarm",
		"MetricName": "M", "Namespace": "N", "ComparisonOperator": "GreaterThanThreshold",
		"Threshold": "1", "EvaluationPeriods": "1", "Period": "60",
		"ActionsEnabled": "true",
	})

	// Disable.
	resp := cwRequest(t, srv, map[string]string{
		"Action":              "DisableAlarmActions",
		"AlarmNames.member.1": "actions-alarm",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Enable.
	resp = cwRequest(t, srv, map[string]string{
		"Action":              "EnableAlarmActions",
		"AlarmNames.member.1": "actions-alarm",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCWAlarm_SetAlarmStateNotFound(t *testing.T) {
	srv := newCWAlarmTestServer(t)
	resp := cwRequest(t, srv, map[string]string{
		"Action":      "SetAlarmState",
		"AlarmName":   "nonexistent",
		"StateValue":  "OK",
		"StateReason": "test",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCW_GetMetricData_Empty(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	resp := cwRequest(t, srv, map[string]string{
		"Action":                                "GetMetricData",
		"StartTime":                             "2024-01-01T00:00:00Z",
		"EndTime":                               "2024-01-02T00:00:00Z",
		"MetricDataQueries.member.1.Id":         "size_0",
		"MetricDataQueries.member.1.MetricStat.Metric.Namespace":  "AWS/S3",
		"MetricDataQueries.member.1.MetricStat.Metric.MetricName": "BucketSizeBytes",
		"MetricDataQueries.member.1.MetricStat.Period":            "86400",
		"MetricDataQueries.member.1.MetricStat.Stat":              "Average",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close() //nolint:errcheck
}
