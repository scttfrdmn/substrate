package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A trail's logging state is what GetTrailStatus reports (#1157).
//
// GetTrailStatus built its response with IsLogging hardcoded true and never read the flag
// StartLogging and StopLogging persist, and CreateTrail stored a trail already logging. So a stopped
// trail reported logging, a trail never started reported logging, and the only operation that
// publishes IsLogging could not tell either from a running one. These assertions go over the wire
// on the raw response bytes, on a frozen clock, so a logging time can be asserted exactly.

// cloudtrailLoggingHost is the Host header every request below carries.
const cloudtrailLoggingHost = "cloudtrail.us-east-1.amazonaws.com"

// cloudtrailLoggingClock is the instant the test server's clock is frozen at.
var cloudtrailLoggingClock = time.Unix(1700000000, 0).UTC()

// cloudtrailWireCall issues one CloudTrail operation through the target an SDK sends and returns the
// status and raw body.
func cloudtrailWireCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err, "build %s", op)
	req.Host = cloudtrailLoggingHost
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101."+op)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "%s", op)
	defer resp.Body.Close() //nolint:errcheck // a test read; the body is fully consumed below.
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s", op)
	return resp.StatusCode, out
}

// cloudtrailStatus answers GetTrailStatus's raw members for the trail.
func cloudtrailStatus(t *testing.T, ts *emulator.TestServer, name string) map[string]json.RawMessage {
	t.Helper()
	code, body := cloudtrailWireCall(t, ts, "GetTrailStatus", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, code, "GetTrailStatus: %s", body)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &out), "decode GetTrailStatus: %s", body)
	return out
}

// cloudtrailOK issues an operation that must succeed.
func cloudtrailOK(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) {
	t.Helper()
	code, out := cloudtrailWireCall(t, ts, op, body)
	require.Equal(t, http.StatusOK, code, "%s: %s", op, out)
}

// epoch renders an instant the way a CloudTrail Timestamp answers it: epoch seconds to three
// decimals, as EpochSeconds does.
func epoch(at time.Time) string {
	b, err := json.Marshal(emulator.EpochSeconds(at))
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestCloudTrailLogging_GetTrailStatusReportsTheWalk(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cloudtrailLoggingClock)
	trail := map[string]any{"Name": "walk-trail"}

	cloudtrailOK(t, ts, "CreateTrail", map[string]any{"Name": "walk-trail", "S3BucketName": "walk-bucket"})

	// A new trail is not logging, and has neither logging time: it has made neither transition.
	created := cloudtrailStatus(t, ts, "walk-trail")
	require.JSONEq(t, "false", string(created["IsLogging"]), "a new trail is not logging until StartLogging")
	require.NotContains(t, created, "StartLoggingTime", "a trail that has never logged reports no StartLoggingTime")
	require.NotContains(t, created, "StopLoggingTime", "a trail that has never stopped reports no StopLoggingTime")

	started := cloudtrailLoggingClock.Add(time.Minute)
	ts.FreezeTimeAt(started)
	cloudtrailOK(t, ts, "StartLogging", trail)
	afterStart := cloudtrailStatus(t, ts, "walk-trail")
	require.JSONEq(t, "true", string(afterStart["IsLogging"]), "StartLogging must be observable")
	require.Equal(t, epoch(started), string(afterStart["StartLoggingTime"]), "StartLoggingTime is the start, as epoch seconds")
	require.NotContains(t, afterStart, "StopLoggingTime")

	stopped := started.Add(time.Minute)
	ts.FreezeTimeAt(stopped)
	cloudtrailOK(t, ts, "StopLogging", trail)
	afterStop := cloudtrailStatus(t, ts, "walk-trail")
	require.JSONEq(t, "false", string(afterStop["IsLogging"]), "StopLogging must be observable")
	require.Equal(t, epoch(started), string(afterStop["StartLoggingTime"]), "the most recent start is unchanged by a stop")
	require.Equal(t, epoch(stopped), string(afterStop["StopLoggingTime"]), "StopLoggingTime is the stop")

	// Neither the fabricated delivery time nor the six members the page marks "no longer in use".
	for _, unpublished := range []string{
		"LatestDeliveryTime", "LatestDeliveryAttemptTime", "LatestDeliveryAttemptSucceeded",
		"LatestNotificationAttemptTime", "LatestNotificationAttemptSucceeded",
		"TimeLoggingStarted", "TimeLoggingStopped",
	} {
		require.NotContainsf(t, afterStop, unpublished, "GetTrailStatus answered %s, which substrate has nothing true to report for", unpublished)
	}
}

// A StartLogging on a trail already logging, or a StopLogging on one already stopped, succeeds and
// moves neither time: each records a transition, and a repeat is not one.
func TestCloudTrailLogging_ARepeatedStartOrStopMovesNoTime(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ts.FreezeTimeAt(cloudtrailLoggingClock)
	trail := map[string]any{"Name": "repeat-trail"}
	cloudtrailOK(t, ts, "CreateTrail", map[string]any{"Name": "repeat-trail", "S3BucketName": "repeat-bucket"})

	// A stop on a trail never started records no stop.
	ts.FreezeTimeAt(cloudtrailLoggingClock.Add(1 * time.Minute))
	cloudtrailOK(t, ts, "StopLogging", trail)
	require.NotContains(t, cloudtrailStatus(t, ts, "repeat-trail"), "StopLoggingTime", "a stop on a trail never started is no transition")

	// The first start records the start; a second, later, leaves it.
	first := cloudtrailLoggingClock.Add(2 * time.Minute)
	ts.FreezeTimeAt(first)
	cloudtrailOK(t, ts, "StartLogging", trail)
	ts.FreezeTimeAt(cloudtrailLoggingClock.Add(3 * time.Minute))
	cloudtrailOK(t, ts, "StartLogging", trail)
	require.Equal(t, epoch(first), string(cloudtrailStatus(t, ts, "repeat-trail")["StartLoggingTime"]), "a second start moved StartLoggingTime")

	// The first stop records the stop; a second, later, leaves it.
	stopped := cloudtrailLoggingClock.Add(4 * time.Minute)
	ts.FreezeTimeAt(stopped)
	cloudtrailOK(t, ts, "StopLogging", trail)
	ts.FreezeTimeAt(cloudtrailLoggingClock.Add(5 * time.Minute))
	cloudtrailOK(t, ts, "StopLogging", trail)
	require.Equal(t, epoch(stopped), string(cloudtrailStatus(t, ts, "repeat-trail")["StopLoggingTime"]), "a second stop moved StopLoggingTime")
}

// A store fault while GetTrailStatus or the logging writes read or write the trail is an error, never
// a 200 reporting a logging state nothing recorded.
func TestCloudTrailLogging_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		op   string
		arm  func(*cfFaultStateManager)
	}{
		{"GetTrailStatus, read", "GetTrailStatus", func(m *cfFaultStateManager) { m.failGet = "trail:" }},
		{"GetTrailStatus, corrupt record", "GetTrailStatus", func(m *cfFaultStateManager) { m.corruptGet = "trail:" }},
		{"StartLogging, write", "StartLogging", func(m *cfFaultStateManager) { m.failPut = "trail:" }},
		{"StopLogging, write", "StopLogging", func(m *cfFaultStateManager) { m.failPut = "trail:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.CloudTrailPlugin{}
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State:   fault,
				Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(cloudtrailLoggingClock)},
			}))
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ct-fault"}
			_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{"Name": "fault-trail", "S3BucketName": "fault-bucket"}))
			require.NoError(t, err, "CreateTrail")

			tc.arm(fault)
			_, err = p.HandleRequest(ctx, cloudtrailRequest(t, tc.op, map[string]any{"Name": "fault-trail"}))
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
