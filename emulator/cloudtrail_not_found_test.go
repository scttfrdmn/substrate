package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudTrail answers a trail that does not exist at the status its pages publish (#1156).
//
// Every CloudTrail page that publishes TrailNotFoundException publishes it at HTTP 400, and no
// CloudTrail page publishes a 404 anywhere. loadTrail answered 404, and every operation that names a
// trail propagated it. These assertions go over the wire so the status a retry policy reads is the
// one under test, not only the code.

func TestCloudTrailNotFound_AnAbsentTrailIs400(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for _, op := range []string{"GetTrail", "GetTrailStatus", "UpdateTrail", "DeleteTrail", "StartLogging", "StopLogging"} {
		t.Run(op, func(t *testing.T) {
			status, body := cloudtrailWireCall(t, ts, op, map[string]any{"Name": "no-such-trail"})
			require.Equalf(t, http.StatusBadRequest, status, "%s on an absent trail: %s", op, body)
			var out map[string]any
			require.NoError(t, json.Unmarshal(body, &out), "decode %s refusal: %s", op, body)
			require.Containsf(t, string(body), "TrailNotFoundException", "%s names the published code: %s", op, body)
		})
	}
}

// DescribeTrails publishes no not-found refusal: a name in trailNameList that matches nothing is left
// out of the list, and the call answers 200.
func TestCloudTrailNotFound_DescribeTrailsSkipsANameThatMatchesNothing(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	cloudtrailOK(t, ts, "CreateTrail", map[string]any{"Name": "kept-trail", "S3BucketName": "bucket"})

	status, body := cloudtrailWireCall(t, ts, "DescribeTrails", map[string]any{"trailNameList": []string{"kept-trail", "no-such-trail"}})
	require.Equal(t, http.StatusOK, status, "DescribeTrails: %s", body)
	var out struct {
		TrailList []struct {
			Name string `json:"Name"`
		} `json:"trailList"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "decode DescribeTrails: %s", body)
	require.Len(t, out.TrailList, 1, "only the trail that exists is listed: %s", body)
	require.Equal(t, "kept-trail", out.TrailList[0].Name)
}

// A store fault while DescribeTrails reads a listed trail is an error, never a short list: only a
// trail the store does not hold is skipped.
func TestCloudTrailNotFound_DescribeTrailsReturnsAStoreFault(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"trail read", func(m *cfFaultStateManager) { m.failGet = "trail:" }},
		{"corrupt trail", func(m *cfFaultStateManager) { m.corruptGet = "trail:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.CloudTrailPlugin{}
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State:   fault,
				Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
			}))
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-ct-fault", IDs: emulator.NewIDMint("req-ct-fault")}
			_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{"Name": "fault-trail", "S3BucketName": "bucket"}))
			require.NoError(t, err, "CreateTrail")

			tc.arm(fault)
			_, err = p.HandleRequest(ctx, cloudtrailRequest(t, "DescribeTrails", map[string]any{}))
			require.Error(t, err, "%s must fail DescribeTrails, not answer a short list", tc.name)
			var awsErr *emulator.AWSError
			require.False(t, errors.As(err, &awsErr), "%s answered as the published %v", tc.name, awsErr)
		})
	}
}
