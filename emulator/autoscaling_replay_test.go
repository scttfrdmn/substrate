package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestAutoScaling_ARecordedSessionReplaysIdentically records a group's whole life — create,
// a seeded launch failure, a self-terminated worker replaced on observation, a scale-in —
// and replays it.
//
// Every launch and termination a group makes is derived from the request that triggered it,
// with identifiers minted from that request's ID, so the replay must reproduce every
// recorded response body byte for byte and leave the group holding the same instances. The
// inner EC2 calls are not events of their own; if they were, the replay would apply each
// twice and the instance counts below would double.
func TestAutoScaling_ARecordedSessionReplaysIdentically(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTime()
	e := &asgEnv{t: t, ts: ts, ltName: "replay-lt"}
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName":         e.ltName,
		"LaunchTemplateData.ImageId": ec2TestImage,
	}, nil)

	seed := func(method, body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+"/v1/autoscaling/launch-failure", bytes.NewBufferString(body))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	params := map[string]string{
		"AutoScalingGroupName":              "replayed",
		"MinSize":                           "0",
		"MaxSize":                           "4",
		"DesiredCapacity":                   "3",
		"AvailabilityZones.member.1":        "us-east-1a",
		"LaunchTemplate.LaunchTemplateName": e.ltName,
		"Tags.member.1.Key":                 "pool",
		"Tags.member.1.Value":               "a",
		"Tags.member.1.PropagateAtLaunch":   "true",
	}
	seed(http.MethodPost, `{"autoScalingGroupName":"replayed"}`)
	e.mustASG("CreateAutoScalingGroup", params)
	require.Empty(t, e.group("replayed").Instances, "seeded, nothing launches")
	seed(http.MethodDelete, "")
	g := e.group("replayed")
	require.Len(t, g.Instances, 3)
	e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": g.Instances[0].InstanceID}, nil)
	require.Len(t, e.group("replayed").Instances, 3)
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "replayed", "DesiredCapacity": "2"})
	e.activities(map[string]string{"AutoScalingGroupName": "replayed"})

	recorded := asgReplayGroupRecord(t, ts)
	require.Len(t, recorded.Instances, 2)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences, "every response replays byte for byte: %s", replayDifferenceSummary(results))

	replayed := asgReplayGroupRecord(t, ts)
	assert.Equal(t, recorded.ARN, replayed.ARN)
	assert.Equal(t, recorded.Instances, replayed.Instances, "the same instances, launched in the same order")
	assert.Len(t, e.ec2GroupInstances("replayed"), 4, "three launched, one replacement; no launch applied twice")
}

func asgReplayGroupRecord(t *testing.T, ts *emulator.TestServer) emulator.AutoScalingGroup {
	t.Helper()
	raw, err := ts.StateManager().Get(context.Background(), "autoscaling", "group:123456789012/us-east-1/replayed") //nolint:contextcheck
	require.NoError(t, err)
	require.NotNil(t, raw)
	var g emulator.AutoScalingGroup
	require.NoError(t, json.Unmarshal(raw, &g))
	return g
}
