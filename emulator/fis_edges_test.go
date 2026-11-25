package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Edges of the FIS plugin (#1619): filter paths, failing actions, and the tagging API's writes.

// TestFIS_FilterPaths resolves one target per filter path against two instances that differ in
// it, and checks each selects only the matching one.
func TestFIS_FilterPaths(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := map[string]string{"fis": "target"}
	small := e.run(false, "", "", tagged)
	var big struct {
		IDs []string `xml:"instancesSet>item>instanceId"`
	}
	e.ec2(map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "m5.large", "MinCount": "1", "MaxCount": "1",
		"Placement.AvailabilityZone":      "us-east-1b",
		"TagSpecification.1.ResourceType": "instance", "TagSpecification.1.Tag.1.Key": "fis",
		"TagSpecification.1.Tag.1.Value": "target",
	}, &big)
	require.Len(t, big.IDs, 1)

	for _, tc := range []struct {
		path, value string
		want        []string
	}{
		{"InstanceType", "t3.micro", []string{small}},
		{"InstanceType", "m5.large", []string{big.IDs[0]}},
		{"Placement.AvailabilityZone", "us-east-1b", []string{big.IDs[0]}},
		{"InstanceId", small, []string{small}},
		{"State.Name", "running", []string{small, big.IDs[0]}},
		{"State.Code", "16", []string{small, big.IDs[0]}},
		{"Architecture", "x86_64", []string{small, big.IDs[0]}},
		{"ImageId", ec2TestImage, []string{small, big.IDs[0]}},
		{"InstanceLifecycle", "spot", nil},
		{"SpotInstanceRequestId", "sir-00000000", nil},
		{"KeyName", "none", nil},
		{"Placement.GroupName", "none", nil},
		{"Unmodeled.Path", "x", nil},
	} {
		tg := instanceTarget("ALL")
		tg["filters"] = []any{map[string]any{"path": tc.path, "values": []string{tc.value}}}
		id := e.template(map[string]any{
			"targets":           map[string]any{"t": tg},
			"experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
			"actions": map[string]any{"reboot": map[string]any{
				"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "t"},
			}},
		})
		x := e.start(id)
		var out struct {
			ResolvedTargets []struct {
				TargetInformation map[string]string `json:"targetInformation"`
			} `json:"resolvedTargets"`
		}
		e.mustFIS(http.MethodGet, "/experiments/"+x.ID+"/resolvedTargets?targetName=t", nil, &out)
		var got []string
		for _, r := range out.ResolvedTargets {
			got = append(got, r.TargetInformation["arn"][strings.LastIndex(r.TargetInformation["arn"], "/")+1:])
		}
		assert.ElementsMatch(t, tc.want, got, "%s=%s", tc.path, tc.value)
	}

	// Each value of one filter matches (OR), and a second filter narrows (AND).
	tg := instanceTarget("COUNT(5)")
	tg["filters"] = []any{
		map[string]any{"path": "InstanceType", "values": []string{"t3.micro", "m5.large"}},
		map[string]any{"path": "SubnetId", "values": []string{"subnet-nope"}},
	}
	id := e.template(map[string]any{
		"targets": map[string]any{"t": tg},
		"actions": map[string]any{"reboot": map[string]any{
			"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "t"},
		}},
	})
	assert.Equal(t, "failed", e.experiment(e.start(id).ID).State.Status, "no instance is in subnet-nope")
}

// A terminate-instances action that termination protection refuses fails the experiment with
// EC2's message.
func TestFIS_AFailingActionFailsTheExperiment(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	var run struct {
		IDs []string `xml:"instancesSet>item>instanceId"`
	}
	e.ec2(map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
		"DisableApiTermination":           "true",
		"TagSpecification.1.ResourceType": "instance", "TagSpecification.1.Tag.1.Key": "fis",
		"TagSpecification.1.Tag.1.Value": "target",
	}, &run)
	require.Len(t, run.IDs, 1)
	id := e.template(map[string]any{
		"targets": map[string]any{"all": instanceTarget("ALL")},
		"actions": map[string]any{
			"terminate": map[string]any{"actionId": "aws:ec2:terminate-instances", "targets": map[string]string{"Instances": "all"}},
			"later": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"},
				"startAfter": []string{"terminate"}},
		},
	})
	got := e.experiment(e.start(id).ID)
	assert.Equal(t, "failed", got.State.Status)
	assert.True(t, strings.HasPrefix(got.State.Reason, "Action terminate failed: "), got.State.Reason)
	assert.Equal(t, "failed", got.Actions["terminate"].State.Status)
	assert.Equal(t, "cancelled", got.Actions["later"].State.Status)
	assert.Equal(t, "running", e.instance(run.IDs[0]).State)
}

// An instance a stop-instances action stopped and something else terminated fails the restart,
// unless completeIfInstancesTerminated is true.
func TestFIS_RestartOfATerminatedInstance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		complete string
		want     string
	}{
		{"", "failed"},
		{"true", "completed"},
	} {
		t.Run("completeIfInstancesTerminated="+tc.complete, func(t *testing.T) {
			t.Parallel()
			e := newFISEnv(t)
			gone := e.run(false, "", "", map[string]string{"fis": "target"})
			kept := e.run(false, "", "", map[string]string{"fis": "target"})
			params := map[string]string{"startInstancesAfterDuration": "PT2M"}
			if tc.complete != "" {
				params["completeIfInstancesTerminated"] = tc.complete
			}
			id := e.template(map[string]any{
				"targets": map[string]any{"all": instanceTarget("ALL")},
				"actions": map[string]any{"stop": map[string]any{
					"actionId": "aws:ec2:stop-instances", "parameters": params,
					"targets": map[string]string{"Instances": "all"},
				}},
			})
			x := e.start(id)
			e.experiment(x.ID)
			e.ec2(map[string]string{"Action": "TerminateInstances", "InstanceId.1": gone}, nil)
			e.ts.AdvanceTime(2 * time.Minute)
			got := e.experiment(x.ID)
			assert.Equal(t, tc.want, got.State.Status, "%s", got.State.Reason)
			if tc.want == "completed" {
				assert.Equal(t, "running", e.instance(kept).State)
			} else {
				assert.Contains(t, got.Actions["stop"].State.Reason, gone)
			}
		})
	}
}

// The Resource Groups Tagging API's TagResources and UntagResources write FIS tags through the
// plugin.
func TestFIS_TaggingAPIWrites(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.template(map[string]any{"actions": map[string]any{"w": fisWaitAction()}})
	arn := "arn:aws:fis:us-east-1:123456789012:experiment-template/" + id
	tagging := func(op string, body any) map[string]any {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.ts.URL+"/", strings.NewReader(string(raw)))
		require.NoError(t, err)
		req.Host = "tagging.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+op)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var out map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		return out
	}
	bogus := "arn:aws:fis:us-east-1:123456789012:experiment-template/EXTnope"
	other := "arn:aws:fis:us-east-1:123456789012:action/aws:fis:wait"
	out := tagging("TagResources", map[string]any{"ResourceARNList": []string{arn, bogus, other},
		"Tags": map[string]string{"team": "aegc", "env": "dev"}})
	failed, _ := out["FailedResourcesMap"].(map[string]any)
	assert.Contains(t, failed, bogus)
	assert.Contains(t, failed, other)
	assert.NotContains(t, failed, arn)
	tagging("UntagResources", map[string]any{"ResourceARNList": []string{arn}, "TagKeys": []string{"env"}})
	var tags struct {
		Tags map[string]string `json:"tags"`
	}
	e.mustFIS(http.MethodGet, "/tags/"+arn, nil, &tags)
	assert.Equal(t, map[string]string{"team": "aegc"}, tags.Tags)

	out = tagging("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"aws:x": "y"}})
	failed, _ = out["FailedResourcesMap"].(map[string]any)
	assert.Contains(t, failed, arn, "an aws: key is refused")
}

// A target of a resource type other than EC2 resolves to its ARNs, and a no-op action with a
// duration completes after it.
func TestFIS_ANonEC2TargetResolvesToItsARNs(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	role := "arn:aws:iam::123456789012:role/worker"
	id := e.template(map[string]any{
		"targets": map[string]any{"roles": map[string]any{
			"resourceType": "aws:iam:role", "resourceArns": []string{role}, "selectionMode": "ALL",
		}},
		"actions": map[string]any{"throttle": map[string]any{
			"actionId": "aws:fis:inject-api-throttle-error", "targets": map[string]string{"Roles": "roles"},
			"parameters": map[string]string{"duration": "PT3M", "service": "ec2", "percentage": "50",
				"operations": "DescribeInstances"},
		}},
	})
	x := e.start(id)
	var out struct {
		ResolvedTargets []struct {
			ResourceType      string            `json:"resourceType"`
			TargetInformation map[string]string `json:"targetInformation"`
		} `json:"resolvedTargets"`
	}
	e.mustFIS(http.MethodGet, "/experiments/"+x.ID+"/resolvedTargets", nil, &out)
	require.Len(t, out.ResolvedTargets, 1)
	assert.Equal(t, "aws:iam:role", out.ResolvedTargets[0].ResourceType)
	assert.Equal(t, role, out.ResolvedTargets[0].TargetInformation["arn"])
	assert.Equal(t, "running", e.experiment(x.ID).State.Status)
	e.ts.AdvanceTime(3 * time.Minute)
	assert.Equal(t, "completed", e.experiment(x.ID).State.Status)
}

// Routes FIS does not serve are refused as unknown operations.
func TestFIS_UnknownRoutes(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/actions/aws:fis:wait"},
		{http.MethodDelete, "/actions"},
		{http.MethodGet, "/experimentTemplates/"},
		{http.MethodPost, "/experimentTemplates/EXT1"},
		{http.MethodGet, "/experiments/EXP1/actions"},
		{http.MethodPut, "/tags/arn:aws:fis:us-east-1:123456789012:experiment/EXP1"},
		{http.MethodGet, "/tags/"},
	} {
		status, out := e.fis(tc.method, tc.path, nil)
		assert.Equal(t, http.StatusNotFound, status, "%s %s: %s", tc.method, tc.path, out)
		assert.Contains(t, string(out), "UnknownOperationException")
	}
}
