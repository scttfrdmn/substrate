package emulator_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Capacity units and instance weights (#1502). The catalog's c5.large — 2 vCPUs, 4,096 MiB,
// Intel, not burstable — is the only type the requirements below match, so every member of
// these groups weighs 2 vCPUs or 4,096 MiB.

const asgMIP = "MixedInstancesPolicy."

// irGroup returns the parameters of a mixed-instances group selecting c5.large by attribute.
func (e *asgEnv) irGroup(name, capacityType string, minSize, maxSize, desired int, extra map[string]string) map[string]string {
	const ir = asgMIP + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
	params := map[string]string{
		"AutoScalingGroupName": name,
		"MinSize":              fmt.Sprint(minSize),
		"MaxSize":              fmt.Sprint(maxSize),
		"DesiredCapacity":      fmt.Sprint(desired),
		"VPCZoneIdentifier":    e.subnetA + "," + e.subnetB,
		asgMIP + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": e.ltName,
		ir + "VCpuCount.Min":             "2",
		ir + "VCpuCount.Max":             "2",
		ir + "MemoryMiB.Min":             "4096",
		ir + "MemoryMiB.Max":             "4096",
		ir + "CpuManufacturers.member.1": "intel",
	}
	if capacityType != "" {
		params["DesiredCapacityType"] = capacityType
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// typedOverrides returns the parameters of a units group whose overrides name types and weights,
// in priority order.
func (e *asgEnv) typedOverrides(pairs ...string) map[string]string {
	out := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		n := i/2 + 1
		out[fmt.Sprintf("%sLaunchTemplate.Overrides.member.%d.InstanceType", asgMIP, n)] = pairs[i]
		if pairs[i+1] != "" {
			out[fmt.Sprintf("%sLaunchTemplate.Overrides.member.%d.WeightedCapacity", asgMIP, n)] = pairs[i+1]
		}
	}
	return out
}

func asgCapacityOf(g asgGroupOut, weight int) int {
	return len(g.Instances) * weight
}

// TestAutoScaling_VCPUCapacityCountsVCPUs is the issue's repro: a vcpu group's desired
// capacity is in vCPUs, so six of them is three 2-vCPU instances, and a decrementing terminate
// lowers desired by the instance's two.
func TestAutoScaling_VCPUCapacityCountsVCPUs(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.mustASG("CreateAutoScalingGroup", e.irGroup("g", "vcpu", 0, 32, 0, nil))
	g := e.group("g")
	assert.Empty(t, g.Instances)
	assert.Equal(t, "vcpu", g.CapacityType)

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "g", "DesiredCapacity": "6"})
	g = e.group("g")
	assert.Equal(t, 6, g.Desired, "desired capacity is reported in the group's units")
	assert.Equal(t, 32, g.Max)
	require.Len(t, g.Instances, 3, "enough 2-vCPU instances to reach 6 vCPUs")
	for _, inst := range g.Instances {
		assert.Equal(t, "c5.large", inst.InstanceType)
		assert.Equal(t, "2", inst.Weighted, "each instance carries its vCPUs as WeightedCapacity")
	}
	acts := e.activities(map[string]string{"AutoScalingGroupName": "g"})
	require.NotEmpty(t, acts)
	assert.Contains(t, acts[0].Cause, "increasing the capacity from 0 to 6.", "the activity speaks in the group's units")

	var details struct {
		Instances []struct {
			ID       string `xml:"InstanceId"`
			Weighted string `xml:"WeightedCapacity"`
		} `xml:"DescribeAutoScalingInstancesResult>AutoScalingInstances>member"`
	}
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingInstances", nil), &details))
	require.Len(t, details.Instances, 3)
	for _, d := range details.Instances {
		assert.Equal(t, "2", d.Weighted, "DescribeAutoScalingInstances carries it too")
	}

	victim := g.Instances[0].InstanceID
	body := e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
		"InstanceId": victim, "ShouldDecrementDesiredCapacity": "true",
	})
	assert.Contains(t, string(body), "shrinking the capacity from 6 to 4.")
	g = e.group("g")
	assert.Equal(t, 4, g.Desired, "desired drops by the instance's weight, not by one")
	assert.Len(t, g.Instances, 2)
	assert.NotContains(t, asgInstanceIDs(g), victim)

	e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
		"InstanceId": g.Instances[0].InstanceID, "ShouldDecrementDesiredCapacity": "false",
	})
	g = e.group("g")
	assert.Equal(t, 4, g.Desired)
	assert.Len(t, g.Instances, 2, "without decrementing the instance is replaced")
}

// TestAutoScaling_WeightedCapacityOvershootAndMax covers "current capacity will either be at the
// desired capacity or above it" and MaxSize's "never go above MaxSize by more than your largest
// instance weight".
func TestAutoScaling_WeightedCapacityOvershootAndMax(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		maxSize, desired   int
		wantInstances      int
		wantCapacityAtMost int
	}{
		{"odd desired overshoots by one vCPU", 32, 5, 3, 6},
		{"desired at an odd max exceeds max by less than one weight", 5, 5, 3, 6},
		{"exact", 8, 8, 4, 8},
		{"one vCPU is one whole instance", 4, 1, 1, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newASGEnv(t)
			e.mustASG("CreateAutoScalingGroup", e.irGroup("g", "vcpu", 0, tc.maxSize, tc.desired, nil))
			g := e.group("g")
			assert.Equal(t, tc.desired, g.Desired)
			require.Len(t, g.Instances, tc.wantInstances)
			capacity := asgCapacityOf(g, 2)
			assert.GreaterOrEqual(t, capacity, tc.desired)
			assert.LessOrEqual(t, capacity, tc.wantCapacityAtMost)
			assert.Less(t, capacity, tc.maxSize+2, "below MaxSize plus the largest weight")
		})
	}
}

func TestAutoScaling_MemoryMiBCapacityCountsMiB(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.mustASG("CreateAutoScalingGroup", e.irGroup("g", "memory-mib", 0, 65536, 12288, nil))
	g := e.group("g")
	assert.Equal(t, "memory-mib", g.CapacityType)
	assert.Equal(t, 12288, g.Desired)
	require.Len(t, g.Instances, 3, "three 4,096 MiB instances")
	for _, inst := range g.Instances {
		assert.Equal(t, "4096", inst.Weighted)
	}
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "g", "DesiredCapacity": "5000"})
	assert.Len(t, e.group("g").Instances, 2, "scale-in stops at the last instance that keeps capacity at or above desired")
}

// TestAutoScaling_UnweightedGroupsReportNoWeight keeps a units group's describe as it was: no
// WeightedCapacity on its members.
func TestAutoScaling_UnweightedGroupsReportNoWeight(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("plain", 0, 4, 2, nil)
	e.mustASG("CreateAutoScalingGroup", e.irGroup("ir", "units", 0, 4, 2, nil))
	for _, name := range []string{"plain", "ir"} {
		g := e.group(name)
		require.Len(t, g.Instances, 2, name)
		for _, inst := range g.Instances {
			assert.Empty(t, inst.Weighted, name)
		}
	}
	_, body := e.asg("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "plain"})
	assert.NotContains(t, string(body), "WeightedCapacity")
}

// TestAutoScaling_OverrideWeightsInUnits covers a units group whose overrides declare
// WeightedCapacity: the explicit weight is the instance's, a changed weight reconciles, and a
// removed type keeps its last weight.
func TestAutoScaling_OverrideWeightsInUnits(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	params := e.typedOverrides("m5.large", "4", "c5.large", "1")
	for k, v := range map[string]string{
		"AutoScalingGroupName": "w", "MinSize": "0", "MaxSize": "20", "DesiredCapacity": "7",
		"VPCZoneIdentifier": e.subnetA + "," + e.subnetB,
		asgMIP + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": e.ltName,
	} {
		params[k] = v
	}
	e.mustASG("CreateAutoScalingGroup", params)
	g := e.group("w")
	require.Len(t, g.Instances, 2, "the first override, m5.large at 4 units, launches twice to reach 7")
	for _, inst := range g.Instances {
		assert.Equal(t, "m5.large", inst.InstanceType, "weights do not change which type launches")
		assert.Equal(t, "4", inst.Weighted)
	}

	// Put c5.large first and ask for one more unit: one c5.large, at weight 1, reaches 9.
	upd := e.typedOverrides("c5.large", "1", "m5.large", "4")
	upd["AutoScalingGroupName"] = "w"
	upd["DesiredCapacity"] = "9"
	e.mustASG("UpdateAutoScalingGroup", upd)
	g = e.group("w")
	require.Len(t, g.Instances, 3)

	// Scale in to 6: the excess is 3, so only the 1-unit c5.large fits; after it goes the excess
	// is 2 and no m5.large fits, so the group stays at 8, above desired, rather than below it.
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "w", "DesiredCapacity": "6"})
	g = e.group("w")
	assert.Equal(t, 6, g.Desired)
	require.Len(t, g.Instances, 2)
	for _, inst := range g.Instances {
		assert.Equal(t, "m5.large", inst.InstanceType)
	}

	// Change m5.large's weight to 2: capacity is now 4, so the group launches back up to 6.
	upd = e.typedOverrides("m5.large", "2", "c5.large", "1")
	upd["AutoScalingGroupName"] = "w"
	e.mustASG("UpdateAutoScalingGroup", upd)
	g = e.group("w")
	require.Len(t, g.Instances, 3, "a changed weight reconciles to the desired capacity")
	for _, inst := range g.Instances {
		assert.Equal(t, "2", inst.Weighted)
	}

	// Drop m5.large from the overrides: its instances keep their last weight, so nothing moves.
	upd = e.typedOverrides("c5.large", "3")
	upd["AutoScalingGroupName"] = "w"
	e.mustASG("UpdateAutoScalingGroup", upd)
	g = e.group("w")
	require.Len(t, g.Instances, 3)
	for _, inst := range g.Instances {
		assert.Equal(t, "2", inst.Weighted, "a removed type keeps its last weight")
	}

	victim := g.Instances[0].InstanceID
	e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{"InstanceId": victim, "ShouldDecrementDesiredCapacity": "true"})
	g = e.group("w")
	assert.Equal(t, 4, g.Desired, "a decrement lowers desired by the explicit weight")
	assert.Len(t, g.Instances, 2)
}

// TestAutoScaling_WeightedSpotSplitIsInUnits checks OnDemandBaseCapacity and the percentage
// above it in vCPUs: base 2, 50% above, desired 6 is 4 On-Demand vCPUs and 2 Spot.
func TestAutoScaling_WeightedSpotSplitIsInUnits(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	const dst = asgMIP + "InstancesDistribution."
	e.mustASG("CreateAutoScalingGroup", e.irGroup("g", "vcpu", 0, 32, 6, map[string]string{
		dst + "OnDemandBaseCapacity":                "2",
		dst + "OnDemandPercentageAboveBaseCapacity": "50",
	}))
	spot, onDemand := 0, 0
	for _, inst := range ec2ByState(e.ec2GroupInstances("g"), "running") {
		if inst.Lifecycle == "spot" {
			spot++
		} else {
			onDemand++
		}
	}
	assert.Equal(t, 2, onDemand, "4 On-Demand vCPUs")
	assert.Equal(t, 1, spot, "2 Spot vCPUs")
}

// TestAutoScaling_WeightedSeedsAndReplacement checks the seeded launch failure and the
// self-termination replacement in vCPUs.
func TestAutoScaling_WeightedSeedsAndReplacement(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	seed := func(method, body, query string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, e.ts.URL+"/v1/autoscaling/launch-failure"+query, bytes.NewBufferString(body))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	seed(http.MethodPost, `{"autoScalingGroupName":"g"}`, "")
	e.mustASG("CreateAutoScalingGroup", e.irGroup("g", "vcpu", 0, 32, 6, nil))
	g := e.group("g")
	assert.Equal(t, 6, g.Desired)
	assert.Empty(t, g.Instances, "seeded, the vCPU capacity is left unmet")
	acts := e.activities(map[string]string{"AutoScalingGroupName": "g"})
	require.NotEmpty(t, acts)
	assert.Equal(t, "Failed", acts[0].StatusCode)

	seed(http.MethodDelete, "", "?autoScalingGroupName=g")
	g = e.group("g")
	require.Len(t, g.Instances, 3, "cleared, the next observation launches 6 vCPUs")

	e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": g.Instances[0].InstanceID}, nil)
	g = e.group("g")
	assert.Equal(t, 6, g.Desired)
	assert.Len(t, g.Instances, 3, "the self-terminated worker's 2 vCPUs are replaced")
}

func TestAutoScaling_CapacityTypeAndWeightValidation(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("plain", 0, 5, 0, nil)
	e.mustASG("CreateAutoScalingGroup", e.irGroup("ir", "", 0, 8, 0, nil))

	typed := func(name, capacityType string, pairs ...string) map[string]string {
		p := e.typedOverrides(pairs...)
		p["AutoScalingGroupName"] = name
		p["MinSize"], p["MaxSize"] = "0", "8"
		p["VPCZoneIdentifier"] = e.subnetA
		p[asgMIP+"LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName"] = e.ltName
		if capacityType != "" {
			p["DesiredCapacityType"] = capacityType
		}
		return p
	}
	const needsIR = "The DesiredCapacityType field is only allowed in mixed instances groups containing InstanceRequirements overrides."
	tests := []struct {
		name    string
		action  string
		params  map[string]string
		wantMsg string
	}{
		{"vcpu with a launch template", "CreateAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "x", "MinSize": "0", "MaxSize": "4", "VPCZoneIdentifier": e.subnetA,
			"LaunchTemplate.LaunchTemplateName": e.ltName, "DesiredCapacityType": "vcpu",
		}, needsIR},
		{"memory-mib with typed overrides", "CreateAutoScalingGroup", typed("x", "memory-mib", "m5.large", ""), needsIR},
		{"update a template group to vcpu", "UpdateAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "plain", "DesiredCapacityType": "vcpu",
		}, needsIR},
		{"unknown capacity type", "CreateAutoScalingGroup", typed("x", "cores", "m5.large", ""), "DesiredCapacityType must be one of"},
		{"some weights missing", "CreateAutoScalingGroup", typed("x", "", "m5.large", "2", "c5.large", ""),
			"Not all instance types have a defined WeightedCapacity."},
		{"weight zero", "CreateAutoScalingGroup", typed("x", "", "m5.large", "0"), "between 1 and 999"},
		{"weight 1000", "CreateAutoScalingGroup", typed("x", "", "m5.large", "1000"), "between 1 and 999"},
		{"weight not a number", "CreateAutoScalingGroup", typed("x", "", "m5.large", "two"), "must be a valid number"},
		{"weight not an integer", "CreateAutoScalingGroup", typed("x", "", "m5.large", "1.5"), "must be a valid integer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := e.asgErr(tc.action, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationError", code)
			assert.Contains(t, msg, tc.wantMsg)
		})
	}

	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "ir", "DesiredCapacityType": "vcpu", "DesiredCapacity": "4"})
	g := e.group("ir")
	assert.Equal(t, "vcpu", g.CapacityType, "an attribute-based group may move to vcpu")
	assert.Len(t, g.Instances, 2, "4 vCPUs")
	assert.Empty(t, e.group("plain").CapacityType, "a refused update changed nothing")
}

// TestAutoScaling_AWeightedSessionReplaysIdentically records a vcpu group scaling out, losing a
// worker to self-termination and being decremented by weight, and replays it byte for byte.
func TestAutoScaling_AWeightedSessionReplaysIdentically(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTime()
	e := &asgEnv{t: t, ts: ts, ltName: "replay-lt"}
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName":         e.ltName,
		"LaunchTemplateData.ImageId": ec2TestImage,
	}, nil)
	params := e.irGroup("replayed", "vcpu", 0, 32, 0, nil)
	delete(params, "VPCZoneIdentifier")
	params["AvailabilityZones.member.1"] = "us-east-1a"
	e.mustASG("CreateAutoScalingGroup", params)
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "replayed", "DesiredCapacity": "6"})
	g := e.group("replayed")
	require.Len(t, g.Instances, 3)
	e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": g.Instances[0].InstanceID}, nil)
	g = e.group("replayed")
	require.Len(t, g.Instances, 3)
	e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
		"InstanceId": g.Instances[1].InstanceID, "ShouldDecrementDesiredCapacity": "true",
	})
	e.mustASG("DescribeAutoScalingInstances", nil)

	recorded := asgReplayGroupRecord(t, ts)
	require.Equal(t, 4, recorded.DesiredCapacity)
	require.Len(t, recorded.Instances, 2)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences, "every response replays byte for byte: %s", replayDifferenceSummary(results))
	replayed := asgReplayGroupRecord(t, ts)
	assert.Equal(t, recorded.DesiredCapacity, replayed.DesiredCapacity)
	assert.Equal(t, recorded.Instances, replayed.Instances)
}
