package emulator_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// asgEnv is a test server holding a VPC with one subnet in each of two zones and a launch
// template, which is what every Auto Scaling test launches from.
type asgEnv struct {
	t       *testing.T
	ts      *emulator.TestServer
	ltID    string
	ltName  string
	subnetA string
	subnetB string
}

func newASGEnv(t *testing.T) *asgEnv {
	t.Helper()
	ts := emulator.StartTestServer(t)
	// A frozen clock: activity causes, launch times and the HonorCooldown window all read it,
	// and no assertion here may depend on how long the test took.
	ts.FreezeTime()
	e := &asgEnv{t: t, ts: ts, ltName: "asg-lt"}

	var vpc struct {
		ID string `xml:"vpc>vpcId"`
	}
	e.ec2XML("CreateVpc", map[string]string{"CidrBlock": "10.40.0.0/16"}, &vpc)
	for _, s := range []struct {
		cidr, az string
		dst      *string
	}{{"10.40.1.0/24", "us-east-1a", &e.subnetA}, {"10.40.2.0/24", "us-east-1b", &e.subnetB}} {
		var sub struct {
			ID string `xml:"subnet>subnetId"`
		}
		e.ec2XML("CreateSubnet", map[string]string{"VpcId": vpc.ID, "CidrBlock": s.cidr, "AvailabilityZone": s.az}, &sub)
		*s.dst = sub.ID
	}
	var lt struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName":                                 e.ltName,
		"LaunchTemplateData.ImageId":                         ec2TestImage,
		"LaunchTemplateData.InstanceType":                    "t3.micro",
		"LaunchTemplateData.TagSpecification.1.ResourceType": "instance",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Key":    "FromTemplate",
		"LaunchTemplateData.TagSpecification.1.Tag.1.Value":  "yes",
		"LaunchTemplateData.TagSpecification.1.Tag.2.Key":    "Team",
		"LaunchTemplateData.TagSpecification.1.Tag.2.Value":  "template-loses",
	}, &lt)
	e.ltID = lt.ID
	require.NotEmpty(t, e.ltID)
	return e
}

// call sends one query-protocol request to the named service.
func (e *asgEnv) call(host, action string, params map[string]string) (int, []byte) {
	e.t.Helper()
	form := url.Values{"Action": {action}}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(e.t.Context(), http.MethodPost, e.ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(e.t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return resp.StatusCode, body
}

func (e *asgEnv) ec2XML(action string, params map[string]string, out any) {
	e.t.Helper()
	status, body := e.call("ec2.us-east-1.amazonaws.com", action, params)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", action, body)
	if out != nil {
		require.NoError(e.t, xml.Unmarshal(body, out), "%s: %s", action, body)
	}
}

// asg sends an Auto Scaling request and returns status and body.
func (e *asgEnv) asg(action string, params map[string]string) (int, []byte) {
	e.t.Helper()
	return e.call("autoscaling.us-east-1.amazonaws.com", action, params)
}

// mustASG sends an Auto Scaling request that must succeed.
func (e *asgEnv) mustASG(action string, params map[string]string) []byte {
	e.t.Helper()
	status, body := e.asg(action, params)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", action, body)
	return body
}

// asgErr sends an Auto Scaling request that must fail, and returns status and error code.
func (e *asgEnv) asgErr(action string, params map[string]string) (int, string, string) {
	e.t.Helper()
	status, body := e.asg(action, params)
	require.NotEqual(e.t, http.StatusOK, status, "%s unexpectedly succeeded: %s", action, body)
	var er struct {
		Code    string `xml:"Error>Code"`
		Message string `xml:"Error>Message"`
	}
	require.NoError(e.t, xml.Unmarshal(body, &er), "%s", body)
	return status, er.Code, er.Message
}

// createGroup creates a group from the environment's template over both subnets.
func (e *asgEnv) createGroup(name string, minSize, maxSize, desired int, extra map[string]string) {
	e.t.Helper()
	params := map[string]string{
		"AutoScalingGroupName":              name,
		"MinSize":                           fmt.Sprint(minSize),
		"MaxSize":                           fmt.Sprint(maxSize),
		"DesiredCapacity":                   fmt.Sprint(desired),
		"VPCZoneIdentifier":                 e.subnetA + "," + e.subnetB,
		"LaunchTemplate.LaunchTemplateName": e.ltName,
	}
	for k, v := range extra {
		params[k] = v
	}
	body := e.mustASG("CreateAutoScalingGroup", params)
	assert.NotContains(e.t, string(body), "CreateAutoScalingGroupResult",
		"an operation with no output shape answers without a result element")
	assert.Contains(e.t, string(body), "<ResponseMetadata><RequestId>")
}

type asgInstanceOut struct {
	InstanceID     string `xml:"InstanceId"`
	InstanceType   string `xml:"InstanceType"`
	AZ             string `xml:"AvailabilityZone"`
	LifecycleState string `xml:"LifecycleState"`
	HealthStatus   string `xml:"HealthStatus"`
	Protected      bool   `xml:"ProtectedFromScaleIn"`
	LTID           string `xml:"LaunchTemplate>LaunchTemplateId"`
	LTVersion      string `xml:"LaunchTemplate>Version"`
}

type asgGroupOut struct {
	Name      string           `xml:"AutoScalingGroupName"`
	ARN       string           `xml:"AutoScalingGroupARN"`
	Min       int              `xml:"MinSize"`
	Max       int              `xml:"MaxSize"`
	Desired   int              `xml:"DesiredCapacity"`
	AZs       []string         `xml:"AvailabilityZones>member"`
	Instances []asgInstanceOut `xml:"Instances>member"`
	Tags      []struct {
		Key       string `xml:"Key"`
		Value     string `xml:"Value"`
		Propagate bool   `xml:"PropagateAtLaunch"`
	} `xml:"Tags>member"`
	Suspended []struct {
		ProcessName string `xml:"ProcessName"`
	} `xml:"SuspendedProcesses>member"`
	LTName       string `xml:"LaunchTemplate>LaunchTemplateName"`
	LTVersion    string `xml:"LaunchTemplate>Version"`
	Distribution struct {
		OnDemandAllocationStrategy string `xml:"OnDemandAllocationStrategy"`
		OnDemandBase               int    `xml:"OnDemandBaseCapacity"`
		OnDemandPct                int    `xml:"OnDemandPercentageAboveBaseCapacity"`
		SpotAllocationStrategy     string `xml:"SpotAllocationStrategy"`
	} `xml:"MixedInstancesPolicy>InstancesDistribution"`
	VCpuMin string `xml:"MixedInstancesPolicy>LaunchTemplate>Overrides>member>InstanceRequirements>VCpuCount>Min"`
}

type asgDescribeGroupsOut struct {
	Groups    []asgGroupOut `xml:"DescribeAutoScalingGroupsResult>AutoScalingGroups>member"`
	NextToken string        `xml:"DescribeAutoScalingGroupsResult>NextToken"`
}

// group describes one group, which is also one observation of it.
func (e *asgEnv) group(name string) asgGroupOut {
	e.t.Helper()
	body := e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": name})
	var out asgDescribeGroupsOut
	require.NoError(e.t, xml.Unmarshal(body, &out))
	require.Len(e.t, out.Groups, 1, "%s", body)
	return out.Groups[0]
}

type asgActivityOut struct {
	ID            string `xml:"ActivityId"`
	Group         string `xml:"AutoScalingGroupName"`
	Description   string `xml:"Description"`
	Cause         string `xml:"Cause"`
	StatusCode    string `xml:"StatusCode"`
	StatusMessage string `xml:"StatusMessage"`
	Progress      int    `xml:"Progress"`
	Details       string `xml:"Details"`
	State         string `xml:"AutoScalingGroupState"`
}

func (e *asgEnv) activities(params map[string]string) []asgActivityOut {
	e.t.Helper()
	body := e.mustASG("DescribeScalingActivities", params)
	var out struct {
		Activities []asgActivityOut `xml:"DescribeScalingActivitiesResult>Activities>member"`
	}
	require.NoError(e.t, xml.Unmarshal(body, &out))
	return out.Activities
}

type ec2InstanceOut struct {
	ID        string `xml:"instanceId"`
	Type      string `xml:"instanceType"`
	State     string `xml:"instanceState>name"`
	Lifecycle string `xml:"instanceLifecycle"`
	Subnet    string `xml:"subnetId"`
	Tags      []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

func (i ec2InstanceOut) tag(key string) (string, bool) {
	for _, t := range i.Tags {
		if t.Key == key {
			return t.Value, true
		}
	}
	return "", false
}

// ec2GroupInstances returns every EC2 instance tagged as the group's, in any state.
func (e *asgEnv) ec2GroupInstances(group string) []ec2InstanceOut {
	e.t.Helper()
	var out struct {
		Instances []ec2InstanceOut `xml:"reservationSet>item>instancesSet>item"`
	}
	e.ec2XML("DescribeInstances", map[string]string{
		"Filter.1.Name":    "tag:aws:autoscaling:groupName",
		"Filter.1.Value.1": group,
	}, &out)
	return out.Instances
}

func ec2ByState(insts []ec2InstanceOut, state string) []ec2InstanceOut {
	var out []ec2InstanceOut
	for _, i := range insts {
		if i.State == state {
			out = append(out, i)
		}
	}
	return out
}

func asgInstanceIDs(g asgGroupOut) []string {
	ids := make([]string, 0, len(g.Instances))
	for _, i := range g.Instances {
		ids = append(ids, i.InstanceID)
	}
	sort.Strings(ids)
	return ids
}

func TestAutoScaling_CreateLaunchesTaggedInstancesAcrossZones(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 1, 6, 3, map[string]string{
		"Tags.member.1.Key":               "Team",
		"Tags.member.1.Value":             "compute",
		"Tags.member.1.PropagateAtLaunch": "true",
		"Tags.member.2.Key":               "Billing",
		"Tags.member.2.Value":             "group-only",
		"Tags.member.2.PropagateAtLaunch": "false",
	})

	g := e.group("pool")
	assert.Equal(t, 3, g.Desired)
	require.Len(t, g.Instances, 3)
	assert.ElementsMatch(t, []string{"us-east-1a", "us-east-1b"}, g.AZs)
	assert.Equal(t, e.ltName, g.LTName)
	assert.Equal(t, "$Default", g.LTVersion, "the version echoes as named, defaulting to $Default")
	assert.True(t, strings.HasPrefix(g.ARN, "arn:aws:autoscaling:us-east-1:"), g.ARN)
	assert.Contains(t, g.ARN, ":autoScalingGroupName/pool")
	perAZ := map[string]int{}
	for _, inst := range g.Instances {
		assert.Equal(t, "InService", inst.LifecycleState)
		assert.Equal(t, "Healthy", inst.HealthStatus)
		assert.Equal(t, "t3.micro", inst.InstanceType)
		assert.Equal(t, e.ltID, inst.LTID)
		assert.Equal(t, "1", inst.LTVersion, "an instance records the version number it launched from")
		perAZ[inst.AZ]++
	}
	assert.Equal(t, map[string]int{"us-east-1a": 2, "us-east-1b": 1}, perAZ, "zones are balanced")

	insts := e.ec2GroupInstances("pool")
	require.Len(t, insts, 3, "DescribeInstances sees the group's instances through the reserved tag")
	for _, inst := range insts {
		assert.Equal(t, "running", inst.State)
		v, _ := inst.tag("Team")
		assert.Equal(t, "compute", v, "a group tag wins over the template's on a shared key")
		_, billing := inst.tag("Billing")
		assert.False(t, billing, "a tag not propagated at launch stays on the group")
		v, _ = inst.tag("FromTemplate")
		assert.Equal(t, "yes", v, "the template's own instance tags are merged in")
		assert.Empty(t, inst.Lifecycle, "an On-Demand instance reports no instanceLifecycle")
	}

	acts := e.activities(map[string]string{"AutoScalingGroupName": "pool"})
	require.Len(t, acts, 3)
	for _, a := range acts {
		assert.Equal(t, "Successful", a.StatusCode)
		assert.Equal(t, 100, a.Progress)
		assert.True(t, strings.HasPrefix(a.Description, "Launching a new EC2 instance: i-"), a.Description)
		assert.Contains(t, a.Cause, "a user request created an AutoScalingGroup changing the desired capacity from 0 to 3.")
		assert.Contains(t, a.Details, `"Availability Zone":"us-east-1`)
	}
}

func TestAutoScaling_SetDesiredCapacityScalesOutAndIn(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 1, nil)

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "4", "HonorCooldown": "false"})
	assert.Len(t, e.group("pool").Instances, 4)
	assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "running"), 4)

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "1"})
	g := e.group("pool")
	assert.Equal(t, 1, g.Desired)
	assert.Len(t, g.Instances, 1)
	insts := e.ec2GroupInstances("pool")
	assert.Len(t, ec2ByState(insts, "running"), 1)
	assert.Len(t, ec2ByState(insts, "terminated"), 3, "scale-in terminates through EC2")

	tests := []struct {
		name, desired, wantMsg string
	}{
		{"above max", "6", "above max value 5"},
		{"below min", "-1", "below min value 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := e.asgErr("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": tc.desired})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationError", code)
			assert.Contains(t, msg, tc.wantMsg)
		})
	}
}

func TestAutoScaling_HonorCooldownRefusesInsideTheCooldown(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 1, map[string]string{"DefaultCooldown": "120"})

	_, code, _ := e.asgErr("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "2", "HonorCooldown": "true"})
	assert.Equal(t, "ScalingActivityInProgress", code)

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "2", "HonorCooldown": "false"})
	e.ts.AdvanceTime(121 * time.Second)
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "3", "HonorCooldown": "true"})
	assert.Len(t, e.group("pool").Instances, 3)
}

func TestAutoScaling_ProtectedInstancesSurviveScaleIn(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 4, nil)
	g := e.group("pool")
	keep := []string{g.Instances[0].InstanceID, g.Instances[3].InstanceID}

	body := e.mustASG("SetInstanceProtection", map[string]string{
		"AutoScalingGroupName": "pool",
		"InstanceIds.member.1": keep[0],
		"InstanceIds.member.2": keep[1],
		"ProtectedFromScaleIn": "true",
	})
	assert.Contains(t, string(body), "<SetInstanceProtectionResult></SetInstanceProtectionResult>",
		"SetInstanceProtection's output shape is the memberless SetInstanceProtectionAnswer under a resultWrapper")

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "0"})
	g = e.group("pool")
	sort.Strings(keep)
	assert.Equal(t, keep, asgInstanceIDs(g), "only the protected instances remain")
	assert.Equal(t, 0, g.Desired, "and the group stays above its desired capacity rather than touching them")

	_, code, _ := e.asgErr("SetInstanceProtection", map[string]string{
		"AutoScalingGroupName": "pool", "InstanceIds.member.1": "i-0000000000000dead", "ProtectedFromScaleIn": "true",
	})
	assert.Equal(t, "ValidationError", code)
}

func TestAutoScaling_NewInstancesProtectedFromScaleIn(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 2, map[string]string{"NewInstancesProtectedFromScaleIn": "true"})
	for _, inst := range e.group("pool").Instances {
		assert.True(t, inst.Protected)
	}
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "0"})
	assert.Len(t, e.group("pool").Instances, 2)
}

func TestAutoScaling_TerminateInstanceInAutoScalingGroup(t *testing.T) {
	t.Parallel()

	t.Run("decrementing shrinks the group", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 3, nil)
		victim := e.group("pool").Instances[1].InstanceID

		body := e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
			"InstanceId": victim, "ShouldDecrementDesiredCapacity": "true",
		})
		var out struct {
			Activity asgActivityOut `xml:"TerminateInstanceInAutoScalingGroupResult>Activity"`
		}
		require.NoError(t, xml.Unmarshal(body, &out))
		assert.Equal(t, "InProgress", out.Activity.StatusCode, "the response is the activity as it stood when requested")
		assert.Equal(t, "Terminating EC2 instance: "+victim, out.Activity.Description)
		assert.Contains(t, out.Activity.Cause, "shrinking the capacity from 3 to 2.")

		g := e.group("pool")
		assert.Equal(t, 2, g.Desired)
		assert.NotContains(t, asgInstanceIDs(g), victim)
		assert.Len(t, g.Instances, 2)

		acts := e.activities(map[string]string{"AutoScalingGroupName": "pool", "ActivityIds.member.1": out.Activity.ID})
		require.Len(t, acts, 1)
		assert.Equal(t, "Successful", acts[0].StatusCode, "while the recorded activity is settled")
	})

	t.Run("without decrementing the instance is replaced", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 3, nil)
		before := asgInstanceIDs(e.group("pool"))
		victim := before[0]

		e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
			"InstanceId": victim, "ShouldDecrementDesiredCapacity": "false",
		})
		g := e.group("pool")
		assert.Equal(t, 3, g.Desired)
		require.Len(t, g.Instances, 3)
		assert.NotContains(t, asgInstanceIDs(g), victim)
		assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "terminated"), 1)
	})

	t.Run("decrementing below the minimum is refused", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 2, 5, 2, nil)
		_, code, msg := e.asgErr("TerminateInstanceInAutoScalingGroup", map[string]string{
			"InstanceId": e.group("pool").Instances[0].InstanceID, "ShouldDecrementDesiredCapacity": "true",
		})
		assert.Equal(t, "ValidationError", code)
		assert.Contains(t, msg, "min size")
	})

	t.Run("an instance in no group is refused", func(t *testing.T) {
		e := newASGEnv(t)
		_, code, _ := e.asgErr("TerminateInstanceInAutoScalingGroup", map[string]string{
			"InstanceId": "i-0123456789abcdef0", "ShouldDecrementDesiredCapacity": "true",
		})
		assert.Equal(t, "ValidationError", code)
	})
}

func TestAutoScaling_SelfTerminatedInstanceIsReplacedOnObservation(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 2, nil)
	victim := e.group("pool").Instances[0].InstanceID

	// The worker terminates itself through EC2, as aws-elastic-globus-compute's do.
	e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": victim}, nil)

	g := e.group("pool")
	require.Len(t, g.Instances, 2, "the next observation noticed and replaced it")
	assert.NotContains(t, asgInstanceIDs(g), victim)

	acts := e.activities(map[string]string{"AutoScalingGroupName": "pool"})
	require.GreaterOrEqual(t, len(acts), 4)
	assert.Equal(t, "Launching a new EC2 instance: "+g.Instances[1].InstanceID, acts[0].Description,
		"the replacement is the most recent activity")
	assert.Equal(t, "Terminating EC2 instance: "+victim, acts[1].Description)
	assert.Contains(t, acts[1].Cause, "EC2 health check indicating it has been terminated or stopped")
}

func TestAutoScaling_SelfTerminationIsNotReplacedWhenDesiredWasLowered(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 3, nil)
	before := asgInstanceIDs(e.group("pool"))

	e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": before[0]}, nil)
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "2"})

	g := e.group("pool")
	assert.Equal(t, before[1:], asgInstanceIDs(g), "nothing launched and nothing else terminated")
	assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "running"), 2)
}

func TestAutoScaling_SuspendedProcessesAreHonored(t *testing.T) {
	t.Parallel()

	t.Run("Launch", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 2, nil)
		e.mustASG("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "Launch"})
		victim := e.group("pool").Instances[0].InstanceID
		e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": victim}, nil)

		g := e.group("pool")
		assert.Len(t, g.Instances, 1, "Launch suspended: the terminated instance is dropped and not replaced")
		require.Len(t, g.Suspended, 1)
		assert.Equal(t, "Launch", g.Suspended[0].ProcessName)

		e.mustASG("ResumeProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "Launch"})
		g = e.group("pool")
		assert.Len(t, g.Instances, 2, "resumed: the next observation launches the shortfall")
		assert.Empty(t, g.Suspended)
	})

	t.Run("Terminate", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 3, nil)
		e.mustASG("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "Terminate"})
		e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "1"})
		assert.Len(t, e.group("pool").Instances, 3)
	})

	t.Run("ReplaceUnhealthy", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 2, nil)
		e.mustASG("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "ReplaceUnhealthy"})
		victim := e.group("pool").Instances[0].InstanceID
		e.ec2XML("TerminateInstances", map[string]string{"InstanceId.1": victim}, nil)
		g := e.group("pool")
		require.Len(t, g.Instances, 2)
		for _, inst := range g.Instances {
			want := "Healthy"
			if inst.InstanceID == victim {
				want = "Unhealthy"
			}
			assert.Equal(t, want, inst.HealthStatus, inst.InstanceID)
		}
	})

	t.Run("all, and AZRebalance", func(t *testing.T) {
		e := newASGEnv(t)
		e.createGroup("pool", 0, 5, 0, nil)
		e.mustASG("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "AZRebalance"})
		assert.Len(t, e.group("pool").Suspended, 1)
		e.mustASG("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool"})
		assert.Len(t, e.group("pool").Suspended, 9, "omitting ScalingProcesses suspends every process")
		e.mustASG("ResumeProcesses", map[string]string{"AutoScalingGroupName": "pool"})
		assert.Empty(t, e.group("pool").Suspended)

		_, code, _ := e.asgErr("SuspendProcesses", map[string]string{"AutoScalingGroupName": "pool", "ScalingProcesses.member.1": "Nope"})
		assert.Equal(t, "ValidationError", code)
	})
}

func TestAutoScaling_MixedInstancesPolicySelectsTypesAndSplitsSpot(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	const (
		mip = "MixedInstancesPolicy."
		ir  = mip + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
		dst = mip + "InstancesDistribution."
	)
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "mixed",
		"MinSize":              "0",
		"MaxSize":              "10",
		"DesiredCapacity":      "5",
		"VPCZoneIdentifier":    e.subnetA,
		"CapacityRebalance":    "true",
		mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateId": e.ltID,
		mip + "LaunchTemplate.LaunchTemplateSpecification.Version":          "$Latest",
		ir + "VCpuCount.Min":                        "2",
		ir + "VCpuCount.Max":                        "2",
		ir + "MemoryMiB.Min":                        "4096",
		ir + "MemoryMiB.Max":                        "8192",
		ir + "CpuManufacturers.member.1":            "intel",
		ir + "BurstablePerformance":                 "excluded",
		dst + "OnDemandBaseCapacity":                "1",
		dst + "OnDemandPercentageAboveBaseCapacity": "50",
		dst + "SpotAllocationStrategy":              "price-capacity-optimized",
	})

	g := e.group("mixed")
	require.Len(t, g.Instances, 5)
	for _, inst := range g.Instances {
		assert.Equal(t, "c5.large", inst.InstanceType, "the smallest matching type, deterministically")
	}
	assert.Equal(t, "lowest-price", g.Distribution.OnDemandAllocationStrategy,
		"the documented default for a policy using InstanceRequirements")
	assert.Equal(t, "price-capacity-optimized", g.Distribution.SpotAllocationStrategy)
	assert.Equal(t, 1, g.Distribution.OnDemandBase)
	assert.Equal(t, 50, g.Distribution.OnDemandPct)
	assert.Equal(t, "2", g.VCpuMin, "the requirements are echoed")

	spot := 0
	for _, inst := range e.ec2GroupInstances("mixed") {
		assert.Equal(t, "c5.large", inst.Type)
		if inst.Lifecycle == "spot" {
			spot++
		}
	}
	// base 1 + ceil(50% of 4) = 3 On-Demand, so 2 Spot.
	assert.Equal(t, 2, spot, "DescribeInstances reports the Spot share as instanceLifecycle spot")
	var sirs struct {
		IDs []string `xml:"spotInstanceRequestSet>item>spotInstanceRequestId"`
	}
	e.ec2XML("DescribeSpotInstanceRequests", nil, &sirs)
	assert.Empty(t, sirs.IDs, "a group's Spot capacity, like a fleet's, writes no sir- request")

	// Scaling in keeps the split trending toward the ratio: at 3, 1 + ceil(50% of 2) = 2
	// On-Demand, so one of each purchase option goes.
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "mixed", "DesiredCapacity": "3"})
	spot = 0
	for _, inst := range ec2ByState(e.ec2GroupInstances("mixed"), "running") {
		if inst.Lifecycle == "spot" {
			spot++
		}
	}
	assert.Equal(t, 1, spot)
}

func TestAutoScaling_OverridesInstanceTypeIsPrioritized(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	const mip = "MixedInstancesPolicy.LaunchTemplate."
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "typed",
		"MinSize":              "0",
		"MaxSize":              "4",
		"DesiredCapacity":      "2",
		"VPCZoneIdentifier":    e.subnetB,
		mip + "LaunchTemplateSpecification.LaunchTemplateName": e.ltName,
		mip + "Overrides.member.1.InstanceType":                "m5.large",
		mip + "Overrides.member.2.InstanceType":                "c5.large",
	})
	g := e.group("typed")
	require.Len(t, g.Instances, 2)
	for _, inst := range g.Instances {
		assert.Equal(t, "m5.large", inst.InstanceType)
		assert.Equal(t, "us-east-1b", inst.AZ)
	}
	assert.Equal(t, "prioritized", g.Distribution.OnDemandAllocationStrategy)
	assert.Equal(t, 100, g.Distribution.OnDemandPct, "the documented default")
}

func TestAutoScaling_InstanceRequirementsMatching(t *testing.T) {
	t.Parallel()
	f := func(v float64) *float64 { return &v }
	tests := []struct {
		name string
		req  emulator.AutoScalingInstanceRequirements
		want []string
	}{
		{"intel, not burstable", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096), Max: f(8192)},
			CPUManufacturers: []string{"intel"},
		}, []string{"c5.large", "m5.large"}},
		{"burstable included", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096), Max: f(4096)},
			BurstablePerformance: "included",
		}, []string{"c5.large", "c5a.large", "t3.medium", "t3a.medium"}},
		{"burstable required", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(512), Max: f(1024)},
			BurstablePerformance: "required", CPUManufacturers: []string{"amd"},
		}, []string{"t3a.nano", "t3a.micro"}},
		{"allowed with wildcard", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(8192), Max: f(16384)},
			AllowedInstanceTypes: []string{"m5*"},
		}, []string{"m5.large", "m5a.large", "m5.xlarge", "m5a.xlarge"}},
		{"excluded", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(8192), Max: f(8192)},
			ExcludedInstanceTypes: []string{"m5a.*"},
		}, []string{"m5.large"}},
		{"no accelerators", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(4), Max: f(4)}, MemoryMiB: emulator.AutoScalingRange{Min: f(16384), Max: f(16384)},
			AcceleratorCount: &emulator.AutoScalingRange{Max: f(0)},
		}, []string{"m5.xlarge", "m5a.xlarge"}},
		{"gpu required", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(4), Max: f(4)}, MemoryMiB: emulator.AutoScalingRange{Min: f(16384), Max: f(16384)},
			AcceleratorTypes: []string{"gpu"},
		}, []string{"g4dn.xlarge", "g5.xlarge", "g6.xlarge"}},
		{"nothing matches", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(1000)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1)},
		}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, emulator.AutoScalingMatchInstanceTypesForTest(&tc.req))
		})
	}
}

// TestAutoScaling_TargetOnDemandReproducesThePublishedTable checks the On-Demand count
// against every cell of the mixed-instances setup overview's scaling-behavior table.
func TestAutoScaling_TargetOnDemandReproducesThePublishedTable(t *testing.T) {
	t.Parallel()
	sizes := []int{10, 20, 30, 40}
	tests := []struct {
		name      string
		base, pct int
		wantOD    []int
	}{
		{"base 10, 50/50", 10, 50, []int{10, 15, 20, 25}},
		{"base 0, 0/100", 0, 0, []int{0, 0, 0, 0}},
		{"base 0, 60/40", 0, 60, []int{6, 12, 18, 24}},
		{"base 0, 100/0", 0, 100, []int{10, 20, 30, 40}},
		{"base 12, 0/100", 12, 0, []int{10, 12, 12, 12}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for i, n := range sizes {
				assert.Equal(t, tc.wantOD[i], emulator.AutoScalingTargetOnDemandForTest(n, tc.base, tc.pct), "size %d", n)
			}
		})
	}
}

func TestAutoScaling_LaunchFailureSeed(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	seed := func(method, body, query string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, e.ts.URL+"/v1/autoscaling/launch-failure"+query, bytes.NewBufferString(body))
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	const msg = "Could not launch Spot Instances. InsufficientInstanceCapacity - There is no Spot capacity available that matches your request. Launching EC2 instance failed."
	seed(http.MethodPost, `{"autoScalingGroupName":"pool","statusMessage":"`+msg+`"}`, "")

	e.createGroup("pool", 0, 5, 2, nil)
	e.createGroup("other", 0, 5, 1, nil)
	g := e.group("pool")
	assert.Equal(t, 2, g.Desired)
	assert.Empty(t, g.Instances, "the capacity is left unmet")
	assert.Len(t, e.group("other").Instances, 1, "a group-keyed seed touches only its group")

	acts := e.activities(map[string]string{"AutoScalingGroupName": "pool"})
	require.NotEmpty(t, acts)
	assert.Equal(t, "Failed", acts[0].StatusCode)
	assert.Equal(t, msg, acts[0].StatusMessage)
	assert.Equal(t, "Launching a new EC2 instance.  Status Reason: "+msg, acts[0].Description)

	failedOnly := e.activities(map[string]string{
		"AutoScalingGroupName": "pool", "Filters.member.1.Name": "Status", "Filters.member.1.Values.member.1": "Failed",
	})
	for _, a := range failedOnly {
		assert.Equal(t, "Failed", a.StatusCode)
	}

	seed(http.MethodDelete, "", "?autoScalingGroupName=pool")
	assert.Len(t, e.group("pool").Instances, 2, "cleared, the next observation launches")

	seed(http.MethodPost, `{}`, "")
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "other", "DesiredCapacity": "3"})
	assert.Len(t, e.group("other").Instances, 1, "the wildcard applies to every group")
	acts = e.activities(map[string]string{"AutoScalingGroupName": "other"})
	assert.Contains(t, acts[0].StatusMessage, "sufficient capacity", "the default message")
	seed(http.MethodDelete, "", "")
}

func TestAutoScaling_Errors(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 1, nil)

	base := func(over map[string]string) map[string]string {
		p := map[string]string{
			"AutoScalingGroupName":              "new",
			"MinSize":                           "1",
			"MaxSize":                           "3",
			"VPCZoneIdentifier":                 e.subnetA,
			"LaunchTemplate.LaunchTemplateName": e.ltName,
		}
		for k, v := range over {
			if v == "" {
				delete(p, k)
			} else {
				p[k] = v
			}
		}
		return p
	}
	tests := []struct {
		name     string
		action   string
		params   map[string]string
		wantCode string
		wantMsg  string
	}{
		{"exists", "CreateAutoScalingGroup", base(map[string]string{"AutoScalingGroupName": "pool"}), "AlreadyExists", "already exists"},
		{"desired below min", "CreateAutoScalingGroup", base(map[string]string{"DesiredCapacity": "0"}), "ValidationError", "Desired capacity:0"},
		{"desired above max", "CreateAutoScalingGroup", base(map[string]string{"DesiredCapacity": "4"}), "ValidationError", "Desired capacity:4"},
		{"max below min", "CreateAutoScalingGroup", base(map[string]string{"MinSize": "4"}), "ValidationError", "Max bound"},
		{"no template", "CreateAutoScalingGroup", base(map[string]string{"LaunchTemplate.LaunchTemplateName": ""}), "ValidationError", "LaunchTemplate"},
		{"unknown template", "CreateAutoScalingGroup", base(map[string]string{"LaunchTemplate.LaunchTemplateName": "nope"}), "ValidationError", "does not exist"},
		{"unknown subnet", "CreateAutoScalingGroup", base(map[string]string{"VPCZoneIdentifier": "subnet-0000000000000dead"}), "ValidationError", "subnet"},
		{"no placement", "CreateAutoScalingGroup", base(map[string]string{"VPCZoneIdentifier": ""}), "ValidationError", "Availability Zone"},
		{"reserved tag", "CreateAutoScalingGroup", base(map[string]string{"Tags.member.1.Key": "aws:x", "Tags.member.1.Value": "y"}), "ValidationError", "aws:"},
		{"colon in name", "CreateAutoScalingGroup", base(map[string]string{"AutoScalingGroupName": "a:b"}), "ValidationError", "colon"},
		{"launch configuration", "CreateAutoScalingGroup", base(map[string]string{"LaunchConfigurationName": "lc"}), "ValidationError", "Launch configurations"},
		{"unknown group", "SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "ghost", "DesiredCapacity": "1"}, "ValidationError", "not found"},
		{"update unknown group", "UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "ghost"}, "ValidationError", "not found"},
		{"update desired above max", "UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "9"}, "ValidationError", "Desired capacity:9"},
		{"in use", "DeleteAutoScalingGroup", map[string]string{"AutoScalingGroupName": "pool"}, "ResourceInUse", "instances"},
		{"bad max records", "DescribeAutoScalingGroups", map[string]string{"MaxRecords": "101"}, "ValidationError", "maxRecords"},
		{"bad token", "DescribeAutoScalingGroups", map[string]string{"NextToken": "!!!"}, "InvalidNextToken", ""},
		{"bad filter", "DescribeAutoScalingGroups", map[string]string{"Filters.member.1.Name": "instance-type", "Filters.member.1.Values.member.1": "x"}, "ValidationError", "Filter"},
		{"unknown action", "AttachInstances", map[string]string{}, "InvalidAction", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, msg := e.asgErr(tc.action, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantCode, code)
			assert.Contains(t, msg, tc.wantMsg)
		})
	}
	var out asgDescribeGroupsOut
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "new"}), &out))
	assert.Empty(t, out.Groups, "no refused create left a group behind")
}

func TestAutoScaling_DeleteAutoScalingGroup(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 2, nil)
	e.createGroup("empty", 0, 5, 0, nil)

	e.mustASG("DeleteAutoScalingGroup", map[string]string{"AutoScalingGroupName": "empty"})
	e.mustASG("DeleteAutoScalingGroup", map[string]string{"AutoScalingGroupName": "pool", "ForceDelete": "true"})
	assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "terminated"), 2, "ForceDelete terminates the instances")

	var out asgDescribeGroupsOut
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingGroups", nil), &out))
	assert.Empty(t, out.Groups)

	assert.Empty(t, e.activities(map[string]string{"AutoScalingGroupName": "pool"}),
		"a deleted group's activities are omitted by default")
	acts := e.activities(map[string]string{"AutoScalingGroupName": "pool", "IncludeDeletedGroups": "true"})
	require.NotEmpty(t, acts)
	for _, a := range acts {
		assert.Equal(t, "Deleted", a.State)
	}
}

func TestAutoScaling_UpdateAutoScalingGroup(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 1, 5, 1, nil)

	// "If you specify a new value for MinSize without specifying a value for DesiredCapacity,
	// and the new MinSize is larger than the current size of the group, this sets the group's
	// DesiredCapacity to the new MinSize value."
	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "pool", "MinSize": "3"})
	g := e.group("pool")
	assert.Equal(t, 3, g.Desired)
	assert.Len(t, g.Instances, 3)

	// And MaxSize below the size lowers it.
	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "pool", "MinSize": "0", "MaxSize": "2"})
	g = e.group("pool")
	assert.Equal(t, 2, g.Desired)
	assert.Equal(t, 2, g.Max)
	assert.Len(t, g.Instances, 2)

	// A new template version, pinned by number: new launches use it, and the default
	// termination policy removes the instances on the older version first.
	e.ec2XML("CreateLaunchTemplateVersion", map[string]string{
		"LaunchTemplateId":                e.ltID,
		"LaunchTemplateData.ImageId":      ec2TestImage,
		"LaunchTemplateData.InstanceType": "t3.small",
	}, nil)
	e.mustASG("UpdateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "pool", "MaxSize": "4", "DesiredCapacity": "4",
		"LaunchTemplate.LaunchTemplateId": e.ltID, "LaunchTemplate.Version": "2",
	})
	g = e.group("pool")
	require.Len(t, g.Instances, 4)
	versions := map[string]int{}
	for _, inst := range g.Instances {
		versions[inst.LTVersion]++
	}
	assert.Equal(t, map[string]int{"1": 2, "2": 2}, versions)

	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "2"})
	for _, inst := range e.group("pool").Instances {
		assert.Equal(t, "2", inst.LTVersion, "the oldest template version went first")
		assert.Equal(t, "t3.small", inst.InstanceType)
	}
}

func TestAutoScaling_TagsAndFilters(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("alpha", 0, 3, 0, map[string]string{"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"})
	e.createGroup("beta", 0, 3, 0, map[string]string{"Tags.member.1.Key": "env", "Tags.member.1.Value": "dev"})
	e.createGroup("gamma", 0, 3, 0, nil)

	names := func(params map[string]string) []string {
		t.Helper()
		var out asgDescribeGroupsOut
		require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingGroups", params), &out))
		var ns []string
		for _, g := range out.Groups {
			ns = append(ns, g.Name)
		}
		return ns
	}
	assert.Equal(t, []string{"alpha"}, names(map[string]string{"Filters.member.1.Name": "tag:env", "Filters.member.1.Values.member.1": "prod"}))
	assert.Equal(t, []string{"alpha", "beta"}, names(map[string]string{"Filters.member.1.Name": "tag-key", "Filters.member.1.Values.member.1": "env"}))
	assert.Equal(t, []string{"beta"}, names(map[string]string{"Filters.member.1.Name": "tag-value", "Filters.member.1.Values.member.1": "dev"}))

	e.mustASG("CreateOrUpdateTags", map[string]string{
		"Tags.member.1.ResourceId": "gamma", "Tags.member.1.ResourceType": "auto-scaling-group",
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod", "Tags.member.1.PropagateAtLaunch": "true",
		"Tags.member.2.ResourceId": "alpha", "Tags.member.2.ResourceType": "auto-scaling-group",
		"Tags.member.2.Key": "env", "Tags.member.2.Value": "staging",
	})
	assert.Equal(t, []string{"gamma"}, names(map[string]string{"Filters.member.1.Name": "tag:env", "Filters.member.1.Values.member.1": "prod"}),
		"CreateOrUpdateTags overwrote alpha's value and tagged gamma")

	var tags struct {
		Tags []struct {
			ResourceID string `xml:"ResourceId"`
			Type       string `xml:"ResourceType"`
			Key        string `xml:"Key"`
			Value      string `xml:"Value"`
			Propagate  bool   `xml:"PropagateAtLaunch"`
		} `xml:"DescribeTagsResult>Tags>member"`
	}
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeTags", map[string]string{
		"Filters.member.1.Name": "propagate-at-launch", "Filters.member.1.Values.member.1": "true",
	}), &tags))
	require.Len(t, tags.Tags, 1)
	assert.Equal(t, "gamma", tags.Tags[0].ResourceID)
	assert.Equal(t, "auto-scaling-group", tags.Tags[0].Type)

	// A propagated tag reaches instances launched after it, not before.
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "gamma", "DesiredCapacity": "1"})
	insts := e.ec2GroupInstances("gamma")
	require.Len(t, insts, 1)
	v, _ := insts[0].tag("env")
	assert.Equal(t, "prod", v)

	e.mustASG("DeleteTags", map[string]string{"Tags.member.1.ResourceId": "beta", "Tags.member.1.Key": "env"})
	assert.Empty(t, e.group("beta").Tags)

	_, code, _ := e.asgErr("CreateOrUpdateTags", map[string]string{
		"Tags.member.1.ResourceId": "ghost", "Tags.member.1.Key": "k", "Tags.member.1.Value": "v",
	})
	assert.Equal(t, "ValidationError", code)

	limit := map[string]string{}
	for i := 1; i <= 51; i++ {
		limit[fmt.Sprintf("Tags.member.%d.ResourceId", i)] = "beta"
		limit[fmt.Sprintf("Tags.member.%d.Key", i)] = fmt.Sprintf("k%02d", i)
	}
	_, code, _ = e.asgErr("CreateOrUpdateTags", limit)
	assert.Equal(t, "LimitExceeded", code)
}

func TestAutoScaling_Pagination(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	for _, n := range []string{"a", "b", "c"} {
		e.createGroup(n, 0, 3, 1, map[string]string{"Tags.member.1.Key": "k", "Tags.member.1.Value": n})
	}

	var seen []string
	token := ""
	for page := 0; ; page++ {
		require.Less(t, page, 5)
		params := map[string]string{"MaxRecords": "2"}
		if token != "" {
			params["NextToken"] = token
		}
		var out asgDescribeGroupsOut
		require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingGroups", params), &out))
		for _, g := range out.Groups {
			seen = append(seen, g.Name)
		}
		if out.NextToken == "" {
			break
		}
		assert.Len(t, out.Groups, 2)
		token = out.NextToken
	}
	assert.Equal(t, []string{"a", "b", "c"}, seen)

	var inst struct {
		Instances []struct {
			ID     string `xml:"InstanceId"`
			Group  string `xml:"AutoScalingGroupName"`
			Health string `xml:"HealthStatus"`
		} `xml:"DescribeAutoScalingInstancesResult>AutoScalingInstances>member"`
		NextToken string `xml:"DescribeAutoScalingInstancesResult>NextToken"`
	}
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeAutoScalingInstances", map[string]string{"MaxRecords": "2"}), &inst))
	assert.Len(t, inst.Instances, 2)
	assert.NotEmpty(t, inst.NextToken)
	assert.Equal(t, "HEALTHY", inst.Instances[0].Health, "the published sample's spelling for this operation")

	var acts struct {
		Activities []asgActivityOut `xml:"DescribeScalingActivitiesResult>Activities>member"`
		NextToken  string           `xml:"DescribeScalingActivitiesResult>NextToken"`
	}
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeScalingActivities", map[string]string{"MaxRecords": "1"}), &acts))
	require.Len(t, acts.Activities, 1)
	assert.Equal(t, "c", acts.Activities[0].Group, "most recent first")
	token = acts.NextToken
	acts.Activities, acts.NextToken = nil, ""
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeScalingActivities", map[string]string{"MaxRecords": "1", "NextToken": token}), &acts))
	require.Len(t, acts.Activities, 1)
	assert.Equal(t, "b", acts.Activities[0].Group)

	var tags struct {
		NextToken string `xml:"DescribeTagsResult>NextToken"`
	}
	require.NoError(t, xml.Unmarshal(e.mustASG("DescribeTags", map[string]string{"MaxRecords": "2"}), &tags))
	assert.NotEmpty(t, tags.NextToken)
}

func TestAutoScaling_ZoneOnlyGroupLaunchesIntoTheNamedZones(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName":            "zones",
		"MinSize":                         "0",
		"MaxSize":                         "4",
		"DesiredCapacity":                 "2",
		"AvailabilityZones.member.1":      "us-east-1a",
		"AvailabilityZones.member.2":      "us-east-1c",
		"LaunchTemplate.LaunchTemplateId": e.ltID,
	})
	g := e.group("zones")
	require.Len(t, g.Instances, 2)
	var zones []string
	for _, inst := range g.Instances {
		zones = append(zones, inst.AZ)
	}
	sort.Strings(zones)
	assert.Equal(t, []string{"us-east-1a", "us-east-1c"}, zones)

	// "Used for launching into the default VPC subnet in each Availability Zone when not using
	// the VPCZoneIdentifier property": each instance is in its zone's default subnet.
	subnets := map[string]bool{}
	for _, inst := range e.ec2GroupInstances("zones") {
		require.NotEmpty(t, inst.Subnet)
		subnets[inst.Subnet] = true
	}
	assert.Len(t, subnets, 2, "two zones, two default subnets")
}
