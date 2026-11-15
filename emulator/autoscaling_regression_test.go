package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Regression tests for the PR #1485 review of the Auto Scaling plugin (#1460).

// TestAutoScaling_TerminateByInstanceIDIsAboutTheOwningGroup pins that a request naming one
// group's instance cannot be authorized, or carried out, as another group's.
func TestAutoScaling_TerminateByInstanceIDIsAboutTheOwningGroup(t *testing.T) {
	t.Parallel()

	t.Run("authorization is decided against the owner", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t, asgStatement("autoscaling:TerminateInstanceInAutoScalingGroup", "*",
			map[string]map[string]emulator.StringOrSlice{"StringEquals": {"autoscaling:ResourceTag/team": {"dev"}}}))
		prod := emulator.AutoScalingGroup{Name: "prod-pool", ARN: asgAuthzProdARN,
			Tags:      []emulator.AutoScalingTag{{Key: "team", Value: "prod"}},
			Instances: []emulator.AutoScalingInstance{{InstanceID: "i-0eeeeffff00001111"}}}
		raw, err := json.Marshal(prod)
		require.NoError(t, err)
		require.NoError(t, f.state.Put(context.Background(), "autoscaling", "group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/prod-pool", raw)) //nolint:contextcheck
		assert.True(t, asgDenied(t, f.call(t, "TerminateInstanceInAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "dev-pool", "InstanceId": "i-0eeeeffff00001111", "ShouldDecrementDesiredCapacity": "true",
		})), "naming the dev group must not authorize terminating the prod group's instance")
	})

	t.Run("the handler refuses a contradicting group name", func(t *testing.T) {
		t.Parallel()
		e := newASGEnv(t)
		e.createGroup("dev", 0, 3, 1, nil)
		e.createGroup("prod", 0, 3, 1, nil)
		victim := e.group("prod").Instances[0].InstanceID
		_, code, _ := e.asgErr("TerminateInstanceInAutoScalingGroup", map[string]string{
			"AutoScalingGroupName": "dev", "InstanceId": victim, "ShouldDecrementDesiredCapacity": "true",
		})
		assert.Equal(t, "ValidationError", code)
		assert.Equal(t, []string{victim}, asgInstanceIDs(e.group("prod")), "nothing was terminated")
		assert.Len(t, ec2ByState(e.ec2GroupInstances("prod"), "running"), 1)
	})
}

// TestAutoScaling_TaggingAuthorizesEveryNamedGroup pins that a tagging request naming several
// groups is decided once per group, each with its own request tags.
func TestAutoScaling_TaggingAuthorizesEveryNamedGroup(t *testing.T) {
	t.Parallel()
	twoGroups := func(devKey, prodKey string) map[string]string {
		return map[string]string{
			"Tags.member.1.ResourceId": "dev-pool", "Tags.member.1.Key": devKey, "Tags.member.1.Value": "v",
			"Tags.member.2.ResourceId": "prod-pool", "Tags.member.2.Key": prodKey, "Tags.member.2.Value": "v",
		}
	}

	t.Run("one denied group denies the request", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t, asgStatement("autoscaling:CreateOrUpdateTags", "*",
			map[string]map[string]emulator.StringOrSlice{"StringEquals": {"autoscaling:ResourceTag/team": {"dev"}}}))
		require.NoError(t, f.call(t, "CreateOrUpdateTags", map[string]string{
			"Tags.member.1.ResourceId": "dev-pool", "Tags.member.1.Key": "k", "Tags.member.1.Value": "v",
		}))
		assert.True(t, asgDenied(t, f.call(t, "CreateOrUpdateTags", twoGroups("k", "k"))),
			"the second group is prod and must be decided too")
		assert.True(t, asgDenied(t, f.call(t, "DeleteTags", twoGroups("k", "k"))))
	})

	t.Run("request tags are scoped to the group they are applied to", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t,
			emulator.PolicyStatement{Effect: "Allow", Action: emulator.StringOrSlice{"autoscaling:CreateOrUpdateTags"},
				Resource:  emulator.StringOrSlice{asgAuthzDevARN},
				Condition: map[string]map[string]emulator.StringOrSlice{"ForAllValues:StringEquals": {"aws:TagKeys": {"team"}}}},
			emulator.PolicyStatement{Effect: "Allow", Action: emulator.StringOrSlice{"autoscaling:CreateOrUpdateTags"},
				Resource:  emulator.StringOrSlice{asgAuthzProdARN},
				Condition: map[string]map[string]emulator.StringOrSlice{"ForAllValues:StringEquals": {"aws:TagKeys": {"owner"}}}},
		)
		require.NoError(t, f.call(t, "CreateOrUpdateTags", twoGroups("team", "owner")),
			"each group sees only its own keys, so each statement is satisfied")
		assert.True(t, asgDenied(t, f.call(t, "CreateOrUpdateTags", twoGroups("owner", "owner"))))
	})
}

// TestAutoScaling_DuplicateInstanceIDsAreRefused is the panic the review found: a repeated ID
// in InstanceIds indexed the group's instance list at -1.
func TestAutoScaling_DuplicateInstanceIDsAreRefused(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 2, nil)
	id := e.group("pool").Instances[0].InstanceID
	status, code, _ := e.asgErr("TerminateInstanceInAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "pool", "InstanceIds.member.1": id, "InstanceIds.member.2": id,
		"ShouldDecrementDesiredCapacity": "true",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationError", code)
	assert.Len(t, e.group("pool").Instances, 2)
}

// TestAutoScaling_BurstableIsTheTSeriesOnly pins that Trainium, whose family name also begins
// with "t", is not burstable: trn1.2xlarge has exactly t3.2xlarge's vCPU and memory.
func TestAutoScaling_BurstableIsTheTSeriesOnly(t *testing.T) {
	t.Parallel()
	f := func(v float64) *float64 { return &v }
	size := func(burst string) *emulator.AutoScalingInstanceRequirements {
		return &emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(8), Max: f(8)}, MemoryMiB: emulator.AutoScalingRange{Min: f(32768), Max: f(32768)},
			BurstablePerformance: burst,
		}
	}
	assert.Equal(t, []string{"t3.2xlarge", "t3a.2xlarge"}, emulator.AutoScalingMatchInstanceTypesForTest(size("required"), "x86_64"))
	assert.Contains(t, emulator.AutoScalingMatchInstanceTypesForTest(size("excluded"), "x86_64"), "trn1.2xlarge",
		"excluding burstable types must not exclude Trainium")
}

// TestAutoScaling_PartialMixedInstancesPolicyUpdateMerges pins UpdateAutoScalingGroup's "Any
// properties that you don't specify are not changed" inside the policy.
func TestAutoScaling_PartialMixedInstancesPolicyUpdateMerges(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	const (
		mip = "MixedInstancesPolicy."
		ir  = mip + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
		dst = mip + "InstancesDistribution."
	)
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "mixed", "MinSize": "0", "MaxSize": "4", "DesiredCapacity": "0",
		"VPCZoneIdentifier": e.subnetA,
		mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateId": e.ltID,
		ir + "VCpuCount.Min": "2", ir + "MemoryMiB.Min": "4096",
		dst + "OnDemandPercentageAboveBaseCapacity": "50",
		dst + "SpotAllocationStrategy":              "price-capacity-optimized",
		dst + "SpotMaxPrice":                        "0.5",
	})

	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "mixed", dst + "OnDemandBaseCapacity": "2"})
	g := e.group("mixed")
	assert.Equal(t, 2, g.Distribution.OnDemandBase, "the member sent changed")
	assert.Equal(t, 50, g.Distribution.OnDemandPct, "the members not sent did not")
	assert.Equal(t, "price-capacity-optimized", g.Distribution.SpotAllocationStrategy)
	assert.Equal(t, "lowest-price", g.Distribution.OnDemandAllocationStrategy)
	assert.Equal(t, "2", g.VCpuMin, "the overrides survive an update that sent none")

	body := e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "mixed"})
	assert.Contains(t, string(body), "<SpotMaxPrice>0.5</SpotMaxPrice>")
	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "mixed", dst + "SpotMaxPrice": ""})
	body = e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "mixed"})
	assert.NotContains(t, string(body), "SpotMaxPrice", "an empty SpotMaxPrice clears it, as documented")

	e.mustASG("UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "mixed",
		mip + "LaunchTemplate.LaunchTemplateSpecification.Version": "$Latest"})
	body = e.mustASG("DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "mixed"})
	assert.Contains(t, string(body), "<LaunchTemplateId>"+e.ltID+"</LaunchTemplateId>", "a Version alone keeps the template")
	assert.Contains(t, string(body), "<Version>$Latest</Version>")
	assert.Equal(t, "2", e.group("mixed").VCpuMin)
}

// TestAutoScaling_TemplateTagsAreHeldToEC2sRules pins that merging a template's tags with the
// group's does not skip EC2's tag-on-create checks: the merged set is counted against EC2's 50.
func TestAutoScaling_TemplateTagsAreHeldToEC2sRules(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	params := map[string]string{
		"LaunchTemplateName":                                 "fifty-tags",
		"LaunchTemplateData.ImageId":                         ec2TestImage,
		"LaunchTemplateData.TagSpecification.1.ResourceType": "instance",
	}
	for i := 1; i <= 50; i++ {
		params[fmt.Sprintf("LaunchTemplateData.TagSpecification.1.Tag.%d.Key", i)] = fmt.Sprintf("t%02d", i)
		params[fmt.Sprintf("LaunchTemplateData.TagSpecification.1.Tag.%d.Value", i)] = "v"
	}
	e.ec2XML("CreateLaunchTemplate", params, nil)

	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "tagged", "MinSize": "0", "MaxSize": "2", "DesiredCapacity": "1",
		"VPCZoneIdentifier": e.subnetA, "LaunchTemplate.LaunchTemplateName": "fifty-tags",
		"Tags.member.1.Key": "fifty-first", "Tags.member.1.Value": "v", "Tags.member.1.PropagateAtLaunch": "true",
	})
	assert.Empty(t, e.group("tagged").Instances, "fifty template tags plus one propagated tag exceed EC2's limit")
	acts := e.activities(map[string]string{"AutoScalingGroupName": "tagged"})
	require.NotEmpty(t, acts)
	assert.Equal(t, "Failed", acts[0].StatusCode)
	assert.Contains(t, acts[0].StatusMessage, "Launching EC2 instance failed.")
	assert.Empty(t, e.ec2GroupInstances("tagged"), "and nothing reached EC2")

	// The fifty alone fit — the reserved group-name tag does not count — so a group
	// propagating nothing launches.
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "untagged", "MinSize": "0", "MaxSize": "2", "DesiredCapacity": "1",
		"VPCZoneIdentifier": e.subnetA, "LaunchTemplate.LaunchTemplateName": "fifty-tags",
	})
	assert.Len(t, e.group("untagged").Instances, 1)
}

// asgFailingState fails every write of an EC2 instance record once armed, which is the only
// way to make a group's termination fail: termination protection does not apply to it.
type asgFailingState struct {
	emulator.StateManager
	armed     atomic.Bool
	asgWrites atomic.Int64
}

func (s *asgFailingState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == "autoscaling" {
		s.asgWrites.Add(1)
	}
	if s.armed.Load() && namespace == "ec2" && strings.HasPrefix(key, "instance:") {
		return errors.New("injected instance write failure")
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

// newASGInProcess builds EC2 and Auto Scaling over an [asgFailingState] with a frozen clock,
// holding a launch template "lt" and a two-instance zone-only group "pool".
func newASGInProcess(t *testing.T) (*asgFailingState, *emulator.EC2Plugin, *emulator.AutoScalingPlugin, func(string) *emulator.RequestContext) {
	t.Helper()
	state := &asgFailingState{StateManager: emulator.NewMemoryStateManager()}
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
	tc.SetScale(0)
	registry := emulator.NewPluginRegistry()
	ec2 := &emulator.EC2Plugin{}
	require.NoError(t, ec2.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
		Options: map[string]any{"time_controller": tc}}))
	registry.Register(ec2)
	asg := &emulator.AutoScalingPlugin{}
	require.NoError(t, asg.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
		Options: map[string]any{"time_controller": tc, "registry": registry}}))
	registry.Register(asg)
	ctx := func(id string) *emulator.RequestContext {
		return &emulator.RequestContext{RequestID: id, AccountID: asgAuthzAccount, Region: asgAuthzRegion, IDs: emulator.NewIDMint(id)}
	}
	_, err := ec2.HandleRequest(ctx("req-lt"), &emulator.AWSRequest{Service: "ec2", Operation: "CreateLaunchTemplate",
		Params: map[string]string{"LaunchTemplateName": "lt", "LaunchTemplateData.ImageId": ec2TestImage}})
	require.NoError(t, err)
	_, err = asg.HandleRequest(ctx("req-create"), &emulator.AWSRequest{Service: "autoscaling", Operation: "CreateAutoScalingGroup",
		Params: map[string]string{"AutoScalingGroupName": "pool", "MinSize": "0", "MaxSize": "2", "DesiredCapacity": "2",
			"AvailabilityZones.member.1": "us-east-1a", "LaunchTemplate.LaunchTemplateName": "lt"}})
	require.NoError(t, err)
	return state, ec2, asg, ctx
}

// TestAutoScaling_ForceDeleteKeepsTheGroupWhenTerminationFails pins that ForceDelete does not
// delete a group whose instances it could not terminate, nor record them as terminated.
func TestAutoScaling_ForceDeleteKeepsTheGroupWhenTerminationFails(t *testing.T) {
	t.Parallel()
	state, _, asg, ctx := newASGInProcess(t)
	state.armed.Store(true)
	_, err := asg.HandleRequest(ctx("req-delete"), &emulator.AWSRequest{Service: "autoscaling", Operation: "DeleteAutoScalingGroup",
		Params: map[string]string{"AutoScalingGroupName": "pool", "ForceDelete": "true"}})
	require.Error(t, err, "a ForceDelete that could not terminate the instances fails")
	state.armed.Store(false)

	raw, err := state.Get(context.Background(), "autoscaling", "group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/pool") //nolint:contextcheck
	require.NoError(t, err)
	require.NotNil(t, raw, "the group stays")
	var g emulator.AutoScalingGroup
	require.NoError(t, json.Unmarshal(raw, &g))
	assert.Len(t, g.Instances, 2, "still owning both instances")

	resp, err := asg.HandleRequest(ctx("req-acts"), &emulator.AWSRequest{Service: "autoscaling", Operation: "DescribeScalingActivities",
		Params: map[string]string{"AutoScalingGroupName": "pool"}})
	require.NoError(t, err)
	assert.NotContains(t, string(resp.Body), "deleted the AutoScalingGroup", "no termination was recorded")
}

// TestAutoScaling_MultiTerminateIsAllOrNothing pins that a batch with one bad member terminates
// nothing, and that a group's terminations are not stopped by EC2 termination protection.
func TestAutoScaling_MultiTerminateIsAllOrNothing(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	e.createGroup("pool", 0, 5, 3, nil)
	ids := asgInstanceIDs(e.group("pool"))

	_, code, _ := e.asgErr("TerminateInstanceInAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "pool", "InstanceIds.member.1": ids[0], "InstanceIds.member.2": "i-0000000000000dead",
		"ShouldDecrementDesiredCapacity": "true",
	})
	assert.Equal(t, "ValidationError", code)
	assert.Equal(t, ids, asgInstanceIDs(e.group("pool")))
	assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "running"), 3, "the valid member was not terminated either")
	before := len(e.activities(map[string]string{"AutoScalingGroupName": "pool"}))

	// "Enabling termination protection does not prevent Amazon EC2 Auto Scaling from
	// terminating an instance" — so a protected member terminates with the rest.
	e.ec2XML("ModifyInstanceAttribute", map[string]string{"InstanceId": ids[1], "DisableApiTermination.Value": "true"}, nil)
	body := e.mustASG("TerminateInstanceInAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "pool", "InstanceIds.member.1": ids[0], "InstanceIds.member.2": ids[1],
		"ShouldDecrementDesiredCapacity": "true",
	})
	assert.Equal(t, 2, strings.Count(string(body), "<ActivityId>"))
	g := e.group("pool")
	assert.Equal(t, []string{ids[2]}, asgInstanceIDs(g))
	assert.Equal(t, 1, g.Desired)
	assert.Len(t, ec2ByState(e.ec2GroupInstances("pool"), "terminated"), 2)
	assert.Len(t, e.activities(map[string]string{"AutoScalingGroupName": "pool"}), before+2)

	// And scale-in is not stopped by it either.
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "2"})
	g = e.group("pool")
	e.ec2XML("ModifyInstanceAttribute", map[string]string{"InstanceId": g.Instances[0].InstanceID, "DisableApiTermination.Value": "true"}, nil)
	e.ec2XML("ModifyInstanceAttribute", map[string]string{"InstanceId": g.Instances[1].InstanceID, "DisableApiTermination.Value": "true"}, nil)
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "pool", "DesiredCapacity": "0"})
	assert.Empty(t, e.group("pool").Instances)
}

// TestAutoScaling_ObservingASettledGroupWritesNothing pins that a describe of a group with
// nothing to reconcile performs no Auto Scaling write at all.
func TestAutoScaling_ObservingASettledGroupWritesNothing(t *testing.T) {
	t.Parallel()
	state, _, asg, ctx := newASGInProcess(t)
	state.asgWrites.Store(0)
	for i, op := range []string{"DescribeAutoScalingGroups", "DescribeAutoScalingInstances", "DescribeScalingActivities", "DescribeAutoScalingGroups"} {
		_, err := asg.HandleRequest(ctx(fmt.Sprintf("req-observe-%d", i)), &emulator.AWSRequest{Service: "autoscaling", Operation: op, Params: map[string]string{}})
		require.NoError(t, err, op)
	}
	assert.Zero(t, state.asgWrites.Load(), "observing a settled group wrote state")
}

// TestAutoScaling_ObservationsDoNotChurn pins that polling a group under a launch-failure seed
// records one failure, not one per poll.
func TestAutoScaling_ObservationsDoNotChurn(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.ts.URL+"/v1/autoscaling/launch-failure",
		bytes.NewBufferString(`{"autoScalingGroupName":"seeded"}`))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	failed := func() int {
		n := 0
		for _, a := range e.activities(map[string]string{"AutoScalingGroupName": "seeded"}) {
			if a.StatusCode == "Failed" {
				n++
			}
		}
		return n
	}
	e.createGroup("seeded", 0, 5, 2, nil)
	for range 4 {
		e.group("seeded")
	}
	assert.Equal(t, 1, failed(), "polling records no new failure for the same desired capacity")
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "seeded", "DesiredCapacity": "3"})
	e.group("seeded")
	assert.Equal(t, 2, failed(), "a desired-capacity change records its own attempt")
}

// TestAutoScaling_DryRunDenialIsAutoScalingsOwnRefusal pins the response a create gets when
// the caller may not launch from the template: Auto Scaling's AccessDenied naming the
// template, not EC2's refusal of a RunInstances the caller never sent.
func TestAutoScaling_DryRunDenialIsAutoScalingsOwnRefusal(t *testing.T) {
	t.Parallel()
	const policyARN = "arn:aws:iam::" + asgAuthzAccount + ":policy/ASGDryRun"
	state := newAuthTestState(t, "asg-user", policyARN, emulator.PolicyDocument{Version: "2012-10-17", Statement: []emulator.PolicyStatement{
		asgStatement("*", "*", nil),
		{Effect: "Deny", Action: emulator.StringOrSlice{"ec2:RunInstances"}, Resource: emulator.StringOrSlice{"*"}},
	}})
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
	registry := emulator.NewPluginRegistry()
	ec2 := &emulator.EC2Plugin{}
	require.NoError(t, ec2.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
		Options: map[string]any{"time_controller": tc}}))
	registry.Register(ec2)
	asg := &emulator.AutoScalingPlugin{}
	require.NoError(t, asg.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
		Options: map[string]any{"time_controller": tc, "registry": registry, "auth_controller": emulator.NewAuthController(state, logger)}}))
	registry.Register(asg)
	var lt struct {
		ID string `xml:"launchTemplate>launchTemplateId"`
	}
	resp, err := ec2.HandleRequest(&emulator.RequestContext{RequestID: "req-lt", AccountID: asgAuthzAccount, Region: asgAuthzRegion},
		&emulator.AWSRequest{Service: "ec2", Operation: "CreateLaunchTemplate", Params: map[string]string{
			"LaunchTemplateName": "named-lt", "LaunchTemplateData.ImageId": ec2TestImage}})
	require.NoError(t, err)
	require.NoError(t, xml.Unmarshal(resp.Body, &lt))

	tests := []struct {
		name, key, value, want string
	}{
		{"by name", "LaunchTemplate.LaunchTemplateName", "named-lt", "You are not authorized to use launch template: named-lt"},
		{"by id", "LaunchTemplate.LaunchTemplateId", lt.ID, "You are not authorized to use launch template: " + lt.ID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqCtx := newAuthTestReqCtx("arn:aws:iam::" + asgAuthzAccount + ":user/asg-user")
			reqCtx.IDs = emulator.NewIDMint("req-" + tt.name)
			_, err := asg.HandleRequest(reqCtx, &emulator.AWSRequest{Service: "autoscaling", Operation: "CreateAutoScalingGroup",
				Params: map[string]string{"AutoScalingGroupName": "pool", "MinSize": "1", "MaxSize": "1",
					"AvailabilityZones.member.1": "us-east-1a", tt.key: tt.value}})
			var awsErr *emulator.AWSError
			require.True(t, errors.As(err, &awsErr), "%v", err)
			assert.Equal(t, "AccessDenied", awsErr.Code)
			assert.Equal(t, http.StatusForbidden, awsErr.HTTPStatus)
			assert.Equal(t, tt.want, awsErr.Message)
		})
	}
}
