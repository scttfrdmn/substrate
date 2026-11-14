package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

const (
	asgAuthzAccount = "123456789012"
	asgAuthzRegion  = "us-east-1"
	asgAuthzDevARN  = "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:11111111-2222-3333-4444-555555555555:autoScalingGroupName/dev-pool"
	asgAuthzProdARN = "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:66666666-7777-8888-9999-000000000000:autoScalingGroupName/prod-pool"
)

// asgAuthzFixture is a state store holding two tagged groups and a user whose one policy a
// test rewrites, plus an AuthController over the same store.
type asgAuthzFixture struct {
	state emulator.StateManager
	auth  *emulator.AuthController
}

func newASGAuthzFixture(t *testing.T, statements ...emulator.PolicyStatement) *asgAuthzFixture {
	t.Helper()
	const policyARN = "arn:aws:iam::" + asgAuthzAccount + ":policy/ASG"
	state := newAuthTestState(t, "asg-user", policyARN, emulator.PolicyDocument{Version: "2012-10-17", Statement: statements})
	f := &asgAuthzFixture{state: state, auth: emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))}
	for _, g := range []emulator.AutoScalingGroup{
		{Name: "dev-pool", ARN: asgAuthzDevARN, Tags: []emulator.AutoScalingTag{{Key: "team", Value: "dev"}}},
		{Name: "prod-pool", ARN: asgAuthzProdARN, Tags: []emulator.AutoScalingTag{{Key: "team", Value: "prod"}}},
	} {
		raw, err := json.Marshal(g)
		require.NoError(t, err)
		require.NoError(t, state.Put(context.Background(), "autoscaling", //nolint:contextcheck
			"group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/"+g.Name, raw))
	}
	return f
}

func (f *asgAuthzFixture) call(t *testing.T, operation string, params map[string]string) error {
	t.Helper()
	return f.auth.CheckAccess(newAuthTestReqCtx("arn:aws:iam::"+asgAuthzAccount+":user/asg-user"), &emulator.AWSRequest{
		Service: "autoscaling", Operation: operation, Path: "/", Params: params,
	})
}

func asgStatement(action, resource string, cond map[string]map[string]emulator.StringOrSlice) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:    "Allow",
		Action:    emulator.StringOrSlice{action},
		Resource:  emulator.StringOrSlice{resource},
		Condition: cond,
	}
}

func asgDenied(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	var awsErr *emulator.AWSError
	require.True(t, errors.As(err, &awsErr), "%T: %v", err, err)
	assert.Equal(t, "AccessDenied", awsErr.Code)
	return true
}

func TestAutoScaling_Authz_ResourceTagConditions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
	}{
		{"service-specific key", "autoscaling:ResourceTag/team"},
		{"global key", "aws:ResourceTag/team"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newASGAuthzFixture(t, asgStatement("autoscaling:SetDesiredCapacity", "*",
				map[string]map[string]emulator.StringOrSlice{"StringEquals": {tc.key: {"dev"}}}))
			require.NoError(t, f.call(t, "SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "dev-pool", "DesiredCapacity": "2"}))
			assert.True(t, asgDenied(t, f.call(t, "SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "prod-pool", "DesiredCapacity": "2"})),
				"a group tagged team=prod is not covered by a team=dev condition")
		})
	}

	t.Run("by instance ID, the instance's group decides", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t, asgStatement("autoscaling:TerminateInstanceInAutoScalingGroup", "*",
			map[string]map[string]emulator.StringOrSlice{"StringEquals": {"autoscaling:ResourceTag/team": {"dev"}}}))
		g := emulator.AutoScalingGroup{Name: "dev-pool", ARN: asgAuthzDevARN,
			Tags:      []emulator.AutoScalingTag{{Key: "team", Value: "dev"}},
			Instances: []emulator.AutoScalingInstance{{InstanceID: "i-0aaaabbbbccccdddd"}}}
		raw, err := json.Marshal(g)
		require.NoError(t, err)
		require.NoError(t, f.state.Put(context.Background(), "autoscaling", "group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/dev-pool", raw)) //nolint:contextcheck
		require.NoError(t, f.call(t, "TerminateInstanceInAutoScalingGroup", map[string]string{
			"InstanceId": "i-0aaaabbbbccccdddd", "ShouldDecrementDesiredCapacity": "true",
		}))
	})
}

func TestAutoScaling_Authz_ResourceARNs(t *testing.T) {
	t.Parallel()

	t.Run("a mutation is decided against the group's ARN", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t, asgStatement("autoscaling:UpdateAutoScalingGroup", asgAuthzDevARN, nil))
		require.NoError(t, f.call(t, "UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "dev-pool"}))
		assert.True(t, asgDenied(t, f.call(t, "UpdateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "prod-pool"})))
	})

	t.Run("a create matches the user guide's wildcard-UUID form", func(t *testing.T) {
		t.Parallel()
		f := newASGAuthzFixture(t, asgStatement("autoscaling:CreateAutoScalingGroup",
			"arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:*:autoScalingGroupName/new-*", nil))
		require.NoError(t, f.call(t, "CreateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "new-pool"}))
		assert.True(t, asgDenied(t, f.call(t, "CreateAutoScalingGroup", map[string]string{"AutoScalingGroupName": "other"})))
	})

	t.Run("a describe supports no resource-level permission", func(t *testing.T) {
		t.Parallel()
		// The reference gives DescribeAutoScalingGroups no resource type, so its resource is
		// "*", which a statement naming one group's ARN does not cover.
		f := newASGAuthzFixture(t, asgStatement("autoscaling:DescribeAutoScalingGroups", asgAuthzDevARN, nil))
		assert.True(t, asgDenied(t, f.call(t, "DescribeAutoScalingGroups", map[string]string{"AutoScalingGroupNames.member.1": "dev-pool"})))
		f = newASGAuthzFixture(t, asgStatement("autoscaling:DescribeAutoScalingGroups", "*", nil))
		require.NoError(t, f.call(t, "DescribeAutoScalingGroups", nil))
	})
}

func TestAutoScaling_Authz_RequestTags(t *testing.T) {
	t.Parallel()
	f := newASGAuthzFixture(t,
		asgStatement("autoscaling:CreateAutoScalingGroup", "*",
			map[string]map[string]emulator.StringOrSlice{"StringEquals": {"aws:RequestTag/team": {"dev"}}}),
		asgStatement("autoscaling:CreateOrUpdateTags", "*",
			map[string]map[string]emulator.StringOrSlice{"ForAllValues:StringEquals": {"aws:TagKeys": {"team", "owner"}}}),
	)
	tagged := func(key, value string) map[string]string {
		return map[string]string{"AutoScalingGroupName": "x", "Tags.member.1.ResourceId": "dev-pool", "Tags.member.1.Key": key, "Tags.member.1.Value": value}
	}
	require.NoError(t, f.call(t, "CreateAutoScalingGroup", tagged("team", "dev")))
	assert.True(t, asgDenied(t, f.call(t, "CreateAutoScalingGroup", tagged("team", "prod"))))
	require.NoError(t, f.call(t, "CreateOrUpdateTags", tagged("owner", "me")))
	assert.True(t, asgDenied(t, f.call(t, "CreateOrUpdateTags", tagged("cost-center", "1"))))
}

// TestAutoScaling_Authz_CreateChecksRunInstances pins the user guide's rule that a create
// checks the caller's ec2:RunInstances permission — a dry run — before anything is written,
// while the launches themselves run under the group's service-linked role.
func TestAutoScaling_Authz_CreateChecksRunInstances(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T, statements ...emulator.PolicyStatement) (*emulator.AutoScalingPlugin, emulator.StateManager) {
		t.Helper()
		const policyARN = "arn:aws:iam::" + asgAuthzAccount + ":policy/ASGLaunch"
		state := newAuthTestState(t, "asg-user", policyARN, emulator.PolicyDocument{Version: "2012-10-17", Statement: statements})
		logger := emulator.NewDefaultLogger(slog.LevelError, false)
		tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
		tc.SetScale(0)
		auth := emulator.NewAuthController(state, logger)
		registry := emulator.NewPluginRegistry()
		ec2 := &emulator.EC2Plugin{}
		require.NoError(t, ec2.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
			Options: map[string]any{"time_controller": tc}}))
		registry.Register(ec2)
		asg := &emulator.AutoScalingPlugin{}
		require.NoError(t, asg.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger, //nolint:contextcheck
			Options: map[string]any{"time_controller": tc, "registry": registry, "auth_controller": auth}}))
		registry.Register(asg)

		// The template is created without a principal, as setup rather than as the subject.
		_, err := ec2.HandleRequest(&emulator.RequestContext{RequestID: "req-lt", AccountID: asgAuthzAccount, Region: asgAuthzRegion},
			&emulator.AWSRequest{Service: "ec2", Operation: "CreateLaunchTemplate", Params: map[string]string{
				"LaunchTemplateName": "lt", "LaunchTemplateData.ImageId": ec2TestImage,
			}})
		require.NoError(t, err)
		return asg, state
	}
	create := func(t *testing.T, asg *emulator.AutoScalingPlugin) error {
		t.Helper()
		reqCtx := newAuthTestReqCtx("arn:aws:iam::" + asgAuthzAccount + ":user/asg-user")
		reqCtx.IDs = emulator.NewIDMint("req-create")
		_, err := asg.HandleRequest(reqCtx, &emulator.AWSRequest{Service: "autoscaling", Operation: "CreateAutoScalingGroup", Params: map[string]string{
			"AutoScalingGroupName":              "pool",
			"MinSize":                           "1",
			"MaxSize":                           "2",
			"AvailabilityZones.member.1":        "us-east-1a",
			"LaunchTemplate.LaunchTemplateName": "lt",
		}})
		return err
	}
	groupExists := func(t *testing.T, state emulator.StateManager) bool {
		t.Helper()
		raw, err := state.Get(context.Background(), "autoscaling", "group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/pool") //nolint:contextcheck
		require.NoError(t, err)
		return raw != nil
	}

	t.Run("denied ec2:RunInstances refuses the create and writes nothing", func(t *testing.T) {
		t.Parallel()
		asg, state := setup(t,
			asgStatement("*", "*", nil),
			emulator.PolicyStatement{Effect: "Deny", Action: emulator.StringOrSlice{"ec2:RunInstances"}, Resource: emulator.StringOrSlice{"*"}},
		)
		assert.True(t, asgDenied(t, create(t, asg)))
		assert.False(t, groupExists(t, state))
	})

	t.Run("allowed, the group is created and launches", func(t *testing.T) {
		t.Parallel()
		asg, state := setup(t, asgStatement("*", "*", nil))
		require.NoError(t, create(t, asg))
		require.True(t, groupExists(t, state))
		raw, err := state.Get(context.Background(), "autoscaling", "group:"+asgAuthzAccount+"/"+asgAuthzRegion+"/pool") //nolint:contextcheck
		require.NoError(t, err)
		var g emulator.AutoScalingGroup
		require.NoError(t, json.Unmarshal(raw, &g))
		assert.Len(t, g.Instances, 1)
	})
}
