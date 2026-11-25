package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errInjectedStore is the failure [nsFaultState] injects.
var errInjectedStore = errors.New("injected store failure")

// nsFaultState wraps a working StateManager and, once armed, fails every call of one kind ("get",
// "put", "delete" or "list") on one namespace.
type nsFaultState struct {
	emulator.StateManager
	namespace string
	fail      string
}

func (s *nsFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if namespace == s.namespace && s.fail == "get" {
		return nil, errInjectedStore
	}
	return s.StateManager.Get(ctx, namespace, key)
}

func (s *nsFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == s.namespace && s.fail == "put" {
		return errInjectedStore
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

func (s *nsFaultState) Delete(ctx context.Context, namespace, key string) error {
	if namespace == s.namespace && s.fail == "delete" {
		return errInjectedStore
	}
	return s.StateManager.Delete(ctx, namespace, key)
}

func (s *nsFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if namespace == s.namespace && s.fail == "list" {
		return nil, errInjectedStore
	}
	return s.StateManager.List(ctx, namespace, prefix)
}

// faultOp is one request a fault test drives.
type faultOp struct {
	op   string
	body map[string]any
}

// runFaultOps drives each op against a fresh plugin under each fault kind, after setup has run
// unfaulted, and asserts that a store failure is never answered as success or as an AWS error: the
// plugin returns the injected failure, wrapped. Every op must be failed by at least one kind.
func runFaultOps(t *testing.T, namespace string, newPlugin func(emulator.StateManager) emulator.Plugin,
	setup []faultOp, ops []faultOp,
) {
	t.Helper()
	call := func(p emulator.Plugin, o faultOp) error {
		body, err := json.Marshal(o.body)
		require.NoError(t, err)
		ctx := &emulator.RequestContext{RequestID: "req-" + o.op, AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-" + o.op)}
		_, err = p.HandleRequest(ctx, &emulator.AWSRequest{Service: namespace, Operation: o.op, Body: body,
			Headers: map[string]string{}, Params: map[string]string{}})
		return err
	}
	for _, o := range ops {
		failedBySome := false
		for _, kind := range []string{"get", "put", "delete", "list"} {
			state := &nsFaultState{StateManager: emulator.NewMemoryStateManager(), namespace: namespace}
			p := newPlugin(state)
			for _, s := range setup {
				require.NoError(t, call(p, s), "setup %s", s.op)
			}
			state.fail = kind
			err := call(p, o)
			if err == nil {
				continue
			}
			var awsErr *emulator.AWSError
			if errors.As(err, &awsErr) {
				continue
			}
			failedBySome = true
			assert.ErrorIs(t, err, errInjectedStore, "%s under a %s failure", o.op, kind)
		}
		assert.True(t, failedBySome, "%s reports a store failure under some fault kind", o.op)
	}
}

// TestApplicationAutoScaling_StoreFailuresAreReported drives every operation that reads or writes
// a target against a failing store.
func TestApplicationAutoScaling_StoreFailuresAreReported(t *testing.T) {
	newPlugin := func(state emulator.StateManager) emulator.Plugin {
		p := &emulator.ApplicationAutoScalingPlugin{}
		tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
		require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state,
			Logger: emulator.NewDefaultLogger(slog.LevelError, false), Options: map[string]any{"time_controller": tc}}))
		return p
	}
	ref := func(extra map[string]any) map[string]any {
		m := map[string]any{"ServiceNamespace": "custom-resource", "ResourceId": "r",
			"ScalableDimension": "custom-resource:ResourceType:Property"}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	arn := "arn:aws:application-autoscaling:us-east-1:123456789012:scalable-target/" +
		emulator.NewIDMint("req-RegisterScalableTarget").Hex(18)
	step := map[string]any{"AdjustmentType": "ChangeInCapacity"}
	tracking := map[string]any{"TargetValue": 1, "PredefinedMetricSpecification": map[string]string{"PredefinedMetricType": "X"}}
	setup := []faultOp{
		{"RegisterScalableTarget", ref(map[string]any{"MinCapacity": 0, "MaxCapacity": 2, "RoleARN": "arn:aws:iam::123456789012:role/r"})},
		{"PutScalingPolicy", ref(map[string]any{"PolicyName": "p", "PolicyType": "StepScaling", "StepScalingPolicyConfiguration": step})},
		{"PutScheduledAction", ref(map[string]any{"ScheduledActionName": "s", "Schedule": "rate(1 day)"})},
		{"TagResource", map[string]any{"ResourceARN": arn, "Tags": map[string]string{"a": "b"}}},
	}
	runFaultOps(t, "application-autoscaling", newPlugin, setup, []faultOp{
		{"RegisterScalableTarget", ref(map[string]any{"MaxCapacity": 3})},
		{"RegisterScalableTarget", ref(map[string]any{"ResourceId": "new", "MinCapacity": 0, "MaxCapacity": 1})},
		{"DescribeScalableTargets", map[string]any{"ServiceNamespace": "custom-resource"}},
		{"DeregisterScalableTarget", ref(nil)},
		{"PutScalingPolicy", ref(map[string]any{"PolicyName": "t", "PolicyType": "TargetTrackingScaling", "TargetTrackingScalingPolicyConfiguration": tracking})},
		{"DescribeScalingPolicies", map[string]any{"ServiceNamespace": "custom-resource"}},
		{"DeleteScalingPolicy", ref(map[string]any{"PolicyName": "p"})},
		{"PutScheduledAction", ref(map[string]any{"ScheduledActionName": "s", "Schedule": "rate(2 days)"})},
		{"DescribeScheduledActions", map[string]any{"ServiceNamespace": "custom-resource"}},
		{"DeleteScheduledAction", ref(map[string]any{"ScheduledActionName": "s"})},
		{"TagResource", map[string]any{"ResourceARN": arn, "Tags": map[string]string{"c": "d"}}},
		{"UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"a"}}},
		{"ListTagsForResource", map[string]any{"ResourceARN": arn}},
	})
}
