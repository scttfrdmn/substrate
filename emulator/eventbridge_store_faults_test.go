package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A store fault, or a record that does not decode, is an internal error on every EventBridge
// operation that reads or writes the record: the operation fails rather than answering as though
// the record were absent or the write had landed.

// ebFaultState fails one kind of store call on keys with one prefix in the eventbridge namespace,
// once armed.
type ebFaultState struct {
	emulator.StateManager
	armed  atomic.Bool
	call   string
	prefix string
}

func (s *ebFaultState) fails(call, ns, key string) bool {
	return s.armed.Load() && call == s.call && ns == "eventbridge" && strings.HasPrefix(key, s.prefix)
}

func (s *ebFaultState) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if s.fails("get", ns, key) {
		return nil, errors.New("injected get failure")
	}
	return s.StateManager.Get(ctx, ns, key)
}

func (s *ebFaultState) Put(ctx context.Context, ns, key string, value []byte) error {
	if s.fails("put", ns, key) {
		return errors.New("injected put failure")
	}
	return s.StateManager.Put(ctx, ns, key, value)
}

func (s *ebFaultState) Delete(ctx context.Context, ns, key string) error {
	if s.fails("delete", ns, key) {
		return errors.New("injected delete failure")
	}
	return s.StateManager.Delete(ctx, ns, key)
}

func (s *ebFaultState) List(ctx context.Context, ns, prefix string) ([]string, error) {
	if s.fails("list", ns, prefix) {
		return nil, errors.New("injected list failure")
	}
	return s.StateManager.List(ctx, ns, prefix)
}

// ebFaultEnv is an EventBridge plugin over an [ebFaultState] holding a bus "orders" with a rule r
// that has a target, an empty bus "empty", and a default-bus rule d with no targets.
type ebFaultEnv struct {
	state *ebFaultState
	p     *emulator.EventBridgePlugin
	ctx   *emulator.RequestContext
}

func newEBFaultEnv(t *testing.T) *ebFaultEnv {
	t.Helper()
	e := &ebFaultEnv{
		state: &ebFaultState{StateManager: emulator.NewMemoryStateManager()},
		p:     &emulator.EventBridgePlugin{},
		ctx: &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-eb-fault",
			IDs: emulator.NewIDMint("req-eb-fault")},
	}
	require.NoError(t, e.p.Initialize(t.Context(), emulator.PluginConfig{
		State: e.state, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}))
	for _, step := range []struct {
		op   string
		body map[string]any
	}{
		{"CreateEventBus", map[string]any{"Name": "orders"}},
		{"CreateEventBus", map[string]any{"Name": "empty"}},
		{"PutRule", map[string]any{"Name": "r", "EventBusName": "orders", "ScheduleExpression": "rate(1 hour)"}},
		{"PutTargets", map[string]any{"Rule": "r", "EventBusName": "orders", "Targets": []map[string]string{{"Id": "t", "Arn": "arn:aws:sqs:us-east-1:123456789012:q"}}}},
		{"PutRule", map[string]any{"Name": "d", "ScheduleExpression": "rate(1 hour)"}},
		{"PutPermission", map[string]any{"EventBusName": "orders", "StatementId": "s", "Action": "events:PutEvents", "Principal": "111122223333"}},
	} {
		_, err := e.call(step.op, step.body)
		require.NoError(t, err, step.op)
	}
	return e
}

func (e *ebFaultEnv) call(op string, body map[string]any) (*emulator.AWSResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return e.p.HandleRequest(e.ctx, &emulator.AWSRequest{Service: "eventbridge", Operation: op, Body: raw,
		Headers: map[string]string{}, Params: map[string]string{}})
}

const (
	ebFaultBusARN  = "arn:aws:events:us-east-1:123456789012:event-bus/orders"
	ebFaultRuleARN = "arn:aws:events:us-east-1:123456789012:rule/orders/r"
)

// ebFaultCases are the operations each store fault must surface through.
var ebFaultCases = []struct {
	call, prefix, op string
	body             map[string]any
}{
	{"get", "ebbus:", "DescribeEventBus", map[string]any{"Name": "orders"}},
	{"get", "ebbus:", "UpdateEventBus", map[string]any{"Name": "orders", "Description": "x"}},
	{"get", "ebbus:", "PutPermission", map[string]any{"EventBusName": "orders", "Policy": "{}"}},
	{"get", "ebbus:", "RemovePermission", map[string]any{"EventBusName": "orders", "RemoveAllPermissions": true}},
	{"get", "ebbus:", "CreateEventBus", map[string]any{"Name": "new"}},
	{"get", "ebbus:", "PutRule", map[string]any{"Name": "r2", "EventBusName": "orders", "ScheduleExpression": "rate(1 hour)"}},
	{"get", "ebbus:", "ListRules", map[string]any{"EventBusName": "orders"}},
	{"get", "ebbus:", "ListEventBuses", map[string]any{}},
	{"get", "ebbus:", "TagResource", map[string]any{"ResourceARN": ebFaultBusARN, "Tags": ebTagList("k", "v")}},
	{"get", "ebbus:", "ListTagsForResource", map[string]any{"ResourceARN": ebFaultBusARN}},
	{"get", "ebbus:", "PutEvents", map[string]any{"Entries": []map[string]string{{"Source": "s", "DetailType": "d", "Detail": "{}", "EventBusName": "orders"}}}},
	{"get", "ebrule:", "DescribeRule", map[string]any{"Name": "r", "EventBusName": "orders"}},
	{"get", "ebrule:", "PutRule", map[string]any{"Name": "r", "EventBusName": "orders", "ScheduleExpression": "rate(1 hour)"}},
	{"get", "ebrule:", "DeleteRule", map[string]any{"Name": "d"}},
	{"get", "ebrule:", "EnableRule", map[string]any{"Name": "d"}},
	{"get", "ebrule:", "PutTargets", map[string]any{"Rule": "d", "Targets": []map[string]string{}}},
	{"get", "ebrule:", "ListRules", map[string]any{}},
	{"get", "ebrule:", "TagResource", map[string]any{"ResourceARN": ebFaultRuleARN, "Tags": ebTagList("k", "v")}},
	{"get", "ebrule:", "ListTagsForResource", map[string]any{"ResourceARN": ebFaultRuleARN}},
	{"get", "ebtargets:", "ListTargetsByRule", map[string]any{"Rule": "r", "EventBusName": "orders"}},
	{"get", "ebtargets:", "DeleteRule", map[string]any{"Name": "d"}},
	{"get", "ebtargets:", "ListRuleNamesByTarget", map[string]any{"TargetArn": "x", "EventBusName": "orders"}},
	{"get", "ebrule_names:", "ListRules", map[string]any{}},
	{"get", "ebrule_names:", "PutRule", map[string]any{"Name": "new", "ScheduleExpression": "rate(1 hour)"}},
	{"get", "ebrule_names:", "DeleteEventBus", map[string]any{"Name": "empty"}},
	{"put", "ebbus:", "CreateEventBus", map[string]any{"Name": "new"}},
	{"put", "ebbus:", "UpdateEventBus", map[string]any{"Name": "orders", "Description": "x"}},
	{"put", "ebbus:", "PutPermission", map[string]any{"EventBusName": "orders", "Policy": "{}"}},
	{"put", "ebbus:", "RemovePermission", map[string]any{"EventBusName": "orders", "StatementId": "s"}},
	{"put", "ebbus:", "TagResource", map[string]any{"ResourceARN": ebFaultBusARN, "Tags": ebTagList("k", "v")}},
	{"put", "ebrule:", "PutRule", map[string]any{"Name": "d", "ScheduleExpression": "rate(1 hour)"}},
	{"put", "ebrule:", "DisableRule", map[string]any{"Name": "d"}},
	{"put", "ebrule:", "UntagResource", map[string]any{"ResourceARN": ebFaultRuleARN, "TagKeys": []string{"k"}}},
	{"put", "ebtargets:", "PutTargets", map[string]any{"Rule": "d", "Targets": []map[string]string{}}},
	{"put", "ebtargets:", "RemoveTargets", map[string]any{"Rule": "d", "Ids": []string{"x"}}},
	{"put", "ebrule_names:", "PutRule", map[string]any{"Name": "new", "ScheduleExpression": "rate(1 hour)"}},
	{"put", "ebrule_names:", "DeleteRule", map[string]any{"Name": "d"}},
	{"put", "ebevents:", "PutEvents", map[string]any{"Entries": []map[string]string{{"Source": "s", "DetailType": "d", "Detail": "{}"}}}},
	{"delete", "ebrule:", "DeleteRule", map[string]any{"Name": "d"}},
	{"delete", "ebtargets:", "DeleteRule", map[string]any{"Name": "d"}},
	{"delete", "ebbus:", "DeleteEventBus", map[string]any{"Name": "empty"}},
	{"list", "ebbus:", "ListEventBuses", map[string]any{}},
	{"list", "ebbus:", "CreateEventBus", map[string]any{"Name": "new"}},
}

func TestEBStoreFaults_SurfaceAsInternalErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range ebFaultCases {
		t.Run(tc.call+" "+tc.prefix+" "+tc.op, func(t *testing.T) {
			t.Parallel()
			e := newEBFaultEnv(t)
			e.state.call, e.state.prefix = tc.call, tc.prefix
			e.state.armed.Store(true)
			_, err := e.call(tc.op, tc.body)
			require.Error(t, err)
			var awsErr *emulator.AWSError
			assert.False(t, errors.As(err, &awsErr), "a store fault is not answered as an AWS refusal: %v", err)
		})
	}
}

func TestEBStoreFaults_ARecordThatDoesNotDecodeIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		key, op string
		body    map[string]any
	}{
		{"ebbus:123456789012/us-east-1/orders", "DescribeEventBus", map[string]any{"Name": "orders"}},
		{"ebrule:123456789012/us-east-1/orders/r", "DescribeRule", map[string]any{"Name": "r", "EventBusName": "orders"}},
		{"ebrule:123456789012/us-east-1/orders/r", "ListTagsForResource", map[string]any{"ResourceARN": ebFaultRuleARN}},
		{"ebtargets:123456789012/us-east-1/orders/r", "ListTargetsByRule", map[string]any{"Rule": "r", "EventBusName": "orders"}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()
			e := newEBFaultEnv(t)
			require.NoError(t, e.state.Put(t.Context(), "eventbridge", tc.key, []byte("{not json")))
			_, err := e.call(tc.op, tc.body)
			require.Error(t, err)
			var awsErr *emulator.AWSError
			assert.False(t, errors.As(err, &awsErr), "%v", err)
		})
	}

	// A stored policy that is not a document fails the statement edits rather than replacing it.
	e := newEBFaultEnv(t)
	raw, err := e.state.Get(t.Context(), "eventbridge", "ebbus:123456789012/us-east-1/orders")
	require.NoError(t, err)
	var bus map[string]any
	require.NoError(t, json.Unmarshal(raw, &bus))
	bus["Policy"] = "[]"
	raw, err = json.Marshal(bus)
	require.NoError(t, err)
	require.NoError(t, e.state.Put(t.Context(), "eventbridge", "ebbus:123456789012/us-east-1/orders", raw))
	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"PutPermission", map[string]any{"EventBusName": "orders", "StatementId": "s2", "Action": "events:PutEvents", "Principal": "111122223333"}},
		{"RemovePermission", map[string]any{"EventBusName": "orders", "StatementId": "s"}},
	} {
		_, err := e.call(tc.op, tc.body)
		assert.Error(t, err, tc.op)
	}
}
