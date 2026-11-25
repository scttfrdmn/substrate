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

// errSDInjectedStore is the failure [sdFaultState] injects.
var errSDInjectedStore = errors.New("injected Cloud Map store failure")

// sdFaultState wraps a working StateManager and, once armed, fails every call of one kind ("get",
// "put", "delete" or "list") on one namespace, Cloud Map's own by default, after skip successful
// calls of that kind.
type sdFaultState struct {
	emulator.StateManager
	// ns is the namespace that fails; empty is Cloud Map's own.
	ns   string
	fail string
	skip int
}

// trip reports whether a call of kind on namespace fails.
func (s *sdFaultState) trip(namespace, kind string) bool {
	want := s.ns
	if want == "" {
		want = "servicediscovery"
	}
	if namespace != want || s.fail != kind {
		return false
	}
	if s.skip > 0 {
		s.skip--
		return false
	}
	return true
}

func (s *sdFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if s.trip(namespace, "get") {
		return nil, errSDInjectedStore
	}
	return s.StateManager.Get(ctx, namespace, key)
}

func (s *sdFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if s.trip(namespace, "put") {
		return errSDInjectedStore
	}
	return s.StateManager.Put(ctx, namespace, key, value)
}

func (s *sdFaultState) Delete(ctx context.Context, namespace, key string) error {
	if s.trip(namespace, "delete") {
		return errSDInjectedStore
	}
	return s.StateManager.Delete(ctx, namespace, key)
}

func (s *sdFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if s.trip(namespace, "list") {
		return nil, errSDInjectedStore
	}
	return s.StateManager.List(ctx, namespace, prefix)
}

// TestServiceDiscovery_StoreFailuresAreReported drives every operation against a store that fails
// each kind of call, at each of its first few calls, and asserts that a store failure is never
// answered as success or as an AWS error: the plugin returns the injected failure, wrapped.
func TestServiceDiscovery_StoreFailuresAreReported(t *testing.T) {
	type sdOp struct {
		op   string
		body map[string]any
	}
	call := func(p emulator.Plugin, o sdOp) error {
		body, err := json.Marshal(o.body)
		require.NoError(t, err)
		ctx := &emulator.RequestContext{RequestID: "req-" + o.op, AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-" + o.op)}
		_, err = p.HandleRequest(ctx, &emulator.AWSRequest{Service: "servicediscovery", Operation: o.op, Body: body,
			Headers: map[string]string{}, Params: map[string]string{}})
		return err
	}
	// The setup's IDs are deterministic, because each request's mint is seeded from its ID.
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	mint := emulator.NewIDMint("req-CreateHttpNamespace")
	nsID := "ns-" + mint.Chars(16, alphabet)
	createOp := mint.Chars(32, alphabet) + "-" + mint.Chars(9, alphabet)
	newPlugin := func(state emulator.StateManager) (emulator.Plugin, string) {
		p := &emulator.ServiceDiscoveryPlugin{}
		tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
		require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state,
			Logger: emulator.NewDefaultLogger(slog.LevelError, false), Options: map[string]any{"time_controller": tc}}))
		require.NoError(t, call(p, sdOp{"CreateHttpNamespace", map[string]any{"Name": "apps"}}))
		svcBody, err := json.Marshal(map[string]any{"Name": "svc", "NamespaceId": nsID,
			"HealthCheckCustomConfig": map[string]any{}})
		require.NoError(t, err)
		resp, err := p.HandleRequest(&emulator.RequestContext{RequestID: "req-svc", AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-svc")}, &emulator.AWSRequest{Service: "servicediscovery", Operation: "CreateService",
			Body: svcBody, Headers: map[string]string{}, Params: map[string]string{}})
		require.NoError(t, err)
		var out struct {
			Service struct {
				ID string `json:"Id"`
			} `json:"Service"`
		}
		require.NoError(t, json.Unmarshal(resp.Body, &out))
		require.NoError(t, call(p, sdOp{"RegisterInstance", map[string]any{"ServiceId": out.Service.ID, "InstanceId": "i",
			"Attributes": map[string]string{"k": "v"}}}))
		return p, out.Service.ID
	}
	nsARN := "arn:aws:servicediscovery:us-east-1:123456789012:namespace/" + nsID
	ops := func(svcID string) []sdOp {
		return []sdOp{
			{"CreateHttpNamespace", map[string]any{"Name": "other"}},
			{"GetNamespace", map[string]any{"Id": nsID}},
			{"ListNamespaces", map[string]any{}},
			{"UpdateHttpNamespace", map[string]any{"Id": nsID, "Namespace": map[string]any{"Description": "d"}}},
			{"DeleteNamespace", map[string]any{"Id": nsID}},
			{"CreateService", map[string]any{"Name": "svc2", "NamespaceId": nsID}},
			{"GetService", map[string]any{"Id": svcID}},
			{"ListServices", map[string]any{}},
			{"UpdateService", map[string]any{"Id": svcID, "Service": map[string]any{"Description": "d"}}},
			{"DeleteService", map[string]any{"Id": svcID}},
			{"RegisterInstance", map[string]any{"ServiceId": svcID, "InstanceId": "j", "Attributes": map[string]string{}}},
			{"DeregisterInstance", map[string]any{"ServiceId": svcID, "InstanceId": "i"}},
			{"GetInstance", map[string]any{"ServiceId": svcID, "InstanceId": "i"}},
			{"ListInstances", map[string]any{"ServiceId": svcID}},
			{"GetInstancesHealthStatus", map[string]any{"ServiceId": svcID}},
			{"UpdateInstanceCustomHealthStatus", map[string]any{"ServiceId": svcID, "InstanceId": "i", "Status": "UNHEALTHY"}},
			{"DiscoverInstances", map[string]any{"NamespaceName": "apps", "ServiceName": "svc", "HealthStatus": "ALL"}},
			{"ListOperations", map[string]any{}},
			{"GetOperation", map[string]any{"OperationId": createOp}},
			{"TagResource", map[string]any{"ResourceARN": nsARN, "Tags": []map[string]string{{"Key": "a", "Value": "b"}}}},
			{"UntagResource", map[string]any{"ResourceARN": nsARN, "TagKeys": []string{"a"}}},
			{"ListTagsForResource", map[string]any{"ResourceARN": nsARN}},
		}
	}
	_, svcID := newPlugin(emulator.NewMemoryStateManager())
	for _, o := range ops(svcID) {
		failedBySome := false
		for _, kind := range []string{"get", "put", "delete", "list"} {
			for skip := range 4 {
				state := &sdFaultState{StateManager: emulator.NewMemoryStateManager()}
				p, _ := newPlugin(state)
				state.fail, state.skip = kind, skip
				err := call(p, o)
				var awsErr *emulator.AWSError
				if err == nil || errors.As(err, &awsErr) {
					continue
				}
				failedBySome = true
				assert.ErrorIs(t, err, errSDInjectedStore, "%s under a %s failure after %d calls", o.op, kind, skip)
			}
		}
		assert.True(t, failedBySome, "%s reports a store failure under some fault", o.op)
	}
}
