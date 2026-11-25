package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdSignedCall posts a JSON 1.1 request signed as account, so the caller's account comes from its
// credentials.
func sdSignedCall(t *testing.T, ts *emulator.TestServer, account, signingName, target string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	creds, ok := ts.CredentialsFor(account)
	require.True(t, ok, account)
	signAs(req, creds, signingName, "us-east-1", raw)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got
}

// TestServiceDiscovery_TaggingAPIStaysInTheCallersAccount pins that the Resource Groups Tagging API
// cannot tag another account's namespace or service by naming its ARN.
func TestServiceDiscovery_TaggingAPIStaysInTheCallersAccount(t *testing.T) {
	const owner, other = "111111111111", "222222222222"
	ts := emulator.StartTestServerWithAccounts(t, owner, other)
	code, raw := sdSignedCall(t, ts, owner, "servicediscovery", "Route53AutoNaming_v20170314.CreateHttpNamespace",
		map[string]any{"Name": "owned", "Tags": []map[string]string{{"Key": "team", "Value": "a"}}})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	code, raw = sdSignedCall(t, ts, owner, "servicediscovery", "Route53AutoNaming_v20170314.ListNamespaces", map[string]any{})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	var list struct {
		Namespaces []struct {
			ID  string `json:"Id"`
			Arn string `json:"Arn"`
		} `json:"Namespaces"`
	}
	require.NoError(t, json.Unmarshal(raw, &list))
	require.Len(t, list.Namespaces, 1)
	arn := list.Namespaces[0].Arn
	code, raw = sdSignedCall(t, ts, owner, "servicediscovery", "Route53AutoNaming_v20170314.CreateService",
		map[string]any{"Name": "svc", "NamespaceId": list.Namespaces[0].ID})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	svcARN := strings.Split(strings.Split(string(raw), `"Arn":"`)[1], `"`)[0]

	for _, target := range []string{arn, svcARN} {
		code, raw = sdSignedCall(t, ts, other, "tagging", "ResourceGroupsTaggingAPI_20170126.TagResources",
			map[string]any{"ResourceARNList": []string{target}, "Tags": map[string]string{"team": "stolen"}})
		require.Equal(t, http.StatusOK, code, "%s", raw)
		assert.Contains(t, string(raw), target, "the foreign ARN is reported failed: %s", raw)
		code, raw = sdSignedCall(t, ts, other, "tagging", "ResourceGroupsTaggingAPI_20170126.UntagResources",
			map[string]any{"ResourceARNList": []string{target}, "TagKeys": []string{"team"}})
		require.Equal(t, http.StatusOK, code, "%s", raw)
		assert.Contains(t, string(raw), target, "the foreign ARN is reported failed: %s", raw)
	}
	code, raw = sdSignedCall(t, ts, owner, "servicediscovery", "Route53AutoNaming_v20170314.ListTagsForResource",
		map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, code, "%s", raw)
	assert.JSONEq(t, `{"Tags":[{"Key":"team","Value":"a"}]}`, string(raw), "the owner's tags are untouched")
	code, raw = sdSignedCall(t, ts, other, "servicediscovery", "Route53AutoNaming_v20170314.ListTagsForResource",
		map[string]any{"ResourceARN": arn})
	assert.Equal(t, http.StatusNotFound, code, "%s", raw)
}

// TestServiceDiscovery_ListOperationsIsInCreationOrderUnderAFrozenClock pins that operations started
// at one simulated instant list in the order they started, not in their random IDs' order.
func TestServiceDiscovery_ListOperationsIsInCreationOrderUnderAFrozenClock(t *testing.T) {
	p := &emulator.ServiceDiscoveryPlugin{}
	tc := emulator.NewTimeController(time.Unix(1_800_000_000, 500_000_000))
	tc.SetScale(0)
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(slog.LevelError, false), Options: map[string]any{"time_controller": tc}}))
	call := func(id, op string, body any) []byte {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		resp, err := p.HandleRequest(&emulator.RequestContext{RequestID: id, AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint(id)}, &emulator.AWSRequest{Service: "servicediscovery", Operation: op, Body: raw,
			Headers: map[string]string{}, Params: map[string]string{}})
		require.NoError(t, err)
		return resp.Body
	}
	var want []string
	for i, name := range []string{"n0", "n1", "n2", "n3", "n4", "n5"} {
		var out struct {
			OperationID string `json:"OperationId"`
		}
		require.NoError(t, json.Unmarshal(call("req-"+string(rune('a'+i)), "CreateHttpNamespace", map[string]string{"Name": name}), &out))
		want = append(want, out.OperationID)
	}
	var listed struct {
		Operations []struct {
			ID string `json:"Id"`
		} `json:"Operations"`
	}
	require.NoError(t, json.Unmarshal(call("req-list", "ListOperations", map[string]any{}), &listed))
	got := make([]string, 0, len(listed.Operations))
	for _, o := range listed.Operations {
		got = append(got, o.ID)
	}
	assert.Equal(t, want, got)
}

// TestServiceDiscovery_FailSeedNeedsAnErrorCode pins that a FAIL seed must say what failed.
func TestServiceDiscovery_FailSeedNeedsAnErrorCode(t *testing.T) {
	ts := emulator.StartTestServer(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/servicediscovery/operation-status",
		strings.NewReader(`{"operationId":"*","status":"FAIL"}`))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestCFN_ServiceDiscoveryNamespaceReplacementAndFailedDeletes pins a PrivateDnsNamespace
// replacement — refused while the Name is unchanged, carried out with a new Name, the instance
// replaced through its ServiceId — and a stack delete whose instance delete fails reporting it.
func TestCFN_ServiceDiscoveryNamespaceReplacementAndFailedDeletes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(name, vpc string) string {
		return `{"Resources":{
			"A":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.30.0.0/16"}},
			"B":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.31.0.0/16"}},
			"Ns":{"Type":"AWS::ServiceDiscovery::PrivateDnsNamespace","Properties":{"Name":"` + name + `","Vpc":{"Ref":"` + vpc + `"}}},
			"Svc":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"Name":"api","NamespaceId":{"Ref":"Ns"},
				"DnsConfig":{"DnsRecords":[{"Type":"A","TTL":10}]}}},
			"Node":{"Type":"AWS::ServiceDiscovery::Instance","Properties":{"ServiceId":{"Ref":"Svc"},
				"InstanceAttributes":{"AWS_INSTANCE_IPV4":"10.30.0.1"}}}},
			"Outputs":{"NsId":{"Value":{"Ref":"Ns"}},"Node":{"Value":{"Ref":"Node"}}}}`
	}
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "moving", "TemplateBody": tmpl("moving.internal", "A")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "moving")
	require.Equal(t, "CREATE_COMPLETE", status, reason)
	first := cfnStackOutput(t, ts, "moving", "NsId")
	assert.Equal(t, "Node", cfnStackOutput(t, ts, "moving", "Node"), "Ref is the InstanceId")

	// A Vpc change that keeps the Name is refused, as CloudFormation refuses replacing a resource
	// whose name is fixed: the replacement's name is taken. The stack rolls back to the original.
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "moving", "TemplateBody": tmpl("moving.internal", "B")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "moving")
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", status, reason)
	assert.Contains(t, reason, "NamespaceAlreadyExists")
	assert.Equal(t, first, cfnStackOutput(t, ts, "moving", "NsId"))
	sdOK(t, ts, "GetNamespace", map[string]string{"Id": first})

	// With a new Name the replacement goes through: a new namespace, service and instance — the
	// instance keeps its InstanceId and moves to the new service — and the old ones are deleted.
	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "moving", "TemplateBody": tmpl("moved.internal", "B")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "moving")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	second := cfnStackOutput(t, ts, "moving", "NsId")
	assert.NotEqual(t, first, second, "a Name change replaces the namespace")
	assert.Equal(t, "Node", cfnStackOutput(t, ts, "moving", "Node"))
	services := sdOK(t, ts, "ListServices", map[string]any{})["Services"].([]any)
	require.Len(t, services, 1, "the replaced service is deleted, so its instance was deregistered")
	newSvc := services[0].(map[string]any)["Id"].(string)
	sdOK(t, ts, "GetInstance", map[string]string{"ServiceId": newSvc, "InstanceId": "Node"})
	code, doc, raw := sdCall(t, ts, "GetNamespace", map[string]string{"Id": first})
	assert.Equal(t, http.StatusNotFound, code, "the replaced namespace is deleted: %s", raw)
	assert.Equal(t, "NamespaceNotFound", sdErrCode(doc))
	namespaces := sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"].([]any)
	require.Len(t, namespaces, 1)
	assert.Equal(t, second, namespaces[0].(map[string]any)["Id"])

	// Every operation fails from here: the instance's deregistration fails the stack delete, which
	// reports DELETE_FAILED rather than DELETE_COMPLETE, and the instance stays.
	sdSeed(t, ts, http.MethodPost, `{"operationId":"*","status":"FAIL","errorCode":"SEEDED","errorMessage":"no"}`, "")
	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "moving"})
	require.Equal(t, http.StatusOK, code, body)
	status, _ = cfnStackStatusOf(t, ts, "moving")
	assert.Equal(t, "DELETE_FAILED", status)
	assert.Len(t, sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"], 1)

	// Cleared, the delete completes.
	sdSeed(t, ts, http.MethodDelete, "", "")
	code, body = cfnStrictCall(t, ts, "DeleteStack", map[string]string{"StackName": "moving"})
	require.Equal(t, http.StatusOK, code, body)
	code, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "moving"})
	assert.Equal(t, http.StatusBadRequest, code, "the stack deleted cleanly: %s", body)
	assert.Empty(t, sdOK(t, ts, "ListNamespaces", map[string]any{})["Namespaces"])
}

// TestServiceDiscovery_NamespaceNamesAreUniqueInTheAccount pins that a namespace name is refused
// again in another VPC or as another type, because DiscoverInstances finds a namespace by name.
func TestServiceDiscovery_NamespaceNamesAreUniqueInTheAccount(t *testing.T) {
	ts := emulator.StartTestServer(t)
	op := sdOK(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "shared.internal", "Vpc": "vpc-aaaa1111"})["OperationId"].(string)
	done, _ := sdWait(t, ts, op)
	require.Equal(t, "SUCCESS", done["Status"])
	for op, body := range map[string]map[string]any{
		"CreatePrivateDnsNamespace": {"Name": "shared.internal", "Vpc": "vpc-bbbb2222"},
		"CreateHttpNamespace":       {"Name": "shared.internal"},
	} {
		code, doc, _ := sdCall(t, ts, op, body)
		assert.Equal(t, http.StatusBadRequest, code, op)
		assert.Equal(t, "NamespaceAlreadyExists", sdErrCode(doc), op)
	}
}

// TestServiceDiscovery_CreateNamespaceUndoesItsZone pins that a namespace whose record cannot be
// stored deletes the hosted zone it created and leaves no operation, and that a Route 53 failure
// that is not a refusal is the request's failure rather than a FAIL operation.
func TestServiceDiscovery_CreateNamespaceUndoesItsZone(t *testing.T) {
	newEnv := func(namespace, kind string, skip int) (*sdFaultState, *emulator.PluginRegistry, *emulator.RequestContext) {
		state := &sdFaultState{StateManager: emulator.NewMemoryStateManager()}
		tc := emulator.NewTimeController(time.Unix(1_800_000_000, 0))
		logger := emulator.NewDefaultLogger(slog.LevelError, false)
		registry := emulator.NewPluginRegistry()
		for _, p := range []emulator.Plugin{&emulator.Route53Plugin{}, &emulator.ServiceDiscoveryPlugin{}} {
			require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger,
				Options: map[string]any{"time_controller": tc, "registry": registry}}))
			registry.Register(p)
		}
		state.ns, state.fail, state.skip = namespace, kind, skip
		return state, registry, &emulator.RequestContext{RequestID: "req-ns", AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-ns")}
	}
	create := func(registry *emulator.PluginRegistry, ctx *emulator.RequestContext) error {
		_, err := registry.RouteRequest(ctx, &emulator.AWSRequest{Service: "servicediscovery", Operation: "CreatePublicDnsNamespace",
			Body: []byte(`{"Name":"undo.example.com"}`), Headers: map[string]string{}, Params: map[string]string{}})
		return err
	}
	zones := func(registry *emulator.PluginRegistry) string {
		resp, err := registry.RouteRequest(&emulator.RequestContext{RequestID: "req-z", AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-z")}, &emulator.AWSRequest{Service: "route53", Operation: http.MethodGet,
			Path: "/2013-04-01/hostedzone", Headers: map[string]string{}, Params: map[string]string{}})
		require.NoError(t, err)
		return string(resp.Body)
	}
	listOps := func(registry *emulator.PluginRegistry) string {
		resp, err := registry.RouteRequest(&emulator.RequestContext{RequestID: "req-o", AccountID: "123456789012", Region: "us-east-1",
			IDs: emulator.NewIDMint("req-o")}, &emulator.AWSRequest{Service: "servicediscovery", Operation: "ListOperations",
			Body: []byte(`{}`), Headers: map[string]string{}, Params: map[string]string{}})
		require.NoError(t, err)
		return string(resp.Body)
	}

	// The second servicediscovery write is the namespace record (the first is the operation's
	// counter, the second the operation, the third the namespace).
	_, registry, ctx := newEnv("servicediscovery", "put", 2)
	err := create(registry, ctx)
	require.ErrorIs(t, err, errSDInjectedStore)
	assert.NotContains(t, zones(registry), "undo.example.com", "the zone the failed create made is deleted")
	assert.JSONEq(t, `{"Operations":[]}`, listOps(registry), "no operation names the failed create")

	// A Route 53 store failure is an internal error, returned as the request's own.
	_, registry, ctx = newEnv("route53", "put", 0)
	err = create(registry, ctx)
	require.ErrorIs(t, err, errSDInjectedStore)
	var awsErr *emulator.AWSError
	assert.False(t, errors.As(err, &awsErr), "not a FAIL operation: %v", err)
	assert.JSONEq(t, `{"Operations":[]}`, listOps(registry))
}
