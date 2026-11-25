package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// TestCFN_ServiceDiscoveryNamespaceReplacementAndFailedDeletes pins a PrivateDnsNamespace replaced
// by a Vpc change — a new namespace, the old one deleted — and a stack delete whose namespace
// delete fails reporting the failure.
func TestCFN_ServiceDiscoveryNamespaceReplacementAndFailedDeletes(t *testing.T) {
	ts := emulator.StartTestServer(t)
	tmpl := func(vpc string) string {
		return `{"Resources":{
			"A":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.30.0.0/16"}},
			"B":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.31.0.0/16"}},
			"Ns":{"Type":"AWS::ServiceDiscovery::PrivateDnsNamespace","Properties":{"Name":"moving.internal","Vpc":{"Ref":"` + vpc + `"}}},
			"Svc":{"Type":"AWS::ServiceDiscovery::Service","Properties":{"Name":"api","NamespaceId":{"Ref":"Ns"},
				"DnsConfig":{"DnsRecords":[{"Type":"A","TTL":10}]}}},
			"Node":{"Type":"AWS::ServiceDiscovery::Instance","Properties":{"ServiceId":{"Ref":"Svc"},
				"InstanceAttributes":{"AWS_INSTANCE_IPV4":"10.30.0.1"}}}},
			"Outputs":{"NsId":{"Value":{"Ref":"Ns"}}}}`
	}
	code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "moving", "TemplateBody": tmpl("A")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason := cfnStackStatusOf(t, ts, "moving")
	require.Equal(t, "CREATE_COMPLETE", status, reason)
	first := cfnStackOutput(t, ts, "moving", "NsId")

	code, body = cfnStrictCall(t, ts, "UpdateStack", map[string]string{"StackName": "moving", "TemplateBody": tmpl("B")})
	require.Equal(t, http.StatusOK, code, body)
	status, reason = cfnStackStatusOf(t, ts, "moving")
	require.Equal(t, "UPDATE_COMPLETE", status, reason)
	second := cfnStackOutput(t, ts, "moving", "NsId")
	assert.NotEqual(t, first, second, "a Vpc change replaces the namespace")
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

// TestServiceDiscovery_PrivateNamespaceNamesAreUniquePerVPC pins that two private namespaces may
// share a name in different VPCs, and not in one.
func TestServiceDiscovery_PrivateNamespaceNamesAreUniquePerVPC(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for _, vpc := range []string{"vpc-aaaa1111", "vpc-bbbb2222"} {
		op := sdOK(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "shared.internal", "Vpc": vpc})["OperationId"].(string)
		done, _ := sdWait(t, ts, op)
		assert.Equal(t, "SUCCESS", done["Status"])
	}
	code, doc, _ := sdCall(t, ts, "CreatePrivateDnsNamespace", map[string]any{"Name": "shared.internal", "Vpc": "vpc-aaaa1111"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "NamespaceAlreadyExists", sdErrCode(doc))
}
