package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Lambda layers, and another account's public layer seen from outside (#1272).
//
// The owner account is wireSetup's 123456789012. A second request context in another account plays
// the consumer that resolves the owner's layer by ARN, which is the case the issue is about: a public
// layer's policy grants only lambda:GetLayerVersion, so ListLayerVersions is refused, and a version
// the policy does not grant, including one that was never published, is AccessDeniedException rather
// than ResourceNotFoundException.

const (
	layerOwner    = "123456789012"
	layerConsumer = "210987654321"
)

// layerHarness drives the Lambda plugin directly, as the owner or as the consumer.
type layerHarness struct {
	t        *testing.T
	p        *emulator.LambdaPlugin
	owner    *emulator.RequestContext
	consumer *emulator.RequestContext
}

func newLayerHarness(t *testing.T, state emulator.StateManager) *layerHarness {
	t.Helper()
	p := &emulator.LambdaPlugin{}
	if state == nil {
		state = emulator.NewMemoryStateManager()
	}
	clock := time.Date(2026, 10, 4, 12, 0, 0, 123_000_000, time.UTC)
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	ctx := func(account, id string) *emulator.RequestContext {
		return &emulator.RequestContext{AccountID: account, Region: "us-east-1", RequestID: id, IDs: emulator.NewIDMint(id)}
	}
	return &layerHarness{t: t, p: p, owner: ctx(layerOwner, "req-layer-owner"), consumer: ctx(layerConsumer, "req-layer-consumer")}
}

// call issues one request and answers its status and body, or the refusal's status, code and message.
func (h *layerHarness) call(ctx *emulator.RequestContext, method, path string, params map[string]string, body any) (int, string, string) {
	h.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(h.t, err)
	}
	if params == nil {
		params = map[string]string{}
	}
	resp, err := h.p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "lambda", HTTPMethod: method, Path: path, Body: raw, Params: params,
		Headers: map[string]string{"Content-Type": "application/json"},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code, awsErr.Message
	}
	require.NoError(h.t, err, "%s %s", method, path)
	return resp.StatusCode, string(resp.Body), ""
}

func (h *layerHarness) publish(name string, extra map[string]any) map[string]any {
	h.t.Helper()
	body := map[string]any{"Content": map[string]string{"ZipFile": base64.StdEncoding.EncodeToString([]byte("layer:" + name))}}
	for k, v := range extra {
		body[k] = v
	}
	status, out, _ := h.call(h.owner, http.MethodPost, "/2018-10-31/layers/"+name+"/versions", nil, body)
	require.Equal(h.t, http.StatusCreated, status, "PublishLayerVersion: %s", out)
	var doc map[string]any
	require.NoError(h.t, json.Unmarshal([]byte(out), &doc))
	return doc
}

func (h *layerHarness) grant(name string, version int, sid, principal, org string) {
	h.t.Helper()
	body := map[string]string{"Action": "lambda:GetLayerVersion", "Principal": principal, "StatementId": sid}
	if org != "" {
		body["OrganizationId"] = org
	}
	status, out, _ := h.call(h.owner, http.MethodPost, "/2018-10-31/layers/"+name+"/versions/"+strconv.Itoa(version)+"/policy", nil, body)
	require.Equal(h.t, http.StatusCreated, status, "AddLayerVersionPermission: %s", out)
}

func layerARN(account, name string) string {
	return "arn:aws:lambda:us-east-1:" + account + ":layer:" + name
}

func TestLambdaLayers_PublishGetListAndDeleteInTheOwningAccount(t *testing.T) {
	t.Parallel()
	h := newLayerHarness(t, nil)

	v1 := h.publish("lib", map[string]any{"CompatibleRuntimes": []string{"python3.12"}, "CompatibleArchitectures": []string{"arm64"}, "Description": "one"})
	assert.EqualValues(t, 1, v1["Version"])
	assert.Equal(t, layerARN(layerOwner, "lib"), v1["LayerArn"])
	assert.Equal(t, layerARN(layerOwner, "lib")+":1", v1["LayerVersionArn"])
	assert.Equal(t, "2026-10-04T12:00:00.123+0000", v1["CreatedDate"], "CreatedDate is the published ISO-8601 string")
	content := v1["Content"].(map[string]any)
	assert.EqualValues(t, len("layer:lib"), content["CodeSize"])
	assert.NotEmpty(t, content["CodeSha256"])
	assert.NotEmpty(t, content["Location"])

	v2 := h.publish("lib", map[string]any{"CompatibleRuntimes": []string{"nodejs22.x"}})
	assert.EqualValues(t, 2, v2["Version"])

	status, body, _ := h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions", nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	var list struct {
		LayerVersions []struct{ Version int } `json:"LayerVersions"`
		NextMarker    string                  `json:"NextMarker"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &list))
	require.Len(t, list.LayerVersions, 2)
	assert.Equal(t, 2, list.LayerVersions[0].Version, "newest first")

	// Filters and pagination.
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions", map[string]string{"CompatibleRuntime": "python3.12"}, nil)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, json.Unmarshal([]byte(body), &list))
	require.Len(t, list.LayerVersions, 1)
	assert.Equal(t, 1, list.LayerVersions[0].Version)
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions", map[string]string{"MaxItems": "1"}, nil)
	require.Equal(t, http.StatusOK, status)
	require.NoError(t, json.Unmarshal([]byte(body), &list))
	require.Len(t, list.LayerVersions, 1)
	require.Equal(t, "1", list.NextMarker)
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions", map[string]string{"MaxItems": "1", "Marker": list.NextMarker}, nil)
	require.Equal(t, http.StatusOK, status)
	list.NextMarker = ""
	require.NoError(t, json.Unmarshal([]byte(body), &list))
	require.Len(t, list.LayerVersions, 1)
	assert.Equal(t, 1, list.LayerVersions[0].Version)
	assert.Empty(t, list.NextMarker)

	// ListLayers reports the latest matching version.
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers", map[string]string{"CompatibleArchitecture": "arm64"}, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"LayerName":"lib"`)
	assert.Contains(t, body, `"LayerVersionArn":"`+layerARN(layerOwner, "lib")+`:1"`)

	// Get by name and by ARN.
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions/2", nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"Version":2`)
	status, body, _ = h.call(h.owner, http.MethodGet, "/2018-10-31/layers",
		map[string]string{"find": "LayerVersion", "Arn": layerARN(layerOwner, "lib") + ":1"}, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"Description":"one"`)

	// Delete; the owner sees ResourceNotFoundException; the number is never reused.
	status, _, _ = h.call(h.owner, http.MethodDelete, "/2018-10-31/layers/lib/versions/2", nil, nil)
	require.Equal(t, http.StatusNoContent, status)
	status, code, _ := h.call(h.owner, http.MethodGet, "/2018-10-31/layers/lib/versions/2", nil, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFoundException", code)
	status, _, _ = h.call(h.owner, http.MethodDelete, "/2018-10-31/layers/lib/versions/2", nil, nil)
	assert.Equal(t, http.StatusNoContent, status, "DeleteLayerVersion publishes no not-found refusal")
	assert.EqualValues(t, 3, h.publish("lib", nil)["Version"], "a deleted version's number is not reused")
}

func TestLambdaLayers_TheVersionPolicy(t *testing.T) {
	t.Parallel()
	h := newLayerHarness(t, nil)
	h.publish("lib", nil)
	policy := "/2018-10-31/layers/lib/versions/1/policy"

	status, code, _ := h.call(h.owner, http.MethodGet, policy, nil, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFoundException", code, "a version with no statement has no policy")

	status, body, _ := h.call(h.owner, http.MethodPost, policy, nil,
		map[string]string{"Action": "lambda:GetLayerVersion", "Principal": layerConsumer, "StatementId": "xaccount"})
	require.Equal(t, http.StatusCreated, status, body)
	var added struct{ Statement, RevisionId string }
	require.NoError(t, json.Unmarshal([]byte(body), &added))
	var stmt map[string]any
	require.NoError(t, json.Unmarshal([]byte(added.Statement), &stmt), "Statement is a JSON document in a string")
	assert.Equal(t, map[string]any{"AWS": "arn:aws:iam::" + layerConsumer + ":root"}, stmt["Principal"])
	assert.Equal(t, layerARN(layerOwner, "lib")+":1", stmt["Resource"])
	require.NotEmpty(t, added.RevisionId)

	status, body, _ = h.call(h.owner, http.MethodGet, policy, nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	var got struct{ Policy, RevisionId string }
	require.NoError(t, json.Unmarshal([]byte(body), &got))
	assert.Equal(t, added.RevisionId, got.RevisionId)
	assert.Contains(t, got.Policy, `"Sid":"xaccount"`)

	for _, tc := range []struct {
		name   string
		params map[string]string
		body   map[string]string
		status int
		code   string
	}{
		{"a duplicate statement ID", nil, map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "*", "StatementId": "xaccount"}, http.StatusConflict, "ResourceConflictException"},
		{"an action the pattern does not allow", nil, map[string]string{"Action": "lambda:ListLayerVersions", "Principal": "*", "StatementId": "list"}, http.StatusBadRequest, "InvalidParameterValueException"},
		{"a principal outside the pattern", nil, map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "alice", "StatementId": "p"}, http.StatusBadRequest, "InvalidParameterValueException"},
		{"an organization ID outside the pattern", nil, map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "*", "StatementId": "o", "OrganizationId": "org-1"}, http.StatusBadRequest, "InvalidParameterValueException"},
		{"a stale revision", map[string]string{"RevisionId": "stale"}, map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "*", "StatementId": "public"}, http.StatusPreconditionFailed, "PreconditionFailedException"},
	} {
		status, code, _ := h.call(h.owner, http.MethodPost, policy, tc.params, tc.body)
		assert.Equalf(t, tc.status, status, "%s", tc.name)
		assert.Equalf(t, tc.code, code, "%s", tc.name)
	}

	status, code, _ = h.call(h.owner, http.MethodDelete, policy+"/missing", nil, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFoundException", code)
	status, _, _ = h.call(h.owner, http.MethodDelete, policy+"/xaccount", map[string]string{"RevisionId": got.RevisionId}, nil)
	assert.Equal(t, http.StatusNoContent, status)
	status, _, _ = h.call(h.owner, http.MethodGet, policy, nil, nil)
	assert.Equal(t, http.StatusNotFound, status, "the last statement removed leaves no policy")
}

func TestLambdaLayers_AnotherAccountSeesOnlyWhatThePolicyGrants(t *testing.T) {
	t.Parallel()
	h := newLayerHarness(t, nil)
	h.publish("adapter", nil)
	h.publish("adapter", nil)
	h.publish("adapter", nil)
	h.grant("adapter", 1, "public", "*", "")
	h.grant("adapter", 2, "consumer", layerConsumer, "")
	h.grant("adapter", 3, "org", "*", "o-abcdefghij")
	arn := layerARN(layerOwner, "adapter")
	versions := "/2018-10-31/layers/" + arn + "/versions"

	status, code, msg := h.call(h.consumer, http.MethodGet, versions, nil, nil)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Equal(t, "AccessDeniedException", code)
	assert.Equal(t, "User: arn:aws:iam::"+layerConsumer+":root is not authorized to perform: lambda:ListLayerVersions on resource: "+
		arn+" because no resource-based policy allows the lambda:ListLayerVersions action", msg,
		"the observed refusal: a public layer's policy grants only GetLayerVersion")

	for _, tc := range []struct {
		version int
		status  int
		why     string
	}{
		{1, http.StatusOK, "granted to *"},
		{2, http.StatusOK, "granted to the consumer's account"},
		{3, http.StatusForbidden, "granted to an organization substrate cannot resolve"},
		{4, http.StatusForbidden, "never published: absent and forbidden are one answer"},
	} {
		status, code, _ := h.call(h.consumer, http.MethodGet, versions+"/"+strconv.Itoa(tc.version), nil, nil)
		assert.Equalf(t, tc.status, status, "version %d, %s", tc.version, tc.why)
		if tc.status != http.StatusOK {
			assert.Equalf(t, "AccessDeniedException", code, "version %d is AccessDeniedException, not ResourceNotFoundException", tc.version)
		}
		status, _, _ = h.call(h.consumer, http.MethodGet, "/2018-10-31/layers",
			map[string]string{"find": "LayerVersion", "Arn": arn + ":" + strconv.Itoa(tc.version)}, nil)
		assert.Equalf(t, tc.status, status, "GetLayerVersionByArn, version %d", tc.version)
	}

	// A version granted to a third account is not granted to the consumer.
	h.grant("adapter", 3, "third", "111122223333", "")
	status, _, _ = h.call(h.consumer, http.MethodGet, versions+"/3", nil, nil)
	assert.Equal(t, http.StatusForbidden, status)

	// No policy can grant the rest, so another account is always refused.
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, versions, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}},
		{http.MethodDelete, versions + "/1", nil},
		{http.MethodGet, versions + "/1/policy", nil},
		{http.MethodPost, versions + "/1/policy", map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "*", "StatementId": "x"}},
		{http.MethodDelete, versions + "/1/policy/public", nil},
	} {
		status, code, _ := h.call(h.consumer, tc.method, tc.path, nil, tc.body)
		assert.Equalf(t, http.StatusForbidden, status, "%s %s", tc.method, tc.path)
		assert.Equalf(t, "AccessDeniedException", code, "%s %s", tc.method, tc.path)
	}

	// The owner's own reads are unaffected.
	status, _, _ = h.call(h.owner, http.MethodGet, versions, nil, nil)
	assert.Equal(t, http.StatusOK, status)
}

func TestLambdaLayers_ASeededPublicLayerDrivesAVersionProbe(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	h := newLayerHarness(t, state)
	srv := emulator.NewServer(*emulator.DefaultConfig(), emulator.NewPluginRegistry(),
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false}), state,
		emulator.NewTimeController(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)), emulator.NewDefaultLogger(slog.LevelError, false))

	const lwa = "arn:aws:lambda:us-east-1:753240598075:layer:LambdaAdapterLayerArm64"
	seed := func(method, query, body string) int {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), method, "/v1/lambda/layer-versions"+query, strings.NewReader(body)))
		return w.Code
	}
	require.Equal(t, http.StatusOK, seed(http.MethodPost, "", `{"layerArn":"`+lwa+`","publishedVersions":30}`))
	assert.Equal(t, http.StatusBadRequest, seed(http.MethodPost, "", `{"layerArn":"not-an-arn","publishedVersions":3}`))

	get := func(n int) int {
		status, _, _ := h.call(h.consumer, http.MethodGet, "/2018-10-31/layers/"+lwa+"/versions/"+strconv.Itoa(n), nil, nil)
		return status
	}
	// The probe the issue describes: double upward, then binary-search the boundary.
	hi := 1
	for get(hi) == http.StatusOK {
		hi *= 2
	}
	lo := hi / 2
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if get(mid) == http.StatusOK {
			lo = mid
		} else {
			hi = mid
		}
	}
	assert.Equal(t, 30, lo, "the probe finds the seeded newest version")
	assert.Equal(t, http.StatusForbidden, get(31))

	status, code, _ := h.call(h.consumer, http.MethodGet, "/2018-10-31/layers/"+lwa+"/versions", nil, nil)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Equal(t, "AccessDeniedException", code)

	require.Equal(t, http.StatusOK, seed(http.MethodDelete, "?layerArn="+lwa, ""))
	assert.Equal(t, http.StatusForbidden, get(1), "a cleared seed leaves no version")
}

func TestLambdaLayers_RefusesWhatThePagesBound(t *testing.T) {
	t.Parallel()
	h := newLayerHarness(t, nil)
	h.publish("lib", nil)
	for _, tc := range []struct {
		name         string
		method, path string
		params       map[string]string
		body         any
	}{
		{"a layer name outside the pattern", http.MethodGet, "/2018-10-31/layers/bad!name/versions/1", nil, nil},
		{"a version that is not a number", http.MethodGet, "/2018-10-31/layers/lib/versions/latest", nil, nil},
		{"a version of zero", http.MethodGet, "/2018-10-31/layers/lib/versions/0", nil, nil},
		{"MaxItems of zero", http.MethodGet, "/2018-10-31/layers/lib/versions", map[string]string{"MaxItems": "0"}, nil},
		{"MaxItems above 50", http.MethodGet, "/2018-10-31/layers", map[string]string{"MaxItems": "51"}, nil},
		{"an unpublished architecture filter", http.MethodGet, "/2018-10-31/layers", map[string]string{"CompatibleArchitecture": "sparc"}, nil},
		{"a marker never issued", http.MethodGet, "/2018-10-31/layers/lib/versions", map[string]string{"Marker": "99"}, nil},
		{"a version ARN outside the pattern", http.MethodGet, "/2018-10-31/layers", map[string]string{"find": "LayerVersion", "Arn": "arn:aws:lambda:us-east-1:123456789012:layer:lib"}, nil},
		{"no Content", http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{}},
		{"Content naming nothing", http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{}}},
		{"an unpublished architecture", http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}, "CompatibleArchitectures": []string{"sparc"}}},
	} {
		status, code, _ := h.call(h.owner, tc.method, tc.path, tc.params, tc.body)
		assert.Equalf(t, http.StatusBadRequest, status, "%s", tc.name)
		assert.Equalf(t, "InvalidParameterValueException", code, "%s", tc.name)
	}
}

// A store fault in any layer read or write is an error, never a refusal or a success.
func TestLambdaLayers_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		arm          func(*cfFaultStateManager)
		method, path string
		params       map[string]string
		body         any
		consumer     bool
	}{
		{"Publish, version write", func(m *cfFaultStateManager) { m.failPut = "layerversion:" }, http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}, false},
		{"Publish, layer write", func(m *cfFaultStateManager) { m.failPut = "layer:" }, http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}, false},
		{"Publish, layer read", func(m *cfFaultStateManager) { m.failGet = "layer:" }, http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}, false},
		{"Publish, corrupt layer", func(m *cfFaultStateManager) { m.corruptGet = "layer:" }, http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}, false},
		{"Publish, seed read", func(m *cfFaultStateManager) { m.failGet = "layer-versions:" }, http.MethodPost, "/2018-10-31/layers/lib/versions", nil, map[string]any{"Content": map[string]string{"ZipFile": "eA=="}}, false},
		{"Get, version read", func(m *cfFaultStateManager) { m.failGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/lib/versions/1", nil, nil, false},
		{"Get, corrupt version", func(m *cfFaultStateManager) { m.corruptGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/lib/versions/1", nil, nil, false},
		{"Get, seed read for an absent version", func(m *cfFaultStateManager) { m.failGet = "layer-versions:" }, http.MethodGet, "/2018-10-31/layers/lib/versions/9", nil, nil, false},
		{"Get, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "layer-versions:" }, http.MethodGet, "/2018-10-31/layers/lib/versions/9", nil, nil, false},
		{"List, version read", func(m *cfFaultStateManager) { m.failGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/lib/versions", nil, nil, false},
		{"List, corrupt version", func(m *cfFaultStateManager) { m.corruptGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/lib/versions", nil, nil, false},
		{"List, seed read", func(m *cfFaultStateManager) { m.failGet = "layer-versions:" }, http.MethodGet, "/2018-10-31/layers/lib/versions", nil, nil, false},
		{"ListLayers, version read", func(m *cfFaultStateManager) { m.failGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers", nil, nil, false},
		{"Delete", func(m *cfFaultStateManager) { m.failDelete = "layerversion:" }, http.MethodDelete, "/2018-10-31/layers/lib/versions/1", nil, nil, false},
		{"AddPermission, write", func(m *cfFaultStateManager) { m.failPut = "layerversion:" }, http.MethodPost, "/2018-10-31/layers/lib/versions/1/policy", nil, map[string]string{"Action": "lambda:GetLayerVersion", "Principal": "*", "StatementId": "s"}, false},
		{"RemovePermission, write", func(m *cfFaultStateManager) { m.failPut = "layerversion:" }, http.MethodDelete, "/2018-10-31/layers/lib/versions/1/policy/public", nil, nil, false},
		{"GetPolicy, read", func(m *cfFaultStateManager) { m.failGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/lib/versions/1/policy", nil, nil, false},
		{"cross-account Get, read", func(m *cfFaultStateManager) { m.failGet = "layerversion:" }, http.MethodGet, "/2018-10-31/layers/" + layerARN(layerOwner, "lib") + "/versions/1", nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newLayerHarness(t, fault)
			h.publish("lib", nil)
			h.grant("lib", 1, "public", "*", "")
			tc.arm(fault)
			ctx := h.owner
			if tc.consumer {
				ctx = h.consumer
			}
			var raw []byte
			if tc.body != nil {
				var err error
				raw, err = json.Marshal(tc.body)
				require.NoError(t, err)
			}
			params := tc.params
			if params == nil {
				params = map[string]string{}
			}
			_, err := h.p.HandleRequest(ctx, &emulator.AWSRequest{Service: "lambda", HTTPMethod: tc.method, Path: tc.path, Body: raw, Params: params})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
