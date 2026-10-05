package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API reaches API Gateway v2 HTTP and WebSocket APIs, and API Gateway v2
// routes its own TagResource, GetTags and UntagResource (#1378).
//
// Every API here is created through POST /v2/apis rather than seeded into state, so a disagreement
// between the key the v2 plugin writes and the key the tagging resolver builds fails the test — the
// failure #1307 found for REST APIs, which a hand-written fixture key had hidden.

const (
	rgtaV2Account = "123456789012"
	rgtaV2Region  = "us-east-1"
)

type rgtaV2Harness struct {
	t       *testing.T
	state   emulator.StateManager
	v2      *emulator.APIGatewayV2Plugin
	tagging *emulator.TaggingPlugin
	ctx     *emulator.RequestContext
}

func newRGTAV2Harness(t *testing.T, state emulator.StateManager) *rgtaV2Harness {
	t.Helper()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	h := &rgtaV2Harness{
		t: t, state: state,
		v2:      &emulator.APIGatewayV2Plugin{},
		tagging: &emulator.TaggingPlugin{},
		ctx: &emulator.RequestContext{
			AccountID: rgtaV2Account, Region: rgtaV2Region,
			RequestID: "req-rgta-v2", IDs: emulator.NewIDMint("req-rgta-v2"),
		},
	}
	require.NoError(t, h.v2.Initialize(t.Context(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}), "APIGatewayV2Plugin.Initialize")
	require.NoError(t, h.tagging.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger}),
		"TaggingPlugin.Initialize")
	return h
}

// v2Call issues one API Gateway v2 request. multi carries a repeated query key, as the parser
// delivers it.
func (h *rgtaV2Harness) v2Call(method, path string, body any, params map[string]string, multi map[string][]string) (*emulator.AWSResponse, error) {
	h.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(h.t, err, "marshal %s %s", method, path)
	}
	if params == nil {
		params = map[string]string{}
	}
	return h.v2.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "apigatewayv2", HTTPMethod: method, Path: path, Body: raw,
		Headers: map[string]string{"Content-Type": "application/json"},
		Params:  params, MultiValueParams: multi,
	})
}

// createAPI creates an HTTP API through POST /v2/apis and returns its ID.
func (h *rgtaV2Harness) createAPI(name string, tags map[string]string) string {
	h.t.Helper()
	body := map[string]any{"name": name, "protocolType": "HTTP"}
	if tags != nil {
		body["tags"] = tags
	}
	resp, err := h.v2Call(http.MethodPost, "/v2/apis", body, nil, nil)
	require.NoError(h.t, err, "CreateApi")
	require.Equal(h.t, http.StatusCreated, resp.StatusCode, "CreateApi: %s", resp.Body)
	var out struct {
		APIID string `json:"apiId"`
	}
	require.NoError(h.t, json.Unmarshal(resp.Body, &out), "decode CreateApi: %s", resp.Body)
	require.NotEmpty(h.t, out.APIID, "CreateApi answered no apiId: %s", resp.Body)
	return out.APIID
}

func rgtaV2ARN(region, apiID string) string {
	return "arn:aws:apigateway:" + region + "::/apis/" + apiID
}

// getTags reads an API's tags through API Gateway v2's own GetTags, on the raw body.
func (h *rgtaV2Harness) getTags(arn string) map[string]string {
	h.t.Helper()
	resp, err := h.v2Call(http.MethodGet, "/v2/tags/"+arn, nil, nil, nil)
	require.NoError(h.t, err, "GetTags %s", arn)
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "GetTags: %s", resp.Body)
	var out struct {
		Tags map[string]string `json:"tags"`
	}
	require.NoError(h.t, json.Unmarshal(resp.Body, &out), "decode GetTags: %s", resp.Body)
	require.NotNil(h.t, out.Tags, "GetTags must answer a tags member, even empty: %s", resp.Body)
	return out.Tags
}

// rgta runs one Resource Groups Tagging API operation and returns its FailedResourcesMap.
func (h *rgtaV2Harness) rgta(op string, body any) map[string]struct {
	ErrorCode  string `json:"ErrorCode"`
	StatusCode int    `json:"StatusCode"`
} {
	h.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(h.t, err, "marshal %s", op)
	resp, err := h.tagging.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "tagging", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	})
	require.NoError(h.t, err, "tagging %s", op)
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "tagging %s: %s", op, resp.Body)
	var out struct {
		FailedResourcesMap map[string]struct {
			ErrorCode  string `json:"ErrorCode"`
			StatusCode int    `json:"StatusCode"`
		} `json:"FailedResourcesMap"`
	}
	require.NoError(h.t, json.Unmarshal(resp.Body, &out), "decode tagging %s: %s", op, resp.Body)
	return out.FailedResourcesMap
}

// reported runs GetResources with the given type filters and returns the resources it reports, by
// ARN, with their tags as "k=v" pairs; an empty slice is a reported resource with no tags. ok is
// false when GetResources itself fails.
func (h *rgtaV2Harness) reported(filters ...string) (map[string][]string, bool) {
	h.t.Helper()
	body := map[string]any{}
	if len(filters) > 0 {
		body["ResourceTypeFilters"] = filters
	}
	raw, err := json.Marshal(body)
	require.NoError(h.t, err)
	resp, err := h.tagging.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "tagging", Operation: "GetResources", Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126.GetResources", "Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	})
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
			Tags        []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
			} `json:"Tags"`
		} `json:"ResourceTagMappingList"`
	}
	require.NoError(h.t, json.Unmarshal(resp.Body, &out), "decode GetResources: %s", resp.Body)
	got := map[string][]string{}
	for _, rm := range out.ResourceTagMappingList {
		pairs := []string{}
		for _, tag := range rm.Tags {
			pairs = append(pairs, tag.Key+"="+tag.Value)
		}
		got[rm.ResourceARN] = pairs
	}
	return got, true
}

func TestTaggingAPIGatewayV2_TagResourcesIsReadBackThroughGetTags(t *testing.T) {
	t.Parallel()
	h := newRGTAV2Harness(t, emulator.NewMemoryStateManager())
	id := h.createAPI("rgta-v2", nil)
	arn := rgtaV2ARN(rgtaV2Region, id)

	require.Empty(t, h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"team": "edge"}}),
		"TagResources must reach an API created through POST /v2/apis")
	assert.Equal(t, map[string]string{"team": "edge"}, h.getTags(arn), "API Gateway v2's GetTags reads what the tagging API wrote")

	// And the other direction: API Gateway v2's own TagResource is visible to GetResources.
	resp, err := h.v2Call(http.MethodPost, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{"env": "prod"}}, nil, nil)
	require.NoError(t, err, "TagResource")
	assert.Equal(t, http.StatusCreated, resp.StatusCode, "TagResource answers 201: %s", resp.Body)
	assert.Empty(t, resp.Body, "TagResource answers no body, as published")

	got, ok := h.reported()
	require.True(t, ok, "GetResources")
	assert.ElementsMatch(t, []string{"team=edge", "env=prod"}, got[arn], "GetResources reports the API under its /apis ARN: %v", got)
}

func TestTaggingAPIGatewayV2_GetResourcesFollowsTheEverTaggedRule(t *testing.T) {
	t.Parallel()
	h := newRGTAV2Harness(t, emulator.NewMemoryStateManager())
	never := h.createAPI("never-tagged", nil)
	tagged := h.createAPI("tagged", nil)
	taggedARN := rgtaV2ARN(rgtaV2Region, tagged)
	createdWithTags := h.createAPI("created-with-tags", map[string]string{"born": "yes"})

	require.Empty(t, h.rgta("TagResources", map[string]any{"ResourceARNList": []string{taggedARN}, "Tags": map[string]string{"a": "b"}}))

	for _, filters := range [][]string{nil, {"apigateway"}} {
		got, ok := h.reported(filters...)
		require.True(t, ok, "GetResources %v", filters)
		assert.Equal(t, []string{"a=b"}, got[taggedARN], "filter %v: a tagged API is reported", filters)
		assert.Equal(t, []string{"born=yes"}, got[rgtaV2ARN(rgtaV2Region, createdWithTags)], "filter %v: an API created with tags is reported", filters)
		_, found := got[rgtaV2ARN(rgtaV2Region, never)]
		assert.False(t, found, "filter %v: an API never tagged is not reported", filters)
	}

	// Untagging every key leaves the API reported with an empty set: it has carried a tag (#938).
	require.Empty(t, h.rgta("UntagResources", map[string]any{"ResourceARNList": []string{taggedARN}, "TagKeys": []string{"a"}}))
	assert.Empty(t, h.getTags(taggedARN), "UntagResources removed the tag")
	got, ok := h.reported("apigateway")
	require.True(t, ok)
	pairs, found := got[taggedARN]
	assert.True(t, found, "an API whose tags were all removed is still reported: %v", got)
	assert.Empty(t, pairs, "and reported with no tags")

	// The same holds when API Gateway v2's own UntagResource empties a set created inline.
	resp, err := h.v2Call(http.MethodDelete, "/v2/tags/"+rgtaV2ARN(rgtaV2Region, createdWithTags), nil, map[string]string{"tagKeys": "born"}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "UntagResource answers 204: %s", resp.Body)
	got, ok = h.reported()
	require.True(t, ok)
	_, found = got[rgtaV2ARN(rgtaV2Region, createdWithTags)]
	assert.True(t, found, "an API created with tags and then untagged is still reported")
}

func TestTaggingAPIGatewayV2_DeleteApiRemovesTheTagHistory(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	h := newRGTAV2Harness(t, state)
	id := h.createAPI("deleted", nil)
	arn := rgtaV2ARN(rgtaV2Region, id)
	require.Empty(t, h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"a": "b"}}))

	sidecar := "apiv2_tagged:" + rgtaV2Account + "/" + rgtaV2Region + "/" + id
	raw, err := state.Get(context.Background(), "apigatewayv2", sidecar)
	require.NoError(t, err)
	require.NotNil(t, raw, "a tag write records the API's tag history")

	resp, err := h.v2Call(http.MethodDelete, "/v2/apis/"+id, nil, nil, nil)
	require.NoError(t, err, "DeleteApi")
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "DeleteApi: %s", resp.Body)

	raw, err = state.Get(context.Background(), "apigatewayv2", sidecar)
	require.NoError(t, err)
	assert.Nil(t, raw, "DeleteApi removes the tag history with the API")
	got, ok := h.reported()
	require.True(t, ok)
	_, found := got[arn]
	assert.False(t, found, "a deleted API is not reported")
}

// The ARN's account segment is empty by format, so the account is the caller's; a Region or an
// account the ARN does name is honored, reaching that scope's API or none — never the caller's.
func TestTaggingAPIGatewayV2_AnARNReachesOnlyTheScopeItNames(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	h := newRGTAV2Harness(t, state)
	id := h.createAPI("scoped", map[string]string{"own": "1"})

	for _, tc := range []struct {
		name string
		arn  string
		key  string
	}{
		{"another Region", rgtaV2ARN("us-west-2", id), "apiv2:" + rgtaV2Account + "/us-west-2/" + id},
		{"an explicit other account", "arn:aws:apigateway:" + rgtaV2Region + ":999999999999:/apis/" + id, "apiv2:999999999999/" + rgtaV2Region + "/" + id},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failed := h.rgta("TagResources", map[string]any{"ResourceARNList": []string{tc.arn}, "Tags": map[string]string{"intruder": "x"}})
			assert.Contains(t, failed, tc.arn, "an ARN naming another scope fails rather than reaching the caller's API")
			raw, err := state.Get(context.Background(), "apigatewayv2", tc.key)
			require.NoError(t, err)
			assert.Nil(t, raw, "and writes no phantom record at %s", tc.key)
			assert.Equal(t, map[string]string{"own": "1"}, h.getTags(rgtaV2ARN(rgtaV2Region, id)), "the caller's API is untouched")
		})
	}
}

func TestAPIGatewayV2Tags_RefusalsArePublished(t *testing.T) {
	t.Parallel()
	h := newRGTAV2Harness(t, emulator.NewMemoryStateManager())
	id := h.createAPI("refusals", nil)
	arn := rgtaV2ARN(rgtaV2Region, id)

	assert.Equal(t, map[string]string{}, h.getTags(arn), "an untagged API answers an empty tags map")

	for _, tc := range []struct {
		name, method, path string
		body               any
		params             map[string]string
		code               string
		status             int
	}{
		{"UntagResource without tagKeys", http.MethodDelete, "/v2/tags/" + arn, nil, nil, "BadRequestException", http.StatusBadRequest},
		{"GetTags on an API that does not exist", http.MethodGet, "/v2/tags/" + rgtaV2ARN(rgtaV2Region, "nope000000"), nil, nil, "NotFoundException", http.StatusNotFound},
		{"TagResource on an API that does not exist", http.MethodPost, "/v2/tags/" + rgtaV2ARN(rgtaV2Region, "nope000000"), map[string]any{"tags": map[string]string{"a": "b"}}, nil, "NotFoundException", http.StatusNotFound},
		{"a stage ARN, which substrate stores no tags on", http.MethodGet, "/v2/tags/arn:aws:apigateway:" + rgtaV2Region + "::/apis/" + id + "/stages/prod", nil, nil, "BadRequestException", http.StatusBadRequest},
		{"an ARN that is not API Gateway's", http.MethodGet, "/v2/tags/arn:aws:lambda:" + rgtaV2Region + ":" + rgtaV2Account + ":function:f", nil, nil, "BadRequestException", http.StatusBadRequest},
		{"a malformed body", http.MethodPost, "/v2/tags/" + arn, json.RawMessage(`{"tags":`), nil, "BadRequestException", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var resp *emulator.AWSResponse
			var err error
			if raw, ok := tc.body.(json.RawMessage); ok {
				resp, err = h.v2.HandleRequest(h.ctx, &emulator.AWSRequest{
					Service: "apigatewayv2", HTTPMethod: tc.method, Path: tc.path, Body: raw,
					Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
				})
			} else {
				resp, err = h.v2Call(tc.method, tc.path, tc.body, tc.params, nil)
			}
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s must be refused, answered %v %v", tc.name, resp, err)
			assert.Equal(t, tc.code, awsErr.Code, tc.name)
			assert.Equal(t, tc.status, awsErr.HTTPStatus, tc.name)
		})
	}

	// A repeated tagKeys removes every key named.
	resp, err := h.v2Call(http.MethodPost, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{"a": "1", "b": "2", "c": "3"}}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp, err = h.v2Call(http.MethodDelete, "/v2/tags/"+arn, nil, map[string]string{"tagKeys": "a"}, map[string][]string{"tagKeys": {"a", "b"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, map[string]string{"c": "3"}, h.getTags(arn), "a repeated tagKeys removes each named key")
}

// A store fault at any read or write the new paths make is an error, never a success over a record
// that was not written or a resource reported as if it had no history.
func TestTaggingAPIGatewayV2_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		run  func(h *rgtaV2Harness, arn, id string) bool // reports whether the request failed
	}{
		{"TagResources, record write", func(m *cfFaultStateManager) { m.failPut = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"TagResources, history write", func(m *cfFaultStateManager) { m.failPut = "apiv2_tagged:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"TagResources, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"v2 TagResource, record read", func(m *cfFaultStateManager) { m.failGet = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			_, err := h.v2Call(http.MethodPost, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{"a": "b"}}, nil, nil)
			return err != nil
		}},
		{"v2 TagResource, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			_, err := h.v2Call(http.MethodPost, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{"a": "b"}}, nil, nil)
			return err != nil
		}},
		{"v2 TagResource, record write", func(m *cfFaultStateManager) { m.failPut = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			_, err := h.v2Call(http.MethodPost, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{"a": "b"}}, nil, nil)
			return err != nil
		}},
		{"v2 UntagResource, history write", func(m *cfFaultStateManager) { m.failPut = "apiv2_tagged:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			_, err := h.v2Call(http.MethodDelete, "/v2/tags/"+arn, nil, map[string]string{"tagKeys": "seed"}, nil)
			return err != nil
		}},
		{"DeleteApi, history delete", func(m *cfFaultStateManager) { m.failDelete = "apiv2_tagged:" }, func(h *rgtaV2Harness, _, id string) bool {
			_, err := h.v2Call(http.MethodDelete, "/v2/apis/"+id, nil, nil, nil)
			return err != nil
		}},
		{"GetResources, record read", func(m *cfFaultStateManager) { m.failGet = "apiv2:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			got, ok := h.reported("apigateway")
			_, found := got[arn]
			return !ok || !found
		}},
		{"GetResources, history read", func(m *cfFaultStateManager) { m.failGet = "apiv2_tagged:" }, func(h *rgtaV2Harness, arn, _ string) bool {
			got, ok := h.reported("apigateway")
			_, found := got[arn]
			return !ok || !found
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newRGTAV2Harness(t, fault)
			id := h.createAPI("fault", map[string]string{"seed": "1"})
			arn := rgtaV2ARN(rgtaV2Region, id)

			tc.arm(fault)
			assert.True(t, tc.run(h, arn, id), "%s must fail on a store fault rather than succeed", tc.name)
		})
	}
}
