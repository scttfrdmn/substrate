package emulator_test

// CloudWatch Logs tagging (#1273). The trio was absent, so these tests are the first to exercise
// it at all, and the rule they exist for is narrow: the ARN a caller most naturally reuses — the
// `:*`-suffixed policy form that DescribeLogGroups itself reports under `arn` — is the one the
// operations refuse.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

const (
	// cwlTagGroup is the log group these tests tag. Named after a Lambda function because that is
	// the case the issue describes: a group created explicitly so retention is set from the
	// start, then tagged so a teardown sweep can find it.
	cwlTagGroup = "/aws/lambda/foray-gateway"

	// cwlTagGroupARN is the form the API accepts — the bare ARN, no trailing ":*".
	cwlTagGroupARN = "arn:aws:logs:us-east-1:123456789012:log-group:" + cwlTagGroup

	// cwlTagGroupPolicyARN is the IAM-policy form, which matches the group's streams and which
	// these operations refuse.
	cwlTagGroupPolicyARN = cwlTagGroupARN + ":*"
)

// cwlCreateTaggedGroup creates cwlTagGroup, optionally with inline tags, and fails the test if the
// create is refused.
func cwlCreateTaggedGroup(t *testing.T, srv *emulator.Server, tags map[string]string) {
	t.Helper()
	body := map[string]any{"logGroupName": cwlTagGroup}
	if tags != nil {
		body["tags"] = tags
	}
	resp := cwLogsRequest(t, srv, "CreateLogGroup", body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// cwlListTags reads a group's tags through ListTagsForResource, requiring a 200.
func cwlListTags(t *testing.T, srv *emulator.Server, resourceARN string) map[string]string {
	t.Helper()
	resp := cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": resourceARN})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		Tags map[string]string `json:"tags"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &got))
	return got.Tags
}

// cwLogsRawRequest is cwLogsRequest over a body that is not a marshaled value — for the one case
// that needs to send bytes no marshal would produce.
func cwLogsRawRequest(t *testing.T, srv *emulator.Server, op string, body []byte) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	r.Host = "logs.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "Logs_20140328."+op)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=fake")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// cwlTag calls TagResource and hands back the response for the caller to judge.
func cwlTag(t *testing.T, srv *emulator.Server, resourceARN string, tags map[string]string) *http.Response {
	t.Helper()
	return cwLogsRequest(t, srv, "TagResource", map[string]any{"resourceArn": resourceARN, "tags": tags})
}

func TestCWLogsTags_ATaggedGroupReportsWhatItWasTagged(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, nil)

	assert.Empty(t, cwlListTags(t, srv, cwlTagGroupARN),
		"a group that was never tagged reports no tags")

	resp := cwlTag(t, srv, cwlTagGroupARN, map[string]string{"Project": "foray", "Env": "dev"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, map[string]string{"Project": "foray", "Env": "dev"}, cwlListTags(t, srv, cwlTagGroupARN))
}

// The response's `tags` is a JSON **object**, not an array of {key, value} pairs the way most
// services render tags. An SDK matches members against the model case-sensitively and a shape
// mismatch parses to nothing rather than erroring (#528), so the wire shape is asserted on the raw
// body and not only through a decode into the shape the test wants.
func TestCWLogsTags_TagsAreAMapOnTheWire(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, nil)
	require.Equal(t, http.StatusOK, cwlTag(t, srv, cwlTagGroupARN, map[string]string{"Project": "foray"}).StatusCode)

	resp := cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": cwlTagGroupARN})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"tags":{"Project":"foray"}}`, string(cwLogsReadBody(t, resp)))
}

// An untagged group answers an empty object rather than omitting the member: a caller converging
// tags compares the map it reads, and an absent member makes that a nil-versus-empty distinction
// the API never draws.
func TestCWLogsTags_AnUntaggedGroupAnswersAnEmptyMap(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, nil)

	resp := cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": cwlTagGroupARN})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"tags":{}}`, string(cwLogsReadBody(t, resp)))
}

// The half of the convergence path that looked like it worked: CreateLogGroup decoded `tags` and
// dropped them, so a group created with tags inline read back untagged.
func TestCWLogsTags_TagsGivenAtCreateAreReadBack(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray", "ManagedBy": "foray-deploy"})

	assert.Equal(t, map[string]string{"Project": "foray", "ManagedBy": "foray-deploy"},
		cwlListTags(t, srv, cwlTagGroupARN))
}

func TestCWLogsTags_TagResourceReplacesAKeyAndAppendsANewOne(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray", "Env": "dev"})

	require.Equal(t, http.StatusOK,
		cwlTag(t, srv, cwlTagGroupARN, map[string]string{"Env": "prod", "Owner": "platform"}).StatusCode)

	assert.Equal(t, map[string]string{"Project": "foray", "Env": "prod", "Owner": "platform"},
		cwlListTags(t, srv, cwlTagGroupARN))
}

func TestCWLogsTags_UntagResourceRemovesOnlyWhatItNames(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray", "Env": "dev"})

	cases := []struct {
		name    string
		tagKeys []string
		want    map[string]string
	}{
		{
			// Array Members is "Minimum number of 0 items", so a list of none is a valid
			// request that asks for nothing.
			name:    "an empty list removes nothing",
			tagKeys: []string{},
			want:    map[string]string{"Project": "foray", "Env": "dev"},
		},
		{
			// The page publishes no code for a key the resource does not carry.
			name:    "a key the group does not carry is not an error",
			tagKeys: []string{"NotThere"},
			want:    map[string]string{"Project": "foray", "Env": "dev"},
		},
		{
			name:    "a key it does carry goes",
			tagKeys: []string{"Env"},
			want:    map[string]string{"Project": "foray"},
		},
		{
			name:    "the last key leaves an empty map, not a refusal",
			tagKeys: []string{"Project"},
			want:    map[string]string{},
		},
	}
	// Sequential rather than independent: each case starts from the previous case's state, which
	// is what makes the last one — untagging down to nothing — reachable.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, "UntagResource", map[string]any{
				"resourceArn": cwlTagGroupARN,
				"tagKeys":     tc.tagKeys,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, tc.want, cwlListTags(t, srv, cwlTagGroupARN))
		})
	}
}

// The rule the issue was filed for, asserted on all three operations: a convergence path reads
// before it writes, so a read that accepted the suffixed form would report a group untagged rather
// than telling the caller which ARN to send.
func TestCWLogsTags_ThePolicyFormARNIsRefusedByEveryOperation(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray"})

	bodies := map[string]map[string]any{
		"TagResource":         {"resourceArn": cwlTagGroupPolicyARN, "tags": map[string]string{"Env": "dev"}},
		"UntagResource":       {"resourceArn": cwlTagGroupPolicyARN, "tagKeys": []string{"Project"}},
		"ListTagsForResource": {"resourceArn": cwlTagGroupPolicyARN},
	}
	for op, body := range bodies {
		t.Run(op, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, op, body)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			raw := cwLogsReadBody(t, resp)
			assert.Equal(t, "ValidationException", cwLogsErrorTypeFrom(t, raw))
			assert.Contains(t, string(raw), "Invalid resourceArn")
		})
	}

	// And the refusal changed nothing: the write that was refused did not half-apply.
	assert.Equal(t, map[string]string{"Project": "foray"}, cwlListTags(t, srv, cwlTagGroupARN))
}

// The round trip that pins the trap: DescribeLogGroups reports both ARN forms, the tagging
// operations accept exactly one of them, and which is which is not guessable from the response.
func TestCWLogsTags_DescribeLogGroupsReportsBothFormsAndOnlyOneTags(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, nil)

	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var described struct {
		LogGroups []struct {
			LogGroupARN string `json:"logGroupArn"`
			ARN         string `json:"arn"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &described))
	require.Len(t, described.LogGroups, 1)
	require.Equal(t, cwlTagGroupARN, described.LogGroups[0].LogGroupARN)
	require.Equal(t, cwlTagGroupPolicyARN, described.LogGroups[0].ARN)

	assert.Equal(t, http.StatusBadRequest,
		cwlTag(t, srv, described.LogGroups[0].ARN, map[string]string{"Project": "foray"}).StatusCode,
		"the `arn` member is the IAM-policy form and the API refuses it")
	assert.Equal(t, http.StatusOK,
		cwlTag(t, srv, described.LogGroups[0].LogGroupARN, map[string]string{"Project": "foray"}).StatusCode,
		"the `logGroupArn` member is the form the API wants")
}

func TestCWLogsTags_AnARNThatNamesNothingTaggableIsRefused(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, nil)

	cases := []struct {
		name        string
		resourceARN string
		wantCode    string
	}{
		{
			// Length Constraints put the minimum at 1, so an empty value is an absent
			// required member rather than a malformed ARN.
			name:        "an absent resourceArn is a missing member",
			resourceARN: "",
			wantCode:    "InvalidParameterException",
		},
		{
			name:        "a bare log group name is not an ARN",
			resourceARN: cwlTagGroup,
			wantCode:    "ValidationException",
		},
		{
			name:        "an ARN with too few segments",
			resourceARN: "arn:aws:logs:us-east-1:123456789012",
			wantCode:    "ValidationException",
		},
		{
			name:        "another service's ARN",
			resourceARN: "arn:aws:lambda:us-east-1:123456789012:function:foray-gateway",
			wantCode:    "ValidationException",
		},
		{
			// A logs ARN of a type these operations do not accept.
			name:        "a log stream ARN",
			resourceARN: cwlTagGroupARN + ":log-stream:2026/09/26/[$LATEST]abc",
			wantCode:    "ValidationException",
		},
		{
			name:        "a logs ARN naming no resource type",
			resourceARN: "arn:aws:logs:us-east-1:123456789012:query-definition-id",
			wantCode:    "ValidationException",
		},
		{
			name:        "a log-group ARN with no name",
			resourceARN: "arn:aws:logs:us-east-1:123456789012:log-group:",
			wantCode:    "ValidationException",
		},
		{
			// A destination is taggable at AWS and unrepresentable here, so the resource is
			// reported absent rather than the ARN invalid — the ARN is not what is wrong.
			name:        "a destination ARN names a type substrate does not model",
			resourceARN: "arn:aws:logs:us-east-1:123456789012:destination:foray-firehose",
			wantCode:    "ResourceNotFoundException",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": tc.resourceARN})
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"this service reports every refusal at 400, with the code in the body's __type")
			assert.Equal(t, tc.wantCode, cwLogsErrorType(t, resp))
		})
	}
}

func TestCWLogsTags_AGroupThatDoesNotExistIsRefused(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwlTag(t, srv, cwlTagGroupARN, map[string]string{"Project": "foray"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	raw := cwLogsReadBody(t, resp)
	assert.Equal(t, "ResourceNotFoundException", cwLogsErrorTypeFrom(t, raw))
	assert.Contains(t, string(raw), cwlTagGroup, "the message names the group, as the plugin's other not-founds do")
}

// The #826 rule: the account and Region come from the ARN, so an ARN naming another account's or
// another Region's group resolves that group or none. Resolving it against the caller's own account
// would tag — and UntagResource would strip a tag from — a same-named group the caller never named.
func TestCWLogsTags_AForeignARNDoesNotReachTheCallersGroup(t *testing.T) {
	srv, state := newCWLogsTestServerWithState(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray"})

	foreign := []struct {
		name        string
		resourceARN string
	}{
		{"another account", "arn:aws:logs:us-east-1:210987654321:log-group:" + cwlTagGroup},
		{"another Region", "arn:aws:logs:eu-west-1:123456789012:log-group:" + cwlTagGroup},
	}
	for _, f := range foreign {
		t.Run(f.name, func(t *testing.T) {
			// The write direction first: an UntagResource that reached the caller's group
			// would strip the tag a teardown sweep finds it by.
			resp := cwLogsRequest(t, srv, "UntagResource", map[string]any{
				"resourceArn": f.resourceARN,
				"tagKeys":     []string{"Project"},
			})
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))

			resp = cwlTag(t, srv, f.resourceARN, map[string]string{"Project": "someone-else"})
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))

			assert.Equal(t, map[string]string{"Project": "foray"}, cwlListTags(t, srv, cwlTagGroupARN),
				"the caller's own group is untouched")
		})
	}

	// Nor did the refused writes leave a phantom record behind for the foreign keys — the failure
	// #826 describes is a write that answers 200 against a key no group is stored at.
	keys, err := state.List(t.Context(), "logs", "")
	require.NoError(t, err)
	assert.Equal(t, []string{
		"loggroup:123456789012/us-east-1" + "/" + cwlTagGroup,
		"loggroup_names:123456789012/us-east-1",
	}, keys)
}

// TooManyTagsException is checked against the merged set, because a small request against an
// already-full group exceeds the ceiling while the request alone does not.
func TestCWLogsTags_TheFiftyFirstTagIsRefused(t *testing.T) {
	srv := newCWLogsTestServer(t)

	fifty := make(map[string]string, 50)
	for i := range 50 {
		fifty[fmt.Sprintf("Key%02d", i)] = "v"
	}
	cwlCreateTaggedGroup(t, srv, fifty)

	resp := cwlTag(t, srv, cwlTagGroupARN, map[string]string{"OneTooMany": "v"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "TooManyTagsException", cwLogsErrorType(t, resp))
	assert.Len(t, cwlListTags(t, srv, cwlTagGroupARN), 50, "the refused request added nothing")

	// Replacing a value at the ceiling is not an increase, so it is allowed.
	assert.Equal(t, http.StatusOK, cwlTag(t, srv, cwlTagGroupARN, map[string]string{"Key00": "w"}).StatusCode)
	assert.Equal(t, "w", cwlListTags(t, srv, cwlTagGroupARN)["Key00"])
}

// CreateLogGroup carries no equivalent ceiling: its page publishes no code for exceeding the `tags`
// map maximum, and TooManyTagsException is published on TagResource alone, so refusing here would
// mean inventing a code (#671). The asymmetry is pinned rather than left to be read as an oversight.
func TestCWLogsTags_CreateLogGroupDoesNotEnforceTheTagCeiling(t *testing.T) {
	srv := newCWLogsTestServer(t)

	tooMany := make(map[string]string, cwlTagCeilingProbe)
	for i := range cwlTagCeilingProbe {
		tooMany[fmt.Sprintf("Key%02d", i)] = "v"
	}
	cwlCreateTaggedGroup(t, srv, tooMany)

	assert.Len(t, cwlListTags(t, srv, cwlTagGroupARN), cwlTagCeilingProbe)
}

// cwlTagCeilingProbe is one more tag than TagResource accepts.
const cwlTagCeilingProbe = 51

func TestCWLogsTags_ARequiredMemberIsRefusedBeforeTheARNIsResolved(t *testing.T) {
	srv := newCWLogsTestServer(t)

	cases := []struct {
		name string
		op   string
		body map[string]any
	}{
		// `tags` and `tagKeys` are both Required: Yes. An absent member is refused whether or
		// not the ARN names anything, because there is no request to carry out.
		{"TagResource without tags", "TagResource", map[string]any{"resourceArn": cwlTagGroupARN}},
		{"UntagResource without tagKeys", "UntagResource", map[string]any{"resourceArn": cwlTagGroupARN}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "InvalidParameterException", cwLogsErrorType(t, resp))
		})
	}
}

// An empty `tags` map is present, not absent, and the page publishes no minimum entry count — so it
// is a request that asks for nothing and gets it, which is a different answer from the one above.
func TestCWLogsTags_AnEmptyTagMapIsAcceptedAndChangesNothing(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray"})

	resp := cwlTag(t, srv, cwlTagGroupARN, map[string]string{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"Project": "foray"}, cwlListTags(t, srv, cwlTagGroupARN))
}

// Tags go with the group. A group re-created under the same name starts untagged, which is the
// observable difference between "the record was deleted" and "the name was reused".
func TestCWLogsTags_DeletingAGroupTakesItsTagsWithIt(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray"})

	require.Equal(t, http.StatusOK,
		cwLogsRequest(t, srv, "DeleteLogGroup", map[string]string{"logGroupName": cwlTagGroup}).StatusCode)
	resp := cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": cwlTagGroupARN})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))

	cwlCreateTaggedGroup(t, srv, nil)
	assert.Empty(t, cwlListTags(t, srv, cwlTagGroupARN))
}

// DescribeLogGroups takes a *prefix*, and a caller that checks existence by calling it and treating
// any result as a match will read `/aws/lambda/foray-gateway` as existing when only
// `/aws/lambda/foray-gateway-v2` does. That is the service's own behavior and substrate reproduces
// it; the pin is here so the tagging change cannot be followed by a well-meaning edit turning the
// filter into an exact match, which would hide the trap rather than model it (#1273).
func TestCWLogsTags_DescribeLogGroupsMatchesAPrefixAndNotAName(t *testing.T) {
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": cwlTagGroup + "-v2"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]string{"logGroupNamePrefix": cwlTagGroup})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var described struct {
		LogGroups []struct {
			LogGroupName string `json:"logGroupName"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(cwLogsReadBody(t, resp), &described))
	require.Len(t, described.LogGroups, 1)
	assert.Equal(t, cwlTagGroup+"-v2", described.LogGroups[0].LogGroupName,
		"the prefix matched a longer name, which is why existence cannot be checked this way")

	// And the group the prefix matched is not the one the tagging ARN names.
	resp = cwLogsRequest(t, srv, "ListTagsForResource", map[string]any{"resourceArn": cwlTagGroupARN})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ResourceNotFoundException", cwLogsErrorType(t, resp))
}

// cwlFailingPutStateManager is a StateManager whose writes fail once armed, so a tagging
// operation's read-modify-*write* can be made to fail after the read has already succeeded.
type cwlFailingPutStateManager struct {
	inner emulator.StateManager
	fail  bool
}

func (m *cwlFailingPutStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	return m.inner.Get(ctx, namespace, key)
}

func (m *cwlFailingPutStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.fail {
		return errCWLStoreFault
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *cwlFailingPutStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *cwlFailingPutStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

// errCWLStoreFault is the store failure cwlFailingPutStateManager injects.
var errCWLStoreFault = errors.New("cwl store fault")

// cwlPluginOn builds a logs server over a caller-supplied state manager, which
// newCWLogsTestServerWithState does not allow.
func cwlPluginOn(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Now())

	plugin := &emulator.CloudWatchLogsPlugin{}
	require.NoError(t, plugin.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)
	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// A write that fails in the store is not a refusal the API publishes: it is reported as a server
// error and carries no AWS error code, so a caller retrying on `TooManyTagsException` or giving up
// on `ValidationException` cannot mistake a broken store for either.
func TestCWLogsTags_AStoreFailureIsNotAPublishedRefusal(t *testing.T) {
	state := &cwlFailingPutStateManager{inner: emulator.NewMemoryStateManager()}
	srv := cwlPluginOn(t, state)
	cwlCreateTaggedGroup(t, srv, map[string]string{"Project": "foray"})
	state.fail = true

	cases := []struct {
		op   string
		body map[string]any
	}{
		{"TagResource", map[string]any{"resourceArn": cwlTagGroupARN, "tags": map[string]string{"Env": "dev"}}},
		{"UntagResource", map[string]any{"resourceArn": cwlTagGroupARN, "tagKeys": []string{"Project"}}},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
			assert.NotContains(t, string(cwLogsReadBody(t, resp)), "Exception",
				"a store failure publishes no exception name for a caller to switch on")
		})
	}
}

// A body that is not JSON at all is refused before anything else, in the shape the plugin's other
// operations use.
func TestCWLogsTags_AnUnreadableBodyIsRefused(t *testing.T) {
	srv := newCWLogsTestServer(t)

	for _, op := range []string{"TagResource", "UntagResource", "ListTagsForResource"} {
		t.Run(op, func(t *testing.T) {
			resp := cwLogsRawRequest(t, srv, op, []byte("{not json"))
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "InvalidParameterException", cwLogsErrorType(t, resp))
		})
	}
}
