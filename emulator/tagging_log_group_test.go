package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API reaches a CloudWatch Logs log group (#1282).
//
// The log group's own tagging trio landed in #1273; until #1282 the tagging resolver had no `logs`
// arm, so TagResources reported every log-group ARN in FailedResourcesMap and GetResources never
// reported a group. These tests drive both services over one store, so a tag written through one is
// read back through the other, and the record under test is one the owning plugin wrote.

const (
	rgtaLogGroup    = "/aws/lambda/rgta-sweep"
	rgtaLogAccount  = "123456789012"
	rgtaLogRegion   = "us-east-1"
	rgtaLogGroupARN = "arn:aws:logs:" + rgtaLogRegion + ":" + rgtaLogAccount + ":log-group:" + rgtaLogGroup
)

// rgtaLogHarness is a CloudWatch Logs plugin and a tagging plugin sharing one state manager.
type rgtaLogHarness struct {
	t       *testing.T
	state   emulator.StateManager
	logs    *emulator.CloudWatchLogsPlugin
	tagging *emulator.TaggingPlugin
	ctx     *emulator.RequestContext
}

func newRGTALogHarness(t *testing.T, state emulator.StateManager) *rgtaLogHarness {
	t.Helper()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Unix(1700000000, 0).UTC())
	tc.Freeze()
	tc.SetTime(time.Unix(1700000000, 0).UTC())
	h := &rgtaLogHarness{
		t: t, state: state,
		logs:    &emulator.CloudWatchLogsPlugin{},
		tagging: &emulator.TaggingPlugin{},
		ctx:     &emulator.RequestContext{AccountID: rgtaLogAccount, Region: rgtaLogRegion, RequestID: "req-rgta-logs"},
	}
	require.NoError(t, h.logs.Initialize(t.Context(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}), "CloudWatchLogsPlugin.Initialize")
	require.NoError(t, h.tagging.Initialize(t.Context(), emulator.PluginConfig{State: state, Logger: logger}),
		"TaggingPlugin.Initialize")
	return h
}

// call issues one JSON-target request against a plugin and returns its response or error.
func (h *rgtaLogHarness) call(p emulator.Plugin, target, op string, body any) (*emulator.AWSResponse, error) {
	h.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(h.t, err, "marshal %s", op)
	return p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: p.Name(), Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": target + "." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:  map[string]string{},
	})
}

// logs issues a CloudWatch Logs request and requires a 200.
func (h *rgtaLogHarness) logsOK(op string, body any) []byte {
	h.t.Helper()
	resp, err := h.call(h.logs, "Logs_20140328", op, body)
	require.NoError(h.t, err, "logs %s", op)
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "logs %s: %s", op, resp.Body)
	return resp.Body
}

// rgta issues a tagging request and returns its FailedResourcesMap, requiring a 200.
func (h *rgtaLogHarness) rgta(op string, body any) map[string]struct {
	ErrorCode  string `json:"ErrorCode"`
	StatusCode int    `json:"StatusCode"`
} {
	h.t.Helper()
	resp, err := h.call(h.tagging, "ResourceGroupsTaggingAPI_20170126", op, body)
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

// tag tags arn through the tagging API, requiring no failure.
func (h *rgtaLogHarness) tag(arn string, tags map[string]string) {
	h.t.Helper()
	require.Empty(h.t, h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": tags}), "TagResources %s", arn)
}

// untag untags arn through the tagging API, requiring no failure.
func (h *rgtaLogHarness) untag(arn string, keys ...string) {
	h.t.Helper()
	require.Empty(h.t, h.rgta("UntagResources", map[string]any{"ResourceARNList": []string{arn}, "TagKeys": keys}), "UntagResources %s", arn)
}

// listTags reads a log group's tags through CloudWatch Logs' own ListTagsForResource.
func (h *rgtaLogHarness) listTags(arn string) map[string]string {
	h.t.Helper()
	var out struct {
		Tags map[string]string `json:"tags"`
	}
	body := h.logsOK("ListTagsForResource", map[string]any{"resourceArn": arn})
	require.NoError(h.t, json.Unmarshal(body, &out), "decode ListTagsForResource: %s", body)
	return out.Tags
}

// reported runs GetResources with the given type filters and returns the log groups it reports,
// by ARN, with their tags; the slice distinguishes a reported empty set from an absent group.
func (h *rgtaLogHarness) reported(filters ...string) map[string][]string {
	h.t.Helper()
	body := map[string]any{}
	if len(filters) > 0 {
		body["ResourceTypeFilters"] = filters
	}
	resp, err := h.call(h.tagging, "ResourceGroupsTaggingAPI_20170126", "GetResources", body)
	require.NoError(h.t, err, "GetResources")
	require.Equal(h.t, http.StatusOK, resp.StatusCode, "GetResources: %s", resp.Body)
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
	return got
}

func TestTaggingLogGroup_TagResourcesIsReadBackByTheLogsOwnRead(t *testing.T) {
	t.Parallel()
	h := newRGTALogHarness(t, emulator.NewMemoryStateManager())
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup})

	h.tag(rgtaLogGroupARN, map[string]string{"team": "sweep"})
	assert.Equal(t, map[string]string{"team": "sweep"}, h.listTags(rgtaLogGroupARN),
		"logs:ListTagsForResource must read the tag the tagging API wrote")

	// The other direction: a tag CloudWatch Logs' own TagResource writes is reported by GetResources.
	h.logsOK("TagResource", map[string]any{"resourceArn": rgtaLogGroupARN, "tags": map[string]string{"owner": "logs"}})
	assert.Equal(t, []string{"owner=logs", "team=sweep"}, h.reported("logs:log-group")[rgtaLogGroupARN])

	h.untag(rgtaLogGroupARN, "team", "owner")
	assert.Empty(t, h.listTags(rgtaLogGroupARN), "UntagResources must remove the tags")
}

// An ARN naming another account's or another Region's same-named group must reach neither the
// caller's group nor a phantom record (#826's rule, applied to the new arm).
func TestTaggingLogGroup_AForeignARNReachesNeitherTheCallersGroupNorAPhantom(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, account, region string
	}{
		{"another account", "999988887777", rgtaLogRegion},
		{"another Region", rgtaLogAccount, "us-west-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := emulator.NewMemoryStateManager()
			h := newRGTALogHarness(t, state)
			h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup, "tags": map[string]string{"env": "prod"}})

			foreign := "arn:aws:logs:" + tc.region + ":" + tc.account + ":log-group:" + rgtaLogGroup
			for _, op := range []struct {
				name string
				body map[string]any
			}{
				{"TagResources", map[string]any{"ResourceARNList": []string{foreign}, "Tags": map[string]string{"intruder": "yes"}}},
				{"UntagResources", map[string]any{"ResourceARNList": []string{foreign}, "TagKeys": []string{"env"}}},
			} {
				failed := h.rgta(op.name, op.body)
				require.Contains(t, failed, foreign, "%s against %s must fail", op.name, foreign)
				assert.Equal(t, "InvalidParameterException", failed[foreign].ErrorCode, "%s: no such group", op.name)
				assert.Equal(t, http.StatusBadRequest, failed[foreign].StatusCode)
			}
			assert.Equal(t, map[string]string{"env": "prod"}, h.listTags(rgtaLogGroupARN), "the caller's group is untouched")

			phantom, err := state.Get(t.Context(), "logs", "loggroup:"+tc.account+"/"+tc.region+"/"+rgtaLogGroup)
			require.NoError(t, err)
			assert.Nil(t, phantom, "no record may be created at the foreign key")
		})
	}
}

// GetResources reports a tagged group with and without a logs:log-group filter, reports a group whose
// tags were all removed with "Tags": [], and does not report one that was never tagged (#938's rule).
func TestTaggingLogGroup_GetResourcesReportsWhatHasBeenTagged(t *testing.T) {
	t.Parallel()
	h := newRGTALogHarness(t, emulator.NewMemoryStateManager())
	never := "arn:aws:logs:" + rgtaLogRegion + ":" + rgtaLogAccount + ":log-group:/never/tagged"
	emptied := "arn:aws:logs:" + rgtaLogRegion + ":" + rgtaLogAccount + ":log-group:/was/tagged"
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup, "tags": map[string]string{"env": "prod"}})
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": "/never/tagged"})
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": "/was/tagged"})
	// Tagged and emptied through CloudWatch Logs' own operations, so the history is stamped by that
	// side; the tagging API reads it back.
	h.logsOK("TagResource", map[string]any{"resourceArn": emptied, "tags": map[string]string{"tmp": "1"}})
	h.logsOK("UntagResource", map[string]any{"resourceArn": emptied, "tagKeys": []string{"tmp"}})

	for _, filters := range [][]string{nil, {"logs:log-group"}, {"logs"}} {
		t.Run(fmt.Sprintf("filters=%v", filters), func(t *testing.T) {
			got := h.reported(filters...)
			assert.Equal(t, []string{"env=prod"}, got[rgtaLogGroupARN], "a tagged group is reported with its tags")
			assert.Contains(t, got, emptied, "a group that had a tag is reported")
			assert.Empty(t, got[emptied], "with an empty tag set")
			assert.NotContains(t, got, never, "a group that never had a tag is not reported")
		})
	}
	// Another type's filter excludes the groups.
	assert.NotContains(t, h.reported("lambda:function"), rgtaLogGroupARN)

	// Deleting a group removes its history, so the same name re-created starts never-tagged.
	h.logsOK("DeleteLogGroup", map[string]any{"logGroupName": "/was/tagged"})
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": "/was/tagged"})
	assert.NotContains(t, h.reported("logs:log-group"), emptied, "a re-created group is a new resource")
}

// A stream ARN, the `:*`-suffixed policy form and a destination ARN are each refused by the resolver
// rather than keyed to the group; an index key is refused by the merge's kind guard.
func TestTaggingLogGroup_ARNsNamingAnythingButAGroupAreRefused(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	h := newRGTALogHarness(t, state)
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup, "tags": map[string]string{"env": "prod"}})

	for _, arn := range []string{
		rgtaLogGroupARN + ":*",
		rgtaLogGroupARN + ":log-stream:app",
		"arn:aws:logs:" + rgtaLogRegion + ":" + rgtaLogAccount + ":destination:dest",
		"arn:aws:logs:" + rgtaLogRegion + ":" + rgtaLogAccount + ":log-group:",
	} {
		t.Run(arn, func(t *testing.T) {
			_, _, err := emulator.TaggingResolveARNForTest(state, arn)
			require.Error(t, err, "the resolver must refuse %s rather than key it", arn)

			failed := h.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"x": "y"}})
			require.Contains(t, failed, arn)
			assert.Equal(t, "InternalServiceException", failed[arn].ErrorCode, "an unsupported type, per the tagging API's split")
		})
	}
	assert.Equal(t, map[string]string{"env": "prod"}, h.listTags(rgtaLogGroupARN), "no refused ARN reached the group")

	// The kind guard: the namespace's index and history keys store no tags, and a merge aimed at one
	// is refused rather than decoded as a group.
	for _, key := range []string{
		"loggroup_names:" + rgtaLogAccount + "/" + rgtaLogRegion,
		"loggroup_tagged:" + rgtaLogAccount + "/" + rgtaLogRegion + "/" + rgtaLogGroup,
	} {
		require.NoError(t, state.Put(t.Context(), "logs", key, []byte(`["x"]`)))
		err := emulator.MergeResourceTagsForTest(state, "logs", key, map[string]string{"x": "y"}, nil, true)
		require.Error(t, err, "a merge aimed at %s must be refused", key)
	}
}

// TagResources enforces CloudWatch Logs' own 50-tag ceiling, with its own code, so the tagging API
// cannot put a group over a quota the service would then refuse every further add against (#1000).
func TestTaggingLogGroup_TagResourcesEnforcesTheLogsTagQuota(t *testing.T) {
	t.Parallel()
	h := newRGTALogHarness(t, emulator.NewMemoryStateManager())
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup})
	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[fmt.Sprintf("k%02d", i)] = "v"
	}
	failed := h.rgta("TagResources", map[string]any{"ResourceARNList": []string{rgtaLogGroupARN}, "Tags": tags})
	require.Contains(t, failed, rgtaLogGroupARN)
	assert.Equal(t, "TooManyTagsException", failed[rgtaLogGroupARN].ErrorCode)
	assert.Equal(t, http.StatusBadRequest, failed[rgtaLogGroupARN].StatusCode)
	assert.Empty(t, h.listTags(rgtaLogGroupARN), "a refused merge writes nothing")
}

// A store fault at any new read or write is an error, never a success or a silent omission
// misreported as "never tagged".
func TestTaggingLogGroup_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		run  func(*rgtaLogHarness) bool // reports whether the request failed
	}{
		{"TagResources, record write", func(m *cfFaultStateManager) { m.failPut = "loggroup:" }, func(h *rgtaLogHarness) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{rgtaLogGroupARN}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"TagResources, history write", func(m *cfFaultStateManager) { m.failPut = "loggroup_tagged:" }, func(h *rgtaLogHarness) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{rgtaLogGroupARN}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"TagResources, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "loggroup:" }, func(h *rgtaLogHarness) bool {
			return len(h.rgta("TagResources", map[string]any{"ResourceARNList": []string{rgtaLogGroupARN}, "Tags": map[string]string{"a": "b"}})) > 0
		}},
		{"logs TagResource, history write", func(m *cfFaultStateManager) { m.failPut = "loggroup_tagged:" }, func(h *rgtaLogHarness) bool {
			_, err := h.call(h.logs, "Logs_20140328", "TagResource", map[string]any{"resourceArn": rgtaLogGroupARN, "tags": map[string]string{"a": "b"}})
			return err != nil
		}},
		{"logs UntagResource, history write", func(m *cfFaultStateManager) { m.failPut = "loggroup_tagged:" }, func(h *rgtaLogHarness) bool {
			_, err := h.call(h.logs, "Logs_20140328", "UntagResource", map[string]any{"resourceArn": rgtaLogGroupARN, "tagKeys": []string{"seed"}})
			return err != nil
		}},
		{"DeleteLogGroup, history delete", func(m *cfFaultStateManager) { m.failDelete = "loggroup_tagged:" }, func(h *rgtaLogHarness) bool {
			_, err := h.call(h.logs, "Logs_20140328", "DeleteLogGroup", map[string]any{"logGroupName": rgtaLogGroup})
			return err != nil
		}},
		{"GetResources, record read", func(m *cfFaultStateManager) { m.failGet = "loggroup:" }, func(h *rgtaLogHarness) bool {
			_, found := h.reported("logs:log-group")[rgtaLogGroupARN]
			return !found
		}},
		{"GetResources, history read", func(m *cfFaultStateManager) { m.failGet = "loggroup_tagged:" }, func(h *rgtaLogHarness) bool {
			_, found := h.reported("logs:log-group")[rgtaLogGroupARN]
			return !found
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newRGTALogHarness(t, fault)
			h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup, "tags": map[string]string{"seed": "1"}})

			tc.arm(fault)
			assert.True(t, tc.run(h), "%s must fail on a store fault rather than succeed", tc.name)
		})
	}
}

// The tag-history side-car is a key of its own, so CWLogGroup's persisted encoding carries no new
// member: the record CloudWatch Logs stores after a tagging-API write is the struct's own fields.
func TestTaggingLogGroup_TheRecordGainsNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	h := newRGTALogHarness(t, state)
	h.logsOK("CreateLogGroup", map[string]any{"logGroupName": rgtaLogGroup})
	h.tag(rgtaLogGroupARN, map[string]string{"a": "b"})
	h.untag(rgtaLogGroupARN, "a")

	raw, err := state.Get(context.Background(), "logs", "loggroup:"+rgtaLogAccount+"/"+rgtaLogRegion+"/"+rgtaLogGroup)
	require.NoError(t, err)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &record), "%s", raw)
	assert.NotContains(t, record, "ever_tagged", "the history lives beside the record, not in it: %s", raw)
	assert.Contains(t, h.reported("logs:log-group"), rgtaLogGroupARN, "and it is still read: the group is reported")
}
