package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for CloudTrailTrail (#756).
//
// CloudTrailTrail declares AccountID, Region and CreatedAt under wire-visible `json` tags, none under
// omitempty, so a record that exists holds all three and no absence below is vacuous. CreateTrail,
// UpdateTrail, GetTrail and DescribeTrails used to answer the record whole; they answer
// emulator/cloudtrail_wire.go's two projections now. The other four routed operations answer maps
// and are driven anyway.

// cloudtrailWireAccount and cloudtrailWireRegion scope every state key the plugin writes.
const (
	cloudtrailWireAccount = "123456789012"
	cloudtrailWireRegion  = "us-east-1"
)

// cloudtrailBookkeepingMembers are the members CloudTrailTrail declares and no CloudTrail shape
// publishes. HomeRegion, which Trail publishes, is a different name, and the walk is a case-folded
// equality.
var cloudtrailBookkeepingMembers = []string{"AccountID", "Region", "CreatedAt"}

// setupCloudTrailWirePlugin returns the CloudTrail plugin, a request context and the state manager
// behind it.
func setupCloudTrailWirePlugin(t *testing.T) (*emulator.CloudTrailPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.CloudTrailPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}), "emulator.CloudTrailPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: cloudtrailWireAccount,
		Region:    cloudtrailWireRegion,
		RequestID: "req-cloudtrail-wire",
		IDs:       emulator.NewIDMint("req-cloudtrail-wire"),
	}, state
}

// cloudtrailWire issues one operation and returns the raw response body, failing on anything but 200.
func cloudtrailWire(t *testing.T, p *emulator.CloudTrailPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

func TestCloudTrailWire_TrailResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCloudTrailWirePlugin(t)

	const name = "wire-trail"
	trail := map[string]any{"Name": name}
	created := cloudtrailWire(t, p, ctx, "CreateTrail", map[string]any{"Name": name, "S3BucketName": "wire-trail-bucket"})

	data, err := state.Get(t.Context(), "cloudtrail", "trail:"+cloudtrailWireAccount+"/"+cloudtrailWireRegion+"/"+name)
	require.NoError(t, err, "state.Get trail")
	require.NotNil(t, data, "no trail record stored")
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode trail: %s", data)
	for _, member := range cloudtrailBookkeepingMembers {
		require.NotEmptyf(t, record[member], "the trail must persist %s before an absence assertion on it means anything", member)
		require.NotEqualf(t, `""`, string(record[member]), "the trail persists an empty %s", member)
	}

	updated := cloudtrailWire(t, p, ctx, "UpdateTrail", map[string]any{"Name": name, "S3BucketName": "wire-trail-bucket-2"})
	got := cloudtrailWire(t, p, ctx, "GetTrail", trail)

	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "CreateTrail", held: created, anchor: `"Name":"` + name + `"`},
		{op: "UpdateTrail", held: updated, anchor: `"S3BucketName":"wire-trail-bucket-2"`},
		{op: "GetTrail", held: got, anchor: `"Name":"` + name + `"`},
		{op: "DescribeTrails", anchor: `"Name":"` + name + `"`},
		{op: "GetTrailStatus", body: trail, anchor: `"IsLogging"`},
		{op: "StopLogging", body: trail, anchor: "{}"},
		{op: "StartLogging", body: trail, anchor: "{}"},
		// Last: it removes the record every case above reads.
		{op: "DeleteTrail", body: trail, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = cloudtrailWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, cloudtrailBookkeepingMembers)
		})
	}

	// After the walk. IsLogging is GetTrailStatus's alone: neither API_Trail nor CreateTrail's and
	// UpdateTrail's responses publish it, and the two writes publish no HomeRegion either.
	for op, body := range map[string][]byte{"CreateTrail": created, "UpdateTrail": updated} {
		var top map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(body, &top), "decode %s: %s", op, body)
		keys := slices.Sorted(maps.Keys(top))
		for _, unpublished := range []string{"IsLogging", "HomeRegion", "HasCustomEventSelectors"} {
			require.NotContainsf(t, keys, unpublished, "%s answered %s, which its response does not publish: %s", op, unpublished, body)
		}
	}
	var get struct {
		Trail map[string]json.RawMessage `json:"Trail"`
	}
	require.NoError(t, json.Unmarshal(got, &get), "decode GetTrail: %s", got)
	require.NotContains(t, slices.Sorted(maps.Keys(get.Trail)), "IsLogging", "API_Trail publishes no IsLogging: %s", got)
	require.Contains(t, slices.Sorted(maps.Keys(get.Trail)), "HomeRegion", "API_Trail publishes HomeRegion: %s", got)
}
