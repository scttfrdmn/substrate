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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for WAFv2's two records
// (#756).
//
// WAFv2WebACL declares AccountID, Region and CreatedAt and WAFv2IPSet declares AccountID and Region,
// none under omitempty, so a record that exists holds every one of them and no absence below is
// vacuous. GetWebACL and GetIPSet used to answer those records whole; they answer
// emulator/wafv2_wire.go's projections now, and this file is what says so. The other eleven routed
// operations build a map of published members and are driven anyway, so the list of thirteen reads
// as complete rather than as a sample.
//
// Each get is also checked for the two non-bookkeeping members the record carried onto the wire,
// Scope and LockToken, which API_WebACL and API_IPSet do not publish. LockToken *is* published one
// level up, at the top of each get's response, so that check is on the object's own members and the
// top-level token is required to survive.

// wafv2WireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var wafv2WireClock = time.Unix(1700000000, 0).UTC()

// wafv2WireAccount and wafv2WireRegion scope every state key the plugin writes.
const (
	wafv2WireAccount = "123456789012"
	wafv2WireRegion  = "us-east-1"
)

// wafv2BookkeepingMembers are the members WAFv2's records declare and no WAFv2 shape publishes.
// CreatedAt is the web ACL's alone, and is listed for both tests because no WAFv2 response may carry
// it.
var wafv2BookkeepingMembers = []string{"AccountID", "Region", "CreatedAt"}

// setupWAFv2WirePlugin returns the WAFv2 plugin, a request context and the state manager behind it.
// The state manager is handed back because a record is the only place its bookkeeping members can be
// read from.
func setupWAFv2WirePlugin(t *testing.T) (*emulator.WAFv2Plugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.WAFv2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(wafv2WireClock)},
	}), "emulator.WAFv2Plugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: wafv2WireAccount,
		Region:    wafv2WireRegion,
		RequestID: "req-wafv2-wire",
		IDs:       emulator.NewIDMint("req-wafv2-wire"),
	}, state
}

// wafv2Wire issues one operation and returns the raw response body, failing on anything but 200.
func wafv2Wire(t *testing.T, p *emulator.WAFv2Plugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, wafv2Request(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// wafv2WireRequireHeld requires that the stored record carries each named member with a non-empty
// value — the presence anchor without which an absence assertion would pass without testing anything.
func wafv2WireRequireHeld(t *testing.T, state emulator.StateManager, key string, members ...string) {
	t.Helper()
	data, err := state.Get(t.Context(), "wafv2", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	for _, member := range members {
		require.NotEmptyf(t, record[member], "%s must persist %s before an absence assertion on it means anything", key, member)
		require.NotEqualf(t, `""`, string(record[member]), "%s persists an empty %s", key, member)
	}
}

// wafv2WireSummary decodes the Summary a create answers.
func wafv2WireSummary(t *testing.T, op string, body []byte) (id, arn, lockToken string) {
	t.Helper()
	var out struct {
		Summary struct {
			ID        string `json:"Id"`
			ARN       string `json:"ARN"`
			LockToken string `json:"LockToken"`
		} `json:"Summary"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "decode %s: %s", op, body)
	require.NotEmpty(t, out.Summary.ID, "%s must report an id", op)
	return out.Summary.ID, out.Summary.ARN, out.Summary.LockToken
}

// wafv2WireRequireObjectOmits requires that the object under member in a get's response carries none
// of the unpublished members, and that the published top-level LockToken is still answered.
func wafv2WireRequireObjectOmits(t *testing.T, op string, body []byte, member string) {
	t.Helper()
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &out), "decode %s: %s", op, body)
	require.NotEmpty(t, out["LockToken"], "%s answers the published top-level LockToken: %s", op, body)
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out[member], &object), "decode %s.%s: %s", op, member, body)
	keys := slices.Sorted(maps.Keys(object))
	for _, unpublished := range []string{"Scope", "LockToken"} {
		require.NotContainsf(t, keys, unpublished, "%s.%s answered %s, which the shape does not publish", op, member, unpublished)
	}
}

// wafv2WireCase is one operation driven by one of the tests below. A non-nil held reuses a response
// the test already has rather than issuing the operation a second time.
type wafv2WireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// wafv2WireRun drives each case as a subtest, in order: the presence anchor first, then the walk.
func wafv2WireRun(t *testing.T, p *emulator.WAFv2Plugin, ctx *emulator.RequestContext, cases []wafv2WireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = wafv2Wire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, wafv2BookkeepingMembers)
		})
	}
}

// wafv2WireVisibility is a VisibilityConfig every web ACL request below carries.
var wafv2WireVisibility = map[string]any{
	"SampledRequestsEnabled":   true,
	"CloudWatchMetricsEnabled": true,
	"MetricName":               "wire-acl",
}

func TestWAFv2Wire_WebACLResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupWAFv2WirePlugin(t)

	const name = "wire-acl"
	created := wafv2Wire(t, p, ctx, "CreateWebACL", map[string]any{
		"Name":             name,
		"Scope":            "REGIONAL",
		"Description":      "wafv2 wire",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": wafv2WireVisibility,
	})
	id, arn, lockToken := wafv2WireSummary(t, "CreateWebACL", created)
	wafv2WireRequireHeld(t, state, "webacl:"+wafv2WireAccount+"/"+wafv2WireRegion+"/REGIONAL/"+id,
		"AccountID", "Region", "CreatedAt")

	ref := map[string]any{"Name": name, "Scope": "REGIONAL", "Id": id}
	got := wafv2Wire(t, p, ctx, "GetWebACL", ref)

	updated := wafv2Wire(t, p, ctx, "UpdateWebACL", map[string]any{
		"Name": name, "Scope": "REGIONAL", "Id": id, "LockToken": lockToken,
		"DefaultAction": map[string]any{"Allow": map[string]any{}}, "VisibilityConfig": wafv2WireVisibility,
	})
	var next struct {
		NextLockToken string `json:"NextLockToken"`
	}
	require.NoError(t, json.Unmarshal(updated, &next), "decode UpdateWebACL: %s", updated)

	const resource = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/wire-alb/abc123"
	wafv2WireRun(t, p, ctx, []wafv2WireCase{
		{op: "CreateWebACL", held: created, anchor: `"Id":"` + id + `"`},
		{op: "GetWebACL", held: got, anchor: `"Id":"` + id + `"`},
		{op: "UpdateWebACL", held: updated, anchor: `"NextLockToken"`},
		{op: "ListWebACLs", body: map[string]any{"Scope": "REGIONAL"}, anchor: `"Id":"` + id + `"`},
		{op: "AssociateWebACL", body: map[string]any{"WebACLArn": arn, "ResourceArn": resource}, anchor: "{}"},
		{op: "GetWebACLForResource", body: map[string]any{"ResourceArn": resource}, anchor: `"ARN":"` + arn + `"`},
		{op: "DisassociateWebACL", body: map[string]any{"ResourceArn": resource}, anchor: "{}"},
		// Last: it removes the record every case above reads.
		{op: "DeleteWebACL", body: map[string]any{"Name": name, "Scope": "REGIONAL", "Id": id, "LockToken": next.NextLockToken},
			anchor: "{}"},
	})

	// After the walk, so a regression is reported as a bookkeeping leak by the cited test first and
	// these two members second, rather than stopping the test before the walk runs.
	wafv2WireRequireObjectOmits(t, "GetWebACL", got, "WebACL")
}

func TestWAFv2Wire_IPSetResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupWAFv2WirePlugin(t)

	const name = "wire-ipset"
	created := wafv2Wire(t, p, ctx, "CreateIPSet", map[string]any{
		"Name":             name,
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{"192.0.2.0/24"},
	})
	id, _, lockToken := wafv2WireSummary(t, "CreateIPSet", created)
	wafv2WireRequireHeld(t, state, "ipset:"+wafv2WireAccount+"/"+wafv2WireRegion+"/REGIONAL/"+id,
		"AccountID", "Region")

	ref := map[string]any{"Name": name, "Scope": "REGIONAL", "Id": id}
	got := wafv2Wire(t, p, ctx, "GetIPSet", ref)

	updated := wafv2Wire(t, p, ctx, "UpdateIPSet", map[string]any{
		"Name": name, "Scope": "REGIONAL", "Id": id, "LockToken": lockToken,
		"Addresses": []string{"198.51.100.0/24"},
	})
	var next struct {
		NextLockToken string `json:"NextLockToken"`
	}
	require.NoError(t, json.Unmarshal(updated, &next), "decode UpdateIPSet: %s", updated)

	wafv2WireRun(t, p, ctx, []wafv2WireCase{
		{op: "CreateIPSet", held: created, anchor: `"Id":"` + id + `"`},
		{op: "GetIPSet", held: got, anchor: `"Id":"` + id + `"`},
		{op: "UpdateIPSet", held: updated, anchor: `"NextLockToken"`},
		{op: "ListIPSets", body: map[string]any{"Scope": "REGIONAL"}, anchor: `"Id":"` + id + `"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteIPSet", body: map[string]any{"Name": name, "Scope": "REGIONAL", "Id": id, "LockToken": next.NextLockToken},
			anchor: "{}"},
	})

	// After the walk, so a regression is reported as a bookkeeping leak by the cited test first and
	// these two members second, rather than stopping the test before the walk runs.
	wafv2WireRequireObjectOmits(t, "GetIPSet", got, "IPSet")
}
