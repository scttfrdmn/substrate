package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for SESv2Identity (#756), and
// the published Tags shape on both sides of the wire (#1335).
//
// SESv2Identity declares AccountId, Region and CreatedAt, none under omitempty, so a record that
// exists holds every one of them and no absence below is vacuous. GetEmailIdentity used to answer the
// record as its whole body; it answers emulator/sesv2_wire.go's projection now, and this file is what
// says so. The other four routed operations build their bodies member by member and are driven
// anyway, so the list of five reads as complete rather than as a sample.
//
// SESv2CapturedEmail, the other record in emulator/sesv2_types.go, is not cited here: no AWS response
// renders it. SendEmail answers a MessageId, and the record is rendered whole only by substrate's own
// GET /v1/emails, where the account and Region are the point. An AWS-response citation for it would be
// vacuous, so it waits with the rest of substrate's own surface.

// sesv2WireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var sesv2WireClock = time.Unix(1700000000, 0).UTC()

// sesv2WireAccount and sesv2WireRegion scope every state key the plugin writes.
const (
	sesv2WireAccount = "123456789012"
	sesv2WireRegion  = "us-east-1"
)

// sesv2BookkeepingMembers are the members SESv2Identity declares and no SESv2 shape publishes.
//
// IdentityName is deliberately not listed: ListEmailIdentities publishes it, in each element of
// EmailIdentities. It is unpublished only as a top-level member of GetEmailIdentity's body, which the
// identity test checks on its own.
var sesv2BookkeepingMembers = []string{"AccountId", "Region", "CreatedAt"}

// setupSESv2WirePlugin returns the SESv2 plugin, a request context and the state manager behind it.
// The state manager is handed back because a record is the only place its bookkeeping members can be
// read from.
func setupSESv2WirePlugin(t *testing.T) (*emulator.SESv2Plugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.SESv2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(sesv2WireClock)},
	}), "emulator.SESv2Plugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: sesv2WireAccount,
		Region:    sesv2WireRegion,
		RequestID: "req-sesv2-wire",
		IDs:       emulator.NewIDMint("req-sesv2-wire"),
	}, state
}

// sesv2Wire issues one request and returns the raw response body, failing on anything but 200.
func sesv2Wire(t *testing.T, p *emulator.SESv2Plugin, ctx *emulator.RequestContext, method, path string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, sesv2Request(method, path, body))
	require.NoError(t, err, "%s %s", method, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, resp.Body)
	return resp.Body
}

func TestSESv2Wire_IdentityResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSESv2WirePlugin(t)

	const identity = "sender@example.com"
	// The published request shape for Tags is an array of Tag objects (#1335). Out of key order on
	// purpose, so the response's ordering is something the handler did rather than something the
	// request already had.
	created := sesv2Wire(t, p, ctx, "POST", "/v2/email/identities", map[string]any{
		"EmailIdentity": identity,
		"Tags": []map[string]string{
			{"Key": "zeta", "Value": "1"},
			{"Key": "alpha", "Value": "2"},
		},
	})

	key := "identity:" + sesv2WireAccount + "/" + sesv2WireRegion + "/" + identity
	data, err := state.Get(t.Context(), "sesv2", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	for _, member := range sesv2BookkeepingMembers {
		require.NotEmptyf(t, record[member], "%s must persist %s before an absence assertion on it means anything", key, member)
		require.NotEqualf(t, `""`, string(record[member]), "%s persists an empty %s", key, member)
	}
	// The record keeps the map it has always stored, so a recorded run replays identically.
	require.JSONEq(t, `{"alpha":"2","zeta":"1"}`, string(record["Tags"]), "the stored Tags format is unchanged: %s", data)

	got := sesv2Wire(t, p, ctx, "GET", "/v2/email/identities/"+identity, nil)

	for _, tc := range []struct {
		op     string
		body   []byte
		method string
		path   string
		req    map[string]any
		anchor string
	}{
		// CreateEmailIdentity answers {} today where the page publishes IdentityType,
		// VerifiedForSendingStatus and DkimAttributes; walking it still pins that nothing of the
		// record is added to it.
		{op: "CreateEmailIdentity", body: created, anchor: "{}"},
		{op: "GetEmailIdentity", body: got, anchor: `"IdentityType":"EMAIL_ADDRESS"`},
		{op: "ListEmailIdentities", method: "GET", path: "/v2/email/identities", anchor: `"IdentityName":"` + identity + `"`},
		{op: "SendEmail", method: "POST", path: "/v2/email/outbound-emails", req: map[string]any{
			"FromEmailAddress": identity,
			"Destination":      map[string]any{"ToAddresses": []string{"to@example.com"}},
			"Content": map[string]any{"Simple": map[string]any{
				"Subject": map[string]any{"Data": "wire"},
				"Body":    map[string]any{"Text": map[string]any{"Data": "wire"}},
			}},
		}, anchor: `"MessageId"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteEmailIdentity", method: "DELETE", path: "/v2/email/identities/" + identity, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.body
			if body == nil {
				body = sesv2Wire(t, p, ctx, tc.method, tc.path, tc.req)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, sesv2BookkeepingMembers)
		})
	}

	// After the walk, so a regression is reported as a bookkeeping leak by the cited test first.
	// The caller named the identity in the request path, so the response does not repeat it.
	var top map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got, &top), "decode GetEmailIdentity: %s", got)
	require.NotContains(t, top, "IdentityName", "API_GetEmailIdentity publishes no IdentityName: %s", got)
	// The published response shape for Tags is an array of Tag objects, in key order (#1335).
	require.JSONEq(t, `[{"Key":"alpha","Value":"2"},{"Key":"zeta","Value":"1"}]`, string(top["Tags"]),
		"GetEmailIdentity answers Tags as the published array: %s", got)
}
