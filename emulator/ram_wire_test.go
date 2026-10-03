package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestRAMWire_ResourceShareResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for RAMResourceShare (#756).
//
// RAMResourceShare declares accountID and region under wire-visible `json` tags, never under
// omitempty, so a record that exists holds both and no absence below is vacuous. CreateResourceShare,
// UpdateResourceShare and GetResourceShares used to answer the record whole; they answer
// emulator/ram_wire.go's projection now. The other five routed operations answer maps built member
// by member and are driven anyway.
//
// The walk folds case. RAM publishes owningAccountId, which an equality fold does not confuse with
// accountID, and resourceShareArn, which it does not confuse with resourceArns.
func TestRAMWire_ResourceShareResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.RAMPlugin{}
	ctx, state := wireSetup(t, p, "req-ram-wire")
	call := func(path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "ram", http.MethodPost, path, body)
	}

	const principal = "210987654321"
	const resource = "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0wire"
	created := call("/createresourceshare", map[string]any{
		"name": "wire-share", "principals": []string{principal}, "resourceArns": []string{resource},
		"tags": []map[string]string{{"key": "team", "value": "wire"}},
	})
	var out struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateResourceShare: %s", created)
	arn := out.ResourceShare.ResourceShareArn
	require.NotEmpty(t, arn, "CreateResourceShare must report an ARN: %s", created)
	record := wireRequireHeld(t, state, "ram", "share:123456789012/us-east-1/"+arn, "accountID", "region")
	// The associations stay in the record, which ListPrincipals and ListResources answer from.
	require.JSONEq(t, `["`+principal+`"]`, string(record["principals"]), "the share must persist its principals")
	require.JSONEq(t, `["`+resource+`"]`, string(record["resourceArns"]), "the share must persist its resources")

	updated := call("/updateresourceshare", map[string]any{"resourceShareArn": arn, "name": "wire-share-2"})
	listed := call("/getresourceshares", map[string]any{"resourceOwner": "SELF"})
	share := map[string]any{"resourceShareArn": arn}
	members := []string{"AccountID", "Region"}
	wireRunJSON(t, members, []wireCase{
		{op: "CreateResourceShare", held: created, anchor: `"resourceShareArn":"` + arn + `"`},
		{op: "UpdateResourceShare", held: updated, anchor: `"name":"wire-share-2"`},
		{op: "GetResourceShares", held: listed, anchor: `"resourceShareArn":"` + arn + `"`},
		{op: "ListPrincipals", call: func() []byte {
			return call("/listprincipals", map[string]any{"resourceOwner": "SELF"})
		}, anchor: principal},
		{op: "ListResources", call: func() []byte {
			return call("/listresources", map[string]any{"resourceOwner": "SELF"})
		}, anchor: resource},
		{op: "AssociateResourceShare", call: func() []byte {
			return call("/associateresourceshare", map[string]any{"resourceShareArn": arn, "principals": []string{"111122223333"}})
		}, anchor: "111122223333"},
		{op: "DisassociateResourceShare", call: func() []byte {
			return call("/disassociateresourceshare", map[string]any{"resourceShareArn": arn, "principals": []string{"111122223333"}})
		}, anchor: "111122223333"},
		// Last: it removes the record every case above reads.
		{op: "DeleteResourceShare", call: func() []byte {
			return call("/deleteresourceshare", share)
		}, anchor: ""},
	})

	// After the walk. API_ResourceShare publishes neither principals nor resourceArns, and its dates
	// are numbers under restJson1.
	for op, body := range map[string][]byte{"CreateResourceShare": created, "UpdateResourceShare": updated} {
		var got struct {
			ResourceShare map[string]json.RawMessage `json:"resourceShare"`
		}
		require.NoError(t, json.Unmarshal(body, &got), "decode %s: %s", op, body)
		requireRAMPublishedShare(t, op, got.ResourceShare)
	}
	var list struct {
		ResourceShares []map[string]json.RawMessage `json:"resourceShares"`
	}
	require.NoError(t, json.Unmarshal(listed, &list), "decode GetResourceShares: %s", listed)
	require.Len(t, list.ResourceShares, 1, "GetResourceShares: %s", listed)
	requireRAMPublishedShare(t, "GetResourceShares", list.ResourceShares[0])
}

// requireRAMPublishedShare requires that one answered ResourceShare omits the associations
// API_ResourceShare does not publish and renders both dates as numbers.
func requireRAMPublishedShare(t *testing.T, op string, share map[string]json.RawMessage) {
	t.Helper()
	for _, unpublished := range []string{"principals", "resourceArns"} {
		require.NotContainsf(t, share, unpublished, "%s answered %s, which API_ResourceShare does not publish", op, unpublished)
	}
	for _, date := range []string{"creationTime", "lastUpdatedTime"} {
		var n float64
		require.NoErrorf(t, json.Unmarshal(share[date], &n), "%s must answer %s as epoch seconds, got %s", op, date, share[date])
		require.Positivef(t, n, "%s answered a zero %s", op, date)
	}
}
