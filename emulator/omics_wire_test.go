package emulator_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestOmicsWire_RunResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for OmicsRun (#756).
//
// OmicsRun declares AccountID and Region under wire-visible `json` tags, never under omitempty, so a
// record that exists holds both and no absence below is vacuous. GetRun used to answer the record
// whole; it answers emulator/omics_wire.go's projection now. StartRun and ListRuns build their bodies
// member by member, and CancelRun answers 204 with no body, so it has nothing to walk.
func TestOmicsWire_RunResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.OmicsPlugin{}
	ctx, state := wireSetup(t, p, "req-omics-wire")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "omics", method, path, body)
	}

	started := call(http.MethodPost, "/run", map[string]any{
		"workflowId": "1234567", "workflowType": "PRIVATE", "name": "wire-run",
		"roleArn": "arn:aws:iam::123456789012:role/wire", "outputUri": "s3://wire-bucket/out",
	})
	var run struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(started, &run), "decode StartRun: %s", started)
	require.NotEmpty(t, run.ID, "StartRun must report an id: %s", started)
	wireRequireHeld(t, state, "omics", "run:123456789012/us-east-1/"+run.ID, "accountID", "region")

	got := call(http.MethodGet, "/run/"+run.ID, nil)
	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "StartRun", held: started, anchor: `"id":"` + run.ID + `"`},
		{op: "GetRun", held: got, anchor: `"name":"wire-run"`},
		{op: "ListRuns", call: func() []byte { return call(http.MethodGet, "/run", nil) }, anchor: `"id":"` + run.ID + `"`},
	})

	// After the walk. GetRun answers the published members the record models, and no others.
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got, &out), "decode GetRun: %s", got)
	require.Equal(t, []string{"id", "name", "outputUri", "roleArn", "status", "workflowId", "workflowType"},
		slices.Sorted(maps.Keys(out)), "GetRun: %s", got)
}

// TestOmicsWire_ACancelledRunReportsThePublishedStatus pins the status CancelRun leaves a run in to
// the spelling API_GetRun and API_RunListItem publish, CANCELLED, on the raw bytes of both reads
// (#1364). A typed SDK's enum would read the old CANCELED as an unknown value.
func TestOmicsWire_ACancelledRunReportsThePublishedStatus(t *testing.T) {
	t.Parallel()
	p := &emulator.OmicsPlugin{}
	ctx, _ := wireSetup(t, p, "req-omics-cancel")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "omics", method, path, body)
	}

	started := call(http.MethodPost, "/run", map[string]any{
		"workflowId": "1234567", "workflowType": "PRIVATE", "name": "cancel-run",
		"roleArn": "arn:aws:iam::123456789012:role/wire", "outputUri": "s3://wire-bucket/out",
	})
	var run struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(started, &run), "decode StartRun: %s", started)
	call(http.MethodDelete, "/run/"+run.ID, nil)

	for op, body := range map[string][]byte{
		"GetRun":   call(http.MethodGet, "/run/"+run.ID, nil),
		"ListRuns": call(http.MethodGet, "/run", nil),
	} {
		require.Containsf(t, string(body), `"status":"CANCELLED"`, "%s must answer the published CANCELLED: %s", op, body)
		require.NotContainsf(t, string(body), `"CANCELED"`, "%s answered CANCELED, which no Omics page publishes: %s", op, body)
	}
}
