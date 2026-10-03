package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestAthenaWire_QueryResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for AthenaQuery (#756).
//
// AthenaQuery declares AccountID and Region under wire-visible `json` tags, neither under omitempty,
// and neither reaches a body: every Athena response is a map built member by member. All nine routed
// operations are driven.
func TestAthenaWire_QueryResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.AthenaPlugin{}
	ctx, state := wireSetup(t, p, "req-athena-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "athena", "AmazonAthena", op, body)
	}

	wg := call("CreateWorkGroup", map[string]any{"Name": "wire-wg"})
	started := call("StartQueryExecution", map[string]any{"QueryString": "SELECT 1", "WorkGroup": "wire-wg"})
	var out struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	require.NoError(t, json.Unmarshal(started, &out), "decode StartQueryExecution: %s", started)
	id := out.QueryExecutionID
	wireRequireHeld(t, state, "athena", "query:123456789012/us-east-1/"+id, "AccountID", "Region")

	query := map[string]any{"QueryExecutionId": id}
	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "CreateWorkGroup", held: wg, anchor: "{"},
		{op: "StartQueryExecution", held: started, anchor: id},
		{op: "GetQueryExecution", call: func() []byte { return call("GetQueryExecution", query) }, anchor: id},
		{op: "GetQueryResults", call: func() []byte { return call("GetQueryResults", query) }, anchor: `"ResultSet"`},
		{op: "ListQueryExecutions", call: func() []byte { return call("ListQueryExecutions", map[string]any{"WorkGroup": "wire-wg"}) }, anchor: id},
		{op: "GetWorkGroup", call: func() []byte { return call("GetWorkGroup", map[string]any{"WorkGroup": "wire-wg"}) }, anchor: "wire-wg"},
		{op: "ListWorkGroups", call: func() []byte { return call("ListWorkGroups", nil) }, anchor: "wire-wg"},
		{op: "StopQueryExecution", call: func() []byte { return call("StopQueryExecution", query) }, anchor: "{"},
		{op: "DeleteWorkGroup", call: func() []byte {
			return call("DeleteWorkGroup", map[string]any{"WorkGroup": "wire-wg", "RecursiveDeleteOption": true})
		}, anchor: "{"},
	})
}
