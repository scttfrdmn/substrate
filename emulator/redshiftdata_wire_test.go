package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestRedshiftDataWire_StatementResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for RedshiftDataStatement (#756).
//
// RedshiftDataStatement declares AccountID and Region under wire-visible `json` tags, neither under
// omitempty, and neither reaches a body: DescribeStatement answers a map built member by member. Its
// CreatedAt is a published member, and is pinned here as the number API_DescribeStatement publishes
// rather than the RFC3339 string #1305 recorded.
func TestRedshiftDataWire_StatementResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.RedshiftDataPlugin{}
	ctx, state := wireSetup(t, p, "req-redshiftdata-wire")
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "redshift-data", "RedshiftData_20191217", op, body)
	}

	executed := call("ExecuteStatement", map[string]any{"Sql": "SELECT 1", "ClusterIdentifier": "wire", "Database": "dev"})
	var out struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(executed, &out), "decode ExecuteStatement: %s", executed)
	wireRequireHeld(t, state, "redshift-data", "statement:123456789012/us-east-1/"+out.ID, "AccountID", "Region")

	stmt := map[string]any{"Id": out.ID}
	described := call("DescribeStatement", stmt)
	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "ExecuteStatement", held: executed, anchor: out.ID},
		{op: "DescribeStatement", held: described, anchor: out.ID},
		{op: "GetStatementResult", call: func() []byte { return call("GetStatementResult", stmt) }, anchor: `"Records"`},
	})

	// After the walk. API_DescribeStatement publishes CreatedAt and UpdatedAt as numbers.
	var desc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(described, &desc), "decode DescribeStatement: %s", described)
	for _, member := range []string{"CreatedAt", "UpdatedAt"} {
		var n float64
		require.NoErrorf(t, json.Unmarshal(desc[member], &n), "%s must be a number: %s", member, described)
	}
}
