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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for Budget (#756).
//
// Budget declares AccountId, Region and CreatedAt under wire-visible `json` tags, none under omitempty,
// so a record that exists holds all three and no absence below is vacuous. DescribeBudget and
// DescribeBudgets used to answer the record whole; they answer emulator/budgets_wire.go's projection
// now. The other three routed operations answer empty objects and are driven anyway.

// budgetsWireAccount scopes every state key the plugin writes. Budgets is a global service; the
// account a request names is the only scope.
const budgetsWireAccount = "123456789012"

// budgetsBookkeepingMembers are the members Budget declares and no Budgets response publishes.
// AccountId is a request member, but no response element carries it.
var budgetsBookkeepingMembers = []string{"AccountId", "Region", "CreatedAt"}

// setupBudgetsWirePlugin returns the Budgets plugin, a request context and the state manager behind
// it.
func setupBudgetsWirePlugin(t *testing.T) (*emulator.BudgetsPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.BudgetsPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}), "emulator.BudgetsPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: budgetsWireAccount,
		Region:    "us-east-1",
		RequestID: "req-budgets-wire",
		IDs:       emulator.NewIDMint("req-budgets-wire"),
	}, state
}

// budgetsWire issues one JSON-target operation and returns the raw response body, failing on anything
// but 200.
func budgetsWire(t *testing.T, p *emulator.BudgetsPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "budgets",
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": "AmazonBudgetServiceGateway." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

func TestBudgetsWire_BudgetResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupBudgetsWirePlugin(t)

	const name = "wire-budget"
	budget := map[string]any{
		"BudgetName":  name,
		"BudgetType":  "COST",
		"TimeUnit":    "MONTHLY",
		"BudgetLimit": map[string]string{"Amount": "100", "Unit": "USD"},
	}
	created := budgetsWire(t, p, ctx, "CreateBudget", map[string]any{"AccountId": budgetsWireAccount, "Budget": budget})

	data, err := state.Get(t.Context(), "budgets", "budget:"+budgetsWireAccount+"/"+name)
	require.NoError(t, err, "state.Get budget")
	require.NotNil(t, data, "no budget record stored")
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode budget: %s", data)
	for _, member := range budgetsBookkeepingMembers {
		require.NotEmptyf(t, record[member], "the budget must persist %s before an absence assertion on it means anything", member)
		require.NotEqualf(t, `""`, string(record[member]), "the budget persists an empty %s", member)
	}

	ref := map[string]any{"AccountId": budgetsWireAccount, "BudgetName": name}
	updated := map[string]any{
		"BudgetName":  name,
		"BudgetType":  "COST",
		"TimeUnit":    "MONTHLY",
		"BudgetLimit": map[string]string{"Amount": "200", "Unit": "USD"},
	}
	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "CreateBudget", held: created, anchor: "{}"},
		{op: "DescribeBudget", body: ref, anchor: `"BudgetName":"` + name + `"`},
		{op: "DescribeBudgets", body: map[string]any{"AccountId": budgetsWireAccount}, anchor: `"BudgetName":"` + name + `"`},
		{op: "UpdateBudget", body: map[string]any{"AccountId": budgetsWireAccount, "NewBudget": updated}, anchor: "{}"},
		// Last: it removes the record every case above reads.
		{op: "DeleteBudget", body: ref, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = budgetsWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, budgetsBookkeepingMembers)
		})
	}
}
