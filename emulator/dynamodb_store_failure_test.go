package emulator_test

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A DynamoDB write is several state writes — the item, the table's item_keys list and
// its stream ring (#1449 moved the last two under a table lock). Each one's failure must
// reach the caller as an error, never a 200 over a write that did not happen (#1192).
// These sweeps refuse each write an operation makes in turn.

// newDynamoDBServerOver builds a DynamoDB-only server over a caller-supplied store.
func newDynamoDBServerOver(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	p := &emulator.DynamoDBPlugin{}
	require.NoError(t, p.Initialize(context.TODO(), emulator.PluginConfig{
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

func TestDynamoDB_WriteFailuresAreReported(t *testing.T) {
	ops := []struct {
		name string
		seed bool // put item "a" before the operation
		op   string
		body map[string]any
	}{
		{"PutItem create", false, "PutItem", map[string]any{"TableName": "wf", "Item": map[string]any{"pk": av("a")}}},
		{"UpdateItem create", false, "UpdateItem", map[string]any{"TableName": "wf", "Key": map[string]any{"pk": av("a")},
			"UpdateExpression": "SET v = :v", "ExpressionAttributeValues": map[string]any{":v": avn("1")}}},
		{"UpdateItem modify", true, "UpdateItem", map[string]any{"TableName": "wf", "Key": map[string]any{"pk": av("a")},
			"UpdateExpression": "SET v = :v", "ExpressionAttributeValues": map[string]any{":v": avn("1")}}},
		{"DeleteItem", true, "DeleteItem", map[string]any{"TableName": "wf", "Key": map[string]any{"pk": av("a")}}},
		{"BatchWriteItem put", false, "BatchWriteItem", map[string]any{"RequestItems": map[string]any{"wf": []any{
			map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"pk": av("a")}}}}}}},
		{"BatchWriteItem delete", true, "BatchWriteItem", map[string]any{"RequestItems": map[string]any{"wf": []any{
			map[string]any{"DeleteRequest": map[string]any{"Key": map[string]any{"pk": av("a")}}}}}}},
		{"TransactWriteItems", true, "TransactWriteItems", map[string]any{"TransactItems": []any{
			map[string]any{"Update": map[string]any{"TableName": "wf", "Key": map[string]any{"pk": av("a")},
				"UpdateExpression": "SET v = :v", "ExpressionAttributeValues": map[string]any{":v": avn("1")}}},
			map[string]any{"Put": map[string]any{"TableName": "wf", "Item": map[string]any{"pk": av("b")}}},
		}}},
		{"PartiQL INSERT", false, "ExecuteStatement", map[string]any{"Statement": `INSERT INTO "wf" VALUE {"pk": {"S": "a"}}`}},
		{"PartiQL UPDATE", true, "ExecuteStatement", map[string]any{"Statement": `UPDATE "wf" SET v = 1 WHERE pk = 'a'`}},
		{"PartiQL DELETE", true, "ExecuteStatement", map[string]any{"Statement": `DELETE FROM "wf" WHERE pk = 'a'`}},
	}
	setup := func(t *testing.T, seed bool) (*emulator.Server, *failingPutStateManager) {
		t.Helper()
		state := newFailingPutStateManager()
		srv := newDynamoDBServerOver(t, state)
		ddbCreateTable(t, srv, "wf", [3]string{"pk", "S", "HASH"})
		if seed {
			ddbMustOK(t, srv, "PutItem", map[string]any{"TableName": "wf", "Item": map[string]any{"pk": av("a")}})
		}
		return srv, state
	}
	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			srv, state := setup(t, tc.seed)
			state.watch()
			ddbMustOK(t, srv, tc.op, tc.body)
			var plugin []int
			for i, key := range state.writes() {
				if strings.HasPrefix(key, "dynamodb/") {
					plugin = append(plugin, i+1)
				}
			}
			require.NotEmpty(t, plugin, "the operation writes state")
			for _, n := range plugin {
				srv, state := setup(t, tc.seed)
				state.failNth(n)
				status, body := ddbCall(t, srv, tc.op, tc.body)
				assert.GreaterOrEqual(t, status, http.StatusInternalServerError, "write %d refused: %v", n, body)
			}
		})
	}
}
