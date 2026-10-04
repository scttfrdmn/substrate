package emulator_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Timestream Write answers CreationTime and LastUpdatedTime as epoch seconds (#1207).
//
// API_Database and API_Table type both as Timestamp, which awsJson publishes as a JSON number with
// fractional seconds. Until #1207 every database and table response answered RFC3339 strings, a
// whole second at a time, so a typed SDK could not decode any of them. These assertions are on the
// raw bytes: a decode into a time.Time cannot tell a number from a string once it has succeeded.

// timestreamWireClock is a sub-second instant, so a rendered date that dropped its fraction fails.
var timestreamWireClock = time.Unix(1700000000, 123456789).UTC()

// timestreamWireEpoch is timestreamWireClock as EpochSeconds renders it, to three decimals.
const timestreamWireEpoch = "1700000000.123"

// setupTimestreamWirePlugin returns the Timestream plugin, a request context and its state manager,
// on a clock frozen at timestreamWireClock. Freeze then SetTime, in the order TimeController.Freeze
// documents.
func setupTimestreamWirePlugin(t *testing.T) (*emulator.TimestreamPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(timestreamWireClock)
	tc.Freeze()
	tc.SetTime(timestreamWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.TimestreamPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.TimestreamPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-timestream-wire",
		IDs:       emulator.NewIDMint("req-timestream-wire"),
	}, state
}

// timestreamWireDates requires that the element at path in body carries both dates as the exact JSON
// number timestreamWireEpoch. path names a top-level member and, for a list, its first element.
func timestreamWireDates(t *testing.T, op string, body []byte, member string, list bool) {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc), "decode %s: %s", op, body)
	raw := doc[member]
	if list {
		var items []json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &items), "decode %s.%s: %s", op, member, body)
		require.NotEmpty(t, items, "%s answered no %s: %s", op, member, body)
		raw = items[0]
	}
	var element map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &element), "decode %s's element: %s", op, body)
	for _, date := range []string{"CreationTime", "LastUpdatedTime"} {
		require.Equalf(t, timestreamWireEpoch, string(element[date]),
			"%s must answer %s as epoch seconds with its fraction, a JSON number: %s", op, date, body)
	}
}

func TestTimestreamWire_DatesAreEpochSeconds(t *testing.T) {
	t.Parallel()
	p, ctx, _ := setupTimestreamWirePlugin(t)
	call := func(op string, body map[string]any) []byte {
		return wireJSONTarget(t, p, ctx, "timestream", "Timestream_20181101", op, body)
	}

	db := map[string]any{"DatabaseName": "wire-db"}
	table := map[string]any{"DatabaseName": "wire-db", "TableName": "wire-table"}
	for _, tc := range []struct {
		op     string
		body   []byte
		member string
		list   bool
	}{
		{"CreateDatabase", call("CreateDatabase", db), "Database", false},
		{"DescribeDatabase", call("DescribeDatabase", db), "Database", false},
		{"ListDatabases", call("ListDatabases", nil), "Databases", true},
		{"CreateTable", call("CreateTable", table), "Table", false},
		{"DescribeTable", call("DescribeTable", table), "Table", false},
		{"ListTables", call("ListTables", db), "Tables", true},
	} {
		t.Run(tc.op, func(t *testing.T) {
			timestreamWireDates(t, tc.op, tc.body, tc.member, tc.list)
		})
	}
}

// #1207's fallback. A record written before the fix holds the whole-second RFC3339 string the field
// was then declared as. It must still decode, and answer that instant as epoch seconds.
func TestTimestreamWire_ARecordWrittenBeforeTheFixStillDecodes(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupTimestreamWirePlugin(t)

	const old = `{"DatabaseName":"old-db","Arn":"arn:aws:timestream:us-east-1:123456789012:database/old-db",` +
		`"TableCount":0,"CreationTime":"2023-11-14T22:13:20Z","LastUpdatedTime":"2023-11-14T22:13:20Z"}`
	require.NoError(t, state.Put(t.Context(), "timestream", "db:123456789012/us-east-1/old-db", []byte(old)))
	keys, err := state.List(t.Context(), "timestream", "")
	require.NoError(t, err)
	require.Contains(t, keys, "db:123456789012/us-east-1/old-db", "the fixture must sit at the key the plugin reads")

	body := wireJSONTarget(t, p, ctx, "timestream", "Timestream_20181101", "DescribeDatabase", map[string]any{"DatabaseName": "old-db"})
	var doc struct {
		Database map[string]json.RawMessage `json:"Database"`
	}
	require.NoError(t, json.Unmarshal(body, &doc), "decode DescribeDatabase: %s", body)
	for _, date := range []string{"CreationTime", "LastUpdatedTime"} {
		require.Equalf(t, "1700000000.000", string(doc.Database[date]),
			"a record stored as RFC3339 must answer %s as epoch seconds: %s", date, body)
	}
}
