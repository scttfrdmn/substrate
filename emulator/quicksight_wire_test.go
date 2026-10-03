package emulator_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for QuickSight's two records
// (#756).
//
// QuickSightDataSource and QuickSightDataSet declare `json:"AccountID"` and `json:"Region"`, neither
// under omitempty, so a record that exists holds both and no absence below is vacuous.
// DescribeDataSource used to answer the data source whole; it answers
// emulator/quicksight_wire.go's projection now. The data set has never been rendered: CreateDataSet
// answers a map of published members and DescribeIngestion reads the record only to confirm it
// exists, so its test is the citation that says so.

// quicksightWireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var quicksightWireClock = time.Unix(1700000000, 0).UTC()

// quicksightWireAccount and quicksightWireRegion scope every request and state key.
const (
	quicksightWireAccount = "123456789012"
	quicksightWireRegion  = "us-east-1"
)

// quicksightBookkeepingMembers are the members QuickSight's records declare and no QuickSight shape
// publishes.
var quicksightBookkeepingMembers = []string{"AccountID", "Region"}

// setupQuickSightWirePlugin returns the QuickSight plugin, a request context and the state manager
// behind it.
func setupQuickSightWirePlugin(t *testing.T) (*emulator.QuickSightPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.QuickSightPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(quicksightWireClock)},
	}), "emulator.QuickSightPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: quicksightWireAccount,
		Region:    quicksightWireRegion,
		RequestID: "req-quicksight-wire",
		IDs:       emulator.NewIDMint("req-quicksight-wire"),
	}, state
}

// quicksightWire issues one REST request and returns the raw response body, failing on anything but
// a 2xx: QuickSight's creates answer 201.
func quicksightWire(t *testing.T, p *emulator.QuickSightPlugin, ctx *emulator.RequestContext, method, path string, body map[string]any) []byte {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err, "marshal %s %s", method, path)
	}
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:    "quicksight",
		HTTPMethod: method,
		Path:       path,
		Body:       raw,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Params:     map[string]string{},
	})
	require.NoError(t, err, "%s %s", method, path)
	require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s %s answered %d: %s", method, path, resp.StatusCode, resp.Body)
	return resp.Body
}

// quicksightWireRequireScoped requires that the stored record at key carries both scope members.
func quicksightWireRequireScoped(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "quicksight", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+quicksightWireAccount+`"`, string(record["AccountID"]), "%s must persist AccountID", key)
	require.JSONEq(t, `"`+quicksightWireRegion+`"`, string(record["Region"]), "%s must persist Region", key)
}

// quicksightWireRun walks each held body as a subtest: the presence anchor first, then the walk.
func quicksightWireRun(t *testing.T, cases []struct {
	op, anchor string
	body       []byte
},
) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			require.Containsf(t, string(tc.body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, tc.body, quicksightBookkeepingMembers)
		})
	}
}

func TestQuickSightWire_DataSourceResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupQuickSightWirePlugin(t)

	const id = "wire-source"
	base := "/accounts/" + quicksightWireAccount + "/data-sources"
	created := quicksightWire(t, p, ctx, "POST", base, map[string]any{"DataSourceId": id, "Name": "wire", "Type": "S3"})
	quicksightWireRequireScoped(t, state, "datasource:"+quicksightWireAccount+"/"+id)
	described := quicksightWire(t, p, ctx, "GET", base+"/"+id, nil)

	quicksightWireRun(t, []struct {
		op, anchor string
		body       []byte
	}{
		{op: "CreateDataSource", anchor: `"DataSourceId":"` + id + `"`, body: created},
		{op: "DescribeDataSource", anchor: `"DataSourceId":"` + id + `"`, body: described},
	})
}

func TestQuickSightWire_DataSetResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupQuickSightWirePlugin(t)

	const id = "wire-set"
	created := quicksightWire(t, p, ctx, "POST", "/accounts/"+quicksightWireAccount+"/data-sets",
		map[string]any{"DataSetId": id, "Name": "wire"})
	quicksightWireRequireScoped(t, state, "dataset:"+quicksightWireAccount+"/"+id)
	var out struct {
		IngestionID string `json:"IngestionId"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateDataSet: %s", created)
	require.NotEmpty(t, out.IngestionID, "CreateDataSet must report an ingestion id")
	ingestion := quicksightWire(t, p, ctx, "GET",
		"/accounts/"+quicksightWireAccount+"/data-sets/"+id+"/ingestions/"+out.IngestionID, nil)

	quicksightWireRun(t, []struct {
		op, anchor string
		body       []byte
	}{
		{op: "CreateDataSet", anchor: `"DataSetId":"` + id + `"`, body: created},
		{op: "DescribeIngestion", anchor: `"IngestionId":"` + out.IngestionID + `"`, body: ingestion},
	})
}
