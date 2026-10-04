package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Timestream's Query result shape, its unseeded fallback, and endpoint discovery (#1209). The
// assertions are on the raw JSON, because a typed decode cannot tell a NullValue from an absent
// ScalarValue, or a missing NextToken from an empty one.

// The cell hosts DescribeEndpoints hands out, and the regional hosts a client discovers them from.
const (
	tsqIngestCell = "ingest-cell1.timestream.us-east-1.amazonaws.com"
	tsqQueryCell  = "query-cell1.timestream.us-east-1.amazonaws.com"
	tsqIngest     = "ingest.timestream.us-east-1.amazonaws.com"
	tsqQuery      = "query.timestream.us-east-1.amazonaws.com"
)

// newTimestreamQueryServer builds a server with the Timestream plugin over state, on a frozen clock.
func newTimestreamQueryServer(t *testing.T, state emulator.StateManager) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	p := &emulator.TimestreamPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State: state, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	ts := httptest.NewServer(emulator.NewServer(*emulator.DefaultConfig(), registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// tsqCall sends one Timestream operation to host and returns the status and raw body.
func tsqCall(t *testing.T, ts *httptest.Server, host, op string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "Timestream_20181101."+op)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// tsqOK sends an operation that must succeed and returns its raw body.
func tsqOK(t *testing.T, ts *httptest.Server, host, op string, body any) []byte {
	t.Helper()
	status, out := tsqCall(t, ts, host, op, body)
	require.Equalf(t, http.StatusOK, status, "%s at %s: %s", op, host, out)
	return out
}

// tsqRefusal requires a refusal and returns its code and message.
func tsqRefusal(t *testing.T, status int, body []byte, wantStatus int) (code, message string) {
	t.Helper()
	require.Equalf(t, wantStatus, status, "want %d: %s", wantStatus, body)
	var doc struct {
		Type     string `json:"__type"`
		Message  string `json:"message"`
		Message2 string `json:"Message"`
	}
	require.NoError(t, json.Unmarshal(body, &doc), "%s", body)
	if _, after, ok := strings.Cut(doc.Type, "#"); ok {
		doc.Type = after
	}
	if doc.Message == "" {
		doc.Message = doc.Message2
	}
	return doc.Type, doc.Message
}

// tsqTable creates a database and table through the Write API's cell host.
func tsqTable(t *testing.T, ts *httptest.Server, db, table string) {
	t.Helper()
	tsqOK(t, ts, tsqIngestCell, "CreateDatabase", map[string]any{"DatabaseName": db})
	tsqOK(t, ts, tsqIngestCell, "CreateTable", map[string]any{"DatabaseName": db, "TableName": table})
}

func TestTimestreamQuery_SelectAllAnswersTheTableInItsLogicalShape(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "qdb", "metrics")

	tsqOK(t, ts, tsqIngestCell, "WriteRecords", map[string]any{
		"DatabaseName": "qdb", "TableName": "metrics",
		// CommonAttributes merge into every record: the shared dimension comes first, and a record's
		// own TimeUnit wins over the common one.
		"CommonAttributes": map[string]any{
			"Dimensions": []map[string]any{{"Name": "region", "Value": "us-east-1"}},
			"TimeUnit":   "SECONDS",
		},
		"Records": []map[string]any{
			{"Dimensions": []map[string]any{{"Name": "host", "Value": "h1"}},
				"MeasureName": "cpu", "MeasureValue": "35.5", "Time": "1700000000"},
			{"Dimensions": []map[string]any{{"Name": "host", "Value": "h2"}},
				"MeasureName": "requests", "MeasureValue": "7", "MeasureValueType": "BIGINT",
				"Time": "1700000001500", "TimeUnit": "MILLISECONDS"},
			{"Dimensions": []map[string]any{{"Name": "host", "Value": "h3"}, {"Name": "az", "Value": "a"}},
				"MeasureName": "status", "MeasureValueType": "MULTI", "Time": "1700000002",
				"MeasureValues": []map[string]any{
					{"Name": "healthy", "Value": "TRUE", "Type": "BOOLEAN"},
					{"Name": "since", "Value": "1699999999", "Type": "TIMESTAMP"},
				}},
		},
	})

	body := tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": `SELECT * FROM "qdb"."metrics";`})
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc), "%s", body)

	require.JSONEq(t, `[
		{"Name":"time","Type":{"ScalarType":"TIMESTAMP"}},
		{"Name":"region","Type":{"ScalarType":"VARCHAR"}},
		{"Name":"host","Type":{"ScalarType":"VARCHAR"}},
		{"Name":"az","Type":{"ScalarType":"VARCHAR"}},
		{"Name":"measure_name","Type":{"ScalarType":"VARCHAR"}},
		{"Name":"measure_value::double","Type":{"ScalarType":"DOUBLE"}},
		{"Name":"measure_value::bigint","Type":{"ScalarType":"BIGINT"}},
		{"Name":"healthy","Type":{"ScalarType":"BOOLEAN"}},
		{"Name":"since","Type":{"ScalarType":"TIMESTAMP"}}
	]`, string(doc["ColumnInfo"]), "ColumnInfo: time, the dimensions as first written, measure_name, then each measure")

	require.JSONEq(t, `[
		{"Data":[{"ScalarValue":"2023-11-14 22:13:20.000000000"},{"ScalarValue":"us-east-1"},{"ScalarValue":"h1"},{"NullValue":true},
			{"ScalarValue":"cpu"},{"ScalarValue":"35.5"},{"NullValue":true},{"NullValue":true},{"NullValue":true}]},
		{"Data":[{"ScalarValue":"2023-11-14 22:13:21.500000000"},{"ScalarValue":"us-east-1"},{"ScalarValue":"h2"},{"NullValue":true},
			{"ScalarValue":"requests"},{"NullValue":true},{"ScalarValue":"7"},{"NullValue":true},{"NullValue":true}]},
		{"Data":[{"ScalarValue":"2023-11-14 22:13:22.000000000"},{"ScalarValue":"us-east-1"},{"ScalarValue":"h3"},{"ScalarValue":"a"},
			{"ScalarValue":"status"},{"NullValue":true},{"NullValue":true},{"ScalarValue":"true"},{"ScalarValue":"2023-11-14 22:13:19.000000000"}]}
	]`, string(doc["Rows"]), "Rows: a value per type, NullValue where a record has no such column")

	_, hasToken := doc["NextToken"]
	require.False(t, hasToken, "NextToken is omitted for a whole result; API_query_Query's minimum length is 1: %s", body)

	var status struct {
		ProgressPercentage     float64
		CumulativeBytesScanned int64
		CumulativeBytesMetered int64
	}
	require.NoError(t, json.Unmarshal(doc["QueryStatus"], &status), "%s", body)
	require.InDelta(t, 100.0, status.ProgressPercentage, 0, "a synchronous query is complete")
	compact := new(bytes.Buffer)
	require.NoError(t, json.Compact(compact, doc["Rows"]))
	require.Equal(t, int64(compact.Len()), status.CumulativeBytesScanned, "bytes scanned is the answered Rows' size")
	require.Equal(t, status.CumulativeBytesScanned, status.CumulativeBytesMetered)
}

func TestTimestreamQuery_AnUnseededQueryItCannotAnswerIsRefused(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "qdb", "metrics")
	tsqOK(t, ts, tsqIngestCell, "WriteRecords", map[string]any{
		"DatabaseName": "qdb", "TableName": "metrics",
		"Records": []map[string]any{{"MeasureName": "cpu", "MeasureValue": "1", "Time": "1700000000000"}},
	})
	for _, qs := range []string{
		"SELECT 1",
		"SELECT time, measure_value::double FROM qdb.metrics",
		"SELECT count(*) FROM qdb.metrics",
		"SELECT * FROM qdb.metrics WHERE measure_name = 'cpu'",
		"SELECT * FROM qdb.metrics LIMIT 1",
		"SELECT * FROM qdb.metrics ORDER BY time",
		"SELECT * FROM metrics",
	} {
		t.Run(qs, func(t *testing.T) {
			status, body := tsqCall(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": qs})
			code, message := tsqRefusal(t, status, body, http.StatusBadRequest)
			require.Equal(t, "ValidationException", code, "%s", body)
			require.Contains(t, message, "/v1/timestream-query/results", "the refusal names the seed endpoint: %s", body)
		})
	}
	t.Run("a table that does not exist", func(t *testing.T) {
		status, body := tsqCall(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": "SELECT * FROM qdb.absent"})
		code, message := tsqRefusal(t, status, body, http.StatusBadRequest)
		require.Equal(t, "ValidationException", code, "%s", body)
		require.Contains(t, message, "qdb.absent", "%s", body)
	})
	t.Run("a table with no records answers an empty result", func(t *testing.T) {
		tsqOK(t, ts, tsqIngestCell, "CreateTable", map[string]any{"DatabaseName": "qdb", "TableName": "empty"})
		body := tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": "SELECT * FROM qdb.empty"})
		require.Contains(t, string(body), `"Rows":[]`, "%s", body)
	})
}

// A seeded result is answered as seeded, ahead of any reconstruction, and may carry its own
// QueryStatus. This is the mechanism for any query whose columns, order or row count matter.
func TestTimestreamQuery_ASeededResultIsAnsweredAsSeeded(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	seed, err := json.Marshal(map[string]any{
		"queryString": "SELECT count(*) AS n FROM qdb.metrics",
		"result": map[string]any{
			"ColumnInfo":  []map[string]any{{"Name": "n", "Type": map[string]any{"ScalarType": "BIGINT"}}},
			"Rows":        []map[string]any{{"Data": []map[string]any{{"ScalarValue": "3"}}}},
			"QueryStatus": map[string]any{"ProgressPercentage": 100, "CumulativeBytesScanned": 10485760, "CumulativeBytesMetered": 10485760},
		},
	})
	require.NoError(t, err)
	resp, err := http.Post(ts.URL+"/v1/timestream-query/results", "application/json", bytes.NewReader(seed)) //nolint:noctx
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": "SELECT count(*) AS n FROM qdb.metrics"})
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc))
	require.JSONEq(t, `[{"Name":"n","Type":{"ScalarType":"BIGINT"}}]`, string(doc["ColumnInfo"]))
	require.JSONEq(t, `[{"Data":[{"ScalarValue":"3"}]}]`, string(doc["Rows"]))
	require.JSONEq(t, `{"ProgressPercentage":100,"CumulativeBytesScanned":10485760,"CumulativeBytesMetered":10485760}`, string(doc["QueryStatus"]))
}

func TestTimestreamEndpoints_DescribeEndpointsAnswersTheCellOfTheAPIItWasCalledOn(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	for _, tc := range []struct {
		host, want string
	}{
		{tsqQuery, tsqQueryCell},
		{tsqIngest, tsqIngestCell},
		{tsqQueryCell, tsqQueryCell},
		{tsqIngestCell, tsqIngestCell},
		{"query.timestream.eu-west-1.amazonaws.com", "query-cell1.timestream.eu-west-1.amazonaws.com"},
		// A host naming neither API answers itself: an --endpoint-url caller has nothing else to use.
		{"timestream.us-east-1.amazonaws.com", "timestream.us-east-1.amazonaws.com"},
	} {
		t.Run(tc.host, func(t *testing.T) {
			body := tsqOK(t, ts, tc.host, "DescribeEndpoints", map[string]any{})
			require.JSONEq(t, `{"Endpoints":[{"Address":"`+tc.want+`","CachePeriodInMinutes":1}]}`, string(body))
		})
	}
}

func TestTimestreamEndpoints_ARequestAtAnEndpointThatDoesNotServeItIsRefused(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "edb", "t")
	write := map[string]any{"DatabaseName": "edb", "TableName": "t",
		"Records": []map[string]any{{"MeasureName": "m", "MeasureValue": "1", "Time": "1"}}}
	query := map[string]any{"QueryString": "SELECT * FROM edb.t"}
	const writeSentence, querySentence = "The requested endpoint was not valid.", "The requested endpoint is invalid."
	for _, tc := range []struct {
		name, host, op string
		body           any
		sentence       string
	}{
		{"a write at the query cell", tsqQueryCell, "WriteRecords", write, writeSentence},
		{"a database create at the query cell", tsqQueryCell, "CreateDatabase", map[string]any{"DatabaseName": "x1"}, writeSentence},
		{"a query at the ingest cell", tsqIngestCell, "Query", query, querySentence},
		{"a cancel at the ingest cell", tsqIngestCell, "CancelQuery", map[string]any{"QueryId": "q1"}, querySentence},
		{"a query at the regional query host", tsqQuery, "Query", query, querySentence},
		{"a write at the regional ingest host", tsqIngest, "WriteRecords", write, writeSentence},
		{"a table list at the regional ingest host", tsqIngest, "ListTables", map[string]any{"DatabaseName": "edb"}, writeSentence},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tsqCall(t, ts, tc.host, tc.op, tc.body)
			code, message := tsqRefusal(t, status, body, http.StatusBadRequest)
			require.Equal(t, "InvalidEndpointException", code, "%s", body)
			require.True(t, strings.HasPrefix(message, tc.sentence), "the page's sentence leads: %s", message)
		})
	}
	t.Run("the right endpoint serves both", func(t *testing.T) {
		tsqOK(t, ts, tsqIngestCell, "WriteRecords", write)
		require.Contains(t, string(tsqOK(t, ts, tsqQueryCell, "Query", query)), `"ScalarValue":"m"`)
	})
	t.Run("a host naming neither API enforces nothing", func(t *testing.T) {
		tsqOK(t, ts, "timestream.us-east-1.amazonaws.com", "WriteRecords", write)
		tsqOK(t, ts, "timestream.us-east-1.amazonaws.com", "Query", query)
	})
}

// A store fault in the record and seed paths is an error, never an empty result or a 200 over records
// that were not written.
func TestTimestreamQuery_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	write := map[string]any{"DatabaseName": "fdb", "TableName": "t",
		"Records": []map[string]any{{"MeasureName": "m", "MeasureValue": "1", "Time": "1"}}}
	query := map[string]any{"QueryString": "SELECT * FROM fdb.t"}
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		host string
		op   string
		body any
	}{
		{"WriteRecords, records read", func(m *cfFaultStateManager) { m.failGet = "records:" }, tsqIngestCell, "WriteRecords", write},
		{"WriteRecords, corrupt records", func(m *cfFaultStateManager) { m.corruptGet = "records:" }, tsqIngestCell, "WriteRecords", write},
		{"WriteRecords, records write", func(m *cfFaultStateManager) { m.failPut = "records:" }, tsqIngestCell, "WriteRecords", write},
		{"WriteRecords, table read", func(m *cfFaultStateManager) { m.failGet = "table:" }, tsqIngestCell, "WriteRecords", write},
		{"WriteRecords, database read", func(m *cfFaultStateManager) { m.failGet = "db:" }, tsqIngestCell, "WriteRecords", write},
		{"Query, seed read", func(m *cfFaultStateManager) { m.failGet = "result:" }, tsqQueryCell, "Query", query},
		{"Query, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "result:*" }, tsqQueryCell, "Query", query},
		{"Query, records read", func(m *cfFaultStateManager) { m.failGet = "records:" }, tsqQueryCell, "Query", query},
		{"Query, corrupt records", func(m *cfFaultStateManager) { m.corruptGet = "records:" }, tsqQueryCell, "Query", query},
		{"Query, table read", func(m *cfFaultStateManager) { m.failGet = "table:" }, tsqQueryCell, "Query", query},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			ts := newTimestreamQueryServer(t, fault)
			tsqTable(t, ts, "fdb", "t")
			tsqOK(t, ts, tsqIngestCell, "WriteRecords", write)
			if strings.Contains(tc.name, "corrupt seed") {
				seed := []byte(`{"queryString":"*","result":{"Rows":[],"ColumnInfo":[]}}`)
				resp, err := http.Post(ts.URL+"/v1/timestream-query/results", "application/json", bytes.NewReader(seed)) //nolint:noctx
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}
			tc.arm(fault)
			status, body := tsqCall(t, ts, tc.host, tc.op, tc.body)
			require.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, body)
		})
	}
}
