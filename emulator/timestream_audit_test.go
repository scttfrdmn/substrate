package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Timestream's share of the batch audits: pagination (#1195), required members (#1197), published
// statuses (#1198) and CancelQuery (#1206). Every assertion is on the raw JSON, because a typed
// decode cannot tell an absent NextToken from an empty one.

// tsaDoc decodes a raw body into its top-level members.
func tsaDoc(t *testing.T, body []byte) map[string]json.RawMessage {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &doc), "%s", body)
	return doc
}

// tsaString reads a string member.
func tsaString(t *testing.T, doc map[string]json.RawMessage, member string) string {
	t.Helper()
	var s string
	require.NoErrorf(t, json.Unmarshal(doc[member], &s), "%s is a string", member)
	return s
}

// tsaLen reads the length of an array member.
func tsaLen(t *testing.T, doc map[string]json.RawMessage, member string) int {
	t.Helper()
	var items []json.RawMessage
	require.NoErrorf(t, json.Unmarshal(doc[member], &items), "%s is an array", member)
	return len(items)
}

// tsaRefused requires a 400 with code, and returns the message.
func tsaRefused(t *testing.T, status int, body []byte, code string) string {
	t.Helper()
	got, msg := tsqRefusal(t, status, body, http.StatusBadRequest)
	require.Equalf(t, code, got, "%s", body)
	return msg
}

func TestTimestreamAudit_ListDatabasesPagesByMaxResultsAndOmitsTheLastToken(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	for _, n := range []string{"db-e", "db-c", "db-a", "db-d", "db-b"} {
		tsqOK(t, ts, tsqIngestCell, "CreateDatabase", map[string]any{"DatabaseName": n})
	}

	var names []string
	token := ""
	pages := 0
	for {
		req := map[string]any{"MaxResults": 2}
		if token != "" {
			req["NextToken"] = token
		}
		doc := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListDatabases", req))
		var dbs []struct {
			DatabaseName string `json:"DatabaseName"`
		}
		require.NoError(t, json.Unmarshal(doc["Databases"], &dbs))
		for _, d := range dbs {
			names = append(names, d.DatabaseName)
		}
		pages++
		raw, ok := doc["NextToken"]
		if !ok {
			break
		}
		require.NoError(t, json.Unmarshal(raw, &token))
		require.NotEmpty(t, token, "a NextToken is never the empty string API_ListDatabases' 'truncated' sense rules out")
		require.Less(t, pages, 10, "the walk terminates")
	}
	require.Equal(t, 3, pages, "five databases at two a page is three pages, the last carrying no NextToken")
	require.Equal(t, []string{"db-a", "db-b", "db-c", "db-d", "db-e"}, names, "every database once, in name order")

	// No MaxResults is the whole listing, unpaged, as before.
	doc := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListDatabases", map[string]any{}))
	require.Equal(t, 5, tsaLen(t, doc, "Databases"))
	require.NotContains(t, doc, "NextToken")
}

func TestTimestreamAudit_ListTablesDatabaseNameNarrows(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "dba", "ta1")
	tsqOK(t, ts, tsqIngestCell, "CreateTable", map[string]any{"DatabaseName": "dba", "TableName": "ta2"})
	tsqTable(t, ts, "dbb", "tb1")

	all := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListTables", map[string]any{}))
	require.Equal(t, 3, tsaLen(t, all, "Tables"), "without DatabaseName every database's tables are listed")

	narrowed := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListTables", map[string]any{"DatabaseName": "dbb"}))
	require.Equal(t, 1, tsaLen(t, narrowed, "Tables"), "DatabaseName narrows the listing")
	require.Contains(t, string(narrowed["Tables"]), `"TableName":"tb1"`)

	paged := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListTables", map[string]any{"MaxResults": 2}))
	require.Equal(t, 2, tsaLen(t, paged, "Tables"))
	next := tsaString(t, paged, "NextToken")
	last := tsaDoc(t, tsqOK(t, ts, tsqIngestCell, "ListTables", map[string]any{"MaxResults": 2, "NextToken": next}))
	require.Equal(t, 1, tsaLen(t, last, "Tables"))
	require.NotContains(t, last, "NextToken")

	status, body := tsqCall(t, ts, tsqIngestCell, "ListTables", map[string]any{"DatabaseName": "nodb"})
	tsaRefused(t, status, body, "ResourceNotFoundException")
}

func TestTimestreamAudit_ListRefusesAnOutOfRangePageSizeAndAnUnissuedToken(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "rdb", "rt")
	for _, op := range []string{"ListDatabases", "ListTables"} {
		for _, tc := range []struct {
			name string
			body map[string]any
		}{
			{"MaxResults 0", map[string]any{"MaxResults": 0}},
			{"MaxResults 21", map[string]any{"MaxResults": 21}},
			{"an unissued token", map[string]any{"NextToken": "not-a-token"}},
			{"a non-canonical offset", map[string]any{"NextToken": "MDE="}}, // base64("01")
		} {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				status, body := tsqCall(t, ts, tsqIngestCell, op, tc.body)
				tsaRefused(t, status, body, "ValidationException")
			})
		}
	}
}

// tsaWrite writes n single-measure records to db.table.
func tsaWrite(t *testing.T, call func(op string, body any) []byte, db, table string, n, base int) {
	t.Helper()
	records := make([]map[string]any, n)
	for i := range records {
		records[i] = map[string]any{"MeasureName": "m", "MeasureValue": fmt.Sprint(base + i), "Time": fmt.Sprint(1700000000000 + base + i)}
	}
	call("WriteRecords", map[string]any{"DatabaseName": db, "TableName": table, "Records": records})
}

func TestTimestreamAudit_QueryMaxRowsPagesTheResult(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "qdb", "qt")
	write := func(op string, body any) []byte { return tsqOK(t, ts, tsqIngestCell, op, body) }
	tsaWrite(t, write, "qdb", "qt", 5, 0)
	const sql = "SELECT * FROM qdb.qt"

	// "Otherwise, the initial invocation of Query only returns a NextToken."
	first := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 2}))
	require.Equal(t, 0, tsaLen(t, first, "Rows"), "a first call whose result is not smaller than MaxRows answers no rows")
	require.Positive(t, tsaLen(t, first, "ColumnInfo"))
	id := tsaString(t, first, "QueryId")
	token := tsaString(t, first, "NextToken")

	// A write between pages does not move what the snapshot pages hold.
	tsaWrite(t, write, "qdb", "qt", 3, 100)

	var sizes []int
	for range 5 {
		doc := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 2, "NextToken": token}))
		require.Equal(t, id, tsaString(t, doc, "QueryId"), "every page answers the query's own QueryId")
		sizes = append(sizes, tsaLen(t, doc, "Rows"))
		raw, ok := doc["NextToken"]
		if !ok {
			break
		}
		require.NoError(t, json.Unmarshal(raw, &token))
	}
	require.Equal(t, []int{2, 2, 1}, sizes, "five rows at two a page, the last page carrying no NextToken")

	// A MaxRows larger than the result answers it whole, with no token.
	whole := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 100}))
	require.Equal(t, 8, tsaLen(t, whole, "Rows"))
	require.NotContains(t, whole, "NextToken")
}

func TestTimestreamAudit_QueryRefusesWhatItsPageConstrains(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "vdb", "vt")
	tsaWrite(t, func(op string, body any) []byte { return tsqOK(t, ts, tsqIngestCell, op, body) }, "vdb", "vt", 3, 0)
	const sql = "SELECT * FROM vdb.vt"
	first := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 1}))
	token := tsaString(t, first, "NextToken")

	for _, tc := range []struct {
		name, wantMessage string
		body              map[string]any
	}{
		{"no QueryString", "QueryString is required", map[string]any{}},
		{"MaxRows 0", "MaxRows must be between 1 and 1000", map[string]any{"QueryString": sql, "MaxRows": 0}},
		{"MaxRows 1001", "MaxRows must be between 1 and 1000", map[string]any{"QueryString": sql, "MaxRows": 1001}},
		{"a short ClientToken", "ClientToken", map[string]any{"QueryString": sql, "ClientToken": "short"}},
		{"an unissued token", "Invalid pagination token", map[string]any{"QueryString": sql, "NextToken": "bm90LWEtdG9rZW4="}},
		{"another query's string", "Invalid pagination token", map[string]any{"QueryString": "SELECT * FROM vdb.other", "NextToken": token}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tsqCall(t, ts, tsqQueryCell, "Query", tc.body)
			require.Contains(t, tsaRefused(t, status, body, "ValidationException"), tc.wantMessage)
		})
	}
}

func TestTimestreamAudit_CancelQuery(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "cdb", "ct")
	tsaWrite(t, func(op string, body any) []byte { return tsqOK(t, ts, tsqIngestCell, op, body) }, "cdb", "ct", 3, 0)
	const sql = "SELECT * FROM cdb.ct"
	cancel := func(id string) map[string]json.RawMessage {
		return tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "CancelQuery", map[string]any{"QueryId": id}))
	}

	t.Run("a query with pages unread is cancelled, and its next page is refused", func(t *testing.T) {
		live := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 1}))
		id, token := tsaString(t, live, "QueryId"), tsaString(t, live, "NextToken")
		require.Equal(t, "Query cancelled successfully", tsaString(t, cancel(id), "CancellationMessage"))
		require.Equal(t, "Cancellation message is posted", tsaString(t, cancel(id), "CancellationMessage"),
			"a repeated cancellation is idempotent and says the query is already cancelled")
		status, body := tsqCall(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 1, "NextToken": token})
		require.Contains(t, tsaRefused(t, status, body, "ConflictException"), "Unable to poll results for a cancelled query")
	})

	t.Run("a completed query answers that it cannot be cancelled now", func(t *testing.T) {
		done := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql}))
		require.Equal(t, "Cancellation message is posted", tsaString(t, cancel(tsaString(t, done, "QueryId")), "CancellationMessage"))
	})

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"no QueryId", map[string]any{}},
		{"a QueryId outside the pattern", map[string]any{"QueryId": "qid-1"}},
		{"a QueryId over 64 characters", map[string]any{"QueryId": strings.Repeat("a", 65)}},
		{"a well-formed QueryId this account never ran", map[string]any{"QueryId": "abc123"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tsqCall(t, ts, tsqQueryCell, "CancelQuery", tc.body)
			tsaRefused(t, status, body, "ValidationException")
		})
	}
}

func TestTimestreamAudit_WriteRecordsRefusesABatchOutsideOneToAHundred(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "wdb", "wt")
	for _, n := range []int{0, 101} {
		records := make([]map[string]any, n)
		for i := range records {
			records[i] = map[string]any{"MeasureName": "m", "MeasureValue": "1", "Time": fmt.Sprint(i + 1)}
		}
		status, body := tsqCall(t, ts, tsqIngestCell, "WriteRecords", map[string]any{"DatabaseName": "wdb", "TableName": "wt", "Records": records})
		require.Contains(t, tsaRefused(t, status, body, "ValidationException"), "Records must contain 1 to 100")
	}
}

// Every published Timestream error but InternalServerException is a 400 (#1198).
func TestTimestreamAudit_ConflictAndNotFoundAnswer400(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "sdb", "st")
	for _, tc := range []struct {
		name, op, code string
		body           map[string]any
	}{
		{"duplicate database", "CreateDatabase", "ConflictException", map[string]any{"DatabaseName": "sdb"}},
		{"duplicate table", "CreateTable", "ConflictException", map[string]any{"DatabaseName": "sdb", "TableName": "st"}},
		{"missing database", "DescribeDatabase", "ResourceNotFoundException", map[string]any{"DatabaseName": "nodb"}},
		{"missing table", "DescribeTable", "ResourceNotFoundException", map[string]any{"DatabaseName": "sdb", "TableName": "notable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := tsqCall(t, ts, tsqIngestCell, tc.op, tc.body)
			tsaRefused(t, status, body, tc.code)
		})
	}
}

// The two deletes answer `{}`; see docs/services.md for why (#1206).
func TestTimestreamAudit_DeletesAnswerAnEmptyObject(t *testing.T) {
	t.Parallel()
	ts := newTimestreamQueryServer(t, emulator.NewMemoryStateManager())
	tsqTable(t, ts, "ddb", "dt")
	require.JSONEq(t, `{}`, string(tsqOK(t, ts, tsqIngestCell, "DeleteTable", map[string]any{"DatabaseName": "ddb", "TableName": "dt"})))
	require.JSONEq(t, `{}`, string(tsqOK(t, ts, tsqIngestCell, "DeleteDatabase", map[string]any{"DatabaseName": "ddb"})))
}

// A store fault in the new list, query-record and cancellation paths is an error, never a page or a
// cancellation over a record that was not read or written.
func TestTimestreamAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	const sql = "SELECT * FROM fdb.ft"
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		host string
		op   string
		// body is built after the fixture runs, from the live query's id and token.
		body func(id, token string) map[string]any
	}{
		{"ListDatabases, index read", func(m *cfFaultStateManager) { m.failGet = "db_names:" }, tsqIngestCell, "ListDatabases",
			func(string, string) map[string]any { return map[string]any{} }},
		{"ListDatabases, record read", func(m *cfFaultStateManager) { m.failGet = "db:" }, tsqIngestCell, "ListDatabases",
			func(string, string) map[string]any { return map[string]any{} }},
		{"ListTables, table index read", func(m *cfFaultStateManager) { m.failGet = "table_names:" }, tsqIngestCell, "ListTables",
			func(string, string) map[string]any { return map[string]any{} }},
		{"ListTables, database index read", func(m *cfFaultStateManager) { m.failGet = "db_names:" }, tsqIngestCell, "ListTables",
			func(string, string) map[string]any { return map[string]any{} }},
		{"ListTables, table read", func(m *cfFaultStateManager) { m.failGet = "table:" }, tsqIngestCell, "ListTables",
			func(string, string) map[string]any { return map[string]any{} }},
		{"CreateDatabase, existence read", func(m *cfFaultStateManager) { m.failGet = "db:" }, tsqIngestCell, "CreateDatabase",
			func(string, string) map[string]any { return map[string]any{"DatabaseName": "other"} }},
		{"CreateTable, existence read", func(m *cfFaultStateManager) { m.failGet = "table:" }, tsqIngestCell, "CreateTable",
			func(string, string) map[string]any {
				return map[string]any{"DatabaseName": "fdb", "TableName": "other"}
			}},
		{"Query, record write", func(m *cfFaultStateManager) { m.failPut = "query:" }, tsqQueryCell, "Query",
			func(string, string) map[string]any { return map[string]any{"QueryString": sql} }},
		{"Query, record read on a token", func(m *cfFaultStateManager) { m.failGet = "query:" }, tsqQueryCell, "Query",
			func(_, tok string) map[string]any {
				return map[string]any{"QueryString": sql, "MaxRows": 1, "NextToken": tok}
			}},
		{"Query, corrupt record on a token", func(m *cfFaultStateManager) { m.corruptGet = "query:" }, tsqQueryCell, "Query",
			func(_, tok string) map[string]any {
				return map[string]any{"QueryString": sql, "MaxRows": 1, "NextToken": tok}
			}},
		{"CancelQuery, record read", func(m *cfFaultStateManager) { m.failGet = "query:" }, tsqQueryCell, "CancelQuery",
			func(id, _ string) map[string]any { return map[string]any{"QueryId": id} }},
		{"CancelQuery, record write", func(m *cfFaultStateManager) { m.failPut = "query:" }, tsqQueryCell, "CancelQuery",
			func(id, _ string) map[string]any { return map[string]any{"QueryId": id} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			ts := newTimestreamQueryServer(t, fault)
			tsqTable(t, ts, "fdb", "ft")
			tsaWrite(t, func(op string, body any) []byte { return tsqOK(t, ts, tsqIngestCell, op, body) }, "fdb", "ft", 2, 0)
			live := tsaDoc(t, tsqOK(t, ts, tsqQueryCell, "Query", map[string]any{"QueryString": sql, "MaxRows": 1}))
			id, token := tsaString(t, live, "QueryId"), tsaString(t, live, "NextToken")
			tc.arm(fault)
			status, body := tsqCall(t, ts, tc.host, tc.op, tc.body(id, token))
			require.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, body)
		})
	}
}
