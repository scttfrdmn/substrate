package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// FilterLogEvents matched filterPattern as one literal substring until #1596, so a JSON pattern matched
// nothing and `ERROR -ARGUMENTS` matched only a message containing that exact text. The cases below are
// the CloudWatch Logs User Guide's own examples ("Filter pattern syntax for metric filters, subscription
// filters, filter log events, and Live Tail"), run against the page's own sample events.

// cwlFilterMessages are the events every case is matched against: the page's unstructured examples, its
// two JSON sample events, and a few that pin the edges (a non-JSON message, a numeric string).
var cwlFilterMessages = []string{
	"[ERROR 400] BAD REQUEST",
	"[ERROR 419] MISSING ARGUMENTS",
	"[INFO] REQUEST FAILED",
	"[ERROR 500] INTERNAL SERVER ERROR",
	`{"eventType":"UpdateTrail","sourceIPAddress":"111.111.111.111","arrayKey":["value","another value"],` +
		`"objectList":[{"name":"a","id":1},{"name":"b","id":2}],"SomeObject":null,"cluster.name":"c","flag":true}`,
	`{"user":{"id":1,"email":"John.Stiles@example.com"},"users":[{"id":2,"email":"John.Doe@example.com"},` +
		`{"id":3,"email":"Jane.Doe@example.com"}],"actions":["GET","PUT","DELETE"],` +
		`"coordinates":[[0,1,2],[4,5,6],[7,8,9]],"bandwidth":80,"number":[1e3],"code":"404"}`,
	`{"task_uuid":"0b5f3f8e-6c1e-4c1a-9f0e-3c1d2b1a0f9e","status":"done","latency":12.5}`,
}

// cwlFilterPutAll seeds one stream with every message in cwlFilterMessages.
func cwlFilterPutAll(t *testing.T, srv *emulator.Server, group string) {
	t.Helper()
	require.Equal(t, http.StatusOK, cwLogsRequest(t, srv, "CreateLogGroup", map[string]any{"logGroupName": group}).StatusCode)
	require.Equal(t, http.StatusOK, cwLogsRequest(t, srv, "CreateLogStream",
		map[string]any{"logGroupName": group, "logStreamName": "s"}).StatusCode)
	events := make([]map[string]any, len(cwlFilterMessages))
	for i, m := range cwlFilterMessages {
		events[i] = map[string]any{"timestamp": 1700000000000 + int64(i), "message": m}
	}
	resp := cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName": group, "logStreamName": "s", "logEvents": events,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// cwlFilterIndexes runs FilterLogEvents with pattern and returns the indexes into cwlFilterMessages of
// the events it answered, or the error type of a refusal.
func cwlFilterIndexes(t *testing.T, srv *emulator.Server, group, pattern string) ([]int, string) {
	t.Helper()
	resp := cwLogsRequest(t, srv, "FilterLogEvents", map[string]any{"logGroupName": group, "filterPattern": pattern})
	raw := cwLogsReadBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", raw)
		return nil, cwLogsErrorTypeFrom(t, raw)
	}
	var out struct {
		Events []struct {
			Message string `json:"message"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	idx := []int{}
	for _, ev := range out.Events {
		for i, m := range cwlFilterMessages {
			if ev.Message == m {
				idx = append(idx, i)
			}
		}
	}
	return idx, ""
}

func TestCWLogs_FilterPatternsMatchAsPublished(t *testing.T) {
	t.Parallel()
	srv := newCWLogsTestServer(t)
	cwlFilterPutAll(t, srv, "/filter/patterns")

	cases := []struct {
		name    string
		pattern string
		want    []int
	}{
		{"empty matches all", "", []int{0, 1, 2, 3, 4, 5, 6}},
		{"match everything", `" "`, []int{0, 1, 2, 3, 4, 5, 6}},
		{"single term", "ERROR", []int{0, 1, 3}},
		{"terms are case-sensitive", "error", []int{}},
		{"multiple terms are AND", "ERROR ARGUMENTS", []int{1}},
		{"optional terms are OR", "?ARGUMENTS ?FAILED", []int{1, 2}},
		{"optional terms are ignored beside a required one", "?ARGUMENTS ?FAILED REQUEST", []int{0, 2}},
		{"exact phrase", `"INTERNAL SERVER ERROR"`, []int{3}},
		{"exclusion", "ERROR -ARGUMENTS", []int{0, 3}},
		{"exclusion alone", "-ERROR", []int{2, 4, 5, 6}},
		{"excluded phrase", `ERROR -"BAD REQUEST"`, []int{1, 3}},
		{"quoted term with non-alphanumerics", `"[ERROR 400]"`, []int{0}},

		{"json string equality", `{ $.eventType = "UpdateTrail" }`, []int{4}},
		{"json unquoted string", `{ $.eventType = UpdateTrail }`, []int{4}},
		{"json consumer case (#1596)", `{ $.task_uuid = "0b5f3f8e-6c1e-4c1a-9f0e-3c1d2b1a0f9e" }`, []int{6}},
		{"json no spaces", `{$.status="done"}`, []int{6}},
		{"json not equal with wildcard", `{ $.sourceIPAddress != 123.123.* }`, []int{4}},
		{"json wildcard value", `{ $.sourceIPAddress = 111.* }`, []int{4}},
		{"json wildcard both ends", `{ $.sourceIPAddress = "*.111.*" }`, []int{4}},
		{"json wildcard prefix mismatch", `{ $.sourceIPAddress = 222.* }`, []int{}},
		{"json wildcard middle mismatch", `{ $.sourceIPAddress = 111.*.999.* }`, []int{}},
		{"escaped quotes in a phrase", `"\"value\""`, []int{4}},
		{"json array element", `{ $.arrayKey[0] = "value" }`, []int{4}},
		{"json nested array", `{ $.coordinates[1][2] = 6 }`, []int{5}},
		{"json object in array", `{ $.objectList[1].name = "b" }`, []int{4}},
		{"json wildcard array", `{ $.arrayKey[*] = "another value" }`, []int{4}},
		{"json wildcard field", `{ $.* = "done" }`, []int{6}},
		{"json bracket key", `{ $.['cluster.name'] = "c" }`, []int{4}},
		{"json IS NULL", `{ $.SomeObject IS NULL }`, []int{4}},
		{"json IS TRUE", `{ $.flag IS TRUE }`, []int{4}},
		{"json IS FALSE", `{ $.flag IS FALSE }`, []int{}},
		{"json NOT EXISTS", `{ $.SomeOtherObject NOT EXISTS }`, []int{4, 5, 6}},
		{"json greater than", `{ $.bandwidth > 75 }`, []int{5}},
		{"json less than decimal", `{ $.latency < 50 }`, []int{6}},
		{"json greater or equal", `{ $.bandwidth >= 80 }`, []int{5}},
		{"json less or equal", `{ $.bandwidth <= 79 }`, []int{}},
		{"json numeric not equal", `{ $.bandwidth != 500 }`, []int{5}},
		{"json scientific notation", `{ $.number[0] = 1e+3 }`, []int{5}},
		{"json number wildcard", `{ $.code = 4* }`, []int{5}},
		{"json numeric op on a string node", `{ $.code > 1 }`, []int{}},
		{"json object node never matches", `{ $.user = "x" }`, []int{}},
		{"json missing node never matches not-equal", `{ $.nothing != "x" }`, []int{}},
		{"json AND", `{ ($.user.id = 1) && ($.users[0].email = "John.Doe@example.com") }`, []int{5}},
		{"json OR binds looser than AND",
			`{ $.user.email = "John.Stiles@example.com" || $.coordinates[0][1] = "nonmatch" && $.actions[2] = "nonmatch" }`,
			[]int{5}},
		{"json parentheses", `{ ($.user.email = "John.Stiles@example.com" || $.coordinates[0][1] = "nonmatch") && $.actions[2] = "nonmatch" }`,
			[]int{}},
		{"json OR fails", `{ ($.user.id = 2 && $.users[0].email = "nonmatch") || $.actions[2] = "GET" }`, []int{}},
		{"json OR across events", `{ $.status = "done" || $.eventType = "UpdateTrail" }`, []int{4, 6}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, errType := cwlFilterIndexes(t, srv, "/filter/patterns", tc.pattern)
			require.Empty(t, errType, "pattern %q is accepted", tc.pattern)
			assert.Equal(t, tc.want, got, "pattern %q", tc.pattern)
		})
	}
}

func TestCWLogs_FilterPatternsRefuseWhatTheyCannotEvaluate(t *testing.T) {
	t.Parallel()
	srv := newCWLogsTestServer(t)
	cwlFilterPutAll(t, srv, "/filter/refusals")

	for _, pattern := range []string{
		`[ip, user, ..., status_code=4*, bytes]`, // space-delimited: #1638
		`%AUTHORIZED%`,                           // regex: #1638
		`ERROR %AUTH%`,                           // regex term: #1638
		`{ $.eventType = %Trail% }`,              // regex value: #1638
		`"unterminated`,
		`ERR"OR`,
		`{ $.eventType = "UpdateTrail"`,
		`{ }`,
		`{ eventType = "x" }`,
		`{ $.a = }`,
		`{ $.a }`,
		`{ $.a > "x" }`,
		`{ $.a IS NOT NULL }`, // "IS NOT and EXISTS currently aren't supported"
		`{ $.a EXISTS }`,
		`{ $.a IS MAYBE }`,
		`{ $.a NOT THERE }`,
		`{ ($.a = 1 }`,
		`{ $.a = 1 ) }`,
		`{ $.a = 1 && }`,
		`{ $.a[*].b[*] = 1 }`, // one wildcard per selector
		`{ $.* = 1 || $.* = 2 || $.* = 3 || $.* = 4 }`, // three per pattern
		`{ $.a["k"] = 1 }`, // double quotes are not supported in bracket notation
		`{ $.a[x] = 1 }`,
		`{ $.a[0 = 1 }`,
		`{ $.a.. = 1 }`,
		`{ $.a = "unterminated }`,
		`{ $.a = %unterminated }`,
		`{ $.a = 1 # }`,
		`{ $ = 1 }`,
		`{ $.a.b* = 1 }`,
		`{ $.a = 1 $.b = 2 }`,
		`{ = 1 }`,
	} {
		_, errType := cwlFilterIndexes(t, srv, "/filter/refusals", pattern)
		assert.Equal(t, "InvalidParameterException", errType, "pattern %q is refused", pattern)
	}
}

// An unparseable pattern is a property of the request alone, so it is refused before the group is
// resolved: the refusal does not depend on whether the group exists.
func TestCWLogs_FilterPatternRefusalPrecedesGroupResolution(t *testing.T) {
	t.Parallel()
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "FilterLogEvents", map[string]any{
		"logGroupName": "/never/created", "filterPattern": "[a, b]",
	})
	raw := cwLogsReadBody(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "InvalidParameterException", cwLogsErrorTypeFrom(t, raw))
	assert.Contains(t, string(raw), "#1638", "the refusal names the follow-up that will implement it")
}
