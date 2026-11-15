package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestOpenSearch_ResultWindowIsBounded asserts a search whose from + size exceeds
// index.max_result_window (10000 by default, the pagination reference) is refused with the engine's
// illegal_argument_exception rather than served, and that an index's own setting moves the limit. A
// huge size used to drive the page allocation, and from + size could overflow.
func TestOpenSearch_ResultWindowIsBounded(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPut, "/win", `{"settings":{"index":{"max_result_window":5}}}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodPut, "/wide/_doc/1", `{"a":1}`)
	require.Equal(t, http.StatusCreated, status, body)

	for _, tc := range []struct {
		name, path, body string
		status           int
		reason           string
	}{
		{"default limit", "/wide/_search", `{"from":9990,"size":11}`, http.StatusBadRequest,
			"Result window is too large, from + size must be less than or equal to: [10000] but was [10001]"},
		{"at the default limit", "/wide/_search", `{"from":9990,"size":10}`, http.StatusOK, ""},
		{"huge size", "/wide/_search", `{"size":9223372036854775807}`, http.StatusBadRequest,
			"but was [9223372036854775807]"},
		{"overflowing sum", "/wide/_search", `{"from":9223372036854775807,"size":9223372036854775807}`, http.StatusBadRequest,
			"but was [9223372036854775807]"},
		{"query parameters", "/wide/_search?size=20000", "", http.StatusBadRequest, "Result window is too large"},
		{"index setting", "/win/_search", `{"size":6}`, http.StatusBadRequest,
			"less than or equal to: [5] but was [6]"},
		{"scroll batch", "/win/_search?scroll=1m", `{"size":6}`, http.StatusBadRequest,
			"Batch size is too large, size must be less than or equal to: [5] but was [6]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := osDo(t, ts, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.status, status, body)
			if tc.reason != "" {
				assert.Contains(t, body, `"type":"illegal_argument_exception"`)
				assert.Contains(t, body, tc.reason)
			}
		})
	}
}

// TestOpenSearch_QueryEdgeCases pins minimum_should_match's published forms, match on a missing
// field and on an empty query, and _source filtering through arrays of objects.
func TestOpenSearch_QueryEdgeCases(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for i, doc := range []string{
		`{"name":"one","tags":["a"],"items":[{"sku":"x1","qty":1},{"sku":"x2","qty":2}]}`,
		`{"name":"two","tags":["a","b"]}`,
		`{"name":"three","tags":["a","b","c"]}`,
		`{"tags":["a","b","c","d"]}`,
	} {
		status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/edge/_doc/%d", i), doc)
		require.Equal(t, http.StatusCreated, status, body)
	}
	ids := func(body string) []string {
		var got struct {
			Hits struct {
				Hits []map[string]interface{} `json:"hits"`
			} `json:"hits"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &got), body)
		out := []string{}
		for _, h := range got.Hits.Hits {
			out = append(out, fmt.Sprintf("%v", h["_id"]))
		}
		return out
	}
	should := `[{"term":{"tags":"a"}},{"term":{"tags":"b"}},{"term":{"tags":"c"}},{"term":{"tags":"d"}}]`
	for _, tc := range []struct {
		name, query string
		want        []string
	}{
		{"integer", `{"bool":{"should":` + should + `,"minimum_should_match":3}}`, []string{"2", "3"}},
		{"negative integer", `{"bool":{"should":` + should + `,"minimum_should_match":-1}}`, []string{"2", "3"}},
		{"percentage rounds down", `{"bool":{"should":` + should + `,"minimum_should_match":"80%"}}`, []string{"2", "3"}},
		{"negative percentage", `{"bool":{"should":` + should + `,"minimum_should_match":"-20%"}}`, []string{"3"}},
		{"combination", `{"bool":{"should":` + should + `,"minimum_should_match":"3<50%"}}`, []string{"1", "2", "3"}},
		{"combination at or below n", `{"bool":{"should":` + should + `,"minimum_should_match":"5<50%"}}`, []string{"3"}},
		{"missing field never matches", `{"match":{"name":"e"}}`, []string{"0", "2"}},
		{"empty match query matches nothing", `{"match":{"name":""}}`, []string{}},
		{"empty match query under zero_terms_query all", `{"match":{"name":{"query":" ","zero_terms_query":"all"}}}`, []string{"0", "1", "2", "3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := osDo(t, ts, http.MethodPost, "/edge/_search", `{"query":`+tc.query+`}`)
			require.Equal(t, http.StatusOK, status, body)
			assert.Equal(t, tc.want, ids(body))
		})
	}

	status, body := osDo(t, ts, http.MethodPost, "/edge/_search",
		`{"query":{"bool":{"should":`+should+`,"minimum_should_match":"most"}}}`)
	assert.Equal(t, http.StatusBadRequest, status, body)
	assert.Contains(t, body, `"type":"illegal_argument_exception"`, "an unreadable value is refused, not ignored")

	for _, tc := range []struct{ source, want string }{
		{`["items.sku"]`, `"_source":{"items":[{"sku":"x1"},{"sku":"x2"}]}`},
		{`{"excludes":["items.qty","tags"]}`, `"_source":{"items":[{"sku":"x1"},{"sku":"x2"}],"name":"one"}`},
	} {
		status, body := osDo(t, ts, http.MethodPost, "/edge/_search",
			`{"query":{"term":{"name":"one"}},"_source":`+tc.source+`}`)
		require.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, tc.want)
	}
}

// TestOpenSearch_BulkMissingIndexAndNoop asserts a bulk delete on a missing index is a failed
// index_not_found_exception item, as the engine's TransportBulkAction fails it, and that an update
// changing nothing is a `noop` that bumps no version, per the update reference's detect_noop.
func TestOpenSearch_BulkMissingIndexAndNoop(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPost, "/_bulk", `{"delete":{"_index":"nowhere","_id":"1"}}`+"\n")
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"errors":true`)
	assert.Contains(t, body, `"type":"index_not_found_exception"`)
	assert.Contains(t, body, `"status":404`)
	status, _ = osDo(t, ts, http.MethodGet, "/nowhere", "")
	assert.Equal(t, http.StatusNotFound, status, "a delete does not create the index")

	status, body = osDo(t, ts, http.MethodPut, "/n/_doc/1", `{"a":1,"b":{"c":2}}`)
	require.Equal(t, http.StatusCreated, status, body)
	status, body = osDo(t, ts, http.MethodPost, "/n/_bulk", `{"update":{"_id":"1"}}`+"\n"+`{"doc":{"b":{"c":2}}}`+"\n")
	require.Equal(t, http.StatusOK, status, body)
	assert.JSONEq(t, `{"took":1,"errors":false,"items":[{"update":{"_index":"n","_id":"1","_version":1,"result":"noop",`+
		`"_shards":{"total":0,"successful":0,"failed":0},"_seq_no":0,"_primary_term":1,"status":200}}]}`, body)

	status, body = osDo(t, ts, http.MethodPost, "/n/_bulk", `{"update":{"_id":"1"}}`+"\n"+`{"doc":{"a":1},"detect_noop":false}`+"\n")
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"result":"updated"`)
	assert.Contains(t, body, `"_version":2`)
}

// TestOpenSearch_IndexCreateAndAutoID covers a POST to `_doc/` with a trailing slash, which mints an
// ID, and a create whose settings name one setting twice, which is refused rather than decided by
// map order.
func TestOpenSearch_IndexCreateAndAutoID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPost, "/auto/_doc/", `{"a":1}`)
	require.Equal(t, http.StatusCreated, status, body)
	assert.Contains(t, body, `"result":"created"`)

	for range 20 {
		status, body := osDo(t, ts, http.MethodPut, "/dup",
			`{"settings":{"number_of_shards":1,"index":{"number_of_shards":2}}}`)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Contains(t, body, "setting [index.number_of_shards] is given more than once")
	}
}
