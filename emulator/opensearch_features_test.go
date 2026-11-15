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

// osFeatureDocs is the fixture the search feature tests query. Document 4 has no `age`, a string
// `rank` and a boolean `active`, so missing and mixed-type sort values are exercised.
var osFeatureDocs = []string{
	`{"name":"ann","age":30,"team":"red","scores":[5,9],"active":true,"rank":2,"addr":{"city":"oslo"}}`,
	`{"name":"bob","age":25,"team":"blue","scores":[7],"active":false,"rank":1}`,
	`{"name":"cat","age":35,"team":"red","scores":[1,3],"active":true,"rank":3}`,
	`{"name":"dan","team":"green","scores":[],"active":false,"rank":"x"}`,
}

// osSearchIDs posts a search body (with query parameters in path) and returns the hit IDs in order
// and the raw body.
func osSearchIDs(t *testing.T, ts *emulator.TestServer, path, body string) ([]string, string) {
	t.Helper()
	status, raw := osDo(t, ts, http.MethodPost, path, body)
	require.Equal(t, http.StatusOK, status, raw)
	var got struct {
		Hits struct {
			Hits []map[string]interface{} `json:"hits"`
		} `json:"hits"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &got), raw)
	ids := []string{}
	for _, h := range got.Hits.Hits {
		ids = append(ids, fmt.Sprintf("%v", h["_id"]))
	}
	return ids, raw
}

// TestOpenSearch_SearchFeatures is a table of the search features #1201 and #1213 added: query
// clauses, sort keys, directions and missing values, `_source` forms, and paging parameters.
func TestOpenSearch_SearchFeatures(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i, doc := range osFeatureDocs {
		status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/feat/_doc/d%d", i+1), doc)
		require.Equal(t, http.StatusCreated, status, body)
	}

	for _, tc := range []struct {
		name, path, body string
		want             []string
		contains         []string
	}{
		{"term with value object", "/feat/_search", `{"query":{"term":{"team":{"value":"red"}}}}`, []string{"d1", "d3"}, nil},
		{"term not an object", "/feat/_search", `{"query":{"term":"red"}}`, []string{}, nil},
		{"term on an array field", "/feat/_search", `{"query":{"term":{"scores":7}}}`, []string{"d2"}, nil},
		{"terms", "/feat/_search", `{"query":{"terms":{"team":["blue","green"]}}}`, []string{"d2", "d4"}, nil},
		{"terms not a list", "/feat/_search", `{"query":{"terms":{"team":"blue"}}}`, []string{}, nil},
		{"terms not an object", "/feat/_search", `{"query":{"terms":["blue"]}}`, []string{}, nil},
		{"exists", "/feat/_search", `{"query":{"exists":{"field":"age"}}}`, []string{"d1", "d2", "d3"}, nil},
		{"exists not an object", "/feat/_search", `{"query":{"exists":"age"}}`, []string{}, nil},
		{"range", "/feat/_search", `{"query":{"range":{"age":{"gt":25,"lte":35}}}}`, []string{"d1", "d3"}, nil},
		{"range gte lt", "/feat/_search", `{"query":{"range":{"age":{"gte":25,"lt":30}}}}`, []string{"d2"}, nil},
		{"range on an array field", "/feat/_search", `{"query":{"range":{"scores":{"gte":9}}}}`, []string{"d1"}, nil},
		{"range on a string field", "/feat/_search", `{"query":{"range":{"name":{"gte":1}}}}`, []string{}, nil},
		{"_source keeps an included array of scalars", "/feat/_search", `{"_source":["scores"],"size":1}`, []string{"d1"}, []string{`"_source":{"scores":[5,9]}`}},
		{"range bad bound", "/feat/_search", `{"query":{"range":{"age":{"gte":"old"}}}}`, []string{}, nil},
		{"range bound not an object", "/feat/_search", `{"query":{"range":{"age":30}}}`, []string{}, nil},
		{"range not an object", "/feat/_search", `{"query":{"range":"age"}}`, []string{}, nil},
		{"match not an object", "/feat/_search", `{"query":{"match":"ann"}}`, []string{}, nil},
		{"match an object query", "/feat/_search", `{"query":{"match_phrase":{"addr.city":{"query":"OSL"}}}}`, []string{"d1"}, nil},
		{"bool must_not", "/feat/_search", `{"query":{"bool":{"must_not":[{"term":{"team":"red"}}]}}}`, []string{"d2", "d4"}, nil},
		{"bool filter and must", "/feat/_search", `{"query":{"bool":{"filter":{"term":{"active":true}},"must":[{"range":{"age":{"gte":31}}}]}}}`, []string{"d3"}, nil},
		{"bool should only", "/feat/_search", `{"query":{"bool":{"should":[{"term":{"name":"bob"}},{"term":{"name":"dan"}}]}}}`, []string{"d2", "d4"}, nil},
		{"bool string minimum_should_match", "/feat/_search", `{"query":{"bool":{"should":[{"term":{"name":"bob"}},{"term":{"team":"blue"}}],"minimum_should_match":"2"}}}`, []string{"d2"}, nil},
		{"bool not an object", "/feat/_search", `{"query":{"bool":[]}}`, []string{}, nil},
		{"unknown clause matches all", "/feat/_search", `{"query":{"wildcard":{"name":"a*"}}}`, []string{"d1", "d2", "d3", "d4"}, nil},
		{"keyword sub-field", "/feat/_search", `{"query":{"term":{"name.keyword":"cat"}}}`, []string{"d3"}, nil},
		{"sort asc, missing last", "/feat/_search", `{"sort":["age"]}`, []string{"d2", "d1", "d3", "d4"}, []string{`"sort":[null]`}},
		{"sort desc, missing last", "/feat/_search", `{"sort":[{"age":"desc"}]}`, []string{"d3", "d1", "d2", "d4"}, nil},
		{"sort missing first", "/feat/_search", `{"sort":[{"age":{"order":"asc","missing":"_first"}}]}`, []string{"d4", "d2", "d1", "d3"}, nil},
		{"sort array field asc uses the minimum", "/feat/_search", `{"sort":["scores"]}`, []string{"d3", "d1", "d2", "d4"}, nil},
		{"sort array field desc uses the maximum", "/feat/_search", `{"sort":[{"scores":"desc"}]}`, []string{"d1", "d2", "d3", "d4"}, nil},
		{"sort mixed types: numbers before strings", "/feat/_search", `{"sort":["rank"]}`, []string{"d2", "d1", "d3", "d4"}, nil},
		{"sort booleans", "/feat/_search", `{"sort":["active",{"name":"desc"}]}`, []string{"d4", "d2", "d3", "d1"}, nil},
		{"sort by _id desc", "/feat/_search", `{"sort":[{"_id":"desc"}]}`, []string{"d4", "d3", "d2", "d1"}, nil},
		{"sort by _doc", "/feat/_search", `{"sort":"_doc"}`, []string{"d1", "d2", "d3", "d4"}, nil},
		{"sort by _score keeps scores", "/feat/_search", `{"sort":["_score"]}`, []string{"d1", "d2", "d3", "d4"}, []string{`"_score":1`, `"max_score":1`}},
		{"sort with track_scores", "/feat/_search", `{"sort":["age"],"track_scores":true}`, []string{"d2", "d1", "d3", "d4"}, []string{`"_score":1`}},
		{"sort on an object has no value", "/feat/_search", `{"sort":["addr"]}`, []string{"d1", "d2", "d3", "d4"}, nil},
		{"sort query parameter", "/feat/_search?sort=team:asc,age:desc", ``, []string{"d2", "d4", "d3", "d1"}, nil},
		{"sort query parameter skips an empty entry", "/feat/_search?sort=,name", ``, []string{"d1", "d2", "d3", "d4"}, nil},
		{"_source parameter false", "/feat/_search?_source=false", ``, []string{"d1", "d2", "d3", "d4"}, nil},
		{"_source parameter list", "/feat/_search?_source=name,age&sort=age", ``, []string{"d2", "d1", "d3", "d4"}, []string{`"_source":{"age":25,"name":"bob"}`}},
		{"_source includes and excludes parameters", "/feat/_search?_source_includes=addr.*,name&_source_excludes=name&sort=_doc", ``, []string{"d1", "d2", "d3", "d4"}, []string{`"_source":{"addr":{"city":"oslo"}}`, `"_source":{}`}},
		{"_source true parameter", "/feat/_search?_source=true&size=1", ``, []string{"d1"}, []string{`"name":"ann"`}},
		{"_source string", "/feat/_search", `{"_source":"team","size":1}`, []string{"d1"}, []string{`"_source":{"team":"red"}`}},
		{"_source singular include and exclude", "/feat/_search", `{"_source":{"include":"addr","exclude":"addr.city"},"size":1}`, []string{"d1"}, []string{`"_source":{"addr":{}}`}},
		{"size 0 with aggregations", "/feat/_search", `{"size":0,"aggregations":{"teams":{"terms":{"field":"team","size":1}}}}`, []string{}, []string{`"doc_count":2,"key":"red"`, `"max_score":null`}},
		{"from as a query parameter", "/feat/_search?from=2&size=1", ``, []string{"d3"}, nil},
		{"from past the end", "/feat/_search?from=10", ``, []string{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, raw := osSearchIDs(t, ts, tc.path, tc.body)
			assert.Equal(t, tc.want, got, raw)
			for _, s := range tc.contains {
				assert.Contains(t, raw, s)
			}
		})
	}
}

// TestOpenSearch_SortSkipsUnsortableArrayElements asserts an array element with no sort value — an
// object or a null — is passed over when an array field picks its sort value.
func TestOpenSearch_SortSkipsUnsortableArrayElements(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i, doc := range []string{`{"v":[{"x":1},null,8]}`, `{"v":[null,{"y":2}]}`, `{"v":[3]}`} {
		status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/arr/_doc/%d", i), doc)
		require.Equal(t, http.StatusCreated, status, body)
	}
	got, raw := osSearchIDs(t, ts, "/arr/_search", `{"sort":["v"]}`)
	assert.Equal(t, []string{"2", "0", "1"}, got, raw)
}

// TestOpenSearch_SearchAggregations covers each aggregation over the fixture, and over an empty set.
func TestOpenSearch_SearchAggregations(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i, doc := range osFeatureDocs {
		status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/agg/_doc/d%d", i+1), doc)
		require.Equal(t, http.StatusCreated, status, body)
	}
	aggs := `"aggs":{"c":{"cardinality":{"field":"team"}},"v":{"value_count":{"field":"age"}},` +
		`"a":{"avg":{"field":"age"}},"hi":{"max":{"field":"age"}},"lo":{"min":{"field":"age"}},` +
		`"s":{"sum":{"field":"age"}},"t":{"terms":{"field":"team"}},"bad":"x","none":{"unknown":{}}}`

	status, body := osDo(t, ts, http.MethodPost, "/agg/_search", `{"size":0,`+aggs+`}`)
	require.Equal(t, http.StatusOK, status, body)
	var got struct {
		Aggregations map[string]map[string]interface{} `json:"aggregations"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &got), body)
	assert.EqualValues(t, 3, got.Aggregations["c"]["value"])
	assert.EqualValues(t, 3, got.Aggregations["v"]["value"])
	assert.EqualValues(t, 30, got.Aggregations["a"]["value"])
	assert.EqualValues(t, 35, got.Aggregations["hi"]["value"])
	assert.EqualValues(t, 25, got.Aggregations["lo"]["value"])
	assert.EqualValues(t, 90, got.Aggregations["s"]["value"])
	assert.Len(t, got.Aggregations["t"]["buckets"], 3)
	assert.NotContains(t, got.Aggregations, "bad")
	assert.NotContains(t, got.Aggregations, "none")

	// Over no documents the numeric aggregations are null.
	status, body = osDo(t, ts, http.MethodPost, "/agg/_search", `{"query":{"match_none":{}},`+aggs+`}`)
	require.Equal(t, http.StatusOK, status, body)
	require.NoError(t, json.Unmarshal([]byte(body), &got), body)
	for _, name := range []string{"a", "hi", "lo"} {
		assert.Nilf(t, got.Aggregations[name]["value"], "%s over no documents", name)
	}
}

// TestOpenSearch_SearchRefusals covers each search refusal in the docs table.
func TestOpenSearch_SearchRefusals(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, body := osDo(t, ts, http.MethodPut, "/ref/_doc/1", `{"a":1}`)
	require.Equal(t, http.StatusCreated, status, body)

	for _, tc := range []struct {
		name, path, body, errType string
		status                    int
	}{
		{"missing index", "/nope/_search", `{}`, "index_not_found_exception", http.StatusNotFound},
		{"sort order", "/ref/_search", `{"sort":[{"a":"sideways"}]}`, "illegal_argument_exception", http.StatusBadRequest},
		{"sort order in an object", "/ref/_search", `{"sort":[{"a":{"order":"up"}}]}`, "illegal_argument_exception", http.StatusBadRequest},
		{"sort entry type", "/ref/_search", `{"sort":[5]}`, "illegal_argument_exception", http.StatusBadRequest},
		{"sort parameter order", "/ref/_search?sort=a:up", ``, "illegal_argument_exception", http.StatusBadRequest},
		{"_source type", "/ref/_search", `{"_source":5}`, "illegal_argument_exception", http.StatusBadRequest},
		{"size not an integer", "/ref/_search?size=ten", ``, "illegal_argument_exception", http.StatusBadRequest},
		{"from negative", "/ref/_search", `{"from":-1}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match fraction", "/ref/_search", `{"query":{"bool":{"should":[{"match_all":{}}],"minimum_should_match":1.5}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match type", "/ref/_search", `{"query":{"bool":{"should":[{"match_all":{}}],"minimum_should_match":true}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match condition", "/ref/_search", `{"query":{"bool":{"should":[{"match_all":{}}],"minimum_should_match":"x<50%"}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match condition value", "/ref/_search", `{"query":{"bool":{"should":[{"match_all":{}}],"minimum_should_match":"2<half"}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match percentage", "/ref/_search", `{"query":{"bool":{"should":[{"match_all":{}}],"minimum_should_match":"a%"}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"minimum_should_match nested", "/ref/_search", `{"query":{"bool":{"must":[{"bool":{"should":[{"match_all":{}}],"minimum_should_match":"most"}}]}}}`, "illegal_argument_exception", http.StatusBadRequest},
		{"scroll without an id", "/_search/scroll", `{}`, "illegal_argument_exception", http.StatusBadRequest},
		{"scroll id unknown", "/_search/scroll", `{"scroll_id":"nope"}`, "search_context_missing_exception", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := osDo(t, ts, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.status, status, body)
			assert.Contains(t, body, `"type":"`+tc.errType+`"`)
		})
	}
}

// TestOpenSearch_ScrollForms covers a continuation addressed by parameter, a scroll's default page
// size, and clearing several contexts at once, only the existing ones counted.
func TestOpenSearch_ScrollForms(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 12 {
		status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/sc/_doc/%02d", i), `{"n":1}`)
		require.Equal(t, http.StatusCreated, status, body)
	}
	open := func() string {
		status, body := osDo(t, ts, http.MethodPost, "/sc/_search", `{"scroll":"1m","size":0}`)
		require.Equal(t, http.StatusOK, status, body)
		var got struct {
			ID string `json:"_scroll_id"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &got))
		return got.ID
	}
	id := open()
	// size 0 stored as the page size: a continuation falls back to ten.
	got, raw := osSearchIDs(t, ts, "/_search/scroll?scroll_id="+id, ``)
	assert.Len(t, got, 10, raw)
	got, _ = osSearchIDs(t, ts, "/_search/scroll?scroll_id="+id, ``)
	assert.Len(t, got, 2)

	other := open()
	status, body := osDo(t, ts, http.MethodDelete, "/_search/scroll", `{"scroll_id":["`+id+`","`+other+`","gone",7]}`)
	require.Equal(t, http.StatusOK, status, body)
	assert.JSONEq(t, `{"succeeded":true,"num_freed":2}`, body)

	status, body = osDo(t, ts, http.MethodDelete, "/_search/scroll", `{"scroll_id":`)
	assert.Equal(t, http.StatusBadRequest, status, body)
}

// TestOpenSearch_BulkActions covers every bulk action per item, each item failure, and each refusal
// of the whole request.
func TestOpenSearch_BulkActions(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, body := osDo(t, ts, http.MethodPut, "/b/_doc/keep", `{"a":1}`)
	require.Equal(t, http.StatusCreated, status, body)

	bulk := osBulkBody(
		`{"index":{"_index":"b"}}`, `{"auto":true}`, // auto ID
		`{"index":{"_index":"b","_id":"keep"}}`, `[1]`, // not an object
		`{"create":{"_index":"b","_id":"new"}}`, `{"a":2}`,
		`{"create":{"_index":"b","_id":"keep"}}`, `{"a":3}`, // exists
		`{"update":{"_index":"b","_id":"keep"}}`, `{"script":{"source":"x"}}`,
		`{"update":{"_index":"b","_id":"keep"}}`, `[1]`,
		`{"update":{"_index":"b","_id":"up"}}`, `{"upsert":{"u":1}}`,
		`{"update":{"_index":"b","_id":"keep"}}`, `{"doc":{"a":9}}`,
		`{"update":{"_index":"made","_id":"x"}}`, `{"doc":{"a":1}}`, // missing index and document
		`{"delete":{"_index":"b","_id":"new"}}`,
		`{"index":{"_index":"b","_id":7}}`, `{"numeric":"id"}`,
	)
	status, body = osDo(t, ts, http.MethodPost, "/_bulk", bulk)
	require.Equal(t, http.StatusOK, status, body)
	var got struct {
		Errors bool                                `json:"errors"`
		Items  []map[string]map[string]interface{} `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &got), body)
	require.True(t, got.Errors)
	require.Len(t, got.Items, 11)
	want := []struct {
		action string
		status float64
		result string
		errT   string
	}{
		{"index", 201, "created", ""},
		{"index", 400, "", "mapper_parsing_exception"},
		{"create", 201, "created", ""},
		{"create", 409, "", "version_conflict_engine_exception"},
		{"update", 400, "", "illegal_argument_exception"},
		{"update", 400, "", "x_content_parse_exception"},
		{"update", 201, "created", ""},
		{"update", 200, "updated", ""},
		{"update", 404, "", "document_missing_exception"},
		{"delete", 200, "deleted", ""},
		{"index", 201, "created", ""},
	}
	for i, w := range want {
		item := got.Items[i][w.action]
		require.NotNilf(t, item, "item %d is a %s: %v", i, w.action, got.Items[i])
		assert.Equalf(t, w.status, item["status"], "item %d", i)
		if w.result != "" {
			assert.Equalf(t, w.result, item["result"], "item %d", i)
		}
		if w.errT != "" {
			errObj, _ := item["error"].(map[string]interface{})
			assert.Equalf(t, w.errT, errObj["type"], "item %d", i)
		}
	}
	assert.Equal(t, "7", got.Items[10]["index"]["_id"], "a numeric _id is read as its string")
	status, _ = osDo(t, ts, http.MethodGet, "/made", "")
	assert.Equal(t, http.StatusOK, status, "an update creates its missing index")

	for _, tc := range []struct{ name, path, body, errType string }{
		{"empty", "/_bulk", "\n\n", "action_request_validation_exception"},
		{"not an action object", "/_bulk", "[1]\n", "illegal_argument_exception"},
		{"two actions on one line", "/_bulk", `{"index":{},"create":{}}` + "\n{}\n", "illegal_argument_exception"},
		{"unknown action", "/_bulk", `{"upsert":{"_index":"b"}}` + "\n{}\n", "illegal_argument_exception"},
		{"no index", "/_bulk", `{"index":{"_id":"1"}}` + "\n{}\n", "action_request_validation_exception"},
		{"update without an id", "/b/_bulk", `{"update":{}}` + "\n{}\n", "action_request_validation_exception"},
		{"delete without an id", "/b/_bulk", `{"delete":{}}` + "\n", "action_request_validation_exception"},
		{"no source line", "/b/_bulk", `{"index":{"_id":"1"}}` + "\n", "illegal_argument_exception"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := osDo(t, ts, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, body)
			assert.Contains(t, body, `"type":"`+tc.errType+`"`)
		})
	}
}

// TestOpenSearch_IndexAndDocumentEdges covers the index and document paths the shape tests do not:
// a mapping put on an index with none, a mapping put on a missing index, an invalid mapping body,
// settings of every value type, a document read and delete on a missing index, and HEAD on a
// document.
func TestOpenSearch_IndexAndDocumentEdges(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPut, "/plain", ``)
	require.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodPost, "/plain/_mapping", `{"properties":{"a":{"type":"keyword"}}}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodGet, "/plain/_mappings", ``)
	require.Equal(t, http.StatusOK, status, body)
	assert.JSONEq(t, `{"plain":{"mappings":{"properties":{"a":{"type":"keyword"}}}}}`, body)
	status, body = osDo(t, ts, http.MethodPut, "/plain/_mapping", `{`)
	assert.Equal(t, http.StatusBadRequest, status, body)
	status, body = osDo(t, ts, http.MethodPut, "/absent/_mapping", `{}`)
	assert.Equal(t, http.StatusNotFound, status, body)
	status, body = osDo(t, ts, http.MethodGet, "/absent/_mapping", ``)
	assert.Equal(t, http.StatusNotFound, status, body)
	status, body = osDo(t, ts, http.MethodDelete, "/plain/_mapping", ``)
	assert.Equal(t, http.StatusNotFound, status, "DELETE _mapping is not routed: %s", body)
	status, body = osDo(t, ts, http.MethodPut, "/never", `{`)
	assert.Equal(t, http.StatusBadRequest, status, body)
	assert.Contains(t, body, "json_parse_exception")
	status, body = osDo(t, ts, http.MethodPut, "/scalar-index", `{"settings":{"index":"5"}}`)
	require.Equal(t, http.StatusOK, status, "an `index` setting that is not an object names nothing: %s", body)

	status, body = osDo(t, ts, http.MethodPut, "/typed", `{"settings":{"index":{"hidden":true,"codec":"best_compression",`+
		`"routing_path":["a",1],"blank":null,"other":{}},"analysis":{"analyzer":{"x":"1","y":"2"}},"index.flat":"1",`+
		`"weird":"1","weird.child":"2"}}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodGet, "/typed", ``)
	require.Equal(t, http.StatusOK, status, body)
	for _, s := range []string{`"hidden":"true"`, `"codec":"best_compression"`, `"routing_path":["a","1"]`,
		`"analysis":{"analyzer":{"x":"1","y":"2"}}`, `"flat":"1"`, `"weird":"1"`} {
		assert.Contains(t, body, s)
	}
	assert.NotContains(t, body, `"child"`, "a setting under a scalar is not nested into it")
	status, body = osDo(t, ts, http.MethodPut, "/empty-settings", `{"settings":{}}`)
	require.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodPut, "/dup2", `{"settings":{"a":"1","index":{"a":"2"},"z":"3"}}`)
	assert.Equal(t, http.StatusBadRequest, status, body)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/absent/_doc/1"},
		{http.MethodDelete, "/absent/_doc/1"},
	} {
		status, body := osDo(t, ts, tc.method, tc.path, ``)
		assert.Equal(t, http.StatusNotFound, status, body)
		assert.Contains(t, body, "index_not_found_exception")
	}
	status, _, raw := osCall(t, ts, osShapesHost, "", http.MethodHead, "/plain/_doc/none", "")
	assert.Equal(t, http.StatusNotFound, status)
	assert.Empty(t, raw)
	status, body = osDo(t, ts, http.MethodPut, "/plain/_doc/1", `{"a":"x","b":"y"}`)
	require.Equal(t, http.StatusCreated, status, body)
	status, _, raw = osCall(t, ts, osShapesHost, "", http.MethodHead, "/plain/_doc/1", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, raw)
	status, body = osDo(t, ts, http.MethodGet, "/plain/_doc/1?_source=false", ``)
	require.Equal(t, http.StatusOK, status, body)
	assert.NotContains(t, body, "_source")
	status, body = osDo(t, ts, http.MethodGet, "/plain/_doc/1?_source_excludes=b", ``)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"_source":{"a":"x"}`)
	status, body = osDo(t, ts, http.MethodPost, "/plain/_refresh", ``)
	assert.Equal(t, http.StatusOK, status, body)
	status, body = osDo(t, ts, http.MethodPatch, "/plain/_doc/1", ``)
	assert.Equal(t, http.StatusNotFound, status, body)
	assert.Contains(t, body, "route_not_found")
}
