package emulator_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// osShapesHost is a documented public domain endpoint, so requests here address the domain "shapes".
const osShapesHost = "search-shapes-abc123def456.us-east-1.es.amazonaws.com"

// osUnregisteredKey signs a request to a base endpoint, whose host routes nothing, so the SigV4
// signing name `es` is what reaches the plugin. The server holds no credential for it, so the request
// is attributed to the default account.
const osUnregisteredKey = "AKIDEXAMPLEUNREGISTERED"

// osCall issues one OpenSearch request to ts at host and returns the status, headers and raw body.
// accessKey, when set, is put in an unverified SigV4 Authorization header so the request is
// attributed to that key's account.
func osCall(t *testing.T, ts *emulator.TestServer, host, accessKey, method, path, body string) (int, http.Header, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, r)
	require.NoError(t, err)
	req.Host = host
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if accessKey != "" {
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+
			"/20250101/us-east-1/es/aws4_request, SignedHeaders=host, Signature=unverified")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, resp.Header, string(raw)
}

// osDo is osCall on osShapesHost as the default account, returning status and body.
func osDo(t *testing.T, ts *emulator.TestServer, method, path, body string) (int, string) {
	t.Helper()
	status, _, raw := osCall(t, ts, osShapesHost, "", method, path, body)
	return status, raw
}

// osShards is the `_shards` member every write reports.
const osShards = `{"total":1,"successful":1,"failed":0}`

// TestOpenSearch_WriteResponsesCarryVersionAndSeqNo asserts the raw bytes of index, re-index, get
// and delete against the index-document, get-document and delete-document references (#1213 items
// 1, 3 and 4): 201 then 200, an incrementing `_version`, `_seq_no` and `_primary_term` on every write,
// `found: false` in a normal 404 body, and `not_found` at 404 for an absent delete.
func TestOpenSearch_WriteResponsesCarryVersionAndSeqNo(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	steps := []struct {
		name, method, path, body string
		status                   int
		want                     string
	}{
		{"create", http.MethodPut, "/books/_doc/1", `{"title":"Dune"}`, http.StatusCreated,
			`{"_index":"books","_id":"1","_version":1,"result":"created","_shards":` + osShards + `,"_seq_no":0,"_primary_term":1}`},
		{"replace", http.MethodPut, "/books/_doc/1", `{"title":"Dune Messiah"}`, http.StatusOK,
			`{"_index":"books","_id":"1","_version":2,"result":"updated","_shards":` + osShards + `,"_seq_no":1,"_primary_term":1}`},
		{"get", http.MethodGet, "/books/_doc/1", "", http.StatusOK,
			`{"_index":"books","_id":"1","_version":2,"_seq_no":1,"_primary_term":1,"found":true,"_source":{"title":"Dune Messiah"}}`},
		{"get absent", http.MethodGet, "/books/_doc/2", "", http.StatusNotFound,
			`{"_index":"books","_id":"2","found":false}`},
		{"delete absent", http.MethodDelete, "/books/_doc/2", "", http.StatusNotFound,
			`{"_index":"books","_id":"2","_version":1,"result":"not_found","_shards":` + osShards + `,"_seq_no":2,"_primary_term":1}`},
		{"delete", http.MethodDelete, "/books/_doc/1", "", http.StatusOK,
			`{"_index":"books","_id":"1","_version":3,"result":"deleted","_shards":` + osShards + `,"_seq_no":3,"_primary_term":1}`},
		{"create-only conflict", http.MethodPut, "/books/_create/9", `{"a":1}`, http.StatusCreated,
			`{"_index":"books","_id":"9","_version":1,"result":"created","_shards":` + osShards + `,"_seq_no":4,"_primary_term":1}`},
	}
	for _, s := range steps {
		status, body := osDo(t, ts, s.method, s.path, s.body)
		assert.Equalf(t, s.status, status, "%s: %s", s.name, body)
		assert.JSONEqf(t, s.want, body, "%s", s.name)
	}

	status, body := osDo(t, ts, http.MethodPut, "/books/_create/9", `{"a":2}`)
	assert.Equal(t, http.StatusConflict, status, body)
	assert.Contains(t, body, `"type":"version_conflict_engine_exception"`)
}

// TestOpenSearch_BulkReportsEachItem asserts a mixed bulk request against the Bulk API reference
// (#1213 item 2): `update` is an action, every item carries its own status and write metadata, a
// failed item carries the published `error` object, and `errors` is true because one item failed —
// while a `not_found` delete, which the reference's own example shows with `errors: false`, is not a
// failure.
func TestOpenSearch_BulkReportsEachItem(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPut, "/movies/_doc/m1", `{"title":"Alien","year":1979}`)
	require.Equal(t, http.StatusCreated, status, body)

	bulk := strings.Join([]string{
		`{"index":{"_index":"movies","_id":"m2"}}`,
		`{"title":"Aliens"}`,
		`{"update":{"_index":"movies","_id":"m1"}}`,
		`{"doc":{"year":1980}}`,
		`{"update":{"_index":"movies","_id":"missing"}}`,
		`{"doc":{"year":1}}`,
		`{"delete":{"_index":"movies","_id":"gone"}}`,
	}, "\n") + "\n"
	status, body = osDo(t, ts, http.MethodPost, "/_bulk", bulk)
	require.Equal(t, http.StatusOK, status, body)

	var got struct {
		Errors bool                                `json:"errors"`
		Items  []map[string]map[string]interface{} `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &got), body)
	assert.True(t, got.Errors, "one item failed, so errors is true: %s", body)
	require.Len(t, got.Items, 4, body)

	item := func(i int, action string) string {
		b, err := json.Marshal(got.Items[i][action])
		require.NoError(t, err)
		return string(b)
	}
	assert.JSONEq(t, `{"_index":"movies","_id":"m2","_version":1,"result":"created","_shards":`+osShards+`,"_seq_no":1,"_primary_term":1,"status":201}`, item(0, "index"))
	assert.JSONEq(t, `{"_index":"movies","_id":"m1","_version":2,"result":"updated","_shards":`+osShards+`,"_seq_no":2,"_primary_term":1,"status":200}`, item(1, "update"))
	failed := got.Items[2]["update"]
	assert.EqualValues(t, http.StatusNotFound, failed["status"])
	errObj, _ := failed["error"].(map[string]interface{})
	assert.Equal(t, "document_missing_exception", errObj["type"])
	assert.Equal(t, "[missing]: document missing", errObj["reason"])
	assert.Equal(t, "movies", errObj["index"])
	assert.Equal(t, "0", errObj["shard"])
	assert.NotEmpty(t, errObj["index_uuid"])
	assert.JSONEq(t, `{"_index":"movies","_id":"gone","_version":1,"result":"not_found","_shards":`+osShards+`,"_seq_no":3,"_primary_term":1,"status":404}`, item(3, "delete"))

	// The partial update merged rather than replaced.
	status, body = osDo(t, ts, http.MethodGet, "/movies/_doc/m1", "")
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"_source":{"title":"Alien","year":1980}`)

	// A bulk with no failing item reports errors: false.
	status, body = osDo(t, ts, http.MethodPost, "/movies/_bulk", `{"delete":{"_id":"nope"}}`+"\n")
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"errors":false`)

	// An action the reference does not publish refuses the whole request.
	status, body = osDo(t, ts, http.MethodPost, "/movies/_bulk", `{"upsert":{"_id":"x"}}`+"\n{}\n")
	assert.Equal(t, http.StatusBadRequest, status, body)
	assert.Contains(t, body, `"type":"illegal_argument_exception"`)
}

// TestOpenSearch_IndexReadShapes covers #1213 items 5–10: a scroll page's total, HEAD's empty body,
// stored settings and mappings, every hit's real `_index`, and `root_cause` in the error envelope.
func TestOpenSearch_IndexReadShapes(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPut, "/logs",
		`{"settings":{"number_of_shards":3,"index":{"number_of_replicas":2}},"mappings":{"properties":{"msg":{"type":"text"}}}}`)
	require.Equal(t, http.StatusOK, status, body)

	t.Run("settings", func(t *testing.T) {
		status, body := osDo(t, ts, http.MethodGet, "/logs", "")
		require.Equal(t, http.StatusOK, status, body)
		var got map[string]struct {
			Settings struct {
				Index map[string]interface{} `json:"index"`
			} `json:"settings"`
			Mappings map[string]interface{} `json:"mappings"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &got))
		idx := got["logs"].Settings.Index
		assert.Equal(t, "3", idx["number_of_shards"], "every setting is a string, as the get-index reference prints it")
		assert.Equal(t, "2", idx["number_of_replicas"])
		assert.Equal(t, "logs", idx["provided_name"])
		assert.NotEmpty(t, idx["uuid"])
		assert.NotEmpty(t, idx["creation_date"])
	})

	t.Run("mapping", func(t *testing.T) {
		status, body := osDo(t, ts, http.MethodPut, "/logs/_mapping", `{"properties":{"level":{"type":"keyword"}}}`)
		require.Equal(t, http.StatusOK, status, body)
		assert.JSONEq(t, `{"acknowledged":true}`, body)
		status, body = osDo(t, ts, http.MethodGet, "/logs/_mapping", "")
		require.Equal(t, http.StatusOK, status, body)
		assert.JSONEq(t, `{"logs":{"mappings":{"properties":{"msg":{"type":"text"},"level":{"type":"keyword"}}}}}`, body)
	})

	t.Run("head", func(t *testing.T) {
		status, _, body := osCall(t, ts, osShapesHost, "", http.MethodHead, "/logs", "")
		assert.Equal(t, http.StatusOK, status)
		assert.Empty(t, body)
		status, _, body = osCall(t, ts, osShapesHost, "", http.MethodHead, "/absent", "")
		assert.Equal(t, http.StatusNotFound, status)
		assert.Empty(t, body)
	})

	t.Run("hits name their index and a scroll page reports the total", func(t *testing.T) {
		for i := range 5 {
			status, body := osDo(t, ts, http.MethodPut, fmt.Sprintf("/logs/_doc/d%d", i), fmt.Sprintf(`{"n":%d}`, i))
			require.Equal(t, http.StatusCreated, status, body)
		}
		status, body := osDo(t, ts, http.MethodPost, "/logs/_search?scroll=1m", `{"size":2}`)
		require.Equal(t, http.StatusOK, status, body)
		var first struct {
			ScrollID string `json:"_scroll_id"`
			Hits     struct {
				Total struct {
					Value int `json:"value"`
				} `json:"total"`
				Hits []map[string]interface{} `json:"hits"`
			} `json:"hits"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &first))
		require.Len(t, first.Hits.Hits, 2)
		for _, h := range first.Hits.Hits {
			assert.Equal(t, "logs", h["_index"], "a search hit reports its real index, not \"unknown\"")
		}
		assert.Equal(t, 5, first.Hits.Total.Value)

		status, body = osDo(t, ts, http.MethodPost, "/_search/scroll", `{"scroll_id":"`+first.ScrollID+`"}`)
		require.Equal(t, http.StatusOK, status, body)
		assert.Contains(t, body, `"total":{"relation":"eq","value":5}`, "a scroll page reports the query's total")
	})

	t.Run("error envelope carries root_cause", func(t *testing.T) {
		status, body := osDo(t, ts, http.MethodGet, "/absent", "")
		assert.Equal(t, http.StatusNotFound, status)
		assert.JSONEq(t, `{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [absent]"}],`+
			`"type":"index_not_found_exception","reason":"no such index [absent]"},"status":404}`, body)
	})
}

// TestOpenSearch_ControlPlaneIsRefusedInTheAWSEnvelope asserts the control-plane paths of both
// published APIs answer a REST-JSON UnknownOperationException naming the operation, not the engine's
// route_not_found (#1212).
func TestOpenSearch_ControlPlaneIsRefusedInTheAWSEnvelope(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct {
		method, path, op string
	}{
		{http.MethodPost, "/2021-01-01/opensearch/domain", "CreateDomain"},
		{http.MethodGet, "/2021-01-01/opensearch/domain/my-domain", "DescribeDomain"},
		{http.MethodDelete, "/2021-01-01/opensearch/domain/my-domain", "DeleteDomain"},
		{http.MethodGet, "/2021-01-01/domain", "ListDomainNames"},
		{http.MethodPost, "/2021-01-01/opensearch/domain/my-domain/config", "UpdateDomainConfig"},
		{http.MethodPost, "/2015-01-01/es/domain", "CreateElasticsearchDomain"},
		{http.MethodGet, "/2015-01-01/es/domain/my-domain", "DescribeElasticsearchDomain"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			status, hdr, body := osCall(t, ts, "es.us-east-1.amazonaws.com", "", tc.method, tc.path, "")
			assert.Equal(t, http.StatusNotFound, status, body)
			assert.Equal(t, "UnknownOperationException", hdr.Get("X-Amzn-ErrorType"), body)
			assert.Contains(t, body, tc.op)
			assert.Contains(t, body, "issues/1564")
			assert.NotContains(t, body, "route_not_found")
		})
	}
}

// TestOpenSearch_StateIsScopedByAccountRegionAndDomain asserts the isolation #1200 restores. Every
// case fails against a tree whose keys carry no scope: the document written in one scope is found in
// the other.
func TestOpenSearch_StateIsScopedByAccountRegionAndDomain(t *testing.T) {
	t.Parallel()
	const other = "111122223333"
	ts := emulator.StartTestServer(t, emulator.WithAccounts(other))
	cred, ok := ts.CredentialsFor(other)
	require.True(t, ok)

	cases := []struct {
		name                string
		writeHost, writeKey string
		readHost, readKey   string
		visible             bool
	}{
		{"same domain endpoint", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "search-alpha-bbb222.us-east-1.es.amazonaws.com", "", true},
		{"public and VPC endpoints of one domain", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "vpc-alpha-ccc333.us-east-1.es.amazonaws.com", "", true},
		{"two domains", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "search-beta-aaa111.us-east-1.es.amazonaws.com", "", false},
		{"hyphenated domain name", "search-my-app-logs-aaa111.us-east-1.es.amazonaws.com", "", "search-my-app-aaa111.us-east-1.es.amazonaws.com", "", false},
		{"domain and base endpoint", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "localhost", osUnregisteredKey, false},
		{"two base endpoints share one domain", "localhost", osUnregisteredKey, "es.us-east-1.amazonaws.com", "", true},
		{"two collections", "col1.us-east-1.aoss.amazonaws.com", "", "col2.us-east-1.aoss.amazonaws.com", "", false},
		{"two accounts", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "search-alpha-aaa111.us-east-1.es.amazonaws.com", cred.AccessKeyID, false},
		{"two Regions", "search-alpha-aaa111.us-east-1.es.amazonaws.com", "", "search-alpha-aaa111.us-west-2.es.amazonaws.com", "", false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			index := fmt.Sprintf("scoped-%d", i)
			status, _, body := osCall(t, ts, tc.writeHost, tc.writeKey, http.MethodPut, "/"+index+"/_doc/1", `{"owner":"writer"}`)
			require.Equal(t, http.StatusCreated, status, body)

			status, _, body = osCall(t, ts, tc.readHost, tc.readKey, http.MethodGet, "/"+index+"/_doc/1", "")
			if tc.visible {
				assert.Equal(t, http.StatusOK, status, body)
				return
			}
			assert.Equal(t, http.StatusNotFound, status, body)
			assert.Contains(t, body, "index_not_found_exception", "the index exists only in the writer's scope")
		})
	}
}

// TestOpenSearch_SearchIsDeterministic runs each request twice and asserts byte-identical bodies, and
// pins the order, scores and source filtering #1201 defines.
func TestOpenSearch_SearchIsDeterministic(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	// Auto-generated IDs: their order is a property of the minted ID, which is why results are
	// ordered by insertion instead.
	for i, doc := range []string{
		`{"name":"carol","age":41,"team":{"id":"b","lead":true}}`,
		`{"name":"alice","age":29,"team":{"id":"a","lead":false}}`,
		`{"name":"bob","age":35,"team":{"id":"a","lead":true}}`,
	} {
		status, body := osDo(t, ts, http.MethodPost, "/people/_doc", doc)
		require.Equalf(t, http.StatusCreated, status, "doc %d: %s", i, body)
	}

	names := func(body string) []string {
		var got struct {
			Hits struct {
				MaxScore interface{}              `json:"max_score"`
				Hits     []map[string]interface{} `json:"hits"`
			} `json:"hits"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &got), body)
		var out []string
		for _, h := range got.Hits.Hits {
			src, _ := h["_source"].(map[string]interface{})
			out = append(out, fmt.Sprintf("%v", src["name"]))
		}
		return out
	}

	for _, tc := range []struct {
		name, query string
		want        []string
		contains    []string
	}{
		{"insertion order without a sort", `{"query":{"match_all":{}}}`,
			[]string{"carol", "alice", "bob"}, []string{`"max_score":1`, `"_score":1`}},
		{"two top-level clauses are a conjunction", `{"query":{"match_all":{},"term":{"team.id":"a"}}}`,
			[]string{"alice", "bob"}, nil},
		{"match_none beside another clause matches nothing", `{"query":{"term":{"team.id":"a"},"match_none":{}}}`,
			nil, []string{`"total":{"relation":"eq","value":0}`, `"max_score":null`}},
		{"sort by a field", `{"sort":[{"age":"desc"}]}`,
			[]string{"carol", "bob", "alice"}, []string{`"_score":null`, `"max_score":null`, `"sort":[41]`}},
		{"sort by a keyword sub-field", `{"sort":["name.keyword"]}`,
			[]string{"alice", "bob", "carol"}, []string{`"sort":["alice"]`}},
		{"bool filter with an optional should", `{"query":{"bool":{"filter":{"term":{"team.lead":true}},"should":[{"term":{"name":"nobody"}}]}}}`,
			[]string{"carol", "bob"}, nil},
		{"source includes", `{"_source":["name","team.id"],"sort":["age"]}`,
			[]string{"alice", "bob", "carol"}, []string{`"_source":{"name":"alice","team":{"id":"a"}}`}},
		{"source excludes", `{"_source":{"excludes":["team"]},"sort":["age"]}`,
			[]string{"alice", "bob", "carol"}, []string{`"_source":{"age":29,"name":"alice"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, first := osDo(t, ts, http.MethodPost, "/people/_search", tc.query)
			require.Equal(t, http.StatusOK, status, first)
			_, second := osDo(t, ts, http.MethodPost, "/people/_search", tc.query)
			assert.Equal(t, first, second, "the same request answers the same bytes")
			assert.Equal(t, tc.want, names(first))
			for _, s := range tc.contains {
				assert.Contains(t, first, s)
			}
		})
	}

	status, body := osDo(t, ts, http.MethodPost, "/people/_search", `{"_source":false}`)
	require.Equal(t, http.StatusOK, status, body)
	assert.NotContains(t, body, `"_source"`, "_source: false omits the source from every hit")
}

// TestOpenSearch_RefusesASourceThatIsNotAnObject asserts a document body that is not a JSON object
// is refused rather than stored, since documents are now stored with their metadata.
func TestOpenSearch_RefusesASourceThatIsNotAnObject(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	for _, tc := range []struct{ body, errType string }{
		{"", "action_request_validation_exception"},
		{"not json", "mapper_parsing_exception"},
		{"[1,2]", "mapper_parsing_exception"},
	} {
		status, body := osDo(t, ts, http.MethodPut, "/docs/_doc/1", tc.body)
		assert.Equal(t, http.StatusBadRequest, status, body)
		assert.Contains(t, body, `"type":"`+tc.errType+`"`)
	}
}
