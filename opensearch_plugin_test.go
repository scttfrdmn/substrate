package substrate_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// newOpenSearchTestServer builds a minimal server with the OpenSearchPlugin registered.
func newOpenSearchTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.OpenSearchPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize opensearch plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func osReq(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal opensearch body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("build opensearch request: %v", err)
	}
	// Use an OpenSearch managed-domain host so routing works.
	req.Host = "my-domain.us-east-1.es.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do opensearch request: %v", err)
	}
	return resp
}

func osBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	defer r.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read opensearch response body: %v", err)
	}
	return b
}

func osRaw(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	b := osBody(t, r)
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode opensearch JSON: %v\nbody: %s", err, b)
	}
	return m
}

// bulkLine builds a NDJSON bulk line pair.
func bulkLine(index, id string, doc map[string]interface{}) string {
	actionLine, _ := json.Marshal(map[string]interface{}{
		"index": map[string]interface{}{"_index": index, "_id": id},
	})
	docLine, _ := json.Marshal(doc)
	return string(actionLine) + "\n" + string(docLine) + "\n"
}

// TestOpenSearchPlugin_IndexLifecycle verifies CreateIndex, GetIndex, DeleteIndex.
func TestOpenSearchPlugin_IndexLifecycle(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	// Create index.
	r := osReq(t, ts, http.MethodPut, "/my-index", map[string]interface{}{
		"settings": map[string]interface{}{"number_of_shards": 1},
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("create index: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	if result["acknowledged"] != true {
		t.Errorf("acknowledged: expected true, got %v", result["acknowledged"])
	}

	// Duplicate create returns error.
	r2 := osReq(t, ts, http.MethodPut, "/my-index", nil)
	if r2.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate create: expected 400, got %d; body: %s", r2.StatusCode, osBody(t, r2))
	}

	// Delete index.
	r3 := osReq(t, ts, http.MethodDelete, "/my-index", nil)
	if r3.StatusCode != http.StatusOK {
		t.Fatalf("delete index: expected 200, got %d; body: %s", r3.StatusCode, osBody(t, r3))
	}

	// Delete again returns 404.
	r4 := osReq(t, ts, http.MethodDelete, "/my-index", nil)
	if r4.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete: expected 404, got %d", r4.StatusCode)
	}
	osBody(t, r4)
}

// TestOpenSearchPlugin_IndexDocument_AndGet verifies single document indexing and retrieval.
func TestOpenSearchPlugin_IndexDocument_AndGet(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	// Index a document (auto-creates index).
	r := osReq(t, ts, http.MethodPut, "/test-idx/_doc/doc1", map[string]interface{}{
		"gene":  "BRCA1",
		"chrom": "17",
	})
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("index doc: expected 201, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	if result["_id"] != "doc1" {
		t.Errorf("_id: expected doc1, got %v", result["_id"])
	}
	if result["result"] != "created" {
		t.Errorf("result: expected created, got %v", result["result"])
	}

	// Get document.
	r2 := osReq(t, ts, http.MethodGet, "/test-idx/_doc/doc1", nil)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("get doc: expected 200, got %d; body: %s", r2.StatusCode, osBody(t, r2))
	}
	got := osRaw(t, r2)
	if got["found"] != true {
		t.Errorf("found: expected true, got %v", got["found"])
	}
	src, _ := got["_source"].(map[string]interface{})
	if src["gene"] != "BRCA1" {
		t.Errorf("_source.gene: expected BRCA1, got %v", src["gene"])
	}

	// Get non-existent document.
	r3 := osReq(t, ts, http.MethodGet, "/test-idx/_doc/nope", nil)
	if r3.StatusCode != http.StatusNotFound {
		t.Fatalf("missing doc: expected 404, got %d", r3.StatusCode)
	}
	osBody(t, r3)
}

// TestOpenSearchPlugin_Bulk verifies bulk indexing.
func TestOpenSearchPlugin_Bulk(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	body := bulkLine("bulk-idx", "1", map[string]interface{}{"gene": "BRCA1", "val": 10}) +
		bulkLine("bulk-idx", "2", map[string]interface{}{"gene": "TP53", "val": 20}) +
		bulkLine("bulk-idx", "3", map[string]interface{}{"gene": "BRCA1", "val": 5})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/bulk-idx/_bulk", bytes.NewBufferString(body))
	req.Host = "my-domain.us-east-1.es.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("bulk request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bulk: expected 200, got %d; body: %s", resp.StatusCode, osBody(t, resp))
	}
	result := osRaw(t, resp)
	if result["errors"] != false {
		t.Errorf("errors: expected false, got %v", result["errors"])
	}
	items, _ := result["items"].([]interface{})
	if len(items) != 3 {
		t.Errorf("items: expected 3, got %d", len(items))
	}
}

// TestOpenSearchPlugin_Search_MatchAll returns all documents.
func TestOpenSearchPlugin_Search_MatchAll(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	// Seed documents.
	for i := 0; i < 5; i++ {
		osReq(t, ts, http.MethodPut, fmt.Sprintf("/search-idx/_doc/d%d", i),
			map[string]interface{}{"n": i})
		osBody(t, osReq(t, ts, http.MethodGet, fmt.Sprintf("/search-idx/_doc/d%d", i), nil))
	}

	r := osReq(t, ts, http.MethodPost, "/search-idx/_search", map[string]interface{}{
		"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		"size":  10,
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("search: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	hits, _ := result["hits"].(map[string]interface{})
	total, _ := hits["total"].(map[string]interface{})
	if total["value"].(float64) != 5 {
		t.Errorf("total: expected 5, got %v", total["value"])
	}
}

// TestOpenSearchPlugin_Search_TermFilter filters documents by field value.
func TestOpenSearchPlugin_Search_TermFilter(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	body := bulkLine("filter-idx", "1", map[string]interface{}{"gene": "BRCA1"}) +
		bulkLine("filter-idx", "2", map[string]interface{}{"gene": "TP53"}) +
		bulkLine("filter-idx", "3", map[string]interface{}{"gene": "BRCA1"})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/filter-idx/_bulk", bytes.NewBufferString(body))
	req.Host = "my-domain.us-east-1.es.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, _ := http.DefaultClient.Do(req)
	osBody(t, resp)

	r := osReq(t, ts, http.MethodPost, "/filter-idx/_search", map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{"gene": "BRCA1"},
		},
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("search term: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	hits, _ := result["hits"].(map[string]interface{})
	total, _ := hits["total"].(map[string]interface{})
	if total["value"].(float64) != 2 {
		t.Errorf("term filter total: expected 2, got %v", total["value"])
	}
}

// TestOpenSearchPlugin_Search_TermsAggregation verifies bucket aggregations.
func TestOpenSearchPlugin_Search_TermsAggregation(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	body := bulkLine("agg-idx", "1", map[string]interface{}{"gene": "BRCA1", "count": 10}) +
		bulkLine("agg-idx", "2", map[string]interface{}{"gene": "TP53", "count": 20}) +
		bulkLine("agg-idx", "3", map[string]interface{}{"gene": "BRCA1", "count": 5}) +
		bulkLine("agg-idx", "4", map[string]interface{}{"gene": "TP53", "count": 15})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/agg-idx/_bulk", bytes.NewBufferString(body))
	req.Host = "my-domain.us-east-1.es.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, _ := http.DefaultClient.Do(req)
	osBody(t, resp)

	r := osReq(t, ts, http.MethodPost, "/agg-idx/_search", map[string]interface{}{
		"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		"size":  0,
		"aggs": map[string]interface{}{
			"gene_counts": map[string]interface{}{
				"terms": map[string]interface{}{"field": "gene", "size": 10},
			},
			"total_count": map[string]interface{}{
				"sum": map[string]interface{}{"field": "count"},
			},
		},
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("agg search: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	aggs, _ := result["aggregations"].(map[string]interface{})
	if aggs == nil {
		t.Fatal("aggregations missing from response")
	}

	geneCounts, _ := aggs["gene_counts"].(map[string]interface{})
	buckets, _ := geneCounts["buckets"].([]interface{})
	if len(buckets) != 2 {
		t.Errorf("gene_counts buckets: expected 2, got %d", len(buckets))
	}

	totalCount, _ := aggs["total_count"].(map[string]interface{})
	if totalCount["value"].(float64) != 50 {
		t.Errorf("total_count sum: expected 50, got %v", totalCount["value"])
	}
}

// TestOpenSearchPlugin_Scroll verifies scroll pagination.
func TestOpenSearchPlugin_Scroll(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	// Seed 5 documents.
	body := ""
	for i := 0; i < 5; i++ {
		body += bulkLine("scroll-idx", fmt.Sprintf("d%d", i), map[string]interface{}{"n": i})
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/scroll-idx/_bulk", bytes.NewBufferString(body))
	req.Host = "my-domain.us-east-1.es.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, _ := http.DefaultClient.Do(req)
	osBody(t, resp)

	// First search with scroll, size=2 — should return 2 hits and a scroll_id.
	r := osReq(t, ts, http.MethodPost, "/scroll-idx/_search?scroll=1m", map[string]interface{}{
		"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		"size":  2,
	})
	if r.StatusCode != http.StatusOK {
		t.Fatalf("scroll search: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	first := osRaw(t, r)
	scrollID, ok := first["_scroll_id"].(string)
	if !ok || scrollID == "" {
		t.Fatal("expected _scroll_id in scroll search response")
	}
	firstHits, _ := first["hits"].(map[string]interface{})
	firstPage, _ := firstHits["hits"].([]interface{})
	if len(firstPage) != 2 {
		t.Errorf("first page: expected 2 hits, got %d", len(firstPage))
	}

	// Scroll continuation — next page.
	r2 := osReq(t, ts, http.MethodPost, "/_search/scroll", map[string]interface{}{
		"scroll":    "1m",
		"scroll_id": scrollID,
	})
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("scroll next: expected 200, got %d; body: %s", r2.StatusCode, osBody(t, r2))
	}
	second := osRaw(t, r2)
	secondHits, _ := second["hits"].(map[string]interface{})
	secondPage, _ := secondHits["hits"].([]interface{})
	if len(secondPage) != 2 {
		t.Errorf("second page: expected 2 hits, got %d", len(secondPage))
	}

	// ClearScroll.
	r3 := osReq(t, ts, http.MethodDelete, "/_search/scroll", map[string]interface{}{
		"scroll_id": scrollID,
	})
	if r3.StatusCode != http.StatusOK {
		t.Fatalf("clear scroll: expected 200, got %d; body: %s", r3.StatusCode, osBody(t, r3))
	}
	cleared := osRaw(t, r3)
	if cleared["succeeded"] != true {
		t.Errorf("clear scroll succeeded: expected true, got %v", cleared["succeeded"])
	}
}

// TestOpenSearchPlugin_ClusterHealth verifies /_cluster/health.
func TestOpenSearchPlugin_ClusterHealth(t *testing.T) {
	ts := newOpenSearchTestServer(t)

	r := osReq(t, ts, http.MethodGet, "/_cluster/health", nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("cluster health: expected 200, got %d; body: %s", r.StatusCode, osBody(t, r))
	}
	result := osRaw(t, r)
	if result["status"] != "green" {
		t.Errorf("status: expected green, got %v", result["status"])
	}
}
