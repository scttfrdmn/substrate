package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// opensearchNamespace is the state namespace used by OpenSearchPlugin.
const opensearchNamespace = "opensearch"

// OpenSearchPlugin emulates Amazon OpenSearch Service (formerly Elasticsearch Service).
// It handles REST requests on hosts matching *.es.amazonaws.com or *.aoss.amazonaws.com
// and supports index lifecycle, document CRUD, search with simple query DSL,
// aggregations, and scroll pagination.
type OpenSearchPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the canonical service name for routing.
func (p *OpenSearchPlugin) Name() string { return "opensearch" }

// Initialize sets up the OpenSearchPlugin with state, logger, and optional TimeController.
func (p *OpenSearchPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown releases resources held by the OpenSearchPlugin.
func (p *OpenSearchPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches incoming requests to the appropriate handler.
func (p *OpenSearchPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	method := req.Operation // For REST plugins, Operation holds the HTTP method.
	path := req.Path

	// Trim leading slash.
	rest := strings.TrimPrefix(path, "/")

	// Clear scroll: DELETE /_search/scroll
	if rest == "_search/scroll" && method == http.MethodDelete {
		return p.clearScroll(ctx, req)
	}
	// Scroll continuation: GET or POST /_search/scroll
	if rest == "_search/scroll" {
		return p.scroll(ctx, req)
	}
	// Cluster health: GET /_cluster/health
	if rest == "_cluster/health" {
		return p.clusterHealth()
	}

	// Parse index name from path: /{index}/{rest...}
	parts := strings.SplitN(rest, "/", 2)
	index := parts[0]
	var subPath string
	if len(parts) == 2 {
		subPath = parts[1]
	}

	if index == "" {
		return openSearchError(http.StatusBadRequest, "index_not_specified", "index name required"), nil
	}

	switch {
	case subPath == "" && method == http.MethodPut:
		return p.createIndex(ctx, req, index)
	case subPath == "" && method == http.MethodDelete:
		return p.deleteIndex(ctx, req, index)
	case subPath == "" && (method == http.MethodGet || method == http.MethodHead):
		return p.getIndex(ctx, index)
	case subPath == "_search" || subPath == "_search/":
		return p.search(ctx, req, index)
	case subPath == "_bulk" || subPath == "_bulk/":
		return p.bulk(ctx, req, index)
	case strings.HasPrefix(subPath, "_doc/"):
		docID := strings.TrimPrefix(subPath, "_doc/")
		switch method {
		case http.MethodPut, http.MethodPost:
			return p.indexDocument(ctx, req, index, docID)
		case http.MethodGet:
			return p.getDocument(ctx, index, docID)
		case http.MethodDelete:
			return p.deleteDocument(ctx, index, docID)
		}
	case subPath == "_doc" && (method == http.MethodPost):
		return p.indexDocument(ctx, req, index, "")
	case strings.HasPrefix(subPath, "_mapping") || strings.HasPrefix(subPath, "_mappings"):
		return p.putMapping(ctx, index)
	case subPath == "_refresh":
		return openSearchOK(map[string]interface{}{"_shards": map[string]int{"total": 1, "successful": 1, "failed": 0}}), nil
	}

	return openSearchError(http.StatusNotFound, "route_not_found", "no handler for "+method+" /"+rest), nil
}

// --- Index operations ---

func (p *OpenSearchPlugin) createIndex(_ *RequestContext, req *AWSRequest, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "index:" + index
	existing, _ := p.state.Get(goCtx, opensearchNamespace, key)
	if existing != nil {
		return openSearchError(http.StatusBadRequest, "resource_already_exists_exception",
			"index ["+index+"] already exists"), nil
	}
	meta := map[string]interface{}{
		"index":        index,
		"created_at":   p.tc.Now().Format(time.RFC3339),
		"doc_count":    0,
	}
	// Optionally parse mappings/settings from body.
	if len(req.Body) > 0 {
		var body map[string]interface{}
		if err := json.Unmarshal(req.Body, &body); err == nil {
			if m, ok := body["mappings"]; ok {
				meta["mappings"] = m
			}
			if s, ok := body["settings"]; ok {
				meta["settings"] = s
			}
		}
	}
	data, _ := json.Marshal(meta)
	if err := p.state.Put(goCtx, opensearchNamespace, key, data); err != nil {
		return nil, fmt.Errorf("opensearch createIndex put: %w", err)
	}
	return openSearchOK(map[string]interface{}{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               index,
	}), nil
}

func (p *OpenSearchPlugin) deleteIndex(_ *RequestContext, _ *AWSRequest, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "index:" + index
	existing, _ := p.state.Get(goCtx, opensearchNamespace, key)
	if existing == nil {
		return openSearchError(http.StatusNotFound, "index_not_found_exception",
			"no such index ["+index+"]"), nil
	}
	if err := p.state.Delete(goCtx, opensearchNamespace, key); err != nil {
		return nil, fmt.Errorf("opensearch deleteIndex: %w", err)
	}
	// Also delete all documents in the index.
	docIDs := p.loadDocIDs(goCtx, index)
	for _, id := range docIDs {
		_ = p.state.Delete(goCtx, opensearchNamespace, "doc:"+index+"/"+id)
	}
	_ = p.state.Delete(goCtx, opensearchNamespace, "doc_ids:"+index)
	return openSearchOK(map[string]interface{}{"acknowledged": true}), nil
}

func (p *OpenSearchPlugin) getIndex(_ *RequestContext, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	key := "index:" + index
	data, _ := p.state.Get(goCtx, opensearchNamespace, key)
	if data == nil {
		return openSearchError(http.StatusNotFound, "index_not_found_exception",
			"no such index ["+index+"]"), nil
	}
	var meta map[string]interface{}
	_ = json.Unmarshal(data, &meta)
	return openSearchOK(map[string]interface{}{
		index: map[string]interface{}{
			"aliases":  map[string]interface{}{},
			"mappings": orEmpty(meta["mappings"]),
			"settings": map[string]interface{}{
				"index": map[string]interface{}{
					"number_of_shards":   "1",
					"number_of_replicas": "0",
				},
			},
		},
	}), nil
}

func (p *OpenSearchPlugin) putMapping(_ *RequestContext, index string) (*AWSResponse, error) {
	// Accept and ignore mapping updates; return acknowledged.
	_ = index
	return openSearchOK(map[string]interface{}{"acknowledged": true}), nil
}

// --- Document operations ---

func (p *OpenSearchPlugin) indexDocument(ctx *RequestContext, req *AWSRequest, index, docID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if docID == "" {
		docID = generateOpenSearchID()
	}
	// Ensure the index exists (auto-create).
	indexKey := "index:" + index
	if data, _ := p.state.Get(goCtx, opensearchNamespace, indexKey); data == nil {
		meta := map[string]interface{}{"index": index, "created_at": p.tc.Now().Format(time.RFC3339), "doc_count": 0}
		d, _ := json.Marshal(meta)
		_ = p.state.Put(goCtx, opensearchNamespace, indexKey, d)
	}

	docKey := "doc:" + index + "/" + docID
	isNew := true
	if existing, _ := p.state.Get(goCtx, opensearchNamespace, docKey); existing != nil {
		isNew = false
	}

	// Store the raw document source.
	if err := p.state.Put(goCtx, opensearchNamespace, docKey, req.Body); err != nil {
		return nil, fmt.Errorf("opensearch indexDocument put: %w", err)
	}
	if isNew {
		updateStringIndex(goCtx, p.state, opensearchNamespace, "doc_ids:"+index, docID)
	}

	result := "updated"
	if isNew {
		result = "created"
	}
	return openSearchStatusOK(http.StatusCreated, map[string]interface{}{
		"_index":   index,
		"_id":      docID,
		"_version": 1,
		"result":   result,
		"_shards":  map[string]int{"total": 1, "successful": 1, "failed": 0},
		"_seq_no":  0,
	}), nil
}

func (p *OpenSearchPlugin) getDocument(_ *RequestContext, index, docID string) (*AWSResponse, error) {
	goCtx := context.Background()
	docKey := "doc:" + index + "/" + docID
	data, _ := p.state.Get(goCtx, opensearchNamespace, docKey)
	if data == nil {
		return openSearchError(http.StatusNotFound, "not_found", "document not found"), nil
	}
	var source interface{}
	_ = json.Unmarshal(data, &source)
	return openSearchOK(map[string]interface{}{
		"_index":   index,
		"_id":      docID,
		"_version": 1,
		"found":    true,
		"_source":  source,
	}), nil
}

func (p *OpenSearchPlugin) deleteDocument(_ *RequestContext, index, docID string) (*AWSResponse, error) {
	goCtx := context.Background()
	docKey := "doc:" + index + "/" + docID
	if err := p.state.Delete(goCtx, opensearchNamespace, docKey); err != nil {
		return nil, fmt.Errorf("opensearch deleteDocument: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, opensearchNamespace, "doc_ids:"+index, docID)
	return openSearchOK(map[string]interface{}{
		"_index":   index,
		"_id":      docID,
		"_version": 2,
		"result":   "deleted",
		"_shards":  map[string]int{"total": 1, "successful": 1, "failed": 0},
	}), nil
}

// --- Bulk API ---

func (p *OpenSearchPlugin) bulk(ctx *RequestContext, req *AWSRequest, defaultIndex string) (*AWSResponse, error) {
	goCtx := context.Background()
	lines := strings.Split(strings.TrimSpace(string(req.Body)), "\n")
	var items []interface{}
	i := 0
	for i < len(lines) {
		if lines[i] == "" {
			i++
			continue
		}
		var action map[string]map[string]string
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			i++
			continue
		}
		i++
		for op, meta := range action {
			idx := meta["_index"]
			if idx == "" {
				idx = defaultIndex
			}
			docID := meta["_id"]
			switch op {
			case "index", "create":
				if i < len(lines) {
					body := []byte(lines[i])
					i++
					if docID == "" {
						docID = generateOpenSearchID()
					}
					docKey := "doc:" + idx + "/" + docID
					isNew := true
					if existing, _ := p.state.Get(goCtx, opensearchNamespace, docKey); existing != nil {
						isNew = false
					}
					_ = p.state.Put(goCtx, opensearchNamespace, docKey, body)
					if isNew {
						updateStringIndex(goCtx, p.state, opensearchNamespace, "doc_ids:"+idx, docID)
						// Ensure index exists.
						indexKey := "index:" + idx
						if d, _ := p.state.Get(goCtx, opensearchNamespace, indexKey); d == nil {
							m := map[string]interface{}{"index": idx, "created_at": p.tc.Now().Format(time.RFC3339)}
							d2, _ := json.Marshal(m)
							_ = p.state.Put(goCtx, opensearchNamespace, indexKey, d2)
						}
					}
					result := "updated"
					if isNew {
						result = "created"
					}
					items = append(items, map[string]interface{}{
						op: map[string]interface{}{"_index": idx, "_id": docID, "result": result, "status": 200},
					})
				}
			case "delete":
				docKey := "doc:" + idx + "/" + docID
				_ = p.state.Delete(goCtx, opensearchNamespace, docKey)
				removeFromStringIndex(goCtx, p.state, opensearchNamespace, "doc_ids:"+idx, docID)
				items = append(items, map[string]interface{}{
					"delete": map[string]interface{}{"_index": idx, "_id": docID, "result": "deleted", "status": 200},
				})
			}
		}
	}
	_ = ctx
	return openSearchOK(map[string]interface{}{
		"took":   1,
		"errors": false,
		"items":  items,
	}), nil
}

// --- Search ---

func (p *OpenSearchPlugin) search(ctx *RequestContext, req *AWSRequest, index string) (*AWSResponse, error) {
	goCtx := context.Background()

	// Parse request body.
	var body openSearchSearchRequest
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	// Load all documents in this index.
	docs := p.loadDocs(goCtx, index)

	// Apply query filter.
	filtered := osFilterDocs(docs, body.Query)

	// Determine pagination.
	size := body.Size
	if size <= 0 {
		size = 10
	}
	from := body.From

	// Scroll support: save full result set and return first page.
	scrollTTL := req.Params["scroll"]
	if scrollTTL == "" && body.Scroll != "" {
		scrollTTL = body.Scroll
	}
	if scrollTTL != "" {
		scrollID := generateOpenSearchID()
		// Store remaining IDs (skip first page) as scroll state.
		allIDs := make([]string, 0, len(filtered))
		for _, d := range filtered {
			allIDs = append(allIDs, d["_id"].(string))
		}
		var remaining []string
		if from+size < len(allIDs) {
			remaining = allIDs[from+size:]
		}
		scrollState := map[string]interface{}{
			"index":     index,
			"remaining": remaining,
			"size":      size,
		}
		sd, _ := json.Marshal(scrollState)
		_ = p.state.Put(goCtx, opensearchNamespace, "scroll:"+scrollID, sd)
		hits := osPageHits(filtered, from, size)
		return openSearchOK(p.buildSearchResponse(filtered, hits, body, scrollID)), nil
	}

	hits := osPageHits(filtered, from, size)
	return openSearchOK(p.buildSearchResponse(filtered, hits, body, "")), nil
}

func (p *OpenSearchPlugin) buildSearchResponse(allDocs []map[string]interface{}, pageHits []map[string]interface{}, body openSearchSearchRequest, scrollID string) map[string]interface{} {
	resp := map[string]interface{}{
		"took":      1,
		"timed_out": false,
		"_shards":   map[string]int{"total": 1, "successful": 1, "skipped": 0, "failed": 0},
		"hits": map[string]interface{}{
			"total":     map[string]interface{}{"value": len(allDocs), "relation": "eq"},
			"max_score": 1.0,
			"hits":      pageHits,
		},
	}
	if scrollID != "" {
		resp["_scroll_id"] = scrollID
	}
	if len(body.Aggs) > 0 {
		resp["aggregations"] = osComputeAggs(allDocs, body.Aggs)
	}
	return resp
}

// --- Scroll ---

func (p *OpenSearchPlugin) scroll(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	var scrollID string
	var body struct {
		ScrollID string `json:"scroll_id"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
		scrollID = body.ScrollID
	}
	if scrollID == "" {
		scrollID = req.Params["scroll_id"]
	}
	if scrollID == "" {
		return openSearchError(http.StatusBadRequest, "illegal_argument_exception", "scroll_id required"), nil
	}

	stateKey := "scroll:" + scrollID
	data, _ := p.state.Get(goCtx, opensearchNamespace, stateKey)
	if data == nil {
		return openSearchError(http.StatusNotFound, "search_context_missing_exception", "scroll context expired"), nil
	}

	var state struct {
		Index     string   `json:"index"`
		Remaining []string `json:"remaining"`
		Size      int      `json:"size"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return openSearchError(http.StatusInternalServerError, "internal_error", "bad scroll state"), nil
	}

	size := state.Size
	if size <= 0 {
		size = 10
	}

	// Build page from remaining IDs.
	var page []string
	var newRemaining []string
	if len(state.Remaining) <= size {
		page = state.Remaining
	} else {
		page = state.Remaining[:size]
		newRemaining = state.Remaining[size:]
	}

	// Load docs for this page.
	hits := make([]map[string]interface{}, 0, len(page))
	for _, id := range page {
		docData, _ := p.state.Get(goCtx, opensearchNamespace, "doc:"+state.Index+"/"+id)
		if docData != nil {
			var src interface{}
			_ = json.Unmarshal(docData, &src)
			hits = append(hits, map[string]interface{}{
				"_index":  state.Index,
				"_id":     id,
				"_score":  1.0,
				"_source": src,
			})
		}
	}

	// Update scroll state.
	state.Remaining = newRemaining
	updated, _ := json.Marshal(state)
	_ = p.state.Put(goCtx, opensearchNamespace, stateKey, updated)

	return openSearchOK(map[string]interface{}{
		"_scroll_id": scrollID,
		"took":       1,
		"timed_out":  false,
		"_shards":    map[string]int{"total": 1, "successful": 1, "skipped": 0, "failed": 0},
		"hits": map[string]interface{}{
			"total":     map[string]interface{}{"value": len(hits), "relation": "eq"},
			"max_score": 1.0,
			"hits":      hits,
		},
	}), nil
}

func (p *OpenSearchPlugin) clearScroll(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var body struct {
		ScrollID interface{} `json:"scroll_id"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	// Accept single ID or slice.
	var ids []string
	switch v := body.ScrollID.(type) {
	case string:
		ids = []string{v}
	case []interface{}:
		for _, x := range v {
			if s, ok := x.(string); ok {
				ids = append(ids, s)
			}
		}
	}
	for _, id := range ids {
		_ = p.state.Delete(goCtx, opensearchNamespace, "scroll:"+id)
	}
	return openSearchOK(map[string]interface{}{
		"succeeded": true,
		"num_freed": len(ids),
	}), nil
}

// --- Cluster health ---

func (p *OpenSearchPlugin) clusterHealth() (*AWSResponse, error) {
	return openSearchOK(map[string]interface{}{
		"cluster_name":                 "substrate",
		"status":                       "green",
		"timed_out":                    false,
		"number_of_nodes":              1,
		"number_of_data_nodes":         1,
		"active_primary_shards":        1,
		"active_shards":                1,
		"relocating_shards":            0,
		"initializing_shards":          0,
		"unassigned_shards":            0,
		"delayed_unassigned_shards":    0,
		"number_of_pending_tasks":      0,
		"active_shards_percent_as_number": 100.0,
	}), nil
}

// --- Helpers ---

func (p *OpenSearchPlugin) loadDocIDs(ctx context.Context, index string) []string {
	ids, _ := loadStringIndex(ctx, p.state, opensearchNamespace, "doc_ids:"+index)
	return ids
}

func (p *OpenSearchPlugin) loadDocs(ctx context.Context, index string) []map[string]interface{} {
	ids := p.loadDocIDs(ctx, index)
	docs := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		data, _ := p.state.Get(ctx, opensearchNamespace, "doc:"+index+"/"+id)
		if data == nil {
			continue
		}
		var src map[string]interface{}
		if err := json.Unmarshal(data, &src); err != nil {
			continue
		}
		docs = append(docs, map[string]interface{}{
			"_id":     id,
			"_source": src,
		})
	}
	return docs
}

// openSearchSearchRequest mirrors the OpenSearch search request body.
type openSearchSearchRequest struct {
	Query  map[string]interface{} `json:"query"`
	Size   int                    `json:"size"`
	From   int                    `json:"from"`
	Source interface{}            `json:"_source"`
	Sort   interface{}            `json:"sort"`
	Aggs   map[string]interface{} `json:"aggs"`
	Scroll string                 `json:"scroll"`
}

// osFilterDocs applies a query to the document list and returns matching docs.
func osFilterDocs(docs []map[string]interface{}, query map[string]interface{}) []map[string]interface{} {
	if len(query) == 0 {
		return docs
	}
	var result []map[string]interface{}
	for _, doc := range docs {
		if osMatchDoc(doc, query) {
			result = append(result, doc)
		}
	}
	return result
}

// osMatchDoc returns true if a document matches the given query clause.
func osMatchDoc(doc map[string]interface{}, query map[string]interface{}) bool {
	src, _ := doc["_source"].(map[string]interface{})

	for qType, qVal := range query {
		switch qType {
		case "match_all":
			return true
		case "match_none":
			return false
		case "term":
			terms, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			for field, val := range terms {
				fieldVal, ok := val.(map[string]interface{})
				if ok {
					val = fieldVal["value"]
				}
				if !osFieldEquals(src, field, val) {
					return false
				}
			}
		case "terms":
			terms, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			for field, vals := range terms {
				list, ok := vals.([]interface{})
				if !ok {
					return false
				}
				docVal := osGetField(src, field)
				matched := false
				for _, v := range list {
					if fmt.Sprintf("%v", docVal) == fmt.Sprintf("%v", v) {
						matched = true
						break
					}
				}
				if !matched {
					return false
				}
			}
		case "bool":
			boolQ, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			if must, ok := boolQ["must"]; ok {
				for _, clause := range osToClauseList(must) {
					if !osMatchDoc(doc, clause) {
						return false
					}
				}
			}
			if should, ok := boolQ["should"]; ok {
				clauses := osToClauseList(should)
				if len(clauses) > 0 {
					anyMatch := false
					for _, clause := range clauses {
						if osMatchDoc(doc, clause) {
							anyMatch = true
							break
						}
					}
					if !anyMatch {
						return false
					}
				}
			}
			if mustNot, ok := boolQ["must_not"]; ok {
				for _, clause := range osToClauseList(mustNot) {
					if osMatchDoc(doc, clause) {
						return false
					}
				}
			}
		case "exists":
			existsQ, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			field, _ := existsQ["field"].(string)
			if osGetField(src, field) == nil {
				return false
			}
		case "range":
			rangeQ, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			for field, rangeVals := range rangeQ {
				rv, ok := rangeVals.(map[string]interface{})
				if !ok {
					return false
				}
				docVal := osGetField(src, field)
				if !osRangeMatch(docVal, rv) {
					return false
				}
			}
		case "match":
			matchQ, ok := qVal.(map[string]interface{})
			if !ok {
				return false
			}
			for field, val := range matchQ {
				query, ok := val.(map[string]interface{})
				if ok {
					val = query["query"]
				}
				docVal := fmt.Sprintf("%v", osGetField(src, field))
				queryStr := fmt.Sprintf("%v", val)
				if !strings.Contains(strings.ToLower(docVal), strings.ToLower(queryStr)) {
					return false
				}
			}
		}
	}
	return true
}

func osToClauseList(v interface{}) []map[string]interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return []map[string]interface{}{t}
	case []interface{}:
		var out []map[string]interface{}
		for _, item := range t {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// osGetField retrieves a (possibly dot-separated) field from a document source.
func osGetField(src map[string]interface{}, field string) interface{} {
	if src == nil {
		return nil
	}
	if !strings.Contains(field, ".") {
		return src[field]
	}
	parts := strings.SplitN(field, ".", 2)
	nested, ok := src[parts[0]].(map[string]interface{})
	if !ok {
		return nil
	}
	return osGetField(nested, parts[1])
}

func osFieldEquals(src map[string]interface{}, field string, val interface{}) bool {
	docVal := osGetField(src, field)
	return fmt.Sprintf("%v", docVal) == fmt.Sprintf("%v", val)
}

func osRangeMatch(docVal interface{}, rv map[string]interface{}) bool {
	dv, err := strconv.ParseFloat(fmt.Sprintf("%v", docVal), 64)
	if err != nil {
		return false
	}
	if gte, ok := rv["gte"]; ok {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", gte), 64)
		if err != nil || dv < v {
			return false
		}
	}
	if gt, ok := rv["gt"]; ok {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", gt), 64)
		if err != nil || dv <= v {
			return false
		}
	}
	if lte, ok := rv["lte"]; ok {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", lte), 64)
		if err != nil || dv > v {
			return false
		}
	}
	if lt, ok := rv["lt"]; ok {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", lt), 64)
		if err != nil || dv >= v {
			return false
		}
	}
	return true
}

func osPageHits(docs []map[string]interface{}, from, size int) []map[string]interface{} {
	if from >= len(docs) {
		return []map[string]interface{}{}
	}
	end := from + size
	if end > len(docs) {
		end = len(docs)
	}
	page := docs[from:end]
	hits := make([]map[string]interface{}, 0, len(page))
	for _, d := range page {
		hits = append(hits, map[string]interface{}{
			"_index":  "unknown",
			"_id":     d["_id"],
			"_score":  1.0,
			"_source": d["_source"],
		})
	}
	return hits
}

// osComputeAggs computes simple aggregations over a set of documents.
func osComputeAggs(docs []map[string]interface{}, aggs map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for name, aggDef := range aggs {
		agg, ok := aggDef.(map[string]interface{})
		if !ok {
			continue
		}
		if termsAgg, ok := agg["terms"].(map[string]interface{}); ok {
			field, _ := termsAgg["field"].(string)
			sizeVal := 10
			if sv, ok := termsAgg["size"].(float64); ok {
				sizeVal = int(sv)
			}
			result[name] = osTermsAgg(docs, field, sizeVal)
		} else if cardinalityAgg, ok := agg["cardinality"].(map[string]interface{}); ok {
			field, _ := cardinalityAgg["field"].(string)
			result[name] = osCardinalityAgg(docs, field)
		} else if valueCountAgg, ok := agg["value_count"].(map[string]interface{}); ok {
			field, _ := valueCountAgg["field"].(string)
			result[name] = osValueCountAgg(docs, field)
		} else if sumAgg, ok := agg["sum"].(map[string]interface{}); ok {
			field, _ := sumAgg["field"].(string)
			result[name] = osSumAgg(docs, field)
		} else if avgAgg, ok := agg["avg"].(map[string]interface{}); ok {
			field, _ := avgAgg["field"].(string)
			result[name] = osAvgAgg(docs, field)
		} else if maxAgg, ok := agg["max"].(map[string]interface{}); ok {
			field, _ := maxAgg["field"].(string)
			result[name] = osMaxAgg(docs, field)
		} else if minAgg, ok := agg["min"].(map[string]interface{}); ok {
			field, _ := minAgg["field"].(string)
			result[name] = osMinAgg(docs, field)
		}
	}
	return result
}

func osTermsAgg(docs []map[string]interface{}, field string, size int) map[string]interface{} {
	counts := make(map[string]int)
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v := fmt.Sprintf("%v", osGetField(src, field))
		counts[v]++
	}
	type bucket struct {
		key   string
		count int
	}
	var buckets []bucket
	for k, c := range counts {
		buckets = append(buckets, bucket{k, c})
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].count != buckets[j].count {
			return buckets[i].count > buckets[j].count
		}
		return buckets[i].key < buckets[j].key
	})
	if len(buckets) > size {
		buckets = buckets[:size]
	}
	result := make([]map[string]interface{}, 0, len(buckets))
	for _, b := range buckets {
		result = append(result, map[string]interface{}{"key": b.key, "doc_count": b.count})
	}
	return map[string]interface{}{
		"doc_count_error_upper_bound": 0,
		"sum_other_doc_count":         0,
		"buckets":                     result,
	}
}

func osCardinalityAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	seen := make(map[string]struct{})
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v := fmt.Sprintf("%v", osGetField(src, field))
		seen[v] = struct{}{}
	}
	return map[string]interface{}{"value": len(seen)}
}

func osValueCountAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	count := 0
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		if osGetField(src, field) != nil {
			count++
		}
	}
	return map[string]interface{}{"value": count}
}

func osSumAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	var sum float64
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v, _ := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(src, field)), 64)
		sum += v
	}
	return map[string]interface{}{"value": sum}
}

func osAvgAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	if len(docs) == 0 {
		return map[string]interface{}{"value": nil}
	}
	var sum float64
	count := 0
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(src, field)), 64)
		if err == nil {
			sum += v
			count++
		}
	}
	if count == 0 {
		return map[string]interface{}{"value": nil}
	}
	return map[string]interface{}{"value": sum / float64(count)}
}

func osMaxAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	var max float64
	first := true
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(src, field)), 64)
		if err == nil && (first || v > max) {
			max = v
			first = false
		}
	}
	if first {
		return map[string]interface{}{"value": nil}
	}
	return map[string]interface{}{"value": max}
}

func osMinAgg(docs []map[string]interface{}, field string) map[string]interface{} {
	var min float64
	first := true
	for _, d := range docs {
		src, _ := d["_source"].(map[string]interface{})
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(src, field)), 64)
		if err == nil && (first || v < min) {
			min = v
			first = false
		}
	}
	if first {
		return map[string]interface{}{"value": nil}
	}
	return map[string]interface{}{"value": min}
}

// generateOpenSearchID generates a random URL-safe base64 ID.
func generateOpenSearchID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func orEmpty(v interface{}) interface{} {
	if v == nil {
		return map[string]interface{}{}
	}
	return v
}

// openSearchOK returns a 200 JSON response.
func openSearchOK(body interface{}) *AWSResponse {
	return openSearchStatusOK(http.StatusOK, body)
}

// openSearchStatusOK returns a JSON response with the given status code.
func openSearchStatusOK(status int, body interface{}) *AWSResponse {
	b, _ := json.Marshal(body)
	return &AWSResponse{
		StatusCode: status,
		Body:       b,
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
}

// openSearchError returns an OpenSearch-style error response.
func openSearchError(status int, errType, reason string) *AWSResponse {
	body := map[string]interface{}{
		"error": map[string]interface{}{
			"type":   errType,
			"reason": reason,
		},
		"status": status,
	}
	b, _ := json.Marshal(body)
	return &AWSResponse{
		StatusCode: status,
		Body:       b,
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
}
