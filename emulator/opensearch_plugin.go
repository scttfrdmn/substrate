package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// opensearchNamespace is the state namespace used by OpenSearchPlugin.
const opensearchNamespace = "opensearch"

// opensearchPrimaryTerm is the `_primary_term` every write reports. A primary term increments when a
// shard's primary is reassigned to another node; substrate models one node that never fails over, so
// the term is the first one a primary is given, 1, for the life of the run.
const opensearchPrimaryTerm = 1

// OpenSearchPlugin emulates Amazon OpenSearch Service (formerly Elasticsearch Service).
// It handles REST requests on hosts matching *.es.amazonaws.com or *.aoss.amazonaws.com
// and supports index lifecycle, document CRUD, search with simple query DSL,
// aggregations, and scroll pagination.
//
// # What a state key is scoped by
//
// Every key carries the request's account and Region and a domain segment (#1200), so two accounts,
// two Regions or two domains in one run are separate clusters. The domain is read from the Host
// header: a documented domain endpoint, `search-{domain}-{id}.{region}.es.amazonaws.com` or
// `vpc-{domain}-{id}.…`, names its domain, and a Serverless collection endpoint
// `{id}.{region}.aoss.amazonaws.com` names its collection. Any other host — a base endpoint such as
// `localhost:4566`, which is what a client configured with an endpoint URL sends — resolves to the
// domain `_default`, so every client addressing a base endpoint shares one cluster per account and
// Region. That is a deliberate simplification rather than an accident: the domain is otherwise
// unobservable to the data plane, which has no domain in its paths. See [openSearchDomainFromHost].
//
// # Determinism
//
// Every response is a function of the request and the stored state. Query clauses are evaluated in
// sorted key order, every hit's `_score` is the constant 1.0, and results are ordered by the `sort`
// member when one is given and otherwise by score and then by insertion order (`_seq_no`), which —
// unlike the document ID — does not depend on an identifier minted from the wall-clock request ID
// (#1201). See osSortDocs.
//
// # The control plane
//
// The `opensearch` and `es` control-plane APIs are not emulated; a call to either is refused with an
// AWS-shaped UnknownOperationException rather than misread as an index (#1212, #1564). See
// opensearch_control_plane.go.
type OpenSearchPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController

	// mu serializes every data-plane request. A write reads and rewrites the index record (its
	// `_seq_no` counter) and the document record, and the StateManager has no compare-and-swap, so
	// two concurrent writes to one index would otherwise hand out the same `_seq_no` and lose a
	// `_version` increment. It guards no field of the plugin, only the read-modify-write sequences.
	mu sync.Mutex
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
	// For REST plugins Operation holds the HTTP method unless something resolved it; requestMethod
	// reads whichever field carries the verb.
	method := requestMethod(req)
	if method == "" {
		method = req.Operation
	}

	if isOpenSearchControlPlanePath(req.Path) {
		return nil, openSearchControlPlaneRefusal(method, req.Path)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	resp, err := p.route(ctx, req, openSearchScopeFor(ctx, req), method)
	// HEAD carries no body, whatever the answer: the index-exists reference publishes a status code
	// alone, and the get-document reference the same for a document.
	if method == http.MethodHead && resp != nil {
		resp.Body = nil
	}
	return resp, err
}

// route dispatches a data-plane request on its method and path shape.
func (p *OpenSearchPlugin) route(ctx *RequestContext, req *AWSRequest, scope openSearchScope, method string) (*AWSResponse, error) {
	rest := strings.TrimPrefix(req.Path, "/")

	switch {
	case rest == "_search/scroll" && method == http.MethodDelete:
		return p.clearScroll(req, scope)
	case rest == "_search/scroll":
		return p.scroll(req, scope)
	case rest == "_cluster/health":
		return p.clusterHealth()
	case (rest == "_bulk" || rest == "_bulk/") && (method == http.MethodPost || method == http.MethodPut):
		return p.bulk(ctx, req, scope, "")
	}

	// Parse index name from path: /{index}/{rest...}
	index, subPath, _ := strings.Cut(rest, "/")
	if index == "" {
		return openSearchError(http.StatusBadRequest, "index_not_specified", "index name required").response()
	}

	switch {
	case subPath == "" && method == http.MethodPut:
		return p.createIndex(ctx, req, scope, index)
	case subPath == "" && method == http.MethodDelete:
		return p.deleteIndex(scope, index)
	case subPath == "" && method == http.MethodGet:
		return p.getIndex(scope, index)
	case subPath == "" && method == http.MethodHead:
		return p.headIndex(scope, index)
	case subPath == "_search" || subPath == "_search/":
		return p.search(ctx, req, scope, index)
	case (subPath == "_bulk" || subPath == "_bulk/") && (method == http.MethodPost || method == http.MethodPut):
		return p.bulk(ctx, req, scope, index)
	case subPath == "_mapping" || subPath == "_mappings":
		switch method {
		case http.MethodGet:
			return p.getMapping(scope, index)
		case http.MethodPut, http.MethodPost:
			return p.putMapping(req, scope, index)
		}
	case strings.HasPrefix(subPath, "_doc/") && strings.TrimPrefix(subPath, "_doc/") != "":
		docID := strings.TrimPrefix(subPath, "_doc/")
		switch method {
		case http.MethodPut, http.MethodPost:
			return p.indexDocument(ctx, req, scope, index, docID, req.Params["op_type"] == "create")
		case http.MethodGet:
			return p.getDocument(req, scope, index, docID)
		case http.MethodHead:
			return p.headDocument(scope, index, docID)
		case http.MethodDelete:
			return p.deleteDocument(scope, index, docID)
		}
	case strings.HasPrefix(subPath, "_create/") && strings.TrimPrefix(subPath, "_create/") != "" &&
		(method == http.MethodPut || method == http.MethodPost):
		return p.indexDocument(ctx, req, scope, index, strings.TrimPrefix(subPath, "_create/"), true)
	case (subPath == "_doc" || subPath == "_doc/") && method == http.MethodPost:
		return p.indexDocument(ctx, req, scope, index, "", false)
	case subPath == "_refresh":
		return openSearchOK(map[string]interface{}{"_shards": map[string]int{"total": 1, "successful": 1, "failed": 0}})
	}

	return openSearchError(http.StatusNotFound, "route_not_found", "no handler for "+method+" /"+rest).response()
}

// --- Stored records ---

// openSearchIndexRecord is an index as substrate stores it.
type openSearchIndexRecord struct {
	// Name is the index name, the setting `provided_name` reports.
	Name string `json:"name"`
	// UUID is the index's `uuid` setting, minted at creation and the `index_uuid` a bulk item
	// error reports.
	UUID string `json:"uuid"`
	// CreationDate is the `creation_date` setting: epoch milliseconds, as a string, which is how
	// the get-index reference prints every setting.
	CreationDate string `json:"creation_date"`
	// Settings holds the settings the create supplied, flattened to dotted keys without the
	// `index.` prefix and with every scalar rendered as a string, the form a read reports.
	Settings map[string]interface{} `json:"settings,omitempty"`
	// Mappings is the index's mapping, as the create supplied it and `PUT _mapping` merged into it.
	Mappings map[string]interface{} `json:"mappings,omitempty"`
	// NextSeqNo is the `_seq_no` the next write to the index is assigned. OpenSearch numbers
	// operations per shard; substrate models one primary shard, so per index.
	NextSeqNo int64 `json:"next_seq_no"`
}

// openSearchDocRecord is a document as substrate stores it: its source and the metadata every
// write and read reports alongside it (#1213).
type openSearchDocRecord struct {
	// Source is the document body, as the last write sent it.
	Source json.RawMessage `json:"_source"`
	// Version is `_version`: 1 on creation, incremented by every later write.
	Version int64 `json:"_version"`
	// SeqNo is `_seq_no` of the write that produced this version.
	SeqNo int64 `json:"_seq_no"`
	// PrimaryTerm is `_primary_term` of that write.
	PrimaryTerm int64 `json:"_primary_term"`
}

// loadIndex returns the stored index, or nil when it does not exist.
func (p *OpenSearchPlugin) loadIndex(goCtx context.Context, scope openSearchScope, index string) (*openSearchIndexRecord, error) {
	data, err := p.state.Get(goCtx, opensearchNamespace, scope.indexKey(index))
	if err != nil {
		return nil, fmt.Errorf("opensearch load index %s: %w", index, err)
	}
	if data == nil {
		return nil, nil
	}
	var rec openSearchIndexRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("opensearch decode index %s: %w", index, err)
	}
	if rec.Name == "" {
		rec.Name = index
	}
	return &rec, nil
}

// putIndex stores rec.
func (p *OpenSearchPlugin) putIndex(goCtx context.Context, scope openSearchScope, rec *openSearchIndexRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("opensearch marshal index %s: %w", rec.Name, err)
	}
	if err := p.state.Put(goCtx, opensearchNamespace, scope.indexKey(rec.Name), data); err != nil {
		return fmt.Errorf("opensearch put index %s: %w", rec.Name, err)
	}
	return nil
}

// newIndexRecord builds the record of a new index, minting its UUID from the request's mint so a
// replayed create reports the UUID it reported when recorded.
func (p *OpenSearchPlugin) newIndexRecord(ctx *RequestContext, index string) *openSearchIndexRecord {
	var ids *IDMint
	if ctx != nil {
		ids = ctx.IDs
	}
	return &openSearchIndexRecord{
		Name: index,
		// Twenty-two URL-safe characters, the width of the uuid the get-index reference's example
		// prints ("Onnd4TKBQMODrfvAvNXjAg").
		UUID:         ids.Base64URL(16),
		CreationDate: strconv.FormatInt(p.tc.Now().UnixMilli(), 10),
	}
}

// ensureIndex returns the stored index, creating it first when it does not exist — the automatic
// index creation a document write performs.
func (p *OpenSearchPlugin) ensureIndex(goCtx context.Context, ctx *RequestContext, scope openSearchScope, index string) (*openSearchIndexRecord, error) {
	rec, err := p.loadIndex(goCtx, scope, index)
	if err != nil || rec != nil {
		return rec, err
	}
	rec = p.newIndexRecord(ctx, index)
	if err := p.putIndex(goCtx, scope, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// loadDoc returns the stored document, or nil when it does not exist.
func (p *OpenSearchPlugin) loadDoc(goCtx context.Context, scope openSearchScope, index, id string) (*openSearchDocRecord, error) {
	data, err := p.state.Get(goCtx, opensearchNamespace, scope.docKey(index, id))
	if err != nil {
		return nil, fmt.Errorf("opensearch load document %s/%s: %w", index, id, err)
	}
	if data == nil {
		return nil, nil
	}
	var rec openSearchDocRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("opensearch decode document %s/%s: %w", index, id, err)
	}
	return &rec, nil
}

// writeDoc stores source as document id of idx, given the document's current record (nil when it
// does not exist), and returns the record written. The version increments from existing's; the
// sequence number is the index's next, and idx's counter is advanced. The index record is not
// rewritten here: a caller persists it once per request, which lets a bulk request write each index
// record once rather than once per item.
func (p *OpenSearchPlugin) writeDoc(goCtx context.Context, scope openSearchScope, idx *openSearchIndexRecord, id string, source json.RawMessage, existing *openSearchDocRecord) (*openSearchDocRecord, error) {
	rec := &openSearchDocRecord{Source: source, Version: 1, SeqNo: idx.NextSeqNo, PrimaryTerm: opensearchPrimaryTerm}
	if existing != nil {
		rec.Version = existing.Version + 1
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("opensearch marshal document %s/%s: %w", idx.Name, id, err)
	}
	if err := p.state.Put(goCtx, opensearchNamespace, scope.docKey(idx.Name, id), data); err != nil {
		return nil, fmt.Errorf("opensearch put document %s/%s: %w", idx.Name, id, err)
	}
	idx.NextSeqNo++
	return rec, nil
}

// removeDoc deletes document id of idx, given its current record (nil when it does not exist), and
// reports the version and sequence number the delete answers. Like [OpenSearchPlugin.writeDoc] it
// advances idx's counter and leaves persisting the index record to the caller.
//
// A delete of an absent document still consumes a sequence number and reports `_version: 1`, which
// is what the Bulk API reference's example shows for its `not_found` delete item.
func (p *OpenSearchPlugin) removeDoc(goCtx context.Context, scope openSearchScope, idx *openSearchIndexRecord, id string, existing *openSearchDocRecord) (version, seqNo int64, err error) {
	version = 1
	if existing != nil {
		version = existing.Version + 1
		if err := p.state.Delete(goCtx, opensearchNamespace, scope.docKey(idx.Name, id)); err != nil {
			return 0, 0, fmt.Errorf("opensearch delete document %s/%s: %w", idx.Name, id, err)
		}
	}
	seqNo = idx.NextSeqNo
	idx.NextSeqNo++
	return version, seqNo, nil
}

// --- Index operations ---

func (p *OpenSearchPlugin) createIndex(ctx *RequestContext, req *AWSRequest, scope openSearchScope, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	existing, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return openSearchError(http.StatusBadRequest, "resource_already_exists_exception",
			"index ["+index+"] already exists").response()
	}
	rec := p.newIndexRecord(ctx, index)
	if len(req.Body) > 0 {
		var body struct {
			Settings map[string]interface{} `json:"settings"`
			Mappings map[string]interface{} `json:"mappings"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil {
			// The refusal travels in the response: this is the domain's own REST API, whose errors
			// are JSON bodies rather than AWSErrors.
			return openSearchInvalidBody().response()
		}
		settings, bad := openSearchFlattenSettings(body.Settings)
		if bad != nil {
			return bad.response()
		}
		rec.Settings = settings
		rec.Mappings = body.Mappings
	}
	if err := p.putIndex(goCtx, scope, rec); err != nil {
		return nil, err
	}
	return openSearchOK(map[string]interface{}{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               index,
	})
}

func (p *OpenSearchPlugin) deleteIndex(scope openSearchScope, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	rec, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return openSearchIndexNotFound(index).response()
	}
	keys, err := p.state.List(goCtx, opensearchNamespace, scope.docPrefix(index))
	if err != nil {
		return nil, fmt.Errorf("opensearch deleteIndex list documents: %w", err)
	}
	for _, k := range keys {
		if err := p.state.Delete(goCtx, opensearchNamespace, k); err != nil {
			return nil, fmt.Errorf("opensearch deleteIndex document: %w", err)
		}
	}
	if err := p.state.Delete(goCtx, opensearchNamespace, scope.indexKey(index)); err != nil {
		return nil, fmt.Errorf("opensearch deleteIndex: %w", err)
	}
	return openSearchOK(map[string]interface{}{"acknowledged": true})
}

// getIndex answers GET /{index}: the index's aliases, mappings and settings, keyed by its name.
//
// The settings are the ones the create stored, nested under `index` with every value a string, plus
// `creation_date`, `uuid` and `provided_name`, which is the shape the get-index reference's example
// prints. A create that supplied no shard or replica count reports one primary shard and no replicas:
// substrate models a single node, on which a replica could never be allocated, and the cluster health
// it reports is green.
func (p *OpenSearchPlugin) getIndex(scope openSearchScope, index string) (*AWSResponse, error) {
	rec, err := p.loadIndex(context.Background(), scope, index)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return openSearchIndexNotFound(index).response()
	}
	return openSearchOK(map[string]interface{}{
		index: map[string]interface{}{
			"aliases":  map[string]interface{}{},
			"mappings": orEmpty(rec.Mappings),
			"settings": openSearchNestSettings(rec),
		},
	})
}

// headIndex answers HEAD /{index}: 200 or 404 and no body, which is all the index-exists reference
// publishes.
func (p *OpenSearchPlugin) headIndex(scope openSearchScope, index string) (*AWSResponse, error) {
	rec, err := p.loadIndex(context.Background(), scope, index)
	if err != nil {
		return nil, err
	}
	status := http.StatusOK
	if rec == nil {
		status = http.StatusNotFound
	}
	return &AWSResponse{StatusCode: status, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// getMapping answers GET /{index}/_mapping as `{"{index}": {"mappings": {…}}}`, the get-mapping
// reference's shape.
func (p *OpenSearchPlugin) getMapping(scope openSearchScope, index string) (*AWSResponse, error) {
	rec, err := p.loadIndex(context.Background(), scope, index)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return openSearchIndexNotFound(index).response()
	}
	return openSearchOK(map[string]interface{}{
		index: map[string]interface{}{"mappings": orEmpty(rec.Mappings)},
	})
}

// putMapping answers PUT|POST /{index}/_mapping. The body's members are merged into the stored
// mapping — a field the body names replaces or extends the stored one, and a field it does not name
// is kept — which is the additive behavior the put-mapping reference's follow-up reads show. The
// reference also states that an existing field's type cannot be changed once it holds data; substrate
// does not refuse that, since it indexes nothing by type.
func (p *OpenSearchPlugin) putMapping(req *AWSRequest, scope openSearchScope, index string) (*AWSResponse, error) {
	goCtx := context.Background()
	rec, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return openSearchIndexNotFound(index).response()
	}
	var body map[string]interface{}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return openSearchInvalidBody().response()
		}
	}
	if rec.Mappings == nil {
		rec.Mappings = map[string]interface{}{}
	}
	osDeepMerge(rec.Mappings, body)
	if err := p.putIndex(goCtx, scope, rec); err != nil {
		return nil, err
	}
	return openSearchOK(map[string]interface{}{"acknowledged": true})
}

// --- Document operations ---

// indexDocument answers a single-document write: PUT|POST /{index}/_doc/{id}, POST /{index}/_doc,
// and the create-only forms PUT|POST /{index}/_create/{id} and `?op_type=create`.
//
// A write that creates the document answers 201 with `result: "created"`; one that replaces an
// existing document answers 200 with `result: "updated"` and the incremented `_version`. The index
// document reference publishes both result values and the version increment, and the Bulk API
// reference's example item statuses — 201 for `created`, 200 for `updated` — are the per-write
// statuses. Every write reports `_seq_no` and `_primary_term`.
func (p *OpenSearchPlugin) indexDocument(ctx *RequestContext, req *AWSRequest, scope openSearchScope, index, docID string, createOnly bool) (*AWSResponse, error) {
	goCtx := context.Background()
	if bad := openSearchValidateSource(req.Body); bad != nil {
		return bad.response()
	}
	if docID == "" {
		docID = generateOpenSearchID(ctx.IDs)
	}
	idx, err := p.ensureIndex(goCtx, ctx, scope, index)
	if err != nil {
		return nil, err
	}
	existing, err := p.loadDoc(goCtx, scope, index, docID)
	if err != nil {
		return nil, err
	}
	if createOnly && existing != nil {
		return openSearchVersionConflict(docID, existing.Version).response()
	}
	rec, err := p.writeDoc(goCtx, scope, idx, docID, json.RawMessage(req.Body), existing)
	if err != nil {
		return nil, err
	}
	if err := p.putIndex(goCtx, scope, idx); err != nil {
		return nil, err
	}
	status, result := http.StatusOK, "updated"
	if existing == nil {
		status, result = http.StatusCreated, "created"
	}
	return openSearchStatusOK(status, openSearchWriteResult(index, docID, rec.Version, rec.SeqNo, result))
}

// getDocument answers GET /{index}/_doc/{id}.
//
// An absent document is not an error: the get-document reference publishes `found: false` in a
// normal body at 404, and a client switches on `found`. A missing index is an error, the
// index_not_found_exception every other read of a missing index answers. `_source`,
// `_source_includes` and `_source_excludes` filter the source as they do on a search.
func (p *OpenSearchPlugin) getDocument(req *AWSRequest, scope openSearchScope, index, docID string) (*AWSResponse, error) {
	goCtx := context.Background()
	idx, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return openSearchIndexNotFound(index).response()
	}
	filter, bad := parseOSSourceFilter(nil, req.Params)
	if bad != nil {
		return bad.response()
	}
	rec, err := p.loadDoc(goCtx, scope, index, docID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return openSearchStatusOK(http.StatusNotFound, map[string]interface{}{
			"_index": index,
			"_id":    docID,
			"found":  false,
		})
	}
	body := map[string]interface{}{
		"_index":        index,
		"_id":           docID,
		"_version":      rec.Version,
		"_seq_no":       rec.SeqNo,
		"_primary_term": rec.PrimaryTerm,
		"found":         true,
	}
	if !filter.Disabled {
		decoded, err := osDecodeSource(rec.Source)
		if err != nil {
			return nil, fmt.Errorf("opensearch getDocument %s/%s: %w", index, docID, err)
		}
		body["_source"] = filter.apply(decoded)
	}
	return openSearchOK(body)
}

// headDocument answers HEAD /{index}/_doc/{id}: 200 when the document exists and 404 when it does
// not, which is what the get-document reference states for HEAD.
func (p *OpenSearchPlugin) headDocument(scope openSearchScope, index, docID string) (*AWSResponse, error) {
	rec, err := p.loadDoc(context.Background(), scope, index, docID)
	if err != nil {
		return nil, err
	}
	status := http.StatusOK
	if rec == nil {
		status = http.StatusNotFound
	}
	return &AWSResponse{StatusCode: status, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// deleteDocument answers DELETE /{index}/_doc/{id}.
//
// The delete-document reference publishes `result` as "deleted if the document was successfully
// deleted or not_found if the document did not exist", and a missing index as
// index_not_found_exception. An absent document answers 404 with `result: "not_found"`, the status
// the Bulk API reference's example gives its `not_found` delete item.
func (p *OpenSearchPlugin) deleteDocument(scope openSearchScope, index, docID string) (*AWSResponse, error) {
	goCtx := context.Background()
	idx, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return openSearchIndexNotFound(index).response()
	}
	existing, err := p.loadDoc(goCtx, scope, index, docID)
	if err != nil {
		return nil, err
	}
	version, seqNo, err := p.removeDoc(goCtx, scope, idx, docID, existing)
	if err != nil {
		return nil, err
	}
	if err := p.putIndex(goCtx, scope, idx); err != nil {
		return nil, err
	}
	status, result := http.StatusOK, "deleted"
	if existing == nil {
		status, result = http.StatusNotFound, "not_found"
	}
	return openSearchStatusOK(status, openSearchWriteResult(index, docID, version, seqNo, result))
}

// openSearchWriteResult is the body of a document write or delete: the members the index-document
// and delete-document references publish.
func openSearchWriteResult(index, id string, version, seqNo int64, result string) map[string]interface{} {
	return map[string]interface{}{
		"_index":        index,
		"_id":           id,
		"_version":      version,
		"result":        result,
		"_shards":       map[string]int{"total": 1, "successful": 1, "failed": 0},
		"_seq_no":       seqNo,
		"_primary_term": opensearchPrimaryTerm,
	}
}

// openSearchValidateSource refuses a document body that is not a JSON object, or nil when it is one.
//
// Documents are stored with their metadata, so a body that is not JSON cannot be stored verbatim as
// it once was. The type names are the engine's: action_request_validation_exception for a missing
// source and mapper_parsing_exception for one that will not parse. The reasons are substrate's own.
func openSearchValidateSource(body []byte) *openSearchRefusal {
	if len(strings.TrimSpace(string(body))) == 0 {
		return openSearchError(http.StatusBadRequest, "action_request_validation_exception", "the document source is missing")
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil || obj == nil {
		return openSearchError(http.StatusBadRequest, "mapper_parsing_exception", "the document source is not a JSON object")
	}
	return nil
}

// --- Cluster health ---

func (p *OpenSearchPlugin) clusterHealth() (*AWSResponse, error) {
	return openSearchOK(map[string]interface{}{
		"cluster_name":                    "substrate",
		"status":                          "green",
		"timed_out":                       false,
		"number_of_nodes":                 1,
		"number_of_data_nodes":            1,
		"active_primary_shards":           1,
		"active_shards":                   1,
		"relocating_shards":               0,
		"initializing_shards":             0,
		"unassigned_shards":               0,
		"delayed_unassigned_shards":       0,
		"number_of_pending_tasks":         0,
		"active_shards_percent_as_number": 100.0,
	})
}

// --- Settings and mappings ---

// openSearchFlattenSettings flattens a create's `settings` object to dotted keys without the
// `index.` prefix, rendering every scalar as a string. `{"number_of_shards": 2}`,
// `{"index": {"number_of_shards": 2}}` and `{"index.number_of_shards": "2"}` all flatten to
// `number_of_shards: "2"`, which is why a read reports the same thing whichever form the create used.
//
// Because those forms converge, one body can name a setting twice — `{"number_of_shards": 1,
// "index": {"number_of_shards": 2}}`. No published page says which wins, and choosing by map order
// would give a different index on different runs (#1201), so a setting named twice is refused:
// illegal_argument_exception at 400, with substrate's own reason naming the setting. The walk is in
// sorted key order so that the setting the refusal names is the same on every run.
func openSearchFlattenSettings(in map[string]interface{}) (map[string]interface{}, *openSearchRefusal) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]interface{})
	var dup string
	var walk func(prefix string, v interface{})
	walk = func(prefix string, v interface{}) {
		if dup != "" {
			return
		}
		switch t := v.(type) {
		case map[string]interface{}:
			for _, k := range osSortedKeys(t) {
				walk(prefix+k+".", t[k])
			}
		default:
			key := strings.TrimPrefix(strings.TrimSuffix(prefix, "."), "index.")
			if key == "" || key == "index" {
				return
			}
			if _, seen := out[key]; seen {
				dup = key
				return
			}
			out[key] = openSearchSettingValue(t)
		}
	}
	walk("", in)
	if dup != "" {
		return nil, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
			"setting [index."+dup+"] is given more than once")
	}
	return out, nil
}

// openSearchSettingValue renders a setting the way the get-index reference prints it: a string, or a
// list of strings.
func openSearchSettingValue(v interface{}) interface{} {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, e := range t {
			out = append(out, openSearchSettingValue(e))
		}
		return out
	default:
		return fmt.Sprintf("%v", t)
	}
}

// openSearchNestSettings builds the `settings` object a read reports: the stored settings over the
// single-node defaults, plus the three the index carries itself, nested under `index`.
func openSearchNestSettings(rec *openSearchIndexRecord) map[string]interface{} {
	flat := map[string]interface{}{
		"number_of_shards":   "1",
		"number_of_replicas": "0",
	}
	for k, v := range rec.Settings {
		flat[k] = v
	}
	flat["creation_date"] = rec.CreationDate
	flat["uuid"] = rec.UUID
	flat["provided_name"] = rec.Name

	keys := make([]string, 0, len(flat))
	for k := range flat {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	indexObj := make(map[string]interface{})
	for _, k := range keys {
		parts := strings.Split(k, ".")
		node := indexObj
		ok := true
		for _, part := range parts[:len(parts)-1] {
			child, exists := node[part]
			if !exists {
				m := make(map[string]interface{})
				node[part] = m
				node = m
				continue
			}
			m, isMap := child.(map[string]interface{})
			if !isMap {
				ok = false
				break
			}
			node = m
		}
		if ok {
			if _, exists := node[parts[len(parts)-1]].(map[string]interface{}); !exists {
				node[parts[len(parts)-1]] = flat[k]
			}
		}
	}
	return map[string]interface{}{"index": indexObj}
}

// osDeepMerge merges src into dst: an object member merges into an object member of the same name,
// and anything else replaces it.
func osDeepMerge(dst, src map[string]interface{}) {
	for k, v := range src {
		if sv, ok := v.(map[string]interface{}); ok {
			if dv, ok := dst[k].(map[string]interface{}); ok {
				osDeepMerge(dv, sv)
				continue
			}
		}
		dst[k] = v
	}
}

// --- IDs, responses and errors ---

// generateOpenSearchID mints a URL-safe base64 ID from m — sixteen characters from twelve bytes,
// which is byte-for-byte the width and alphabet the crypto/rand form produced.
//
// It serves two kinds of value, and the wider one is the reason deriving it matters. A document `_id`
// substrate generates is returned in the index response and is the path of every later `GET`, `PUT`
// and `DELETE` of that document; a `_scroll_id` is handed straight back to `_search/scroll`. Both are
// values a caller sends back, so a re-minted one turned a recorded read into a `not_found` 404 and a
// recorded scroll continuation into `search_context_missing_exception` against a cursor the recording
// had just opened.
//
// There is no AWS API model here to consult: the document and scroll APIs are OpenSearch's own REST
// interface rather than the `es`/`opensearch` control plane, so the shape substrate publishes is
// OpenSearch's convention rather than an AWS constraint — which is the other reason not to change it
// while deriving it. Real OpenSearch generates a wider id (a 20-character Flake); substrate's
// sixteen characters are narrower and always were, and #856 is not where that changes.
//
// [IDMint.Base64URL] rather than [IDMint.Base64], because a document id travels in a URL path and the
// two encodings differ in exactly the characters that would need escaping there.
func generateOpenSearchID(m *IDMint) string {
	return m.Base64URL(12)
}

func orEmpty(v map[string]interface{}) interface{} {
	if v == nil {
		return map[string]interface{}{}
	}
	return v
}

// openSearchOK returns a 200 JSON response.
func openSearchOK(body interface{}) (*AWSResponse, error) {
	return openSearchStatusOK(http.StatusOK, body)
}

// openSearchStatusOK returns a JSON response with the given status code.
//
// It returns the marshal error rather than an empty body: body is arbitrary, and a search
// response carries stored documents, so it is not a value that provably encodes (#1365).
func openSearchStatusOK(status int, body interface{}) (*AWSResponse, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("opensearch response marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Body:       b,
		Headers:    map[string]string{"Content-Type": "application/json"},
	}, nil
}

// openSearchInvalidBody reports that a request body would not decode.
//
// This is the one plugin in #1007's sweep whose refusal is not an [AWSError]. The
// three sites behind it — search, scroll and clearScroll — are the OpenSearch
// domain's own REST API, not an AWS control-plane operation, so there is no AWS
// API reference publishing a code for them and no `X-Amzn-ErrorType` shape to
// answer in. They answer the engine's envelope, which is why this returns an
// *AWSResponse through [openSearchError] rather than an error.
//
// The type name follows the file's existing convention of using the engine's own
// lowercased exception name — `resource_already_exists_exception` at createIndex,
// `illegal_argument_exception` at scroll — and `json_parse_exception` at 400 is
// what OpenSearch reports for a body it cannot parse. That provenance is the
// engine's behavior rather than a published AWS API model, and is recorded as such
// in docs/reference/opensearch.md.
func openSearchInvalidBody() *openSearchRefusal {
	return openSearchError(http.StatusBadRequest, "json_parse_exception", "request body is not valid JSON")
}

// openSearchIndexNotFound is the refusal every operation on a missing index answers.
func openSearchIndexNotFound(index string) *openSearchRefusal {
	return openSearchError(http.StatusNotFound, "index_not_found_exception", "no such index ["+index+"]")
}

// openSearchVersionConflict is the 409 a create-only write answers for a document that exists. The
// reason is substrate's own.
func openSearchVersionConflict(id string, version int64) *openSearchRefusal {
	return openSearchError(http.StatusConflict, "version_conflict_engine_exception",
		fmt.Sprintf("[%s]: document already exists (current version [%d])", id, version))
}

// openSearchRefusal is a data-plane refusal: an HTTP status and the engine's error type and reason.
// It is a value rather than a response so that the helpers building one need not marshal, and the one
// place that does — [openSearchRefusal.response] — returns the marshal error rather than discarding it.
type openSearchRefusal struct {
	status  int
	errType string
	reason  string
}

// openSearchError builds an OpenSearch-style refusal.
func openSearchError(status int, errType, reason string) *openSearchRefusal {
	return &openSearchRefusal{status: status, errType: errType, reason: reason}
}

// openSearchErrorCause is one entry of an error envelope's `root_cause`.
type openSearchErrorCause struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// openSearchErrorDetail is the envelope's `error` member.
type openSearchErrorDetail struct {
	RootCause []openSearchErrorCause `json:"root_cause"`
	Type      string                 `json:"type"`
	Reason    string                 `json:"reason"`
}

// openSearchErrorBody is the error envelope.
type openSearchErrorBody struct {
	Error  openSearchErrorDetail `json:"error"`
	Status int                   `json:"status"`
}

// response renders the refusal as the engine's error envelope.
//
// The envelope is `{"error": {"root_cause": [{"type", "reason"}], "type", "reason"}, "status"}`, the
// shape OpenSearch's documented error responses print (the ML Commons delete-task reference shows one
// for a missing task). substrate has one cause per error, so `root_cause` holds exactly the error
// itself. A marshal failure is returned, as [openSearchStatusOK] returns one.
func (r *openSearchRefusal) response() (*AWSResponse, error) {
	return openSearchStatusOK(r.status, openSearchErrorBody{
		Error: openSearchErrorDetail{
			RootCause: []openSearchErrorCause{{Type: r.errType, Reason: r.reason}},
			Type:      r.errType,
			Reason:    r.reason,
		},
		Status: r.status,
	})
}
