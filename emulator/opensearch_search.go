package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
)

// openSearchConstantScore is the `_score` of every hit. Substrate does not rank: it evaluates a
// query as a filter, so every matching document is equally relevant, and the order a consumer sees
// is decided by `sort` or, absent one, by insertion order (see osSortDocs).
const openSearchConstantScore = 1.0

// openSearchSearchRequest mirrors the OpenSearch search request body.
type openSearchSearchRequest struct {
	Query        map[string]interface{} `json:"query"`
	Size         *int                   `json:"size"`
	From         *int                   `json:"from"`
	Source       json.RawMessage        `json:"_source"`
	Sort         json.RawMessage        `json:"sort"`
	Aggs         map[string]interface{} `json:"aggs"`
	Aggregations map[string]interface{} `json:"aggregations"`
	Scroll       string                 `json:"scroll"`
	TrackScores  bool                   `json:"track_scores"`
}

// osDoc is a stored document loaded for a search: its ID, its record, and its decoded source.
type osDoc struct {
	id  string
	rec openSearchDocRecord
	src map[string]interface{}
}

// openSearchScrollState is a scroll context: the hits a search matched beyond the page it returned,
// in the order it returned them, and what every later page needs to render them the same way.
type openSearchScrollState struct {
	Index     string                `json:"index"`
	Remaining []openSearchScrollHit `json:"remaining"`
	Size      int                   `json:"size"`
	Total     int                   `json:"total"`
	Scored    bool                  `json:"scored"`
	Sorted    bool                  `json:"sorted"`
	Source    osSourceFilter        `json:"source"`
}

// openSearchScrollHit is one hit a scroll context still owes: the document ID and, for a sorted
// search, the sort values it was ordered by.
type openSearchScrollHit struct {
	ID   string        `json:"id"`
	Sort []interface{} `json:"sort,omitempty"`
}

// search answers {any method} /{index}/_search.
func (p *OpenSearchPlugin) search(ctx *RequestContext, req *AWSRequest, scope openSearchScope, index string) (*AWSResponse, error) {
	goCtx := context.Background()

	var body openSearchSearchRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			// The refusal travels in the response, not the error slot: this is the domain's own
			// REST API, whose errors are JSON bodies rather than AWSErrors. Same shape as the
			// bad-scroll-state return below.
			return openSearchInvalidBody().response()
		}
	}

	keys, bad := parseOSSort(body.Sort, req.Params["sort"])
	if bad != nil {
		return bad.response()
	}
	filter, bad := parseOSSourceFilter(body.Source, req.Params)
	if bad != nil {
		return bad.response()
	}
	size, bad := osIntParam(body.Size, req.Params["size"], 10, "size")
	if bad != nil {
		return bad.response()
	}
	from, bad := osIntParam(body.From, req.Params["from"], 0, "from")
	if bad != nil {
		return bad.response()
	}

	idx, err := p.loadIndex(goCtx, scope, index)
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return openSearchIndexNotFound(index).response()
	}
	scrollTTL := req.Params["scroll"]
	if scrollTTL == "" {
		scrollTTL = body.Scroll
	}
	if bad := osCheckResultWindow(idx, from, size, scrollTTL != ""); bad != nil {
		return bad.response()
	}
	if bad := osValidateQuery(body.Query); bad != nil {
		return bad.response()
	}
	docs, err := p.loadDocs(goCtx, scope, index)
	if err != nil {
		return nil, err
	}

	matched := osFilterDocs(docs, body.Query)
	sortVals := osSortDocs(matched, keys)
	scored := len(keys) == 0 || osSortHasScore(keys) || body.TrackScores
	sorted := len(keys) > 0

	// Bounded by the documents present, never by the requested size alone.
	page := make([]map[string]interface{}, 0, min(size, max(len(matched)-from, 0)))
	for i := from; i < len(matched) && i < from+size; i++ {
		hit, err := osHit(index, matched[i], scored, sorted, sortVals[i], filter)
		if err != nil {
			return nil, err
		}
		page = append(page, hit)
	}

	resp := osSearchResponse(len(matched), page, scored)
	aggs := body.Aggs
	if len(aggs) == 0 {
		aggs = body.Aggregations
	}
	if len(aggs) > 0 {
		resp["aggregations"] = osComputeAggs(matched, aggs)
	}

	// Scroll support: save the hits beyond this page as the scroll context.
	if scrollTTL != "" {
		scrollID := generateOpenSearchID(ctx.IDs)
		state := openSearchScrollState{
			Index: index, Size: size, Total: len(matched),
			Scored: scored, Sorted: sorted, Source: filter,
			Remaining: []openSearchScrollHit{},
		}
		for i := from + size; i < len(matched); i++ {
			state.Remaining = append(state.Remaining, openSearchScrollHit{ID: matched[i].id, Sort: sortVals[i]})
		}
		sd, err := json.Marshal(state)
		if err != nil {
			return nil, fmt.Errorf("opensearch search marshal: %w", err)
		}
		if err := p.state.Put(goCtx, opensearchNamespace, scope.scrollKey(scrollID), sd); err != nil {
			return nil, fmt.Errorf("opensearch search state.Put: %w", err)
		}
		resp["_scroll_id"] = scrollID
	}
	return openSearchOK(resp)
}

// osSearchResponse is the envelope of a search or scroll page. `hits.total` is the number of
// documents the query matched, not the length of the page — the scroll reference's end-of-results
// example reports the full total beside an empty page. `max_score` is null when the search did not
// score (a sort on a field, without `track_scores`), which the search reference states, and when the
// page holds no hit.
func osSearchResponse(total int, page []map[string]interface{}, scored bool) map[string]interface{} {
	var maxScore interface{}
	if scored && len(page) > 0 {
		maxScore = openSearchConstantScore
	}
	return map[string]interface{}{
		"took":      1,
		"timed_out": false,
		"_shards":   map[string]int{"total": 1, "successful": 1, "skipped": 0, "failed": 0},
		"hits": map[string]interface{}{
			"total":     map[string]interface{}{"value": total, "relation": "eq"},
			"max_score": maxScore,
			"hits":      page,
		},
	}
}

// osHit renders one search hit: its real `_index`, its `_id`, its `_score` (null when the search
// did not score), its `sort` values when the search sorted, and its `_source` as filtered.
func osHit(index string, d osDoc, scored, sorted bool, sortVals []interface{}, filter osSourceFilter) (map[string]interface{}, error) {
	hit := map[string]interface{}{"_index": index, "_id": d.id, "_score": nil}
	if scored {
		hit["_score"] = openSearchConstantScore
	}
	if sorted {
		hit["sort"] = sortVals
	}
	if filter.Disabled {
		return hit, nil
	}
	if d.src == nil {
		src, err := osDecodeSource(d.rec.Source)
		if err != nil {
			return nil, fmt.Errorf("opensearch hit %s/%s: %w", index, d.id, err)
		}
		d.src = src
	}
	hit["_source"] = filter.apply(d.src)
	return hit, nil
}

// openSearchDefaultMaxResultWindow is the default of the `index.max_result_window` setting: "Default
// is `10000`" (the OpenSearch pagination reference).
const openSearchDefaultMaxResultWindow = 10000

// osCheckResultWindow refuses a search whose `from + size` exceeds the index's
// `index.max_result_window`, which the pagination reference states OpenSearch "rejects". The page
// publishes no error body; the type and both reasons are the engine's own, from
// DefaultSearchContext.preProcess in the OpenSearch source, which raises an IllegalArgumentException
// (`illegal_argument_exception`, 400) worded one way for a search and another for a scroll's batch.
//
// The check is also what bounds the work a search does: from and size are each at most the window,
// so their sum cannot overflow and the page cannot be larger than it.
func osCheckResultWindow(idx *openSearchIndexRecord, from, size int, scroll bool) *openSearchRefusal {
	limit := int64(openSearchDefaultMaxResultWindow)
	if v, ok := idx.Settings["max_result_window"].(string); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			limit = n
		}
	}
	// Saturating, so two huge values report the largest window rather than an overflowed one.
	window := int64(math.MaxInt64)
	if int64(from) <= math.MaxInt64-int64(size) {
		window = int64(from) + int64(size)
	}
	if window <= limit {
		return nil
	}
	if scroll {
		return openSearchError(http.StatusBadRequest, "illegal_argument_exception", fmt.Sprintf(
			"Batch size is too large, size must be less than or equal to: [%d] but was [%d]. Scroll batch sizes "+
				"cost as much memory as result windows so they are controlled by the [index.max_result_window] "+
				"index level setting.", limit, window))
	}
	return openSearchError(http.StatusBadRequest, "illegal_argument_exception", fmt.Sprintf(
		"Result window is too large, from + size must be less than or equal to: [%d] but was [%d]. See the "+
			"scroll api for a more efficient way to request large data sets. This limit can be set by changing "+
			"the [index.max_result_window] index level setting.", limit, window))
}

// osIntParam reads a non-negative integer from the body member or, absent it, the query parameter,
// answering def when neither is present.
func osIntParam(body *int, param string, def int, name string) (int, *openSearchRefusal) {
	v := def
	switch {
	case body != nil:
		v = *body
	case param != "":
		n, err := strconv.Atoi(param)
		if err != nil {
			return 0, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
				fmt.Sprintf("[%s] must be an integer, got [%s]", name, param))
		}
		v = n
	}
	if v < 0 {
		return 0, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
			fmt.Sprintf("[%s] must be non-negative, got [%d]", name, v))
	}
	return v, nil
}

// loadDocs returns every document of index, in key order.
func (p *OpenSearchPlugin) loadDocs(goCtx context.Context, scope openSearchScope, index string) ([]osDoc, error) {
	prefix := scope.docPrefix(index)
	keys, err := p.state.List(goCtx, opensearchNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("opensearch list documents of %s: %w", index, err)
	}
	docs := make([]osDoc, 0, len(keys))
	for _, k := range keys {
		id := strings.TrimPrefix(k, prefix)
		rec, err := p.loadDoc(goCtx, scope, index, id)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			continue
		}
		var src map[string]interface{}
		if err := json.Unmarshal(rec.Source, &src); err != nil {
			return nil, fmt.Errorf("opensearch decode source %s/%s: %w", index, id, err)
		}
		docs = append(docs, osDoc{id: id, rec: *rec, src: src})
	}
	return docs, nil
}

// --- Scroll ---

func (p *OpenSearchPlugin) scroll(req *AWSRequest, scope openSearchScope) (*AWSResponse, error) {
	goCtx := context.Background()

	var scrollID string
	var body struct {
		ScrollID string `json:"scroll_id"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			// The refusal travels in the response, not the error slot: this is the domain's own
			// REST API, whose errors are JSON bodies rather than AWSErrors. Same shape as the
			// bad-scroll-state return below.
			return openSearchInvalidBody().response()
		}
		scrollID = body.ScrollID
	}
	if scrollID == "" {
		scrollID = req.Params["scroll_id"]
	}
	if scrollID == "" {
		return openSearchError(http.StatusBadRequest, "illegal_argument_exception", "scroll_id required").response()
	}

	stateKey := scope.scrollKey(scrollID)
	data, err := p.state.Get(goCtx, opensearchNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("opensearch scroll state.Get: %w", err)
	}
	if data == nil {
		return openSearchError(http.StatusNotFound, "search_context_missing_exception", "scroll context expired").response()
	}

	var state openSearchScrollState
	if err := json.Unmarshal(data, &state); err != nil {
		return openSearchError(http.StatusInternalServerError, "internal_error", "bad scroll state").response()
	}

	size := state.Size
	if size <= 0 {
		size = 10
	}
	pageIDs := state.Remaining
	if len(pageIDs) > size {
		pageIDs = state.Remaining[:size]
	}
	state.Remaining = state.Remaining[len(pageIDs):]

	hits := make([]map[string]interface{}, 0, len(pageIDs))
	for _, h := range pageIDs {
		rec, err := p.loadDoc(goCtx, scope, state.Index, h.ID)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			continue
		}
		hit, err := osHit(state.Index, osDoc{id: h.ID, rec: *rec}, state.Scored, state.Sorted, h.Sort, state.Source)
		if err != nil {
			return nil, err
		}
		hits = append(hits, hit)
	}

	updated, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("opensearch scroll marshal: %w", err)
	}
	if err := p.state.Put(goCtx, opensearchNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("opensearch scroll state.Put: %w", err)
	}

	resp := osSearchResponse(state.Total, hits, state.Scored)
	resp["_scroll_id"] = scrollID
	return openSearchOK(resp)
}

// clearScroll answers DELETE /_search/scroll. `num_freed` counts the contexts that existed and were
// freed, which is what the scroll reference states it counts.
func (p *OpenSearchPlugin) clearScroll(req *AWSRequest, scope openSearchScope) (*AWSResponse, error) {
	goCtx := context.Background()
	var body struct {
		ScrollID interface{} `json:"scroll_id"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			// The refusal travels in the response, not the error slot: this is the domain's own
			// REST API, whose errors are JSON bodies rather than AWSErrors. Same shape as the
			// bad-scroll-state return below.
			return openSearchInvalidBody().response()
		}
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
	freed := 0
	for _, id := range ids {
		key := scope.scrollKey(id)
		data, err := p.state.Get(goCtx, opensearchNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("opensearch clearScroll state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		if err := p.state.Delete(goCtx, opensearchNamespace, key); err != nil {
			return nil, fmt.Errorf("opensearch clearScroll state.Delete: %w", err)
		}
		freed++
	}
	return openSearchOK(map[string]interface{}{
		"succeeded": true,
		"num_freed": freed,
	})
}

// --- Query matching ---

// osFilterDocs applies a query to the document list and returns matching docs.
func osFilterDocs(docs []osDoc, query map[string]interface{}) []osDoc {
	if len(query) == 0 {
		return docs
	}
	result := make([]osDoc, 0, len(docs))
	for _, doc := range docs {
		if osMatchDoc(doc.src, query) {
			result = append(result, doc)
		}
	}
	return result
}

// osSortedKeys returns m's keys in sorted order.
func osSortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// osMatchDoc reports whether a document source matches a query object.
//
// Every top-level clause must match, and they are evaluated in sorted key order, so the answer for
// an object carrying several clauses — `{"match_all": {}, "term": {…}}` — is the conjunction of all of
// them and the same on every run (#1201). The Query DSL documents `bool` as the way to combine clauses
// and publishes no error for a multi-clause object, so substrate does not refuse one. A clause type
// substrate does not implement matches every document; the supported set is listed in
// docs/reference/opensearch.md.
func osMatchDoc(src map[string]interface{}, query map[string]interface{}) bool {
	for _, qType := range osSortedKeys(query) {
		if !osMatchClause(src, qType, query[qType]) {
			return false
		}
	}
	return true
}

// osMatchClause evaluates one query clause against a document source.
func osMatchClause(src map[string]interface{}, qType string, qVal interface{}) bool {
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
		for _, field := range osSortedKeys(terms) {
			val := terms[field]
			if fieldVal, ok := val.(map[string]interface{}); ok {
				val = fieldVal["value"]
			}
			if !osValueMatches(osGetField(src, field), func(v interface{}) bool { return osScalarEquals(v, val) }) {
				return false
			}
		}
	case "terms":
		terms, ok := qVal.(map[string]interface{})
		if !ok {
			return false
		}
		for _, field := range osSortedKeys(terms) {
			list, ok := terms[field].([]interface{})
			if !ok {
				return false
			}
			matched := osValueMatches(osGetField(src, field), func(v interface{}) bool {
				for _, want := range list {
					if osScalarEquals(v, want) {
						return true
					}
				}
				return false
			})
			if !matched {
				return false
			}
		}
	case "bool":
		boolQ, ok := qVal.(map[string]interface{})
		if !ok {
			return false
		}
		return osMatchBool(src, boolQ)
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
		for _, field := range osSortedKeys(rangeQ) {
			rv, ok := rangeQ[field].(map[string]interface{})
			if !ok {
				return false
			}
			if !osValueMatches(osGetField(src, field), func(v interface{}) bool { return osRangeMatch(v, rv) }) {
				return false
			}
		}
	case "match", "match_phrase":
		matchQ, ok := qVal.(map[string]interface{})
		if !ok {
			return false
		}
		for _, field := range osSortedKeys(matchQ) {
			if !osMatchField(src, field, matchQ[field]) {
				return false
			}
		}
	}
	return true
}

// osMatchBool evaluates a `bool` query. `must` and `filter` clauses must all match and `must_not`
// clauses must all fail. `should` clauses are optional when the query has a `must` or `filter`
// clause and at least one is required otherwise, which is the Query DSL's default for
// `minimum_should_match`; an explicit integer `minimum_should_match` overrides it.
func osMatchBool(src map[string]interface{}, boolQ map[string]interface{}) bool {
	required := 0
	for _, occ := range []string{"must", "filter"} {
		for _, clause := range osToClauseList(boolQ[occ]) {
			required++
			if !osMatchDoc(src, clause) {
				return false
			}
		}
	}
	for _, clause := range osToClauseList(boolQ["must_not"]) {
		if osMatchDoc(src, clause) {
			return false
		}
	}
	should := osToClauseList(boolQ["should"])
	minShould := 1
	if required > 0 {
		minShould = 0
	}
	if spec, ok := boolQ["minimum_should_match"]; ok {
		// osValidateQuery has already refused an unparseable value, so an error cannot reach here.
		if n, err := osMinimumShouldMatch(spec, len(should)); err == nil {
			minShould = n
		}
	}
	if len(should) == 0 {
		return true
	}
	matched := 0
	for _, clause := range should {
		if osMatchDoc(src, clause) {
			matched++
		}
	}
	return matched >= minShould
}

// osMatchField evaluates a `match` or `match_phrase` clause on one field. The query is a string, or
// an object carrying `query` and optionally `zero_terms_query`.
//
// Substrate does not analyze text, so a match is a case-insensitive substring test. Two published
// behaviors still apply. A query that yields no terms — here, one that is empty or only whitespace —
// matches no document, since `zero_terms_query` "Default is `none`" and `none` "returns no documents"
// (the match query reference); `"zero_terms_query": "all"` matches every document. And a field the
// document does not have matches nothing: there is no value to hold a term.
func osMatchField(src map[string]interface{}, field string, val interface{}) bool {
	zeroTerms := "none"
	if q, ok := val.(map[string]interface{}); ok {
		val = q["query"]
		if z, ok := q["zero_terms_query"].(string); ok {
			zeroTerms = strings.ToLower(z)
		}
	}
	queryStr := ""
	if val != nil {
		queryStr = strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", val)))
	}
	if queryStr == "" {
		return zeroTerms == "all"
	}
	docVal := osGetField(src, field)
	if docVal == nil {
		return false
	}
	return osValueMatches(docVal, func(v interface{}) bool {
		return v != nil && strings.Contains(strings.ToLower(fmt.Sprintf("%v", v)), queryStr)
	})
}

// osMinimumShouldMatch resolves a `minimum_should_match` value against the number of optional
// clauses, per the minimum_should_match reference:
//
//   - a non-negative integer is that many clauses, and a negative one is the total less that many;
//   - a percentage is that share of the total, rounded down, and a negative percentage is the share
//     that may be missed, rounded down, with the rest required;
//   - `n<v` applies v when there are more than n clauses and requires all of them otherwise, and
//     several space-separated `n<v` conditions apply the one with the largest n the total exceeds.
//
// A count from a percentage below 1 becomes 1 and one above the total becomes the total, as the
// reference states; an integer is held to the same 0..total range. Any other form is an error.
func osMinimumShouldMatch(spec interface{}, total int) (int, error) {
	var s string
	switch t := spec.(type) {
	case float64:
		if t != float64(int64(t)) {
			return 0, fmt.Errorf("[minimum_should_match] must be an integer or a string, got [%v]", t)
		}
		s = strconv.FormatInt(int64(t), 10)
	case string:
		s = strings.TrimSpace(t)
	default:
		return 0, fmt.Errorf("[minimum_should_match] must be an integer or a string")
	}
	if !strings.Contains(s, "<") {
		return osMinimumShouldMatchValue(s, total)
	}
	// Every condition must parse, whichever one applies.
	best, chosen := -1, ""
	for _, cond := range strings.Fields(s) {
		left, right, ok := strings.Cut(cond, "<")
		n, err := strconv.Atoi(left)
		if !ok || err != nil || right == "" {
			return 0, fmt.Errorf("[minimum_should_match] condition [%s] is not of the form n<value", cond)
		}
		if _, err := osMinimumShouldMatchValue(right, total); err != nil {
			return 0, err
		}
		if total > n && n > best {
			best, chosen = n, right
		}
	}
	if chosen == "" {
		return total, nil
	}
	return osMinimumShouldMatchValue(chosen, total)
}

// osMinimumShouldMatchValue resolves one integer or percentage value.
func osMinimumShouldMatchValue(s string, total int) (int, error) {
	if pct, ok := strings.CutSuffix(s, "%"); ok {
		p, err := strconv.Atoi(pct)
		if err != nil {
			return 0, fmt.Errorf("[minimum_should_match] percentage [%s] is not an integer percentage", s)
		}
		var n int
		if p >= 0 {
			n = total * p / 100
		} else {
			n = total - total*(-p)/100
		}
		return min(max(n, 1), total), nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("[minimum_should_match] value [%s] is not an integer or a percentage", s)
	}
	if v < 0 {
		v += total
	}
	return min(max(v, 0), total), nil
}

// osValidateQuery refuses a query whose `bool` clauses carry a `minimum_should_match` no published
// form describes, so a value substrate cannot read is reported rather than ignored. The type is the
// engine's illegal_argument_exception at 400; the reason is substrate's own.
func osValidateQuery(query map[string]interface{}) *openSearchRefusal {
	for _, qType := range osSortedKeys(query) {
		boolQ, ok := query[qType].(map[string]interface{})
		if qType != "bool" || !ok {
			continue
		}
		if spec, ok := boolQ["minimum_should_match"]; ok {
			if _, err := osMinimumShouldMatch(spec, len(osToClauseList(boolQ["should"]))); err != nil {
				return openSearchError(http.StatusBadRequest, "illegal_argument_exception", err.Error())
			}
		}
		for _, occ := range []string{"must", "filter", "should", "must_not"} {
			for _, clause := range osToClauseList(boolQ[occ]) {
				if bad := osValidateQuery(clause); bad != nil {
					return bad
				}
			}
		}
	}
	return nil
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

// osValueMatches applies pred to a field value, or to each element when the field holds an array —
// a term matches an array field when any element equals it, which is how the engine indexes one.
func osValueMatches(docVal interface{}, pred func(interface{}) bool) bool {
	if arr, ok := docVal.([]interface{}); ok {
		for _, v := range arr {
			if pred(v) {
				return true
			}
		}
		return false
	}
	return pred(docVal)
}

// osScalarEquals compares a document value to a query value by their printed form, so `1` and `1.0`
// and `"1"` agree as they do against a keyword or numeric field.
func osScalarEquals(docVal, want interface{}) bool {
	if docVal == nil {
		return false
	}
	return fmt.Sprintf("%v", docVal) == fmt.Sprintf("%v", want)
}

// osGetField retrieves a (possibly dot-separated) field from a document source.
//
// A `.keyword` suffix names the keyword sub-field dynamic mapping gives a string field, which holds
// the same value; when the source has no member of that name, the field without the suffix is read.
func osGetField(src map[string]interface{}, field string) interface{} {
	if v := osGetPath(src, field); v != nil {
		return v
	}
	if base, ok := strings.CutSuffix(field, ".keyword"); ok {
		return osGetPath(src, base)
	}
	return nil
}

func osGetPath(src map[string]interface{}, field string) interface{} {
	if src == nil {
		return nil
	}
	if v, ok := src[field]; ok {
		return v
	}
	head, tail, ok := strings.Cut(field, ".")
	if !ok {
		return nil
	}
	nested, ok := src[head].(map[string]interface{})
	if !ok {
		return nil
	}
	return osGetPath(nested, tail)
}

func osRangeMatch(docVal interface{}, rv map[string]interface{}) bool {
	if docVal == nil {
		return false
	}
	dv, err := strconv.ParseFloat(fmt.Sprintf("%v", docVal), 64)
	if err != nil {
		return false
	}
	for _, bound := range []struct {
		key string
		ok  func(dv, v float64) bool
	}{
		{"gte", func(dv, v float64) bool { return dv >= v }},
		{"gt", func(dv, v float64) bool { return dv > v }},
		{"lte", func(dv, v float64) bool { return dv <= v }},
		{"lt", func(dv, v float64) bool { return dv < v }},
	} {
		raw, present := rv[bound.key]
		if !present {
			continue
		}
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", raw), 64)
		if err != nil || !bound.ok(dv, v) {
			return false
		}
	}
	return true
}

// --- Sort ---

// osSortKey is one entry of a search's `sort`.
type osSortKey struct {
	field        string
	desc         bool
	missingFirst bool
}

// parseOSSort reads the body `sort` member, or the `sort` query parameter when the body has none.
//
// The body takes a field name, an object `{"field": "desc"}` or `{"field": {"order": "desc",
// "missing": "_first"}}`, or an array of those, which is the form the search reference publishes;
// the query parameter is a comma-separated list of `field:direction` pairs. A field name alone sorts
// ascending, except `_score`, which sorts descending. `_doc` is index (insertion) order and `_id` the
// document ID.
func parseOSSort(raw json.RawMessage, param string) ([]osSortKey, *openSearchRefusal) {
	var keys []osSortKey
	if len(raw) > 0 && string(raw) != "null" {
		var v interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, openSearchInvalidBody()
		}
		items, ok := v.([]interface{})
		if !ok {
			items = []interface{}{v}
		}
		for _, item := range items {
			k, bad := osSortKeyFrom(item)
			if bad != nil {
				return nil, bad
			}
			keys = append(keys, k...)
		}
		return keys, nil
	}
	if param == "" {
		return nil, nil
	}
	for _, part := range strings.Split(param, ",") {
		field, dir, _ := strings.Cut(strings.TrimSpace(part), ":")
		if field == "" {
			continue
		}
		k, bad := osSortKeyNamed(field, dir, "")
		if bad != nil {
			return nil, bad
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func osSortKeyFrom(item interface{}) ([]osSortKey, *openSearchRefusal) {
	switch t := item.(type) {
	case string:
		k, bad := osSortKeyNamed(t, "", "")
		return []osSortKey{k}, bad
	case map[string]interface{}:
		var out []osSortKey
		for _, field := range osSortedKeys(t) {
			var order, missing string
			switch spec := t[field].(type) {
			case string:
				order = spec
			case map[string]interface{}:
				order, _ = spec["order"].(string)
				missing, _ = spec["missing"].(string)
			}
			k, bad := osSortKeyNamed(field, order, missing)
			if bad != nil {
				return nil, bad
			}
			out = append(out, k)
		}
		return out, nil
	}
	return nil, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
		"a sort entry must be a field name or an object")
}

func osSortKeyNamed(field, order, missing string) (osSortKey, *openSearchRefusal) {
	k := osSortKey{field: field, desc: field == "_score", missingFirst: missing == "_first"}
	switch strings.ToLower(order) {
	case "":
	case "asc":
		k.desc = false
	case "desc":
		k.desc = true
	default:
		return k, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
			fmt.Sprintf("sort order [%s] for [%s] must be asc or desc", order, field))
	}
	return k, nil
}

func osSortHasScore(keys []osSortKey) bool {
	for _, k := range keys {
		if k.field == "_score" {
			return true
		}
	}
	return false
}

// osSortDocs orders docs in place and returns each document's sort values, aligned with the result.
//
// With no `sort`, every hit has the same constant score, so the order is insertion order: ascending
// `_seq_no` of each document's last write, which is the engine's own tie-break for equal scores on a
// single shard and — unlike the document ID — does not depend on an identifier minted from the
// wall-clock request ID (#1201). With a `sort`, documents are ordered by each key in turn; a missing
// value sorts last unless the key asks for `_first`; an array field sorts by its minimum ascending
// and its maximum descending; and insertion order breaks any remaining tie.
func osSortDocs(docs []osDoc, keys []osSortKey) [][]interface{} {
	vals := make([][]interface{}, len(docs))
	for i, d := range docs {
		row := make([]interface{}, len(keys))
		for j, k := range keys {
			row[j] = osSortValue(d, k)
		}
		vals[i] = row
	}
	idx := make([]int, len(docs))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		da, db := docs[idx[a]], docs[idx[b]]
		for j, k := range keys {
			va, vb := vals[idx[a]][j], vals[idx[b]][j]
			if va == nil || vb == nil {
				if (va == nil) == (vb == nil) {
					continue
				}
				return (va == nil) == k.missingFirst
			}
			c := osCompareValues(va, vb)
			if c == 0 {
				continue
			}
			if k.desc {
				return c > 0
			}
			return c < 0
		}
		if da.rec.SeqNo != db.rec.SeqNo {
			return da.rec.SeqNo < db.rec.SeqNo
		}
		return da.id < db.id
	})
	sortedDocs := make([]osDoc, len(docs))
	sortedVals := make([][]interface{}, len(docs))
	for i, j := range idx {
		sortedDocs[i], sortedVals[i] = docs[j], vals[j]
	}
	copy(docs, sortedDocs)
	return sortedVals
}

// osSortValue is the value a document sorts by under k, or nil when it has none.
func osSortValue(d osDoc, k osSortKey) interface{} {
	switch k.field {
	case "_score":
		return openSearchConstantScore
	case "_doc":
		return float64(d.rec.SeqNo)
	case "_id":
		return d.id
	}
	v := osGetField(d.src, k.field)
	arr, ok := v.([]interface{})
	if !ok {
		return osSortable(v)
	}
	var best interface{}
	for _, e := range arr {
		e = osSortable(e)
		if e == nil {
			continue
		}
		if best == nil {
			best = e
			continue
		}
		c := osCompareValues(e, best)
		if (k.desc && c > 0) || (!k.desc && c < 0) {
			best = e
		}
	}
	return best
}

// osSortable reduces a value to one that orders: a number, a string or a boolean. An object or a
// null has no sort value.
func osSortable(v interface{}) interface{} {
	switch v.(type) {
	case float64, string, bool:
		return v
	}
	return nil
}

// osCompareValues orders two sortable values: numbers numerically, strings lexicographically,
// booleans false before true, and across types numbers before strings before booleans.
func osCompareValues(a, b interface{}) int {
	rank := func(v interface{}) int {
		switch v.(type) {
		case float64:
			return 0
		case string:
			return 1
		}
		return 2
	}
	if ra, rb := rank(a), rank(b); ra != rb {
		return ra - rb
	}
	switch av := a.(type) {
	case float64:
		bv := b.(float64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		}
		return 0
	case string:
		return strings.Compare(av, b.(string))
	case bool:
		bv := b.(bool)
		switch {
		case av == bv:
			return 0
		case !av:
			return -1
		}
		return 1
	}
	return 0
}

// --- Source filtering ---

// osSourceFilter is a search's or a get's `_source` request: whether the source is returned at all,
// and which fields it keeps.
type osSourceFilter struct {
	Disabled bool     `json:"disabled,omitempty"`
	Includes []string `json:"includes,omitempty"`
	Excludes []string `json:"excludes,omitempty"`
}

// parseOSSourceFilter reads the body `_source` member, or the `_source`, `_source_includes` and
// `_source_excludes` query parameters when the body has none. The body takes `true`, `false`, a field
// name or pattern, an array of them, or an object with `includes` and `excludes`, which are the forms
// the search reference publishes; a pattern may carry `*` wildcards.
func parseOSSourceFilter(raw json.RawMessage, params map[string]string) (osSourceFilter, *openSearchRefusal) {
	var f osSourceFilter
	if len(raw) > 0 && string(raw) != "null" {
		var v interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return f, openSearchInvalidBody()
		}
		switch t := v.(type) {
		case bool:
			f.Disabled = !t
		case string, []interface{}:
			f.Includes = osStringList(t)
		case map[string]interface{}:
			f.Includes = append(osStringList(t["includes"]), osStringList(t["include"])...)
			f.Excludes = append(osStringList(t["excludes"]), osStringList(t["exclude"])...)
		default:
			return f, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
				"_source must be a boolean, a field pattern, a list of patterns or an object")
		}
		return f, nil
	}
	switch s := params["_source"]; s {
	case "":
	case "false":
		return osSourceFilter{Disabled: true}, nil
	case "true":
	default:
		f.Includes = osSplitList(s)
	}
	f.Includes = append(f.Includes, osSplitList(params["_source_includes"])...)
	f.Excludes = append(f.Excludes, osSplitList(params["_source_excludes"])...)
	return f, nil
}

func osStringList(v interface{}) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func osSplitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// apply returns the filtered source. src is the already-decoded source, which is never modified. A
// caller checks Disabled first, since a disabled filter returns no `_source` member at all.
func (f osSourceFilter) apply(src map[string]interface{}) map[string]interface{} {
	if len(f.Includes) == 0 && len(f.Excludes) == 0 {
		return src
	}
	return osFilterSource(src, "", f.Includes, f.Excludes)
}

// osDecodeSource decodes a stored document source.
func osDecodeSource(raw json.RawMessage) (map[string]interface{}, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(raw, &src); err != nil {
		return nil, fmt.Errorf("decode source: %w", err)
	}
	return src, nil
}

// osFilterSource keeps the members of obj whose dotted path matches an include (every member, when
// there is none) and no exclude. An object whose own path matches an include is kept whole, less its
// excluded members; one that does not is kept only for the members beneath it that match.
//
// An array is filtered element by element under the array's own path, which is how a path such as
// `items.name` addresses the `name` of every object in an `items` array: an object element is filtered
// as an object, and dropped when nothing in it survives; any other element is kept only when the array's
// path is itself included.
func osFilterSource(obj map[string]interface{}, prefix string, includes, excludes []string) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range obj {
		if fv, keep := osFilterValue(v, prefix+k, includes, excludes); keep {
			out[k] = fv
		}
	}
	return out
}

// osFilterValue filters one value at path p and reports whether it survives.
func osFilterValue(v interface{}, p string, includes, excludes []string) (interface{}, bool) {
	if osMatchesAny(excludes, p) {
		return nil, false
	}
	included := len(includes) == 0 || osMatchesAny(includes, p)
	// Beneath an included path every member is included, so only the excludes still apply.
	childIncludes := includes
	if included {
		childIncludes = nil
	}
	switch t := v.(type) {
	case map[string]interface{}:
		sub := osFilterSource(t, p+".", childIncludes, excludes)
		return sub, included || len(sub) > 0
	case []interface{}:
		out := make([]interface{}, 0, len(t))
		for _, e := range t {
			if m, ok := e.(map[string]interface{}); ok {
				sub := osFilterSource(m, p+".", childIncludes, excludes)
				if included || len(sub) > 0 {
					out = append(out, sub)
				}
				continue
			}
			if included {
				out = append(out, e)
			}
		}
		return out, included || len(out) > 0
	}
	return v, included
}

func osMatchesAny(patterns []string, p string) bool {
	for _, pat := range patterns {
		if ok, err := path.Match(pat, p); err == nil && ok {
			return true
		}
	}
	return false
}

// --- Aggregations ---

// osComputeAggs computes simple aggregations over a set of documents.
func osComputeAggs(docs []osDoc, aggs map[string]interface{}) map[string]interface{} {
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

func osTermsAgg(docs []osDoc, field string, size int) map[string]interface{} {
	counts := make(map[string]int)
	for _, d := range docs {
		v := fmt.Sprintf("%v", osGetField(d.src, field))
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

func osCardinalityAgg(docs []osDoc, field string) map[string]interface{} {
	seen := make(map[string]struct{})
	for _, d := range docs {
		v := fmt.Sprintf("%v", osGetField(d.src, field))
		seen[v] = struct{}{}
	}
	return map[string]interface{}{"value": len(seen)}
}

func osValueCountAgg(docs []osDoc, field string) map[string]interface{} {
	count := 0
	for _, d := range docs {
		if osGetField(d.src, field) != nil {
			count++
		}
	}
	return map[string]interface{}{"value": count}
}

func osSumAgg(docs []osDoc, field string) map[string]interface{} {
	var sum float64
	for _, d := range docs {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(d.src, field)), 64)
		if err == nil {
			sum += v
		}
	}
	return map[string]interface{}{"value": sum}
}

func osAvgAgg(docs []osDoc, field string) map[string]interface{} {
	var sum float64
	count := 0
	for _, d := range docs {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(d.src, field)), 64)
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

func osMaxAgg(docs []osDoc, field string) map[string]interface{} {
	var best float64
	first := true
	for _, d := range docs {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(d.src, field)), 64)
		if err == nil && (first || v > best) {
			best = v
			first = false
		}
	}
	if first {
		return map[string]interface{}{"value": nil}
	}
	return map[string]interface{}{"value": best}
}

func osMinAgg(docs []osDoc, field string) map[string]interface{} {
	var best float64
	first := true
	for _, d := range docs {
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", osGetField(d.src, field)), 64)
		if err == nil && (first || v < best) {
			best = v
			first = false
		}
	}
	if first {
		return map[string]interface{}{"value": nil}
	}
	return map[string]interface{}{"value": best}
}
