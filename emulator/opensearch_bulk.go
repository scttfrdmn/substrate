package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
)

// openSearchBulkActions are the four actions the Bulk API reference publishes.
var openSearchBulkActions = map[string]bool{"index": true, "create": true, "update": true, "delete": true}

// openSearchBulkOp is one parsed bulk action: its action name, target, and the source line that
// followed it (empty for delete).
type openSearchBulkOp struct {
	action string
	index  string
	id     string
	source []byte
}

// openSearchBulkRun is the state one bulk request carries across its items: every index record it
// has read, so each is read once, and which of them its items changed, so each is written once.
type openSearchBulkRun struct {
	p       *OpenSearchPlugin
	goCtx   context.Context
	ctx     *RequestContext
	scope   openSearchScope
	indexes map[string]*openSearchIndexRecord // nil value: read, and absent
	dirty   map[string]bool
}

// index returns the named index record, or nil when it does not exist, reading it at most once.
func (r *openSearchBulkRun) index(name string) (*openSearchIndexRecord, error) {
	if rec, seen := r.indexes[name]; seen {
		return rec, nil
	}
	rec, err := r.p.loadIndex(r.goCtx, r.scope, name)
	if err != nil {
		return nil, err
	}
	r.indexes[name] = rec
	return rec, nil
}

// ensureIndex returns the named index record, creating it when it does not exist — the automatic
// index creation every bulk action but a delete performs.
func (r *openSearchBulkRun) ensureIndex(name string) (*openSearchIndexRecord, error) {
	rec, err := r.index(name)
	if err != nil || rec != nil {
		return rec, err
	}
	rec = r.p.newIndexRecord(r.ctx, name)
	r.indexes[name] = rec
	r.dirty[name] = true
	return rec, nil
}

// flush writes every index record an item changed, in name order.
func (r *openSearchBulkRun) flush() error {
	names := make([]string, 0, len(r.dirty))
	for name := range r.dirty {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := r.p.putIndex(r.goCtx, r.scope, r.indexes[name]); err != nil {
			return err
		}
	}
	return nil
}

// bulk answers POST|PUT /_bulk and /{index}/_bulk.
//
// The request is parsed and validated whole before any action runs, because a malformed action line
// or a missing index or ID refuses the whole request rather than one item. Each action then runs in
// order and reports its own item, per the Bulk API reference:
//
//   - index creates or replaces a document: 201 `created` or 200 `updated`.
//   - create creates one, and answers a 409 version_conflict_engine_exception item when it exists.
//   - update merges the line's `doc` into the existing source (200 `updated`, or 200 `noop` when the
//     merge changes nothing); for a missing document it creates `upsert`, or `doc` under
//     `doc_as_upsert` (201 `created`), and otherwise answers the reference's 404
//     document_missing_exception item. A `script` update is refused per item: running a script is the
//     workload, not an API observation.
//   - delete answers 200 `deleted`, or 404 `not_found` for an absent document — not an error, so it
//     does not set `errors`. A delete on an index that does not exist is a failed item,
//     index_not_found_exception at 404.
//
// Every action but a delete creates a missing index, as the engine does: TransportBulkAction collects
// the indices to auto-create from every request except a delete ("delete requests should not attempt
// to create the index"), and fails a delete whose index is absent with IndexNotFoundException.
//
// `errors` is true when any item carries an `error` object, which is what the reference states it
// reports.
func (p *OpenSearchPlugin) bulk(ctx *RequestContext, req *AWSRequest, scope openSearchScope, defaultIndex string) (*AWSResponse, error) {
	ops, bad := parseOpenSearchBulk(req.Body, defaultIndex)
	if bad != nil {
		return bad.response()
	}
	run := &openSearchBulkRun{
		p: p, goCtx: context.Background(), ctx: ctx, scope: scope,
		indexes: make(map[string]*openSearchIndexRecord), dirty: make(map[string]bool),
	}
	items := make([]interface{}, 0, len(ops))
	anyError := false
	for _, op := range ops {
		if op.id == "" && (op.action == "index" || op.action == "create") {
			op.id = generateOpenSearchID(ctx.IDs)
		}
		item, err := run.item(op)
		if err != nil {
			return nil, err
		}
		if _, failed := item["error"]; failed {
			anyError = true
		}
		items = append(items, map[string]interface{}{op.action: item})
	}
	if err := run.flush(); err != nil {
		return nil, err
	}
	return openSearchOK(map[string]interface{}{
		"took":   1,
		"errors": anyError,
		"items":  items,
	})
}

// parseOpenSearchBulk splits an NDJSON bulk body into its actions, refusing the whole request for a
// line that is not an action, an action with no source line, or one that names no index or (for
// update and delete) no ID. The type names are the engine's; the reasons are substrate's own.
func parseOpenSearchBulk(body []byte, defaultIndex string) ([]openSearchBulkOp, *openSearchRefusal) {
	var lines []string
	for _, l := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) == 0 {
		return nil, openSearchError(http.StatusBadRequest, "action_request_validation_exception", "the bulk request holds no action")
	}
	var ops []openSearchBulkOp
	for i := 0; i < len(lines); i++ {
		lineNo := i + 1
		var action map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil || len(action) != 1 {
			return nil, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
				fmt.Sprintf("line [%d] is not an action/metadata object with exactly one action", lineNo))
		}
		var op openSearchBulkOp
		for name, meta := range action {
			op.action = name
			op.index, _ = meta["_index"].(string)
			if id, ok := meta["_id"]; ok && id != nil {
				op.id = fmt.Sprintf("%v", id)
			}
		}
		if !openSearchBulkActions[op.action] {
			return nil, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
				fmt.Sprintf("line [%d] names action [%s]; expected one of [create, delete, index, update]", lineNo, op.action))
		}
		if op.index == "" {
			op.index = defaultIndex
		}
		if op.index == "" {
			return nil, openSearchError(http.StatusBadRequest, "action_request_validation_exception",
				fmt.Sprintf("line [%d]: index is missing", lineNo))
		}
		if op.id == "" && (op.action == "update" || op.action == "delete") {
			return nil, openSearchError(http.StatusBadRequest, "action_request_validation_exception",
				fmt.Sprintf("line [%d]: id is missing", lineNo))
		}
		if op.action != "delete" {
			if i+1 >= len(lines) {
				return nil, openSearchError(http.StatusBadRequest, "illegal_argument_exception",
					fmt.Sprintf("line [%d]: the %s action has no source line", lineNo, op.action))
			}
			i++
			op.source = []byte(lines[i])
		}
		ops = append(ops, op)
	}
	return ops, nil
}

// item runs one bulk action and returns its item body.
func (r *openSearchBulkRun) item(op openSearchBulkOp) (map[string]interface{}, error) {
	switch op.action {
	case "delete":
		idx, err := r.index(op.index)
		if err != nil {
			return nil, err
		}
		if idx == nil {
			return osBulkError(op, nil, http.StatusNotFound, "index_not_found_exception", "no such index ["+op.index+"]"), nil
		}
		existing, err := r.p.loadDoc(r.goCtx, r.scope, op.index, op.id)
		if err != nil {
			return nil, err
		}
		version, seqNo, err := r.p.removeDoc(r.goCtx, r.scope, idx, op.id, existing)
		if err != nil {
			return nil, err
		}
		r.dirty[op.index] = true
		if existing == nil {
			return osBulkResult(http.StatusNotFound, openSearchWriteResult(op.index, op.id, version, seqNo, "not_found")), nil
		}
		return osBulkResult(http.StatusOK, openSearchWriteResult(op.index, op.id, version, seqNo, "deleted")), nil

	case "index", "create":
		if bad := openSearchValidateSource(op.source); bad != nil {
			return osBulkError(op, nil, http.StatusBadRequest, bad.errType, bad.reason), nil
		}
		idx, err := r.ensureIndex(op.index)
		if err != nil {
			return nil, err
		}
		existing, err := r.p.loadDoc(r.goCtx, r.scope, op.index, op.id)
		if err != nil {
			return nil, err
		}
		if op.action == "create" && existing != nil {
			return osBulkError(op, idx, http.StatusConflict, "version_conflict_engine_exception",
				fmt.Sprintf("[%s]: document already exists (current version [%d])", op.id, existing.Version)), nil
		}
		return r.write(idx, op, op.source, existing)

	default: // update
		return r.update(op)
	}
}

// update runs one update action.
//
// An update whose merged source equals the stored one is a no-op: the update-document reference
// states `detect_noop` defaults to `true`, "If no changes are detected, the update is skipped", and
// that a skipped update returns `"result": "noop"` with `_shards.total` 0. Nothing is written, so the
// version and sequence number are the stored document's. `"detect_noop": false` writes regardless.
func (r *openSearchBulkRun) update(op openSearchBulkOp) (map[string]interface{}, error) {
	var body struct {
		Doc         map[string]interface{} `json:"doc"`
		DocAsUpsert bool                   `json:"doc_as_upsert"`
		Upsert      map[string]interface{} `json:"upsert"`
		Script      interface{}            `json:"script"`
		DetectNoop  *bool                  `json:"detect_noop"`
	}
	if err := json.Unmarshal(op.source, &body); err != nil {
		return osBulkError(op, nil, http.StatusBadRequest, "x_content_parse_exception", "the update line is not a JSON object"), nil //nolint:nilerr
	}
	if body.Script != nil {
		return osBulkError(op, nil, http.StatusBadRequest, "illegal_argument_exception",
			"substrate does not run update scripts; send a partial document in doc"), nil
	}
	idx, err := r.ensureIndex(op.index)
	if err != nil {
		return nil, err
	}
	existing, err := r.p.loadDoc(r.goCtx, r.scope, op.index, op.id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		upsert := body.Upsert
		if upsert == nil && body.DocAsUpsert {
			upsert = body.Doc
		}
		if upsert == nil {
			// The Bulk API reference's failed-item example: document_missing_exception at 404,
			// reason "[{id}]: document missing".
			return osBulkError(op, idx, http.StatusNotFound, "document_missing_exception", "["+op.id+"]: document missing"), nil
		}
		src, err := json.Marshal(upsert)
		if err != nil {
			return nil, fmt.Errorf("opensearch bulk upsert marshal: %w", err)
		}
		return r.write(idx, op, src, nil)
	}
	stored, err := osDecodeSource(existing.Source)
	if err != nil {
		return nil, fmt.Errorf("opensearch bulk update %s/%s: %w", op.index, op.id, err)
	}
	merged := osDeepCopy(stored)
	osDeepMerge(merged, body.Doc)
	if (body.DetectNoop == nil || *body.DetectNoop) && reflect.DeepEqual(stored, merged) {
		result := openSearchWriteResult(op.index, op.id, existing.Version, existing.SeqNo, "noop")
		result["_shards"] = map[string]int{"total": 0, "successful": 0, "failed": 0}
		result["_primary_term"] = existing.PrimaryTerm
		return osBulkResult(http.StatusOK, result), nil
	}
	src, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("opensearch bulk update marshal: %w", err)
	}
	return r.write(idx, op, src, existing)
}

// osDeepCopy copies a decoded source so a merge into the copy leaves the original intact. Only
// objects are copied member by member; [osDeepMerge] replaces any other value rather than editing
// it, so sharing them is safe.
func osDeepCopy(src map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		if m, ok := v.(map[string]interface{}); ok {
			out[k] = osDeepCopy(m)
			continue
		}
		out[k] = v
	}
	return out
}

// write stores one document for a bulk action and returns its item.
func (r *openSearchBulkRun) write(idx *openSearchIndexRecord, op openSearchBulkOp, source []byte, existing *openSearchDocRecord) (map[string]interface{}, error) {
	rec, err := r.p.writeDoc(r.goCtx, r.scope, idx, op.id, json.RawMessage(source), existing)
	if err != nil {
		return nil, err
	}
	r.dirty[op.index] = true
	if existing == nil {
		return osBulkResult(http.StatusCreated, openSearchWriteResult(op.index, op.id, rec.Version, rec.SeqNo, "created")), nil
	}
	return osBulkResult(http.StatusOK, openSearchWriteResult(op.index, op.id, rec.Version, rec.SeqNo, "updated")), nil
}

// osBulkResult is a successful item: the write result with the item's `status`.
func osBulkResult(status int, result map[string]interface{}) map[string]interface{} {
	result["status"] = status
	return result
}

// osBulkError is a failed item. The Bulk API reference publishes its members: `_index`, `_id`,
// `status`, and an `error` object of `type`, `reason`, `index`, `shard` and `index_uuid`. A failure
// against an index that does not exist reports the uuid `_na_`.
func osBulkError(op openSearchBulkOp, idx *openSearchIndexRecord, status int, errType, reason string) map[string]interface{} {
	uuid := "_na_"
	if idx != nil {
		uuid = idx.UUID
	}
	return map[string]interface{}{
		"_index": op.index,
		"_id":    op.id,
		"status": status,
		"error": map[string]interface{}{
			"type":       errType,
			"reason":     reason,
			"index":      op.index,
			"shard":      "0",
			"index_uuid": uuid,
		},
	}
}
