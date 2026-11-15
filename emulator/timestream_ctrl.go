package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// handleTimestreamSeedResult handles POST /v1/timestream-query/results.
// It seeds a Query response for a given query string, or for every query with the explicit "*"
// wildcard, within an account and Region.
// Body: {"queryString": "SELECT ...", "accountId": "123456789012", "region": "us-east-1",
// "result": {"Rows": [...], "ColumnInfo": [...]}}.
//
// `queryString` is required (#1200): before, an omitted one silently became "*", so a caller who
// meant to seed one query installed the wildcard that answers every query. `accountId` and `region`
// are optional and each defaults to the "*" wildcard, so a single-account fixture can seed without
// knowing its own account ID — the convention the AWS Config seeds set (configservice_control.go).
func (s *Server) handleTimestreamSeedResult(w http.ResponseWriter, r *http.Request) {
	var body struct {
		QueryString *string                `json:"queryString"`
		AccountID   string                 `json:"accountId"`
		Region      string                 `json:"region"`
		Result      *TimestreamQueryResult `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.QueryString == nil || *body.QueryString == "" {
		http.Error(w, `{"error":"queryString is required; send \"*\" to seed every query"}`, http.StatusBadRequest)
		return
	}
	if body.Result == nil {
		http.Error(w, `{"error":"result is required"}`, http.StatusBadRequest)
		return
	}
	qs := *body.QueryString
	data, err := json.Marshal(body.Result)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := timestreamCtrlResultKey(body.AccountID, body.Region, qs)
	if err := s.state.Put(r.Context(), timestreamCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{
		"ok":          true,
		"queryString": qs,
		"accountId":   timestreamCtrlScopePart(body.AccountID),
		"region":      timestreamCtrlScopePart(body.Region),
	})
}

// handleTimestreamClearResults handles DELETE /v1/timestream-query/results.
//
// Each of ?queryString=, ?accountId= and ?region= narrows what is removed, and each one omitted
// matches every value: `?accountId=A` removes account A's seeds in every Region and for every query,
// `?region=R` every account's seeds in Region R, `?queryString=Q` the seeds for Q in every scope, and
// no parameter at all every seed. A value given is matched literally against the scope a seed was
// installed under, so `?accountId=*` removes only the seeds installed for any account.
func (s *Server) handleTimestreamClearResults(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	qs, accountID, region := q.Get("queryString"), q.Get("accountId"), q.Get("region")
	keys, err := s.state.List(r.Context(), timestreamCtrlNamespace, "result:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		// result:{account}/{region}/{queryString}; the query string may itself contain a slash.
		parts := strings.SplitN(strings.TrimPrefix(k, "result:"), "/", 3)
		if len(parts) != 3 ||
			(accountID != "" && parts[0] != accountID) ||
			(region != "" && parts[1] != region) ||
			(qs != "" && parts[2] != qs) {
			continue
		}
		if err := s.state.Delete(r.Context(), timestreamCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
