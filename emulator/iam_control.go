package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// iamCtrlNamespace is the state namespace for IAM control-plane seed data.
//
// It is separate from iamNamespace for the reason every other …-ctrl namespace is: a
// seed is test scaffolding, not an IAM record, and putting it in the same namespace
// would make it appear in a prefix scan that lists entities.
const iamCtrlNamespace = "iam-ctrl"

// iamCtrlSLRDeletionStatusKey returns the state key for a seeded service-linked-role
// deletion outcome, keyed by role name or by the "*" wildcard.
func iamCtrlSLRDeletionStatusKey(roleName string) string {
	return "slr_deletion_status:" + roleName
}

// handleIAMSeedSLRDeletionStatus handles POST /v1/iam/slr-deletion-status.
//
// It overrides the outcome DeleteServiceLinkedRole records for a service-linked role
// (or the wildcard "*"), which is what makes the failure AWS documents reachable: "If
// you submit a deletion request for a service-linked role whose linked service is still
// accessing a resource, then the deletion task fails." Substrate runs no linked service,
// so nothing here can ever be still using the role — the outcome has to be seeded or the
// FAILED branch of a consumer's poll loop is untestable.
//
// Body: {"roleName": "...", "status": "FAILED", "reason": "...",
// "roleUsageList": [{"Region": "...", "Resources": ["..."]}]}
// "roleName" defaults to "*" when omitted. "status" must be one of AWS's four documented
// values, so a typo is refused here rather than surfacing later as a status no SDK models.
//
// A seeded FAILED or IN_PROGRESS status also leaves the role in place, because that is
// the observable difference between the outcomes — a caller who polls to FAILED and then
// calls GetRole must still find the role there.
func (s *Server) handleIAMSeedSLRDeletionStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RoleName      string            `json:"roleName"`
		Status        string            `json:"status"`
		Reason        string            `json:"reason"`
		RoleUsageList []IAMSLRRoleUsage `json:"roleUsageList"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Status == "" {
		http.Error(w, `{"error":"status is required"}`, http.StatusBadRequest)
		return
	}
	if !iamSLRDeletionStatuses[body.Status] {
		http.Error(w, fmt.Sprintf(`{"error":"status must be one of %s, %s, %s, %s"}`,
			iamSLRDeletionSucceeded, iamSLRDeletionInProgress,
			iamSLRDeletionFailed, iamSLRDeletionNotStarted), http.StatusBadRequest)
		return
	}
	name := body.RoleName
	if name == "" {
		name = "*"
	}
	seed, err := json.Marshal(IAMSLRDeletionTask{
		RoleName:      body.RoleName,
		Status:        body.Status,
		Reason:        body.Reason,
		RoleUsageList: body.RoleUsageList,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), iamCtrlNamespace,
		iamCtrlSLRDeletionStatusKey(name), seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "roleName": name, "status": body.Status})
}

// handleIAMClearSLRDeletionStatus handles DELETE /v1/iam/slr-deletion-status.
//
// With ?roleName=... it removes the seed for that role. Without a query parameter it
// removes every seeded deletion outcome.
func (s *Server) handleIAMClearSLRDeletionStatus(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("roleName")
	if name != "" {
		if err := s.state.Delete(r.Context(), iamCtrlNamespace,
			iamCtrlSLRDeletionStatusKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), iamCtrlNamespace, "slr_deletion_status:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), iamCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
