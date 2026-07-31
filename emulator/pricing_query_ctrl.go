package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// handlePricingSeedQueryFailure handles POST /v1/pricing/query-failures.
// It seeds the error returned by a Price List Query API operation — GetProducts,
// DescribeServices or GetAttributeValues — for a given operation name (or
// wildcard "*").
//
// This is what makes "pricing degrades, I/O does not" a testable property rather
// than an assumption: a consumer seeds a ThrottlingException here, drives their
// code path, and asserts their reads still succeed with whatever fallback rate
// they carry.
//
// Body: {"operation": "GetProducts", "code": "ThrottlingException",
// "message": "...", "statusCode": 400}
// The "operation" field defaults to "*" (wildcard) if omitted or empty, and
// "statusCode" defaults to the status the Price List API documents for the code.
func (s *Server) handlePricingSeedQueryFailure(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Operation  string `json:"operation"`
		Code       string `json:"code"`
		Message    string `json:"message"`
		StatusCode int    `json:"statusCode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if body.Code == "" {
		http.Error(w, `{"error":"code is required"}`, http.StatusBadRequest)
		return
	}
	if !pricingSeedableErrorCodes[body.Code] {
		http.Error(w, fmt.Sprintf(`{"error":"unknown code %q; must be one of %s"}`,
			body.Code, strings.Join(pricingSeedableCodeList(), ", ")), http.StatusBadRequest)
		return
	}
	operation := body.Operation
	if operation == "" {
		operation = "*"
	}
	message := body.Message
	if message == "" {
		message = "seeded " + body.Code + " for " + operation
	}
	seed, err := json.Marshal(pricingSeededFailure{
		Code:       body.Code,
		Message:    message,
		StatusCode: body.StatusCode,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), pricingCtrlNamespace,
		pricingSeededFailureKey(operation), seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true, "operation": operation})
}

// handlePricingClearQueryFailures handles DELETE /v1/pricing/query-failures.
// With ?operation=... it removes the seeded failure for that specific operation.
// Without a query param it removes all seeded Price List failures.
func (s *Server) handlePricingClearQueryFailures(w http.ResponseWriter, r *http.Request) {
	operation := r.URL.Query().Get("operation")
	if operation != "" {
		if err := s.state.Delete(r.Context(), pricingCtrlNamespace,
			pricingSeededFailureKey(operation)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
		return
	}
	keys, err := s.state.List(r.Context(), pricingCtrlNamespace, "failure:")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), pricingCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]interface{}{"ok": true})
}
