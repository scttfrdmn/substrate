package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// lambdaCtrlNamespace is the state namespace for Lambda control-plane (seed) data.
const lambdaCtrlNamespace = "lambda-ctrl"

// lambdaSeededError is a seeded function-execution failure. Substrate does not
// run the handler, so an invoke always takes the nominal success path; seeding is
// what makes the failure observable, letting a caller's error branch be exercised
// deterministically. Keyed by function name or the "*" wildcard.
type lambdaSeededError struct {
	// FunctionName the seed applies to, or "*" for any function.
	FunctionName string `json:"functionName"`

	// ErrorType is the X-Amz-Function-Error header value: "Handled" for an
	// exception the runtime caught and reported, "Unhandled" for a process that
	// died (timeout, out-of-memory, an exit before responding). Defaults to
	// "Unhandled" when a seed sets neither this nor a payload.
	ErrorType string `json:"errorType"`

	// ErrorMessage and ExceptionType populate the synthesized error payload's
	// "errorMessage" and "errorType" members. Ignored when Payload is set.
	ErrorMessage  string `json:"errorMessage"`
	ExceptionType string `json:"exceptionType"`

	// Payload, when set, is returned verbatim as the invoke response body,
	// overriding the synthesized error object. omitempty matters here: a nil
	// json.RawMessage marshals to the four-byte literal "null", so without it the
	// round trip through the state store would turn "no payload seeded" into a
	// seeded payload of null and every seed would answer "null".
	Payload json.RawMessage `json:"payload,omitempty"`
}

// lambdaCtrlKey returns the state key for a seeded error. Function-scoped seeds
// use "invoke-error:{name}"; the wildcard uses "invoke-error:*".
func lambdaCtrlKey(functionName string) string {
	if functionName == "" {
		functionName = "*"
	}
	return "invoke-error:" + functionName
}

// resolveSeededError returns the seeded failure for a function, matching by exact
// name first then the "*" wildcard. It returns (nil, nil) when no seed applies,
// so the caller falls back to the nominal success path.
func (p *LambdaPlugin) resolveSeededError(functionName string) (*lambdaSeededError, error) {
	goCtx := context.Background()
	for _, key := range []string{lambdaCtrlKey(functionName), lambdaCtrlKey("*")} {
		data, err := p.state.Get(goCtx, lambdaCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("lambda resolveSeededError get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed lambdaSeededError
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("lambda resolveSeededError unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// errorPayload returns the response body for a seeded failure: the verbatim
// Payload when one was seeded, otherwise an error object in the shape the Lambda
// runtime interface clients produce.
func (s *lambdaSeededError) errorPayload(requestID string) []byte {
	// A literal null is treated as "not seeded" rather than passed through: it is
	// what an explicit "payload": null decodes to, and returning it would give the
	// caller a body with no error information at all.
	if body := bytes.TrimSpace(s.Payload); len(body) > 0 && !bytes.Equal(body, []byte("null")) {
		return body
	}
	exceptionType := s.ExceptionType
	if exceptionType == "" {
		exceptionType = "Exception"
	}
	payload, err := json.Marshal(struct {
		ErrorMessage string   `json:"errorMessage"`
		ErrorType    string   `json:"errorType"`
		RequestID    string   `json:"requestId"`
		StackTrace   []string `json:"stackTrace"`
	}{
		ErrorMessage: s.ErrorMessage,
		ErrorType:    exceptionType,
		RequestID:    requestID,
		StackTrace:   []string{},
	})
	if err != nil {
		// Unreachable for all-string fields; fall back to a valid error object so
		// the caller never receives a body the SDK cannot parse.
		return []byte(`{"errorMessage":"","errorType":"Exception","stackTrace":[]}`)
	}
	return payload
}

// functionErrorHeader returns the X-Amz-Function-Error value for a seed,
// defaulting to "Unhandled" when the seed did not name one.
func (s *lambdaSeededError) functionErrorHeader() string {
	if s.ErrorType != "" {
		return s.ErrorType
	}
	return lambdaFunctionErrorUnhandled
}

// handleLambdaSeedInvokeError handles POST /v1/lambda/invoke-error. It seeds the
// function-execution failure Invoke reports for the given function (default "*").
// Substrate does not execute the handler; this sets the observable outcome.
// Body: {"functionName","errorType","errorMessage","exceptionType","payload"}.
func (s *Server) handleLambdaSeedInvokeError(w http.ResponseWriter, r *http.Request) {
	var seed lambdaSeededError
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	switch seed.ErrorType {
	case "", lambdaFunctionErrorHandled, lambdaFunctionErrorUnhandled:
	default:
		http.Error(w, `{"error":"errorType must be \"Handled\" or \"Unhandled\""}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), lambdaCtrlNamespace, lambdaCtrlKey(seed.FunctionName), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "functionName": lambdaCtrlKey(seed.FunctionName)})
}

// handleLambdaClearInvokeError handles DELETE /v1/lambda/invoke-error. With
// ?functionName=... it removes that seed; without it removes all.
func (s *Server) handleLambdaClearInvokeError(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("functionName"); name != "" {
		if err := s.state.Delete(r.Context(), lambdaCtrlNamespace, lambdaCtrlKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	s.clearLambdaSeeds(w, r, "invoke-error:")
}

// clearLambdaSeeds deletes every Lambda seed whose key starts with prefix and
// answers the DELETE.
func (s *Server) clearLambdaSeeds(w http.ResponseWriter, r *http.Request, prefix string) {
	keys, err := s.state.List(r.Context(), lambdaCtrlNamespace, prefix)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	for _, k := range keys {
		if err := s.state.Delete(r.Context(), lambdaCtrlNamespace, k); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}

// lambdaSeededResult is a seeded successful Invoke result. Substrate does not run
// the handler, so without a seed an invoke answers a fixed stub payload; a seed
// sets the payload a caller's success branch reads. Keyed by function name or the
// "*" wildcard. A seeded error for the same function takes precedence.
type lambdaSeededResult struct {
	// FunctionName the seed applies to, or "*" for any function.
	FunctionName string `json:"functionName"`

	// Payload is any JSON value, returned verbatim as the invoke response body.
	Payload json.RawMessage `json:"payload"`
}

// lambdaResultKey returns the state key for a seeded result. Function-scoped
// seeds use "invoke-result:{name}"; the wildcard uses "invoke-result:*".
func lambdaResultKey(functionName string) string {
	if functionName == "" {
		functionName = "*"
	}
	return "invoke-result:" + functionName
}

// resolveSeededResult returns the seeded result for a function, matching by
// exact name first then the "*" wildcard. It returns (nil, nil) when no seed
// applies, so the caller answers the stub payload.
func (p *LambdaPlugin) resolveSeededResult(functionName string) (*lambdaSeededResult, error) {
	goCtx := context.Background()
	for _, key := range []string{lambdaResultKey(functionName), lambdaResultKey("*")} {
		data, err := p.state.Get(goCtx, lambdaCtrlNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("lambda resolveSeededResult get: %w", err)
		}
		if data == nil {
			continue
		}
		var seed lambdaSeededResult
		if err := json.Unmarshal(data, &seed); err != nil {
			return nil, fmt.Errorf("lambda resolveSeededResult unmarshal: %w", err)
		}
		return &seed, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no seed applies", handled by caller.
}

// body returns the seeded payload as the invoke response body.
func (s *lambdaSeededResult) body() []byte {
	return bytes.TrimSpace(s.Payload)
}

// handleLambdaSeedInvokeResult handles POST /v1/lambda/invoke-result. It seeds
// the payload a successful Invoke returns for the given function (default "*").
// The response carries no X-Amz-Function-Error, and a seeded invoke-error for the
// same function still wins. Body: {"functionName","payload"}, where payload is any
// JSON value and is required.
func (s *Server) handleLambdaSeedInvokeResult(w http.ResponseWriter, r *http.Request) {
	var seed lambdaSeededResult
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if len(bytes.TrimSpace(seed.Payload)) == 0 {
		http.Error(w, `{"error":"payload is required"}`, http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	key := lambdaResultKey(seed.FunctionName)
	if err := s.state.Put(r.Context(), lambdaCtrlNamespace, key, data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "functionName": strings.TrimPrefix(key, "invoke-result:")})
}

// handleLambdaClearInvokeResult handles DELETE /v1/lambda/invoke-result. With
// ?functionName=... it removes that seed; without it removes all.
func (s *Server) handleLambdaClearInvokeResult(w http.ResponseWriter, r *http.Request) {
	if name := r.URL.Query().Get("functionName"); name != "" {
		if err := s.state.Delete(r.Context(), lambdaCtrlNamespace, lambdaResultKey(name)); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		writeJSONDebug(w, s.logger, map[string]any{"ok": true})
		return
	}
	s.clearLambdaSeeds(w, r, "invoke-result:")
}
