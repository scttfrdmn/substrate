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
	return "invoke-error:" + lambdaSeedFunctionName(functionName)
}

// lambdaSeedFunctionName reduces the forms Invoke accepts as FunctionName to the bare
// name a seed is keyed by, which is the name [LambdaPlugin.invoke] looks seeds up
// under. Invoke's FunctionName documents three forms, each optionally followed by a
// version or alias qualifier: a name (my-function), a full ARN
// (arn:aws:lambda:us-west-2:123456789012:function:my-function) and a partial ARN
// (123456789012:function:my-function). Only the two ARN forms are read for their
// function segment; anything else is a name, whose first segment is the name, which is
// how a function named "function" keeps its name. Empty is the "*" wildcard.
func lambdaSeedFunctionName(functionName string) string {
	if functionName == "" || functionName == "*" {
		return "*"
	}
	parts := strings.Split(functionName, ":")
	switch {
	case parts[0] == "arn" && len(parts) >= 7 && parts[5] == "function":
		return parts[6]
	case len(parts) >= 3 && isAccountID(parts[0]) && parts[1] == "function":
		return parts[2]
	}
	return parts[0]
}

// resolveInvokeSeeds returns the seeded error or the seeded result an invoke of
// functionName answers, at most one of them, or neither when no seed applies.
//
// A seed for the function's own name outranks a "*" seed of either kind, and at the
// same scope an error outranks a result. So the order is exact error, exact result,
// "*" error, "*" result: a wildcard failure does not mask a result seeded for one
// function, and a result never masks an error seeded at the same scope.
func (p *LambdaPlugin) resolveInvokeSeeds(functionName string) (*lambdaSeededError, *lambdaSeededResult, error) {
	goCtx := context.Background()
	for _, name := range []string{functionName, "*"} {
		data, err := p.state.Get(goCtx, lambdaCtrlNamespace, lambdaCtrlKey(name))
		if err != nil {
			return nil, nil, fmt.Errorf("lambda invoke-error seed get: %w", err)
		}
		if data != nil {
			var seed lambdaSeededError
			if err := json.Unmarshal(data, &seed); err != nil {
				return nil, nil, fmt.Errorf("lambda invoke-error seed unmarshal: %w", err)
			}
			return &seed, nil, nil
		}
		data, err = p.state.Get(goCtx, lambdaCtrlNamespace, lambdaResultKey(name))
		if err != nil {
			return nil, nil, fmt.Errorf("lambda invoke-result seed get: %w", err)
		}
		if data != nil {
			var seed lambdaSeededResult
			if err := json.Unmarshal(data, &seed); err != nil {
				return nil, nil, fmt.Errorf("lambda invoke-result seed unmarshal: %w", err)
			}
			return nil, &seed, nil
		}
	}
	return nil, nil, nil
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
	writeJSONDebug(w, s.logger, map[string]any{"ok": true,
		"functionName": strings.TrimPrefix(lambdaCtrlKey(seed.FunctionName), "invoke-error:")})
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
// "*" wildcard; see [LambdaPlugin.resolveInvokeSeeds] for how it ranks against a
// seeded error.
type lambdaSeededResult struct {
	// FunctionName the seed applies to, or "*" for any function.
	FunctionName string `json:"functionName"`

	// Payload is any JSON value, returned verbatim as the invoke response body.
	Payload json.RawMessage `json:"payload"`
}

// lambdaResultKey returns the state key for a seeded result. Function-scoped
// seeds use "invoke-result:{name}"; the wildcard uses "invoke-result:*".
func lambdaResultKey(functionName string) string {
	return "invoke-result:" + lambdaSeedFunctionName(functionName)
}

// body returns the seeded payload as the invoke response body.
func (s *lambdaSeededResult) body() []byte {
	return bytes.TrimSpace(s.Payload)
}

// handleLambdaSeedInvokeResult handles POST /v1/lambda/invoke-result. It seeds
// the payload a successful Invoke returns for the given function (default "*").
// The response carries no X-Amz-Function-Error; see [LambdaPlugin.resolveInvokeSeeds]
// for the order a result and an error seed are resolved in. Body: {"functionName","payload"}, where payload is any
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
