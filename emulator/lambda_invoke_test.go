package emulator_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// lambdaRequestWithHeaders posts to the Lambda endpoint with extra request
// headers, which the plain lambdaRequest helper cannot express. Invoke's LogType
// and InvocationType are headers, not body fields.
func lambdaRequestWithHeaders(t *testing.T, srv *emulator.Server, method, path string, body any, headers map[string]string) *http.Response {
	t.Helper()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	r.Host = "lambda.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// createInvokeTestFunction creates a function and returns its name.
func createInvokeTestFunction(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name,
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	return name
}

// seedInvokeError posts a seeded function-execution failure to the control plane.
func seedInvokeError(t *testing.T, srv *emulator.Server, seed map[string]any) {
	t.Helper()
	body, err := json.Marshal(seed)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/v1/lambda/invoke-error", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "seed failed: %s", w.Body.String())
}

// TestInvoke_SuccessOmitsFunctionError is the regression in #393. AWS documents
// X-Amz-Function-Error as "If present, indicates that an error occurred", so the
// SDK maps its absence to the key being absent from the response dict. Substrate
// sent the header with an empty value, which materializes as
// FunctionError: "" — inverting the natural `if "FunctionError" in response`
// check so that every successful invocation looked like a failure.
func TestInvoke_SuccessOmitsFunctionError(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "omit-fe")

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/omit-fe/invocations", map[string]any{"k": "v"})
	defer resp.Body.Close() //nolint:errcheck

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_, present := resp.Header["X-Amz-Function-Error"]
	assert.False(t, present, "X-Amz-Function-Error must be absent on success")
	assert.Equal(t, "$LATEST", resp.Header.Get("X-Amz-Executed-Version"))
}

// TestInvoke_OmitsLogResultWithoutTail covers the second half of #393. LogResult
// is returned only when the caller sets X-Amz-Log-Type: Tail; substrate returned
// it unconditionally.
func TestInvoke_OmitsLogResultWithoutTail(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "omit-log")

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/omit-log/invocations", map[string]any{"k": "v"})
	defer resp.Body.Close() //nolint:errcheck

	_, present := resp.Header["X-Amz-Log-Result"]
	assert.False(t, present, "X-Amz-Log-Result must be absent without LogType=Tail")
}

// TestInvoke_LogTypeNoneOmitsLogResult pins the other enum value: LogType's
// valid values are None and Tail, and None must behave like absence.
func TestInvoke_LogTypeNoneOmitsLogResult(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "logtype-none")

	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost,
		"/2015-03-31/functions/logtype-none/invocations", map[string]any{"k": "v"},
		map[string]string{"X-Amz-Log-Type": "None"})
	defer resp.Body.Close() //nolint:errcheck

	_, present := resp.Header["X-Amz-Log-Result"]
	assert.False(t, present, "LogType=None must not return a log")
}

// TestInvoke_LogTypeTailReturnsBase64Log asserts the log is returned when asked
// for, and that it is base64 — the field is documented as "the last 4 KB of the
// execution log, which is base64-encoded", and a caller decodes it blindly.
func TestInvoke_LogTypeTailReturnsBase64Log(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "logtype-tail")

	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost,
		"/2015-03-31/functions/logtype-tail/invocations", map[string]any{"k": "v"},
		map[string]string{"X-Amz-Log-Type": "Tail"})
	defer resp.Body.Close() //nolint:errcheck

	encoded := resp.Header.Get("X-Amz-Log-Result")
	require.NotEmpty(t, encoded)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err, "LogResult must be base64")
	assert.Contains(t, string(decoded), "START RequestId:")
	assert.Contains(t, string(decoded), "REPORT RequestId:")
}

// TestInvoke_LogTypeIsCaseInsensitive guards the header *value* comparison. Go
// canonicalizes header names but not values, and an SDK that sends "tail" would
// otherwise silently get no log.
func TestInvoke_LogTypeIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "logtype-case")

	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost,
		"/2015-03-31/functions/logtype-case/invocations", map[string]any{"k": "v"},
		map[string]string{"X-Amz-Log-Type": "tail"})
	defer resp.Body.Close() //nolint:errcheck

	assert.NotEmpty(t, resp.Header.Get("X-Amz-Log-Result"))
}

// TestInvoke_SeededErrorReportsFunctionError is the other side of #393: the
// header must appear when the function *did* fail. Substrate does not run the
// handler, so a seed is the only way to reach that state — and without this the
// omission above could be "correct" simply by never setting the header at all.
func TestInvoke_SeededErrorReportsFunctionError(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "seeded-fail")
	seedInvokeError(t, srv, map[string]any{
		"functionName":  "seeded-fail",
		"errorType":     "Handled",
		"errorMessage":  "boom",
		"exceptionType": "ValueError",
	})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/seeded-fail/invocations", map[string]any{"k": "v"})
	defer resp.Body.Close() //nolint:errcheck

	// The status code must stay 200: per the Invoke reference, "the status code in
	// the API response doesn't reflect function errors".
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Handled", resp.Header.Get("X-Amz-Function-Error"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, "boom", payload["errorMessage"])
	assert.Equal(t, "ValueError", payload["errorType"])
	assert.NotEmpty(t, payload["requestId"])
	assert.NotNil(t, payload["stackTrace"])
}

// TestInvoke_SeededErrorDefaultsToUnhandled covers the seed's default: a seed
// that names no errorType is a process that died, which AWS reports as Unhandled.
func TestInvoke_SeededErrorDefaultsToUnhandled(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "seeded-default")
	seedInvokeError(t, srv, map[string]any{"functionName": "seeded-default"})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/seeded-default/invocations", nil)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, "Unhandled", resp.Header.Get("X-Amz-Function-Error"))
}

// TestInvoke_SeededErrorWildcard asserts the "*" wildcard seed applies to any
// function, matching the seed convention the other plugins use.
func TestInvoke_SeededErrorWildcard(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "wildcard-target")
	seedInvokeError(t, srv, map[string]any{"errorType": "Unhandled", "errorMessage": "any"})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/wildcard-target/invocations", nil)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, "Unhandled", resp.Header.Get("X-Amz-Function-Error"))
}

// TestInvoke_SeededErrorVerbatimPayload asserts a seeded payload is returned
// byte-for-byte, letting a test drive a caller that parses a runtime-specific
// error shape substrate does not synthesize.
func TestInvoke_SeededErrorVerbatimPayload(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "seeded-payload")
	seedInvokeError(t, srv, map[string]any{
		"functionName": "seeded-payload",
		"errorType":    "Handled",
		"payload":      map[string]any{"custom": "shape"},
	})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/seeded-payload/invocations", nil)
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"custom":"shape"}`, string(body))
}

// TestInvoke_SeededErrorNullPayloadSynthesizes guards a trap in the seed's
// round trip: a nil json.RawMessage marshals to the four-byte literal "null", so
// "no payload seeded" could come back out of the state store looking like a
// seeded payload and every seeded failure would answer "null" — a body carrying
// no error information at all. An explicit "payload": null is treated the same
// way, since it decodes identically.
func TestInvoke_SeededErrorNullPayloadSynthesizes(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "seeded-null")
	seedInvokeError(t, srv, map[string]any{
		"functionName": "seeded-null",
		"errorMessage": "still reported",
		"payload":      nil,
	})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/seeded-null/invocations", nil)
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotEqual(t, "null", string(body))
	var payload map[string]any
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, "still reported", payload["errorMessage"])
	// No exceptionType was seeded, so the synthesized default applies.
	assert.Equal(t, "Exception", payload["errorType"])
}

// TestInvoke_SeededErrorWithTailLogsError asserts the requested log reflects the
// failure, so a caller reading LogResult to explain a failure sees one.
func TestInvoke_SeededErrorWithTailLogsError(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "seeded-tail")
	seedInvokeError(t, srv, map[string]any{"functionName": "seeded-tail", "errorType": "Unhandled"})

	resp := lambdaRequestWithHeaders(t, srv, http.MethodPost,
		"/2015-03-31/functions/seeded-tail/invocations", nil,
		map[string]string{"X-Amz-Log-Type": "Tail"})
	defer resp.Body.Close() //nolint:errcheck

	decoded, err := base64.StdEncoding.DecodeString(resp.Header.Get("X-Amz-Log-Result"))
	require.NoError(t, err)
	assert.Contains(t, string(decoded), "[ERROR]")
}

// TestInvoke_ClearSeededErrorRestoresSuccess asserts the seed is removable, so a
// test can exercise the failure path and then the success path in one process.
func TestInvoke_ClearSeededErrorRestoresSuccess(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "clearable")
	seedInvokeError(t, srv, map[string]any{"functionName": "clearable", "errorType": "Handled"})

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/clearable/invocations", nil)
	require.Equal(t, "Handled", resp.Header.Get("X-Amz-Function-Error"))
	resp.Body.Close() //nolint:errcheck

	r := httptest.NewRequest(http.MethodDelete, "/v1/lambda/invoke-error?functionName=clearable", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	after := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/clearable/invocations", nil)
	defer after.Body.Close() //nolint:errcheck
	_, present := after.Header["X-Amz-Function-Error"]
	assert.False(t, present, "clearing the seed must restore the success path")
}

// TestInvoke_ClearAllSeededErrors covers the no-query-param branch, which clears
// every seed including the wildcard.
func TestInvoke_ClearAllSeededErrors(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "clear-all")
	seedInvokeError(t, srv, map[string]any{"errorType": "Handled"})

	r := httptest.NewRequest(http.MethodDelete, "/v1/lambda/invoke-error", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	resp := lambdaRequest(t, srv, http.MethodPost,
		"/2015-03-31/functions/clear-all/invocations", nil)
	defer resp.Body.Close() //nolint:errcheck
	_, present := resp.Header["X-Amz-Function-Error"]
	assert.False(t, present)
}

// TestSeedInvokeError_RejectsInvalidErrorType asserts the control plane rejects a
// value that is not Handled or Unhandled, rather than storing a seed that would
// put an unparseable value on the wire.
func TestSeedInvokeError_RejectsInvalidErrorType(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)

	body, err := json.Marshal(map[string]any{"errorType": "Sideways"})
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/v1/lambda/invoke-error", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestSeedInvokeError_RejectsMalformedBody covers the decode-failure branch.
func TestSeedInvokeError_RejectsMalformedBody(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/v1/lambda/invoke-error",
		bytes.NewReader([]byte(`{not json`)))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// lambdaControl sends one request to a Lambda control-plane endpoint and returns the
// status and body.
func lambdaControl(t *testing.T, srv *emulator.Server, method, path string, body any) (int, string) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, path, reader)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// invokeBody invokes name and returns the response body and whether
// X-Amz-Function-Error was present.
func invokeBody(t *testing.T, srv *emulator.Server, name string) (string, bool) {
	t.Helper()
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions/"+name+"/invocations", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_, errored := resp.Header["X-Amz-Function-Error"]
	return string(b), errored
}

// TestInvoke_SeededResult covers POST /v1/lambda/invoke-result: a seeded success
// payload is returned verbatim with no X-Amz-Function-Error, an exact function name
// beats the wildcard, and a seeded invoke-error still wins over either.
func TestInvoke_SeededResult(t *testing.T) {
	t.Parallel()
	const stub = `{"statusCode":200,"body":"null"}`
	tests := []struct {
		name        string
		results     []map[string]any
		seedError   bool
		wantBody    string
		wantErrored bool
	}{
		{name: "no seed answers the stub", wantBody: stub},
		{
			name:     "exact function name",
			results:  []map[string]any{{"functionName": "fn", "payload": map[string]any{"ok": true, "n": 3}}},
			wantBody: `{"ok":true,"n":3}`,
		},
		{
			name:     "wildcard",
			results:  []map[string]any{{"payload": []int{1, 2}}},
			wantBody: `[1,2]`,
		},
		{
			name: "exact name beats the wildcard",
			results: []map[string]any{
				{"functionName": "*", "payload": "wild"},
				{"functionName": "fn", "payload": "exact"},
			},
			wantBody: `"exact"`,
		},
		{
			name:     "a different function's seed does not apply",
			results:  []map[string]any{{"functionName": "other", "payload": 1}},
			wantBody: stub,
		},
		{
			name:        "a seeded error wins",
			results:     []map[string]any{{"functionName": "fn", "payload": 1}},
			seedError:   true,
			wantErrored: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := newLambdaTestServer(t)
			createInvokeTestFunction(t, srv, "fn")
			for _, seed := range tt.results {
				code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result", seed)
				require.Equal(t, http.StatusOK, code, body)
			}
			if tt.seedError {
				seedInvokeError(t, srv, map[string]any{"functionName": "fn", "errorType": "Handled"})
			}
			body, errored := invokeBody(t, srv, "fn")
			assert.Equal(t, tt.wantErrored, errored)
			if !tt.wantErrored {
				assert.JSONEq(t, tt.wantBody, body)
			}
		})
	}
}

// TestInvoke_ClearSeededResult covers both DELETE forms: by function name, which
// leaves other seeds in place, and with no name, which removes every result seed
// but no error seed.
func TestInvoke_ClearSeededResult(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	createInvokeTestFunction(t, srv, "a")
	createInvokeTestFunction(t, srv, "b")
	for _, name := range []string{"a", "b"} {
		code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result",
			map[string]any{"functionName": name, "payload": name})
		require.Equal(t, http.StatusOK, code, body)
		assert.JSONEq(t, `{"ok":true,"functionName":"`+name+`"}`, body)
	}

	code, _ := lambdaControl(t, srv, http.MethodDelete, "/v1/lambda/invoke-result?functionName=a", nil)
	require.Equal(t, http.StatusOK, code)
	body, _ := invokeBody(t, srv, "a")
	assert.JSONEq(t, `{"statusCode":200,"body":"null"}`, body)
	body, _ = invokeBody(t, srv, "b")
	assert.JSONEq(t, `"b"`, body)

	seedInvokeError(t, srv, map[string]any{"functionName": "a", "errorType": "Handled"})
	code, _ = lambdaControl(t, srv, http.MethodDelete, "/v1/lambda/invoke-result", nil)
	require.Equal(t, http.StatusOK, code)
	body, _ = invokeBody(t, srv, "b")
	assert.JSONEq(t, `{"statusCode":200,"body":"null"}`, body)
	_, errored := invokeBody(t, srv, "a")
	assert.True(t, errored, "clearing result seeds must leave error seeds alone")
}

// TestSeedInvokeResult_RejectsBadBodies covers the two refusals: a body that is not
// JSON, and a seed with no payload, which would otherwise answer an empty body.
func TestSeedInvokeResult_RejectsBadBodies(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/v1/lambda/invoke-result", bytes.NewReader([]byte(`{not json`)))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result", map[string]any{"functionName": "f"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "payload is required")
}

// TestInvokeResult_StoreFaults pins that a store failure on a result seed is a 500
// rather than a silent fall-through to the stub, on every path that touches it.
func TestInvokeResult_StoreFaults(t *testing.T) {
	t.Parallel()
	const record = "invoke-result:"
	const invocations = "/2015-03-31/functions/fn/invocations"
	for _, tt := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		path   string
		body   any
	}{
		{"Invoke, seed read", func(m *cfFaultStateManager) { m.failGet = record }, http.MethodPost, invocations, map[string]any{}},
		{"Invoke, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = record }, http.MethodPost, invocations, map[string]any{}},
		{"POST, seed write", func(m *cfFaultStateManager) { m.failPut = record }, http.MethodPost, "/v1/lambda/invoke-result", map[string]any{"payload": 1}},
		{"DELETE one", func(m *cfFaultStateManager) { m.failDelete = record }, http.MethodDelete, "/v1/lambda/invoke-result?functionName=fn", nil},
		{"DELETE all", func(m *cfFaultStateManager) { m.failDelete = record }, http.MethodDelete, "/v1/lambda/invoke-result", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newFullServerOn(t, state)
			createInvokeTestFunction(t, srv, "fn")
			code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result",
				map[string]any{"functionName": "fn", "payload": 1})
			require.Equal(t, http.StatusOK, code, body)

			tt.arm(state)
			if tt.path != invocations {
				code, body = lambdaControl(t, srv, tt.method, tt.path, tt.body)
				assert.Equal(t, http.StatusInternalServerError, code, body)
				return
			}
			resp := lambdaRequest(t, srv, tt.method, tt.path, tt.body)
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	}
}

// TestInvoke_SeedPrecedence pins the order an invoke resolves its seeds in: exact error,
// exact result, "*" error, "*" result. A seed for the function's own name outranks any
// wildcard, and at one scope an error outranks a result.
func TestInvoke_SeedPrecedence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		exactErr, wildErr       bool
		exactResult, wildResult bool
		want                    string // "error", "exact", "wild" or "stub"
	}{
		{name: "exact error beats exact result", exactErr: true, exactResult: true, want: "error"},
		{name: "exact result beats wildcard error", wildErr: true, exactResult: true, want: "exact"},
		{name: "exact error beats wildcard result", exactErr: true, wildResult: true, want: "error"},
		{name: "wildcard error beats wildcard result", wildErr: true, wildResult: true, want: "error"},
		{name: "wildcard result alone", wildResult: true, want: "wild"},
		{name: "all four", exactErr: true, wildErr: true, exactResult: true, wildResult: true, want: "error"},
		{name: "none", want: "stub"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := newLambdaTestServer(t)
			createInvokeTestFunction(t, srv, "fn")
			seedResult := func(name, payload string) {
				code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result",
					map[string]any{"functionName": name, "payload": payload})
				require.Equal(t, http.StatusOK, code, body)
			}
			if tt.exactErr {
				seedInvokeError(t, srv, map[string]any{"functionName": "fn", "errorType": "Handled"})
			}
			if tt.wildErr {
				seedInvokeError(t, srv, map[string]any{"functionName": "*", "errorType": "Handled"})
			}
			if tt.exactResult {
				seedResult("fn", "exact")
			}
			if tt.wildResult {
				seedResult("*", "wild")
			}

			body, errored := invokeBody(t, srv, "fn")
			switch tt.want {
			case "error":
				assert.True(t, errored, body)
			case "stub":
				assert.False(t, errored)
				assert.JSONEq(t, `{"statusCode":200,"body":"null"}`, body)
			default:
				assert.False(t, errored, body)
				assert.JSONEq(t, `"`+tt.want+`"`, body)
			}
		})
	}
}

// TestInvokeSeeds_FunctionNameForms pins which name each FunctionName form keys a seed
// by, read off both seed endpoints' responses. Only the two ARN forms are searched for
// a function segment, so a function named "function" keeps its name.
func TestInvokeSeeds_FunctionNameForms(t *testing.T) {
	t.Parallel()
	const arn = "arn:aws:lambda:us-east-1:123456789012:function:"
	tests := []struct{ in, want string }{
		{"", "*"},
		{"*", "*"},
		{"fn", "fn"},
		{"fn:v1", "fn"},
		{"fn:$LATEST", "fn"},
		{"function", "function"},
		{"function:prod", "function"},
		{"123456789012:function:fn", "fn"},
		{"123456789012:function:fn:prod", "fn"},
		{"123456789012:function:function", "function"},
		{"12345:function:fn", "12345"},
		{arn + "fn", "fn"},
		{arn + "fn:prod", "fn"},
		{arn + "function", "function"},
		{arn + "function:prod", "function"},
	}
	srv := newLambdaTestServer(t)
	for _, tt := range tests {
		for _, endpoint := range []string{"/v1/lambda/invoke-result", "/v1/lambda/invoke-error"} {
			code, body := lambdaControl(t, srv, http.MethodPost, endpoint,
				map[string]any{"functionName": tt.in, "payload": 1})
			require.Equal(t, http.StatusOK, code, body)
			var got struct {
				FunctionName string `json:"functionName"`
			}
			require.NoError(t, json.Unmarshal([]byte(body), &got))
			assert.Equal(t, tt.want, got.FunctionName, "%s %q", endpoint, tt.in)
		}
	}
}

// TestInvokeSeeds_NormaliseTheFunctionName pins that a seed POST or DELETE may name the
// function in any form Invoke accepts — a qualified name, a full ARN or a partial one —
// and keys the seed by the bare name an invoke looks it up under.
func TestInvokeSeeds_NormaliseTheFunctionName(t *testing.T) {
	t.Parallel()
	const arn = "arn:aws:lambda:us-east-1:123456789012:function:fn"
	for _, name := range []string{"fn:v1", arn, arn + ":prod", "123456789012:function:fn"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := newLambdaTestServer(t)
			createInvokeTestFunction(t, srv, "fn")

			code, body := lambdaControl(t, srv, http.MethodPost, "/v1/lambda/invoke-result",
				map[string]any{"functionName": name, "payload": "seeded"})
			require.Equal(t, http.StatusOK, code, body)
			assert.JSONEq(t, `{"ok":true,"functionName":"fn"}`, body)
			got, _ := invokeBody(t, srv, "fn")
			assert.JSONEq(t, `"seeded"`, got)

			seedInvokeError(t, srv, map[string]any{"functionName": name, "errorType": "Handled"})
			_, errored := invokeBody(t, srv, "fn")
			assert.True(t, errored)

			query := "?functionName=" + url.QueryEscape(name)
			code, _ = lambdaControl(t, srv, http.MethodDelete, "/v1/lambda/invoke-error"+query, nil)
			require.Equal(t, http.StatusOK, code)
			code, _ = lambdaControl(t, srv, http.MethodDelete, "/v1/lambda/invoke-result"+query, nil)
			require.Equal(t, http.StatusOK, code)
			got, errored = invokeBody(t, srv, "fn")
			assert.False(t, errored)
			assert.JSONEq(t, `{"statusCode":200,"body":"null"}`, got)
		})
	}
}
