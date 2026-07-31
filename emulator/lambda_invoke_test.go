package emulator_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// startFakeRIE stands in a Lambda runtime interface emulator on a local port,
// answering with the given body and X-Amz-Function-Error header. The real RIE
// needs Docker, so without this the header plumbing invokePOST performs would go
// untested — and that plumbing is the only path by which a genuine handler
// failure becomes observable.
func startFakeRIE(t *testing.T, body, functionError string) int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/2015-03-31/functions/function/invocations", func(w http.ResponseWriter, _ *http.Request) {
		if functionError != "" {
			w.Header().Set("X-Amz-Function-Error", functionError)
		}
		// The RIE answers 200 whether or not the handler raised; the header is the
		// only difference.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})

	port, err := emulator.FindFreePort()
	require.NoError(t, err)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return port
}

// TestInvokePOST_PropagatesFunctionErrorHeader asserts invokePOST surfaces the
// runtime's X-Amz-Function-Error. It was discarding the response headers
// entirely, so a handler that raised was indistinguishable from one that
// succeeded — there was no path by which the header could ever be set (#393).
func TestInvokePOST_PropagatesFunctionErrorHeader(t *testing.T) {
	t.Parallel()
	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{ReplayMode: "live"},
		emulator.NewDefaultLogger(-4, false), true)
	defer exec.StopAll()

	port := startFakeRIE(t, `{"errorMessage":"boom","errorType":"ValueError"}`, "Handled")
	body, funcErr, err := emulator.InvokePOSTForTest(exec, port, []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, "Handled", funcErr)
	assert.Contains(t, string(body), "boom")
}

// TestInvokePOST_NoHeaderOnSuccess is the other side: a runtime that sets no
// header must yield an empty functionError, which is what lets invoke omit it.
func TestInvokePOST_NoHeaderOnSuccess(t *testing.T) {
	t.Parallel()
	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{ReplayMode: "live"},
		emulator.NewDefaultLogger(-4, false), true)
	defer exec.StopAll()

	port := startFakeRIE(t, `{"statusCode":200}`, "")
	body, funcErr, err := emulator.InvokePOSTForTest(exec, port, []byte(`{}`))
	require.NoError(t, err)
	assert.Empty(t, funcErr)
	assert.Contains(t, string(body), "statusCode")
}

// TestInvokePOST_UnreachableRuntimeErrors asserts a runtime that cannot be
// reached is a propagated error, not a silent empty success.
func TestInvokePOST_UnreachableRuntimeErrors(t *testing.T) {
	t.Parallel()
	exec := emulator.NewLambdaExecutorForTest(emulator.LambdaExecCfg{ReplayMode: "live"},
		emulator.NewDefaultLogger(-4, false), true)
	defer exec.StopAll()

	// Claim a port, then release it so nothing is listening.
	port, err := emulator.FindFreePort()
	require.NoError(t, err)
	_, _, err = emulator.InvokePOSTForTest(exec, port, []byte(`{}`))
	require.Error(t, err)
}
