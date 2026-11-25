package emulator_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// lambdaServeStatus sends one Lambda request and answers its status. It reports failure with
// assert rather than require, so it is safe on a goroutine other than the test's own.
func lambdaServeStatus(t *testing.T, srv *emulator.Server, method, path string, body any) int {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if !assert.NoError(t, err) {
			return 0
		}
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(raw))
	r.Host = "lambda.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Code
}

// lambdaCreateStreamMapping creates a mapping on a Kinesis stream, which nothing polls, and returns
// its UUID and ARN.
func lambdaCreateStreamMapping(t *testing.T, do func(method, path string, body any) (int, map[string]any, string)) (string, string) {
	t.Helper()
	status, got, raw := do(http.MethodPost, "/2015-03-31/event-source-mappings", map[string]any{
		"FunctionName": "fn", "EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
	})
	require.Equal(t, http.StatusCreated, status, raw)
	return got["UUID"].(string), got["EventSourceMappingArn"].(string)
}

// TestLambdaESMConcurrency_TagsSurviveConcurrentUpdates pins that a tag written while updates of the
// same mapping are in flight is kept: an update reads the record under the lock a tag write holds,
// so it cannot save a copy older than the tag.
func TestLambdaESMConcurrency_TagsSurviveConcurrentUpdates(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "fn", "print(1)", nil)
	do := func(method, path string, body any) (int, map[string]any, string) {
		return lambdaDo(t, srv, method, path, body)
	}
	uuid, arn := lambdaCreateStreamMapping(t, do)

	const n = 16
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			code := lambdaServeStatus(t, srv, http.MethodPost, "/2017-03-31/tags/"+arn,
				map[string]any{"Tags": map[string]string{fmt.Sprintf("k%02d", i): "v"}})
			assert.Equal(t, http.StatusNoContent, code)
		}()
		go func() {
			defer wg.Done()
			code := lambdaServeStatus(t, srv, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
				map[string]any{"BatchSize": i + 1})
			assert.Equal(t, http.StatusOK, code)
		}()
	}
	wg.Wait()

	status, got, raw := lambdaDo(t, srv, http.MethodGet, "/2017-03-31/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, status, raw)
	assert.Len(t, got["Tags"], n, "no tag is lost to an update's stale copy: %s", raw)
}

// TestLambdaESMConcurrency_ATagDoesNotResurrectADeletedMapping pins that a tag write racing a delete
// either lands before it or answers ResourceNotFoundException; it never re-saves the mapping.
func TestLambdaESMConcurrency_ATagDoesNotResurrectADeletedMapping(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "fn", "print(1)", nil)
	do := func(method, path string, body any) (int, map[string]any, string) {
		return lambdaDo(t, srv, method, path, body)
	}

	for range 16 {
		uuid, arn := lambdaCreateStreamMapping(t, do)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			_ = lambdaServeStatus(t, srv, http.MethodPost, "/2017-03-31/tags/"+arn, map[string]any{"Tags": map[string]string{"k": "v"}})
		}()
		go func() {
			defer wg.Done()
			_ = lambdaServeStatus(t, srv, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid, map[string]any{"BatchSize": 7})
		}()
		go func() {
			defer wg.Done()
			code := lambdaServeStatus(t, srv, http.MethodDelete, "/2015-03-31/event-source-mappings/"+uuid, nil)
			assert.Equal(t, http.StatusOK, code)
		}()
		wg.Wait()

		status, _, raw := lambdaDo(t, srv, http.MethodGet, "/2015-03-31/event-source-mappings/"+uuid, nil)
		assert.Equal(t, http.StatusNotFound, status, "the deleted mapping stays deleted: %s", raw)
	}
}

// TestLambdaEventInvokeConcurrency_AConfigDoesNotOutliveItsAlias pins that a Put racing DeleteAlias
// leaves no configuration behind for an alias re-created under the same name.
func TestLambdaEventInvokeConcurrency_AConfigDoesNotOutliveItsAlias(t *testing.T) {
	t.Parallel()
	srv := newLambdaTestServer(t)
	lambdaCreateZipFunction(t, srv, "eic", "print(1)", nil)
	version := lambdaPublish(t, srv, "eic")

	for i := range 16 {
		alias := fmt.Sprintf("a%02d", i)
		status, _, raw := lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/eic/aliases",
			map[string]any{"Name": alias, "FunctionVersion": version})
		require.Equal(t, http.StatusCreated, status, raw)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = lambdaServeStatus(t, srv, http.MethodPut, lambdaEventInvokePath+"?Qualifier="+alias, map[string]any{"MaximumRetryAttempts": 1})
		}()
		go func() {
			defer wg.Done()
			code := lambdaServeStatus(t, srv, http.MethodDelete, "/2015-03-31/functions/eic/aliases/"+alias, nil)
			assert.Equal(t, http.StatusNoContent, code)
		}()
		wg.Wait()

		status, _, raw = lambdaDo(t, srv, http.MethodPost, "/2015-03-31/functions/eic/aliases",
			map[string]any{"Name": alias, "FunctionVersion": version})
		require.Equal(t, http.StatusCreated, status, raw)
		status, _, raw = lambdaDo(t, srv, http.MethodGet, lambdaEventInvokePath+"?Qualifier="+alias, nil)
		assert.Equal(t, http.StatusNotFound, status, "the re-created alias inherits no configuration: %s", raw)
	}
}
