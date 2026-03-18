package substrate_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetTime(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/control/time", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Contains(t, resp, "simulated_time")
	assert.Contains(t, resp, "scale")
	assert.InDelta(t, 1.0, resp["scale"], 0.001)
}

func TestHandleSetTime_Valid(t *testing.T) {
	srv := newTestServer(t)

	body := `{"time":"2030-01-01T00:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/control/time", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	simTime, ok := resp["simulated_time"].(string)
	require.True(t, ok)
	assert.True(t, strings.HasPrefix(simTime, "2030-01-01"), "expected simulated_time to start with 2030-01-01, got %s", simTime)

	// Verify GET also reflects the new time.
	req2 := httptest.NewRequest(http.MethodGet, "/v1/control/time", nil)
	rr2 := httptest.NewRecorder()
	srv.ServeHTTP(rr2, req2)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &resp2))
	simTime2, _ := resp2["simulated_time"].(string)
	assert.True(t, strings.HasPrefix(simTime2, "2030-01-01"), "expected simulated_time to start with 2030-01-01 after GET, got %s", simTime2)
}

func TestHandleSetTime_RFC3339NanoFormat(t *testing.T) {
	srv := newTestServer(t)

	body := `{"time":"2030-06-15T10:30:00.123456789Z"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/control/time", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	simTime, _ := resp["simulated_time"].(string)
	assert.True(t, strings.HasPrefix(simTime, "2030-06-15"), "expected 2030-06-15, got %s", simTime)
}

func TestHandleSetTime_InvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/time", strings.NewReader("notjson"))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleSetTime_InvalidFormat(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/time", strings.NewReader(`{"time":"not-a-time"}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleSetScale_Valid(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/scale", strings.NewReader(`{"scale":7200}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.InDelta(t, 7200.0, resp["scale"], 0.001)
}

func TestHandleSetScale_ZeroScale(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/scale", strings.NewReader(`{"scale":0}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleSetScale_NegativeScale(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/scale", strings.NewReader(`{"scale":-1}`))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleSetScale_InvalidJSON(t *testing.T) {
	srv := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/control/scale", strings.NewReader("notjson"))
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestTimeScale_AcceleratesTime(t *testing.T) {
	srv := newTestServer(t)

	// Set a known baseline time.
	baseline := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	setTimeReq := httptest.NewRequest(http.MethodPost, "/v1/control/time",
		strings.NewReader(`{"time":"2026-01-01T00:00:00Z"}`))
	srv.ServeHTTP(httptest.NewRecorder(), setTimeReq)

	// Set a very high scale so even a tiny sleep advances simulated time noticeably.
	// scale=86400000 → 1ms wall time = 86400s = 1 day simulated.
	setScaleReq := httptest.NewRequest(http.MethodPost, "/v1/control/scale",
		strings.NewReader(`{"scale":86400000}`))
	srv.ServeHTTP(httptest.NewRecorder(), setScaleReq)

	// Sleep 1ms to let simulated time advance.
	time.Sleep(time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/v1/control/time", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	simTimeStr, ok := resp["simulated_time"].(string)
	require.True(t, ok)
	simTime, err := time.Parse(time.RFC3339Nano, simTimeStr)
	require.NoError(t, err)

	// Simulated time must have advanced by at least 1 second (well beyond baseline).
	assert.True(t, simTime.After(baseline.Add(time.Second)),
		"expected simulated time %v to be well after baseline+1s; scale acceleration not working", simTime)
}
