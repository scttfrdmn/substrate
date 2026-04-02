package substrate_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newS3PresignTestServer creates a minimal test server for presign endpoint tests.
func newS3PresignTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// --- handleS3Presign ---------------------------------------------------------

func TestHandleS3Presign_GeneratesURL(t *testing.T) {
	srv := newS3PresignTestServer(t)

	body := `{"bucket":"my-bucket","key":"path/to/obj","method":"GET","expires":300}`
	r := httptest.NewRequest(http.MethodPost, "/v1/s3/presign", strings.NewReader(body))
	r.Host = "localhost:4566"
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))

	rawURL, _ := out["url"].(string)
	assert.Contains(t, rawURL, "X-Amz-Algorithm=AWS4-HMAC-SHA256")
	assert.Contains(t, rawURL, "X-Amz-Expires=300")
	assert.Contains(t, rawURL, "my-bucket")
	assert.Contains(t, rawURL, "path/to/obj")
	assert.Equal(t, "GET", out["method"])
	assert.Equal(t, float64(300), out["expires"])
}

func TestHandleS3Presign_DefaultsGetAndExpiry(t *testing.T) {
	srv := newS3PresignTestServer(t)

	body := `{"bucket":"b","key":"k"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/s3/presign", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&out))
	assert.Equal(t, "GET", out["method"])
	assert.Equal(t, float64(3600), out["expires"])
	assert.Contains(t, out["url"].(string), "X-Amz-Expires=3600")
}

func TestHandleS3Presign_MissingBucket_Returns400(t *testing.T) {
	srv := newS3PresignTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/v1/s3/presign", strings.NewReader(`{"key":"k"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleS3Presign_MissingKey_Returns400(t *testing.T) {
	srv := newS3PresignTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/v1/s3/presign", strings.NewReader(`{"bucket":"b"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- checkPresignedExpiry ----------------------------------------------------

func TestCheckPresignedExpiry_NotPresigned(t *testing.T) {
	q := make(url.Values)
	assert.False(t, substrate.CheckPresignedExpiryForTest(q, time.Now()))
}

func TestCheckPresignedExpiry_NoDateOrExpires(t *testing.T) {
	q := make(url.Values)
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	assert.False(t, substrate.CheckPresignedExpiryForTest(q, time.Now()))
}

func TestCheckPresignedExpiry_NotExpired(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	q := make(url.Values)
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Date", "20260101T120000Z")
	q.Set("X-Amz-Expires", "3600")
	// now == X-Amz-Date, expiry is 1 hour later — not yet expired
	assert.False(t, substrate.CheckPresignedExpiryForTest(q, now))
}

func TestCheckPresignedExpiry_Expired(t *testing.T) {
	now := time.Date(2026, 1, 1, 13, 1, 0, 0, time.UTC) // 1 hr 1 min after date
	q := make(url.Values)
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Date", "20260101T120000Z")
	q.Set("X-Amz-Expires", "3600")
	assert.True(t, substrate.CheckPresignedExpiryForTest(q, now))
}

func TestCheckPresignedExpiry_BadDate_NotExpired(t *testing.T) {
	q := make(url.Values)
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Date", "not-a-date")
	q.Set("X-Amz-Expires", "10")
	assert.False(t, substrate.CheckPresignedExpiryForTest(q, time.Now()))
}

// --- Server presigned URL expiry enforcement ---------------------------------

func TestServer_PresignedURL_ExpiredReturns403(t *testing.T) {
	// Use a TimeController set well into the future so any "just-issued" presigned
	// URL (X-Amz-Expires=10) appears already expired.
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	future := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(future)
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	// Presigned GET request issued in 2026 with X-Amz-Expires=10 — expired by 2030.
	r := httptest.NewRequest(http.MethodGet, "/my-bucket/my-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260101T120000Z&X-Amz-Expires=10&X-Amz-Signature=fake", nil)
	r.Host = "s3.us-east-1.amazonaws.com"
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	assert.Equal(t, http.StatusForbidden, w.Code)
	body, _ := io.ReadAll(w.Body)
	assert.Contains(t, string(body), "AccessDenied")
}

func TestServer_PresignedURL_ValidPassesThrough(t *testing.T) {
	// TimeController at the same time as X-Amz-Date — presigned URL is fresh.
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	state := substrate.NewMemoryStateManager()
	issued := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tc := substrate.NewTimeController(issued)
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	// X-Amz-Expires=3600 — not yet expired at the issued time.
	r := httptest.NewRequest(http.MethodGet, "/my-bucket/my-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260101T120000Z&X-Amz-Expires=3600&X-Amz-Signature=fake", nil)
	r.Host = "s3.us-east-1.amazonaws.com"
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	// 404/501 from S3 (bucket doesn't exist) is fine — the point is it's NOT 403 AccessDenied expiry.
	assert.NotEqual(t, http.StatusForbidden, w.Code)
}
