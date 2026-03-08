package substrate_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newS3TestServer creates a Server with an S3Plugin backed by an in-memory
// afero filesystem. It returns the server and the filesystem for direct inspection.
func newS3TestServer(t *testing.T) (*substrate.Server, afero.Fs) {
	t.Helper()
	cfg := substrate.DefaultConfig()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	fs := afero.NewMemMapFs()

	s3p := &substrate.S3Plugin{}
	err := s3p.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	})
	require.NoError(t, err)

	registry := substrate.NewPluginRegistry()
	registry.Register(s3p)

	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})

	costCtrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	srv := substrate.NewServer(
		*cfg,
		registry,
		store,
		state,
		tc,
		logger,
		substrate.ServerOptions{Costs: costCtrl},
	)

	return srv, fs
}

// s3Request is a helper for issuing HTTP requests to the S3 server and
// returning the response recorder.
func s3Request(t *testing.T, srv *substrate.Server, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	r := httptest.NewRequest(method, "http://s3.amazonaws.com"+path, bodyReader)
	r.Host = "s3.amazonaws.com"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w
}

// s3VirtualHostRequest issues an HTTP request using virtual-hosted-style S3 addressing.
func s3VirtualHostRequest(t *testing.T, srv *substrate.Server, method, bucket, keyPath string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	host := bucket + ".s3.amazonaws.com"
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	url := "http://" + host + keyPath
	r := httptest.NewRequest(method, url, bodyReader)
	r.Host = host
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w
}

// --- Bucket CRUD ---

func TestS3_CreateBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestS3_CreateBucket_InvalidName(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodPut, "/UPPERCASE", nil, nil)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "InvalidBucketName")
}

func TestS3_CreateBucket_AlreadyExists(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "BucketAlreadyExists")
}

func TestS3_HeadBucket_Exists(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodHead, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestS3_HeadBucket_NotFound(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodHead, "/no-such-bucket", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestS3_DeleteBucket_Empty(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodDelete, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestS3_DeleteBucket_NotEmpty(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/my-bucket/key.txt", []byte("hello"), nil)
	w := s3Request(t, srv, http.MethodDelete, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "BucketNotEmpty")
}

func TestS3_ListBuckets(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/bucket-a", nil, nil)
	s3Request(t, srv, http.MethodPut, "/bucket-b", nil, nil)

	w := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "bucket-a")
	assert.Contains(t, body, "bucket-b")
}

// --- Object CRUD ---

func TestS3_PutObject_GetObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)

	content := []byte("hello world")
	pw := s3Request(t, srv, http.MethodPut, "/my-bucket/hello.txt", content,
		map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, pw.Code)
	assert.NotEmpty(t, pw.Header().Get("ETag"))

	gw := s3Request(t, srv, http.MethodGet, "/my-bucket/hello.txt", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, content, gw.Body.Bytes())
	assert.Equal(t, "text/plain", gw.Header().Get("Content-Type"))
}

func TestS3_GetObject_NoSuchKey(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodGet, "/my-bucket/missing.txt", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "NoSuchKey")
}

func TestS3_GetObject_NoSuchBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodGet, "/no-bucket/key", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestS3_HeadObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/my-bucket/obj.bin", []byte("data"), nil)

	w := s3Request(t, srv, http.MethodHead, "/my-bucket/obj.bin", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "4", w.Header().Get("Content-Length"))
}

func TestS3_DeleteObject_Idempotent(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)

	// Delete a non-existent key — should still return 204.
	w := s3Request(t, srv, http.MethodDelete, "/my-bucket/ghost.txt", nil, nil)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

func TestS3_DeleteObject_ThenBucketEmpty(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/my-bucket/key.txt", []byte("data"), nil)
	s3Request(t, srv, http.MethodDelete, "/my-bucket/key.txt", nil, nil)
	w := s3Request(t, srv, http.MethodDelete, "/my-bucket", nil, nil)
	assert.Equal(t, http.StatusNoContent, w.Code)
}

// --- User metadata round-trip ---

func TestS3_UserMetadata_RoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)

	s3Request(t, srv, http.MethodPut, "/my-bucket/obj.txt", []byte("hi"),
		map[string]string{
			"Content-Type":      "text/plain",
			"X-Amz-Meta-Author": "Alice",
			"X-Amz-Meta-Tag":    "test",
		})

	w := s3Request(t, srv, http.MethodGet, "/my-bucket/obj.txt", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Alice", w.Header().Get("X-Amz-Meta-Author"))
	assert.Equal(t, "test", w.Header().Get("X-Amz-Meta-Tag"))
}

// --- CopyObject ---

func TestS3_CopyObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/src-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/dst-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/src-bucket/original.txt", []byte("content"), nil)

	w := s3Request(t, srv, http.MethodPut, "/dst-bucket/copy.txt", nil,
		map[string]string{"X-Amz-Copy-Source": "/src-bucket/original.txt"})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "CopyObjectResult")

	// Verify copy is readable.
	gw := s3Request(t, srv, http.MethodGet, "/dst-bucket/copy.txt", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, []byte("content"), gw.Body.Bytes())
}

// --- ListObjects / ListObjectsV2 ---

func TestS3_ListObjects(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	for _, k := range []string{"a.txt", "b.txt", "c.txt"} {
		s3Request(t, srv, http.MethodPut, "/my-bucket/"+k, []byte(k), nil)
	}

	w := s3Request(t, srv, http.MethodGet, "/my-bucket", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "a.txt")
	assert.Contains(t, body, "b.txt")
	assert.Contains(t, body, "c.txt")
}

func TestS3_ListObjectsV2_Pagination(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	for i := range 5 {
		key := fmt.Sprintf("file%02d.txt", i)
		s3Request(t, srv, http.MethodPut, "/my-bucket/"+key, []byte("x"), nil)
	}

	// First page: max-keys=3.
	w1 := s3Request(t, srv, http.MethodGet, "/my-bucket?list-type=2&max-keys=3", nil, nil)
	require.Equal(t, http.StatusOK, w1.Code)
	assert.Contains(t, w1.Body.String(), "true") // IsTruncated

	// Extract NextContinuationToken from first response.
	type listResult struct {
		NextContinuationToken string `xml:"NextContinuationToken"`
	}
	var r1 listResult
	require.NoError(t, xml.NewDecoder(strings.NewReader(w1.Body.String())).Decode(&r1))
	require.NotEmpty(t, r1.NextContinuationToken)

	// Second page.
	w2 := s3Request(t, srv, http.MethodGet,
		"/my-bucket?list-type=2&max-keys=3&continuation-token="+r1.NextContinuationToken,
		nil, nil)
	require.Equal(t, http.StatusOK, w2.Code)
	assert.Contains(t, w2.Body.String(), "false") // IsTruncated
}

func TestS3_ListObjectsV2_Prefix(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/my-bucket/logs/app.log", []byte("l"), nil)
	s3Request(t, srv, http.MethodPut, "/my-bucket/data/file.csv", []byte("d"), nil)

	w := s3Request(t, srv, http.MethodGet, "/my-bucket?list-type=2&prefix=logs/", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "logs/app.log")
	assert.NotContains(t, body, "data/file.csv")
}

// --- Multipart upload ---

func TestS3_MultipartUpload_Complete(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)

	// Initiate.
	iw := s3Request(t, srv, http.MethodPost, "/my-bucket/bigfile.bin?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)

	type initiateResult struct {
		UploadId string `xml:"UploadId"` //nolint:revive
	}
	var ir initiateResult
	require.NoError(t, xml.NewDecoder(strings.NewReader(iw.Body.String())).Decode(&ir))
	require.NotEmpty(t, ir.UploadId)

	uploadID := ir.UploadId

	// Upload two parts.
	for i, part := range [][]byte{[]byte("part one data"), []byte("part two data")} {
		uw := s3Request(t, srv, http.MethodPut,
			fmt.Sprintf("/my-bucket/bigfile.bin?partNumber=%d&uploadId=%s", i+1, uploadID),
			part, nil)
		require.Equal(t, http.StatusOK, uw.Code)
	}

	// List multipart uploads.
	lw := s3Request(t, srv, http.MethodGet, "/my-bucket?uploads", nil, nil)
	require.Equal(t, http.StatusOK, lw.Code)
	assert.Contains(t, lw.Body.String(), uploadID)

	// Complete.
	completeBody := `<CompleteMultipartUpload>` +
		`<Part><PartNumber>1</PartNumber><ETag>e1</ETag></Part>` +
		`<Part><PartNumber>2</PartNumber><ETag>e2</ETag></Part>` +
		`</CompleteMultipartUpload>`
	cw := s3Request(t, srv, http.MethodPost,
		"/my-bucket/bigfile.bin?uploadId="+uploadID,
		[]byte(completeBody), nil)
	require.Equal(t, http.StatusOK, cw.Code)
	assert.Contains(t, cw.Body.String(), "CompleteMultipartUploadResult")

	// Verify assembled object is readable.
	gw := s3Request(t, srv, http.MethodGet, "/my-bucket/bigfile.bin", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, []byte("part one datapart two data"), gw.Body.Bytes())
}

func TestS3_MultipartUpload_Abort(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)

	iw := s3Request(t, srv, http.MethodPost, "/my-bucket/obj.bin?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)

	type initiateResult struct {
		UploadId string `xml:"UploadId"` //nolint:revive
	}
	var ir initiateResult
	require.NoError(t, xml.NewDecoder(strings.NewReader(iw.Body.String())).Decode(&ir))

	uploadID := ir.UploadId
	s3Request(t, srv, http.MethodPut,
		fmt.Sprintf("/my-bucket/obj.bin?partNumber=1&uploadId=%s", uploadID),
		[]byte("data"), nil)

	aw := s3Request(t, srv, http.MethodDelete,
		"/my-bucket/obj.bin?uploadId="+uploadID, nil, nil)
	assert.Equal(t, http.StatusNoContent, aw.Code)

	// Object should not exist.
	gw := s3Request(t, srv, http.MethodGet, "/my-bucket/obj.bin", nil, nil)
	assert.Equal(t, http.StatusNotFound, gw.Code)
}

func TestS3_MultipartUpload_NoSuchUpload(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/my-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodPost, "/my-bucket/obj.bin?uploadId=does-not-exist", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "NoSuchUpload")
}

// --- Virtual-hosted routing ---

func TestS3_VirtualHostedPutGet(t *testing.T) {
	srv, _ := newS3TestServer(t)

	// Create bucket via path-style.
	s3Request(t, srv, http.MethodPut, "/vhost-bucket", nil, nil)

	// Put via virtual-hosted.
	pw := s3VirtualHostRequest(t, srv, http.MethodPut, "vhost-bucket", "/hello.txt",
		[]byte("virtual"), nil)
	require.Equal(t, http.StatusOK, pw.Code)

	// Get via path-style — should see the object.
	gw := s3Request(t, srv, http.MethodGet, "/vhost-bucket/hello.txt", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, []byte("virtual"), gw.Body.Bytes())
}

// --- Cost tracking ---

func TestS3_PutObject_CostTracked(t *testing.T) {
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fs := afero.NewMemMapFs()
	cfg := substrate.DefaultConfig()

	s3p := &substrate.S3Plugin{}
	require.NoError(t, s3p.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	}))

	registry := substrate.NewPluginRegistry()
	registry.Register(s3p)

	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	costCtrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Costs: costCtrl})

	// Create bucket then put an object.
	s3Request(t, srv, http.MethodPut, "/cost-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/cost-bucket/obj.txt", []byte("hello"), nil)

	// Retrieve cost summary.
	summary, err := store.GetCostSummary(context.Background(), "000000000000", time.Time{}, time.Time{})
	require.NoError(t, err)
	// PutObject costs $0.000005.
	assert.InDelta(t, 0.000005, summary.ByOperation["s3/PutObject"], 1e-9)
}
