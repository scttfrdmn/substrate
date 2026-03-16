package substrate_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
)

// TestS3Versioning_EnableAndGetStatus enables versioning on a bucket and
// reads it back.
func TestS3Versioning_EnableAndGetStatus(t *testing.T) {
	srv, _ := newS3TestServer(t)

	// Create bucket.
	s3Request(t, srv, http.MethodPut, "/v-bucket", nil, nil)

	// PUT versioning configuration.
	body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
	w := s3Request(t, srv, http.MethodPut, "/v-bucket?versioning", []byte(body), map[string]string{
		"Content-Type": "application/xml",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("PutBucketVersioning: got %d, body: %s", w.Code, w.Body.String())
	}

	// GET versioning status.
	w = s3Request(t, srv, http.MethodGet, "/v-bucket?versioning", nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GetBucketVersioning: got %d, body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Enabled") {
		t.Errorf("expected Enabled in GetBucketVersioning response, got: %s", w.Body.String())
	}
}

// TestS3Versioning_PutCreatesVersionID verifies that putting an object on a
// versioned bucket returns an x-amz-version-id header.
func TestS3Versioning_PutCreatesVersionID(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/v-bucket", nil, nil)

	// Enable versioning.
	body := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	s3Request(t, srv, http.MethodPut, "/v-bucket?versioning", []byte(body), map[string]string{
		"Content-Type": "application/xml",
	})

	// Put object — expect version ID header.
	w := s3Request(t, srv, http.MethodPut, "/v-bucket/key.txt", []byte("version1"), nil)
	if w.Code != http.StatusOK {
		t.Fatalf("PutObject: got %d", w.Code)
	}
	vid := w.Header().Get("X-Amz-Version-Id")
	if vid == "" {
		t.Fatal("expected X-Amz-Version-Id header on versioned put")
	}

	// Put again — should produce a different version ID.
	w2 := s3Request(t, srv, http.MethodPut, "/v-bucket/key.txt", []byte("version2"), nil)
	vid2 := w2.Header().Get("X-Amz-Version-Id")
	if vid2 == "" || vid2 == vid {
		t.Fatalf("expected distinct X-Amz-Version-Id on second put, got first=%s second=%s", vid, vid2)
	}

	// GET specific version.
	w3 := s3Request(t, srv, http.MethodGet, "/v-bucket/key.txt?versionId="+vid, nil, nil)
	if w3.Code != http.StatusOK {
		t.Fatalf("GetObject(versionId=%s): got %d", vid, w3.Code)
	}
	if w3.Body.String() != "version1" {
		t.Errorf("expected version1 body, got %q", w3.Body.String())
	}

	// GET without versionId → latest.
	w4 := s3Request(t, srv, http.MethodGet, "/v-bucket/key.txt", nil, nil)
	if w4.Body.String() != "version2" {
		t.Errorf("expected version2 as latest, got %q", w4.Body.String())
	}
}

// TestS3Versioning_DeleteMarker verifies that deleting a versioned object
// creates a delete marker rather than removing the object permanently.
func TestS3Versioning_DeleteMarker(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/v-bucket", nil, nil)

	// Enable versioning.
	body := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	s3Request(t, srv, http.MethodPut, "/v-bucket?versioning", []byte(body), map[string]string{
		"Content-Type": "application/xml",
	})

	// Put an object.
	w := s3Request(t, srv, http.MethodPut, "/v-bucket/key.txt", []byte("hello"), nil)
	vid := w.Header().Get("X-Amz-Version-Id")

	// Delete without versionId → creates delete marker.
	wd := s3Request(t, srv, http.MethodDelete, "/v-bucket/key.txt", nil, nil)
	if wd.Code != http.StatusNoContent {
		t.Fatalf("delete: got %d", wd.Code)
	}

	// GET without versionId → 404 (delete marker is current).
	wg := s3Request(t, srv, http.MethodGet, "/v-bucket/key.txt", nil, nil)
	if wg.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete marker, got %d", wg.Code)
	}

	// GET specific original version → still accessible.
	wv := s3Request(t, srv, http.MethodGet, "/v-bucket/key.txt?versionId="+vid, nil, nil)
	if wv.Code != http.StatusOK {
		t.Fatalf("versioned get after delete: got %d", wv.Code)
	}
	if wv.Body.String() != "hello" {
		t.Errorf("expected 'hello', got %q", wv.Body.String())
	}
}

// TestS3Versioning_ListObjectVersions lists all versions and delete markers.
func TestS3Versioning_ListObjectVersions(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/v-bucket", nil, nil)

	// Enable versioning.
	body := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	s3Request(t, srv, http.MethodPut, "/v-bucket?versioning", []byte(body), map[string]string{
		"Content-Type": "application/xml",
	})

	// Two versions of the same key.
	s3Request(t, srv, http.MethodPut, "/v-bucket/k", []byte("v1"), nil)
	s3Request(t, srv, http.MethodPut, "/v-bucket/k", []byte("v2"), nil)

	// List versions.
	wl := s3Request(t, srv, http.MethodGet, "/v-bucket?versions", nil, nil)
	if wl.Code != http.StatusOK {
		t.Fatalf("ListObjectVersions: got %d body: %s", wl.Code, wl.Body.String())
	}

	type listVersionsResult struct {
		XMLName  xml.Name `xml:"ListVersionsResult"`
		Versions []struct {
			Key       string `xml:"Key"`
			VersionID string `xml:"VersionId"`
		} `xml:"Version"`
	}
	var result listVersionsResult
	if err := xml.Unmarshal(wl.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal ListVersionsResult: %v\nbody: %s", err, wl.Body.String())
	}
	if len(result.Versions) < 2 {
		t.Errorf("expected at least 2 versions, got %d", len(result.Versions))
	}
}

// TestS3Versioning_SuspendVersioning verifies that versioning can be suspended.
func TestS3Versioning_SuspendVersioning(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/sv-bucket", nil, nil)

	// Enable then suspend.
	s3Request(t, srv, http.MethodPut, "/sv-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})

	w := s3Request(t, srv, http.MethodPut, "/sv-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})
	if w.Code != http.StatusOK {
		t.Fatalf("suspend versioning: %d body: %s", w.Code, w.Body.String())
	}

	// GET should return Suspended.
	wg := s3Request(t, srv, http.MethodGet, "/sv-bucket?versioning", nil, nil)
	if !strings.Contains(wg.Body.String(), "Suspended") {
		t.Errorf("expected Suspended, got: %s", wg.Body.String())
	}
}

// TestS3Versioning_ListObjectVersions_WithDeleteMarker verifies that delete
// markers appear in the ListObjectVersions response.
func TestS3Versioning_ListObjectVersions_WithDeleteMarker(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/dm-bucket", nil, nil)
	s3Request(t, srv, http.MethodPut, "/dm-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})

	// Put then delete (creates delete marker).
	s3Request(t, srv, http.MethodPut, "/dm-bucket/k", []byte("content"), nil)
	s3Request(t, srv, http.MethodDelete, "/dm-bucket/k", nil, nil)

	wl := s3Request(t, srv, http.MethodGet, "/dm-bucket?versions", nil, nil)
	if wl.Code != http.StatusOK {
		t.Fatalf("ListObjectVersions: %d body: %s", wl.Code, wl.Body.String())
	}
	// Should contain both a Version and a DeleteMarker.
	body := wl.Body.String()
	if !strings.Contains(body, "<Version>") {
		t.Error("expected <Version> in list versions response")
	}
	if !strings.Contains(body, "<DeleteMarker>") {
		t.Error("expected <DeleteMarker> in list versions response")
	}
}

// TestS3Versioning_ListObjectVersions_NonVersioned verifies that objects
// created before versioning was enabled also appear in the version list.
func TestS3Versioning_ListObjectVersions_NonVersioned(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/nv-bucket", nil, nil)

	// Put object WITHOUT versioning.
	s3Request(t, srv, http.MethodPut, "/nv-bucket/obj", []byte("data"), nil)

	// Now enable versioning.
	s3Request(t, srv, http.MethodPut, "/nv-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})

	wl := s3Request(t, srv, http.MethodGet, "/nv-bucket?versions", nil, nil)
	if wl.Code != http.StatusOK {
		t.Fatalf("ListObjectVersions: %d", wl.Code)
	}
	if !strings.Contains(wl.Body.String(), "obj") {
		t.Error("expected pre-versioning object in list")
	}
}

// TestS3Versioning_InvalidStatus verifies that an invalid versioning status
// returns a 400 error.
func TestS3Versioning_InvalidStatus(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/bad-status-bucket", nil, nil)
	w := s3Request(t, srv, http.MethodPut, "/bad-status-bucket?versioning",
		[]byte(`<VersioningConfiguration><Status>INVALID</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid status, got %d body: %s", w.Code, w.Body.String())
	}
}

// TestS3Versioning_PutOnNonexistentBucket returns 404.
func TestS3Versioning_PutOnNonexistentBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodPut, "/nonexistent?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
		map[string]string{"Content-Type": "application/xml"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent bucket, got %d body: %s", w.Code, w.Body.String())
	}
}

// TestS3Versioning_GetOnNonexistentBucket returns 404.
func TestS3Versioning_GetOnNonexistentBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodGet, "/nonexistent?versioning", nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent bucket, got %d", w.Code)
	}
}

// TestS3Lifecycle_NonexistentBucket tests lifecycle ops on a missing bucket.
func TestS3Lifecycle_NonexistentBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)

	wp := s3Request(t, srv, http.MethodPut, "/nonexistent?lifecycle",
		[]byte(`<LifecycleConfiguration/>`),
		map[string]string{"Content-Type": "application/xml"})
	if wp.Code != http.StatusNotFound {
		t.Fatalf("put lifecycle on nonexistent: %d", wp.Code)
	}

	wg := s3Request(t, srv, http.MethodGet, "/nonexistent?lifecycle", nil, nil)
	if wg.Code != http.StatusNotFound {
		t.Fatalf("get lifecycle on nonexistent: %d", wg.Code)
	}

	wd := s3Request(t, srv, http.MethodDelete, "/nonexistent?lifecycle", nil, nil)
	if wd.Code != http.StatusNotFound {
		t.Fatalf("delete lifecycle on nonexistent: %d", wd.Code)
	}
}

// TestS3Versioning_ListObjectVersions_NonexistentBucket returns 404.
func TestS3Versioning_ListObjectVersions_NonexistentBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodGet, "/nonexistent?versions", nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent bucket, got %d", w.Code)
	}
}

// TestS3Lifecycle_RoundTrip verifies that lifecycle configurations can be
// stored and retrieved.
func TestS3Lifecycle_RoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	s3Request(t, srv, http.MethodPut, "/lc-bucket", nil, nil)

	// GET before set → 404.
	wg := s3Request(t, srv, http.MethodGet, "/lc-bucket?lifecycle", nil, nil)
	if wg.Code != http.StatusNotFound {
		t.Fatalf("expected 404 before lifecycle set, got %d", wg.Code)
	}

	// PUT lifecycle config.
	lc := `<LifecycleConfiguration><Rule><ID>expire-old</ID><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`
	wp := s3Request(t, srv, http.MethodPut, "/lc-bucket?lifecycle", []byte(lc), map[string]string{
		"Content-Type": "application/xml",
	})
	if wp.Code != http.StatusOK {
		t.Fatalf("PutBucketLifecycleConfiguration: got %d body: %s", wp.Code, wp.Body.String())
	}

	// GET lifecycle config back.
	wg2 := s3Request(t, srv, http.MethodGet, "/lc-bucket?lifecycle", nil, nil)
	if wg2.Code != http.StatusOK {
		t.Fatalf("GetBucketLifecycleConfiguration: got %d", wg2.Code)
	}
	if !strings.Contains(wg2.Body.String(), "expire-old") {
		t.Errorf("expected rule ID in response, got: %s", wg2.Body.String())
	}

	// DELETE lifecycle config.
	wd := s3Request(t, srv, http.MethodDelete, "/lc-bucket?lifecycle", nil, nil)
	if wd.Code != http.StatusNoContent {
		t.Fatalf("DeleteBucketLifecycle: got %d", wd.Code)
	}

	// GET after delete → 404.
	wg3 := s3Request(t, srv, http.MethodGet, "/lc-bucket?lifecycle", nil, nil)
	if wg3.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after lifecycle delete, got %d body: %s", wg3.Code, wg3.Body.String())
	}
}
