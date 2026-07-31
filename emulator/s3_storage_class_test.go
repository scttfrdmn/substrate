package emulator_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// storageClassFixture creates a bucket and returns the server.
func storageClassFixture(t *testing.T, bucket string) *emulator.Server {
	t.Helper()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")
	return srv
}

// listedStorageClass returns the <StorageClass> ListObjectsV2 reports for a key.
func listedStorageClass(t *testing.T, srv *emulator.Server, bucket, key string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "list objects v2")

	var result struct {
		Contents []struct {
			Key          string `xml:"Key"`
			StorageClass string `xml:"StorageClass"`
		} `xml:"Contents"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))

	for _, c := range result.Contents {
		if c.Key == key {
			return c.StorageClass
		}
	}
	t.Fatalf("key %q absent from ListObjectsV2: %s", key, w.Body.String())
	return ""
}

// TestS3_StorageClass_RoundTrip covers every storage class S3 documents: the value
// PutObject records must come back on HEAD and in ListObjectsV2, and the
// x-amz-storage-class response header must be omitted for STANDARD only.
func TestS3_StorageClass_RoundTrip(t *testing.T) {
	// The full documented enumeration, so a valid value is never rejected.
	classes := []string{
		"STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA",
		"INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "OUTPOSTS",
		"GLACIER_IR", "SNOW", "EXPRESS_ONEZONE", "FSX_OPENZFS", "FSX_ONTAP",
	}

	for _, class := range classes {
		t.Run(class, func(t *testing.T) {
			srv := storageClassFixture(t, "sc-bucket")
			key := "obj-" + class

			pw := s3Request(t, srv, http.MethodPut, "/sc-bucket/"+key, []byte("payload"),
				map[string]string{"x-amz-storage-class": class})
			require.Equal(t, http.StatusOK, pw.Code, "put with storage class %s", class)

			// HEAD reports the class even for the archival tiers: object metadata
			// stays available regardless of where the body lives.
			hw := s3Request(t, srv, http.MethodHead, "/sc-bucket/"+key, nil, nil)
			require.Equal(t, http.StatusOK, hw.Code, "head %s", class)

			if class == "STANDARD" {
				assert.Empty(t, hw.Header().Get("x-amz-storage-class"),
					"S3 returns x-amz-storage-class for all objects except STANDARD")
			} else {
				assert.Equal(t, class, hw.Header().Get("x-amz-storage-class"))
			}

			// Unlike the header, the XML element is emitted for every class.
			assert.Equal(t, class, listedStorageClass(t, srv, "sc-bucket", key))
		})
	}
}

// TestS3_StorageClass_DefaultsToStandard asserts a write with no
// x-amz-storage-class records STANDARD, not the empty string.
func TestS3_StorageClass_DefaultsToStandard(t *testing.T) {
	srv := storageClassFixture(t, "sc-default")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-default/plain", []byte("body"), nil).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sc-default/plain", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Empty(t, hw.Header().Get("x-amz-storage-class"))

	assert.Equal(t, "STANDARD", listedStorageClass(t, srv, "sc-default", "plain"),
		"an absent header means STANDARD, and the list element is never blank")
}

// TestS3_StorageClass_Invalid asserts an unrecognized class is a 400
// InvalidStorageClass, and that the rejected write leaves nothing behind.
func TestS3_StorageClass_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"unknown", "TURBO_FROZEN"},
		{"lowercase", "standard"},
		{"typo", "GLACIER_INSTANT"},
		{"whitespace", " STANDARD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := storageClassFixture(t, "sc-invalid")

			w := s3Request(t, srv, http.MethodPut, "/sc-invalid/obj", []byte("body"),
				map[string]string{"x-amz-storage-class": tt.value})
			require.Equal(t, http.StatusBadRequest, w.Code)

			body := parseS3Error(t, w.Body.Bytes())
			assert.Equal(t, "InvalidStorageClass", body.Code)
			assert.Equal(t, "The storage class you specified is not valid.", body.Message)

			// Validation runs before any write, so the key must not exist.
			assert.Equal(t, http.StatusNotFound,
				s3Request(t, srv, http.MethodHead, "/sc-invalid/obj", nil, nil).Code,
				"a rejected storage class must not create the object")
		})
	}
}

// TestS3_StorageClass_ArchivedGetIsInvalidObjectState covers the read gate: the two
// archival classes fail a GET until restored, and GLACIER_IR — instant retrieval —
// does not. Conflating the two is the mistake this test exists to catch.
func TestS3_StorageClass_ArchivedGetIsInvalidObjectState(t *testing.T) {
	tests := []struct {
		class      string
		wantStatus int
	}{
		{"GLACIER", http.StatusForbidden},
		{"DEEP_ARCHIVE", http.StatusForbidden},
		{"GLACIER_IR", http.StatusOK},
		{"STANDARD", http.StatusOK},
		{"STANDARD_IA", http.StatusOK},
		{"INTELLIGENT_TIERING", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.class, func(t *testing.T) {
			srv := storageClassFixture(t, "sc-archive")
			key := "obj-" + tt.class

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sc-archive/"+key, []byte("payload"),
					map[string]string{"x-amz-storage-class": tt.class}).Code)

			gw := s3Request(t, srv, http.MethodGet, "/sc-archive/"+key, nil, nil)
			assert.Equal(t, tt.wantStatus, gw.Code)

			if tt.wantStatus == http.StatusForbidden {
				body := parseS3Error(t, gw.Body.Bytes())
				assert.Equal(t, "InvalidObjectState", body.Code)
				assert.Equal(t, "The action is not valid for the object's storage class", body.Message)
			} else {
				assert.Equal(t, "payload", gw.Body.String())
			}
		})
	}
}

// TestS3_StorageClass_ArchivedHeadSucceeds pins the deliberate asymmetry: HeadObject
// documents no InvalidObjectState, because "even if the object is stored in S3
// Glacier, all object metadata is still available". HEAD is therefore how a consumer
// discovers that a GET needs a restore first.
func TestS3_StorageClass_ArchivedHeadSucceeds(t *testing.T) {
	for _, class := range []string{"GLACIER", "DEEP_ARCHIVE"} {
		t.Run(class, func(t *testing.T) {
			srv := storageClassFixture(t, "sc-head")
			key := "obj-" + class

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sc-head/"+key, []byte("payload"),
					map[string]string{"x-amz-storage-class": class}).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sc-head/"+key, nil, nil)
			require.Equal(t, http.StatusOK, hw.Code, "HEAD of an archived object is a 200")
			assert.Equal(t, class, hw.Header().Get("x-amz-storage-class"))
			assert.Equal(t, "7", hw.Header().Get("Content-Length"),
				"metadata, including the size, stays available")
		})
	}
}

// TestS3_StorageClass_ArchivedRangedGet asserts the archival gate precedes the range
// step: an archived object has no readable bytes, so a ranged GET is the same 403 as
// an unranged one rather than a 206.
func TestS3_StorageClass_ArchivedRangedGet(t *testing.T) {
	srv := storageClassFixture(t, "sc-range")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-range/frozen", []byte("0123456789"),
			map[string]string{"x-amz-storage-class": "GLACIER"}).Code)

	w := s3Request(t, srv, http.MethodGet, "/sc-range/frozen", nil,
		map[string]string{"Range": "bytes=0-3"})
	require.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "InvalidObjectState", parseS3Error(t, w.Body.Bytes()).Code)
}

// TestS3_StorageClass_MultipartCarriesClass asserts the class supplied at
// CreateMultipartUpload survives to the assembled object — the header is accepted
// there, not on CompleteMultipartUpload.
func TestS3_StorageClass_MultipartCarriesClass(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-mpu", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sc-mpu/big?uploads", nil,
		map[string]string{"x-amz-storage-class": "STANDARD_IA"})
	require.Equal(t, http.StatusOK, iw.Code)

	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	// ListMultipartUploads reports the in-progress upload's class.
	lw := s3Request(t, srv, http.MethodGet, "/sc-mpu?uploads", nil, nil)
	require.Equal(t, http.StatusOK, lw.Code)
	var lr struct {
		Uploads []struct {
			StorageClass string `xml:"StorageClass"`
		} `xml:"Upload"`
	}
	require.NoError(t, xml.Unmarshal(lw.Body.Bytes(), &lr))
	require.Len(t, lr.Uploads, 1)
	assert.Equal(t, "STANDARD_IA", lr.Uploads[0].StorageClass)

	etag := uploadPart(t, srv, "sc-mpu", "big", ir.UploadID, 1, []byte("single part is exempt from the 5 MB floor"))
	cw := s3Request(t, srv, http.MethodPost, "/sc-mpu/big?uploadId="+ir.UploadID,
		completeBody(etag), nil)
	require.Equal(t, http.StatusOK, cw.Code, "complete multipart upload")

	hw := s3Request(t, srv, http.MethodHead, "/sc-mpu/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "STANDARD_IA", hw.Header().Get("x-amz-storage-class"))
}

// TestS3_StorageClass_MultipartInvalidClass asserts CreateMultipartUpload validates
// the class up front rather than failing at Complete, after parts have been paid for.
func TestS3_StorageClass_MultipartInvalidClass(t *testing.T) {
	srv := storageClassFixture(t, "sc-mpu-bad")

	w := s3Request(t, srv, http.MethodPost, "/sc-mpu-bad/big?uploads", nil,
		map[string]string{"x-amz-storage-class": "NOPE"})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "InvalidStorageClass", parseS3Error(t, w.Body.Bytes()).Code)
}

// TestS3_StorageClass_ListObjectVersions asserts each <Version> carries a
// <StorageClass>, while a <DeleteMarker> does not — matching the response shape S3
// documents.
func TestS3_StorageClass_ListObjectVersions(t *testing.T) {
	srv := storageClassFixture(t, "sc-versions")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-versions?versioning",
			[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil).Code)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-versions/obj", []byte("v1"),
			map[string]string{"x-amz-storage-class": "STANDARD_IA"}).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-versions/obj", []byte("v2"),
			map[string]string{"x-amz-storage-class": "GLACIER_IR"}).Code)
	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/sc-versions/obj", nil, nil).Code)

	w := s3Request(t, srv, http.MethodGet, "/sc-versions?versions", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)

	var result struct {
		Versions []struct {
			VersionID    string `xml:"VersionId"`
			StorageClass string `xml:"StorageClass"`
		} `xml:"Version"`
		DeleteMarkers []struct {
			VersionID string `xml:"VersionId"`
			// Decoded only to assert it is never emitted: the DeleteMarkerEntry
			// shape S3 documents has no StorageClass child.
			StorageClass string `xml:"StorageClass"`
		} `xml:"DeleteMarker"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))

	require.Len(t, result.Versions, 2)
	classes := []string{result.Versions[0].StorageClass, result.Versions[1].StorageClass}
	assert.ElementsMatch(t, []string{"STANDARD_IA", "GLACIER_IR"}, classes,
		"each version reports the class it was written with")

	require.Len(t, result.DeleteMarkers, 1)
	assert.Empty(t, result.DeleteMarkers[0].StorageClass,
		"a <DeleteMarker> carries no <StorageClass>")
}

// TestS3_StorageClass_ListObjectsV1 asserts ListObjects v1 emits <StorageClass> too,
// since both list operations share one entry type.
func TestS3_StorageClass_ListObjectsV1(t *testing.T) {
	srv := storageClassFixture(t, "sc-v1")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sc-v1/obj", []byte("body"),
			map[string]string{"x-amz-storage-class": "ONEZONE_IA"}).Code)

	w := s3Request(t, srv, http.MethodGet, "/sc-v1", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)

	var result struct {
		Contents []struct {
			Key          string `xml:"Key"`
			StorageClass string `xml:"StorageClass"`
		} `xml:"Contents"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result.Contents, 1)
	assert.Equal(t, "ONEZONE_IA", result.Contents[0].StorageClass)
}

// --- CopyObject storage class ---

// TestS3_CopyObject_StorageClass asserts the copy's class comes from the request and
// is never inherited from the source: "if the x-amz-storage-class header is not used,
// the copied object will be stored in the STANDARD Storage Class by default".
func TestS3_CopyObject_StorageClass(t *testing.T) {
	tests := []struct {
		name        string
		sourceClass string
		copyHeader  string
		wantClass   string
	}{
		{"unqualified copy resets to STANDARD", "STANDARD_IA", "", "STANDARD"},
		{"explicit class is recorded", "STANDARD", "GLACIER_IR", "GLACIER_IR"},
		{"in-place transition to archival", "STANDARD", "GLACIER", "GLACIER"},
		{"transition away from archival", "GLACIER_IR", "STANDARD", "STANDARD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := storageClassFixture(t, "cp-sc")

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/cp-sc/src", []byte("payload"),
					map[string]string{"x-amz-storage-class": tt.sourceClass}).Code)

			headers := map[string]string{"X-Amz-Copy-Source": "/cp-sc/src"}
			if tt.copyHeader != "" {
				headers["x-amz-storage-class"] = tt.copyHeader
			}
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/cp-sc/dst", nil, headers).Code, "copy")

			hw := s3Request(t, srv, http.MethodHead, "/cp-sc/dst", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assert.Equal(t, tt.wantClass, listedStorageClass(t, srv, "cp-sc", "dst"))
		})
	}
}

// TestS3_CopyObject_InPlaceTransition covers the tier-transition pattern directly: a
// CopyObject onto the object's own key changes its class, and the archival target is
// then unreadable by GET while still readable by HEAD.
func TestS3_CopyObject_InPlaceTransition(t *testing.T) {
	srv := storageClassFixture(t, "cp-inplace")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/cp-inplace/obj", []byte("payload"), nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodGet, "/cp-inplace/obj", nil, nil).Code)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/cp-inplace/obj", nil, map[string]string{
			"X-Amz-Copy-Source":   "/cp-inplace/obj",
			"x-amz-storage-class": "DEEP_ARCHIVE",
		}).Code, "in-place transition")

	gw := s3Request(t, srv, http.MethodGet, "/cp-inplace/obj", nil, nil)
	require.Equal(t, http.StatusForbidden, gw.Code, "the transitioned object is no longer readable")
	assert.Equal(t, "InvalidObjectState", parseS3Error(t, gw.Body.Bytes()).Code)

	hw := s3Request(t, srv, http.MethodHead, "/cp-inplace/obj", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "DEEP_ARCHIVE", hw.Header().Get("x-amz-storage-class"))
}

// TestS3_CopyObject_ArchivedSource asserts an archived source must be restored before
// it can be copied — the class gate applies to the read side of a copy too.
func TestS3_CopyObject_ArchivedSource(t *testing.T) {
	srv := storageClassFixture(t, "cp-frozen")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/cp-frozen/src", []byte("payload"),
			map[string]string{"x-amz-storage-class": "GLACIER"}).Code)

	w := s3Request(t, srv, http.MethodPut, "/cp-frozen/dst", nil, map[string]string{
		"X-Amz-Copy-Source":   "/cp-frozen/src",
		"x-amz-storage-class": "STANDARD",
	})
	require.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "InvalidObjectState", parseS3Error(t, w.Body.Bytes()).Code)

	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodHead, "/cp-frozen/dst", nil, nil).Code,
		"a rejected copy must leave no destination object")
}

// TestS3_CopyObject_InvalidStorageClass asserts a bad class on a copy is rejected
// before the destination is written.
func TestS3_CopyObject_InvalidStorageClass(t *testing.T) {
	srv := storageClassFixture(t, "cp-bad")

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/cp-bad/src", []byte("payload"), nil).Code)

	w := s3Request(t, srv, http.MethodPut, "/cp-bad/dst", nil, map[string]string{
		"X-Amz-Copy-Source":   "/cp-bad/src",
		"x-amz-storage-class": "ICEBOX",
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "InvalidStorageClass", parseS3Error(t, w.Body.Bytes()).Code)

	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodHead, "/cp-bad/dst", nil, nil).Code)
}

// --- CopyObject metadata directive ---

// TestS3_CopyObject_MetadataDirectiveCopy asserts the documented COPY behavior:
// "when you copy an object, user-controlled system metadata and user-defined metadata
// are also copied", and Content-Encoding is user-controlled. A COPY therefore
// preserves it, and headers restated on the request are *ignored* — only REPLACE
// reads them.
func TestS3_CopyObject_MetadataDirectiveCopy(t *testing.T) {
	tests := []struct {
		name      string
		directive string
	}{
		{"default (no directive header)", ""},
		{"explicit COPY", "COPY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := storageClassFixture(t, "cp-meta")

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/cp-meta/src", []byte("payload"),
					map[string]string{
						"Content-Type":     "text/plain",
						"Content-Encoding": "gzip",
						"x-amz-meta-owner": "alice",
					}).Code)

			headers := map[string]string{"X-Amz-Copy-Source": "/cp-meta/src"}
			if tt.directive != "" {
				headers["x-amz-metadata-directive"] = tt.directive
			}
			// Restated headers a COPY must ignore.
			headers["Content-Type"] = "application/json"
			headers["x-amz-meta-owner"] = "bob"

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/cp-meta/dst", nil, headers).Code)

			hw := s3Request(t, srv, http.MethodHead, "/cp-meta/dst", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assert.Equal(t, "gzip", hw.Header().Get("Content-Encoding"),
				"COPY preserves Content-Encoding")
			assert.Equal(t, "text/plain", hw.Header().Get("Content-Type"),
				"COPY takes the source's Content-Type, not the request's")
			assert.Equal(t, "alice", hw.Header().Get("X-Amz-Meta-Owner"),
				"COPY takes the source's user metadata, not the request's")
		})
	}
}

// TestS3_CopyObject_MetadataDirectiveReplace asserts REPLACE drops everything the
// request does not restate: "you must explicitly specify all of the user-configurable
// metadata present on the source object in your request, even if you are changing
// only one of the metadata values." This is the asymmetry an in-place tier transition
// trips over — a REPLACE that omits Content-Encoding loses it.
func TestS3_CopyObject_MetadataDirectiveReplace(t *testing.T) {
	t.Run("omitted metadata is dropped", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-repl")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-repl/src", []byte("payload"),
				map[string]string{
					"Content-Type":     "text/plain",
					"Content-Encoding": "gzip",
					"x-amz-meta-owner": "alice",
				}).Code)

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-repl/dst", nil, map[string]string{
				"X-Amz-Copy-Source":        "/cp-repl/src",
				"x-amz-metadata-directive": "REPLACE",
			}).Code)

		hw := s3Request(t, srv, http.MethodHead, "/cp-repl/dst", nil, nil)
		require.Equal(t, http.StatusOK, hw.Code)
		assert.Empty(t, hw.Header().Get("Content-Encoding"),
			"REPLACE drops a Content-Encoding the request did not restate")
		assert.Equal(t, "application/octet-stream", hw.Header().Get("Content-Type"),
			"REPLACE falls back to the default Content-Type")
		assert.Empty(t, hw.Header().Get("X-Amz-Meta-Owner"),
			"REPLACE drops user metadata the request did not restate")
	})

	t.Run("restated metadata is kept", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-repl2")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-repl2/src", []byte("payload"),
				map[string]string{
					"Content-Type":     "text/plain",
					"Content-Encoding": "gzip",
					"x-amz-meta-owner": "alice",
				}).Code)

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-repl2/dst", nil, map[string]string{
				"X-Amz-Copy-Source":        "/cp-repl2/src",
				"x-amz-metadata-directive": "REPLACE",
				"Content-Type":             "application/json",
				"Content-Encoding":         "br",
				"x-amz-meta-owner":         "bob",
			}).Code)

		hw := s3Request(t, srv, http.MethodHead, "/cp-repl2/dst", nil, nil)
		require.Equal(t, http.StatusOK, hw.Code)
		assert.Equal(t, "br", hw.Header().Get("Content-Encoding"))
		assert.Equal(t, "application/json", hw.Header().Get("Content-Type"))
		assert.Equal(t, "bob", hw.Header().Get("X-Amz-Meta-Owner"))
	})
}

// TestS3_CopyObject_MetadataDirectiveInvalid asserts an unrecognized directive is
// rejected rather than silently defaulting to COPY. A typo that quietly preserved
// metadata is exactly the false success this emulator exists to surface.
func TestS3_CopyObject_MetadataDirectiveInvalid(t *testing.T) {
	for _, header := range []string{"x-amz-metadata-directive", "x-amz-tagging-directive"} {
		t.Run(header, func(t *testing.T) {
			srv := storageClassFixture(t, "cp-dir")

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/cp-dir/src", []byte("payload"), nil).Code)

			w := s3Request(t, srv, http.MethodPut, "/cp-dir/dst", nil, map[string]string{
				"X-Amz-Copy-Source": "/cp-dir/src",
				header:              "MERGE",
			})
			require.Equal(t, http.StatusBadRequest, w.Code)

			body := parseS3Error(t, w.Body.Bytes())
			assert.Equal(t, "InvalidArgument", body.Code)
			assert.Contains(t, body.Message, header)

			assert.Equal(t, http.StatusNotFound,
				s3Request(t, srv, http.MethodHead, "/cp-dir/dst", nil, nil).Code)
		})
	}
}

// TestS3_CopyObject_TaggingDirective asserts the tag-set follows its own directive:
// COPY (the default) carries the source's tags, REPLACE takes them from the
// URL-query-encoded x-amz-tagging header.
func TestS3_CopyObject_TaggingDirective(t *testing.T) {
	putSourceTags := func(t *testing.T, srv *emulator.Server, bucket string) {
		t.Helper()
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/"+bucket+"/src?tagging",
				[]byte(`<Tagging><TagSet><Tag><Key>team</Key><Value>infra</Value></Tag></TagSet></Tagging>`),
				nil).Code)
	}

	getTags := func(t *testing.T, srv *emulator.Server, bucket, key string) map[string]string {
		t.Helper()
		w := s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key+"?tagging", nil, nil)
		require.Equal(t, http.StatusOK, w.Code)

		var result struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"TagSet>Tag"`
		}
		require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))

		tags := make(map[string]string, len(result.Tags))
		for _, tag := range result.Tags {
			tags[tag.Key] = tag.Value
		}
		return tags
	}

	t.Run("COPY carries the source tag-set", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-tag")
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag/src", []byte("payload"), nil).Code)
		putSourceTags(t, srv, "cp-tag")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag/dst", nil, map[string]string{
				"X-Amz-Copy-Source": "/cp-tag/src",
			}).Code)

		assert.Equal(t, map[string]string{"team": "infra"}, getTags(t, srv, "cp-tag", "dst"))
	})

	t.Run("REPLACE takes the request tag-set", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-tag2")
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag2/src", []byte("payload"), nil).Code)
		putSourceTags(t, srv, "cp-tag2")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag2/dst", nil, map[string]string{
				"X-Amz-Copy-Source":       "/cp-tag2/src",
				"x-amz-tagging-directive": "REPLACE",
				"x-amz-tagging":           "stage=prod&owner=alice",
			}).Code)

		assert.Equal(t, map[string]string{"stage": "prod", "owner": "alice"},
			getTags(t, srv, "cp-tag2", "dst"))
	})

	t.Run("REPLACE with no tagging header clears the tag-set", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-tag3")
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag3/src", []byte("payload"), nil).Code)
		putSourceTags(t, srv, "cp-tag3")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag3/dst", nil, map[string]string{
				"X-Amz-Copy-Source":       "/cp-tag3/src",
				"x-amz-tagging-directive": "REPLACE",
			}).Code)

		assert.Empty(t, getTags(t, srv, "cp-tag3", "dst"),
			`the default value of x-amz-tagging is the empty value`)
	})

	t.Run("REPLACE with an unparseable tagging header is rejected", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-tag-bad")
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag-bad/src", []byte("payload"), nil).Code)

		// %zz is not valid percent-encoding, so the tag-set cannot be decoded.
		w := s3Request(t, srv, http.MethodPut, "/cp-tag-bad/dst", nil, map[string]string{
			"X-Amz-Copy-Source":       "/cp-tag-bad/src",
			"x-amz-tagging-directive": "REPLACE",
			"x-amz-tagging":           "stage=%zz",
		})
		require.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "InvalidArgument", parseS3Error(t, w.Body.Bytes()).Code)

		assert.Equal(t, http.StatusNotFound,
			s3Request(t, srv, http.MethodHead, "/cp-tag-bad/dst", nil, nil).Code)
	})

	t.Run("copied tags do not alias the source", func(t *testing.T) {
		srv := storageClassFixture(t, "cp-tag4")
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag4/src", []byte("payload"), nil).Code)
		putSourceTags(t, srv, "cp-tag4")

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag4/dst", nil, map[string]string{
				"X-Amz-Copy-Source": "/cp-tag4/src",
			}).Code)

		// Retagging the destination must not disturb the source.
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/cp-tag4/dst?tagging",
				[]byte(`<Tagging><TagSet><Tag><Key>team</Key><Value>data</Value></Tag></TagSet></Tagging>`),
				nil).Code)

		assert.Equal(t, map[string]string{"team": "infra"}, getTags(t, srv, "cp-tag4", "src"))
		assert.Equal(t, map[string]string{"team": "data"}, getTags(t, srv, "cp-tag4", "dst"))
	})
}
