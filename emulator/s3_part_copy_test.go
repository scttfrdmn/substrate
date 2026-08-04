package emulator_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// These are the #532 and #551 gates: UploadPartCopy and ListParts are both
// addressed exactly like a general object operation of the same verb, so nothing
// but parseS3Operation's ordering distinguishes them from CopyObject and GetObject.
// Every assertion here is against the raw wire response, not a Go round-trip,
// because the defects were in what the router chose rather than in any struct.

// s3CopyPartResult is the parsed form of UploadPartCopy's response body.
type s3CopyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// s3ListPartsResult is the parsed form of ListParts' response body.
type s3ListPartsResult struct {
	XMLName              xml.Name `xml:"ListPartsResult"`
	Bucket               string   `xml:"Bucket"`
	Key                  string   `xml:"Key"`
	UploadID             string   `xml:"UploadId"`
	PartNumberMarker     int      `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker"`
	MaxParts             int      `xml:"MaxParts"`
	IsTruncated          bool     `xml:"IsTruncated"`
	StorageClass         string   `xml:"StorageClass"`
	Parts                []struct {
		PartNumber   int    `xml:"PartNumber"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	} `xml:"Part"`
}

// partBody builds a part or object body of n bytes filled with fill.
func partBody(fill byte, n int) []byte {
	return bytes.Repeat([]byte{fill}, n)
}

// partCopyFixture creates a source bucket holding one object of size bytes, plus a
// destination bucket with an open multipart upload, and returns the upload ID.
func partCopyFixture(t *testing.T, size int) (*emulator.Server, string) {
	t.Helper()
	srv, uploadID := multipartFixture(t, "dst", "joined")
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/src", nil, nil).Code, "create source bucket")
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/src/big", partBody('s', size), nil).Code, "put source object")
	return srv, uploadID
}

// uploadPartCopy issues one UploadPartCopy against src/big and returns the recorder.
func uploadPartCopy(t *testing.T, srv *emulator.Server, uploadID string, partNumber int, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	h := map[string]string{"X-Amz-Copy-Source": "/src/big"}
	for k, v := range headers {
		h[k] = v
	}
	return s3Request(t, srv, http.MethodPut,
		fmt.Sprintf("/dst/joined?partNumber=%d&uploadId=%s", partNumber, uploadID), nil, h)
}

// listParts issues a ListParts request with the given query suffix.
func listParts(t *testing.T, srv *emulator.Server, uploadID, extra string) (int, s3ListPartsResult, []byte) {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/dst/joined?uploadId="+uploadID+extra, nil, nil)
	var out s3ListPartsResult
	if w.Code == http.StatusOK {
		require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &out), "decode ListPartsResult: %s", w.Body.Bytes())
	}
	return w.Code, out, w.Body.Bytes()
}

// TestS3UploadPartCopy_DestinationKeyStaysAbsent is #532's headline gate. Before
// the fix the request was routed to CopyObject, which wrote the destination object,
// so a HeadObject mid-upload reported a ContentLength real S3 never exposes.
func TestS3UploadPartCopy_DestinationKeyStaysAbsent(t *testing.T) {
	srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)

	resp := uploadPartCopy(t, srv, uploadID, 1, nil)
	require.Equal(t, http.StatusOK, resp.Code, "upload part copy: %s", resp.Body.Bytes())

	var result s3CopyPartResult
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &result), "decode CopyPartResult: %s", resp.Body.Bytes())
	assert.NotEmpty(t, result.ETag, "CopyPartResult must carry the part's ETag")
	assert.NotEmpty(t, result.LastModified, "CopyPartResult must carry LastModified")
	assert.Contains(t, resp.Body.String(), "<CopyPartResult>",
		"the wire body must be a CopyPartResult, not a CopyObjectResult")
	assert.NotContains(t, resp.Body.String(), "CopyObjectResult")

	// The observable that a routed-to-CopyObject part copy broke.
	hw := s3Request(t, srv, http.MethodHead, "/dst/joined", nil, nil)
	assert.Equal(t, http.StatusNotFound, hw.Code,
		"the destination key must not exist until CompleteMultipartUpload")

	gw := s3Request(t, srv, http.MethodGet, "/dst/joined", nil, nil)
	assert.Equal(t, http.StatusNotFound, gw.Code, "GetObject on the destination must be 404 too")
}

// TestS3UploadPartCopy_CompletesTheUpload proves the copied bytes are a real part:
// Complete assembles them, and the assembled object is the source's bytes.
func TestS3UploadPartCopy_CompletesTheUpload(t *testing.T) {
	srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)

	resp := uploadPartCopy(t, srv, uploadID, 1, nil)
	require.Equal(t, http.StatusOK, resp.Code, "upload part copy: %s", resp.Body.Bytes())
	var result s3CopyPartResult
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &result))

	cw := s3Request(t, srv, http.MethodPost, "/dst/joined?uploadId="+uploadID,
		completeBody(result.ETag), nil)
	require.Equal(t, http.StatusOK, cw.Code, "complete after a part copy: %s", cw.Body.Bytes())

	gw := s3Request(t, srv, http.MethodGet, "/dst/joined", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code, "the assembled object must be readable")
	assert.Equal(t, partBody('s', s3MinPartSize+1024), gw.Body.Bytes(),
		"the assembled object must be the copied source bytes")
}

// TestS3UploadPartCopy_MixedWithUploadedParts proves a copied part and an uploaded
// part assemble together, in part-number order — the reason UploadPartCopy exists.
func TestS3UploadPartCopy_MixedWithUploadedParts(t *testing.T) {
	srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)

	resp := uploadPartCopy(t, srv, uploadID, 1, nil)
	require.Equal(t, http.StatusOK, resp.Code, "upload part copy: %s", resp.Body.Bytes())
	var copied s3CopyPartResult
	require.NoError(t, xml.Unmarshal(resp.Body.Bytes(), &copied))

	tail := partBody('t', 64)
	uploadedETag := uploadPart(t, srv, "dst", "joined", uploadID, 2, tail)

	cw := s3Request(t, srv, http.MethodPost, "/dst/joined?uploadId="+uploadID,
		completeBody(copied.ETag, uploadedETag), nil)
	require.Equal(t, http.StatusOK, cw.Code, "complete a mixed upload: %s", cw.Body.Bytes())

	gw := s3Request(t, srv, http.MethodGet, "/dst/joined", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, append(partBody('s', s3MinPartSize+1024), tail...), gw.Body.Bytes(),
		"the copied part must precede the uploaded one")
}

// TestS3UploadPartCopy_SourceRange covers x-amz-copy-source-range: the honored
// case, and each way a range must be refused rather than quietly widened.
func TestS3UploadPartCopy_SourceRange(t *testing.T) {
	const srcSize = s3MinPartSize + 1024

	t.Run("honors an explicit range", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, srcSize)
		resp := uploadPartCopy(t, srv, uploadID, 1,
			map[string]string{"x-amz-copy-source-range": "bytes=0-9"})
		require.Equal(t, http.StatusOK, resp.Code, "ranged part copy: %s", resp.Body.Bytes())

		_, result, raw := listParts(t, srv, uploadID, "")
		require.Len(t, result.Parts, 1, "raw: %s", raw)
		assert.Equal(t, int64(10), result.Parts[0].Size,
			"only the named ten bytes may be copied")
	})

	t.Run("refuses a range past the end of the source", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, srcSize)
		resp := uploadPartCopy(t, srv, uploadID, 1,
			map[string]string{"x-amz-copy-source-range": fmt.Sprintf("bytes=0-%d", srcSize)})
		require.Equal(t, http.StatusBadRequest, resp.Code, "body: %s", resp.Body.Bytes())
		assert.Equal(t, "InvalidArgument", parseS3Error(t, resp.Body.Bytes()).Code)
	})

	t.Run("refuses a malformed range", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, srcSize)
		for _, bad := range []string{"0-9", "bytes=9-0", "bytes=abc-def", "bytes=0", "bytes=-9", "bytes=0-"} {
			resp := uploadPartCopy(t, srv, uploadID, 1,
				map[string]string{"x-amz-copy-source-range": bad})
			require.Equal(t, http.StatusBadRequest, resp.Code, "range %q must be refused: %s", bad, resp.Body.Bytes())
			assert.Equal(t, "InvalidArgument", parseS3Error(t, resp.Body.Bytes()).Code, "range %q", bad)
		}
	})

	t.Run("refuses a range against a source of 5 MB or less", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, s3MinPartSize)
		resp := uploadPartCopy(t, srv, uploadID, 1,
			map[string]string{"x-amz-copy-source-range": "bytes=0-9"})
		require.Equal(t, http.StatusBadRequest, resp.Code, "body: %s", resp.Body.Bytes())
		assert.Equal(t, "InvalidRequest", parseS3Error(t, resp.Body.Bytes()).Code,
			"the reference's documented special error for an unsupported byte-range source")
	})

	t.Run("copies the whole source when no range is named", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, s3MinPartSize)
		resp := uploadPartCopy(t, srv, uploadID, 1, nil)
		require.Equal(t, http.StatusOK, resp.Code, "an unranged copy of a small source is fine: %s", resp.Body.Bytes())
	})
}

// TestS3UploadPartCopy_Errors covers the refusals whose codes a caller's retry
// logic branches on.
func TestS3UploadPartCopy_Errors(t *testing.T) {
	t.Run("unknown upload is NoSuchUpload", func(t *testing.T) {
		srv, _ := partCopyFixture(t, s3MinPartSize+1024)
		resp := uploadPartCopy(t, srv, "no-such-upload", 1, nil)
		require.Equal(t, http.StatusNotFound, resp.Code, "body: %s", resp.Body.Bytes())
		assert.Equal(t, "NoSuchUpload", parseS3Error(t, resp.Body.Bytes()).Code)
	})

	t.Run("absent source is NoSuchKey", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)
		w := s3Request(t, srv, http.MethodPut,
			fmt.Sprintf("/dst/joined?partNumber=1&uploadId=%s", uploadID), nil,
			map[string]string{"X-Amz-Copy-Source": "/src/missing"})
		require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.Bytes())
		assert.Equal(t, "NoSuchKey", parseS3Error(t, w.Body.Bytes()).Code)
	})

	t.Run("out-of-range part number is InvalidPart", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)
		for _, n := range []int{0, 10001} {
			resp := uploadPartCopy(t, srv, uploadID, n, nil)
			require.Equal(t, http.StatusBadRequest, resp.Code, "part %d: %s", n, resp.Body.Bytes())
			assert.Equal(t, "InvalidPart", parseS3Error(t, resp.Body.Bytes()).Code, "part %d", n)
		}
	})

	t.Run("a failed source precondition is 412", func(t *testing.T) {
		srv, uploadID := partCopyFixture(t, s3MinPartSize+1024)
		resp := uploadPartCopy(t, srv, uploadID, 1,
			map[string]string{"x-amz-copy-source-if-match": `"not-the-etag"`})
		assert.Equal(t, http.StatusPreconditionFailed, resp.Code, "body: %s", resp.Body.Bytes())
	})
}

// TestS3UploadPartCopy_DoesNotDisturbCopyObject pins the other side of the
// reordering: a copy with no partNumber/uploadId is still CopyObject, and does
// still write the destination object.
func TestS3UploadPartCopy_DoesNotDisturbCopyObject(t *testing.T) {
	srv, _ := partCopyFixture(t, 32)

	w := s3Request(t, srv, http.MethodPut, "/dst/plain", nil,
		map[string]string{"X-Amz-Copy-Source": "/src/big"})
	require.Equal(t, http.StatusOK, w.Code, "plain copy: %s", w.Body.Bytes())
	assert.Contains(t, w.Body.String(), "<CopyObjectResult>",
		"a copy without a part number is still CopyObject")

	hw := s3Request(t, srv, http.MethodHead, "/dst/plain", nil, nil)
	assert.Equal(t, http.StatusOK, hw.Code, "CopyObject must still write the destination")
	assert.Equal(t, "32", hw.Header().Get("Content-Length"))
}

// TestS3ListParts_EmptyUpload is #551's headline gate: an open upload with no parts
// is a 200 with an empty list. Before the fix the request reached GetObject and
// answered 404 NoSuchKey, so a caller could not tell an upload from a missing key.
func TestS3ListParts_EmptyUpload(t *testing.T) {
	srv, uploadID := multipartFixture(t, "dst", "joined")

	code, result, raw := listParts(t, srv, uploadID, "")
	require.Equal(t, http.StatusOK, code, "body: %s", raw)
	assert.Contains(t, string(raw), "<ListPartsResult>",
		"the wire body must be a ListPartsResult")
	assert.Empty(t, result.Parts, "a fresh upload has no parts")
	assert.False(t, result.IsTruncated)
	assert.Equal(t, "dst", result.Bucket)
	assert.Equal(t, "joined", result.Key)
	assert.Equal(t, uploadID, result.UploadID)
	assert.Equal(t, 1000, result.MaxParts, "the documented default")
	assert.Equal(t, "STANDARD", result.StorageClass)
}

// TestS3ListParts_ReportsUploadedParts covers the ordering that state keys get
// wrong on their own: part 10 sorts before part 2 lexicographically.
func TestS3ListParts_ReportsUploadedParts(t *testing.T) {
	srv, uploadID := multipartFixture(t, "dst", "joined")

	for _, n := range []int{2, 10, 1} {
		uploadPart(t, srv, "dst", "joined", uploadID, n, partBody('a', 16*n))
	}

	code, result, raw := listParts(t, srv, uploadID, "")
	require.Equal(t, http.StatusOK, code, "body: %s", raw)
	require.Len(t, result.Parts, 3)

	var numbers []int
	for _, p := range result.Parts {
		numbers = append(numbers, p.PartNumber)
		assert.NotEmpty(t, p.ETag, "part %d must report an ETag", p.PartNumber)
		assert.NotEmpty(t, p.LastModified, "part %d must report LastModified", p.PartNumber)
		assert.Equal(t, int64(16*p.PartNumber), p.Size, "part %d size", p.PartNumber)
	}
	assert.Equal(t, []int{1, 2, 10}, numbers, "parts must be listed in ascending part-number order")
}

// TestS3ListParts_Pagination covers max-parts and part-number-marker, the two
// parameters a caller listing a large upload depends on to see all of it.
func TestS3ListParts_Pagination(t *testing.T) {
	srv, uploadID := multipartFixture(t, "dst", "joined")
	for n := 1; n <= 3; n++ {
		uploadPart(t, srv, "dst", "joined", uploadID, n, partBody('a', 16))
	}

	code, page1, raw := listParts(t, srv, uploadID, "&max-parts=2")
	require.Equal(t, http.StatusOK, code, "body: %s", raw)
	require.Len(t, page1.Parts, 2)
	assert.Equal(t, 2, page1.MaxParts)
	assert.True(t, page1.IsTruncated, "a third part remains")
	assert.Equal(t, 2, page1.NextPartNumberMarker, "resume after the last part returned")

	code, page2, raw := listParts(t, srv, uploadID,
		fmt.Sprintf("&max-parts=2&part-number-marker=%d", page1.NextPartNumberMarker))
	require.Equal(t, http.StatusOK, code, "body: %s", raw)
	require.Len(t, page2.Parts, 1, "only parts above the marker are listed")
	assert.Equal(t, 3, page2.Parts[0].PartNumber)
	assert.False(t, page2.IsTruncated)
	assert.Equal(t, 2, page2.PartNumberMarker, "the marker is echoed")

	code, all, raw := listParts(t, srv, uploadID, "&max-parts=99999")
	require.Equal(t, http.StatusOK, code, "body: %s", raw)
	assert.Len(t, all.Parts, 3)
	assert.Equal(t, 1000, all.MaxParts, "max-parts above the documented ceiling is clamped")
}

// TestS3ListParts_Errors covers the refusal that distinguishes #551's fix from its
// symptom: an unknown upload is NoSuchUpload, not the NoSuchKey GetObject answered.
func TestS3ListParts_Errors(t *testing.T) {
	srv, uploadID := multipartFixture(t, "dst", "joined")

	t.Run("unknown upload", func(t *testing.T) {
		w := s3Request(t, srv, http.MethodGet, "/dst/joined?uploadId=no-such-upload", nil, nil)
		require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.Bytes())
		assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code,
			"an unknown upload is NoSuchUpload, not the NoSuchKey a GetObject would answer")
	})

	t.Run("upload addressed through the wrong key", func(t *testing.T) {
		w := s3Request(t, srv, http.MethodGet, "/dst/other?uploadId="+uploadID, nil, nil)
		require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.Bytes())
		assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code)
	})

	t.Run("after abort", func(t *testing.T) {
		aw := s3Request(t, srv, http.MethodDelete, "/dst/joined?uploadId="+uploadID, nil, nil)
		require.Equal(t, http.StatusNoContent, aw.Code)
		w := s3Request(t, srv, http.MethodGet, "/dst/joined?uploadId="+uploadID, nil, nil)
		require.Equal(t, http.StatusNotFound, w.Code)
		assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code)
	})
}

// TestS3ListParts_DoesNotDisturbGetObject pins the other side of #551's fix: a GET
// with no uploadId is still GetObject, including for a key that shares its name
// with an open upload's key.
func TestS3ListParts_DoesNotDisturbGetObject(t *testing.T) {
	srv, uploadID := multipartFixture(t, "dst", "joined")
	uploadPart(t, srv, "dst", "joined", uploadID, 1, partBody('a', 16))

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/dst/plain", []byte("hello"), nil).Code)

	w := s3Request(t, srv, http.MethodGet, "/dst/plain", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello", w.Body.String())

	// The upload's own key still has no object, and a GET without uploadId says so
	// rather than falling through to the parts list.
	nw := s3Request(t, srv, http.MethodGet, "/dst/joined", nil, nil)
	require.Equal(t, http.StatusNotFound, nw.Code)
	assert.Equal(t, "NoSuchKey", parseS3Error(t, nw.Body.Bytes()).Code)
}

// TestS3ListParts_ReportsChecksumConfiguration covers the members a caller uses to
// confirm the checksum type it asked for before it commits to a Complete.
func TestS3ListParts_ReportsChecksumConfiguration(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/dst", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/dst/joined?uploads", nil,
		map[string]string{"x-amz-checksum-algorithm": "SHA256"})
	require.Equal(t, http.StatusOK, iw.Code, "body: %s", iw.Body.Bytes())
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	uploadPart(t, srv, "dst", "joined", ir.UploadID, 1, partBody('a', 16))

	w := s3Request(t, srv, http.MethodGet, "/dst/joined?uploadId="+ir.UploadID, nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.Bytes())
	raw := w.Body.String()
	assert.Contains(t, raw, "<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>")
	assert.Contains(t, raw, "<ChecksumSHA256>", "each part reports its own checksum")
	assert.True(t, strings.Contains(raw, "<ChecksumType>COMPOSITE</ChecksumType>") ||
		strings.Contains(raw, "<ChecksumType>FULL_OBJECT</ChecksumType>"),
		"the upload's checksum type must be reported: %s", raw)
}
