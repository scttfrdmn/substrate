package emulator_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// s3MinPartSize mirrors the plugin's non-final part minimum. It is duplicated
// rather than exported because these tests must fail if the plugin's constant
// drifts from S3's documented 5 MB, which a shared constant would hide.
const s3MinPartSize = 5 * 1024 * 1024

// completeBody builds a CompleteMultipartUpload request body naming parts 1..N in
// order, with the supplied ETags.
func completeBody(etags ...string) []byte {
	var b strings.Builder
	b.WriteString(`<CompleteMultipartUpload>`)
	for i, etag := range etags {
		fmt.Fprintf(&b, `<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>`, i+1, etag)
	}
	b.WriteString(`</CompleteMultipartUpload>`)
	return []byte(b.String())
}

// multipartFixture creates a bucket and initiates a multipart upload in it,
// returning the server and the upload ID.
func multipartFixture(t *testing.T, bucket, key string) (*emulator.Server, string) {
	t.Helper()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")

	iw := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code, "initiate multipart upload")

	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))
	require.NotEmpty(t, ir.UploadID)

	return srv, ir.UploadID
}

// uploadPart uploads one part and returns the ETag the emulator assigned it.
func uploadPart(t *testing.T, srv *emulator.Server, bucket, key, uploadID string, partNumber int, body []byte) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut,
		fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucket, key, partNumber, uploadID),
		body, nil)
	require.Equal(t, http.StatusOK, w.Code, "upload part %d", partNumber)
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag, "UploadPart must return an ETag")
	return etag
}

// s3ErrorBody is the parsed form of an S3 <Error> document, including the
// error-specific child elements EntityTooSmall carries.
type s3ErrorBody struct {
	Code           string `xml:"Code"`
	Message        string `xml:"Message"`
	ETag           string `xml:"ETag"`
	MinSizeAllowed string `xml:"MinSizeAllowed"`
	ProposedSize   string `xml:"ProposedSize"`
	PartNumber     string `xml:"PartNumber"`
}

// parseS3Error decodes an S3 XML error response body.
func parseS3Error(t *testing.T, raw []byte) s3ErrorBody {
	t.Helper()
	var e s3ErrorBody
	require.NoError(t, xml.Unmarshal(raw, &e), "decode error body: %s", raw)
	return e
}

// openUploadIDs returns the upload IDs ListMultipartUploads reports for a bucket.
func openUploadIDs(t *testing.T, srv *emulator.Server, bucket string) []string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "list multipart uploads")

	var result struct {
		Uploads []struct {
			UploadID string `xml:"UploadId"`
		} `xml:"Upload"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))

	ids := make([]string, 0, len(result.Uploads))
	for _, u := range result.Uploads {
		ids = append(ids, u.UploadID)
	}
	return ids
}

// TestS3_CompleteMultipartUpload_EntityTooSmall is the acceptance test for #400:
// a non-final part below the 5 MB minimum must fail Complete rather than silently
// assembling an object real S3 would have rejected.
func TestS3_CompleteMultipartUpload_EntityTooSmall(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-small", "obj.bin")

	small := bytes.Repeat([]byte("x"), 1024)
	e1 := uploadPart(t, srv, "mpu-small", "obj.bin", uploadID, 1, small)
	e2 := uploadPart(t, srv, "mpu-small", "obj.bin", uploadID, 2, []byte("tail"))

	w := s3Request(t, srv, http.MethodPost,
		"/mpu-small/obj.bin?uploadId="+uploadID, completeBody(e1, e2), nil)
	require.Equal(t, http.StatusBadRequest, w.Code)

	got := parseS3Error(t, w.Body.Bytes())
	assert.Equal(t, "EntityTooSmall", got.Code)
	assert.Contains(t, got.Message, "smaller than the minimum allowed object size")
	assert.Equal(t, strconv.Itoa(s3MinPartSize), got.MinSizeAllowed)
	assert.Equal(t, "1024", got.ProposedSize, "ProposedSize must be the offending part's size")
	assert.Equal(t, "1", got.PartNumber, "PartNumber must identify the offending part")
	assert.Equal(t, strings.Trim(e1, `"`), got.ETag, "ETag must be the offending part's, unquoted")

	// No object may exist at the key: a rejected Complete assembles nothing.
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodHead, "/mpu-small/obj.bin", nil, nil).Code,
		"failed Complete must not create the object")
}

// TestS3_CompleteMultipartUpload_SmallFinalPartSucceeds asserts the exemption that
// makes the minimum usable: only non-final parts are constrained.
func TestS3_CompleteMultipartUpload_SmallFinalPartSucceeds(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-tail", "obj.bin")

	big := bytes.Repeat([]byte("a"), s3MinPartSize)
	tail := bytes.Repeat([]byte("z"), 1024)
	e1 := uploadPart(t, srv, "mpu-tail", "obj.bin", uploadID, 1, big)
	e2 := uploadPart(t, srv, "mpu-tail", "obj.bin", uploadID, 2, tail)

	w := s3Request(t, srv, http.MethodPost,
		"/mpu-tail/obj.bin?uploadId="+uploadID, completeBody(e1, e2), nil)
	require.Equal(t, http.StatusOK, w.Code, "1 KiB final part must be allowed: %s", w.Body)

	gw := s3Request(t, srv, http.MethodGet, "/mpu-tail/obj.bin", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, append(big, tail...), gw.Body.Bytes())
}

// TestS3_CompleteMultipartUpload_SinglePartAnySize asserts a single-part upload is
// exempt from the minimum: it is nothing but its own final part.
func TestS3_CompleteMultipartUpload_SinglePartAnySize(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "tiny", body: []byte("hi")},
		{name: "empty", body: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, uploadID := multipartFixture(t, "mpu-single", "obj.bin")
			etag := uploadPart(t, srv, "mpu-single", "obj.bin", uploadID, 1, tt.body)

			w := s3Request(t, srv, http.MethodPost,
				"/mpu-single/obj.bin?uploadId="+uploadID, completeBody(etag), nil)
			require.Equal(t, http.StatusOK, w.Code, "single part of any size must complete: %s", w.Body)

			gw := s3Request(t, srv, http.MethodGet, "/mpu-single/obj.bin", nil, nil)
			require.Equal(t, http.StatusOK, gw.Code)
			assert.Equal(t, string(tt.body), gw.Body.String())
		})
	}
}

// TestS3_CompleteMultipartUpload_FailedCompleteLeavesUploadOpen is the property the
// issue reporter actually needs: after a rejected Complete the upload is still
// listed, so a consumer's cleanup path is observable through the API rather than
// inferred from its own call log. Aborting it then clears the listing.
func TestS3_CompleteMultipartUpload_FailedCompleteLeavesUploadOpen(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-orphan", "obj.bin")

	e1 := uploadPart(t, srv, "mpu-orphan", "obj.bin", uploadID, 1, bytes.Repeat([]byte("x"), 1024))
	e2 := uploadPart(t, srv, "mpu-orphan", "obj.bin", uploadID, 2, []byte("tail"))

	require.Equal(t, []string{uploadID}, openUploadIDs(t, srv, "mpu-orphan"),
		"upload must be listed before Complete")

	cw := s3Request(t, srv, http.MethodPost,
		"/mpu-orphan/obj.bin?uploadId="+uploadID, completeBody(e1, e2), nil)
	require.Equal(t, http.StatusBadRequest, cw.Code)

	assert.Equal(t, []string{uploadID}, openUploadIDs(t, srv, "mpu-orphan"),
		"a rejected Complete must leave the upload open so it can be retried or aborted")

	aw := s3Request(t, srv, http.MethodDelete, "/mpu-orphan/obj.bin?uploadId="+uploadID, nil, nil)
	require.Equal(t, http.StatusNoContent, aw.Code)

	assert.Empty(t, openUploadIDs(t, srv, "mpu-orphan"),
		"Abort must remove the upload from the listing")
}

// TestS3_CompleteMultipartUpload_SuccessfulCompleteClosesUpload is the companion to
// the failure case: a successful Complete also ends the upload.
func TestS3_CompleteMultipartUpload_SuccessfulCompleteClosesUpload(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-closed", "obj.bin")
	etag := uploadPart(t, srv, "mpu-closed", "obj.bin", uploadID, 1, []byte("payload"))

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPost,
		"/mpu-closed/obj.bin?uploadId="+uploadID, completeBody(etag), nil).Code)

	assert.Empty(t, openUploadIDs(t, srv, "mpu-closed"))

	// A second Complete on the same upload ID is now NoSuchUpload, per the
	// documented "might have been aborted or completed" case.
	w := s3Request(t, srv, http.MethodPost,
		"/mpu-closed/obj.bin?uploadId="+uploadID, completeBody(etag), nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code)
}

// TestS3_CompleteMultipartUpload_InvalidPart covers the two conditions S3 reports
// identically: a part that was never uploaded, and a part whose supplied ETag does
// not match the stored one.
func TestS3_CompleteMultipartUpload_InvalidPart(t *testing.T) {
	tests := []struct {
		name string
		// body builds the Complete request from the ETag of the one uploaded part.
		body func(etag string) []byte
	}{
		{
			name: "part never uploaded",
			body: func(etag string) []byte {
				return []byte(`<CompleteMultipartUpload>` +
					`<Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part>` +
					`<Part><PartNumber>7</PartNumber><ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag></Part>` +
					`</CompleteMultipartUpload>`)
			},
		},
		{
			name: "etag does not match stored part",
			body: func(string) []byte {
				return completeBody(`"00000000000000000000000000000000"`)
			},
		},
		{
			name: "etag empty",
			body: func(string) []byte { return completeBody("") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, uploadID := multipartFixture(t, "mpu-invalid", "obj.bin")
			etag := uploadPart(t, srv, "mpu-invalid", "obj.bin", uploadID, 1,
				bytes.Repeat([]byte("a"), s3MinPartSize))

			w := s3Request(t, srv, http.MethodPost,
				"/mpu-invalid/obj.bin?uploadId="+uploadID, tt.body(etag), nil)
			require.Equal(t, http.StatusBadRequest, w.Code)

			got := parseS3Error(t, w.Body.Bytes())
			assert.Equal(t, "InvalidPart", got.Code)
			assert.Contains(t, got.Message, "could not be found")

			assert.Equal(t, http.StatusNotFound,
				s3Request(t, srv, http.MethodHead, "/mpu-invalid/obj.bin", nil, nil).Code,
				"rejected Complete must not create the object")
			assert.Equal(t, []string{uploadID}, openUploadIDs(t, srv, "mpu-invalid"),
				"rejected Complete must leave the upload open")
		})
	}
}

// TestS3_CompleteMultipartUpload_ETagQuotingIgnored asserts that quoting and hex
// case do not decide validity. Clients differ on whether they echo back the quotes
// S3 sends, and a false InvalidPart over quoting would send a consumer hunting a
// data bug that does not exist.
func TestS3_CompleteMultipartUpload_ETagQuotingIgnored(t *testing.T) {
	tests := []struct {
		name string
		form func(etag string) string
	}{
		{name: "as returned", form: func(e string) string { return e }},
		{name: "unquoted", form: func(e string) string { return strings.Trim(e, `"`) }},
		{name: "uppercase hex", form: strings.ToUpper},
		{name: "surrounding whitespace", form: func(e string) string { return " " + e + " " }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, uploadID := multipartFixture(t, "mpu-etag", "obj.bin")
			etag := uploadPart(t, srv, "mpu-etag", "obj.bin", uploadID, 1, []byte("payload"))

			w := s3Request(t, srv, http.MethodPost,
				"/mpu-etag/obj.bin?uploadId="+uploadID, completeBody(tt.form(etag)), nil)
			assert.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body)
		})
	}
}

// TestS3_CompleteMultipartUpload_InvalidPartOrder asserts the parts list must be
// ascending. An ordering bug in a consumer that assembles the list concurrently is
// silent data corruption, so this must be an error rather than a sort.
func TestS3_CompleteMultipartUpload_InvalidPartOrder(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-order", "obj.bin")

	big := bytes.Repeat([]byte("a"), s3MinPartSize)
	e1 := uploadPart(t, srv, "mpu-order", "obj.bin", uploadID, 1, big)
	e2 := uploadPart(t, srv, "mpu-order", "obj.bin", uploadID, 2, []byte("tail"))

	tests := []struct {
		name string
		body string
	}{
		{
			name: "descending",
			body: `<CompleteMultipartUpload>` +
				`<Part><PartNumber>2</PartNumber><ETag>` + e2 + `</ETag></Part>` +
				`<Part><PartNumber>1</PartNumber><ETag>` + e1 + `</ETag></Part>` +
				`</CompleteMultipartUpload>`,
		},
		{
			name: "duplicate part number",
			body: `<CompleteMultipartUpload>` +
				`<Part><PartNumber>1</PartNumber><ETag>` + e1 + `</ETag></Part>` +
				`<Part><PartNumber>1</PartNumber><ETag>` + e1 + `</ETag></Part>` +
				`</CompleteMultipartUpload>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPost,
				"/mpu-order/obj.bin?uploadId="+uploadID, []byte(tt.body), nil)
			require.Equal(t, http.StatusBadRequest, w.Code)

			got := parseS3Error(t, w.Body.Bytes())
			assert.Equal(t, "InvalidPartOrder", got.Code)
			assert.Contains(t, got.Message, "ascending order")
		})
	}
}

// TestS3_CompleteMultipartUpload_MalformedXML asserts that a body carrying no
// usable part list is a 400, whether it fails to parse or simply names no parts.
func TestS3_CompleteMultipartUpload_MalformedXML(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "not well-formed", body: []byte(`<CompleteMultipartUpload><Part>`)},
		{name: "no parts", body: []byte(`<CompleteMultipartUpload></CompleteMultipartUpload>`)},
		{name: "empty body", body: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, uploadID := multipartFixture(t, "mpu-xml", "obj.bin")
			uploadPart(t, srv, "mpu-xml", "obj.bin", uploadID, 1, []byte("payload"))

			w := s3Request(t, srv, http.MethodPost,
				"/mpu-xml/obj.bin?uploadId="+uploadID, tt.body, nil)
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Equal(t, "MalformedXML", parseS3Error(t, w.Body.Bytes()).Code)
		})
	}
}

// TestS3_CompleteMultipartUpload_WrongBucketOrKey asserts an upload ID is only
// valid against the bucket and key it was created for, matching the guard uploadPart
// and abortMultipartUpload already applied.
func TestS3_CompleteMultipartUpload_WrongBucketOrKey(t *testing.T) {
	srv, uploadID := multipartFixture(t, "mpu-owner", "right.bin")
	etag := uploadPart(t, srv, "mpu-owner", "right.bin", uploadID, 1, []byte("payload"))

	w := s3Request(t, srv, http.MethodPost,
		"/mpu-owner/wrong.bin?uploadId="+uploadID, completeBody(etag), nil)
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code)

	// The upload survives the mismatched request and still completes correctly.
	assert.Equal(t, []string{uploadID}, openUploadIDs(t, srv, "mpu-owner"))
	assert.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPost,
		"/mpu-owner/right.bin?uploadId="+uploadID, completeBody(etag), nil).Code)
}

// TestS3_Multipart_ContentEncoding asserts the Content-Encoding supplied at
// CreateMultipartUpload survives to the assembled object on both GET and HEAD.
//
// The header is accepted at creation, not on Complete, so an encoding the upload
// record does not carry is lost — which made a compressed object large enough to
// cross the multipart threshold round-trip as uncompressed, and made an
// application that never set the header indistinguishable from one that did (#406).
//
// Only the canonical header casing is exercised: s3Request goes through
// net/http, which canonicalizes on the way in, so a lowercase-header case would
// assert nothing about the plugin. createMultipartUpload resolves the header
// case-insensitively for the benefit of in-process callers that build an
// AWSRequest directly.
func TestS3_Multipart_ContentEncoding(t *testing.T) {
	for _, tc := range []struct {
		name     string
		encoding string
	}{
		{"gzip", "gzip"},
		{"zstd", "zstd"},
		{"brotli", "br"},
		{"absent", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/mpu-enc", nil, nil).Code, "create bucket")

			// User metadata rides alongside: it was already carried through when the
			// encoding was not, and that asymmetry is what made the bug look like an
			// application fault rather than an emulator gap.
			headers := map[string]string{"x-amz-meta-original-size": "999"}
			if tc.encoding != "" {
				headers["Content-Encoding"] = tc.encoding
			}
			iw := s3Request(t, srv, http.MethodPost, "/mpu-enc/big?uploads", nil, headers)
			require.Equal(t, http.StatusOK, iw.Code, "initiate multipart upload")

			var ir struct {
				UploadID string `xml:"UploadId"`
			}
			require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

			// Two parts, so this exercises the assembly path a real compressed upload
			// takes rather than the single-part shortcut.
			body1 := bytes.Repeat([]byte("a"), s3MinPartSize)
			body2 := []byte("tail")
			etag1 := uploadPart(t, srv, "mpu-enc", "big", ir.UploadID, 1, body1)
			etag2 := uploadPart(t, srv, "mpu-enc", "big", ir.UploadID, 2, body2)

			cw := s3Request(t, srv, http.MethodPost, "/mpu-enc/big?uploadId="+ir.UploadID,
				completeBody(etag1, etag2), nil)
			require.Equal(t, http.StatusOK, cw.Code, "complete multipart upload")

			// Both read paths must agree; the reporter saw an empty value on each.
			for _, method := range []string{http.MethodHead, http.MethodGet} {
				w := s3Request(t, srv, method, "/mpu-enc/big", nil, nil)
				require.Equal(t, http.StatusOK, w.Code, method)
				assert.Equal(t, tc.encoding, w.Header().Get("Content-Encoding"),
					"%s Content-Encoding", method)
				assert.Equal(t, "999", w.Header().Get("X-Amz-Meta-Original-Size"),
					"%s user metadata must survive alongside the encoding", method)
			}
		})
	}
}
