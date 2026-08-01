package emulator_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// s3SystemMetadataHeaders is the family under test (#430), with a distinguishable
// value for each. Table-driving the header *name* is what keeps a test from passing
// because one header happens to work — the family drifting apart on a single member
// is what #406 was.
var s3SystemMetadataHeaders = []struct {
	header string
	value  string
}{
	{"Cache-Control", "max-age=3600, public"},
	{"Content-Disposition", `attachment; filename="report.pdf"`},
	{"Content-Language", "en-GB"},
	{"Expires", "Wed, 21 Oct 2026 07:28:00 GMT"},
}

// s3SystemMetadataAll returns every family header as a request-header map, so a
// write can set the whole family in one call.
func s3SystemMetadataAll() map[string]string {
	headers := make(map[string]string, len(s3SystemMetadataHeaders))
	for _, h := range s3SystemMetadataHeaders {
		headers[h.header] = h.value
	}
	return headers
}

// assertSystemMetadata asserts the response carries every family header with the
// value the write supplied.
func assertSystemMetadata(t *testing.T, got http.Header, context string) {
	t.Helper()
	for _, h := range s3SystemMetadataHeaders {
		assert.Equal(t, h.value, got.Get(h.header), "%s: %s", context, h.header)
	}
}

// assertNoSystemMetadata asserts the response carries none of the family headers.
// An object written without them must produce no header at all, not an empty one:
// "Cache-Control: " and no Cache-Control are different observations, and an SDK
// that distinguishes nil from "" would report the wrong one.
func assertNoSystemMetadata(t *testing.T, got http.Header, context string) {
	t.Helper()
	for _, h := range s3SystemMetadataHeaders {
		_, present := got[http.CanonicalHeaderKey(h.header)]
		assert.False(t, present, "%s: %s must be absent, not empty", context, h.header)
	}
}

// newS3PluginDirect returns an initialized S3 plugin with no server in front of it,
// for the one test that must hand it non-canonical header names — Go canonicalizes
// them on the ServeHTTP path, so a lowercase header sent through the server would
// arrive canonical and prove nothing.
func newS3PluginDirect(t *testing.T) *emulator.S3Plugin {
	t.Helper()
	p := &emulator.S3Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{
			"time_controller": emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	return p
}

// TestS3_SystemMetadata_PutObjectRoundTrip asserts the family persists through
// PutObject and is returned by both GetObject and HeadObject. Substrate previously
// accepted these headers and discarded them, so a consumer asserting "the download
// filename survives an upload" passed while verifying nothing (#430).
func TestS3_SystemMetadata_PutObjectRoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta/doc", []byte("payload"), s3SystemMetadataAll()).Code)

	gw := s3Request(t, srv, http.MethodGet, "/sysmeta/doc", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assertSystemMetadata(t, gw.Header(), "GetObject")

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta/doc", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertSystemMetadata(t, hw.Header(), "HeadObject")
}

// TestS3_SystemMetadata_PutObjectPerHeader asserts each header survives on its own,
// so the round-trip test cannot pass on one header's behalf. Setting one also pins
// that it does not cause the others to be emitted.
func TestS3_SystemMetadata_PutObjectPerHeader(t *testing.T) {
	for _, h := range s3SystemMetadataHeaders {
		t.Run(h.header, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-one", nil, nil).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-one/obj", []byte("x"),
					map[string]string{h.header: h.value}).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sysmeta-one/obj", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assert.Equal(t, h.value, hw.Header().Get(h.header))

			for _, other := range s3SystemMetadataHeaders {
				if other.header == h.header {
					continue
				}
				_, present := hw.Header()[http.CanonicalHeaderKey(other.header)]
				assert.False(t, present, "%s must stay absent", other.header)
			}
		})
	}
}

// TestS3_SystemMetadata_AbsentHeadersNotEmitted asserts an object written without
// the family produces no family headers on a read — the acceptance criterion that
// absent headers are absent rather than empty.
func TestS3_SystemMetadata_AbsentHeadersNotEmitted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-bare", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-bare/obj", []byte("x"), nil).Code)

	gw := s3Request(t, srv, http.MethodGet, "/sysmeta-bare/obj", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assertNoSystemMetadata(t, gw.Header(), "GetObject")

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-bare/obj", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSystemMetadata(t, hw.Header(), "HeadObject")
}

// TestS3_SystemMetadata_ExpiresStoredVerbatim asserts Expires is never parsed as a
// date: an unparseable value round-trips unchanged rather than being normalized or
// dropped.
//
// This is the deliberate design decision on Expires the issue asked to record. S3
// stores and returns what the caller sent; the Go SDK's own GetObject output
// deprecates its time.Time Expires in favor of ExpiresString, "the unparsed value
// of the Expires field from the service response". Parsing here would be lower
// fidelity, and would make a consumer's parse-failure branch unreachable — the
// vacuous-pass shape this whole issue is about.
func TestS3_SystemMetadata_ExpiresStoredVerbatim(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"RFC 1123 as an SDK sends it", "Wed, 21 Oct 2026 07:28:00 GMT"},
		{"non-GMT offset", "Wed, 21 Oct 2026 07:28:00 +0100"},
		{"not a date at all", "tomorrow-ish"},
		{"a max-age style value", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-exp", nil, nil).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-exp/obj", []byte("x"),
					map[string]string{"Expires": tt.value}).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sysmeta-exp/obj", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assert.Equal(t, tt.value, hw.Header().Get("Expires"),
				"Expires must round-trip verbatim, never reformatted")
		})
	}
}

// TestS3_SystemMetadata_HeaderCaseInsensitive asserts the family is read
// case-insensitively. An AWSRequest reaching a plugin has not always been through
// net/http's canonicalization, since substrate builds requests in-process too.
//
// It goes through the plugin directly rather than s3Request for exactly that reason:
// on the ServeHTTP path Go canonicalizes header names, so a lowercase header sent
// through the server would arrive canonical and the test would prove nothing.
func TestS3_SystemMetadata_HeaderCaseInsensitive(t *testing.T) {
	p := newS3PluginDirect(t)
	rctx := &emulator.RequestContext{AccountID: "000000000000", Region: "us-east-1"}

	// Operation carries the HTTP verb on entry; the plugin resolves the semantic
	// operation from it and the path, exactly as the server pipeline does.
	_, err := p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodPut, Path: "/case-bucket",
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)

	_, err = p.HandleRequest(rctx, &emulator.AWSRequest{
		Service:   "s3",
		Operation: http.MethodPut,
		Path:      "/case-bucket/obj",
		Body:      []byte("x"),
		Params:    map[string]string{},
		Headers: map[string]string{
			"cache-control":       "no-store",
			"CONTENT-DISPOSITION": "inline",
			"content-Language":    "fr",
			"expires":             "0",
		},
	})
	require.NoError(t, err)

	resp, err := p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodHead, Path: "/case-bucket/obj",
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Headers["Cache-Control"])
	assert.Equal(t, "inline", resp.Headers["Content-Disposition"])
	assert.Equal(t, "fr", resp.Headers["Content-Language"])
	assert.Equal(t, "0", resp.Headers["Expires"])
}

// TestS3_SystemMetadata_MultipartRoundTrip asserts the family supplied at
// CreateMultipartUpload reaches the assembled object. Create is the only place it
// can be supplied — CompleteMultipartUpload accepts no object-metadata headers — so
// a value not carried on the upload record is lost for good, which is the #406
// failure shape.
func TestS3_SystemMetadata_MultipartRoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-mpu", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sysmeta-mpu/big?uploads", nil, s3SystemMetadataAll())
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "sysmeta-mpu", "big", ir.UploadID, 1, []byte("a single part is exempt from the floor"))
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPost, "/sysmeta-mpu/big?uploadId="+ir.UploadID,
			completeBody(etag), nil).Code)

	gw := s3Request(t, srv, http.MethodGet, "/sysmeta-mpu/big", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assertSystemMetadata(t, gw.Header(), "GetObject after Complete")

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-mpu/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertSystemMetadata(t, hw.Header(), "HeadObject after Complete")
}

// TestS3_SystemMetadata_MultipartCompleteHeadersIgnored asserts the family cannot be
// supplied at Complete. Per the API reference, CompleteMultipartUpload takes only the
// checksum family, x-amz-mp-object-size, request-payer, SSE-C and the conditional
// headers — so a caller setting Cache-Control there gets no effect, and one set at
// Create is not overridden by a differing value at Complete.
func TestS3_SystemMetadata_MultipartCompleteHeadersIgnored(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-late", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sysmeta-late/big?uploads", nil,
		map[string]string{"Cache-Control": "max-age=60"})
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "sysmeta-late", "big", ir.UploadID, 1, []byte("body"))
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPost, "/sysmeta-late/big?uploadId="+ir.UploadID,
			completeBody(etag), map[string]string{
				"Cache-Control":       "no-store",
				"Content-Disposition": "attachment",
			}).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-late/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "max-age=60", hw.Header().Get("Cache-Control"),
		"Complete accepts no metadata headers; Create's value stands")
	_, present := hw.Header()[http.CanonicalHeaderKey("Content-Disposition")]
	assert.False(t, present, "a Content-Disposition set only at Complete is ignored")
}

// TestS3_SystemMetadata_MultipartAbsentNotEmitted asserts a multipart object created
// without the family emits none of it, so the carry cannot smuggle in empty headers.
func TestS3_SystemMetadata_MultipartAbsentNotEmitted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-mpu-bare", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sysmeta-mpu-bare/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "sysmeta-mpu-bare", "big", ir.UploadID, 1, []byte("body"))
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPost, "/sysmeta-mpu-bare/big?uploadId="+ir.UploadID,
			completeBody(etag), nil).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-mpu-bare/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSystemMetadata(t, hw.Header(), "HeadObject after bare Complete")
}

// TestS3_SystemMetadata_CopyObjectCopyDirective asserts COPY — the default —
// preserves the family from the source and ignores headers restated on the request:
// "when you copy an object, user-controlled system metadata and user-defined
// metadata are also copied", and all four are user-controlled.
func TestS3_SystemMetadata_CopyObjectCopyDirective(t *testing.T) {
	tests := []struct {
		name      string
		directive string
	}{
		{"default (no directive header)", ""},
		{"explicit COPY", "COPY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-cp", nil, nil).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-cp/src", []byte("payload"),
					s3SystemMetadataAll()).Code)

			headers := map[string]string{"X-Amz-Copy-Source": "/sysmeta-cp/src"}
			if tt.directive != "" {
				headers["x-amz-metadata-directive"] = tt.directive
			}
			// Restated values a COPY must ignore.
			headers["Cache-Control"] = "no-store"
			headers["Content-Language"] = "de"

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sysmeta-cp/dst", nil, headers).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sysmeta-cp/dst", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assertSystemMetadata(t, hw.Header(), "COPY destination")
		})
	}
}

// TestS3_SystemMetadata_CopyObjectReplaceDirective asserts REPLACE drops what the
// request does not restate and takes what it does. The single
// x-amz-metadata-directive governs the whole family — S3 documents no per-header
// variant — so a REPLACE restating only Content-Type loses the download name a
// consumer set, which is the failure being modeled rather than a shortcut.
func TestS3_SystemMetadata_CopyObjectReplaceDirective(t *testing.T) {
	t.Run("omitted metadata is dropped", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl", nil, nil).Code)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl/src", []byte("payload"),
				s3SystemMetadataAll()).Code)

		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl/dst", nil, map[string]string{
				"X-Amz-Copy-Source":        "/sysmeta-repl/src",
				"x-amz-metadata-directive": "REPLACE",
				"Content-Type":             "text/plain",
			}).Code)

		hw := s3Request(t, srv, http.MethodHead, "/sysmeta-repl/dst", nil, nil)
		require.Equal(t, http.StatusOK, hw.Code)
		assertNoSystemMetadata(t, hw.Header(), "REPLACE destination")
		assert.Equal(t, "text/plain", hw.Header().Get("Content-Type"),
			"the one restated header is kept")
	})

	t.Run("restated metadata is kept", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl2", nil, nil).Code)
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl2/src", []byte("payload"),
				map[string]string{"Cache-Control": "max-age=60"}).Code)

		headers := s3SystemMetadataAll()
		headers["X-Amz-Copy-Source"] = "/sysmeta-repl2/src"
		headers["x-amz-metadata-directive"] = "REPLACE"
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/sysmeta-repl2/dst", nil, headers).Code)

		hw := s3Request(t, srv, http.MethodHead, "/sysmeta-repl2/dst", nil, nil)
		require.Equal(t, http.StatusOK, hw.Code)
		assertSystemMetadata(t, hw.Header(), "REPLACE destination with everything restated")
	})

	t.Run("each header is independently replaceable", func(t *testing.T) {
		for _, h := range s3SystemMetadataHeaders {
			t.Run(h.header, func(t *testing.T) {
				srv, _ := newS3TestServer(t)
				require.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, "/sysmeta-repl3", nil, nil).Code)
				require.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, "/sysmeta-repl3/src", []byte("payload"),
						s3SystemMetadataAll()).Code)

				require.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, "/sysmeta-repl3/dst", nil, map[string]string{
						"X-Amz-Copy-Source":        "/sysmeta-repl3/src",
						"x-amz-metadata-directive": "REPLACE",
						h.header:                   h.value,
					}).Code)

				hw := s3Request(t, srv, http.MethodHead, "/sysmeta-repl3/dst", nil, nil)
				require.Equal(t, http.StatusOK, hw.Code)
				assert.Equal(t, h.value, hw.Header().Get(h.header), "the restated header survives")
				for _, other := range s3SystemMetadataHeaders {
					if other.header == h.header {
						continue
					}
					_, present := hw.Header()[http.CanonicalHeaderKey(other.header)]
					assert.False(t, present, "%s was not restated and must be dropped", other.header)
				}
			})
		}
	})
}

// TestS3_SystemMetadata_CopyObjectDoesNotAliasSource asserts a COPY leaves the
// destination's metadata independent of the source's, so overwriting one does not
// change the other. S3SystemMetadata is a value type, which is what makes this hold
// — unlike UserMetadata, which needs an explicit map copy.
func TestS3_SystemMetadata_CopyObjectDoesNotAliasSource(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-alias", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-alias/src", []byte("payload"),
			map[string]string{"Cache-Control": "max-age=60"}).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-alias/dst", nil, map[string]string{
			"X-Amz-Copy-Source": "/sysmeta-alias/src",
		}).Code)

	// Overwrite the source with a different value.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-alias/src", []byte("payload"),
			map[string]string{"Cache-Control": "no-store"}).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-alias/dst", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "max-age=60", hw.Header().Get("Cache-Control"),
		"the copy keeps the value it was made from")
}

// TestS3_SystemMetadata_SurvivesVersioning asserts the family is recorded on a
// versioned object and returned when that version is read by ID, since a versioned
// PUT marshals the object a second time under its own key.
func TestS3_SystemMetadata_SurvivesVersioning(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-ver", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-ver?versioning",
			[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil).Code)

	pw := s3Request(t, srv, http.MethodPut, "/sysmeta-ver/obj", []byte("v1"), s3SystemMetadataAll())
	require.Equal(t, http.StatusOK, pw.Code)
	versionID := pw.Header().Get("x-amz-version-id")
	require.NotEmpty(t, versionID)

	// A second write with none of the family, so reading the first version by ID
	// cannot be satisfied by the current object.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-ver/obj", []byte("v2"), nil).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sysmeta-ver/obj?versionId="+versionID, nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertSystemMetadata(t, hw.Header(), "HeadObject by versionId")

	cw := s3Request(t, srv, http.MethodHead, "/sysmeta-ver/obj", nil, nil)
	require.Equal(t, http.StatusOK, cw.Code)
	assertNoSystemMetadata(t, cw.Header(), "current version, written without the family")
}

// TestS3_SystemMetadata_RangedReadCarriesHeaders asserts a 206 response carries the
// family too. A ranged read builds its own Content-Length and Content-Range over the
// common header set, so it is worth pinning that it does not lose the rest.
func TestS3_SystemMetadata_RangedReadCarriesHeaders(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-range", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sysmeta-range/obj", []byte("0123456789"),
			s3SystemMetadataAll()).Code)

	gw := s3Request(t, srv, http.MethodGet, "/sysmeta-range/obj", nil,
		map[string]string{"Range": "bytes=2-5"})
	require.Equal(t, http.StatusPartialContent, gw.Code)
	assertSystemMetadata(t, gw.Header(), "ranged GetObject")
}
