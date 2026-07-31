package emulator_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// rangeFixture creates a bucket holding one 1000-byte object whose bytes are
// distinguishable by position, so a served range can be checked for content and
// not merely length.
func rangeFixture(t *testing.T) (*emulator.Server, []byte) {
	t.Helper()
	srv, _ := newS3TestServer(t)

	body := make([]byte, 1000)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/ranged", nil, nil).Code, "create bucket")
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/ranged/obj.bin", body, nil).Code, "put object")

	return srv, body
}

func TestS3_GetObject_Range(t *testing.T) {
	tests := []struct {
		name        string
		rangeHeader string
		wantStatus  int
		wantRange   string // expected Content-Range, "" when none
		wantStart   int64  // body slice bounds, used when wantStatus is 200 or 206
		wantEnd     int64  // exclusive
	}{
		{
			name:        "prefix range",
			rangeHeader: "bytes=0-99", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 0-99/1000", wantStart: 0, wantEnd: 100,
		},
		{
			name:        "open ended range to EOF",
			rangeHeader: "bytes=900-", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 900-999/1000", wantStart: 900, wantEnd: 1000,
		},
		{
			name:        "suffix range",
			rangeHeader: "bytes=-100", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 900-999/1000", wantStart: 900, wantEnd: 1000,
		},
		{
			name:        "range past EOF is clamped not rejected",
			rangeHeader: "bytes=0-99999", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 0-999/1000", wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "single byte",
			rangeHeader: "bytes=0-0", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 0-0/1000", wantStart: 0, wantEnd: 1,
		},
		{
			name:        "whole object as an explicit range is still 206",
			rangeHeader: "bytes=0-999", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 0-999/1000", wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "suffix longer than the object clamps to the whole object",
			rangeHeader: "bytes=-5000", wantStatus: http.StatusPartialContent,
			wantRange: "bytes 0-999/1000", wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "start beyond EOF is unsatisfiable",
			rangeHeader: "bytes=1000-1099", wantStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:        "start exactly at EOF is unsatisfiable",
			rangeHeader: "bytes=1000-", wantStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:        "zero-length suffix is unsatisfiable",
			rangeHeader: "bytes=-0", wantStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:        "unparseable range is ignored",
			rangeHeader: "bytes=abc", wantStatus: http.StatusOK, wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "multiple ranges are ignored: S3 serves the whole object",
			rangeHeader: "bytes=0-99,200-299", wantStatus: http.StatusOK, wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "non-bytes unit is ignored",
			rangeHeader: "items=0-99", wantStatus: http.StatusOK, wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "end before start is ignored",
			rangeHeader: "bytes=500-100", wantStatus: http.StatusOK, wantStart: 0, wantEnd: 1000,
		},
		{
			name:        "absent header serves the whole object",
			rangeHeader: "", wantStatus: http.StatusOK, wantStart: 0, wantEnd: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, full := rangeFixture(t)

			var headers map[string]string
			if tt.rangeHeader != "" {
				headers = map[string]string{"Range": tt.rangeHeader}
			}
			w := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, headers)

			require.Equal(t, tt.wantStatus, w.Code)

			if tt.wantStatus == http.StatusRequestedRangeNotSatisfiable {
				// RFC 9110 requires the unsatisfied-range form here.
				assert.Equal(t, "bytes */1000", w.Header().Get("Content-Range"))
				return
			}
			assert.Equal(t, tt.wantRange, w.Header().Get("Content-Range"))

			assert.Equal(t, full[tt.wantStart:tt.wantEnd], w.Body.Bytes(),
				"served bytes must be exactly the requested range")

			// The desync guard: writeResponse only defaults Content-Length when
			// the plugin supplied none, and the object header block always
			// supplies the full size. A 206 that forgets to overwrite it sends a
			// short body under a full-object length and hangs SDK clients.
			assert.Equal(t, strconv.Itoa(w.Body.Len()), w.Header().Get("Content-Length"),
				"Content-Length must match the bytes actually written")
		})
	}
}

func TestS3_GetObject_Range_AcceptRangesAndMetadataMatchFullGet(t *testing.T) {
	srv, _ := rangeFixture(t)

	full := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, nil)
	require.Equal(t, http.StatusOK, full.Code)
	assert.Equal(t, "bytes", full.Header().Get("Accept-Ranges"),
		"S3 advertises range support on an unranged GET too")

	part := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, map[string]string{"Range": "bytes=10-19"})
	require.Equal(t, http.StatusPartialContent, part.Code)

	// A ranged read describes the same object, so its identity headers must be
	// byte-identical to the full read's.
	for _, h := range []string{"ETag", "Last-Modified", "Content-Type", "Accept-Ranges"} {
		assert.Equal(t, full.Header().Get(h), part.Header().Get(h), "header %s", h)
	}
}

func TestS3_GetObject_Range_InvalidRangeBody(t *testing.T) {
	srv, _ := rangeFixture(t)

	w := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, map[string]string{"Range": "bytes=2000-2099"})
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)

	assert.Equal(t, "bytes */1000", w.Header().Get("Content-Range"),
		"RFC 9110 requires the actual length on a 416")

	body := w.Body.String()
	assert.Contains(t, body, "<Code>InvalidRange</Code>")
	assert.Contains(t, body, "<ActualObjectSize>1000</ActualObjectSize>")
	assert.Contains(t, body, "<RangeRequested>bytes=2000-2099</RangeRequested>")
}

func TestS3_GetObject_Range_ZeroByteObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/empty", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/empty/zero.bin", []byte{}, nil).Code)

	// Any range against an empty object is unsatisfiable — there is no byte 0.
	for _, rh := range []string{"bytes=0-0", "bytes=0-", "bytes=-1"} {
		t.Run(rh, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/empty/zero.bin", nil, map[string]string{"Range": rh})
			require.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)
			assert.Equal(t, "bytes */0", w.Header().Get("Content-Range"))
		})
	}

	// Without a Range header an empty object is a normal 200.
	w := s3Request(t, srv, http.MethodGet, "/empty/zero.bin", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 0, w.Body.Len())
}

func TestS3_HeadObject_Range(t *testing.T) {
	srv, _ := rangeFixture(t)

	w := s3Request(t, srv, http.MethodHead, "/ranged/obj.bin", nil, map[string]string{"Range": "bytes=100-199"})
	require.Equal(t, http.StatusPartialContent, w.Code, "HEAD honors Range like GET")
	assert.Equal(t, "bytes 100-199/1000", w.Header().Get("Content-Range"))
	assert.Equal(t, "100", w.Header().Get("Content-Length"),
		"Content-Length reports the range length even with no body")
	assert.Equal(t, 0, w.Body.Len(), "HEAD never carries a body")

	unsat := s3Request(t, srv, http.MethodHead, "/ranged/obj.bin", nil, map[string]string{"Range": "bytes=5000-"})
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, unsat.Code)
	assert.Equal(t, "bytes */1000", unsat.Header().Get("Content-Range"))
}

func TestS3_HeadObject_EmitsVersionID(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/hver", nil, nil).Code)
	versioning := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Status>Enabled</Status></VersioningConfiguration>`
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/hver?versioning", []byte(versioning),
			map[string]string{"Content-Type": "application/xml"}).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/hver/k.txt", []byte("v1"), nil).Code)

	g := s3Request(t, srv, http.MethodGet, "/hver/k.txt", nil, nil)
	require.Equal(t, http.StatusOK, g.Code)
	vid := g.Header().Get("x-amz-version-id")
	require.NotEmpty(t, vid, "GET reports the version id")

	h := s3Request(t, srv, http.MethodHead, "/hver/k.txt", nil, nil)
	require.Equal(t, http.StatusOK, h.Code)
	assert.Equal(t, vid, h.Header().Get("x-amz-version-id"),
		"HEAD must report the same version id as GET")
}

func TestS3_GetObject_Range_WithVersionID(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/rver", nil, nil).Code)
	versioning := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Status>Enabled</Status></VersioningConfiguration>`
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/rver?versioning", []byte(versioning),
			map[string]string{"Content-Type": "application/xml"}).Code)

	first := s3Request(t, srv, http.MethodPut, "/rver/k.txt", []byte("AAAABBBBCCCC"), nil)
	require.Equal(t, http.StatusOK, first.Code)
	v1 := first.Header().Get("x-amz-version-id")
	require.NotEmpty(t, v1)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/rver/k.txt", []byte("ZZZZZZZZZZZZZZZZ"), nil).Code)

	// A range must apply to the named version, not the current one.
	w := s3Request(t, srv, http.MethodGet, "/rver/k.txt?versionId="+v1, nil,
		map[string]string{"Range": "bytes=4-7"})
	require.Equal(t, http.StatusPartialContent, w.Code)
	assert.Equal(t, "BBBB", w.Body.String())
	assert.Equal(t, "bytes 4-7/12", w.Header().Get("Content-Range"))
	assert.Equal(t, v1, w.Header().Get("x-amz-version-id"))
	assert.Equal(t, strconv.Itoa(w.Body.Len()), w.Header().Get("Content-Length"))
}

func TestS3_GetObject_Range_HeaderCaseInsensitive(t *testing.T) {
	srv, _ := rangeFixture(t)

	// Plugins receive canonicalized header keys through the parser, but tests and
	// alternate transports may not canonicalize, so the lookup must fold case.
	for _, name := range []string{"Range", "range", "RANGE"} {
		t.Run(name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, map[string]string{name: "bytes=0-9"})
			require.Equal(t, http.StatusPartialContent, w.Code)
			assert.Equal(t, "bytes 0-9/1000", w.Header().Get("Content-Range"))
		})
	}
}

func TestS3_GetObject_Range_TaskCompletionRecord(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/results", nil, nil).Code)

	// The synthesized spore.host completion record now flows through the same
	// response path as a stored object, so it is rangeable too (#360, #396).
	full := s3Request(t, srv, http.MethodGet, "/results/tasks/task-r/completion.json", nil, nil)
	require.Equal(t, http.StatusOK, full.Code)
	require.Contains(t, full.Body.String(), `"state":"completed"`)
	assert.Equal(t, "bytes", full.Header().Get("Accept-Ranges"))

	w := s3Request(t, srv, http.MethodGet, "/results/tasks/task-r/completion.json", nil,
		map[string]string{"Range": "bytes=0-3"})
	require.Equal(t, http.StatusPartialContent, w.Code)
	assert.Equal(t, full.Body.String()[:4], w.Body.String())
	assert.Equal(t, strconv.Itoa(full.Body.Len()),
		strings.SplitN(w.Header().Get("Content-Range"), "/", 2)[1],
		"the /total in Content-Range is the whole record's length")
}

func TestS3_ErrorResponse_DetailElements(t *testing.T) {
	srv, _ := rangeFixture(t)

	// The dynamic detail elements rely on encoding/xml resolving an element name
	// from the XMLName field's value ahead of the parent field's tag. Assert the
	// exact bytes, since a change in that precedence would silently rename them.
	w := s3Request(t, srv, http.MethodGet, "/ranged/obj.bin", nil, map[string]string{"Range": "bytes=4096-"})
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)

	assert.Equal(t,
		`<?xml version="1.0" encoding="UTF-8"?>`+"\n"+
			`<Error><Code>InvalidRange</Code>`+
			`<Message>The requested range cannot be satisfied.</Message>`+
			`<RequestId>SUBSTRATE</RequestId>`+
			`<RangeRequested>bytes=4096-</RangeRequested>`+
			`<ActualObjectSize>1000</ActualObjectSize></Error>`,
		w.Body.String())
}

func TestS3_ErrorResponse_NoDetailsIsUnchanged(t *testing.T) {
	srv, _ := newS3TestServer(t)

	// An error with no detail elements must serialize exactly as before the
	// builder gained the Details field — 61 call sites depend on this shape.
	w := s3Request(t, srv, http.MethodGet, "/nosuchbucket/nosuchkey", nil, nil)
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.NotContains(t, w.Body.String(), "<Details>")
	assert.Contains(t, w.Body.String(), "<RequestId>SUBSTRATE</RequestId></Error>")
}
