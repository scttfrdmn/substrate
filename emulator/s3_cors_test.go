package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A bucket's CORS configuration is recorded and answered back (#1278). The assertions are on the raw
// XML, so a member dropped or renamed on the way through is visible, not decoded away.

// s3CORSBody is a two-rule configuration in the published request shape, namespaced as an SDK sends
// it, exercising every API_CORSRule member: repeated AllowedMethod/AllowedOrigin/AllowedHeader/
// ExposeHeader, ID, and MaxAgeSeconds, including an explicit 0 on the second rule.
const s3CORSBody = `<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <CORSRule>
    <ID>browser-upload</ID>
    <AllowedOrigin>http://www.example.com</AllowedOrigin>
    <AllowedOrigin>https://app.example.com</AllowedOrigin>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedMethod>POST</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <ExposeHeader>x-amz-server-side-encryption</ExposeHeader>
    <MaxAgeSeconds>3000</MaxAgeSeconds>
  </CORSRule>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <MaxAgeSeconds>0</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>`

// s3CORSWant is what GetBucketCors answers for s3CORSBody: the same rules in API_CORSRule's element
// names, with nothing added for the members the second rule omits.
const s3CORSWant = `<CORSConfiguration>` +
	`<CORSRule><AllowedHeader>*</AllowedHeader><AllowedMethod>PUT</AllowedMethod><AllowedMethod>POST</AllowedMethod>` +
	`<AllowedOrigin>http://www.example.com</AllowedOrigin><AllowedOrigin>https://app.example.com</AllowedOrigin>` +
	`<ExposeHeader>ETag</ExposeHeader><ExposeHeader>x-amz-server-side-encryption</ExposeHeader>` +
	`<ID>browser-upload</ID><MaxAgeSeconds>3000</MaxAgeSeconds></CORSRule>` +
	`<CORSRule><AllowedMethod>GET</AllowedMethod><AllowedOrigin>*</AllowedOrigin><MaxAgeSeconds>0</MaxAgeSeconds></CORSRule>` +
	`</CORSConfiguration>`

func TestS3CORS_PutGetDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "cors-roundtrip"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

	// Absent before any put: the documented absence code, not an empty configuration.
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors", nil, nil)
	require.Equal(t, http.StatusNotFound, w.Code, "GetBucketCors on an unconfigured bucket: %s", w.Body.String())
	require.Equal(t, "NoSuchCORSConfiguration", parseS3Error(t, w.Body.Bytes()).Code)

	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"?cors", []byte(s3CORSBody), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutBucketCors: %s", w.Body.String())
	require.Empty(t, w.Body.String(), "PutBucketCors answers an empty body")

	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors=", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetBucketCors: %s", w.Body.String())
	require.Contains(t, w.Body.String(), s3CORSWant, "every rule member must round-trip verbatim")

	// A put replaces the configuration rather than merging into it.
	replacement := `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>HEAD</AllowedMethod></CORSRule></CORSConfiguration>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"?cors", []byte(replacement), nil).Code)
	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors", nil, nil)
	require.Contains(t, w.Body.String(), `<CORSConfiguration><CORSRule><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>`, "%s", w.Body.String())

	// Delete is 204, removes only the configuration, and is idempotent.
	for i := range 2 {
		w = s3Request(t, srv, http.MethodDelete, "/"+bucket+"?cors", nil, nil)
		require.Equalf(t, http.StatusNoContent, w.Code, "DeleteBucketCors #%d: %s", i+1, w.Body.String())
	}
	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors", nil, nil)
	require.Equal(t, "NoSuchCORSConfiguration", parseS3Error(t, w.Body.Bytes()).Code, "after delete: %s", w.Body.String())
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodHead, "/"+bucket, nil, nil).Code, "DeleteBucketCors must not delete the bucket")
}

func TestS3CORS_AMissingBucketIsNoSuchBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	for _, tc := range []struct {
		op, method string
		body       []byte
	}{
		{"PutBucketCors", http.MethodPut, []byte(s3CORSBody)},
		{"GetBucketCors", http.MethodGet, nil},
		{"DeleteBucketCors", http.MethodDelete, nil},
	} {
		t.Run(tc.op, func(t *testing.T) {
			w := s3Request(t, srv, tc.method, "/no-such-cors-bucket?cors", tc.body, nil)
			require.Equal(t, http.StatusNotFound, w.Code, "%s: %s", tc.op, w.Body.String())
			require.Equal(t, "NoSuchBucket", parseS3Error(t, w.Body.Bytes()).Code, "%s", tc.op)
		})
	}
}

func TestS3CORS_RefusesABodyThatDoesNotValidate(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "cors-refusals"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

	for _, tc := range []struct{ name, body string }{
		{"no body", ""},
		{"not XML", "<CORSConfiguration><CORSRule>"},
		{"no CORSRule", "<CORSConfiguration></CORSConfiguration>"},
		{"a rule with no AllowedMethod", "<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>"},
		{"a rule with no AllowedOrigin", "<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?cors", []byte(tc.body), nil)
			require.Equal(t, http.StatusBadRequest, w.Code, "%s: %s", tc.name, w.Body.String())
			require.Equal(t, "MalformedXML", parseS3Error(t, w.Body.Bytes()).Code, "%s", tc.name)
		})
	}
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors", nil, nil)
	require.Equal(t, "NoSuchCORSConfiguration", parseS3Error(t, w.Body.Bytes()).Code, "a refused put must store nothing: %s", w.Body.String())
}

// A deleted bucket takes its CORS configuration with it, so a bucket re-created under the name does
// not inherit it (#508's rule for every bucket-scoped configuration).
func TestS3CORS_DeleteBucketRemovesTheConfiguration(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "cors-inherit"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"?cors", []byte(s3CORSBody), nil).Code)
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/"+bucket, nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)

	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?cors", nil, nil)
	require.Equal(t, "NoSuchCORSConfiguration", parseS3Error(t, w.Body.Bytes()).Code, "%s", w.Body.String())
}

// A store fault at any read or write the CORS handlers make is an error, never answered as a
// configuration, an absence or a success.
func TestS3CORS_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		body   []byte
	}{
		{"PutBucketCors, bucket read", func(m *cfFaultStateManager) { m.failGet = "bucket:fault-cors" }, http.MethodPut, []byte(s3CORSBody)},
		{"PutBucketCors, write", func(m *cfFaultStateManager) { m.failPut = "bucket_cors:" }, http.MethodPut, []byte(s3CORSBody)},
		{"GetBucketCors, read", func(m *cfFaultStateManager) { m.failGet = "bucket_cors:" }, http.MethodGet, nil},
		{"GetBucketCors, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "bucket_cors:" }, http.MethodGet, nil},
		{"DeleteBucketCors, delete", func(m *cfFaultStateManager) { m.failDelete = "bucket_cors:" }, http.MethodDelete, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, fault)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fault-cors", nil, nil).Code)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fault-cors?cors", []byte(s3CORSBody), nil).Code)

			tc.arm(fault)
			w := s3Request(t, srv, tc.method, "/fault-cors?cors", tc.body, nil)
			require.GreaterOrEqualf(t, w.Code, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, w.Body.String())
		})
	}
}
