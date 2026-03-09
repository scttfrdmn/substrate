package substrate_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestCredentialRegistry_RegisterLookup(t *testing.T) {
	reg := substrate.NewCredentialRegistry()

	entry := substrate.CredentialEntry{
		AccessKeyID:     "AKIAEXAMPLE00000001",
		SecretAccessKey: "secret1",
		AccountID:       "111122223333",
	}
	reg.Register(entry)

	got, ok := reg.Lookup("AKIAEXAMPLE00000001")
	require.True(t, ok)
	assert.Equal(t, entry.AccountID, got.AccountID)
	assert.Equal(t, entry.SecretAccessKey, got.SecretAccessKey)
}

func TestCredentialRegistry_DefaultTestEntry(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	entry, ok := reg.Lookup("AKIATEST12345678901")
	require.True(t, ok)
	assert.Equal(t, "123456789012", entry.AccountID)
}

func TestCredentialRegistry_NotFound(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	_, ok := reg.Lookup("AKIADOESNOTEXIST111")
	assert.False(t, ok)
}

func TestCredentialRegistry_Concurrent(_ *testing.T) {
	reg := substrate.NewCredentialRegistry()
	done := make(chan struct{})

	// Writers.
	for i := 0; i < 10; i++ {
		go func(i int) {
			reg.Register(substrate.CredentialEntry{
				AccessKeyID: fmt.Sprintf("AKIA%017d", i),
				AccountID:   fmt.Sprintf("%012d", i),
			})
			done <- struct{}{}
		}(i)
	}
	// Readers.
	for i := 0; i < 10; i++ {
		go func(i int) {
			reg.Lookup(fmt.Sprintf("AKIA%017d", i))
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestExtractAccessKeyFromAuth(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{
			name:   "valid SigV4",
			header: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=aaa",
			want:   "AKIAIOSFODNN7EXAMPLE",
		},
		{
			name:   "empty header",
			header: "",
			want:   "",
		},
		{
			name:   "no credential",
			header: "AWS4-HMAC-SHA256 SignedHeaders=host, Signature=bbb",
			want:   "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := substrate.ExtractAccessKeyFromAuthForTest(tc.header)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildCallerARN(t *testing.T) {
	arn := substrate.BuildCallerARNForTest("123456789012", "AKIATEST00000000001")
	assert.Equal(t, "arn:aws:iam::123456789012:user/AKIATEST00000000001", arn)
}

// computeSigV4Signature produces a valid SigV4 Authorization header value for
// testing purposes. It mirrors the algorithm in credentials.go.
func computeSigV4Signature(t *testing.T, method, urlStr string, body []byte, accessKey, secretKey, region, service, dateTime string) string {
	t.Helper()
	date := dateTime[:8]

	req := httptest.NewRequest(method, urlStr, bytes.NewReader(body))
	req.Header.Set("X-Amz-Date", dateTime)
	req.Host = req.URL.Host
	if req.Host == "" {
		req.Host = "s3.amazonaws.com"
	}

	signedHeaders := "host;x-amz-date"

	// Canonical headers.
	canonicalHeaders := "host:" + req.Host + "\n" +
		"x-amz-date:" + dateTime + "\n"

	// Body hash.
	bodyHash := sha256Hex(body)

	// Canonical request.
	canonicalReq := method + "\n" +
		req.URL.EscapedPath() + "\n" +
		"" + "\n" + // empty query
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		bodyHash

	// String to sign.
	credScope := date + "/" + region + "/" + service + "/aws4_request"
	strToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + credScope + "\n" + sha256Hex([]byte(canonicalReq))

	// Signing key.
	kDate := sigHMAC([]byte("AWS4"+secretKey), []byte(date))
	kRegion := sigHMAC(kDate, []byte(region))
	kService := sigHMAC(kRegion, []byte(service))
	kSigning := sigHMAC(kService, []byte("aws4_request"))

	sig := hex.EncodeToString(sigHMAC(kSigning, []byte(strToSign)))
	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credScope, signedHeaders, sig)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func sigHMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func TestVerifySigV4_NilRegistry(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=X/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc")
	err := substrate.VerifySigV4ForTest(r, nil, nil)
	assert.NoError(t, err, "nil registry should always pass")
}

func TestVerifySigV4_NoAuthHeader(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	err := substrate.VerifySigV4ForTest(r, nil, reg)
	assert.NoError(t, err, "absent Authorization should pass")
}

func TestVerifySigV4_UnknownKey(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAUNKNOWNKEY000001/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=bad")
	r.Header.Set("X-Amz-Date", "20260101T000000Z")
	err := substrate.VerifySigV4ForTest(r, nil, reg)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "InvalidClientTokenId", awsErr.Code)
}

func TestVerifySigV4_ValidSignature(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	accessKey := "AKIATEST12345678901"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	body := []byte("test body")
	dateTime := "20260101T120000Z"

	authHeader := computeSigV4Signature(t, http.MethodPut, "http://s3.amazonaws.com/my-bucket/key", body, accessKey, secretKey, "us-east-1", "s3", dateTime)

	r := httptest.NewRequest(http.MethodPut, "http://s3.amazonaws.com/my-bucket/key", bytes.NewReader(body))
	r.Host = "s3.amazonaws.com"
	r.Header.Set("Authorization", authHeader)
	r.Header.Set("X-Amz-Date", dateTime)

	err := substrate.VerifySigV4ForTest(r, body, reg)
	assert.NoError(t, err)
}

func TestVerifySigV4_InvalidSignature(t *testing.T) {
	reg := substrate.NewCredentialRegistry()
	accessKey := "AKIATEST12345678901"
	body := []byte("test body")
	dateTime := "20260101T120000Z"
	date := dateTime[:8]

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		accessKey, date)

	r := httptest.NewRequest(http.MethodPut, "/my-bucket/key", bytes.NewReader(body))
	r.Host = "s3.amazonaws.com"
	r.Header.Set("Authorization", authHeader)
	r.Header.Set("X-Amz-Date", dateTime)

	err := substrate.VerifySigV4ForTest(r, body, reg)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "SignatureDoesNotMatch", awsErr.Code)
}
