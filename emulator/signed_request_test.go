package emulator_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// This file holds the shared machinery for a test that needs to be *a particular
// account*. It exists because most of substrate's tests cannot be: they send no
// Authorization header, and extractAccount answers 000000000000 for a request
// without one and 123456789012 for any AKIA-prefixed key. So every caller in them
// is the same caller, which is why both #623 and #624 — two different services
// answering as if there were only ever one account — survived a green suite.
//
// Reaching a second identity means a CredentialRegistry, and a registry is also
// what switches SigV4 verification on, so these requests carry real signatures.
// The signing algorithm is reimplemented here rather than called into the
// emulator, so a mistake in substrate's own signing cannot make a test pass by
// agreeing with it.

// signedRequestTarget names the wire details of one service's JSON-target
// endpoint, so a signed request can be built for it.
type signedRequestTarget struct {
	// host is the Host header, which is one of the signals the parser uses to
	// route a request to a service.
	host string
	// target is the X-Amz-Target prefix, "Organizations_20161128" and the like.
	// The operation is appended to it.
	target string
	// signingName is the service field of the SigV4 credential scope. It is not
	// always the service name — Service Quotas signs as servicequotasv20190624 —
	// and VerifySigV4 derives the signing key from whatever is in the header, so
	// this only has to be what a real SDK would send.
	signingName string
}

// signedRequest posts an operation to a JSON-target service, signed as the given
// account.
//
// The account must have been registered with the server by
// [emulator.StartTestServerWithAccounts] or [emulator.TestServer.RegisterAccount];
// an unregistered key is InvalidClientTokenId 403 before any plugin sees it.
func signedRequest(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget,
	account, op string, body any,
) *http.Response {
	t.Helper()

	creds, ok := ts.CredentialsFor(account)
	if !ok {
		t.Fatalf("no credential registered for account %s", account)
	}

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s: %v", op, err)
	}

	// A fixed instant rather than time.Now: the signature covers the date, and a
	// test that depends on the wall clock is exactly what CLAUDE.md forbids.
	const dateTime = "20260101T120000Z"

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", op, err)
	}
	req.Host = tgt.host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", tgt.target+"."+op)
	req.Header.Set("X-Amz-Date", dateTime)
	req.Header.Set("Authorization", sigV4Header(
		"/", tgt.host, tgt.signingName, dateTime, data, creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s as %s: %v", op, account, err)
	}
	return resp
}

// sigV4Header computes a valid SigV4 Authorization header for a POST to path with
// the host and x-amz-date headers signed.
//
// path is a parameter because a rest-json service carries its operation in the URL
// — the account service posts to /listRegions and the like — and the canonical
// request covers the path, so signing "/" for a request sent elsewhere produces a
// signature the server correctly refuses.
func sigV4Header(path, host, signingName, dateTime string, body []byte, accessKey, secretKey string) string {
	const (
		region        = "us-east-1"
		signedHeaders = "host;x-amz-date"
	)
	date := dateTime[:8]

	bodyHash := sha256.Sum256(body)
	canonicalReq := strings.Join([]string{
		http.MethodPost,
		path,
		"",
		"host:" + host + "\n" + "x-amz-date:" + dateTime + "\n",
		signedHeaders,
		hex.EncodeToString(bodyHash[:]),
	}, "\n")

	canonicalHash := sha256.Sum256([]byte(canonicalReq))
	credScope := date + "/" + region + "/" + signingName + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + credScope + "\n" +
		hex.EncodeToString(canonicalHash[:])

	mac := func(key, data []byte) []byte {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		return h.Sum(nil)
	}
	signing := mac(mac(mac(mac([]byte("AWS4"+secretKey), []byte(date)), []byte(region)),
		[]byte(signingName)), []byte("aws4_request"))

	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credScope, signedHeaders, hex.EncodeToString(mac(signing, []byte(stringToSign))))
}

// decodeAWSResponse decodes a JSON-protocol response into out, returning the
// status and the error code a refusal carries.
//
// The code is returned separately rather than left in out because a refusal and a
// success have different shapes, and a test asserting on a refusal cares about
// which one it got — not about the message text, which is prose.
func decodeAWSResponse(t *testing.T, resp *http.Response, out any) (int, string) {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var errShape struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr == nil && errShape.Type != "" {
		return resp.StatusCode, errShape.Type
	}
	if out != nil {
		if unmarshalErr := json.Unmarshal(raw, out); unmarshalErr != nil {
			t.Fatalf("decode %s: %v", raw, unmarshalErr)
		}
	}
	return resp.StatusCode, ""
}
