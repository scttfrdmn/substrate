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
// Authorization header, and a request that names no credential the server knows
// resolves to the one default account (#734). So every caller in them is the same
// caller, which is why both #623 and #624 — two different services answering as if
// there were only ever one account — survived a green suite.
//
// Substrate used to hand out a second account for free, to any request whose access
// key did not start with "AKIA", and a handful of tests used that as their way of
// being two callers. It was never a second account in any sense a consumer could
// rely on — nothing on the wire names an account — so those tests now sign, like
// these ones.
//
// Reaching a second identity means a CredentialRegistry, and a registry is also
// what switches SigV4 verification on, so these requests carry real signatures.
// The signing algorithm is reimplemented here rather than called into the
// emulator, so a mistake in substrate's own signing cannot make a test pass by
// agreeing with it.

// sigV4TestDateTime is the instant every signature in the suite is computed at. It
// is fixed rather than time.Now because the signature covers the date, and a test
// that depends on the wall clock is exactly what CLAUDE.md forbids. Substrate does
// not check a signature's age, so an instant in the past signs as well as any.
const sigV4TestDateTime = "20260101T120000Z"

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

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", op, err)
	}
	req.Host = tgt.host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", tgt.target+"."+op)
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(
		http.MethodPost, "/", tgt.host, tgt.signingName, "us-east-1", sigV4TestDateTime, data,
		creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s as %s: %v", op, account, err)
	}
	return resp
}

// sigV4Header computes a valid SigV4 Authorization header for a request to path
// with the host and x-amz-date headers signed.
//
// method and path are parameters because both are covered by the canonical request:
// a rest-json service carries its operation in the URL — the account service posts
// to /listRegions and the like — and S3 addresses a bucket with a HEAD. Signing "/"
// or POST for a request sent otherwise produces a signature the server correctly
// refuses.
//
// region is a parameter for the same reason it is part of the credential scope: the
// parser reads the Region off the scope, so a test asserting on a Region other than
// us-east-1 has to sign for it.
func sigV4Header(method, path, host, signingName, region, dateTime string, body []byte, accessKey, secretKey string) string {
	const signedHeaders = "host;x-amz-date"
	date := dateTime[:8]

	bodyHash := sha256.Sum256(body)
	canonicalReq := strings.Join([]string{
		method,
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

// signAs sets the Authorization and X-Amz-Date headers on req so the server
// attributes it to creds' account, for a test that builds its own request rather
// than going through [signedRequest].
//
// A zero creds leaves req unsigned, which is how a test asks to be the server's
// default account: [emulator.VerifySigV4] passes a request carrying no
// Authorization header, so an unsigned request reaches its plugin even against a
// server with a credential registry wired.
func signAs(req *http.Request, creds emulator.CredentialEntry, signingName, region string, body []byte) {
	if creds.AccessKeyID == "" {
		return
	}
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(req.Method, req.URL.Path, req.Host, signingName,
		region, sigV4TestDateTime, body, creds.AccessKeyID, creds.SecretAccessKey))
}

// testCredentialsFor derives a signing credential for an account the same way
// [emulator.TestServer.RegisterAccount] does, for a test that builds its own server
// and registry rather than using [emulator.StartTestServerWithAccounts].
//
// The derivation is duplicated rather than exported because it is a convention, not
// a contract: what matters is that the key is 20 uppercase alphanumerics, which is
// what an AWS access key ID is and what resolvePrincipal will accept.
func testCredentialsFor(accountID string) emulator.CredentialEntry {
	return emulator.CredentialEntry{
		AccessKeyID:     "AKIA" + accountID + "TEST",
		SecretAccessKey: "substrate-secret-" + accountID,
		AccountID:       accountID,
	}
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
