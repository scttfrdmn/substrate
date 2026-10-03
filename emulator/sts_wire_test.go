package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSTSWire_SessionCredentialResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for STSSessionCredentials (#756).
//
// STSSessionCredentials declares AccountID as `json:"AccountId"`, never under omitempty, so a
// session that exists holds it and no absence below is vacuous. The record never reaches a body:
// AssumeRole and GetSessionToken answer an XML struct declared for their operation, copying the four
// published Credentials members out of it. The record is read back on every request signed with the
// session's key, so GetCallerIdentity is driven that way too. It publishes the account as `Account`,
// a different name from `AccountId`, and the walk is a case-folded equality, not a substring test.
func TestSTSWire_SessionCredentialResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	srv, state, _ := newSTSTestServer(t)

	iamReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"RoleName":"wire-role"}`))
	iamReq.Host = "iam.amazonaws.com"
	iamReq.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateRole")
	iamReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	iamW := httptest.NewRecorder()
	srv.ServeHTTP(iamW, iamReq)
	require.Equal(t, http.StatusOK, iamW.Code, "CreateRole: %s", iamW.Body)

	read := func(op string, resp *http.Response) []byte {
		t.Helper()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "read %s", op)
		require.NoError(t, resp.Body.Close(), "close %s", op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, body)
		return body
	}
	keyOf := func(op string, body []byte) string {
		t.Helper()
		m := regexp.MustCompile(`<AccessKeyId>(ASIA[^<]+)</AccessKeyId>`).FindSubmatch(body)
		require.NotNilf(t, m, "%s must mint a session key: %s", op, body)
		key := string(m[1])
		data, err := state.Get(context.Background(), "sts", "session:"+key)
		require.NoError(t, err, "state.Get session:%s", key)
		require.NotNil(t, data, "no session record stored for %s", key)
		var record map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &record), "decode session:%s: %s", key, data)
		require.JSONEqf(t, `"123456789012"`, string(record["AccountId"]),
			"the %s session must persist AccountId before an absence assertion on it means anything", op)
		return key
	}

	assumed := read("AssumeRole", stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleArn": "arn:aws:iam::123456789012:role/wire-role", "RoleSessionName": "wire-session",
	}))
	roleKey := keyOf("AssumeRole", assumed)
	token := read("GetSessionToken", stsRequest(t, srv, "GetSessionToken", nil))
	keyOf("GetSessionToken", token)

	// Signed with the assumed-role session's key: the server resolves the caller from the record.
	r := httptest.NewRequest(http.MethodPost, "/?Action=GetCallerIdentity", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+roleKey+"/20250101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=abc")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	caller := read("GetCallerIdentity", w.Result())

	for _, tc := range []struct {
		op     string
		body   []byte
		anchor string
	}{
		{"AssumeRole", assumed, "<AssumedRoleId>"},
		{"GetSessionToken", token, "<SessionToken>"},
		// The session record is what names this caller, so the anchor is the assumed-role ARN.
		{"GetCallerIdentity", caller, "assumed-role/wire-role/wire-session"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			require.Containsf(t, string(tc.body), tc.anchor, "presence anchor: %s has to render %s", tc.op, tc.anchor)
			wireAssertNoMemberXML(t, tc.op, tc.body, []string{"AccountID"}, "")
		})
	}
}
