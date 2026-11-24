package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// idpCall issues an IAM query-protocol request and returns its status and body.
func idpCall(t *testing.T, srv *emulator.Server, op string, params map[string]string) (int, string) {
	t.Helper()
	resp := iamFormRequest(t, srv, op, params)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, string(body)
}

// idpOK is idpCall for a call that must succeed.
func idpOK(t *testing.T, srv *emulator.Server, op string, params map[string]string) string {
	t.Helper()
	status, body := idpCall(t, srv, op, params)
	require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
	return body
}

// idpErrorCode returns the Code of an IAM error document.
func idpErrorCode(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		Code string `xml:"Error>Code"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	return doc.Code
}

// controlCall issues a control-plane request against srv.
func controlCall(t *testing.T, srv *emulator.Server, method, path, body string) int {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Code
}

const ghaThumbprint = "6938fd4d98bab03faadb97b34396831e3780aea1"

func TestIAMOIDCProvider_Lifecycle(t *testing.T) {
	srv := newIAMTestServer(t)
	const arn = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"

	body := idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{
		"Url":                     "https://token.actions.githubusercontent.com",
		"ClientIDList.member.1":   "sts.amazonaws.com",
		"ThumbprintList.member.1": ghaThumbprint,
		"Tags.member.1.Key":       "team",
		"Tags.member.1.Value":     "ci",
	})
	assert.Contains(t, body, "<OpenIDConnectProviderArn>"+arn+"</OpenIDConnectProviderArn>")
	assert.Contains(t, body, "<Key>team</Key>")

	var got struct {
		URL         string   `xml:"GetOpenIDConnectProviderResult>Url"`
		ClientIDs   []string `xml:"GetOpenIDConnectProviderResult>ClientIDList>member"`
		Thumbprints []string `xml:"GetOpenIDConnectProviderResult>ThumbprintList>member"`
		CreateDate  string   `xml:"GetOpenIDConnectProviderResult>CreateDate"`
	}
	get := func() {
		got.ClientIDs, got.Thumbprints = nil, nil
		require.NoError(t, xml.Unmarshal([]byte(idpOK(t, srv, "GetOpenIDConnectProvider",
			map[string]string{"OpenIDConnectProviderArn": arn})), &got))
	}
	get()
	assert.Equal(t, "token.actions.githubusercontent.com", got.URL, "Url is reported without its scheme")
	assert.Equal(t, []string{"sts.amazonaws.com"}, got.ClientIDs)
	assert.Equal(t, []string{ghaThumbprint}, got.Thumbprints)
	assert.NotEmpty(t, got.CreateDate)

	// Adding an existing client ID and removing an absent one both succeed: each page says the
	// operation "is idempotent".
	idpOK(t, srv, "AddClientIDToOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn, "ClientID": "sts.amazonaws.com"})
	idpOK(t, srv, "AddClientIDToOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn, "ClientID": "app-2"})
	idpOK(t, srv, "RemoveClientIDFromOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn, "ClientID": "never-added"})
	get()
	assert.Equal(t, []string{"sts.amazonaws.com", "app-2"}, got.ClientIDs)
	idpOK(t, srv, "RemoveClientIDFromOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn, "ClientID": "app-2"})

	// The thumbprint list is replaced, not merged.
	other := strings.Repeat("a", 40)
	idpOK(t, srv, "UpdateOpenIDConnectProviderThumbprint", map[string]string{
		"OpenIDConnectProviderArn": arn, "ThumbprintList.member.1": other})
	get()
	assert.Equal(t, []string{other}, got.Thumbprints)
	assert.Equal(t, []string{"sts.amazonaws.com"}, got.ClientIDs)

	idpOK(t, srv, "TagOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn,
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"})
	idpOK(t, srv, "UntagOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn, "TagKeys.member.1": "team"})
	tags := idpOK(t, srv, "ListOpenIDConnectProviderTags", map[string]string{"OpenIDConnectProviderArn": arn})
	assert.Contains(t, tags, "<Key>env</Key>")
	assert.NotContains(t, tags, "<Key>team</Key>")

	list := idpOK(t, srv, "ListOpenIDConnectProviders", nil)
	assert.Contains(t, list, "<Arn>"+arn+"</Arn>")

	idpOK(t, srv, "DeleteOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	// The delete page makes no idempotency statement and declares NoSuchEntity.
	status, body := idpCall(t, srv, "DeleteOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "NoSuchEntity", idpErrorCode(t, body))
	status, body = idpCall(t, srv, "GetOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": arn})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, "OpenIDConnect Provider not found for arn "+arn)
	assert.NotContains(t, idpOK(t, srv, "ListOpenIDConnectProviders", nil), "<member>")
}

func TestIAMOIDCProvider_Refusals(t *testing.T) {
	srv := newIAMTestServer(t)
	const url = "https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": url, "ClientIDList.member.1": "sts.amazonaws.com"})
	const arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"

	six := map[string]string{"Url": "https://six.example.com"}
	for i := 1; i <= 6; i++ {
		six["ThumbprintList.member."+string(rune('0'+i))] = strings.Repeat(string(rune('a'+i)), 40)
	}
	cases := []struct {
		name   string
		op     string
		params map[string]string
		status int
		code   string
	}{
		{"duplicate url", "CreateOpenIDConnectProvider", map[string]string{"Url": url}, http.StatusConflict, "EntityAlreadyExists"},
		{"six thumbprints", "CreateOpenIDConnectProvider", six, http.StatusConflict, "LimitExceeded"},
		{"short thumbprint", "CreateOpenIDConnectProvider",
			map[string]string{"Url": "https://x.example.com", "ThumbprintList.member.1": "abc"}, http.StatusBadRequest, "ValidationError"},
		{"not https", "CreateOpenIDConnectProvider", map[string]string{"Url": "http://x.example.com"}, http.StatusBadRequest, "InvalidInput"},
		{"query", "CreateOpenIDConnectProvider", map[string]string{"Url": "https://x.example.com/?a=b"}, http.StatusBadRequest, "InvalidInput"},
		{"no url", "CreateOpenIDConnectProvider", map[string]string{}, http.StatusBadRequest, "ValidationError"},
		{"no arn", "GetOpenIDConnectProvider", map[string]string{}, http.StatusBadRequest, "ValidationError"},
		{"other account", "GetOpenIDConnectProvider",
			map[string]string{"OpenIDConnectProviderArn": strings.Replace(arn, "123456789012", "111122223333", 1)},
			http.StatusNotFound, "NoSuchEntity"},
		{"empty client id", "AddClientIDToOpenIDConnectProvider",
			map[string]string{"OpenIDConnectProviderArn": arn, "ClientID": ""}, http.StatusBadRequest, "ValidationError"},
		{"update six", "UpdateOpenIDConnectProviderThumbprint", func() map[string]string {
			m := map[string]string{"OpenIDConnectProviderArn": arn}
			for k, v := range six {
				if k != "Url" {
					m[k] = v
				}
			}
			return m
		}(), http.StatusConflict, "LimitExceeded"},
		{"tag missing", "TagOpenIDConnectProvider",
			map[string]string{"OpenIDConnectProviderArn": arn + "x", "Tags.member.1.Key": "k", "Tags.member.1.Value": "v"},
			http.StatusNotFound, "NoSuchEntity"},
		{"list tags missing", "ListOpenIDConnectProviderTags",
			map[string]string{"OpenIDConnectProviderArn": arn + "x"}, http.StatusNotFound, "NoSuchEntity"},
		{"untag missing", "UntagOpenIDConnectProvider",
			map[string]string{"OpenIDConnectProviderArn": arn + "x", "TagKeys.member.1": "k"}, http.StatusNotFound, "NoSuchEntity"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := idpCall(t, srv, tc.op, tc.params)
			assert.Equal(t, tc.status, status, body)
			assert.Equal(t, tc.code, idpErrorCode(t, body))
		})
	}
}

func TestIAMOIDCProvider_SeededCommunicationError(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/iam/oidc-provider-outcome",
		`{"url":"https://unreachable.example.com"}`))

	status, body := idpCall(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://unreachable.example.com"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "OpenIdIdpCommunicationError", idpErrorCode(t, body))
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://reachable.example.com"})

	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodDelete, "/v1/iam/oidc-provider-outcome", ""))
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://unreachable.example.com"})

	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/iam/oidc-provider-outcome", `{}`))
	status, _ = idpCall(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://any.example.com"})
	assert.Equal(t, http.StatusBadRequest, status, "the wildcard seed applies to every URL")
	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodDelete, "/v1/iam/oidc-provider-outcome?url=*", ""))
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://any.example.com"})
	assert.Equal(t, http.StatusBadRequest, controlCall(t, srv, http.MethodPost, "/v1/iam/oidc-provider-outcome", `{`))
}

// samlMetadata is a minimal SAML metadata document.
const samlMetadata = `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/saml" validUntil="2030-01-01T00:00:00Z">
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"/>
</md:EntityDescriptor>`

func TestIAMSAMLProvider_Lifecycle(t *testing.T) {
	srv := newIAMTestServer(t)
	const arn = "arn:aws:iam::123456789012:saml-provider/Corp"

	body := idpOK(t, srv, "CreateSAMLProvider", map[string]string{
		"Name": "Corp", "SAMLMetadataDocument": samlMetadata,
		"Tags.member.1.Key": "team", "Tags.member.1.Value": "id",
	})
	assert.Contains(t, body, "<SAMLProviderArn>"+arn+"</SAMLProviderArn>")

	var got struct {
		UUID       string   `xml:"GetSAMLProviderResult>SAMLProviderUUID"`
		ValidUntil string   `xml:"GetSAMLProviderResult>ValidUntil"`
		Metadata   string   `xml:"GetSAMLProviderResult>SAMLMetadataDocument"`
		Mode       string   `xml:"GetSAMLProviderResult>AssertionEncryptionMode"`
		KeyIDs     []string `xml:"GetSAMLProviderResult>PrivateKeyList>member>KeyId"`
	}
	get := func() {
		got.KeyIDs = nil
		require.NoError(t, xml.Unmarshal([]byte(idpOK(t, srv, "GetSAMLProvider", map[string]string{"SAMLProviderArn": arn})), &got))
	}
	get()
	assert.Len(t, got.UUID, 22)
	assert.Equal(t, "2030-01-01T00:00:00Z", got.ValidUntil, "ValidUntil comes from the metadata document")
	assert.Equal(t, samlMetadata, got.Metadata)

	list := idpOK(t, srv, "ListSAMLProviders", nil)
	assert.Contains(t, list, "<Arn>"+arn+"</Arn>")
	assert.Contains(t, list, "<ValidUntil>2030-01-01T00:00:00Z</ValidUntil>")

	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AddPrivateKey": "-----BEGIN PRIVATE KEY-----"})
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AssertionEncryptionMode": "Required"})
	get()
	require.Len(t, got.KeyIDs, 1)
	assert.Equal(t, "Required", got.Mode)
	assert.NotContains(t, idpOK(t, srv, "GetSAMLProvider", map[string]string{"SAMLProviderArn": arn}), "BEGIN PRIVATE KEY",
		"the private key text is never stored")

	status, body := idpCall(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "RemovePrivateKey": got.KeyIDs[0]})
	assert.Equal(t, http.StatusBadRequest, status, "the only key of a Required provider cannot be removed")
	assert.Contains(t, body, "Failed to remove private key.")
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AssertionEncryptionMode": "Allowed"})
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "RemovePrivateKey": got.KeyIDs[0]})

	newDoc := strings.Replace(samlMetadata, "2030-01-01", "2031-06-01", 1)
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "SAMLMetadataDocument": newDoc})
	get()
	assert.Equal(t, "2031-06-01T00:00:00Z", got.ValidUntil)
	assert.Empty(t, got.KeyIDs)

	idpOK(t, srv, "TagSAMLProvider", map[string]string{"SAMLProviderArn": arn, "Tags.member.1.Key": "env", "Tags.member.1.Value": "dev"})
	idpOK(t, srv, "UntagSAMLProvider", map[string]string{"SAMLProviderArn": arn, "TagKeys.member.1": "team"})
	tags := idpOK(t, srv, "ListSAMLProviderTags", map[string]string{"SAMLProviderArn": arn})
	assert.Contains(t, tags, "<Key>env</Key>")
	assert.NotContains(t, tags, "<Key>team</Key>")

	idpOK(t, srv, "DeleteSAMLProvider", map[string]string{"SAMLProviderArn": arn})
	status, body = idpCall(t, srv, "DeleteSAMLProvider", map[string]string{"SAMLProviderArn": arn})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "NoSuchEntity", idpErrorCode(t, body))
}

func TestIAMSAMLProvider_Refusals(t *testing.T) {
	srv := newIAMTestServer(t)
	const arn = "arn:aws:iam::123456789012:saml-provider/Corp"
	idpOK(t, srv, "CreateSAMLProvider", map[string]string{"Name": "Corp", "SAMLMetadataDocument": samlMetadata})

	cases := []struct {
		name    string
		op      string
		params  map[string]string
		status  int
		code    string
		message string
	}{
		{"duplicate", "CreateSAMLProvider", map[string]string{"Name": "Corp", "SAMLMetadataDocument": samlMetadata},
			http.StatusConflict, "EntityAlreadyExists", ""},
		{"unparseable metadata", "CreateSAMLProvider", map[string]string{"Name": "Bad", "SAMLMetadataDocument": "not xml"},
			http.StatusBadRequest, "InvalidInput", "Could not parse metadata."},
		{"required without key", "CreateSAMLProvider",
			map[string]string{"Name": "Enc", "SAMLMetadataDocument": samlMetadata, "AssertionEncryptionMode": "Required"},
			http.StatusBadRequest, "InvalidInput", "Unable to set assertion encryption mode to Required because no private key is provided."},
		{"bad mode", "CreateSAMLProvider",
			map[string]string{"Name": "Enc", "SAMLMetadataDocument": samlMetadata, "AssertionEncryptionMode": "Sometimes"},
			http.StatusBadRequest, "ValidationError", ""},
		{"no name", "CreateSAMLProvider", map[string]string{"SAMLMetadataDocument": samlMetadata}, http.StatusBadRequest, "ValidationError", ""},
		{"no metadata", "CreateSAMLProvider", map[string]string{"Name": "X"}, http.StatusBadRequest, "ValidationError", ""},
		{"bad name", "CreateSAMLProvider", map[string]string{"Name": "a/b", "SAMLMetadataDocument": samlMetadata},
			http.StatusBadRequest, "ValidationError", ""},
		{"nothing to update", "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn},
			http.StatusBadRequest, "InvalidInput", "No updates are defined"},
		{"add and remove", "UpdateSAMLProvider",
			map[string]string{"SAMLProviderArn": arn, "AddPrivateKey": "k", "RemovePrivateKey": "SAMLABCDEFGHIJKLMNOPQR"},
			http.StatusBadRequest, "InvalidInput", "Unable to add and remove private keys in the same request."},
		{"remove unknown key", "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "RemovePrivateKey": "SAMLABCDEFGHIJKLMNOPQR"},
			http.StatusBadRequest, "InvalidInput", "does not match a private key"},
		{"update bad metadata", "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "SAMLMetadataDocument": "<x/>"},
			http.StatusBadRequest, "InvalidInput", ""},
		{"update missing", "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn + "x", "AssertionEncryptionMode": "Allowed"},
			http.StatusNotFound, "NoSuchEntity", ""},
		{"get missing", "GetSAMLProvider", map[string]string{"SAMLProviderArn": arn + "x"}, http.StatusNotFound, "NoSuchEntity", ""},
		{"get no arn", "GetSAMLProvider", map[string]string{}, http.StatusBadRequest, "ValidationError", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := idpCall(t, srv, tc.op, tc.params)
			assert.Equal(t, tc.status, status, body)
			assert.Equal(t, tc.code, idpErrorCode(t, body))
			assert.Contains(t, body, tc.message)
		})
	}

	// Three keys is one over the limit.
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AddPrivateKey": "k1"})
	idpOK(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AddPrivateKey": "k2"})
	status, body := idpCall(t, srv, "UpdateSAMLProvider", map[string]string{"SAMLProviderArn": arn, "AddPrivateKey": "k3"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, body, "Private key limit of 2 is reached.")
}

func TestIAMSAMLProvider_Base64Metadata(t *testing.T) {
	srv := newIAMTestServer(t)
	encoded := base64Std(samlMetadata)
	idpOK(t, srv, "CreateSAMLProvider", map[string]string{"Name": "B64", "SAMLMetadataDocument": encoded})
	var got struct {
		ValidUntil string `xml:"GetSAMLProviderResult>ValidUntil"`
	}
	require.NoError(t, xml.Unmarshal([]byte(idpOK(t, srv, "GetSAMLProvider",
		map[string]string{"SAMLProviderArn": "arn:aws:iam::123456789012:saml-provider/B64"})), &got))
	assert.Equal(t, "2030-01-01T00:00:00Z", got.ValidUntil)
}
