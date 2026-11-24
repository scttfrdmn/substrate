package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// stsFedT0 is the instant newSTSTestServer's clock starts at.
var stsFedT0 = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

func base64Std(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// fedJWT returns an unsigned-looking JWT carrying claims. Substrate decodes the payload and does
// not check the signature, which is what lets a test mint one.
func fedJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	enc := func(v any) string {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return enc(map[string]string{"alg": "RS256", "typ": "JWT"}) + "." + enc(claims) + ".c2lnbmF0dXJl"
}

// fedSTS sends an unsigned STS form request to an emulator-local host, the way an SDK pointed at a
// base endpoint sends AssumeRoleWithWebIdentity: no Authorization header, no X-Amz-Target, no
// service host. Only the Action says it is STS.
func fedSTS(t *testing.T, srv *emulator.Server, action string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2011-06-15"}}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "localhost:4566"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	return w.Code, string(body)
}

// fedErrorCode returns the Code and Message of a Query error document.
func fedErrorCode(t *testing.T, body string) (string, string) {
	t.Helper()
	var doc struct {
		Code    string `xml:"Error>Code"`
		Message string `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), body)
	return doc.Code, doc.Message
}

// fedRole creates a role with trust policy trust.
func fedRole(t *testing.T, srv *emulator.Server, name, trust string) string {
	t.Helper()
	idpOK(t, srv, "CreateRole", map[string]string{"RoleName": name, "AssumeRolePolicyDocument": trust})
	return "arn:aws:iam::123456789012:role/" + name
}

const (
	ghaIssuer   = "https://token.actions.githubusercontent.com"
	ghaProvider = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
	eksHost     = "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
	eksProvider = "arn:aws:iam::123456789012:oidc-provider/" + eksHost
)

// ghaTrust is the trust policy GitHub's and AWS's guides give for a repository's workflows.
const ghaTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	"Principal":{"Federated":"` + ghaProvider + `"},
	"Action":"sts:AssumeRoleWithWebIdentity",
	"Condition":{"StringEquals":{"token.actions.githubusercontent.com:aud":"sts.amazonaws.com"},
		"StringLike":{"token.actions.githubusercontent.com:sub":"repo:octo-org/octo-repo:*"}}}]}`

// irsaTrust is the trust policy EKS's IAM-roles-for-service-accounts guide gives.
const irsaTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	"Principal":{"Federated":"` + eksProvider + `"},
	"Action":"sts:AssumeRoleWithWebIdentity",
	"Condition":{"StringEquals":{"` + eksHost + `:aud":"sts.amazonaws.com",
		"` + eksHost + `:sub":"system:serviceaccount:default:my-sa"}}}]}`

func newFederationServer(t *testing.T) *emulator.Server {
	t.Helper()
	srv, _, tc := newSTSTestServer(t)
	tc.SetTime(stsFedT0)
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{
		"Url": ghaIssuer, "ClientIDList.member.1": "sts.amazonaws.com", "ThumbprintList.member.1": ghaThumbprint})
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{
		"Url": "https://" + eksHost, "ClientIDList.member.1": "sts.amazonaws.com"})
	fedRole(t, srv, "gha-deploy", ghaTrust)
	fedRole(t, srv, "irsa-app", irsaTrust)
	fedRole(t, srv, "account-trust", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRoleWithWebIdentity"}]}`)
	return srv
}

func TestSTSAssumeRoleWithWebIdentity(t *testing.T) {
	exp := stsFedT0.Add(time.Hour).Unix()
	gha := func(sub, aud string) map[string]any {
		return map[string]any{"iss": ghaIssuer, "aud": aud, "sub": sub, "exp": exp, "iat": stsFedT0.Unix()}
	}
	irsa := func(sub string) map[string]any {
		return map[string]any{"iss": "https://" + eksHost, "aud": []string{"sts.amazonaws.com"}, "sub": sub, "exp": exp}
	}
	const mainRef = "repo:octo-org/octo-repo:ref:refs/heads/main"
	cases := []struct {
		name    string
		role    string
		claims  map[string]any
		token   string
		status  int
		code    string
		message string
	}{
		{name: "github actions allowed", role: "gha-deploy", claims: gha(mainRef, "sts.amazonaws.com"), status: http.StatusOK},
		{name: "github actions other repo", role: "gha-deploy", claims: gha("repo:evil/repo:ref:refs/heads/main", "sts.amazonaws.com"),
			status: http.StatusForbidden, code: "AccessDenied", message: "Not authorized to perform sts:AssumeRoleWithWebIdentity"},
		{name: "unknown audience", role: "gha-deploy", claims: gha(mainRef, "api://other"),
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "Incorrect token audience"},
		{name: "irsa allowed", role: "irsa-app", claims: irsa("system:serviceaccount:default:my-sa"), status: http.StatusOK},
		{name: "irsa other service account", role: "irsa-app", claims: irsa("system:serviceaccount:kube-system:other"),
			status: http.StatusForbidden, code: "AccessDenied"},
		{name: "unregistered issuer", role: "gha-deploy",
			claims: map[string]any{"iss": "https://accounts.example.com", "aud": "sts.amazonaws.com", "sub": "x", "exp": exp},
			status: http.StatusBadRequest, code: "InvalidIdentityToken",
			message: "No OpenIDConnect provider found in your account for https://accounts.example.com"},
		{name: "account principal does not admit a provider", role: "account-trust", claims: gha(mainRef, "sts.amazonaws.com"),
			status: http.StatusForbidden, code: "AccessDenied"},
		{name: "missing role", role: "no-such-role", claims: gha(mainRef, "sts.amazonaws.com"),
			status: http.StatusForbidden, code: "AccessDenied"},
		{name: "malformed token", role: "gha-deploy", token: "not-a-jwt",
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
		{name: "no exp", role: "gha-deploy", claims: map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com", "sub": mainRef},
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFederationServer(t)
			token := tc.token
			if token == "" {
				token = fedJWT(t, tc.claims)
			}
			status, body := fedSTS(t, srv, "AssumeRoleWithWebIdentity", map[string]string{
				"RoleArn":          "arn:aws:iam::123456789012:role/" + tc.role,
				"RoleSessionName":  "run-42",
				"WebIdentityToken": token,
			})
			require.Equal(t, tc.status, status, body)
			if tc.status != http.StatusOK {
				code, msg := fedErrorCode(t, body)
				assert.Equal(t, tc.code, code)
				assert.Contains(t, msg, tc.message)
				return
			}
			var res struct {
				Subject  string `xml:"AssumeRoleWithWebIdentityResult>SubjectFromWebIdentityToken"`
				Audience string `xml:"AssumeRoleWithWebIdentityResult>Audience"`
				Provider string `xml:"AssumeRoleWithWebIdentityResult>Provider"`
				Arn      string `xml:"AssumeRoleWithWebIdentityResult>AssumedRoleUser>Arn"`
				RoleID   string `xml:"AssumeRoleWithWebIdentityResult>AssumedRoleUser>AssumedRoleId"`
				KeyID    string `xml:"AssumeRoleWithWebIdentityResult>Credentials>AccessKeyId"`
				Expiry   string `xml:"AssumeRoleWithWebIdentityResult>Credentials>Expiration"`
			}
			require.NoError(t, xml.Unmarshal([]byte(body), &res))
			assert.Equal(t, tc.claims["sub"], res.Subject)
			assert.Equal(t, "sts.amazonaws.com", res.Audience)
			assert.Equal(t, tc.claims["iss"], res.Provider, "Provider is the token's iss")
			assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/"+tc.role+"/run-42", res.Arn)
			assert.True(t, strings.HasPrefix(res.RoleID, "AROA") && strings.HasSuffix(res.RoleID, ":run-42"), res.RoleID)
			assert.True(t, strings.HasPrefix(res.KeyID, "ASIA"))
			assert.True(t, strings.HasPrefix(res.Expiry, "2025-01-01T13:00:00"), res.Expiry)

			// The session authenticates as the assumed role, and the role records the use.
			caller := stsSignedCall(t, srv, res.KeyID, "GetCallerIdentity")
			assert.Contains(t, caller, "<Arn>"+res.Arn+"</Arn>")
			assert.Contains(t, idpOK(t, srv, "GetRole", map[string]string{"RoleName": tc.role}), "<RoleLastUsed><LastUsedDate>")
		})
	}
}

// stsSignedCall sends an STS request signed (unverified) with accessKeyID.
func stsSignedCall(t *testing.T, srv *emulator.Server, accessKeyID, action string) string {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+action+"&Version=2011-06-15"))
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code, string(body))
	return string(body)
}

func TestSTSAssumeRoleWithWebIdentity_ExpiredOnTheSimulatedClock(t *testing.T) {
	srv, _, tc := newSTSTestServer(t)
	tc.SetTime(stsFedT0)
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": ghaIssuer, "ClientIDList.member.1": "sts.amazonaws.com"})
	fedRole(t, srv, "gha-deploy", ghaTrust)
	exp := stsFedT0.Add(10 * time.Minute)
	token := fedJWT(t, map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com",
		"sub": "repo:octo-org/octo-repo:ref:refs/heads/main", "exp": exp.Unix()})
	params := map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy",
		"RoleSessionName": "s1", "WebIdentityToken": token}

	status, body := fedSTS(t, srv, "AssumeRoleWithWebIdentity", params)
	require.Equal(t, http.StatusOK, status, body)

	tc.SetTime(exp)
	status, body = fedSTS(t, srv, "AssumeRoleWithWebIdentity", params)
	require.Equal(t, http.StatusBadRequest, status, body)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "ExpiredTokenException", code)
	assert.Equal(t, fmt.Sprintf("Token expired: current date/time %d must be before the expiration date/time %d",
		exp.Unix(), exp.Unix()), msg)
}

func TestSTSAssumeRoleWithWebIdentity_SeededOutcomes(t *testing.T) {
	params := func(t *testing.T) map[string]string {
		return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1",
			"WebIdentityToken": fedJWT(t, map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com",
				"sub": "repo:octo-org/octo-repo:ref:refs/heads/main", "exp": stsFedT0.Add(time.Hour).Unix()})}
	}
	cases := []struct {
		seed    string
		status  int
		code    string
		message string
	}{
		{`{"provider":"` + ghaProvider + `","error":"IDPRejectedClaim","count":1}`, http.StatusForbidden, "IDPRejectedClaim",
			"The identity provider rejected the claim."},
		{`{"error":"InvalidIdentityToken","count":1}`, http.StatusBadRequest, "InvalidIdentityToken",
			"OpenIDConnect provider's HTTPS certificate doesn't match configured thumbprint"},
		{`{"provider":"` + ghaProvider + `","error":"IDPCommunicationError","message":"down","count":1}`,
			http.StatusBadRequest, "IDPCommunicationError", "down"},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			srv := newFederationServer(t)
			require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome", tc.seed))
			status, body := fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
			require.Equal(t, tc.status, status, body)
			code, msg := fedErrorCode(t, body)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.message, msg)
			// A count of one answers one assume; the retry succeeds.
			status, body = fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
			assert.Equal(t, http.StatusOK, status, body)
		})
	}

	t.Run("held until cleared", func(t *testing.T) {
		srv := newFederationServer(t)
		require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome",
			`{"provider":"`+ghaProvider+`","error":"IDPRejectedClaim","count":2}`))
		for range 2 {
			status, _ := fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
			assert.Equal(t, http.StatusForbidden, status)
		}
		status, _ := fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
		assert.Equal(t, http.StatusOK, status, "a count of two answers two assumes")

		require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome",
			`{"error":"IDPRejectedClaim"}`))
		for range 3 {
			status, _ := fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
			assert.Equal(t, http.StatusForbidden, status)
		}
		require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodDelete, "/v1/sts/federation-outcome?provider=*", ""))
		status, _ = fedSTS(t, srv, "AssumeRoleWithWebIdentity", params(t))
		assert.Equal(t, http.StatusOK, status)
		require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodDelete, "/v1/sts/federation-outcome", ""))
	})

	t.Run("seed validation", func(t *testing.T) {
		srv := newFederationServer(t)
		assert.Equal(t, http.StatusBadRequest, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome", `{"error":"Nope"}`))
		assert.Equal(t, http.StatusBadRequest, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome",
			`{"error":"IDPRejectedClaim","count":-1}`))
		assert.Equal(t, http.StatusBadRequest, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome", `{`))
	})
}

func TestSTSAssumeRoleWithWebIdentity_ParameterRefusals(t *testing.T) {
	srv := newFederationServer(t)
	token := fedJWT(t, map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com", "sub": "x", "exp": stsFedT0.Add(time.Hour).Unix()})
	cases := []struct {
		name   string
		params map[string]string
	}{
		{"no role", map[string]string{"RoleSessionName": "s1", "WebIdentityToken": token}},
		{"no token", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1"}},
		{"bad session name", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy",
			"RoleSessionName": "a b", "WebIdentityToken": token}},
		{"duration too long", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy",
			"RoleSessionName": "s1", "WebIdentityToken": token, "DurationSeconds": "50000"}},
		{"duration not a number", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy",
			"RoleSessionName": "s1", "WebIdentityToken": token, "DurationSeconds": "soon"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := fedSTS(t, srv, "AssumeRoleWithWebIdentity", tc.params)
			assert.Equal(t, http.StatusBadRequest, status, body)
			code, _ := fedErrorCode(t, body)
			assert.Equal(t, "ValidationError", code)
		})
	}
}

func TestParseAWSRequest_UnsignedFederationRoutesToSTS(t *testing.T) {
	for action, want := range map[string]string{
		"AssumeRoleWithWebIdentity": "sts",
		"AssumeRoleWithSAML":        "sts",
		"GetCallerIdentity":         "unknown",
	} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+action))
		r.Host = "localhost:4566"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req, _, err := emulator.ParseAWSRequest(r)
		require.NoError(t, err)
		assert.Equal(t, want, req.Service, action)
	}
}

// --- SAML -------------------------------------------------------------------------

const (
	samlProvider = "arn:aws:iam::123456789012:saml-provider/Corp"
	samlRole     = "arn:aws:iam::123456789012:role/saml-admin"
)

// samlTrust is the trust policy IAM's console writes for a SAML 2.0 role.
const samlTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	"Principal":{"Federated":"` + samlProvider + `"},
	"Action":"sts:AssumeRoleWithSAML",
	"Condition":{"StringEquals":{"SAML:aud":"https://signin.aws.amazon.com/saml"}}}]}`

// samlAssertion builds a base64 SAML response.
type samlAssertionOpts struct {
	roleAttr     string
	sessionName  string
	notOnOrAfter time.Time
	sessionCap   time.Time
	recipient    string
	nameID       string
}

func (o samlAssertionOpts) encode() string {
	attrs := ""
	if o.roleAttr != "" {
		attrs += `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/Role"><saml:AttributeValue>` +
			o.roleAttr + `</saml:AttributeValue></saml:Attribute>`
	}
	if o.sessionName != "" {
		attrs += `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml:AttributeValue>` +
			o.sessionName + `</saml:AttributeValue></saml:Attribute>`
	}
	authn := ""
	if !o.sessionCap.IsZero() {
		authn = `<saml:AuthnStatement SessionNotOnOrAfter="` + o.sessionCap.Format(time.RFC3339) + `"/>`
	}
	doc := `<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
<saml:Issuer>https://idp.example.com/saml</saml:Issuer>
<saml:Assertion>
 <saml:Issuer>https://idp.example.com/saml</saml:Issuer>
 <saml:Subject>
  <saml:NameID Format="urn:oasis:names:tc:SAML:2.0:nameid-format:persistent">` + o.nameID + `</saml:NameID>
  <saml:SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
   <saml:SubjectConfirmationData NotOnOrAfter="` + o.notOnOrAfter.Format(time.RFC3339) + `" Recipient="` + o.recipient + `"/>
  </saml:SubjectConfirmation>
 </saml:Subject>
 <saml:Conditions NotOnOrAfter="` + o.notOnOrAfter.Format(time.RFC3339) + `"/>
 ` + authn + `
 <saml:AttributeStatement>` + attrs + `</saml:AttributeStatement>
</saml:Assertion>
</samlp:Response>`
	return base64Std(doc)
}

func defaultSAML() samlAssertionOpts {
	return samlAssertionOpts{
		roleAttr:     samlRole + "," + samlProvider,
		sessionName:  "jane@example.com",
		notOnOrAfter: stsFedT0.Add(5 * time.Minute),
		recipient:    "https://signin.aws.amazon.com/saml",
		nameID:       "jane",
	}
}

func newSAMLServer(t *testing.T) (*emulator.Server, *emulator.TimeController) {
	t.Helper()
	srv, _, tc := newSTSTestServer(t)
	tc.SetTime(stsFedT0)
	idpOK(t, srv, "CreateSAMLProvider", map[string]string{"Name": "Corp", "SAMLMetadataDocument": samlMetadata})
	fedRole(t, srv, "saml-admin", samlTrust)
	return srv, tc
}

func TestSTSAssumeRoleWithSAML(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*samlAssertionOpts)
		params  map[string]string
		status  int
		code    string
		message string
	}{
		{name: "allowed", mutate: func(*samlAssertionOpts) {}, status: http.StatusOK},
		{name: "provider first in role attribute", mutate: func(o *samlAssertionOpts) { o.roleAttr = samlProvider + "," + samlRole },
			status: http.StatusOK},
		{name: "role attribute names another role", mutate: func(o *samlAssertionOpts) {
			o.roleAttr = "arn:aws:iam::123456789012:role/other," + samlProvider
		}, status: http.StatusForbidden, code: "AccessDenied", message: "Not authorized to perform sts:AssumeRoleWithSAML"},
		{name: "wrong recipient fails the trust condition", mutate: func(o *samlAssertionOpts) { o.recipient = "https://elsewhere.example.com" },
			status: http.StatusForbidden, code: "AccessDenied"},
		{name: "no session name", mutate: func(o *samlAssertionOpts) { o.sessionName = "" },
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "RoleSessionName is required in AuthnResponse"},
		{name: "bad session name", mutate: func(o *samlAssertionOpts) { o.sessionName = "has space" },
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "RoleSessionName in AuthnResponse must match"},
		{name: "no NameID", mutate: func(o *samlAssertionOpts) { o.nameID = "" }, status: http.StatusForbidden, code: "AccessDenied"},
		{name: "expired", mutate: func(o *samlAssertionOpts) { o.notOnOrAfter = stsFedT0 },
			status: http.StatusBadRequest, code: "ExpiredTokenException"},
		{name: "unknown provider", mutate: func(*samlAssertionOpts) {},
			params: map[string]string{"PrincipalArn": "arn:aws:iam::123456789012:saml-provider/Nope"},
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "Specified provider doesn't exist."},
		{name: "not base64", mutate: func(*samlAssertionOpts) {}, params: map[string]string{"SAMLAssertion": "%%%"},
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
		{name: "encrypted", mutate: func(*samlAssertionOpts) {},
			params: map[string]string{"SAMLAssertion": base64Std(`<Response><EncryptedAssertion/></Response>`)},
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
		{name: "missing principal", mutate: func(*samlAssertionOpts) {}, params: map[string]string{"PrincipalArn": ""},
			status: http.StatusBadRequest, code: "ValidationError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newSAMLServer(t)
			opts := defaultSAML()
			tc.mutate(&opts)
			params := map[string]string{"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": opts.encode()}
			for k, v := range tc.params {
				params[k] = v
			}
			status, body := fedSTS(t, srv, "AssumeRoleWithSAML", params)
			require.Equal(t, tc.status, status, body)
			if tc.status != http.StatusOK {
				code, msg := fedErrorCode(t, body)
				assert.Equal(t, tc.code, code)
				assert.Contains(t, msg, tc.message)
				return
			}
			var res struct {
				Arn           string `xml:"AssumeRoleWithSAMLResult>AssumedRoleUser>Arn"`
				Subject       string `xml:"AssumeRoleWithSAMLResult>Subject"`
				SubjectType   string `xml:"AssumeRoleWithSAMLResult>SubjectType"`
				Issuer        string `xml:"AssumeRoleWithSAMLResult>Issuer"`
				Audience      string `xml:"AssumeRoleWithSAMLResult>Audience"`
				NameQualifier string `xml:"AssumeRoleWithSAMLResult>NameQualifier"`
			}
			require.NoError(t, xml.Unmarshal([]byte(body), &res))
			assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/saml-admin/jane@example.com", res.Arn)
			assert.Equal(t, "jane", res.Subject)
			assert.Equal(t, "persistent", res.SubjectType, "the nameid-format prefix is removed")
			assert.Equal(t, "https://idp.example.com/saml", res.Issuer)
			assert.Equal(t, "https://signin.aws.amazon.com/saml", res.Audience)
			// BASE64(SHA1("https://idp.example.com/saml" + "123456789012" + "/Corp")), computed
			// outside Go.
			assert.Equal(t, "F4n4tHfsTNbdJ8Yl1OzB++/YKYg=", res.NameQualifier)
		})
	}
}

func TestSTSAssumeRoleWithSAML_SessionNotOnOrAfterCapsTheSession(t *testing.T) {
	srv, _ := newSAMLServer(t)
	opts := defaultSAML()
	opts.sessionCap = stsFedT0.Add(20 * time.Minute)
	status, body := fedSTS(t, srv, "AssumeRoleWithSAML", map[string]string{
		"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": opts.encode(), "DurationSeconds": "3600"})
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "<Expiration>2025-01-01T12:20:00Z</Expiration>")
}

func TestSTSAssumeRoleWithSAML_SeededSignatureFailure(t *testing.T) {
	srv, _ := newSAMLServer(t)
	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome",
		`{"provider":"`+samlProvider+`","error":"InvalidIdentityToken"}`))
	params := map[string]string{"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": defaultSAML().encode()}
	status, body := fedSTS(t, srv, "AssumeRoleWithSAML", params)
	require.Equal(t, http.StatusBadRequest, status, body)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "InvalidIdentityToken", code)
	assert.Equal(t, "Response signature invalid", msg)

	// IDPCommunicationError is not an AssumeRoleWithSAML error, so a wildcard seed of it does not
	// reach the SAML operation.
	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodDelete, "/v1/sts/federation-outcome", ""))
	require.Equal(t, http.StatusOK, controlCall(t, srv, http.MethodPost, "/v1/sts/federation-outcome",
		`{"error":"IDPCommunicationError"}`))
	status, body = fedSTS(t, srv, "AssumeRoleWithSAML", params)
	assert.Equal(t, http.StatusOK, status, body)
}

// TestSTSWire_FederatedResponsesCarryNoBookkeepingMember extends #756's assertion to the two
// federated assumes, which answer from the same session record.
func TestSTSWire_FederatedResponsesCarryNoBookkeepingMember(t *testing.T) {
	srv := newFederationServer(t)
	status, web := fedSTS(t, srv, "AssumeRoleWithWebIdentity", map[string]string{
		"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1",
		"WebIdentityToken": fedJWT(t, map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com",
			"sub": "repo:octo-org/octo-repo:ref:refs/heads/main", "exp": stsFedT0.Add(time.Hour).Unix()}),
	})
	require.Equal(t, http.StatusOK, status, web)
	require.Contains(t, web, "<AssumedRoleId>")
	wireAssertNoMemberXML(t, "AssumeRoleWithWebIdentity", []byte(web), []string{"AccountID"}, "")

	samlSrv, _ := newSAMLServer(t)
	status, saml := fedSTS(t, samlSrv, "AssumeRoleWithSAML", map[string]string{
		"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": defaultSAML().encode()})
	require.Equal(t, http.StatusOK, status, saml)
	require.Contains(t, saml, "<AssumedRoleId>")
	wireAssertNoMemberXML(t, "AssumeRoleWithSAML", []byte(saml), []string{"AccountID"}, "")
}
