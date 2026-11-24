package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// idpDirect drives the IAM and STS plugins without a server, so a faulting store reaches each
// handler's error path.
type idpDirect struct {
	state *cfFaultStateManager
	iam   *emulator.IAMPlugin
	sts   *emulator.STSPlugin
	n     int
}

func newIDPDirect(t *testing.T) *idpDirect {
	t.Helper()
	state := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(stsFedT0)
	d := &idpDirect{state: state, iam: &emulator.IAMPlugin{}, sts: &emulator.STSPlugin{}}
	opts := map[string]any{"time_controller": tc}
	require.NoError(t, d.iam.Initialize(context.TODO(), emulator.PluginConfig{State: state, Logger: logger, Options: opts}))
	require.NoError(t, d.sts.Initialize(context.TODO(), emulator.PluginConfig{State: state, Logger: logger, Options: opts}))
	return d
}

func (d *idpDirect) ctx() *emulator.RequestContext {
	d.n++
	id := "req-" + strings.Repeat("x", d.n)
	return &emulator.RequestContext{RequestID: id, AccountID: "123456789012", Region: "us-east-1",
		IDs: emulator.NewIDMint(id), Metadata: map[string]any{}}
}

// call sends op to plugin with body as the JSON body and params as the query parameters.
func (d *idpDirect) call(t *testing.T, plugin emulator.Plugin, op string, body map[string]string,
	params map[string]string) (*emulator.AWSResponse, error) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	if params == nil {
		params = map[string]string{}
	}
	return plugin.HandleRequest(d.ctx(), &emulator.AWSRequest{Service: plugin.Name(), Operation: op,
		Body: raw, Params: params, Headers: map[string]string{}})
}

func TestIdentityProviders_StoreFaults(t *testing.T) {
	const oidcARN = "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
	const samlARN = "arn:aws:iam::123456789012:saml-provider/Corp"
	seed := func(t *testing.T, d *idpDirect) {
		t.Helper()
		resp, err := d.call(t, d.iam, "CreateOpenIDConnectProvider", map[string]string{"Url": ghaIssuer},
			map[string]string{"ClientIDList.member.1": "sts.amazonaws.com"})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
		resp, err = d.call(t, d.iam, "CreateSAMLProvider", map[string]string{"Name": "Corp", "SAMLMetadataDocument": samlMetadata}, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
		resp, err = d.call(t, d.iam, "CreateRole", map[string]string{"RoleName": "gha-deploy", "AssumeRolePolicyDocument": ghaTrust}, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	}
	webToken := func(t *testing.T) string {
		return fedJWT(t, map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com",
			"sub": "repo:octo-org/octo-repo:ref:refs/heads/main", "exp": stsFedT0.Add(time.Hour).Unix()})
	}
	oidc := map[string]string{"OpenIDConnectProviderArn": oidcARN}
	saml := map[string]string{"SAMLProviderArn": samlARN}
	cases := []struct {
		name    string
		sts     bool
		op      string
		body    map[string]string
		params  func(t *testing.T) map[string]string
		failGet string
		corrupt string
		failPut string
		failDel string
	}{
		{name: "create oidc get", op: "CreateOpenIDConnectProvider", body: map[string]string{"Url": "https://x.example.com"}, failGet: "oidc_provider"},
		{name: "create oidc put", op: "CreateOpenIDConnectProvider", body: map[string]string{"Url": "https://x.example.com"}, failPut: "oidc_provider"},
		{name: "get oidc", op: "GetOpenIDConnectProvider", body: oidc, failGet: "oidc_provider"},
		{name: "get oidc corrupt", op: "GetOpenIDConnectProvider", body: oidc, corrupt: "oidc_provider"},
		{name: "delete oidc", op: "DeleteOpenIDConnectProvider", body: oidc, failDel: "oidc_provider"},
		{name: "thumbprint put", op: "UpdateOpenIDConnectProviderThumbprint", body: oidc,
			params: func(*testing.T) map[string]string {
				return map[string]string{"ThumbprintList.member.1": strings.Repeat("c", 40)}
			}, failPut: "oidc_provider"},
		{name: "tag oidc put", op: "TagOpenIDConnectProvider", body: oidc,
			params: func(*testing.T) map[string]string {
				return map[string]string{"Tags.member.1.Key": "k", "Tags.member.1.Value": "v"}
			}, failPut: "oidc_provider"},
		{name: "untag saml put", op: "UntagSAMLProvider", body: saml,
			params: func(*testing.T) map[string]string { return map[string]string{"TagKeys.member.1": "k"} }, failPut: "saml_provider"},
		{name: "list saml tags", op: "ListSAMLProviderTags", body: saml, failGet: "saml_provider"},
		{name: "create saml get", op: "CreateSAMLProvider", body: map[string]string{"Name": "Two", "SAMLMetadataDocument": samlMetadata}, failGet: "saml_provider"},
		{name: "create saml put", op: "CreateSAMLProvider", body: map[string]string{"Name": "Two", "SAMLMetadataDocument": samlMetadata}, failPut: "saml_provider"},
		{name: "get saml corrupt", op: "GetSAMLProvider", body: saml, corrupt: "saml_provider"},
		{name: "list saml", op: "ListSAMLProviders", failGet: "saml_provider"},
		{name: "list saml corrupt", op: "ListSAMLProviders", corrupt: "saml_provider"},
		{name: "update saml put", op: "UpdateSAMLProvider", body: map[string]string{"SAMLProviderArn": samlARN, "AssertionEncryptionMode": "Allowed"},
			failPut: "saml_provider"},
		{name: "delete saml", op: "DeleteSAMLProvider", body: saml, failDel: "saml_provider"},
		{name: "web identity provider", sts: true, op: "AssumeRoleWithWebIdentity", failGet: "oidc_provider",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "web identity provider corrupt", sts: true, op: "AssumeRoleWithWebIdentity", corrupt: "oidc_provider",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "web identity seed", sts: true, op: "AssumeRoleWithWebIdentity", failGet: "federation_outcome",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "web identity role", sts: true, op: "AssumeRoleWithWebIdentity", failGet: "role:",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "web identity role corrupt", sts: true, op: "AssumeRoleWithWebIdentity", corrupt: "role:",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "web identity session", sts: true, op: "AssumeRoleWithWebIdentity", failPut: "session:",
			params: func(t *testing.T) map[string]string {
				return map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gha-deploy", "RoleSessionName": "s1", "WebIdentityToken": webToken(t)}
			}},
		{name: "saml provider", sts: true, op: "AssumeRoleWithSAML", failGet: "saml_provider",
			params: func(*testing.T) map[string]string {
				return map[string]string{"RoleArn": samlRole, "PrincipalArn": samlARN, "SAMLAssertion": defaultSAML().encode()}
			}},
		{name: "saml provider corrupt", sts: true, op: "AssumeRoleWithSAML", corrupt: "saml_provider",
			params: func(*testing.T) map[string]string {
				return map[string]string{"RoleArn": samlRole, "PrincipalArn": samlARN, "SAMLAssertion": defaultSAML().encode()}
			}},
		{name: "saml seed", sts: true, op: "AssumeRoleWithSAML", failGet: "federation_outcome",
			params: func(*testing.T) map[string]string {
				return map[string]string{"RoleArn": samlRole, "PrincipalArn": samlARN, "SAMLAssertion": defaultSAML().encode()}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newIDPDirect(t)
			seed(t, d)
			d.state.failGet, d.state.corruptGet, d.state.failPut, d.state.failDelete = tc.failGet, tc.corrupt, tc.failPut, tc.failDel
			var params map[string]string
			if tc.params != nil {
				params = tc.params(t)
			}
			plugin := emulator.Plugin(d.iam)
			if tc.sts {
				plugin = d.sts
			}
			_, err := d.call(t, plugin, tc.op, tc.body, params)
			assert.Error(t, err, "a store failure surfaces as an error, never as a published refusal")
		})
	}
}

func TestIdentityProviders_RequestShapeRefusals(t *testing.T) {
	d := newIDPDirect(t)
	cases := []struct {
		op   string
		body string
	}{
		{"CreateOpenIDConnectProvider", `{"Url":5}`},
		{"GetOpenIDConnectProvider", `{"OpenIDConnectProviderArn":5}`},
		{"AddClientIDToOpenIDConnectProvider", `{"ClientID":5}`},
		{"TagOpenIDConnectProvider", `{"OpenIDConnectProviderArn":5}`},
		{"TagOpenIDConnectProvider", `{"OpenIDConnectProviderArn":"arn:aws:iam::123456789012:oidc-provider/x","Tags":5}`},
		{"UntagSAMLProvider", `{"SAMLProviderArn":"arn:aws:iam::123456789012:saml-provider/x","TagKeys":5}`},
		{"ListSAMLProviderTags", `{"SAMLProviderArn":"arn:aws:iam::123456789012:saml-provider/x","MaxItems":"many"}`},
		{"ListSAMLProviderTags", `{}`},
		{"CreateSAMLProvider", `{"Name":5}`},
		{"GetSAMLProvider", `{"SAMLProviderArn":5}`},
		{"UpdateSAMLProvider", `{"SAMLProviderArn":"arn:aws:iam::123456789012:saml-provider/x","AddPrivateKey":5}`},
	}
	for _, tc := range cases {
		t.Run(tc.op+" "+tc.body, func(t *testing.T) {
			resp, err := d.iam.HandleRequest(d.ctx(), &emulator.AWSRequest{Service: "iam", Operation: tc.op,
				Body: []byte(tc.body), Params: map[string]string{}, Headers: map[string]string{}})
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, string(resp.Body))
		})
	}

	// A tag set that breaks the tag rules is refused before the provider is looked up.
	resp, err := d.call(t, d.iam, "TagSAMLProvider", map[string]string{"SAMLProviderArn": "arn:aws:iam::123456789012:saml-provider/x"},
		map[string]string{"Tags.member.1.Key": "aws:reserved", "Tags.member.1.Value": "v"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, string(resp.Body))
	resp, err = d.call(t, d.iam, "UntagOpenIDConnectProvider", map[string]string{"OpenIDConnectProviderArn": "arn:aws:iam::123456789012:oidc-provider/x"},
		map[string]string{"TagKeys.member.1": ""})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, string(resp.Body))
	resp, err = d.call(t, d.iam, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://t.example.com"},
		map[string]string{"Tags.member.1.Key": "aws:reserved", "Tags.member.1.Value": "v"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, string(resp.Body))
	resp, err = d.call(t, d.iam, "CreateSAMLProvider", map[string]string{"Name": "T", "SAMLMetadataDocument": samlMetadata},
		map[string]string{"Tags.member.1.Key": "aws:reserved", "Tags.member.1.Value": "v"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, string(resp.Body))
}

func TestSTSAssumeRoleWithWebIdentity_ClaimShapes(t *testing.T) {
	srv, _, tc := newSTSTestServer(t)
	tc.SetTime(stsFedT0)
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": "https://accounts.example.com",
		"ClientIDList.member.1": "web-client"})
	const provider = "arn:aws:iam::123456789012:oidc-provider/accounts.example.com"
	// azp sets :aud, aud sets :oaud, and amr is multivalued.
	fedRole(t, srv, "hybrid", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Principal":{"Federated":"`+provider+`"},"Action":"sts:AssumeRoleWithWebIdentity",
		"Condition":{"StringEquals":{"accounts.example.com:aud":"android-client","accounts.example.com:oaud":"web-client",
			"accounts.example.com:email":"jane@example.com"},
			"ForAnyValue:StringEquals":{"accounts.example.com:amr":"authenticated"}}}]}`)
	call := func(claims map[string]any) (int, string) {
		return fedSTS(t, srv, "AssumeRoleWithWebIdentity", map[string]string{
			"RoleArn": "arn:aws:iam::123456789012:role/hybrid", "RoleSessionName": "s1",
			"WebIdentityToken": fedJWT(t, claims)})
	}
	base := func() map[string]any {
		return map[string]any{"iss": "https://accounts.example.com", "aud": "web-client", "azp": "android-client",
			"email": "jane@example.com", "amr": []string{"authenticated", "accounts.example.com"}, "sub": "1234567890",
			"exp": float64(stsFedT0.Add(time.Hour).Unix()) + 0.5}
	}
	status, body := call(base())
	require.Equal(t, http.StatusOK, status, body)

	noAMR := base()
	delete(noAMR, "amr")
	status, _ = call(noAMR)
	assert.Equal(t, http.StatusForbidden, status, "the amr condition fails without the claim")

	badExp := base()
	badExp["exp"] = "tomorrow"
	status, body = call(badExp)
	assert.Equal(t, http.StatusBadRequest, status, body)

	// A JWT whose payload is not JSON, and one whose payload is not base64url.
	for _, token := range []string{"a.bm90IGpzb24.c", "a.!!!.c"} {
		status, body = fedSTS(t, srv, "AssumeRoleWithWebIdentity", map[string]string{
			"RoleArn": "arn:aws:iam::123456789012:role/hybrid", "RoleSessionName": "s1", "WebIdentityToken": token})
		assert.Equal(t, http.StatusBadRequest, status, body)
		code, _ := fedErrorCode(t, body)
		assert.Equal(t, "InvalidIdentityToken", code)
	}
}

func TestSTSAssumeRoleWithSAML_AssertionShapes(t *testing.T) {
	srv, _ := newSAMLServer(t)
	call := func(assertion string, extra map[string]string) (int, string) {
		params := map[string]string{"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": assertion}
		for k, v := range extra {
			params[k] = v
		}
		return fedSTS(t, srv, "AssumeRoleWithSAML", params)
	}

	// A bare Assertion, with no Response envelope, and a SessionDuration attribute.
	bare := base64Std(`<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">
		<saml:Issuer>https://idp.example.com/saml</saml:Issuer>
		<saml:Subject><saml:NameID>jane</saml:NameID><saml:SubjectConfirmation>
			<saml:SubjectConfirmationData Recipient="https://signin.aws.amazon.com/saml"/></saml:SubjectConfirmation></saml:Subject>
		<saml:AttributeStatement>
			<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/Role"><saml:AttributeValue>` + samlRole + `,` + samlProvider + `</saml:AttributeValue></saml:Attribute>
			<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml:AttributeValue>jane</saml:AttributeValue></saml:Attribute>
			<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/SessionDuration"><saml:AttributeValue>1800</saml:AttributeValue></saml:Attribute>
		</saml:AttributeStatement></saml:Assertion>`)
	status, body := call(bare, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "<Expiration>2025-01-01T12:30:00", "SessionDuration sets the default duration")
	assert.Contains(t, body, "<SubjectType></SubjectType>")

	for name, tc := range map[string]struct {
		assertion string
		extra     map[string]string
		status    int
	}{
		"not xml":                  {base64Std("not xml"), nil, http.StatusBadRequest},
		"no assertion":             {base64Std(`<Response><Issuer>x</Issuer></Response>`), nil, http.StatusBadRequest},
		"unreadable NotOnOrAfter":  {base64Std(`<Assertion><Conditions NotOnOrAfter="soon"/></Assertion>`), nil, http.StatusBadRequest},
		"duration out of range":    {defaultSAML().encode(), map[string]string{"DurationSeconds": "100"}, http.StatusBadRequest},
		"principal not a provider": {defaultSAML().encode(), map[string]string{"PrincipalArn": "arn:aws:iam::123456789012:role/x"}, http.StatusBadRequest},
		"role missing":             {defaultSAML().encode(), map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/gone"}, http.StatusForbidden},
	} {
		t.Run(name, func(t *testing.T) {
			status, body := call(tc.assertion, tc.extra)
			assert.Equal(t, tc.status, status, body)
		})
	}
}
