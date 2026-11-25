package emulator_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What a federated session carries beyond its identity — its maximum duration, session policies,
// session tags and source identity — and the remaining claim checks: nbf, NotBefore, sub, and a
// SAML assertion's issuer and audience.

// fedAuthServer is a federation server with authorization enforced, so a session's policies,
// boundary and tags decide what it may do.
func fedAuthServer(t *testing.T) *emulator.Server {
	t.Helper()
	srv := newTrustPolicyTestServer(t)
	idpOK(t, srv, "CreateOpenIDConnectProvider", map[string]string{"Url": ghaIssuer, "ClientIDList.member.1": "sts.amazonaws.com"})
	return srv
}

// ghaClaims is a current GitHub Actions token's claims, with extra merged over them.
func ghaClaims(extra map[string]any) map[string]any {
	c := map[string]any{"iss": ghaIssuer, "aud": "sts.amazonaws.com", "sub": "repo:octo-org/octo-repo:ref:refs/heads/main",
		"exp": stsFedT0.Add(time.Hour).Unix()}
	for k, v := range extra {
		c[k] = v
	}
	return c
}

// fedTrust is a trust policy admitting the GitHub provider for actions.
func fedTrust(actions ...string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"` + ghaProvider +
		`"},"Action":["` + strings.Join(actions, `","`) + `"]}]}`
}

// fedAssume assumes role with claims and extra parameters, returning the status and body.
func fedAssume(t *testing.T, srv *emulator.Server, role string, claims map[string]any, extra map[string]string) (int, string) {
	t.Helper()
	params := map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/" + role, "RoleSessionName": "s1",
		"WebIdentityToken": fedJWT(t, claims)}
	for k, v := range extra {
		params[k] = v
	}
	return fedSTS(t, srv, "AssumeRoleWithWebIdentity", params)
}

// fedSessionKey extracts the minted access key ID.
func fedSessionKey(t *testing.T, body string) string {
	t.Helper()
	m := regexp.MustCompile(`<AccessKeyId>(ASIA[^<]+)</AccessKeyId>`).FindStringSubmatch(body)
	require.NotNil(t, m, body)
	return m[1]
}

// fedIAMAs sends an IAM request signed with accessKeyID and returns its status.
func fedIAMAs(t *testing.T, srv *emulator.Server, accessKeyID, op string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{"Action": {op}, "Version": {"2010-05-08"}}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "iam"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	return w.Code, string(body)
}

func TestSTSFederation_DurationIsBoundedByTheRolesMaxSessionDuration(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "short", fedTrust("sts:AssumeRoleWithWebIdentity"))
	idpOK(t, srv, "CreateRole", map[string]string{"RoleName": "long", "MaxSessionDuration": "7200",
		"AssumeRolePolicyDocument": fedTrust("sts:AssumeRoleWithWebIdentity")})

	status, body := fedAssume(t, srv, "short", ghaClaims(nil), map[string]string{"DurationSeconds": "7200"})
	require.Equal(t, http.StatusBadRequest, status, body)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "ValidationError", code)
	assert.Equal(t, "Requested DurationSeconds exceeds MaxSessionDuration set for this role.", msg)

	status, body = fedAssume(t, srv, "long", ghaClaims(nil), map[string]string{"DurationSeconds": "7200"})
	require.Equal(t, http.StatusOK, status, body)
}

func TestSTSFederation_SessionPoliciesNarrowTheSession(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "admin", fedTrust("sts:AssumeRoleWithWebIdentity"))
	idpOK(t, srv, "AttachRolePolicy", map[string]string{"RoleName": "admin", "PolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess"})

	// Without a session policy the session has the role's permissions.
	status, body := fedAssume(t, srv, "admin", ghaClaims(nil), nil)
	require.Equal(t, http.StatusOK, status, body)
	full := fedSessionKey(t, body)
	status, _ = fedIAMAs(t, srv, full, "ListRoles", nil)
	assert.Equal(t, http.StatusOK, status)

	// An inline session policy allowing only GetRole leaves the session only GetRole.
	status, body = fedAssume(t, srv, "admin", ghaClaims(nil), map[string]string{
		"Policy": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:GetRole","Resource":"*"}]}`})
	require.Equal(t, http.StatusOK, status, body)
	narrow := fedSessionKey(t, body)
	status, _ = fedIAMAs(t, srv, narrow, "GetRole", map[string]string{"RoleName": "admin"})
	assert.Equal(t, http.StatusOK, status)
	status, body = fedIAMAs(t, srv, narrow, "ListRoles", nil)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "blocked by session policy")

	// A managed session policy narrows the same way.
	status, body = fedAssume(t, srv, "admin", ghaClaims(nil), map[string]string{
		"PolicyArns.member.1.arn": "arn:aws:iam::aws:policy/IAMReadOnlyAccess"})
	require.Equal(t, http.StatusOK, status, body)
	readOnly := fedSessionKey(t, body)
	status, _ = fedIAMAs(t, srv, readOnly, "ListRoles", nil)
	assert.Equal(t, http.StatusOK, status)
	status, _ = fedIAMAs(t, srv, readOnly, "CreateUser", map[string]string{"UserName": "nope"})
	assert.Equal(t, http.StatusForbidden, status)

	for name, extra := range map[string]map[string]string{
		"malformed inline": {"Policy": `{not json`},
		"empty inline":     {"Policy": `{"Version":"2012-10-17","Statement":[]}`},
		"missing managed":  {"PolicyArns.member.1.arn": "arn:aws:iam::123456789012:policy/absent"},
		"too long":         {"Policy": strings.Repeat("x", 2049)},
		"too many managed": stsManyPolicyARNs(11),
	} {
		t.Run(name, func(t *testing.T) {
			status, body := fedAssume(t, srv, "admin", ghaClaims(nil), extra)
			assert.Equal(t, http.StatusBadRequest, status, body)
		})
	}
	status, body = fedAssume(t, srv, "admin", ghaClaims(nil), map[string]string{"PolicyArns.member.1.arn": "arn:aws:iam::123456789012:policy/absent"})
	require.Equal(t, http.StatusBadRequest, status)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "MalformedPolicyDocument", code)
	assert.Equal(t, "Policy arn:aws:iam::123456789012:policy/absent does not exist or is not attachable.", msg)
}

func stsManyPolicyARNs(n int) map[string]string {
	m := map[string]string{}
	for i := 1; i <= n; i++ {
		m["PolicyArns.member."+strconv.Itoa(i)+".arn"] = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	}
	return m
}

func TestSTSAssumeRole_SessionPolicyNarrowsTheSession(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	idpOK(t, srv, "CreateRole", map[string]string{"RoleName": "worker",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`})
	idpOK(t, srv, "AttachRolePolicy", map[string]string{"RoleName": "worker", "PolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess"})
	resp := stsRequest(t, srv, "AssumeRole", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/worker",
		"RoleSessionName": "s1", "PolicyArns.member.1.arn": "arn:aws:iam::aws:policy/IAMReadOnlyAccess"})
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	key := fedSessionKey(t, string(body))
	status, _ := fedIAMAs(t, srv, key, "ListRoles", nil)
	assert.Equal(t, http.StatusOK, status)
	status, _ = fedIAMAs(t, srv, key, "CreateUser", map[string]string{"UserName": "nope"})
	assert.Equal(t, http.StatusForbidden, status)
}

func TestSTSFederation_SessionTags(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "untaggable", fedTrust("sts:AssumeRoleWithWebIdentity"))
	fedRole(t, srv, "taggable", fedTrust("sts:AssumeRoleWithWebIdentity", "sts:TagSession"))
	// The role may list roles only for a session tagged team=ci.
	idpOK(t, srv, "PutRolePolicy", map[string]string{"RoleName": "taggable", "PolicyName": "abac",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:ListRoles","Resource":"*",
			"Condition":{"StringEquals":{"aws:PrincipalTag/team":"ci"}}}]}`})

	nested := ghaClaims(map[string]any{"https://aws.amazon.com/tags": map[string]any{
		"principal_tags": map[string]any{"team": []string{"ci"}}, "transitive_tag_keys": []string{"team"}}})
	flat := ghaClaims(map[string]any{"https://aws.amazon.com/tags/principal_tags/team": "ci"})
	other := ghaClaims(map[string]any{"https://aws.amazon.com/tags/principal_tags/team": "web"})

	status, body := fedAssume(t, srv, "untaggable", nested, nil)
	assert.Equal(t, http.StatusForbidden, status, "sts:TagSession is required to pass session tags: %s", body)

	for name, claims := range map[string]map[string]any{"nested": nested, "flattened": flat} {
		t.Run(name, func(t *testing.T) {
			status, body := fedAssume(t, srv, "taggable", claims, nil)
			require.Equal(t, http.StatusOK, status, body)
			status, body = fedIAMAs(t, srv, fedSessionKey(t, body), "ListRoles", nil)
			assert.Equal(t, http.StatusOK, status, "the session tag is the principal tag: %s", body)
		})
	}
	status, body = fedAssume(t, srv, "taggable", other, nil)
	require.Equal(t, http.StatusOK, status, body)
	status, _ = fedIAMAs(t, srv, fedSessionKey(t, body), "ListRoles", nil)
	assert.Equal(t, http.StatusForbidden, status)

	tooLong := ghaClaims(map[string]any{"https://aws.amazon.com/tags/principal_tags/" + strings.Repeat("k", 129): "v"})
	status, body = fedAssume(t, srv, "taggable", tooLong, nil)
	assert.Equal(t, http.StatusBadRequest, status, body)
}

func TestSTSFederation_SourceIdentity(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "plain", fedTrust("sts:AssumeRoleWithWebIdentity"))
	fedRole(t, srv, "audited", fedTrust("sts:AssumeRoleWithWebIdentity", "sts:SetSourceIdentity"))
	idpOK(t, srv, "PutRolePolicy", map[string]string{"RoleName": "audited", "PolicyName": "by-source",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:ListRoles","Resource":"*",
			"Condition":{"StringEquals":{"aws:SourceIdentity":"octocat"}}}]}`})
	claims := ghaClaims(map[string]any{"https://aws.amazon.com/source_identity": "octocat"})

	status, body := fedAssume(t, srv, "plain", claims, nil)
	assert.Equal(t, http.StatusForbidden, status, "sts:SetSourceIdentity is required: %s", body)

	status, body = fedAssume(t, srv, "audited", claims, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "<SourceIdentity>octocat</SourceIdentity>")
	status, _ = fedIAMAs(t, srv, fedSessionKey(t, body), "ListRoles", nil)
	assert.Equal(t, http.StatusOK, status, "the session publishes aws:SourceIdentity")

	status, body = fedAssume(t, srv, "audited", ghaClaims(map[string]any{"https://aws.amazon.com/source_identity": "aws:me"}), nil)
	require.Equal(t, http.StatusBadRequest, status, body)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "InvalidIdentityToken", code)
	assert.Contains(t, msg, "Source Identity must match")
}

func TestSTSFederation_TokenTimesAndSubject(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "r", fedTrust("sts:AssumeRoleWithWebIdentity"))
	cases := map[string]map[string]any{
		"nbf in the future": ghaClaims(map[string]any{"nbf": stsFedT0.Add(10 * time.Minute).Unix()}),
		"nbf unreadable":    ghaClaims(map[string]any{"nbf": "later"}),
		"no sub":            {"iss": ghaIssuer, "aud": "sts.amazonaws.com", "exp": stsFedT0.Add(time.Hour).Unix()},
	}
	for name, claims := range cases {
		t.Run(name, func(t *testing.T) {
			status, body := fedAssume(t, srv, "r", claims, nil)
			require.Equal(t, http.StatusBadRequest, status, body)
			code, _ := fedErrorCode(t, body)
			assert.Equal(t, "InvalidIdentityToken", code)
		})
	}
	status, body := fedAssume(t, srv, "r", ghaClaims(map[string]any{"nbf": stsFedT0.Add(-time.Minute).Unix()}), nil)
	assert.Equal(t, http.StatusOK, status, body)
}

func TestSTSAssumeRoleWithSAML_IssuerAudienceAndSessionExtras(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*samlAssertionOpts)
		trust   string
		status  int
		code    string
		message string
	}{
		{name: "issuer the metadata does not declare", mutate: func(o *samlAssertionOpts) { o.issuer = "https://evil.example.com" },
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "Issuer not present in specified provider"},
		{name: "no audience", mutate: func(o *samlAssertionOpts) { o.audience = "" },
			status: http.StatusBadRequest, code: "InvalidIdentityToken", message: "Response does not contain the required audience."},
		{name: "another audience", mutate: func(o *samlAssertionOpts) { o.audience = "https://app.example.com" },
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
		{name: "regional audience", mutate: func(o *samlAssertionOpts) { o.audience = "https://us-west-2.signin.aws.amazon.com/saml" },
			status: http.StatusOK},
		{name: "not yet valid", mutate: func(o *samlAssertionOpts) { o.notBefore = stsFedT0.Add(time.Minute) },
			status: http.StatusBadRequest, code: "InvalidIdentityToken"},
		{name: "session tag without TagSession", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/PrincipalTag:team"><saml:AttributeValue>ci</saml:AttributeValue></saml:Attribute>`
		}, status: http.StatusForbidden, code: "AccessDenied"},
		{name: "session tag with TagSession", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/PrincipalTag:team"><saml:AttributeValue>ci</saml:AttributeValue></saml:Attribute>` +
				`<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/TransitiveTagKeys"><saml:AttributeValue>team</saml:AttributeValue></saml:Attribute>`
		}, trust: "sts:TagSession", status: http.StatusOK},
		{name: "transitive key in another case", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/PrincipalTag:team"><saml:AttributeValue>ci</saml:AttributeValue></saml:Attribute>` +
				`<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/TransitiveTagKeys"><saml:AttributeValue>TEAM</saml:AttributeValue></saml:Attribute>`
		}, trust: "sts:TagSession", status: http.StatusOK},
		{name: "transitive key naming no tag", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/PrincipalTag:team"><saml:AttributeValue>ci</saml:AttributeValue></saml:Attribute>` +
				`<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/TransitiveTagKeys"><saml:AttributeValue>project</saml:AttributeValue></saml:Attribute>`
		}, trust: "sts:TagSession", status: http.StatusBadRequest, code: "InvalidIdentityToken",
			message: "names none of the assertion's session tags"},
		{name: "source identity with SetSourceIdentity", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/SourceIdentity"><saml:AttributeValue>jane</saml:AttributeValue></saml:Attribute>`
		}, trust: "sts:SetSourceIdentity", status: http.StatusOK, message: "<SourceIdentity>jane</SourceIdentity>"},
		{name: "invalid source identity", mutate: func(o *samlAssertionOpts) {
			o.extraAttrs = `<saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/SourceIdentity"><saml:AttributeValue>x</saml:AttributeValue></saml:Attribute>`
		}, trust: "sts:SetSourceIdentity", status: http.StatusBadRequest, code: "InvalidIdentityToken"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, clock := newSTSTestServer(t)
			clock.SetTime(stsFedT0)
			idpOK(t, srv, "CreateSAMLProvider", map[string]string{"Name": "Corp", "SAMLMetadataDocument": samlMetadata})
			trust := samlTrust
			if tc.trust != "" {
				trust = strings.Replace(samlTrust, `"Action":"sts:AssumeRoleWithSAML"`,
					`"Action":["sts:AssumeRoleWithSAML","`+tc.trust+`"]`, 1)
			}
			fedRole(t, srv, "saml-admin", trust)
			opts := defaultSAML()
			tc.mutate(&opts)
			status, body := fedSTS(t, srv, "AssumeRoleWithSAML", map[string]string{
				"RoleArn": samlRole, "PrincipalArn": samlProvider, "SAMLAssertion": opts.encode()})
			require.Equal(t, tc.status, status, body)
			if tc.status == http.StatusOK {
				assert.Contains(t, body, tc.message)
				return
			}
			code, msg := fedErrorCode(t, body)
			assert.Equal(t, tc.code, code)
			assert.Contains(t, msg, tc.message)
		})
	}
}

func TestIAMSAMLProvider_MetadataMustDeclareAnEntityID(t *testing.T) {
	srv := newIAMTestServer(t)
	noEntity := strings.Replace(samlMetadata, ` entityID="https://idp.example.com/saml"`, "", 1)
	status, body := idpCall(t, srv, "CreateSAMLProvider", map[string]string{"Name": "NoID", "SAMLMetadataDocument": noEntity})
	assert.Equal(t, http.StatusBadRequest, status, body)
	assert.Contains(t, body, "Could not parse metadata.")

	// An EntitiesDescriptor declares its entities' IDs.
	many := `<md:EntitiesDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata">` +
		`<md:EntityDescriptor entityID="https://a.example.com"/><md:EntityDescriptor entityID="https://b.example.com"/>` +
		`</md:EntitiesDescriptor>`
	idpOK(t, srv, "CreateSAMLProvider", map[string]string{"Name": "Many", "SAMLMetadataDocument": many})
}
