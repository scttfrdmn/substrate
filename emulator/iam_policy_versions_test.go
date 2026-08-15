package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetPolicyVersion and ListPolicyVersions (#498).
//
// The point of these operations is that a managed policy's *document* becomes observable
// over the wire. GetPolicy returns metadata only, matching AWS, so before this the 52
// bundled documents — several copied verbatim from their AWS reference pages — were
// readable by a Go consumer through GetManagedPolicy and by no consumer over HTTP.

// policyVersionXML mirrors a PolicyVersion element. Document is a pointer so its
// *absence* is observable: ListPolicyVersions must not send it.
type policyVersionXML struct {
	VersionID        string  `xml:"VersionId"`
	IsDefaultVersion bool    `xml:"IsDefaultVersion"`
	CreateDate       string  `xml:"CreateDate"`
	Document         *string `xml:"Document"`
}

// policyVersionResponse decodes either operation's response, so one decoder serves both.
type policyVersionResponse struct {
	Version   *policyVersionXML  `xml:"GetPolicyVersionResult>PolicyVersion"`
	Versions  []policyVersionXML `xml:"ListPolicyVersionsResult>Versions>member"`
	Truncated bool               `xml:"ListPolicyVersionsResult>IsTruncated"`
}

// iamDecodePolicyVersion posts an IAM request, requires 200, and decodes it.
func iamDecodePolicyVersion(t *testing.T, resp *http.Response) policyVersionResponse {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)
	require.NotContains(t, string(raw), "InvalidAction")

	var decoded policyVersionResponse
	require.NoError(t, xml.Unmarshal(raw, &decoded), "body: %s", raw)
	return decoded
}

// iamPolicyDocumentFor fetches a policy version's document and URL-decodes it, which is
// how a consumer reads it: the response carries it RFC 3986 percent-encoded.
func iamPolicyDocumentFor(t *testing.T, srv *emulator.Server, arn, versionID string) string {
	t.Helper()
	got := iamDecodePolicyVersion(t, iamRequest(t, srv, "GetPolicyVersion", map[string]any{
		"PolicyArn": arn,
		"VersionId": versionID,
	}))
	require.NotNil(t, got.Version)
	require.NotNil(t, got.Version.Document, "GetPolicyVersion must return the document")

	decoded, err := url.QueryUnescape(*got.Version.Document)
	require.NoError(t, err, "the document must decode as a URL-encoded value")
	return decoded
}

// TestGetPolicyVersion_ReturnsABundledDocument is the reason the operation exists: a
// document that only a Go caller could read is now readable over the wire, and it decodes
// to the JSON the catalog holds.
func TestGetPolicyVersion_ReturnsABundledDocument(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	catalog, ok := emulator.GetManagedPolicy(arn)
	require.True(t, ok, "the catalog must carry the policy this test reads")

	document := iamPolicyDocumentFor(t, srv, arn, catalog.DefaultVersionID)

	// Compared as parsed documents rather than as bytes: the assertion is about the
	// policy's content, and pinning substrate's own marshaling order would fail on a
	// harmless field reordering while saying nothing about fidelity.
	var got, want emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(document), &got))
	wantRaw, err := json.Marshal(catalog.Document)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(wantRaw, &want))
	assert.Equal(t, want, got)

	// And the document actually says what AmazonS3ReadOnlyAccess says, so a test that
	// round-tripped an empty document would not pass.
	assert.Contains(t, document, "s3:Get*")
	assert.Equal(t, "2012-10-17", got.Version)
}

// TestGetPolicyVersion_DocumentIsRFC3986Encoded pins the encoding, which the reference
// specifies and which no stdlib escaper produces.
//
// url.QueryEscape encodes a space as "+", which a strict RFC 3986 decoder reads back as a
// literal plus — silently corrupting any document containing a space. url.PathEscape
// leaves ":" bare. Most SDKs decode automatically, so getting this wrong breaks the raw
// HTTP client and nobody else, which is the kind of gap that only shows up in production.
func TestGetPolicyVersion_DocumentIsRFC3986Encoded(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	// A Sid holding a space and a tilde: the space distinguishes RFC 3986 from
	// QueryEscape, and the tilde is unreserved so it must survive un-encoded.
	document := `{"Version":"2012-10-17","Statement":[{"Sid":"needs escaping~","Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*"}]}`
	arn := iamCreatePolicyForVersions(t, srv, "spacey", document)

	got := iamDecodePolicyVersion(t, iamRequest(t, srv, "GetPolicyVersion", map[string]any{
		"PolicyArn": arn,
		"VersionId": "v1",
	}))
	require.NotNil(t, got.Version)
	require.NotNil(t, got.Version.Document)
	encoded := *got.Version.Document

	assert.NotContains(t, encoded, "+", "a space must be %20, not +, per RFC 3986")
	assert.Contains(t, encoded, "%20")
	assert.Contains(t, encoded, "~", "a tilde is unreserved and must not be encoded")
	assert.Contains(t, encoded, "%3A", "a colon is reserved and must be encoded")
	assert.NotContains(t, encoded, `"`, "every reserved character must be encoded")

	// The decisive property: it round-trips through a strict decoder.
	decoded, err := url.PathUnescape(encoded)
	require.NoError(t, err)
	var parsed emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(decoded), &parsed))
	require.Len(t, parsed.Statement, 1)
	assert.Equal(t, "needs escaping~", parsed.Statement[0].Sid)
}

// TestGetPolicyVersion_ReturnsACustomerManagedDocument covers the other resolution arm:
// a policy created through CreatePolicy behaves the same as a bundled one.
func TestGetPolicyVersion_ReturnsACustomerManagedDocument(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	document := iamPolicyJSON(t, "Allow", []string{"s3:ListAllMyBuckets"}, "*")
	arn := iamCreatePolicyForVersions(t, srv, "listbuckets", document)

	got := iamPolicyDocumentFor(t, srv, arn, "v1")
	var parsed emulator.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(got), &parsed))
	require.Len(t, parsed.Statement, 1)
	assert.Equal(t, emulator.StringOrSlice{"s3:ListAllMyBuckets"}, parsed.Statement[0].Action)
}

// TestGetPolicyVersion_IsDefaultVersionTracksThePolicy pins IsDefaultVersion against the
// two bundled policies whose default is not v1.
//
// AWS has edited these policies since publication, and their reference pages report v2 and
// v3; substrate's catalog carries those values. So a caller asking for the default gets
// IsDefaultVersion true under the version ID the policy actually names — and asking for
// v1 is refused, which is what makes this pair worth asserting rather than assuming.
func TestGetPolicyVersion_IsDefaultVersionTracksThePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		arn         string
		wantVersion string
	}{
		{
			name:        "a policy AWS reports at v2",
			arn:         "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
			wantVersion: "v2",
		},
		{
			name:        "a policy AWS reports at v3",
			arn:         "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
			wantVersion: "v3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)

			catalog, ok := emulator.GetManagedPolicy(tc.arn)
			require.True(t, ok)
			require.Equal(t, tc.wantVersion, catalog.DefaultVersionID,
				"the catalog's default version is the premise of this test")

			got := iamDecodePolicyVersion(t, iamRequest(t, srv, "GetPolicyVersion", map[string]any{
				"PolicyArn": tc.arn,
				"VersionId": tc.wantVersion,
			}))
			require.NotNil(t, got.Version)
			assert.Equal(t, tc.wantVersion, got.Version.VersionID)
			assert.True(t, got.Version.IsDefaultVersion)

			// v1 is a version AWS *has* and substrate does not store, so it is refused
			// rather than served with the default's document under the wrong name.
			resp := iamRequest(t, srv, "GetPolicyVersion", map[string]any{
				"PolicyArn": tc.arn,
				"VersionId": "v1",
			})
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "NoSuchEntity", result["__type"])
		})
	}
}

// TestListPolicyVersions_OmitsTheDocument pins the member the model says this operation
// does not send.
//
// PolicyVersion.Document is documented as returned by GetPolicyVersion and
// GetAccountAuthorizationDetails, and *not* by ListPolicyVersions or CreatePolicyVersion.
// Sending it would hand a caller a member AWS omits — the kind of difference that makes a
// consumer work against substrate and fail against AWS.
func TestListPolicyVersions_OmitsTheDocument(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	got := iamDecodePolicyVersion(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": arn,
	}))

	require.Len(t, got.Versions, 1, "substrate models exactly one version per policy")
	assert.Nil(t, got.Versions[0].Document,
		"ListPolicyVersions must not send the document")
	assert.Equal(t, "v1", got.Versions[0].VersionID)
	assert.True(t, got.Versions[0].IsDefaultVersion)
	assert.NotEmpty(t, got.Versions[0].CreateDate)
	assert.False(t, got.Truncated)
}

// TestListPolicyVersions_ReportsOneVersionForACreatedPolicy covers the state arm and the
// pagination members.
//
// One version cannot truncate, but IsTruncated must still be present and MaxItems must not
// fail the request — which it did for every paginated IAM operation before #642, because
// the query protocol sends it as a string.
func TestListPolicyVersions_ReportsOneVersionForACreatedPolicy(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	arn := iamCreatePolicyForVersions(t, srv, "listable",
		iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"))

	got := iamDecodePolicyVersion(t, iamRequest(t, srv, "ListPolicyVersions", map[string]any{
		"PolicyArn": arn,
		"MaxItems":  10,
	}))
	require.Len(t, got.Versions, 1)
	assert.Equal(t, "v1", got.Versions[0].VersionID)
	assert.False(t, got.Truncated)
}

// TestPolicyVersions_RefuseBadInput covers the InvalidInput and NoSuchEntity arms of both
// operations.
//
// The malformed-VersionId case is checked before the policy is resolved on purpose: a
// version ID that cannot match the model's pattern is the caller's error whether or not
// the policy exists, and answering NoSuchEntity would send them looking for a policy that
// is right there.
func TestPolicyVersions_RefuseBadInput(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const real = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		wantCode  string
		wantHTTP  int
	}{
		{
			name:      "GetPolicyVersion requires PolicyArn",
			operation: "GetPolicyVersion",
			body:      map[string]any{"VersionId": "v1"},
			wantCode:  "ValidationError",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "GetPolicyVersion requires VersionId",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real},
			wantCode:  "ValidationError",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "ListPolicyVersions requires PolicyArn",
			operation: "ListPolicyVersions",
			body:      map[string]any{},
			wantCode:  "ValidationError",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "a VersionId with no v prefix is InvalidInput",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "1"},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			// The model's pattern is v[1-9]…, so a zero first digit cannot occur.
			name:      "v0 is InvalidInput",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "v0"},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "a non-numeric VersionId is InvalidInput",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "latest"},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			// The pattern is anchored, so a well-formed ID embedded in other text is
			// refused rather than matched loosely.
			name:      "a VersionId with trailing junk is InvalidInput",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "v1 or so"},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			// Checked before resolution: the parameter is wrong, not the policy.
			name:      "a malformed VersionId is refused even for an unknown policy",
			operation: "GetPolicyVersion",
			body: map[string]any{
				"PolicyArn": "arn:aws:iam::123456789012:policy/ghost",
				"VersionId": "nope",
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "an unknown PolicyArn is NoSuchEntity for GetPolicyVersion",
			operation: "GetPolicyVersion",
			body: map[string]any{
				"PolicyArn": "arn:aws:iam::123456789012:policy/ghost",
				"VersionId": "v1",
			},
			wantCode: "NoSuchEntity",
			wantHTTP: http.StatusNotFound,
		},
		{
			name:      "an unknown PolicyArn is NoSuchEntity for ListPolicyVersions",
			operation: "ListPolicyVersions",
			body:      map[string]any{"PolicyArn": "arn:aws:iam::123456789012:policy/ghost"},
			wantCode:  "NoSuchEntity",
			wantHTTP:  http.StatusNotFound,
		},
		{
			// A version substrate does not store, on a policy it does.
			name:      "a non-default VersionId is NoSuchEntity",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "v9"},
			wantCode:  "NoSuchEntity",
			wantHTTP:  http.StatusNotFound,
		},
		{
			// A dotted suffix is legal per the pattern, so it passes the shape check and
			// is then refused as a version substrate does not hold — which proves the two
			// checks are distinct.
			name:      "a legal dotted VersionId that does not exist is NoSuchEntity",
			operation: "GetPolicyVersion",
			body:      map[string]any{"PolicyArn": real, "VersionId": "v2.abc-1"},
			wantCode:  "NoSuchEntity",
			wantHTTP:  http.StatusNotFound,
		},
		{
			name:      "MaxItems that is not a number is refused by name",
			operation: "ListPolicyVersions",
			body:      map[string]any{"PolicyArn": real, "MaxItems": "abc"},
			wantCode:  "ValidationError",
			wantHTTP:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := iamRequest(t, srv, tc.operation, tc.body)
			require.Equal(t, tc.wantHTTP, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, tc.wantCode, result["__type"])
		})
	}
}

// TestPolicyVersions_AreAuthorized covers the authorization arm on both operations, so the
// iam:GetPolicyVersion and iam:ListPolicyVersions grants in the bundled policies mean
// something.
func TestPolicyVersions_AreAuthorized(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"GetPolicyVersion", "ListPolicyVersions"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCallerWithoutAssume(t, srv, "ungranted")

			resp := iamCallAs(t, srv, accessKey, op, map[string]any{
				"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
				"VersionId": "v1",
			})
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, emulator.IAMAccessDeniedCodeForTest, result["__type"])
		})
	}
}

// TestPolicyVersionsWire_OverTheQueryProtocol drives both operations the way a client
// does. An operation that is implemented but unroutable answers InvalidAction with a fully
// green unit suite, which is what #636 was.
func TestPolicyVersionsWire_OverTheQueryProtocol(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

	got := iamDecodePolicyVersion(t, iamFormRequest(t, srv, "GetPolicyVersion",
		map[string]string{"PolicyArn": arn, "VersionId": "v1"}))
	require.NotNil(t, got.Version)
	require.NotNil(t, got.Version.Document)
	decoded, err := url.PathUnescape(*got.Version.Document)
	require.NoError(t, err)
	assert.Contains(t, decoded, "logs:PutLogEvents")

	// MaxItems arrives as a string over this wire — the #642 shape.
	listed := iamDecodePolicyVersion(t, iamFormRequest(t, srv, "ListPolicyVersions",
		map[string]string{"PolicyArn": arn, "MaxItems": "5"}))
	require.Len(t, listed.Versions, 1)
	assert.Nil(t, listed.Versions[0].Document)
}

// iamCreatePolicyForVersions creates a customer-managed policy and returns its ARN.
//
// The ARN is read back rather than constructed: an unsigned test request resolves to the
// fallback account, so a hardcoded 123456789012 ARN would name a policy that does not
// exist and the test would assert against a NoSuchEntity it never expected.
func iamCreatePolicyForVersions(t *testing.T, srv *emulator.Server, name, document string) string {
	t.Helper()
	resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName":     name,
		"PolicyDocument": document,
	})
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)

	var created struct {
		ARN string `xml:"CreatePolicyResult>Policy>Arn"`
	}
	require.NoError(t, xml.Unmarshal(raw, &created))
	require.NotEmpty(t, created.ARN)
	return created.ARN
}
