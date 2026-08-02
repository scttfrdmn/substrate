package emulator_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Public-group grantee URIs, spelled out here rather than imported so a change to
// the constants in the package under test has to be reflected deliberately.
const (
	testGroupAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	testGroupAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

// pabBlockACLsOnly and pabBlockPolicyOnly turn on exactly one of the two settings
// substrate enforces, so a test proving one is enforced cannot pass because the
// other happened to fire.
const (
	pabBlockACLsOnly = `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <BlockPublicAcls>true</BlockPublicAcls>
</PublicAccessBlockConfiguration>`

	pabBlockPolicyOnly = `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <BlockPublicPolicy>true</BlockPublicPolicy>
</PublicAccessBlockConfiguration>`

	// pabAllFalse is the configuration that exists but blocks nothing. It is the
	// case that distinguishes "configured, all four false" from "unconfigured", and
	// the gate on not having regressed the record-only behavior of #446.
	pabAllFalse = `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <BlockPublicAcls>false</BlockPublicAcls>
  <IgnorePublicAcls>false</IgnorePublicAcls>
  <BlockPublicPolicy>false</BlockPublicPolicy>
  <RestrictPublicBuckets>false</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>`
)

// aclXML builds an AccessControlPolicy body granting permission to a grantee URI,
// which is how an ACL arrives when it is not one of the canned values.
func aclXML(granteeURI, permission string) string {
	if granteeURI == "" {
		return `<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy>
  <Owner><ID>owner-id</ID><DisplayName>owner</DisplayName></Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>owner-id</ID>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>`
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy>
  <Owner><ID>owner-id</ID><DisplayName>owner</DisplayName></Owner>
  <AccessControlList>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
        <ID>owner-id</ID>
      </Grantee>
      <Permission>FULL_CONTROL</Permission>
    </Grant>
    <Grant>
      <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
        <URI>%s</URI>
      </Grantee>
      <Permission>%s</Permission>
    </Grant>
  </AccessControlList>
</AccessControlPolicy>`, granteeURI, permission)
}

// policyJSON builds a one-statement bucket policy. condition is spliced in raw so
// a test can express any condition block, including a malformed one.
func policyJSON(effect, principal, condition string) string {
	if condition == "" {
		return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[
  {"Sid":"s","Effect":%q,"Principal":%s,"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			effect, principal)
	}
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[
  {"Sid":"s","Effect":%q,"Principal":%s,"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*",
   "Condition":%s}]}`, effect, principal, condition)
}

// s3ErrorCode decodes the Code from an S3 XML error body.
func s3ErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var out struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), "decode error body: %s", string(body))
	return out.Code
}

// s3ErrorMessage decodes the Message from an S3 XML error body.
func s3ErrorMessage(t *testing.T, body []byte) string {
	t.Helper()
	var out struct {
		Message string `xml:"Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), "decode error body: %s", string(body))
	return out.Message
}

// getBucketACLGrants reads a bucket's stored ACL back as grantee-URI/permission
// pairs, which is the surface a "nothing was stored" assertion has to be made
// against.
func getBucketACLGrants(t *testing.T, srv *emulator.Server, bucket string) []string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?acl", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetBucketAcl: %s", w.Body.String())
	return aclGrantPairs(t, w.Body.Bytes())
}

// getObjectACLGrants is getBucketACLGrants for an object.
func getObjectACLGrants(t *testing.T, srv *emulator.Server, bucket, key string) []string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key+"?acl", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetObjectAcl: %s", w.Body.String())
	return aclGrantPairs(t, w.Body.Bytes())
}

// aclGrantPairs renders an ACL response as "<grantee>/<permission>" strings.
func aclGrantPairs(t *testing.T, body []byte) []string {
	t.Helper()
	var out struct {
		Grants []struct {
			URI        string `xml:"Grantee>URI"`
			ID         string `xml:"Grantee>ID"`
			Permission string `xml:"Permission"`
		} `xml:"AccessControlList>Grant"`
	}
	require.NoError(t, xml.Unmarshal(body, &out), "decode acl: %s", string(body))
	pairs := make([]string, 0, len(out.Grants))
	for _, g := range out.Grants {
		grantee := g.URI
		if grantee == "" {
			grantee = g.ID
		}
		pairs = append(pairs, grantee+"/"+g.Permission)
	}
	return pairs
}

// getBucketPolicyBody reads a bucket policy back, or "" when there is none.
func getBucketPolicyBody(t *testing.T, srv *emulator.Server, bucket string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?policy", nil, nil)
	if w.Code == http.StatusNotFound {
		return ""
	}
	require.Equal(t, http.StatusOK, w.Code, "GetBucketPolicy: %s", w.Body.String())
	return w.Body.String()
}

// aclCase is one public-or-not ACL, expressed in whichever of the three forms
// carries it.
type aclCase struct {
	name    string
	body    string            // AccessControlPolicy XML, or "" to use headers
	headers map[string]string // x-amz-acl or x-amz-grant-*
	public  bool
}

// publicACLCases covers all three documented ways a public ACL arrives, plus the
// non-public counterparts that must keep working on a fully blocked bucket.
func publicACLCases() []aclCase {
	return []aclCase{
		{name: "canned public-read", headers: map[string]string{"x-amz-acl": "public-read"}, public: true},
		{name: "canned public-read-write", headers: map[string]string{"x-amz-acl": "public-read-write"}, public: true},
		{name: "xml grant to AllUsers", body: aclXML(testGroupAllUsers, "READ"), public: true},
		{name: "xml grant to AuthenticatedUsers", body: aclXML(testGroupAuthenticatedUsers, "READ"), public: true},
		{
			// "any permissions" is literal: WRITE_ACP counts as much as READ.
			name:   "xml WRITE_ACP grant to AllUsers",
			body:   aclXML(testGroupAllUsers, "WRITE_ACP"),
			public: true,
		},
		{
			name:    "x-amz-grant-read naming AllUsers",
			headers: map[string]string{"x-amz-grant-read": `uri="` + testGroupAllUsers + `"`},
			public:  true,
		},
		{
			name: "x-amz-grant-full-control naming AuthenticatedUsers unquoted",
			headers: map[string]string{
				"x-amz-grant-full-control": "uri=" + testGroupAuthenticatedUsers,
			},
			public: true,
		},
		{
			// A grant list mixing a canonical user with a public group is public on
			// the second grantee, so the parse cannot stop at the first.
			name: "x-amz-grant-read list ending in AllUsers",
			headers: map[string]string{
				"x-amz-grant-read": `id="abc123", uri="` + testGroupAllUsers + `"`,
			},
			public: true,
		},
		{name: "canned private", headers: map[string]string{"x-amz-acl": "private"}, public: false},
		{name: "owner-only xml", body: aclXML("", ""), public: false},
		{
			name:    "x-amz-grant-read naming a canonical user",
			headers: map[string]string{"x-amz-grant-read": `id="abc123"`},
			public:  false,
		},
		{
			// A non-public group URI must not be mistaken for a public one.
			name:   "xml grant to LogDelivery",
			body:   aclXML("http://acs.amazonaws.com/groups/s3/LogDelivery", "WRITE"),
			public: false,
		},
	}
}

// TestS3_BlockPublicAcls_PutBucketAcl asserts that a bucket with BlockPublicAcls
// set refuses a public ACL and keeps the one it already had (#458).
func TestS3_BlockPublicAcls_PutBucketAcl(t *testing.T) {
	t.Parallel()

	for _, tc := range publicACLCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			bucket := newPABBucket(t, srv, "acl-bucket")
			require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabBlockACLsOnly).Code)

			before := getBucketACLGrants(t, srv, bucket)

			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?acl", nilIfEmptyBody(tc.body), tc.headers)
			if !tc.public {
				require.Equal(t, http.StatusOK, w.Code, "PutBucketAcl should be accepted: %s", w.Body.String())
				return
			}

			require.Equal(t, http.StatusForbidden, w.Code, "PutBucketAcl should be refused: %s", w.Body.String())
			assert.Equal(t, "AccessDenied", s3ErrorCode(t, w.Body.Bytes()))
			assert.Equal(t, "Access Denied", s3ErrorMessage(t, w.Body.Bytes()))

			// "existing policies and ACLs for buckets and objects aren't modified" —
			// a rejection must store nothing.
			assert.Equal(t, before, getBucketACLGrants(t, srv, bucket),
				"a refused PutBucketAcl must leave the existing ACL in place")
		})
	}
}

// TestS3_BlockPublicAcls_PutObjectAcl is the object-level counterpart. The
// configuration read is the bucket's: Block Public Access has no per-object
// setting.
func TestS3_BlockPublicAcls_PutObjectAcl(t *testing.T) {
	t.Parallel()

	for _, tc := range publicACLCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			bucket := newPABBucket(t, srv, "obj-acl-bucket")
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket+"/k.txt", []byte("hi"), nil).Code)
			require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabBlockACLsOnly).Code)

			before := getObjectACLGrants(t, srv, bucket, "k.txt")

			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/k.txt?acl", nilIfEmptyBody(tc.body), tc.headers)
			if !tc.public {
				require.Equal(t, http.StatusOK, w.Code, "PutObjectAcl should be accepted: %s", w.Body.String())
				return
			}

			require.Equal(t, http.StatusForbidden, w.Code, "PutObjectAcl should be refused: %s", w.Body.String())
			assert.Equal(t, "AccessDenied", s3ErrorCode(t, w.Body.Bytes()))
			assert.Equal(t, before, getObjectACLGrants(t, srv, bucket, "k.txt"),
				"a refused PutObjectAcl must leave the existing ACL in place")
		})
	}
}

// TestS3_BlockPublicAcls_UnenforcedConfigurations is the gate on not having
// regressed #446: with no configuration at all, or a configuration whose four
// members are false, or one that sets only the other three settings, every public
// ACL is accepted exactly as before enforcement existed.
func TestS3_BlockPublicAcls_UnenforcedConfigurations(t *testing.T) {
	t.Parallel()

	configs := []struct {
		name string
		body string // "" means send no PutPublicAccessBlock at all
	}{
		{name: "no configuration", body: ""},
		{name: "all four false", body: pabAllFalse},
		{
			// The three settings substrate records but does not enforce must not
			// acquire enforcement by accident.
			name: "only the other three settings",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration>
  <IgnorePublicAcls>true</IgnorePublicAcls>
  <BlockPublicPolicy>true</BlockPublicPolicy>
  <RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>`,
		},
	}

	for _, cfg := range configs {
		for _, tc := range publicACLCases() {
			if !tc.public {
				continue
			}
			t.Run(cfg.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				srv, _ := newS3TestServer(t)
				bucket := newPABBucket(t, srv, "unblocked")
				if cfg.body != "" {
					require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, cfg.body).Code)
				}

				w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?acl", nilIfEmptyBody(tc.body), tc.headers)
				assert.Equal(t, http.StatusOK, w.Code,
					"a public ACL must still be accepted: %s", w.Body.String())
			})
		}
	}
}

// TestS3_BlockPublicAcls_DeleteReenablesPublicAcls covers the documented
// reversibility: "removing a block public access setting causes a bucket or object
// with a public policy or ACL to again be publicly accessible".
func TestS3_BlockPublicAcls_DeleteReenablesPublicAcls(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "reversible")

	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabAllTrue).Code)
	blocked := s3Request(t, srv, http.MethodPut, "/"+bucket+"?acl", nil,
		map[string]string{"x-amz-acl": "public-read"})
	require.Equal(t, http.StatusForbidden, blocked.Code, "%s", blocked.Body.String())

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/"+bucket+"?publicAccessBlock", nil, nil).Code)

	allowed := s3Request(t, srv, http.MethodPut, "/"+bucket+"?acl", nil,
		map[string]string{"x-amz-acl": "public-read"})
	assert.Equal(t, http.StatusOK, allowed.Code,
		"deleting the configuration must re-allow a public ACL: %s", allowed.Body.String())
	assert.Contains(t, getBucketACLGrants(t, srv, bucket), testGroupAllUsers+"/READ")
}

// policyCase is one bucket policy and whether Block Public Access considers it
// public.
type policyCase struct {
	name   string
	policy string
	public bool
}

// publicPolicyCases is the documented rule reduced to cases. Every entry marked
// public exercises the assume-public-then-qualify direction rather than
// wildcard-detection.
func publicPolicyCases() []policyCase {
	return []policyCase{
		{
			name:   "wildcard principal, no condition",
			policy: policyJSON("Allow", `"*"`, ""),
			public: true,
		},
		{
			// The guide's own example, and the case a wildcard-detecting
			// implementation gets wrong: the narrowing value is itself a wildcard.
			name:   "wildcard principal narrowed by StringLike vpc-*",
			policy: policyJSON("Allow", `"*"`, `{"StringLike":{"aws:SourceVpc":"vpc-*"}}`),
			public: true,
		},
		{
			name:   "wildcard principal pinned to a fixed vpc",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"aws:SourceVpc":"vpc-91237329"}}`),
			public: false,
		},
		{
			name:   "fixed account principal",
			policy: policyJSON("Allow", `{"AWS":"arn:aws:iam::123456789012:root"}`, ""),
			public: false,
		},
		{
			name:   "fixed service principal",
			policy: policyJSON("Allow", `{"Service":"cloudtrail.amazonaws.com"}`, ""),
			public: false,
		},
		{
			name:   "wildcard principal pinned to a /24",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"203.0.113.0/24"}}`),
			public: false,
		},
		{
			// Broader than /8, so public despite carrying no wildcard character.
			name:   "wildcard principal pinned to 0.0.0.0/1",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"0.0.0.0/1"}}`),
			public: true,
		},
		{
			name:   "wildcard principal pinned to 0.0.0.0/0",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}`),
			public: true,
		},
		{
			// /8 is the documented bound, read as acceptable rather than "broader
			// than /8".
			name:   "wildcard principal pinned to a public /8",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"203.0.0.0/8"}}`),
			public: false,
		},
		{
			// The RFC1918 exclusion: a private range is never internet-reachable, so
			// its breadth cannot make the bucket public.
			name:   "wildcard principal pinned to 10.0.0.0/8",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}`),
			public: false,
		},
		{
			name:   "wildcard principal pinned to a single host",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"203.0.113.7"}}`),
			public: false,
		},
		{
			// IPv6's bound is /32, so a /48 pins and a /16 does not.
			name:   "wildcard principal pinned to an IPv6 /48",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"2001:db8:1234::/48"}}`),
			public: false,
		},
		{
			name:   "wildcard principal pinned to an IPv6 /16",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"2001::/16"}}`),
			public: true,
		},
		{
			// The RFC1918 exclusion, on the family where it changes the answer: a
			// unique-local IPv6 range is broader than the /32 bound but is private, so
			// its breadth cannot make the bucket public. (For IPv4 the exclusion is
			// unobservable, since the broadest private range is 10.0.0.0/8 and the /8
			// bound already admits it.)
			name:   "wildcard principal pinned to an IPv6 unique-local /8",
			policy: policyJSON("Allow", `"*"`, `{"IpAddress":{"aws:SourceIp":"fd00::/8"}}`),
			public: false,
		},
		{
			// One wildcard value in a list admits the wildcard, so the list pins
			// nothing.
			name:   "condition list mixing a fixed and a wildcard value",
			policy: policyJSON("Allow", `"*"`, `{"StringLike":{"aws:SourceVpc":["vpc-91237329","vpc-*"]}}`),
			public: true,
		},
		{
			// A policy variable is not a fixed value, same as a wildcard.
			name:   "condition pinned to a policy variable",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"aws:SourceVpc":"${aws:PrincipalTag/vpc}"}}`),
			public: true,
		},
		{
			// A key that is not on the qualifying list cannot make a statement
			// non-public however fixed its value.
			name:   "condition on a non-qualifying key",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"s3:prefix":"public/"}}`),
			public: true,
		},
		{
			name:   "deny to everyone",
			policy: policyJSON("Deny", `"*"`, ""),
			public: false,
		},
		{
			name:   "wildcard inside a principal ARN",
			policy: policyJSON("Allow", `{"AWS":"arn:aws:iam::123456789012:user/*"}`, ""),
			public: true,
		},
		{
			name:   "wildcard principal pinned by aws:PrincipalOrgID",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"aws:PrincipalOrgID":"o-abc123"}}`),
			public: false,
		},
		{
			name:   "wildcard principal pinned by aws:SourceAccount",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"aws:SourceAccount":"123456789012"}}`),
			public: false,
		},
		{
			// aws:userid qualifies "outside the pattern AROLEID:*", and the trailing
			// wildcard is what puts this value inside it.
			name:   "wildcard principal pinned by an AROLEID:* userid",
			policy: policyJSON("Allow", `"*"`, `{"StringLike":{"aws:userid":"AROAEXAMPLEID:*"}}`),
			public: true,
		},
		{
			// The s3:DataAccessPointArn carve-out: a wildcard access-point name does
			// not make a *bucket* policy public so long as the account is fixed.
			name: "wildcard principal pinned by a wildcard access-point ARN",
			policy: policyJSON("Allow", `"*"`,
				`{"StringEquals":{"s3:DataAccessPointArn":"arn:aws:s3:us-west-2:123456789012:accesspoint/*"}}`),
			public: false,
		},
		{
			// …but a wildcard in the account field is not carved out.
			name: "wildcard principal pinned by a wildcard-account access-point ARN",
			policy: policyJSON("Allow", `"*"`,
				`{"StringEquals":{"s3:DataAccessPointArn":"arn:aws:s3:us-west-2:*:accesspoint/ap"}}`),
			public: true,
		},
		{
			// A condition key is case-insensitive, so the qualifying-key lookup must
			// be too.
			name:   "qualifying key in a different case",
			policy: policyJSON("Allow", `"*"`, `{"StringEquals":{"AWS:SourceVpc":"vpc-91237329"}}`),
			public: false,
		},
		{
			name: "fixed cross-account grant plus one public statement",
			policy: `{"Version":"2012-10-17","Statement":[
  {"Sid":"trail","Effect":"Allow","Principal":{"Service":"cloudtrail.amazonaws.com"},
   "Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*"},
  {"Sid":"acct2","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::210987654321:root"},
   "Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"},
  {"Sid":"pub","Effect":"Allow","Principal":"*",
   "Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			public: true,
		},
		{
			// The same policy without the third statement is not public — the guide's
			// "if you remove statement 3" half of the worked example.
			name: "fixed cross-account grant alone",
			policy: `{"Version":"2012-10-17","Statement":[
  {"Sid":"trail","Effect":"Allow","Principal":{"Service":"cloudtrail.amazonaws.com"},
   "Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*"},
  {"Sid":"acct2","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::210987654321:root"},
   "Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			public: false,
		},
		{
			// Effect is case-insensitive in IAM, so a lowercase "allow" must not slip
			// a public statement past the check.
			name:   "lowercase allow with a wildcard principal",
			policy: policyJSON("allow", `"*"`, ""),
			public: true,
		},
		{
			// JSON that is not a policy document must not acquire a new rejection
			// reason from this check. PutBucketPolicy's own MalformedPolicy check
			// already governs the non-JSON case.
			name:   "json object with no statements",
			policy: `{"not":"a policy"}`,
			public: false,
		},
		{
			// A body that is a JSON object — so it clears PutBucketPolicy's
			// MalformedPolicy check — but does not unmarshal as a policy document.
			// Substrate accepted this before enforcement existed and must still, for
			// the same reason: the new check is not a validity check.
			name:   "json object whose Statement is not an array",
			policy: `{"Version":"2012-10-17","Statement":"nope"}`,
			public: false,
		},
		{
			// A statement with no Principal at all. Principal is required in a
			// resource policy, so this is malformed — and substrate does not validate
			// that. Assuming public is the direction that matches S3's
			// assume-public-then-qualify start; the alternative would let a caller
			// bypass the block by omitting the field.
			name: "allow statement with no principal",
			policy: `{"Version":"2012-10-17","Statement":[
  {"Sid":"s","Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			public: true,
		},
	}
}

// TestS3_BlockPublicPolicy_PutBucketPolicy asserts that a bucket with
// BlockPublicPolicy set refuses a public policy and keeps the one it had (#458).
func TestS3_BlockPublicPolicy_PutBucketPolicy(t *testing.T) {
	t.Parallel()

	for _, tc := range publicPolicyCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			bucket := newPABBucket(t, srv, "policy-bucket")

			// A pre-existing non-public policy makes the "nothing was stored"
			// assertion meaningful: a rejection must leave this one readable.
			prior := policyJSON("Allow", `{"AWS":"arn:aws:iam::123456789012:root"}`, "")
			require.Equal(t, http.StatusNoContent,
				s3Request(t, srv, http.MethodPut, "/"+bucket+"?policy", []byte(prior), nil).Code)
			require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabBlockPolicyOnly).Code)

			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?policy", []byte(tc.policy), nil)
			if !tc.public {
				require.Equal(t, http.StatusNoContent, w.Code,
					"PutBucketPolicy should be accepted: %s", w.Body.String())
				assert.JSONEq(t, tc.policy, getBucketPolicyBody(t, srv, bucket))
				return
			}

			require.Equal(t, http.StatusForbidden, w.Code,
				"PutBucketPolicy should be refused: %s", w.Body.String())
			assert.Equal(t, "AccessDenied", s3ErrorCode(t, w.Body.Bytes()))
			assert.Equal(t, "Access Denied", s3ErrorMessage(t, w.Body.Bytes()))
			assert.JSONEq(t, prior, getBucketPolicyBody(t, srv, bucket),
				"a refused PutBucketPolicy must leave the existing policy in place")
		})
	}
}

// TestS3_BlockPublicPolicy_UnenforcedConfigurations is the policy-side gate on
// #446: unconfigured, all-false, and the other-three-settings configurations all
// accept a public policy.
func TestS3_BlockPublicPolicy_UnenforcedConfigurations(t *testing.T) {
	t.Parallel()

	configs := []struct {
		name string
		body string
	}{
		{name: "no configuration", body: ""},
		{name: "all four false", body: pabAllFalse},
		{name: "only BlockPublicAcls", body: pabBlockACLsOnly},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			bucket := newPABBucket(t, srv, "unblocked-policy")
			if cfg.body != "" {
				require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, cfg.body).Code)
			}

			public := policyJSON("Allow", `"*"`, "")
			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?policy", []byte(public), nil)
			require.Equal(t, http.StatusNoContent, w.Code,
				"a public policy must still be accepted: %s", w.Body.String())
			assert.JSONEq(t, public, getBucketPolicyBody(t, srv, bucket))
		})
	}
}

// TestS3_BlockPublicPolicy_MalformedStillMalformed asserts the new check did not
// displace the existing MalformedPolicy rejections: a non-JSON body and an empty
// one still fail the way they did, with 400 rather than the new 403.
func TestS3_BlockPublicPolicy_MalformedStillMalformed(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "malformed-policy")
	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabAllTrue).Code)

	notJSON := s3Request(t, srv, http.MethodPut, "/"+bucket+"?policy", []byte("not json"), nil)
	assert.Equal(t, http.StatusBadRequest, notJSON.Code, "%s", notJSON.Body.String())
	assert.Equal(t, "MalformedPolicy", s3ErrorCode(t, notJSON.Body.Bytes()))

	empty := s3Request(t, srv, http.MethodPut, "/"+bucket+"?policy", nil, nil)
	assert.Equal(t, http.StatusBadRequest, empty.Code, "%s", empty.Body.String())
	assert.Equal(t, "MalformedPolicy", s3ErrorCode(t, empty.Body.Bytes()))
}

// TestS3_BlockPublicAccess_MissingBucket asserts the existence checks still run
// first: a public ACL or policy on a bucket that does not exist is NoSuchBucket,
// not the new AccessDenied.
func TestS3_BlockPublicAccess_MissingBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	acl := s3Request(t, srv, http.MethodPut, "/absent?acl", nil,
		map[string]string{"x-amz-acl": "public-read"})
	assert.Equal(t, http.StatusNotFound, acl.Code, "%s", acl.Body.String())
	assert.Equal(t, "NoSuchBucket", s3ErrorCode(t, acl.Body.Bytes()))

	policy := s3Request(t, srv, http.MethodPut, "/absent?policy",
		[]byte(policyJSON("Allow", `"*"`, "")), nil)
	assert.Equal(t, http.StatusNotFound, policy.Code, "%s", policy.Body.String())
	assert.Equal(t, "NoSuchBucket", s3ErrorCode(t, policy.Body.Bytes()))
}

// TestS3_BlockPublicAccess_ConfigurationIsPerBucket asserts one bucket's
// configuration does not govern another's, which a shared or mis-keyed lookup
// would break.
func TestS3_BlockPublicAccess_ConfigurationIsPerBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	blocked := newPABBucket(t, srv, "locked-down")
	open := newPABBucket(t, srv, "left-open")
	require.Equal(t, http.StatusOK, putPAB(t, srv, blocked, pabAllTrue).Code)

	headers := map[string]string{"x-amz-acl": "public-read"}
	assert.Equal(t, http.StatusForbidden,
		s3Request(t, srv, http.MethodPut, "/"+blocked+"?acl", nil, headers).Code)
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+open+"?acl", nil, headers).Code)
}

// nilIfEmptyBody converts an empty body string to a nil slice, so a header-only
// request sends no body at all — which is what makes the handler read x-amz-acl.
func nilIfEmptyBody(body string) []byte {
	if body == "" {
		return nil
	}
	return []byte(body)
}
