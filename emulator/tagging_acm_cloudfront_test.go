package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API had no arm for ACM or CloudFront, so a certificate ARN and a
// distribution ARN both fell to resolveARN's default and answered an InternalServiceException
// FailedResourcesMap entry — while both services tag the resource perfectly well through their own
// calls. That is two of the eleven rows of #835.
//
// Two halves are needed per row and neither substitutes for the other: a resolver arm, so
// TagResources and UntagResources reach the record, and a scanner, so GetResources reports it. A
// resolver arm alone leaves the resource writable and invisible — the caller can tag it and then
// cannot find it.
//
// Every assertion here goes over the wire, and every tag written through the tagging API is read
// back through the *owning service's own* tag call rather than out of the state store. That is
// #765's standing rule and it is the only thing that can distinguish a tag that landed on the
// right record from one that landed on a phantom key and answered success — which is precisely
// the defect class #826, #845, #910 and #918 walked through.
//
// One guard is deliberately untested from the wire. [acmKeyIsTaggable] and [cfKeyIsTaggable]
// refuse the index keys in each namespace ("cert_arns:", "cfdist_ids:", "cfinval_ids:") and the
// invalidation record, but no ARN can produce those keys — the resolvers build only "cert:" and
// "cfdist:". The guards exist for mergeResourceTags' other two callers, the CloudFormation paths
// that build a namespace and key themselves, so a wire test for them would be asserting a path
// the tagging API cannot take.

// acmTarget and cloudfrontTaggingTarget are the wire details a real SDK would send. The tagging
// API is reached on two hosts because the parser takes the Region off the Host, and the CloudFront
// scanner's Region gate is the one behavior here that differs between two Regions.
var (
	acmTarget          = signedRequestTarget{host: "acm.us-east-1.amazonaws.com", target: "CertificateManager", signingName: "acm"}
	acmWest2Target     = signedRequestTarget{host: "acm.us-west-2.amazonaws.com", target: "CertificateManager", signingName: "acm"}
	taggingWest2Target = signedRequestTarget{host: "tagging.us-west-2.amazonaws.com", target: "ResourceGroupsTaggingAPI_20170126", signingName: "tagging"}
)

// requestACMCertificate requests a certificate and returns the ARN ACM itself minted, rather than
// one built here — so no assertion below can pass by agreeing with a key substrate happens to
// build.
func requestACMCertificate(t *testing.T, ts *emulator.TestServer, domain string) string {
	t.Helper()
	return requestACMCertificateIn(t, ts, acmTarget, domain)
}

// requestACMCertificateIn requests a certificate through a specific Region's endpoint. A
// certificate is regional and its state key carries the Region, so this is what makes a
// per-Region assertion possible.
func requestACMCertificateIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, domain string) string {
	t.Helper()
	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "RequestCertificate",
			map[string]any{"DomainName": domain}), &out)
	require.Empty(t, errCode, "RequestCertificate %s", domain)
	require.Equal(t, http.StatusOK, status, "RequestCertificate %s", domain)
	require.NotEmpty(t, out.CertificateArn, "RequestCertificate reports an ARN")
	return out.CertificateArn
}

// acmAddTagsIn writes tags through ACM's own AddTagsToCertificate against a chosen Region's
// endpoint.
//
// It exists for the Region-attribution assertions, whose control resource lives in us-west-2: an
// untagged certificate is absent from GetResources by rule (#938), so a control that was never
// tagged would let "the distribution is absent from us-west-2" pass against an endpoint that
// scanned nothing at all — the very thing the control is there to rule out.
func acmAddTagsIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, arn string, tags map[string]string) {
	t.Helper()
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "AddTagsToCertificate",
			map[string]any{"CertificateArn": arn, "Tags": list}), nil)
	require.Empty(t, errCode, "AddTagsToCertificate %s", arn)
	require.Equal(t, http.StatusOK, status, "AddTagsToCertificate %s", arn)
}

// acmTags reads a certificate's tags through ACM's own ListTagsForCertificate.
func acmTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, acmTarget, taggingTestAccount, "ListTagsForCertificate",
			map[string]any{"CertificateArn": arn}), &out)
	require.Empty(t, errCode, "ListTagsForCertificate %s", arn)
	require.Equal(t, http.StatusOK, status, "ListTagsForCertificate %s", arn)
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// tagResourcesWith posts TagResources for one ARN with the given tags and requires that no
// FailedResourcesMap entry came back. It is separate from tagResourcesFailures because these
// tests assert the success path, where an entry is the failure.
func tagResourcesWith(t *testing.T, ts *emulator.TestServer, arn string, tags map[string]string) {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, "TagResources",
			map[string]any{"ResourceARNList": []string{arn}, "Tags": tags}), &out)
	require.Empty(t, errCode, "TagResources %s", arn)
	require.Equal(t, http.StatusOK, status, "TagResources %s", arn)
	require.Empty(t, out.FailedResourcesMap, "TagResources %s reports no failure", arn)
}

// untagResourcesWith posts UntagResources for one ARN and requires that it succeeded.
func untagResourcesWith(t *testing.T, ts *emulator.TestServer, arn string, keys ...string) {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, "UntagResources",
			map[string]any{"ResourceARNList": []string{arn}, "TagKeys": keys}), &out)
	require.Empty(t, errCode, "UntagResources %s", arn)
	require.Equal(t, http.StatusOK, status, "UntagResources %s", arn)
	require.Empty(t, out.FailedResourcesMap, "UntagResources %s reports no failure", arn)
}

// getResourcesARNsIn calls GetResources against a specific Region's endpoint and returns the ARNs
// reported.
//
// The Region-agnostic helpers in rds_tags_test.go (getResourcesARNs, getResourcesTags) are reused
// for everything else; this one exists only because the CloudFront scanner's Region attribution is
// the one behavior here that has to be asserted from two Regions.
func getResourcesARNsIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget) []string {
	t.Helper()
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
		} `json:"ResourceTagMappingList"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "GetResources", map[string]any{}), &out)
	require.Empty(t, errCode, "GetResources on %s", tgt.host)
	require.Equal(t, http.StatusOK, status, "GetResources on %s", tgt.host)

	arns := make([]string, 0, len(out.ResourceTagMappingList))
	for _, rm := range out.ResourceTagMappingList {
		arns = append(arns, rm.ResourceARN)
	}
	return arns
}

// ----- ACM -----------------------------------------------------------------

// TestTaggingACM_ACertificateTagIsReadBackThroughACM is the cross-readability assertion #765
// requires of every row: the tagging API and the owning service must agree on where one
// resource's tags live. Both sides build the key through acmCertKey, which is what makes that
// true by construction rather than by two call sites happening to agree.
func TestTaggingACM_ACertificateTagIsReadBackThroughACM(t *testing.T) {
	ts := arnGuardServer(t)
	arn := requestACMCertificate(t, ts, "tagging.example.com")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, acmTags(t, ts, arn),
		"ACM reports the tags the tagging API wrote")
}

// TestTaggingACM_UntagResourcesRemovesOnlyTheNamedKeys asserts the damaging direction. A removal
// that reaches the wrong record, or that removes more than it was asked to, turns an
// aws:ResourceTag Deny into an allow.
func TestTaggingACM_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := arnGuardServer(t)
	arn := requestACMCertificate(t, ts, "untag.example.com")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})
	untagResourcesWith(t, ts, arn, "env")

	assert.Equal(t, map[string]string{"owner": "platform"}, acmTags(t, ts, arn),
		"only the named key is removed")
}

// TestTaggingACM_ACertificateIsReportedByGetResources is the scanner half. A resolver arm alone
// leaves the certificate writable and invisible.
func TestTaggingACM_ACertificateIsReportedByGetResources(t *testing.T) {
	ts := arnGuardServer(t)
	arn := requestACMCertificate(t, ts, "scan.example.com")
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	require.Contains(t, getResourcesARNs(t, ts, ""), arn, "GetResources reports the certificate")
	assert.Equal(t, map[string]string{"env": "prod"}, getResourcesTags(t, ts, arn),
		"GetResources reports the certificate's tags")
}

// TestTaggingACM_TheCertificateIndexIsNotReportedAsAResource guards the one thing a
// Region-qualified prefix buys that a bare account prefix would not: the "cert_arns:" index lives
// in the same namespace and is a JSON array of ARN strings, so a scanner that matched it would
// report a certificate with an empty ARN.
func TestTaggingACM_TheCertificateIndexIsNotReportedAsAResource(t *testing.T) {
	ts := arnGuardServer(t)
	requestACMCertificate(t, ts, "index.example.com")

	for _, arn := range getResourcesARNs(t, ts, "") {
		assert.NotEmpty(t, arn, "no reported resource has an empty ARN")
	}
}

// TestTaggingACM_AWrongTypeARNIsRefusedRatherThanMisKeyed is the anchored-segment rule #910
// established, applied to ACM. AWS scopes the three certificate tag operations to the certificate
// type in prose — "This action applies only to the certificate resource type" — and publishes at
// least one other ACM type with its own ARN shape, the ACME endpoint. Substrate models no other
// ACM resource, so those ARNs must be refused rather than resolved to a certificate.
func TestTaggingACM_AWrongTypeARNIsRefusedRatherThanMisKeyed(t *testing.T) {
	cases := []struct {
		name string
		arn  string
	}{
		{"an ACME endpoint, an ACM type substrate does not model",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":acme-endpoint/prod"},
		{"no resource type at all",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":12345678-1234-1234-1234-123456789012"},
		{"a type prefix with no identifier",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate/"},
		{"something nested under a certificate",
			"arn:aws:acm:us-east-1:" + taggingTestAccount + ":certificate/abc/renewal/def"},
		{"no Region, which a certificate ARN always carries",
			"arn:aws:acm::" + taggingTestAccount + ":certificate/abc"},
		{"no account",
			"arn:aws:acm:us-east-1::certificate/abc"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			for _, op := range []string{"TagResources", "UntagResources"} {
				failures := tagResourcesFailures(t, ts, op, tc.arn)
				require.Contains(t, failures, tc.arn, "%s refuses %s", op, tc.arn)
			}
		})
	}
}

// TestTaggingACM_AForeignAccountARNDoesNotReachTheCallersCertificate is #826's rule for this arm.
// The ARN names another account and a certificate identifier the caller does own, which is the
// shape of the defect — a differently-named resource would not reproduce it.
//
// UntagResources is asserted as well as TagResources, and it is the direction that matters:
// stripping a tag is what can turn a Deny into an allow.
func TestTaggingACM_AForeignAccountARNDoesNotReachTheCallersCertificate(t *testing.T) {
	ts := arnGuardServer(t)
	arn := requestACMCertificate(t, ts, "foreign.example.com")
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	foreign := strings.Replace(arn, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, arn, foreign, "the foreign ARN differs only in the account")

	for _, op := range []string{"TagResources", "UntagResources"} {
		failures := tagResourcesFailures(t, ts, op, foreign)
		require.Contains(t, failures, foreign, "%s does not silently succeed for a foreign ARN", op)
	}

	assert.Equal(t, map[string]string{"env": "prod"}, acmTags(t, ts, arn),
		"the caller's own certificate is untouched")
}

// ----- CloudFront ----------------------------------------------------------

// TestTaggingCloudFront_ADistributionTagIsReadBackThroughCloudFront is the cross-readability
// assertion for the CloudFront row. The tagging API's arm and CloudFront's own three operations
// resolve the ARN through one parser, so neither can address a distribution the other would not.
func TestTaggingCloudFront_ADistributionTagIsReadBackThroughCloudFront(t *testing.T) {
	ts, arn := cloudfrontTagServer(t)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})

	tags := cloudfrontListTagsXML(t, ts, arn)
	assert.Contains(t, tags, "<Key>env</Key><Value>prod</Value>",
		"CloudFront reports the tag the tagging API wrote: %s", tags)
	assert.Contains(t, tags, "<Key>owner</Key><Value>platform</Value>",
		"CloudFront reports both tags: %s", tags)
}

// TestTaggingCloudFront_UntagResourcesRemovesOnlyTheNamedKeys asserts the damaging direction for
// the CloudFront row.
func TestTaggingCloudFront_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts, arn := cloudfrontTagServer(t)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})
	untagResourcesWith(t, ts, arn, "env")

	tags := cloudfrontListTagsXML(t, ts, arn)
	assert.NotContains(t, tags, "<Key>env</Key>", "the named key is gone: %s", tags)
	assert.Contains(t, tags, "<Key>owner</Key><Value>platform</Value>",
		"the unnamed key survives: %s", tags)
}

// TestTaggingCloudFront_ADistributionIsReportedOnlyInTheGlobalRegion is the scanner half and the
// Region attribution together.
//
// CloudFront is global and its ARN carries an empty Region segment, but GetResources is a
// per-Region operation, so the distribution has to be attributed to exactly one Region or every
// Region's call would report it. AWS attributes a global resource to us-east-1. That the gate
// exists is substrate's reading; which Region it names is AWS's.
func TestTaggingCloudFront_ADistributionIsReportedOnlyInTheGlobalRegion(t *testing.T) {
	ts, arn := cloudfrontTagServer(t)
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	require.Contains(t, getResourcesARNsIn(t, ts, taggingTarget), arn,
		"GetResources in us-east-1 reports the distribution")
	assert.Equal(t, map[string]string{"env": "prod"}, getResourcesTags(t, ts, arn),
		"GetResources reports the distribution's tags")

	// A certificate requested in us-west-2 is the control. Without it, "the distribution is absent
	// from us-west-2" would pass just as well against an endpoint that scanned nothing at all, and
	// the Region gate would be asserted by an empty response rather than by the gate.
	west2Cert := requestACMCertificateIn(t, ts, acmWest2Target, "control.example.com")
	acmAddTagsIn(t, ts, acmWest2Target, west2Cert, map[string]string{"env": "control"})
	west2 := getResourcesARNsIn(t, ts, taggingWest2Target)
	require.Contains(t, west2, west2Cert, "GetResources in us-west-2 scans that Region's resources")
	assert.NotContains(t, west2, arn,
		"GetResources in another Region does not report a global resource")
}

// TestTaggingCloudFront_AWrongTypeARNIsRefusedRatherThanMisKeyed applies the anchored-segment rule
// to CloudFront. The reference draws the type boundary itself — "You can tag distributions, but
// you can't tag origin access identities or invalidations" — and substrate stores invalidations in
// the same namespace, so an invalidation ARN is a reachable-looking target that has to be refused.
//
// The streaming-distribution case carries the caller's own real distribution ID, which is the
// point: an unanchored substring match finds it (#918).
func TestTaggingCloudFront_AWrongTypeARNIsRefusedRatherThanMisKeyed(t *testing.T) {
	ts, arn := cloudfrontTagServer(t)
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	distID := arn[strings.LastIndex(arn, "/")+1:]
	require.NotEmpty(t, distID, "the created ARN names a distribution ID")

	cases := []struct {
		name string
		arn  string
	}{
		{"a streaming distribution sharing the caller's real ID",
			"arn:aws:cloudfront::" + taggingTestAccount + ":streaming-distribution/" + distID},
		{"an origin access identity, refused by the reference itself",
			"arn:aws:cloudfront::" + taggingTestAccount + ":origin-access-identity/cloudfront/E15MNIMTCFKK4C"},
		{"an invalidation nested under the caller's real distribution",
			"arn:aws:cloudfront::" + taggingTestAccount + ":distribution/" + distID + "/invalidation/I2J0I21PCUYOIK"},
		{"a function, a type substrate does not model",
			"arn:aws:cloudfront::" + taggingTestAccount + ":function/my-fn"},
		{"a Region-bearing ARN, which CloudFront never mints",
			"arn:aws:cloudfront:us-east-1:" + taggingTestAccount + ":distribution/" + distID},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"TagResources", "UntagResources"} {
				failures := tagResourcesFailures(t, ts, op, tc.arn)
				require.Contains(t, failures, tc.arn, "%s refuses %s", op, tc.arn)
			}
		})
	}

	assert.Contains(t, cloudfrontListTagsXML(t, ts, arn), "<Key>env</Key><Value>prod</Value>",
		"none of the refused ARNs reached the caller's distribution")
}

// TestTaggingCloudFront_AForeignAccountARNDoesNotReachTheCallersDistribution is #826's rule for
// this arm, and the ARN names the caller's own distribution ID under another account — the exact
// shape #918 fixed in CloudFront's own tagging operations, asserted here at the tagging API.
func TestTaggingCloudFront_AForeignAccountARNDoesNotReachTheCallersDistribution(t *testing.T) {
	ts, arn := cloudfrontTagServer(t)
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	foreign := strings.Replace(arn, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, arn, foreign, "the foreign ARN differs only in the account")

	for _, op := range []string{"TagResources", "UntagResources"} {
		failures := tagResourcesFailures(t, ts, op, foreign)
		require.Contains(t, failures, foreign, "%s does not silently succeed for a foreign ARN", op)
	}

	assert.Contains(t, cloudfrontListTagsXML(t, ts, arn), "<Key>env</Key><Value>prod</Value>",
		"the caller's own distribution is untouched")
}
