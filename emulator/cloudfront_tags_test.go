package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CloudFront's tagging ARN resolver (#918).
//
// Two defects, both in the two lines that turned the Resource parameter into a state key: the
// account came from the caller's request context rather than from the ARN, and the resource
// type was matched with strings.LastIndex over the whole ARN rather than against the ARN's own
// segment. So arn:aws:cloudfront::999988887777:distribution/E1EXAMPLE addressed the caller's
// own E1EXAMPLE, and arn:aws:cloudfront::123456789012:streaming-distribution/E1EXAMPLE
// addressed the web distribution E1EXAMPLE.
//
// This is the rule #826 set for SQS and DynamoDB, #845 for the Resource Groups Tagging API's
// resolver and #910 for Step Functions, in the last plugin that lacked it. Every assertion here
// reads the tags back through CloudFront's own ListTagsForResource for the reason
// cloudfront_untag_test.go's preamble gives: a helper reading state directly cannot tell a write
// that reached the wrong record from one that reached none.

// cloudfrontDistIDFromARN returns the distribution ID an ARN reported by CreateDistribution
// carries, so a test can rebuild the same ID under a different account or resource type without
// having assembled the original ARN itself.
func cloudfrontDistIDFromARN(t *testing.T, arn string) string {
	t.Helper()
	slash := strings.LastIndex(arn, "/")
	require.Greater(t, slash, 0, "ARN %q carries a resource segment", arn)
	id := arn[slash+1:]
	require.NotEmpty(t, id, "ARN %q names a distribution", arn)
	return id
}

// TestCloudFrontTagging_AForeignAccountARNDoesNotReachTheCallersDistribution is #918's gate for
// the account half.
//
// The ARN names another account and the same distribution ID the caller owns, which is the one
// case the parent commit could not tell apart from the caller's own ARN: it read the ID out of
// the ARN and then keyed the load by ctx.AccountID, ignoring the account segment entirely. The
// assertion that matters is the second one — that the caller's tags are untouched — because the
// refusal alone would also be satisfied by a resolver that happened to answer 404 for an
// unrelated reason.
//
// UntagResource is asserted separately from TagResource rather than assumed to follow, because
// it is the damaging direction: stripping a tag can turn an aws:ResourceTag Deny into an allow,
// and the parent commit answered the documented 204 while doing it.
func TestCloudFrontTagging_AForeignAccountARNDoesNotReachTheCallersDistribution(t *testing.T) {
	t.Parallel()

	const foreignAccount = "999988887777"

	ops := []struct {
		name, query, body string
	}{
		{"TagResource", "Operation=Tag", cloudfrontTagBody("owner", "intruder")},
		{"UntagResource", "Operation=Untag", cloudfrontUntagBody("env")},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()

			ts, arn := cloudfrontTagServer(t)
			cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))

			foreign := "arn:aws:cloudfront::" + foreignAccount + ":distribution/" +
				cloudfrontDistIDFromARN(t, arn)
			status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
				op.query+"&Resource="+foreign, op.body)
			assert.Equal(t, http.StatusNotFound, status,
				"%s against another account's ARN — body: %s", op.name, body)
			assert.Equal(t, "NoSuchDistribution", cloudfrontErrorCode(t, body),
				"%s: the account comes from the ARN, and that account holds no such distribution", op.name)

			tags := cloudfrontListTagsXML(t, ts, arn)
			assert.Contains(t, tags, "<Tag><Key>env</Key><Value>prod</Value></Tag>",
				"%s against %s reached the caller's own distribution: %s", op.name, foreign, tags)
			assert.NotContains(t, tags, "intruder",
				"%s against %s wrote to the caller's own distribution: %s", op.name, foreign, tags)
		})
	}
}

// TestCloudFrontTagging_AStreamingDistributionARNIsNotReadAsADistribution is #918's gate for the
// resource-type half.
//
// A streaming distribution is a distinct CloudFront resource type, and the ID here is the
// caller's real distribution ID — so an unanchored search for "distribution/" finds it and
// resolves to a record the ARN does not name. That the ID exists is the point: the parent
// commit's LastIndex answered 204 for this request.
func TestCloudFrontTagging_AStreamingDistributionARNIsNotReadAsADistribution(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))

	streaming := "arn:aws:cloudfront::" + taggingTestAccount + ":streaming-distribution/" +
		cloudfrontDistIDFromARN(t, arn)

	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag&Resource="+streaming, cloudfrontUntagBody("env"))
	assert.Equal(t, http.StatusNotFound, status, "UntagResource on a streaming distribution: %s", body)
	assert.Equal(t, "NoSuchResource", cloudfrontErrorCode(t, body),
		"a well-formed ARN of a type substrate does not model answers the published NoSuchResource")

	assert.Contains(t, cloudfrontListTagsXML(t, ts, arn),
		"<Tag><Key>env</Key><Value>prod</Value></Tag>",
		"a streaming-distribution ARN reached the web distribution of the same ID")
}

// TestCloudFrontTagging_AnUnmodelledResourceTypeAnswersNoSuchResource pins the code for the
// CloudFront resource types a tagging ARN can legally name and substrate does not resolve.
//
// Two of these are refused on the reference's own authority rather than on substrate's reading:
// the developer guide's tagging page states "You can tag distributions, but you can't tag origin
// access identities or invalidations", and both are reachable-looking targets here — substrate
// stores invalidations in this same namespace and CloudFormation mints origin access identities
// (#859). The code is the published NoSuchResource/404, not the NoSuchDistribution that only an
// ARN naming a distribution that does not exist answers.
func TestCloudFrontTagging_AnUnmodelledResourceTypeAnswersNoSuchResource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, resource, why string
	}{{
		name:     "origin access identity",
		resource: "origin-access-identity/cloudfront/E15MNIMTCFKK4C",
		why:      `the guide states "you can't tag origin access identities"`,
	}, {
		name:     "function",
		resource: "function/my-fn",
		why:      "substrate models no CloudFront function",
	}, {
		name:     "cache policy",
		resource: "cache-policy/658327ea-f89d-4fab-a63d-7e88639e58f6",
		why:      "substrate models no cache policy",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts, _ := cloudfrontTagServer(t)
			arn := "arn:aws:cloudfront::" + taggingTestAccount + ":" + tc.resource

			status, body := cloudfrontRequest(t, ts, http.MethodGet, cloudfrontTaggingPath,
				"Resource="+arn, "")
			assert.Equal(t, http.StatusNotFound, status, "%s — body: %s", tc.why, body)
			assert.Equal(t, "NoSuchResource", cloudfrontErrorCode(t, body), tc.why)
		})
	}
}

// TestCloudFrontTagging_AMalformedARNAnswersInvalidArgument pins the other refusal, which is a
// different failure and answers a different published code: the ARN itself is wrong, rather than
// naming something that is not here.
//
// The Region case is the one worth stating outright. CloudFront is global and
// ListTagsForResource publishes the Resource pattern as arn:aws(-cn)?:cloudfront::[0-9]+:.*, in
// which the empty segment is the Region — so a Region-bearing ARN is one a caller built for a
// regional service, and resolving it anyway would answer for a distribution it does not name.
//
// An invalidation is addressed under its distribution, so its ARN lands here rather than in the
// unmodelled-type case above: the type segment reads "distribution" and the remainder carries a
// further "/". Either refusal satisfies the guide's "you can't tag ... invalidations"; this one
// says which of the two substrate answers.
func TestCloudFrontTagging_AMalformedARNAnswersInvalidArgument(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, arn, why string
	}{{
		name: "not an ARN at all",
		arn:  "E15MNIMTCFKK4C",
		why:  "a bare distribution ID is not the ARN the parameter's pattern requires",
	}, {
		name: "another service's ARN",
		arn:  "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":orders",
		why:  "the service segment is not cloudfront",
	}, {
		name: "a Region-bearing CloudFront ARN",
		arn:  "arn:aws:cloudfront:us-east-1:" + taggingTestAccount + ":distribution/E15MNIMTCFKK4C",
		why:  "CloudFront ARNs carry no Region, per the published Resource pattern",
	}, {
		name: "no account",
		arn:  "arn:aws:cloudfront:::distribution/E15MNIMTCFKK4C",
		why:  "the account is what the resolution keys on, so an absent one is not resolvable",
	}, {
		name: "a resource type with no identifier",
		arn:  "arn:aws:cloudfront::" + taggingTestAccount + ":distribution",
		why:  "the ARN names a type and no resource",
	}, {
		name: "an empty identifier",
		arn:  "arn:aws:cloudfront::" + taggingTestAccount + ":distribution/",
		why:  "an empty ID would build the key of the account's index rather than of a record",
	}, {
		name: "an invalidation under a distribution",
		arn:  "arn:aws:cloudfront::" + taggingTestAccount + ":distribution/E15MNIMTCFKK4C/invalidation/I2J0I21PCUYOIK",
		why:  `the guide states "you can't tag ... invalidations"`,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts, _ := cloudfrontTagServer(t)
			status, body := cloudfrontRequest(t, ts, http.MethodGet, cloudfrontTaggingPath,
				"Resource="+tc.arn, "")
			assert.Equal(t, http.StatusBadRequest, status, "%s — body: %s", tc.why, body)
			assert.Equal(t, "InvalidArgument", cloudfrontErrorCode(t, body), tc.why)
		})
	}
}

// TestCloudFrontTagging_TheARNCreateDistributionReportsResolves is the positive case the three
// refusals above are only meaningful against.
//
// It uses the ARN CloudFront itself reported rather than one assembled here, so the producer and
// the resolver are asserted to agree on the ARN's shape — which is the property that makes the
// account-from-ARN rule usable at all. A resolver stricter than the producer would refuse every
// ARN a caller could have obtained.
func TestCloudFrontTagging_TheARNCreateDistributionReportsResolves(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	require.Equal(t, "arn:aws:cloudfront::"+taggingTestAccount+":distribution/"+
		cloudfrontDistIDFromARN(t, arn), arn,
		"CreateDistribution reports the shape the resolver parses")

	cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))
	assert.Contains(t, cloudfrontListTagsXML(t, ts, arn),
		"<Tag><Key>env</Key><Value>prod</Value></Tag>",
		"the ARN CloudFront reported resolves to the distribution that reported it")

	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag&Resource="+arn, cloudfrontUntagBody("env"))
	require.Equal(t, http.StatusNoContent, status, "UntagResource: %s", body)
	assert.NotContains(t, cloudfrontListTagsXML(t, ts, arn), "<Key>env</Key>",
		"the tag is gone, so the untag reached the record the tag wrote")
}
