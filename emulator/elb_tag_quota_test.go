package emulator_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The two ELB generations publish different per-resource tag caps — #1148.
//
// Substrate held one number, ELBv2's 50, and #844's Tier 1a made that reachable: a classic load
// balancer became a record in the elb namespace, `elbKeyIsTaggable` admits its key prefix, and the
// Resource Groups Tagging API's quota check counted it against 50 — where the 2012-06-01 `AddTags`
// page's first sentence publishes "Each load balancer can have a maximum of 10 tags". So
// `TagResources` accepted an 11th tag on a classic load balancer, and a 50th.
//
// The two caps come from different places, which is why both are pinned here rather than one being
// derived from the other. ELBv2's `API_AddTags` (2015-12-01) states no maximum anywhere — not in its
// description, not as an `Array Members` constraint, not in its Errors section beyond naming
// `TooManyTags` — so the 50 is read off the ELB user guide's restrictions list ("Maximum number of
// tags per resource—50"). The classic 10 is API-reference text. A test that asserted only one of the
// two would let the other be swallowed again the next time a shared helper grew a default.
//
// Every case writes through the wire and reads back through a different door than it wrote through,
// which is #765's rule: the classic tag trio is still unrouted (#844 Tier 1b), so `GetResources` is
// the read-back for a tag a classic `CreateLoadBalancer` wrote, and a classic `DescribeLoadBalancers`
// is how the *absence* of a refused create's load balancer is observed. Nothing writes state
// directly.

// elbClassicTagCap and elbV2TagCap are the two published per-resource tag caps, spelled here rather
// than read from the emulator so the test pins them.
const (
	elbClassicTagCap = 10
	elbV2TagCap      = 50
)

// elbQuotaClassicARN is the ARN the tagging API addresses a classic load balancer by. Classic ARNs
// carry the bare name under `loadbalancer/`, where an ELBv2 one carries `app/<name>/<id>`.
func elbQuotaClassicARN(name string) string {
	return "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount + ":loadbalancer/" + name
}

// elbQuotaCreateTagParams renders n distinct `Tags.member.N` pairs for a create.
func elbQuotaCreateTagParams(n int) map[string]string {
	params := make(map[string]string, 2*n)
	for i := 1; i <= n; i++ {
		params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	return params
}

// TestELBTagQuota_EachGenerationCountsAgainstItsOwnPublishedCap is the pair of boundaries, asserted
// against one state store so neither constant can stand in for the other.
//
// The cap is resolved from the record's own state-key prefix rather than from the caller, so both
// rows reach it through the *same* door — the tagging API's `TagResources`, which knows nothing about
// ELB generations — and still get different answers.
func TestELBTagQuota_EachGenerationCountsAgainstItsOwnPublishedCap(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	v2 := elbCreateLB(t, ts.URL, "quota-v2", nil)
	elbClassicCreate(t, ts.URL, "quota-classic", nil)

	for _, tc := range []struct {
		name string
		arn  string
		cap  int
	}{
		{name: "ELBv2", arn: v2, cap: elbV2TagCap},
		{name: "classic", arn: elbQuotaClassicARN("quota-classic"), cap: elbClassicTagCap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atCap := tagQuotaTags("fill", tc.cap)
			require.Empty(t, tagQuotaTag(t, ts, tc.arn, atCap),
				"%s accepts its published cap of %d tags", tc.name, tc.cap)
			assert.Equal(t, atCap, getResourcesTags(t, ts, tc.arn),
				"%s carries every tag the accepted TagResources wrote", tc.name)

			got, ok := tagQuotaTag(t, ts, tc.arn, map[string]string{"one-too-many": "v"})[tc.arn]
			require.Truef(t, ok, "%s reported no failure for tag %d", tc.name, tc.cap+1)
			assert.Equal(t, "TooManyTags", got.ErrorCode, "%s quota refusal code", tc.name)
			assert.Equal(t, http.StatusBadRequest, got.StatusCode, "%s quota refusal status", tc.name)

			// #965's rule: a refused merge writes nothing, so the resource is still at its cap
			// rather than at the cap plus the tag the refusal was about.
			assert.Equal(t, atCap, getResourcesTags(t, ts, tc.arn),
				"a refused TagResources left %s's tags untouched", tc.name)
		})
	}
}

// TestELBTagQuota_TheClassicCreateRefusesAnEleventhTagAndWritesNothing is the create door's half.
//
// Classic `CreateLoadBalancer` publishes `TooManyTags` itself, and the validation runs before the
// write for the reason ELBv2's four creates document: a create carrying a tag it cannot legally apply
// must leave no load balancer behind. Both halves are one assertion here.
func TestELBTagQuota_TheClassicCreateRefusesAnEleventhTagAndWritesNothing(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	elbClassicCreate(t, ts.URL, "at-cap", elbQuotaCreateTagParams(elbClassicTagCap))
	assert.Len(t, getResourcesTags(t, ts, elbQuotaClassicARN("at-cap")), elbClassicTagCap,
		"a create carrying exactly the cap keeps every tag")

	resp := elbRequest(t, ts.URL,
		elbClassicCreateParams("over-cap", elbQuotaCreateTagParams(elbClassicTagCap+1)))
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"a classic create carrying %d tags is refused", elbClassicTagCap+1)
	assert.Equal(t, "TooManyTags", elbErrorCode(t, resp))

	absent := elbRequest(t, ts.URL, map[string]string{
		"Action": "DescribeLoadBalancers", "Version": elbClassicVersion,
		"LoadBalancerNames.member.1": "over-cap",
	})
	defer absent.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, absent.StatusCode,
		"a refused create left no load balancer behind")
	assert.Equal(t, "LoadBalancerNotFound", elbErrorCode(t, absent))
}

// TestELBTagQuota_AReservedKeyDoesNotCountAgainstTheClassicCap mirrors
// [TestELB_AddTags_ReservedPrefixDoesNotCountAgainstTheLimit] on the classic side.
//
// The exclusion is published for the service rather than for one generation — the restrictions list
// says "Tags with this prefix do not count against your tags per resource limit" and the classic API
// page publishes no reserved-prefix rule of its own — so it is applied to both caps. The tag count
// here is the discriminating one: ten records of which one is reserved is nine counted, so a tenth
// user tag fits. Were the reserved key counted, the resource would already be at ten and that add
// would be refused.
func TestELBTagQuota_AReservedKeyDoesNotCountAgainstTheClassicCap(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	params := elbQuotaCreateTagParams(elbClassicTagCap - 1)
	params["Tags.member."+strconv.Itoa(elbClassicTagCap)+".Key"] = "aws:cloudformation:stack-name"
	params["Tags.member."+strconv.Itoa(elbClassicTagCap)+".Value"] = "my-stack"
	elbClassicCreate(t, ts.URL, "reserved-classic", params)

	arn := elbQuotaClassicARN("reserved-classic")
	assert.Len(t, getResourcesTags(t, ts, arn), elbClassicTagCap,
		"the reserved key is stored like any other")

	require.Empty(t, tagQuotaTag(t, ts, arn, map[string]string{"tenth": "v"}),
		"nine user tags plus a reserved one leaves room for a tenth user tag")
	assert.Len(t, getResourcesTags(t, ts, arn), elbClassicTagCap+1,
		"the resource now carries ten user tags and one reserved key")

	got, ok := tagQuotaTag(t, ts, arn, map[string]string{"eleventh": "v"})[arn]
	require.True(t, ok, "the eleventh user tag is still refused")
	assert.Equal(t, "TooManyTags", got.ErrorCode)
	assert.Equal(t, http.StatusBadRequest, got.StatusCode)
}

// TestELBTagQuota_TheTwoGenerationsAnswerTheirOwnRefusalWording pins that the message travels with
// the cap.
//
// The two pages word `TooManyTags` differently, and a cap paired with the wrong message is how one
// generation's refusal came to be answered behind the other's number in the first place. The classic
// text names a load balancer where ELBv2's names "this resource", which is the difference a consumer
// reading the message sees.
func TestELBTagQuota_TheTwoGenerationsAnswerTheirOwnRefusalWording(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	classic := elbRequest(t, ts.URL,
		elbClassicCreateParams("wording-classic", elbQuotaCreateTagParams(elbClassicTagCap+1)))
	defer classic.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, classic.StatusCode)
	assert.Contains(t, elbErrorMessage(t, classic),
		"The quota for the number of tags that can be assigned to a load balancer has been reached.")

	lb := elbCreateLB(t, ts.URL, "wording-v2", nil)
	params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": lb}
	for k, v := range elbQuotaCreateTagParams(elbV2TagCap + 1) {
		params[k] = v
	}
	v2 := elbRequest(t, ts.URL, params)
	defer v2.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, v2.StatusCode)
	assert.Contains(t, elbErrorMessage(t, v2),
		"You've reached the limit on the number of tags for this resource.")
}
