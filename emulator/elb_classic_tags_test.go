package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #844 Tier 1b: the classic tag trio.
//
// Every assertion is over the wire, because what was wrong was a routing decision read out of the
// request body, and each operation is asserted in **both** directions — the classic call answers the
// classic shape, and the ELBv2 call of the same action name answers exactly what it answered before.
// That is the discipline elb_classic_test.go sets for Tier 1a and it matters more here, not less:
// three more action names just became version-sensitive, and every recorded event log and exported
// fixture in the tree replays through the ELBv2 half of each.

// elbClassicDescribeTagsMaxNames is the twenty names the 2012-06-01 `DescribeTags` publishes
// ("Maximum number of 20 items"), spelled here so the test asserts the published bound rather than
// whatever constant the implementation reached for.
const elbClassicDescribeTagsMaxNames = 20

// elbClassicTagParams is a classic tag request's base: the action, and the version that routes it.
func elbClassicTagParams(action string, extra map[string]string) map[string]string {
	params := map[string]string{"Action": action, "Version": elbClassicVersion}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// elbClassicTagRequest sends a classic tag request and returns the response for the caller to read.
func elbClassicTagRequest(
	t *testing.T, baseURL, action string, extra map[string]string,
) *http.Response {
	t.Helper()
	return elbRequest(t, baseURL, elbClassicTagParams(action, extra))
}

// elbClassicAddTags adds tags to one classic load balancer, requiring success.
func elbClassicAddTags(t *testing.T, baseURL, name string, tags map[string]string) {
	t.Helper()
	params := map[string]string{"LoadBalancerNames.member.1": name}
	for i, key := range sortedKeys(tags) {
		params["Tags.member."+strconv.Itoa(i+1)+".Key"] = key
		params["Tags.member."+strconv.Itoa(i+1)+".Value"] = tags[key]
	}
	resp := elbClassicTagRequest(t, baseURL, "AddTags", params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "classic AddTags %s", name)
}

// elbClassicRemoveTags removes keys from one classic load balancer in the `TagKeyOnly` shape the
// operation publishes, requiring success.
func elbClassicRemoveTags(t *testing.T, baseURL, name string, keys ...string) {
	t.Helper()
	params := map[string]string{"LoadBalancerNames.member.1": name}
	for i, key := range keys {
		params["Tags.member."+strconv.Itoa(i+1)+".Key"] = key
	}
	resp := elbClassicTagRequest(t, baseURL, "RemoveTags", params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "classic RemoveTags %s", name)
}

// elbClassicDescribeTags reads the tags on the named classic load balancers, keyed by the
// `LoadBalancerName` the response reports them under.
//
// The key is read out of the body rather than carried over from the request, which is the point: the
// member ELBv2 reports here is `ResourceArn`, so a handler answering v2's shape yields an
// empty-string key and every lookup through this helper misses.
func elbClassicDescribeTags(
	t *testing.T, baseURL string, names ...string,
) map[string]map[string]string {
	t.Helper()
	params := map[string]string{}
	for i, name := range names {
		params["LoadBalancerNames.member."+strconv.Itoa(i+1)] = name
	}
	resp := elbClassicTagRequest(t, baseURL, "DescribeTags", params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "classic DescribeTags")

	var result struct {
		Result struct {
			TagDescriptions []struct {
				LoadBalancerName string `xml:"LoadBalancerName"`
				Tags             []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"Tags>member"`
			} `xml:"TagDescriptions>member"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))

	out := make(map[string]map[string]string, len(result.Result.TagDescriptions))
	for _, td := range result.Result.TagDescriptions {
		tags := make(map[string]string, len(td.Tags))
		for _, tag := range td.Tags {
			tags[tag.Key] = tag.Value
		}
		out[td.LoadBalancerName] = tags
	}
	return out
}

// TestELBClassicTags_RoundTrip is the trio read through each other: what `AddTags` writes
// `DescribeTags` reports, and what `RemoveTags` takes away it stops reporting.
//
// The raw body is asserted as well as the decode, because the response member is the half a decode
// cannot see: `ResourceArn` is what ELBv2 reports and the 2012-06-01 `TagDescription` does not
// publish it, so its absence has to be checked against the bytes.
func TestELBClassicTags_RoundTrip(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "round-trip", nil)

	elbClassicAddTags(t, ts.URL, "round-trip", map[string]string{"env": "prod", "team": "infra"})
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"},
		elbClassicDescribeTags(t, ts.URL, "round-trip")["round-trip"])

	// "If a tag with the same key is already associated with the load balancer, AddTags updates its
	// value" — an update, not the `DuplicateTagKeys` a repeat within one request answers.
	elbClassicAddTags(t, ts.URL, "round-trip", map[string]string{"env": "staging"})
	assert.Equal(t, map[string]string{"env": "staging", "team": "infra"},
		elbClassicDescribeTags(t, ts.URL, "round-trip")["round-trip"])

	elbClassicRemoveTags(t, ts.URL, "round-trip", "env")
	assert.Equal(t, map[string]string{"team": "infra"},
		elbClassicDescribeTags(t, ts.URL, "round-trip")["round-trip"])

	status, body := elbClassicRawBody(t, ts.URL, elbClassicTagParams("DescribeTags",
		map[string]string{"LoadBalancerNames.member.1": "round-trip"}))
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "<LoadBalancerName>round-trip</LoadBalancerName>",
		"the classic TagDescription names the load balancer")
	assert.NotContains(t, body, "ResourceArn",
		"ResourceArn is ELBv2's member and the 2012-06-01 TagDescription does not publish it")
	assert.Contains(t, body, elbClassicNamespace,
		"the response carries the 2012-06-01 document namespace")
}

// TestELBClassicTags_AnUntaggedLoadBalancerIsStillReported is the distinction the operation has a
// published error to make: "carries no tags" and "is not there" are different answers, and a caller
// that cannot tell them apart cannot decide whether to retry or to create.
func TestELBClassicTags_AnUntaggedLoadBalancerIsStillReported(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "untagged", nil)

	got := elbClassicDescribeTags(t, ts.URL, "untagged")
	require.Contains(t, got, "untagged",
		"an untagged load balancer is reported, with an empty Tags list")
	assert.Empty(t, got["untagged"])
}

// TestELBClassicTags_RemoveTagsTakesTagKeyOnlyNotTagKeys is the one shape difference whose wrong
// spelling could have been accepted silently.
//
// Classic `RemoveTags` takes `Tags.member.N` of `TagKeyOnly`; ELBv2's takes `TagKeys.member.N`.
// Unrouted, a classic request reached ELBv2's handler and was refused `ResourceArns is required` —
// loud, and wrong in a way a caller can see. Routed but decoded with v2's member name it would have
// answered 200 and removed nothing, which is the silent wrong answer, so both spellings are asserted
// against each other here rather than only the published one.
func TestELBClassicTags_RemoveTagsTakesTagKeyOnlyNotTagKeys(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "keys-only", nil)
	elbClassicAddTags(t, ts.URL, "keys-only", map[string]string{"env": "prod"})

	// ELBv2's spelling names no key the classic decode reads, so the list is empty and the request is
	// refused rather than quietly succeeding — `Tags` is `Required: Yes`.
	resp := elbClassicTagRequest(t, ts.URL, "RemoveTags", map[string]string{
		"LoadBalancerNames.member.1": "keys-only",
		"TagKeys.member.1":           "env",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
	assert.Equal(t, map[string]string{"env": "prod"},
		elbClassicDescribeTags(t, ts.URL, "keys-only")["keys-only"],
		"the tag survives a request made in the other generation's spelling")

	// The published spelling removes it.
	elbClassicRemoveTags(t, ts.URL, "keys-only", "env")
	assert.Empty(t, elbClassicDescribeTags(t, ts.URL, "keys-only")["keys-only"])
}

// TestELBClassicTags_TheELBv2FormOfTheSameActionsIsUntouched is the other direction of the
// discriminator, and it is the half a regression would be worst in: ELBv2's tag operations are
// reached by every recorded event log and exported fixture in the tree, and by every consumer that
// never sends a `Version` at all.
func TestELBClassicTags_TheELBv2FormOfTheSameActionsIsUntouched(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts.URL, "v2-tags", nil)

	// No `Version` member, which is how every ELBv2 assertion in the tree sends these.
	resp := elbRequest(t, ts.URL, map[string]string{
		"Action": "AddTags", "ResourceArns.member.1": arn,
		"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, map[string]string{"env": "prod"}, elbDescribeTags(t, ts.URL, arn)[arn],
		"the v2 DescribeTags still reports by ResourceArn")

	resp = elbRequest(t, ts.URL, map[string]string{
		"Action": "RemoveTags", "ResourceArns.member.1": arn, "TagKeys.member.1": "env",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Empty(t, elbDescribeTags(t, ts.URL, arn)[arn],
		"the v2 RemoveTags still reads TagKeys.member.N")

	// And the classic shape arriving at the v2 door is still refused for the member it lacks, which is
	// the refusal that proves the two decodes have not been merged into one that accepts either.
	resp = elbRequest(t, ts.URL, map[string]string{
		"Action": "DescribeTags", "LoadBalancerNames.member.1": "v2-tags",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
}

// TestELBClassicTags_Refusals is every refusal the three operations answer, each with the code, the
// status and the message its own page publishes.
//
// The `LoadBalancerNotFound` rows are the ones worth the repetition: **400** is what all three pages
// publish for a load balancer that is not there, not the 404 a reader expects, and a consumer's error
// branch dispatches on the pair.
//
// The message is asserted as well as the pair, and that is not thoroughness for its own sake — it is
// what makes seven of these rows mean anything. Unrouted, these same requests reached the ELBv2
// handler, which refused most of them `ValidationError`/400 for a different reason ("ResourceArns is
// required"). Code and status alone therefore pass with the routing removed; the member the message
// names is the only thing that distinguishes the classic refusal from v2's.
func TestELBClassicTags_Refusals(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "refusals", nil)

	overTwenty := map[string]string{}
	for i := 1; i <= elbClassicDescribeTagsMaxNames+1; i++ {
		overTwenty["LoadBalancerNames.member."+strconv.Itoa(i)] = "refusals"
	}

	tests := []struct {
		name    string
		action  string
		params  map[string]string
		code    string
		status  int
		message string
	}{
		{
			name:   "AddTags names no load balancer",
			action: "AddTags",
			params: map[string]string{"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"},
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames is required",
		},
		{
			name:   "AddTags names two load balancers",
			action: "AddTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "refusals", "LoadBalancerNames.member.2": "refusals",
				"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
			},
			code: "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames names 2 load balancers; this operation accepts at most 1",
		},
		{
			name:   "AddTags carries no Tags",
			action: "AddTags",
			params: map[string]string{"LoadBalancerNames.member.1": "refusals"},
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "Tags is required",
		},
		{
			name:   "AddTags repeats a tag key",
			action: "AddTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "refusals",
				"Tags.member.1.Key":          "env", "Tags.member.1.Value": "prod",
				"Tags.member.2.Key": "env", "Tags.member.2.Value": "staging",
			},
			code: "DuplicateTagKeys", status: http.StatusBadRequest,
			message: "A tag key was specified more than once.",
		},
		{
			name:   "AddTags carries a tag key outside the Tag constraints",
			action: "AddTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "refusals",
				"Tags.member.1.Key":          "env!", "Tags.member.1.Value": "prod",
			},
			code: "ValidationError", status: http.StatusBadRequest,
			message: "Tag key 'env!' contains characters that are not permitted",
		},
		{
			name:   "AddTags names a load balancer that is not there",
			action: "AddTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "no-such-lb",
				"Tags.member.1.Key":          "env", "Tags.member.1.Value": "prod",
			},
			code: "LoadBalancerNotFound", status: http.StatusBadRequest,
			message: "There is no ACTIVE Load Balancer named 'no-such-lb'",
		},
		{
			name:   "RemoveTags names no load balancer",
			action: "RemoveTags",
			params: map[string]string{"Tags.member.1.Key": "env"},
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames is required",
		},
		{
			name:   "RemoveTags names two load balancers",
			action: "RemoveTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "refusals", "LoadBalancerNames.member.2": "refusals",
				"Tags.member.1.Key": "env",
			},
			code: "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames names 2 load balancers; this operation accepts at most 1",
		},
		{
			name:   "RemoveTags carries no Tags",
			action: "RemoveTags",
			params: map[string]string{"LoadBalancerNames.member.1": "refusals"},
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "Tags is required",
		},
		{
			name:   "RemoveTags names a key outside the TagKeyOnly constraints",
			action: "RemoveTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "refusals", "Tags.member.1.Key": "env!",
			},
			code: "ValidationError", status: http.StatusBadRequest,
			message: "Tag key 'env!' contains characters that are not permitted",
		},
		{
			name:   "RemoveTags names a load balancer that is not there",
			action: "RemoveTags",
			params: map[string]string{
				"LoadBalancerNames.member.1": "no-such-lb", "Tags.member.1.Key": "env",
			},
			code: "LoadBalancerNotFound", status: http.StatusBadRequest,
			message: "There is no ACTIVE Load Balancer named 'no-such-lb'",
		},
		{
			name:   "DescribeTags names no load balancer",
			action: "DescribeTags",
			params: map[string]string{},
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames is required",
		},
		{
			name:   "DescribeTags names more than twenty load balancers",
			action: "DescribeTags",
			params: overTwenty,
			code:   "ValidationError", status: http.StatusBadRequest,
			message: "LoadBalancerNames names 21 load balancers; this operation accepts at most 20",
		},
		{
			name:   "DescribeTags names a load balancer that is not there",
			action: "DescribeTags",
			params: map[string]string{"LoadBalancerNames.member.1": "no-such-lb"},
			code:   "LoadBalancerNotFound", status: http.StatusBadRequest,
			message: "There is no ACTIVE Load Balancer named 'no-such-lb'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := elbClassicTagRequest(t, ts.URL, tc.action, tc.params)
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, tc.status, resp.StatusCode)
			code, message := elbErrorBody(t, resp)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.message, message)
		})
	}

	// Every refusal above left the stored set alone, which is why the duplicate and limit checks both
	// run before any write.
	assert.Empty(t, elbClassicDescribeTags(t, ts.URL, "refusals")["refusals"])
}

// TestELBClassicTags_ARecordThatWillNotDecodeReadsAsAbsent is the reading
// [TestELBClassic_ARecordTheTaggingAPICannotDecodeReadsAsAbsent] pins for the tagging resolvers,
// arriving through the classic tag doors.
//
// A record whose bytes are not a classic load balancer — written by hand, or by an older substrate —
// is answered `LoadBalancerNotFound`/400 rather than a 5xx, because nothing is wrong with the store.
// A store that genuinely fails is the opposite case and answers a 5xx;
// [TestELBClassicTags_AStoreFailureIsAnsweredAsOne] holds that half, and the two must not be
// conflated, since a consumer retries one and gives up on the other.
func TestELBClassicTags_ARecordThatWillNotDecodeReadsAsAbsent(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ts := newELBTestServerWithState(t, state)
	elbClassicCreate(t, ts.URL, "intact", nil)

	// The scope is read back off the record the create wrote rather than assumed, so the key this
	// seeds is the one the resolver reaches for whatever account the server attributes to.
	keys, err := state.List(t.Context(), elbClassicStateNamespace, elbClassicRecordPrefix)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	scope := strings.TrimSuffix(keys[0], "/intact")
	require.NoError(t, state.Put(t.Context(), elbClassicStateNamespace,
		scope+"/undecodable", []byte("{not-json")))

	for _, action := range []string{"AddTags", "RemoveTags", "DescribeTags"} {
		t.Run(action, func(t *testing.T) {
			resp := elbClassicTagRequest(t, ts.URL, action, map[string]string{
				"LoadBalancerNames.member.1": "undecodable",
				"Tags.member.1.Key":          "env", "Tags.member.1.Value": "prod",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "LoadBalancerNotFound", elbErrorCode(t, resp))
		})
	}
}

// TestELBClassicTags_TheCapIsTenAndTheWordingIsTheClassicPages is #1148's rule reached through the
// door #844 Tier 1b adds.
//
// Classic publishes its cap on the operation page itself — "Each load balancer can have a maximum of
// 10 tags" — where ELBv2's 50 is only in the user guide's quota list. The two `TooManyTags` messages
// differ and each generation answers its own, so a consumer reading the message can see which API
// refused it.
func TestELBClassicTags_TheCapIsTenAndTheWordingIsTheClassicPages(t *testing.T) {
	ts := newELBTestServer(t)
	elbClassicCreate(t, ts.URL, "capped", nil)

	ten := map[string]string{}
	for i := 1; i <= 10; i++ {
		ten["k"+strconv.Itoa(i)] = "v"
	}
	elbClassicAddTags(t, ts.URL, "capped", ten)
	require.Len(t, elbClassicDescribeTags(t, ts.URL, "capped")["capped"], 10)

	resp := elbClassicTagRequest(t, ts.URL, "AddTags", map[string]string{
		"LoadBalancerNames.member.1": "capped",
		"Tags.member.1.Key":          "k11", "Tags.member.1.Value": "v",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	code, message := elbErrorBody(t, resp)
	assert.Equal(t, "TooManyTags", code)
	assert.Equal(t,
		"The quota for the number of tags that can be assigned to a load balancer has been reached.",
		message, "the 2012-06-01 AddTags page's own TooManyTags wording")
	assert.Len(t, elbClassicDescribeTags(t, ts.URL, "capped")["capped"], 10,
		"the refused eleventh tag was not written")

	// Re-tagging a key already present on a load balancer that is at the cap succeeds, which is the
	// published update rule — the check counts the post-merge key set, not the request's length.
	elbClassicAddTags(t, ts.URL, "capped", map[string]string{"k1": "updated"})
	assert.Equal(t, "updated", elbClassicDescribeTags(t, ts.URL, "capped")["capped"]["k1"])

	// And the v2 door still answers the v2 wording, which is the assertion that keeps the two
	// messages from drifting into one as the shared rule is reused.
	v2 := elbCreateLB(t, ts.URL, "v2-capped", nil)
	v2Params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": v2}
	for i := 1; i <= 51; i++ {
		v2Params["Tags.member."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
		v2Params["Tags.member."+strconv.Itoa(i)+".Value"] = "v"
	}
	v2Resp := elbRequest(t, ts.URL, v2Params)
	defer v2Resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusBadRequest, v2Resp.StatusCode)
	v2Code, v2Message := elbErrorBody(t, v2Resp)
	assert.Equal(t, "TooManyTags", v2Code)
	assert.NotEqual(t, message, v2Message,
		"each generation answers TooManyTags in its own published wording")
}

// TestELBClassicTags_BothGenerationsHoldTheirOwnTags is the key-space assertion from Tier 1a, now
// through the tag doors — which are the only doors that can write to the wrong record.
func TestELBClassicTags_BothGenerationsHoldTheirOwnTags(t *testing.T) {
	ts := newELBTestServer(t)
	arn := elbCreateLB(t, ts.URL, "shared-name", nil)
	elbClassicCreate(t, ts.URL, "shared-name", nil)

	elbClassicAddTags(t, ts.URL, "shared-name", map[string]string{"gen": "classic"})
	resp := elbRequest(t, ts.URL, map[string]string{
		"Action": "AddTags", "ResourceArns.member.1": arn,
		"Tags.member.1.Key": "gen", "Tags.member.1.Value": "v2",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, map[string]string{"gen": "classic"},
		elbClassicDescribeTags(t, ts.URL, "shared-name")["shared-name"])
	assert.Equal(t, map[string]string{"gen": "v2"}, elbDescribeTags(t, ts.URL, arn)[arn])

	// And a removal through one generation's door does not reach the other's record.
	elbClassicRemoveTags(t, ts.URL, "shared-name", "gen")
	assert.Empty(t, elbClassicDescribeTags(t, ts.URL, "shared-name")["shared-name"])
	assert.Equal(t, map[string]string{"gen": "v2"}, elbDescribeTags(t, ts.URL, arn)[arn])
}

// TestELBClassicTags_AreReadableThroughTheTaggingAPIAndBack is #844's last acceptance row and #765's
// standing rule: a tag written through one service is readable through the other.
//
// It is the row that could not be asserted before, because there was no classic tag door to write
// through. Both directions, because the two writers are different code — classic `AddTags` goes
// through [elbTaggedResource.encode] and `TagResources` through the tagging plugin's own merge — and
// either one writing somewhere the other does not read would satisfy only half the rule.
func TestELBClassicTags_AreReadableThroughTheTaggingAPIAndBack(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	elbClassicCreate(t, ts.URL, "cross-read", nil)
	arn := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount + ":loadbalancer/cross-read"

	// Classic AddTags → the tagging API's GetResources.
	elbClassicAddTags(t, ts.URL, "cross-read", map[string]string{"env": "prod"})
	assert.Equal(t, map[string]string{"env": "prod"}, getResourcesTags(t, ts, arn),
		"a tag written through classic AddTags is readable through the tagging API")

	// The tagging API's TagResources → classic DescribeTags.
	taggingTagResources(t, ts, arn, map[string]string{"team": "infra"})
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"},
		elbClassicDescribeTags(t, ts.URL, "cross-read")["cross-read"],
		"a tag written through the tagging API is readable through classic DescribeTags")

	// And a removal through either door is visible through the other.
	taggingUntagResources(t, ts, arn, []string{"env"})
	assert.Equal(t, map[string]string{"team": "infra"},
		elbClassicDescribeTags(t, ts.URL, "cross-read")["cross-read"])
	elbClassicRemoveTags(t, ts.URL, "cross-read", "team")
	assert.Empty(t, getResourcesTags(t, ts, arn))
}

// errELBClassicTagFault is the error the fault stub reports, so each assertion below is about the
// status the handler answers rather than about the text it carries.
var errELBClassicTagFault = errors.New("state store unavailable")

// elbClassicTagFaultState fails a classic record's reads or writes on command.
//
// Armed through [atomic.Bool] rather than a plain field, which is the one difference from
// [elbClassicFaultState]: a tag operation needs a load balancer that already exists, so the fault has
// to be armed *after* the create rather than before the server starts, and a plain field written
// between two requests is a race the detector flags — which is exactly why the older stub spells its
// one post-start fault as a load balancer *name* instead.
type elbClassicTagFaultState struct {
	emulator.StateManager
	getErr atomic.Bool
	putErr atomic.Bool
}

// isClassicRecord reports whether a key names a classic load balancer's own record, as against the
// name index or any of the four ELBv2 kinds.
func (m *elbClassicTagFaultState) isClassicRecord(namespace, key string) bool {
	return namespace == elbClassicStateNamespace && strings.HasPrefix(key, elbClassicRecordPrefix)
}

func (m *elbClassicTagFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.getErr.Load() && m.isClassicRecord(namespace, key) {
		return nil, errELBClassicTagFault
	}
	return m.StateManager.Get(ctx, namespace, key)
}

func (m *elbClassicTagFaultState) Put(
	ctx context.Context, namespace, key string, value []byte,
) error {
	if m.putErr.Load() && m.isClassicRecord(namespace, key) {
		return errELBClassicTagFault
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

// TestELBClassicTags_AStoreFailureIsAnsweredAsOne asserts that a tag call whose record could not be
// read or written answers a 5xx rather than a plausible 200.
//
// A 200 from an `AddTags` whose `Put` failed is the silent wrong answer this whole effort removes,
// arriving by another route: the caller's next `DescribeTags` reports the old set and nothing said
// so. The read side is asserted too, and for a second reason — an unreadable record must not be
// answered `LoadBalancerNotFound`. A broken backend is not an absent load balancer, and a consumer
// reads that difference to decide between retrying and creating.
func TestELBClassicTags_AStoreFailureIsAnsweredAsOne(t *testing.T) {
	addParams := map[string]string{
		"LoadBalancerNames.member.1": "faulty",
		"Tags.member.1.Key":          "env", "Tags.member.1.Value": "prod",
	}
	removeParams := map[string]string{
		"LoadBalancerNames.member.1": "faulty", "Tags.member.1.Key": "env",
	}
	describeParams := map[string]string{"LoadBalancerNames.member.1": "faulty"}

	cases := []struct {
		name   string
		action string
		params map[string]string
		arm    func(*elbClassicTagFaultState)
	}{
		{
			name: "AddTags cannot write the record", action: "AddTags", params: addParams,
			arm: func(s *elbClassicTagFaultState) { s.putErr.Store(true) },
		},
		{
			name: "RemoveTags cannot write the record", action: "RemoveTags", params: removeParams,
			arm: func(s *elbClassicTagFaultState) { s.putErr.Store(true) },
		},
		{
			name: "AddTags cannot read the record", action: "AddTags", params: addParams,
			arm: func(s *elbClassicTagFaultState) { s.getErr.Store(true) },
		},
		{
			name: "RemoveTags cannot read the record", action: "RemoveTags", params: removeParams,
			arm: func(s *elbClassicTagFaultState) { s.getErr.Store(true) },
		},
		{
			name: "DescribeTags cannot read the record", action: "DescribeTags", params: describeParams,
			arm: func(s *elbClassicTagFaultState) { s.getErr.Store(true) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := &elbClassicTagFaultState{StateManager: emulator.NewMemoryStateManager()}
			ts := newELBTestServerWithState(t, state)
			elbClassicCreate(t, ts.URL, "faulty", nil)

			tc.arm(state)
			resp := elbClassicTagRequest(t, ts.URL, tc.action, tc.params)
			defer resp.Body.Close() //nolint:errcheck
			assert.GreaterOrEqual(t, resp.StatusCode, http.StatusInternalServerError,
				"a store failure is a 5xx, not a success and not LoadBalancerNotFound")
		})
	}
}
