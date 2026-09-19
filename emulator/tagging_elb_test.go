package emulator_test

import (
	"io"
	"net/http"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API's `elasticloadbalancing` arm (#863).
//
// ELBv2 has kept its own tag store since #748 and the tagging API could reach none of it: `resolveARN`
// had no arm for the service, so `TagResources` on a load balancer, target group, listener or rule
// failed at the resolver and `GetResources` reported none of the four. That left #765's standing rule
// — a tag written through one service must be readable through the other — unmet for a service
// substrate models fully.
//
// Every assertion here crosses the two APIs, in both directions, over the wire: ELBv2 speaks the Query
// protocol on `elasticloadbalancing.us-east-1.amazonaws.com` and the tagging API speaks JSON-RPC on
// `tagging.us-east-1.amazonaws.com`, and one write is read back through the *other* one's own call.
// Nothing here writes state directly, for the reason `tagging_arn_guards_test.go` gives: a helper that
// writes the record cannot prove the record is reachable, because it writes the key the assertion
// would have to trust.
//
// The arm is the first one that *finds* its state key rather than building it. A listener's and a
// rule's key carries a minted suffix (`generateELBSuffix`) that no ARN component yields, so the
// resolver delegates to [elbResolveTaggedResource] — ELBv2's own resolver, the one its `AddTags` uses
// — instead of rebuilding a key that could drift from it (#935). That is why the listener and rule
// rows below are not redundant with the load-balancer one: they are the shapes a key-building arm
// could not have reached at all.

// elbTaggingServer starts a server carrying every default plugin, so ELBv2 and the tagging API share
// one state store.
//
// [newELBTestServer] cannot be used: it registers the ELB plugin alone, and a cross-readability
// assertion needs both halves. [emulator.WithAccounts] rather than a bare start because
// [signedRequest] needs a registered credential to sign the tagging call with — the same reason
// [arnGuardServer] gives — and signature verification stays off, which is the default (#630).
func elbTaggingServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// elbTaggingResources creates one of each of ELBv2's four taggable kinds and returns their ARNs in
// the order load balancer, target group, listener, rule.
func elbTaggingResources(t *testing.T, ts *emulator.TestServer, prefix string) (lb, tg, listener, rule string) {
	t.Helper()
	lb = elbCreateLB(t, ts.URL, prefix+"-alb", nil)
	tg = elbCreateTG(t, ts.URL, prefix+"-tg", nil)
	listener = elbCreateListener(t, ts.URL, lb, tg, nil)
	rule = elbCreateRule(t, ts.URL, listener, tg, nil)
	return lb, tg, listener, rule
}

// taggingTagResources posts TagResources for one ARN and fails the test if it reported a failure.
func taggingTagResources(t *testing.T, ts *emulator.TestServer, arn string, tags map[string]string) {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, taggingTarget, taggingTestAccount,
		"TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": tags}), &out)
	require.Empty(t, errCode, "TagResources %s", arn)
	require.Equal(t, http.StatusOK, status, "TagResources %s", arn)
	require.Empty(t, out.FailedResourcesMap, "TagResources %s reported a failure", arn)
}

// taggingUntagResources posts UntagResources for one ARN and fails the test if it reported a failure.
func taggingUntagResources(t *testing.T, ts *emulator.TestServer, arn string, keys []string) {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, taggingTarget, taggingTestAccount,
		"UntagResources", map[string]any{"ResourceARNList": []string{arn}, "TagKeys": keys}), &out)
	require.Empty(t, errCode, "UntagResources %s", arn)
	require.Equal(t, http.StatusOK, status, "UntagResources %s", arn)
	require.Empty(t, out.FailedResourcesMap, "UntagResources %s reported a failure", arn)
}

// elbAddTags posts ELBv2's own AddTags for one resource.
func elbAddTags(t *testing.T, ts *emulator.TestServer, arn string, tags map[string]string) {
	t.Helper()
	params := map[string]string{"Action": "AddTags", "ResourceArns.member.1": arn}
	for i, key := range sortedKeys(tags) {
		params["Tags.member."+strconv.Itoa(i+1)+".Key"] = key
		params["Tags.member."+strconv.Itoa(i+1)+".Value"] = tags[key]
	}
	resp := elbRequest(t, ts.URL, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "AddTags %s", arn)
}

// elbRawDescribe returns the response body of an ELBv2 Describe call verbatim.
//
// The bytes rather than a decoded struct, for the reason
// [TestELBTagging_TheMergeLeavesTheRestOfTheRecordIntact] needs: a struct can only assert the members
// it declares, and the defect being guarded against is a member disappearing. A decode would have to
// enumerate every member of every shape to see the same thing the bytes show for free.
func elbRawDescribe(t *testing.T, baseURL string, params map[string]string) string {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, params["Action"])
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

// TestELBTagging_TagResourcesIsReadableThroughDescribeTags is the first half of #765's rule for
// ELBv2: a tag the tagging API writes must be readable through ELBv2's own DescribeTags.
//
// All four kinds, because each is a separate record and two of them — the listener and the rule — are
// keyed by a minted suffix, which is the part a resolver arm cannot derive from the ARN.
func TestELBTagging_TagResourcesIsReadableThroughDescribeTags(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb, tg, listener, rule := elbTaggingResources(t, ts, "rgta-write")

	for _, arn := range []string{lb, tg, listener, rule} {
		taggingTagResources(t, ts, arn, map[string]string{"env": "prod", "team": ""})

		// An empty value survives the merge, as it does through AddTags: Tag.Value has a documented
		// minimum length of 0, so "team" must be present rather than having been dropped.
		assert.Equal(t, map[string]string{"env": "prod", "team": ""},
			elbDescribeTags(t, ts.URL, arn)[arn], arn)
	}
}

// TestELBTagging_AddTagsIsReadableThroughGetResources is the other half: a tag ELBv2 writes must be
// discoverable through GetResources.
//
// It also pins the scanners, which are what make a resource *findable* rather than merely taggable —
// the tagging API's whole purpose is answering "which of my resources carry this tag?", and a service
// that answers "none" for a resource it does hold is wrong in the way that matters.
func TestELBTagging_AddTagsIsReadableThroughGetResources(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb, tg, listener, rule := elbTaggingResources(t, ts, "native-write")

	for _, arn := range []string{lb, tg, listener, rule} {
		elbAddTags(t, ts, arn, map[string]string{"owner": "platform"})
	}

	reported := getResourcesARNs(t, ts, "elasticloadbalancing")
	for _, arn := range []string{lb, tg, listener, rule} {
		assert.Contains(t, reported, arn)
		assert.Equal(t, map[string]string{"owner": "platform"}, getResourcesTags(t, ts, arn), arn)
	}
}

// TestELBTagging_GetResourcesFiltersByEachELBType asserts a ResourceTypeFilters entry selects one of
// the four kinds and only that one.
//
// The four type segments fall out of the ARNs without any ELB-specific handling in the filter, which
// is what #936's anchored-segment rule bought: `loadbalancer`, `targetgroup`, `listener` and
// `listener-rule` are each delimited by the ARN itself. The `listener` case is the one worth pinning,
// because a listener ARN of the pre-#774 *nested* shape carries `loadbalancer` as its type segment,
// so a filter and a kind are not the same question.
func TestELBTagging_GetResourcesFiltersByEachELBType(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb, tg, listener, rule := elbTaggingResources(t, ts, "filter")

	for _, arn := range []string{lb, tg, listener, rule} {
		elbAddTags(t, ts, arn, map[string]string{"env": "prod"})
	}

	for _, tc := range []struct {
		filter string
		want   string
	}{
		{"elasticloadbalancing:loadbalancer", lb},
		{"elasticloadbalancing:targetgroup", tg},
		{"elasticloadbalancing:listener", listener},
		{"elasticloadbalancing:listener-rule", rule},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			assert.Equal(t, []string{tc.want}, getResourcesARNs(t, ts, tc.filter))
		})
	}
}

// TestELBTagging_UntagResourcesRemovesFromTheELBRecord asserts the removal direction, which is the
// more damaging one to get wrong and so is never the one left untested.
//
// The resource stays *reported* with an empty tag set rather than disappearing, which is #938's rule:
// GetResources reports what has been tagged, not what is tagged, so a caller walking it can tell "this
// resource's tags were removed" from "this resource was never tagged". The `ever_tagged` member the
// four ELB records gained is what carries that across the write.
func TestELBTagging_UntagResourcesRemovesFromTheELBRecord(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb, tg, listener, rule := elbTaggingResources(t, ts, "untag")

	for _, arn := range []string{lb, tg, listener, rule} {
		taggingTagResources(t, ts, arn, map[string]string{"env": "prod", "team": "platform"})
		taggingUntagResources(t, ts, arn, []string{"env", "not-present"})

		// A key that is not there is silently ignored, as it is through RemoveTags.
		assert.Equal(t, map[string]string{"team": "platform"},
			elbDescribeTags(t, ts.URL, arn)[arn], arn)

		taggingUntagResources(t, ts, arn, []string{"team"})
		assert.Empty(t, elbDescribeTags(t, ts.URL, arn)[arn], arn)
		assert.Contains(t, getResourcesARNs(t, ts, "elasticloadbalancing"), arn,
			"a resource whose tags were removed stays reported (#938)")
		assert.Empty(t, getResourcesTags(t, ts, arn), arn)
	}
}

// TestELBTagging_AnUntaggedResourceIsNotReported is the other side of #938: a resource that has never
// carried a tag is not reported at all, so `ever_tagged` cannot be read as "every resource".
func TestELBTagging_AnUntaggedResourceIsNotReported(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb, tg, listener, rule := elbTaggingResources(t, ts, "never-tagged")

	reported := getResourcesARNs(t, ts, "elasticloadbalancing")
	for _, arn := range []string{lb, tg, listener, rule} {
		assert.NotContains(t, reported, arn, arn)
	}
}

// TestELBTagging_TheMergeLeavesTheRestOfTheRecordIntact asserts the merge edits the record's tags
// member and nothing else.
//
// This is the failure the raw-JSON merge helpers exist to prevent: an arm that unmarshaled into a
// concrete Go struct and marshaled it back would silently drop every member the struct does not
// carry — the truncation #835 had to convert the `states` and `rds` arms away from. It is asserted on
// the four Describe calls' own bytes rather than out of state, because a truncated record is exactly
// what those calls would stop reporting, and because ELBv2 renders no tags in a Describe body: the
// merge is invisible there when it is correct, so any difference at all is the defect.
func TestELBTagging_TheMergeLeavesTheRestOfTheRecordIntact(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)
	lb := elbCreateLB(t, ts.URL, "intact-alb", map[string]string{
		"Scheme": "internal", "IpAddressType": "ipv4",
	})
	tg := elbCreateTG(t, ts.URL, "intact-tg", nil)
	listener := elbCreateListener(t, ts.URL, lb, tg, nil)
	rule := elbCreateRule(t, ts.URL, listener, tg, nil)

	describes := map[string]map[string]string{
		"DescribeLoadBalancers": {"Action": "DescribeLoadBalancers"},
		"DescribeTargetGroups":  {"Action": "DescribeTargetGroups"},
		"DescribeListeners":     {"Action": "DescribeListeners", "LoadBalancerArn": lb},
		"DescribeRules":         {"Action": "DescribeRules", "ListenerArn": listener},
	}
	before := make(map[string]string, len(describes))
	for name, params := range describes {
		before[name] = elbRawDescribe(t, ts.URL, params)
	}

	for _, arn := range []string{lb, tg, listener, rule} {
		taggingTagResources(t, ts, arn, map[string]string{"env": "prod"})
	}

	for name, params := range describes {
		assert.Equal(t, before[name], elbRawDescribe(t, ts.URL, params), name)
	}
}

// TestELBTagging_AClassicLoadBalancerARNIsCallerScoped pins the reversal #844 makes of #863's fifth
// acceptance row: which door the ARN arrives at decides whether it is accepted.
//
// A classic ARN carries one segment after `loadbalancer/` where an ELBv2 one carries three
// (`app/<name>/<id>`), which is the arity [elbResourceKindFromARN] discriminates on, and that arity
// check stands — it is what keeps a classic ARN off the `lb:` prefix where ELBv2's load balancers
// live, where it would have been answered `LoadBalancerNotFound` as though a *v2* load balancer of
// that ARN could have existed. What #863 could not get right is the *answer*, because until #844
// substrate held no classic record to give.
//
// Now it does, and the two callers diverge because AWS's own pages diverge:
//
//   - ELBv2's `AddTags` enumerates the resources it tags — "load balancers, target groups, listeners
//     and rules" — and Classic is absent from that list, with no published code for a
//     wrong-generation ARN. So the refusal stays, at the code ELBv2's own consolidated Common Errors
//     page publishes.
//   - The Resource Groups Tagging API matches on the type segment embedded in an ARN, which both
//     generations spell `loadbalancer`, and the Service Authorization Reference lists classic
//     `loadbalancer` under the single `AddTags` action. So RGT accepts it, and a tag written through
//     RGT is readable back through RGT — #765's rule, which the old answer could not meet because
//     there was no resource to meet it for.
//
// The generation-blind half goes through [elbAnyGenerationKindFromARN]; ELBv2's three tag doors keep
// [elbResourceKindFromARN].
func TestELBTagging_AClassicLoadBalancerARNIsCallerScoped(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	// One name held by both generations, so a collision would be visible rather than merely possible:
	// the two records are what the arity check keeps apart.
	v2 := elbCreateLB(t, ts.URL, "collide", nil)
	taggingTagResources(t, ts, v2, map[string]string{"env": "prod"})
	elbClassicCreate(t, ts.URL, "collide", nil)

	classic := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount + ":loadbalancer/collide"

	t.Run("ELBv2 AddTags still refuses it", func(t *testing.T) {
		resp := elbRequest(t, ts.URL, map[string]string{
			"Action": "AddTags", "ResourceArns.member.1": classic,
			"Tags.member.1.Key": "env", "Tags.member.1.Value": "classic",
		})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "ValidationError", elbErrorCode(t, resp))
	})

	t.Run("the tagging API accepts it", func(t *testing.T) {
		taggingTagResources(t, ts, classic, map[string]string{"tier": "classic"})
		assert.Equal(t, map[string]string{"tier": "classic"}, getResourcesTags(t, ts, classic))
		assert.Contains(t, getResourcesARNs(t, ts, "elasticloadbalancing:loadbalancer"), classic,
			"the type segment both generations spell is what the filter matches")

		taggingUntagResources(t, ts, classic, []string{"tier"})
		assert.Empty(t, getResourcesTags(t, ts, classic))
	})

	// The v2 load balancer of the same name carries its own tags throughout, which is the collision
	// the arity check exists to make impossible.
	assert.Equal(t, map[string]string{"env": "prod"}, elbDescribeTags(t, ts.URL, v2)[v2])

	// And a classic ARN naming nothing is still refused by the tagging API, at the absent-resource
	// code [TestELBTagging_AnAbsentELBResourceIsRefusedIndistinguishably] pins: accepting the
	// generation is not accepting a resource that is not there.
	absent := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount + ":loadbalancer/no-such-lb"
	got, ok := tagResourcesFailures(t, ts, "TagResources", absent)[absent]
	require.True(t, ok, "TagResources reported no failure for an absent classic load balancer")
	assert.Equal(t, "InvalidParameterException", got.ErrorCode)
	assert.Equal(t, 400, got.StatusCode)
}

// TestELBTagging_AnAbsentELBResourceIsRefusedIndistinguishably asserts that "this resource is not
// there" reads the same whether the resolver or the merge discovered it.
//
// ELB is the arm where that can differ: it resolves by finding the record, so an absent resource is
// refused before the merge runs, and [TaggingPlugin.tagResolveFailure] delegates that one refusal to
// the merge's own mapping rather than answering the unsupported-type code. A caller must not be able
// to tell which stage found it, because the two are the same fact.
func TestELBTagging_AnAbsentELBResourceIsRefusedIndistinguishably(t *testing.T) {
	t.Parallel()
	ts := elbTaggingServer(t)

	// A well-formed v2 load balancer ARN naming nothing, beside a table ARN naming nothing — whose
	// refusal comes from the merge. The two must be byte-identical in code and status.
	elb := "arn:aws:elasticloadbalancing:us-east-1:" + taggingTestAccount +
		":loadbalancer/app/no-such-lb/50dc6c495c0c9188"
	table := "arn:aws:dynamodb:us-east-1:" + taggingTestAccount + ":table/no-such-table"

	for _, op := range []string{"TagResources", "UntagResources"} {
		fromResolver := tagResourcesFailures(t, ts, op, elb)[elb]
		fromMerge := tagResourcesFailures(t, ts, op, table)[table]
		assert.Equal(t, fromMerge.ErrorCode, fromResolver.ErrorCode, op)
		assert.Equal(t, fromMerge.StatusCode, fromResolver.StatusCode, op)
		assert.Equal(t, "InvalidParameterException", fromResolver.ErrorCode, op)
		assert.Equal(t, 400, fromResolver.StatusCode, op)
	}
}

// sortedKeys returns a map's keys in lexicographic order, so a request built from one is the same on
// every run.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
