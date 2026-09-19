package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The shape of an ELBv2 listener and listener-rule ARN (#774).
//
// Substrate nested the child under its load balancer's ARN — `…:loadbalancer/app/web/<lbid>/
// listener/<id>` — where AWS mints a **sibling**: `…:listener/app/web/<lbid>/<id>`, repeating the
// load balancer's name and id inside the child's own ARN. The consequence is not cosmetic. Every
// AWS policy example scopes a listener statement with `…:listener/*`, and that wildcard matched
// nothing at all against the nested shape, so a `Deny` written the documented way silently failed
// to deny while an `Allow` on `…:loadbalancer/*` reached listeners it should not have.
//
// The assertions are against **AWS's own published format strings** rather than a hand-copied
// template: the Service Reference Information document for elasticloadbalancing is vendored at
// `emulator/authzref/elasticloadbalancing.json`, so the check is a join between what substrate
// mints and what AWS publishes, and a future drift in either fails here.

// elbARNFormat returns the single ARN format AWS publishes for one ELB resource type, failing the
// test when the vendored snapshot carries none.
func elbARNFormat(t *testing.T, resourceType string) string {
	t.Helper()
	formats, ok := emulator.AuthzResourceARNFormatsForTest("elasticloadbalancing", resourceType)
	require.True(t, ok, "AWS publishes the %q resource type", resourceType)
	require.Len(t, formats, 1, "%q has one published format", resourceType)
	return formats[0]
}

// elbFillARNFormat substitutes AWS's `${Placeholder}` tokens, so a published template can be
// compared against a minted ARN directly.
//
// An unsubstituted token left in the result is a test bug rather than a product one — the
// placeholder names come from AWS and could change — so it is asserted rather than tolerated.
func elbFillARNFormat(t *testing.T, format string, values map[string]string) string {
	t.Helper()
	out := format
	for name, value := range values {
		out = strings.ReplaceAll(out, "${"+name+"}", value)
	}
	require.NotContains(t, out, "${", "every placeholder in %q was substituted", format)
	return out
}

// elbARNSegments splits an ARN's resource part into the type and the segments after it.
func elbARNSegments(t *testing.T, arn string) (resourceType string, segments []string) {
	t.Helper()
	fields := strings.SplitN(arn, ":", 6)
	require.Len(t, fields, 6, "%q is a six-field ARN", arn)
	parts := strings.Split(fields[5], "/")
	require.Greater(t, len(parts), 1, "%q has a resource type and at least one segment", arn)
	return parts[0], parts[1:]
}

func TestELB_AListenerAndRuleARNMatchTheFormatAWSPublishes(t *testing.T) {
	// The load balancer's name and id are read back out of the minted child ARNs and substituted
	// into AWS's template, so the test asserts the *structure* — which type, how many segments,
	// which parts repeated — rather than restating a literal that would agree with a wrong
	// implementation.
	t.Parallel()

	ts := newELBTestServer(t)
	lbARN := elbCreateLB(t, ts.URL, "web", nil)
	tgARN := elbCreateTG(t, ts.URL, "web-tg", nil)
	listenerARN := elbCreateListener(t, ts.URL, lbARN, tgARN, nil)
	ruleARN := elbCreateRule(t, ts.URL, listenerARN, tgARN, nil)

	lbType, lbSegments := elbARNSegments(t, lbARN)
	require.Equal(t, "loadbalancer", lbType)
	require.Len(t, lbSegments, 3, "loadbalancer/app/<name>/<id>")
	subtype, lbName, lbID := lbSegments[0], lbSegments[1], lbSegments[2]
	require.Equal(t, "app", subtype, "an application load balancer's ARN subtype")

	listenerType, listenerSegments := elbARNSegments(t, listenerARN)
	assert.Equal(t, "listener", listenerType,
		"a listener's resource type is listener/<subtype>, not the load balancer's")
	require.Len(t, listenerSegments, 4, "listener/app/<name>/<lbid>/<listenerid>")
	listenerID := listenerSegments[3]

	assert.Equal(t, elbFillARNFormat(t, elbARNFormat(t, "listener/app"), map[string]string{
		"Partition":        "aws",
		"Region":           "us-east-1",
		"Account":          "123456789012",
		"LoadBalancerName": lbName,
		"LoadBalancerId":   lbID,
		"ListenerId":       listenerID,
	}), listenerARN)

	ruleType, ruleSegments := elbARNSegments(t, ruleARN)
	assert.Equal(t, "listener-rule", ruleType, "a rule's own resource type, not listener/…/rule/…")
	require.Len(t, ruleSegments, 5, "listener-rule/app/<name>/<lbid>/<listenerid>/<ruleid>")

	assert.Equal(t, elbFillARNFormat(t, elbARNFormat(t, "listener-rule/app"), map[string]string{
		"Partition":        "aws",
		"Region":           "us-east-1",
		"Account":          "123456789012",
		"LoadBalancerName": lbName,
		"LoadBalancerId":   lbID,
		"ListenerId":       listenerID,
		"ListenerRuleId":   ruleSegments[4],
	}), ruleARN)

	// The rule repeats its listener's id, which is what makes a listener-scoped wildcard and a
	// rule-scoped one independently writable.
	assert.Equal(t, listenerID, ruleSegments[3], "a rule carries its listener's id")

	// And neither is nested under the load balancer's ARN any more, which is the defect itself.
	assert.NotContains(t, listenerARN, ":loadbalancer/")
	assert.NotContains(t, ruleARN, ":loadbalancer/")
	assert.False(t, strings.HasPrefix(listenerARN, lbARN),
		"a listener is a sibling of its load balancer, not a child of its ARN")
}

func TestELB_AnARNCarriesAWSsTypeAbbreviation(t *testing.T) {
	// `LoadBalancerTypeEnum` is application|network|gateway and the ARN says app|net|gwy — two
	// vocabularies, and substrate used the wrong one in the ARN. The abbreviations are not asserted
	// as literals but looked up in the vendored snapshot, which publishes each as a resource type
	// of its own, so this is a join between substrate's spelling and AWS's rather than a restated
	// guess.
	//
	// It belongs with the listener reshaping rather than after it: a listener ARN repeats its load
	// balancer's subtype, so leaving `application` in place would mint `listener/application/…`
	// and `listener/app/*` would still match nothing.
	t.Parallel()

	for _, tc := range []struct{ lbType, subtype string }{
		{"application", "app"},
		{"network", "net"},
		{"gateway", "gwy"},
	} {
		t.Run(tc.lbType, func(t *testing.T) {
			t.Parallel()
			_, ok := emulator.AuthzResourceARNFormatsForTest("elasticloadbalancing",
				"loadbalancer/"+tc.subtype+"/")
			require.True(t, ok, "AWS publishes loadbalancer/%s/ as a resource type", tc.subtype)

			ts := newELBTestServer(t)
			lbARN := elbCreateLB(t, ts.URL, "web", map[string]string{"Type": tc.lbType})
			_, segments := elbARNSegments(t, lbARN)
			assert.Equal(t, tc.subtype, segments[0],
				"a %s load balancer's ARN carries %q, not %q", tc.lbType, tc.subtype, tc.lbType)
		})
	}
}

func TestELB_AMalformedParentARNIsRefusedRatherThanMintingAMalformedChild(t *testing.T) {
	// A child's ARN is built from its parent's, so an unparseable parent cannot produce one.
	// Refusing beats minting an ARN of the wrong arity that no policy could ever match — which is
	// the failure mode #774 is about, arrived at from the other direction.
	t.Parallel()

	ts := newELBTestServer(t)
	tgARN := elbCreateTG(t, ts.URL, "web-tg", nil)

	resp := elbRequest(t, ts.URL, map[string]string{
		"Action":          "CreateListener",
		"LoadBalancerArn": "not-an-arn",
		"Protocol":        "HTTP",
		"Port":            "80",
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, resp))

	// The classic-ELB form AWS also publishes — `loadbalancer/<name>`, with no subtype — carries
	// too few segments to build an ELBv2 listener from, and must not be padded out into one.
	//
	// Since #844 that ARN can name a real classic load balancer, which makes the refusal sharper
	// rather than weaker: `CreateListener` is an operation only the 2015-12-01 API publishes, and a
	// classic load balancer is not a parent it can hang a listener on whether the record exists or
	// not. The arity is the whole of the check, and it is the same check either way.
	classic := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/web"
	resp2 := elbRequest(t, ts.URL, map[string]string{
		"Action":                                 "CreateListener",
		"LoadBalancerArn":                        classic,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	})
	defer resp2.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	assert.Equal(t, "ValidationError", elbErrorCode(t, resp2))
}

func TestELB_APolicyScopedToTheTypeWildcardReachesAListenerAndARule(t *testing.T) {
	// #774's fourth criterion, and the reason the shape matters: the wildcards below are exactly
	// what AWS's own examples write, and against the nested shape each matched nothing — so a
	// listener statement was unenforceable in both directions.
	t.Parallel()

	listenerWildcard := "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":listener/*"
	ruleWildcard := "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":listener-rule/*"

	t.Run("a listener wildcard allows ModifyListener", func(t *testing.T) {
		t.Parallel()
		f := newELBAuthzFixture(t, "listener-writer", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:ModifyListener", listenerWildcard))
		assert.NoError(t, f.call(t, "ModifyListener",
			map[string]string{"ListenerArn": elbAuthzListenerARN}))
	})

	t.Run("a listener wildcard does not reach a rule", func(t *testing.T) {
		// The two types are siblings, so a listener grant must not carry rules with it —
		// `listener/*` is not a prefix of a `listener-rule/…` ARN.
		t.Parallel()
		f := newELBAuthzFixture(t, "listener-only", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:*", listenerWildcard))
		assert.Error(t, f.call(t, "DeleteRule",
			map[string]string{"RuleArn": elbAuthzRuleARN}))
	})

	t.Run("a rule wildcard allows DeleteRule", func(t *testing.T) {
		t.Parallel()
		f := newELBAuthzFixture(t, "rule-writer", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:DeleteRule", ruleWildcard))
		assert.NoError(t, f.call(t, "DeleteRule",
			map[string]string{"RuleArn": elbAuthzRuleARN}))
	})

	t.Run("a rule wildcard Deny refuses DeleteRule", func(t *testing.T) {
		// The direction that made this a correctness problem rather than an inconvenience: a Deny
		// that matches nothing is a Deny that does not deny, and the caller is allowed through.
		t.Parallel()
		f := newELBAuthzFixture(t, "rule-denied", emulator.PolicyDocument{})
		f.setPolicy(t,
			elbAuthzAllow("elasticloadbalancing:*", "*"),
			emulator.PolicyStatement{
				Effect:   "Deny",
				Action:   emulator.StringOrSlice{"elasticloadbalancing:DeleteRule"},
				Resource: emulator.StringOrSlice{ruleWildcard},
			},
		)
		assert.Error(t, f.call(t, "DeleteRule",
			map[string]string{"RuleArn": elbAuthzRuleARN}))
		assert.NoError(t, f.call(t, "ModifyListener",
			map[string]string{"ListenerArn": elbAuthzListenerARN}),
			"the Deny is scoped to rules and must not spill onto listeners")
	})

	t.Run("a load balancer wildcard no longer reaches its listeners", func(t *testing.T) {
		// The compatibility half, stated as an assertion: under the nested shape a
		// `loadbalancer/*` grant was a prefix of every listener and rule ARN, so it silently
		// carried them. It does not any more, which is what AWS does.
		t.Parallel()
		f := newELBAuthzFixture(t, "lb-only", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:*",
			"arn:aws:elasticloadbalancing:"+elbAuthzRegion+":"+elbAuthzAccount+":loadbalancer/*"))
		assert.Error(t, f.call(t, "ModifyListener",
			map[string]string{"ListenerArn": elbAuthzListenerARN}))
		assert.Error(t, f.call(t, "DeleteRule",
			map[string]string{"RuleArn": elbAuthzRuleARN}))
	})
}

func TestELB_TheDescribeFiltersDoNotDependOnARNNesting(t *testing.T) {
	// #774's third criterion. DescribeListeners filters on the *stored* LoadBalancerARN and
	// DescribeRules on the stored ListenerARN, never on a prefix of the child's ARN — so both
	// survive the reshaping. That was true before this change and is pinned here so a later
	// "optimization" back to prefix matching cannot reintroduce the coupling.
	t.Parallel()

	ts := newELBTestServer(t)
	tgARN := elbCreateTG(t, ts.URL, "web-tg", nil)
	lbA := elbCreateLB(t, ts.URL, "web-a", nil)
	lbB := elbCreateLB(t, ts.URL, "web-b", nil)
	listenerA := elbCreateListener(t, ts.URL, lbA, tgARN, nil)
	listenerB := elbCreateListener(t, ts.URL, lbB, tgARN, nil)
	ruleA := elbCreateRule(t, ts.URL, listenerA, tgARN, nil)
	elbCreateRule(t, ts.URL, listenerB, tgARN, map[string]string{"Priority": "20"})

	assert.Equal(t, []string{listenerA}, elbDescribeListenerARNs(t, ts, lbA))
	assert.Equal(t, []string{listenerB}, elbDescribeListenerARNs(t, ts, lbB))
	assert.Equal(t, []string{ruleA}, elbDescribeRuleARNs(t, ts, listenerA))
}

func TestELB_TaggingResolvesTheNewShapeAndStillResolvesTheOld(t *testing.T) {
	// AddTags and DescribeTags round-trip a minted ARN, which is the new shape. And an ARN of the
	// pre-#774 nested shape — the one an event log recorded by an earlier version holds — still
	// names its resource, because [elbResourceKindFromARN] recognizes both. A replay whose tagging
	// calls suddenly named nothing would defeat the property the event store exists to provide.
	t.Parallel()

	ts := newELBTestServer(t)
	tgARN := elbCreateTG(t, ts.URL, "web-tg", nil)
	lbARN := elbCreateLB(t, ts.URL, "web", nil)
	listenerARN := elbCreateListener(t, ts.URL, lbARN, tgARN, nil)
	ruleARN := elbCreateRule(t, ts.URL, listenerARN, tgARN, nil)

	addResp := elbRequest(t, ts.URL, map[string]string{
		"Action":                "AddTags",
		"ResourceArns.member.1": listenerARN,
		"ResourceArns.member.2": ruleARN,
		"Tags.member.1.Key":     "team",
		"Tags.member.1.Value":   "platform",
	})
	require.Equal(t, http.StatusOK, addResp.StatusCode)
	require.NoError(t, addResp.Body.Close())

	tags := elbDescribeTags(t, ts.URL, listenerARN, ruleARN)
	assert.Equal(t, map[string]string{"team": "platform"}, tags[listenerARN])
	assert.Equal(t, map[string]string{"team": "platform"}, tags[ruleARN])

	// The old shapes are still classified, so a request naming one is refused for the resource
	// being absent rather than for the ARN being unrecognizable.
	assert.Equal(t, "listener",
		emulator.ELBResourceKindFromARNForTest(lbARN+"/listener/0aaa333"))
	assert.Equal(t, "listener-rule",
		emulator.ELBResourceKindFromARNForTest(lbARN+"/listener/0aaa333/rule/0bbb444"))
	assert.Equal(t, "listener",
		emulator.ELBResourceKindFromARNForTest(listenerARN), "and so is the new one")
	assert.Equal(t, "listener-rule",
		emulator.ELBResourceKindFromARNForTest(ruleARN))
}

// elbDescribeListenerARNs returns the listener ARNs DescribeListeners reports for one load
// balancer, in the order it reports them.
func elbDescribeListenerARNs(t *testing.T, ts *httptest.Server, lbARN string) []string {
	t.Helper()
	resp := elbRequest(t, ts.URL, map[string]string{
		"Action":          "DescribeListeners",
		"LoadBalancerArn": lbARN,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		Result struct {
			Listeners []struct {
				ListenerArn string `xml:"ListenerArn"`
			} `xml:"Listeners>member"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	arns := make([]string, 0, len(result.Result.Listeners))
	for _, l := range result.Result.Listeners {
		arns = append(arns, l.ListenerArn)
	}
	return arns
}

// elbDescribeRuleARNs returns the rule ARNs DescribeRules reports for one listener.
func elbDescribeRuleARNs(t *testing.T, ts *httptest.Server, listenerARN string) []string {
	t.Helper()
	resp := elbRequest(t, ts.URL, map[string]string{
		"Action":      "DescribeRules",
		"ListenerArn": listenerARN,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		Result struct {
			Rules []struct {
				RuleArn string `xml:"RuleArn"`
			} `xml:"Rules>member"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	arns := make([]string, 0, len(result.Result.Rules))
	for _, r := range result.Result.Rules {
		arns = append(arns, r.RuleArn)
	}
	return arns
}
