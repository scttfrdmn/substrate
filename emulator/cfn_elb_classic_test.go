package emulator_test

import (
	"context"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFormation's classic load balancer, #844's Tier 2.
//
// `AWS::ElasticLoadBalancing::LoadBalancer` — no `V2` — had no arm in the deployer's dispatch at
// all, so a template declaring one reached the generic stub: the stack reported `CREATE_COMPLETE`
// and no `DescribeLoadBalancers` of either generation could find the load balancer. That is the
// #388 class of defect, and the reason every assertion below reads the result back through the
// **classic API** rather than off the returned [emulator.DeployedResource]. A test satisfied by the
// physical ID and the ARN the deployer reported would have passed against the stub, which is how
// the stub survived.
//
// The one parameter that does all the routing is `Version`. `CreateLoadBalancer`,
// `DescribeLoadBalancers` and `DeleteLoadBalancer` are published by both generations, so a deploy
// helper that omitted it would create an Application Load Balancer, report a plausible DNS name and
// satisfy a `Ref` assertion — see `TestCFN_ClassicELBIsNotAnELBv2LoadBalancer`, which is the half
// that fails if it is ever dropped.

// cfnClassicELBTemplate declares one classic load balancer with everything the deployer forwards:
// two listeners (the second deliberately omitting `InstanceProtocol`, which is `Required: No`), both
// placement lists, a security group and a resource-level tag.
//
// The `Outputs` carry `Ref` and all five published `Fn::GetAtt` attributes, because four of those
// five are #1013 absences and "answers empty" is an assertion rather than an omission.
const cfnClassicELBTemplate = `{
	"Resources": {
		"Web": {
			"Type": "AWS::ElasticLoadBalancing::LoadBalancer",
			"Properties": {
				"LoadBalancerName": "cfn-classic-web",
				"Scheme": "internet-facing",
				"AvailabilityZones": ["us-east-1a"],
				"Subnets": ["subnet-11111111", "subnet-22222222"],
				"SecurityGroups": ["sg-33333333"],
				"Listeners": [
					{"Protocol": "HTTP", "LoadBalancerPort": "80",
					 "InstancePort": "8080", "InstanceProtocol": "HTTP"},
					{"Protocol": "TCP", "LoadBalancerPort": "9000", "InstancePort": "9001"}
				],
				"Tags": [{"Key": "team", "Value": "platform"}]
			}
		}
	},
	"Outputs": {
		"TheRef":    {"Value": {"Ref": "Web"}},
		"DNSName":   {"Value": {"Fn::GetAtt": ["Web", "DNSName"]}},
		"ZoneName":  {"Value": {"Fn::GetAtt": ["Web", "CanonicalHostedZoneName"]}},
		"ZoneID":    {"Value": {"Fn::GetAtt": ["Web", "CanonicalHostedZoneNameID"]}},
		"SGName":    {"Value": {"Fn::GetAtt": ["Web", "SourceSecurityGroup.GroupName"]}},
		"SGOwner":   {"Value": {"Fn::GetAtt": ["Web", "SourceSecurityGroup.OwnerAlias"]}}
	}
}`

// cfnClassicELBQuery sends one 2012-06-01 request through the deployer's own registry and returns
// the response, which is how these assertions observe the same records the deploy wrote.
//
// `Version` is set here rather than by each caller for the reason the helper exists: it is the whole
// of the routing, and a test that forgot it would silently assert against ELBv2's handler.
func cfnClassicELBQuery(
	t *testing.T, d *emulator.StackDeployer, action string, params map[string]string,
) *emulator.AWSResponse {
	t.Helper()
	full := map[string]string{"Action": action, "Version": "2012-06-01"}
	for k, v := range params {
		full[k] = v
	}
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: action,
		Params:    full,
		Headers:   map[string]string{},
	}, "probe")
	require.NoError(t, err, "%s was refused", action)
	require.NotNil(t, resp)
	return resp
}

// cfnClassicELBDescription is what `DescribeLoadBalancers` reports for one load balancer, decoded
// from the members `LoadBalancerDescription` publishes.
type cfnClassicELBDescription struct {
	Name              string   `xml:"LoadBalancerName"`
	DNSName           string   `xml:"DNSName"`
	Scheme            string   `xml:"Scheme"`
	AvailabilityZones []string `xml:"AvailabilityZones>member"`
	Subnets           []string `xml:"Subnets>member"`
	SecurityGroups    []string `xml:"SecurityGroups>member"`
	Listeners         []struct {
		Protocol         string `xml:"Protocol"`
		LoadBalancerPort int    `xml:"LoadBalancerPort"`
		InstanceProtocol string `xml:"InstanceProtocol"`
		InstancePort     int    `xml:"InstancePort"`
	} `xml:"ListenerDescriptions>member>Listener"`
}

// cfnClassicELBDescribe returns every classic load balancer the account holds.
func cfnClassicELBDescribe(
	t *testing.T, d *emulator.StackDeployer,
) []cfnClassicELBDescription {
	t.Helper()
	resp := cfnClassicELBQuery(t, d, "DescribeLoadBalancers", nil)
	var doc struct {
		Members []cfnClassicELBDescription `xml:"DescribeLoadBalancersResult>LoadBalancerDescriptions>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "describe body: %s", resp.Body)
	return doc.Members
}

// cfnClassicELBTags reads one load balancer's tags through the classic `DescribeTags`, which is the
// door #844's Tier 1b routed and the only one that can see a classic record's tags by name.
func cfnClassicELBTags(t *testing.T, d *emulator.StackDeployer, name string) map[string]string {
	t.Helper()
	resp := cfnClassicELBQuery(t, d, "DescribeTags",
		map[string]string{"LoadBalancerNames.member.1": name})
	var doc struct {
		Descriptions []struct {
			LoadBalancerName string `xml:"LoadBalancerName"`
			Tags             []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tags>member"`
		} `xml:"DescribeTagsResult>TagDescriptions>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "DescribeTags body: %s", resp.Body)
	require.Len(t, doc.Descriptions, 1)
	require.Equal(t, name, doc.Descriptions[0].LoadBalancerName)

	out := make(map[string]string, len(doc.Descriptions[0].Tags))
	for _, tag := range doc.Descriptions[0].Tags {
		out[tag.Key] = tag.Value
	}
	return out
}

// TestCFN_ClassicELBDeploysThroughTheClassicAPI is the Tier 2 round trip: a template's classic load
// balancer is afterwards reported by the classic `DescribeLoadBalancers` with everything the
// template declared.
//
// The listener assertion is the one that would have caught a `Listeners` list dropped on the floor,
// which is what `AWS::ElasticLoadBalancingV2::LoadBalancer` still does with its own `Subnets` and
// `SecurityGroups`: the CFN type's one `Required: Yes` property is `Listeners`, and a load balancer
// deployed with none would answer every other assertion here.
func TestCFN_ClassicELBDeploysThroughTheClassicAPI(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), cfnClassicELBTemplate, "classic-elb", nil)
	require.NoError(t, err)

	r := findResource(t, result, "Web")
	require.Empty(t, r.Error)
	assert.Equal(t, "AWS::ElasticLoadBalancing::LoadBalancer", r.Type)

	described := cfnClassicELBDescribe(t, d)
	require.Len(t, described, 1, "the classic describe must report the deployed load balancer")
	lb := described[0]
	assert.Equal(t, "cfn-classic-web", lb.Name)
	assert.Equal(t, "internet-facing", lb.Scheme)
	assert.Equal(t, []string{"us-east-1a"}, lb.AvailabilityZones)
	assert.Equal(t, []string{"subnet-11111111", "subnet-22222222"}, lb.Subnets)
	assert.Equal(t, []string{"sg-33333333"}, lb.SecurityGroups)

	require.Len(t, lb.Listeners, 2, "both declared listeners must reach the create")
	assert.Equal(t, "HTTP", lb.Listeners[0].Protocol)
	assert.Equal(t, 80, lb.Listeners[0].LoadBalancerPort)
	assert.Equal(t, 8080, lb.Listeners[0].InstancePort)
	assert.Equal(t, "HTTP", lb.Listeners[0].InstanceProtocol)
	assert.Equal(t, "TCP", lb.Listeners[1].Protocol)
	assert.Equal(t, 9000, lb.Listeners[1].LoadBalancerPort)
	assert.Equal(t, 9001, lb.Listeners[1].InstancePort)
	// Not defaulted to Protocol: the record reports the value the request set, which is the rule
	// elbClassicListenerFrom states and which the flattening must not quietly fill in.
	assert.Empty(t, lb.Listeners[1].InstanceProtocol,
		"an omitted InstanceProtocol must stay omitted")
}

// TestCFN_ClassicELBIsNotAnELBv2LoadBalancer is the negative control for the one parameter the whole
// deploy turns on.
//
// Without `Version: "2012-06-01"` the create reaches ELBv2's handler, which would answer a
// `LoadBalancerArn` and a DNS name — so the resource would still look deployed and `Ref` would still
// resolve. The only observable difference is which generation's describe can see it, so that is what
// is asserted on both sides.
func TestCFN_ClassicELBIsNotAnELBv2LoadBalancer(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	_, err := d.Deploy(context.Background(), cfnClassicELBTemplate, "classic-only", nil)
	require.NoError(t, err)

	require.Len(t, cfnClassicELBDescribe(t, d), 1)

	// The same action name without Version is ELBv2's, and its result member is `LoadBalancers`.
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "DescribeLoadBalancers",
		Params:    map[string]string{"Action": "DescribeLoadBalancers"},
		Headers:   map[string]string{},
	}, "probe")
	require.NoError(t, err)
	require.NotNil(t, resp)
	var v2 struct {
		Members []struct {
			LoadBalancerArn string `xml:"LoadBalancerArn"`
		} `xml:"DescribeLoadBalancersResult>LoadBalancers>member"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &v2))
	assert.Empty(t, v2.Members,
		"the classic deploy must not have created an ELBv2 load balancer")
}

// TestCFN_ClassicELBRefIsTheNameAndDNSNameIsAnAttribute is the acceptance criterion this work
// corrected rather than implemented.
//
// #844's AC8 and `docs/services.md` both said `Ref` resolves to the DNS name. AWS publishes the
// opposite — "`Ref` returns the name of the load balancer" — and the DNS name is `Fn::GetAtt
// DNSName`. Both are asserted here against each other, so neither can drift into the other's value:
// the name is the one the template declared and the DNS name is the one the API itself reports.
//
// The four remaining published attributes assert **empty**, which is the honest answer for a value
// substrate records nothing for (#1013): no hosted zone and no source security group are minted for
// a classic load balancer, and a plausible-looking hosted-zone ID in a stack `Output` is worse than
// a missing one because a consumer would assert against it happily.
func TestCFN_ClassicELBRefIsTheNameAndDNSNameIsAnAttribute(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), cfnClassicELBTemplate, "classic-refs", nil)
	require.NoError(t, err)

	assert.Equal(t, "cfn-classic-web", result.Outputs["TheRef"],
		"Ref returns the name of the load balancer")

	described := cfnClassicELBDescribe(t, d)
	require.Len(t, described, 1)
	assert.Equal(t, described[0].DNSName, result.Outputs["DNSName"],
		"GetAtt DNSName must equal what the API itself reports")
	assert.NotEqual(t, result.Outputs["TheRef"], result.Outputs["DNSName"],
		"the two must not be the same value, or neither assertion above means anything")
	assert.True(t, strings.HasSuffix(result.Outputs["DNSName"], ".elb.amazonaws.com"),
		"the DNS name must be the published form, got %q", result.Outputs["DNSName"])

	for _, attr := range []string{"ZoneName", "ZoneID", "SGName", "SGOwner"} {
		assert.Empty(t, result.Outputs[attr],
			"%s is recorded nowhere, so it must answer empty rather than a plausible value", attr)
	}
}

// TestCFN_ClassicELBTagsReachTheRecord asserts both halves of what a stack puts on this resource:
// the tags the template declared and the three `aws:cloudformation:*` keys the deployer stamps.
//
// Read through the classic `DescribeTags`, which is the only door that can see them by name — the
// stamp is written by ARN through the generation-blind resolver, so a stamp landing on the wrong
// key space would be invisible to a state assertion and visible here.
func TestCFN_ClassicELBTagsReachTheRecord(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	result, err := d.Deploy(context.Background(), cfnClassicELBTemplate, "classic-tags", nil)
	require.NoError(t, err)

	tags := cfnClassicELBTags(t, d, "cfn-classic-web")
	assert.Equal(t, "platform", tags["team"],
		"the template's own tag must reach the create")
	assert.Equal(t, "classic-tags", tags[cfnStampStackNameTag])
	assert.Equal(t, "Web", tags[cfnStampLogicalIDTag])
	assert.NotEmpty(t, tags[cfnStampStackIDTag])

	// The ARN the stamp was written by is substrate's own identifier for the record, since no
	// classic response carries one — so it is asserted to be the classic arity (one segment after
	// `loadbalancer/`) rather than ELBv2's three.
	r := findResource(t, result, "Web")
	assert.True(t, strings.HasSuffix(r.ARN, ":loadbalancer/cfn-classic-web"),
		"the recorded ARN must be the classic form, got %q", r.ARN)
}

// TestCFN_ClassicELBDeleteSweepsIt asserts the stack's delete removes the load balancer, and that a
// second sweep still succeeds.
//
// The deleter is the one piece of this work that could not reuse `queryDeleter`: the classic delete
// needs `LoadBalancerName` **and** `Version`, and a request carrying only the first reaches ELBv2's
// `DeleteLoadBalancer`, which refuses a bare name — so the sweep would report an error and the
// record would stay. The second delete covers the published idempotency ("If the load balancer does
// not exist or has already been deleted, the call to DeleteLoadBalancer still succeeds"), which the
// classic handler already implements and which a sweep retried after a partial failure depends on.
func TestCFN_ClassicELBDeleteSweepsIt(t *testing.T) {
	ctx := context.Background()
	d, _, _, _ := newSweepDeployer(t)

	_, err := d.Deploy(ctx, cfnClassicELBTemplate, "classic-sweep", nil)
	require.NoError(t, err)
	require.Len(t, cfnClassicELBDescribe(t, d), 1)

	require.NoError(t, d.DeleteStack(ctx, "classic-sweep"))
	assert.Empty(t, cfnClassicELBDescribe(t, d),
		"the sweep must have reached the classic DeleteLoadBalancer")

	// Idempotent, so a second delete of the same name is a success rather than a refusal.
	resp := cfnClassicELBQuery(t, d, "DeleteLoadBalancer",
		map[string]string{"LoadBalancerName": "cfn-classic-web"})
	assert.Less(t, resp.StatusCode, 400,
		"a repeated classic delete is published as succeeding, got %d", resp.StatusCode)
}

// cfnClassicELBTemplateWith wraps one set of extra properties around the minimum a classic load
// balancer needs, so a test can vary a single property without restating the rest.
func cfnClassicELBTemplateWith(name, extra string) string {
	return `{"Resources":{"Web":{"Type":"AWS::ElasticLoadBalancing::LoadBalancer",` +
		`"Properties":{"LoadBalancerName":"` + name + `",` +
		`"Subnets":["subnet-11111111"],` + extra + `}}}}`
}

// TestCFN_ClassicELBFlatteningEdges covers what the two flatteners do with a template that is not
// shaped the way the CFN type publishes.
//
// None of these is a hypothetical: `Listeners` and `Tags` are both JSON arrays of objects in the
// Template Reference, and a hand-written or generated template gets either wrong by writing a
// string, by leaving an element out, or by omitting a required member of one element. What matters
// is that each case produces a **visible** outcome rather than a silently half-built load balancer,
// and the deployer has no validator of its own — the API's refusal is the whole of the check.
func TestCFN_ClassicELBFlatteningEdges(t *testing.T) {
	const listener = `{"Protocol":"HTTP","LoadBalancerPort":"80","InstancePort":"8080"}`

	t.Run("ListenersThatIsNotAListIsNoListeners", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(context.Background(),
			cfnClassicELBTemplateWith("scalar-listeners", `"Listeners":"HTTP:80"`),
			"classic-scalar", nil)
		require.NoError(t, err)
		assert.Contains(t, findResource(t, result, "Web").Error, "Listeners.member.1 is required")
	})

	t.Run("AListenerMissingItsProtocolEndsTheList", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		// The second entry carries ports but no Protocol, which is how the query protocol's
		// 1-based contiguous indexing terminates — so the first listener is created and the
		// second is dropped, rather than the whole create being refused.
		result, err := d.Deploy(context.Background(),
			cfnClassicELBTemplateWith("short-listeners",
				`"Listeners":[`+listener+`,{"LoadBalancerPort":"81","InstancePort":"8081"}]`),
			"classic-short", nil)
		require.NoError(t, err)
		require.Empty(t, findResource(t, result, "Web").Error)

		described := cfnClassicELBDescribe(t, d)
		require.Len(t, described, 1)
		assert.Len(t, described[0].Listeners, 1,
			"the entry with no Protocol must end the list rather than widening it")
	})

	t.Run("ANonObjectListenerLeavesAGapTheAPIRefuses", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		// A skipped element does not renumber the ones after it, so a valid listener behind a
		// malformed one lands at `Listeners.member.2` and the API sees no member 1. The refusal
		// is the right outcome for a template that is wrong: the alternative is silently
		// promoting the second listener into the first's place.
		result, err := d.Deploy(context.Background(),
			cfnClassicELBTemplateWith("gapped-listeners", `"Listeners":["oops",`+listener+`]`),
			"classic-gapped", nil)
		require.NoError(t, err)
		assert.Contains(t, findResource(t, result, "Web").Error, "Listeners.member.1 is required")
	})

	t.Run("MalformedTagEntriesAreSkippedAndTheRestStayContiguous", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		// A tag has nothing to store without a key, and the key is what the query list is
		// indexed by — so a keyless entry and a non-object entry are both dropped, and the
		// index advances only for the ones kept. If it did not, the valid tag would land at
		// `Tags.member.2` and extractELBTags' walk would never reach it.
		result, err := d.Deploy(context.Background(),
			cfnClassicELBTemplateWith("odd-tags",
				`"Listeners":[`+listener+`],`+
					`"Tags":[{"Value":"orphan"},"oops",{"Key":"team","Value":"platform"}]`),
			"classic-odd-tags", nil)
		require.NoError(t, err)
		require.Empty(t, findResource(t, result, "Web").Error)

		tags := cfnClassicELBTags(t, d, "odd-tags")
		assert.Equal(t, "platform", tags["team"],
			"the valid tag must land at member 1 and be read back")
		assert.NotContains(t, tags, "", "a keyless entry must not reach the record")
	})

	t.Run("TagsThatIsNotAListIsNoTags", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		result, err := d.Deploy(context.Background(),
			cfnClassicELBTemplateWith("scalar-tags",
				`"Listeners":[`+listener+`],"Tags":{"Key":"team","Value":"platform"}`),
			"classic-scalar-tags", nil)
		require.NoError(t, err)
		require.Empty(t, findResource(t, result, "Web").Error,
			"a misshapen Tags property is dropped, not a create failure: no AWS error covers it")

		// The stamp still lands, which is what distinguishes "the tags were dropped" from
		// "nothing was tagged at all".
		tags := cfnClassicELBTags(t, d, "scalar-tags")
		assert.NotContains(t, tags, "team")
		assert.Equal(t, "Web", tags[cfnStampLogicalIDTag])
	})
}

// TestCFN_ClassicELBWithoutListenersFailsTheCreate is the other direction: `Listeners` is the CFN
// type's one `Required: Yes` property and the classic create's one required list, so a template
// omitting it must report `CREATE_FAILED` rather than a load balancer carrying none.
//
// A deployer that forwarded no listener parameters and ignored the refusal would produce exactly
// that — a resource reported complete with an empty `ListenerDescriptions` — which is the shape of
// the stub this work replaced.
func TestCFN_ClassicELBWithoutListenersFailsTheCreate(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)

	tmpl := `{"Resources":{"Web":{"Type":"AWS::ElasticLoadBalancing::LoadBalancer",` +
		`"Properties":{"LoadBalancerName":"no-listeners","Subnets":["subnet-11111111"]}}}}`
	result, err := d.Deploy(context.Background(), tmpl, "classic-no-listeners", nil)
	require.NoError(t, err)

	r := findResource(t, result, "Web")
	assert.Contains(t, r.Error, "Listeners.member.1 is required",
		"the create's own refusal must reach the resource, got %q", r.Error)
	assert.Empty(t, cfnClassicELBDescribe(t, d),
		"a refused create must leave no load balancer behind")
}
