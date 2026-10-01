package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ELB answers every resource through an item struct — the four ELBv2 shapes in elb_wire.go and the
// classic one in elb_classic.go — rather than by encoding the persisted record, and nothing asserted
// that (#756). These tests are that assertion: for each of the five records, every operation that
// answers it is driven and the body's element names are compared against the published shape's — so
// AccountID, Region, Suffix and `ever_tagged` cannot reach a body, and neither can anything else
// substrate has not deliberately published.
//
// Element *names*, not a decode into a Go struct, for the reason glue_wire_test.go gives: decoding
// into a type that has no field for an extra member is exactly the step that hides it. A path rather
// than a bare name is compared, as in rds_wire_test.go, because it also catches a published member
// rendered at the wrong depth — and ELB nests four deep in a classic listener description.
//
// Both generations are pinned here even though only the ELBv2 rendering moved to elb_wire.go: the
// classic record carries the same three baseline lines and is answered by the same envelope, so one
// walker serves both.

// elbWireClock is the instant setupELBWirePlugin starts the simulated clock at, so every projected
// timestamp has a known value. Non-zero on purpose: a test whose clock was the zero time would see
// CreatedTime rendered as the year-one instant and could not tell that apart from a projection that
// never read the record.
//
// The controller advances a small deterministic step per observation rather than standing still, so
// the assertions below bound the value rather than equate it — still nothing read off the wall clock.
var elbWireClock = time.Unix(1700000000, 0).UTC()

// elbWireAccount and elbWireRegion scope every state key the plugin writes, and are what the ARNs in
// the responses are built from. The region is us-east-1 because the classic subtest on the full
// server reaches the plugin through elbRequest, which signs for that region.
const (
	elbWireAccount = taggingTestAccount
	elbWireRegion  = "us-east-1"
)

// The published member paths of each shape, one entry per element the body may carry, relative to
// the record's own element. A nested element appears as "parent/child" so a member rendered at the
// wrong depth fails rather than passing on its leaf name.
//
// A list member therefore contributes two entries: the wrapper element itself and "Wrapper/member"
// beneath it, which is the Query protocol's spelling of a list and what the walker sees. Listing the
// wrapper separately is also what pins an empty-but-present element — a classic ListenerDescription's
// PolicyNames is published that way on purpose.
//
// Each list is the published shape's membership minus what substrate does not model; elb_wire.go and
// classicLBToItem record which members those are and why each is absent rather than present and zero.
var (
	// API_LoadBalancer. State is an API_LoadBalancerState object and each AvailabilityZones member
	// an API_AvailabilityZone, so both are one element deeper than their wrapper.
	elbWireLoadBalancerMembers = []string{
		"AvailabilityZones",
		"AvailabilityZones/member",
		"AvailabilityZones/member/ZoneName",
		"CreatedTime",
		"DNSName",
		"LoadBalancerArn",
		"LoadBalancerName",
		"Scheme",
		"SecurityGroups",
		"SecurityGroups/member",
		"State",
		"State/Code",
		"Type",
		"VpcId",
	}

	// API_TargetGroup. LoadBalancerArns is deliberately absent: a target group's record holds no
	// association to a load balancer, so there is nothing to report.
	elbWireTargetGroupMembers = []string{
		"HealthCheckPath",
		"HealthCheckPort",
		"HealthCheckProtocol",
		"Port",
		"Protocol",
		"TargetGroupArn",
		"TargetGroupName",
		"TargetType",
		"VpcId",
	}

	// API_Listener. Each DefaultActions member is an API_Action; Order is `omitempty` and no create
	// records one, so it is absent from the body rather than reported as zero.
	elbWireListenerMembers = []string{
		"DefaultActions",
		"DefaultActions/member",
		"DefaultActions/member/TargetGroupArn",
		"DefaultActions/member/Type",
		"ListenerArn",
		"LoadBalancerArn",
		"Port",
		"Protocol",
	}

	// API_Rule. There is deliberately no ListenerArn entry: API_Rule publishes none, the listener
	// being how DescribeRules selects rather than something it reports.
	elbWireRuleMembers = []string{
		"Actions",
		"Actions/member",
		"Actions/member/TargetGroupArn",
		"Actions/member/Type",
		"Conditions",
		"Conditions/member",
		"Conditions/member/Field",
		"Conditions/member/Values",
		"Conditions/member/Values/member",
		"IsDefault",
		"Priority",
		"RuleArn",
	}

	// API_LoadBalancerDescription, the 2012-06-01 shape. PolicyNames is a present-but-empty element
	// by design (see elbClassicPolicyNames), so it appears here with no member beneath it.
	// SSLCertificateId is absent because an HTTP listener carries no certificate, and VPCId because
	// no classic create persists one — both `omitempty`.
	elbWireClassicMembers = []string{
		"AvailabilityZones",
		"AvailabilityZones/member",
		"CreatedTime",
		"DNSName",
		"ListenerDescriptions",
		"ListenerDescriptions/member",
		"ListenerDescriptions/member/Listener",
		"ListenerDescriptions/member/Listener/InstancePort",
		"ListenerDescriptions/member/Listener/InstanceProtocol",
		"ListenerDescriptions/member/Listener/LoadBalancerPort",
		"ListenerDescriptions/member/Listener/Protocol",
		"ListenerDescriptions/member/PolicyNames",
		"LoadBalancerName",
		"Scheme",
		"SecurityGroups",
		"SecurityGroups/member",
		"Subnets",
		"Subnets/member",
	}
)

// setupELBWirePlugin returns the ELB plugin, a request context and the state manager behind it.
//
// The state manager is handed back because half of what is under test is that the record keeps the
// members the response drops — and because it is the only place the anchors below can read
// `ever_tagged` from, that member having no published home to read it back through.
//
// Its own harness rather than newELBTestServer, which seeds the clock from time.Now(): a timestamp
// assertion needs a known instant. The plugin is called directly rather than through an httptest
// server because what is under test is the bytes of a response body, and the server adds nothing to
// those. The IDMint is seeded so the minted ARN suffixes a listener's and a rule's state key carry
// are the same on every run.
func setupELBWirePlugin(t *testing.T) (*emulator.ELBPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.ELBPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(elbWireClock)},
	}), "emulator.ELBPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: elbWireAccount,
		Region:    elbWireRegion,
		RequestID: "req-elb-wire",
		IDs:       emulator.NewIDMint("elb-wire"),
	}, state
}

// elbWire issues one query-protocol action and returns the raw response body, failing the test on
// anything but 200.
func elbWire(t *testing.T, p *emulator.ELBPlugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: action,
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// elbWireMembers walks an ELB response body and returns every element inside wrapper, keyed by its
// path relative to wrapper and valued with the text it carries.
//
// A list response carries wrapper more than once (DescribeRulesResult>Rules>member), and the paths
// are unioned across every occurrence — so an extra member on any item in the page fails, not just
// on the first. Every ELB list is spelled with `member` as the item element, so the wrapper this is
// called with is the enclosing list element rather than the item's own name.
func elbWireMembers(t *testing.T, action string, body []byte, wrapper string) map[string]string {
	t.Helper()
	out := map[string]string{}
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	inside := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "%s: decode body: %s", action, body)
		switch el := tok.(type) {
		case xml.StartElement:
			if !inside {
				// Everything above the record — the response envelope, the result element and the
				// list wrapper — is skipped, so one expectation serves every operation that answers
				// the record.
				inside = el.Name.Local == wrapper
				continue
			}
			path = append(path, el.Name.Local)
			key := strings.Join(path, "/")
			if _, seen := out[key]; !seen {
				out[key] = ""
			}
		case xml.EndElement:
			switch {
			case !inside:
			case len(path) == 0:
				inside = false // wrapper's own end tag
			default:
				path = path[:len(path)-1]
			}
		case xml.CharData:
			if !inside || len(path) == 0 {
				continue
			}
			if text := strings.TrimSpace(string(el)); text != "" {
				out[strings.Join(path, "/")] += text
			}
		}
	}
	require.NotEmpty(t, out, "%s: body carries no %s element: %s", action, wrapper, body)
	return out
}

// elbWireAssertMembers requires that the record's elements are exactly the published ones.
//
// Equality rather than a set of NotContains assertions: it subsumes every absence check at once, and
// it cannot go vacuous the way an absence check on a member the body could not have carried does.
func elbWireAssertMembers(t *testing.T, action string, body []byte, wrapper string, want []string) map[string]string {
	t.Helper()
	members := elbWireMembers(t, action, body, wrapper)
	assert.Equal(t, want, slices.Sorted(maps.Keys(members)),
		"%s: %s must carry exactly the published members: %s", action, wrapper, body)
	return members
}

// elbWireTime parses a projected timestamp and requires it to be the simulated clock's, within a
// tolerance.
//
// Bounded rather than equated because TimeController advances a deterministic step per observation;
// a test demanding the seeded instant exactly fails on the second request of a run. RFC3339Nano
// rather than RFC3339 so that a projection which gained sub-second precision still parses — what is
// asserted is that the value is ISO8601 and is the seeded clock's, not its field width.
func elbWireTime(t *testing.T, action string, members map[string]string, member string) {
	t.Helper()
	raw, ok := members[member]
	require.True(t, ok, "%s must report %s", action, member)
	ts, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err, "%s: %s is not an ISO8601 instant: %q", action, member, raw)
	assert.WithinDuration(t, elbWireClock, ts, time.Second, "%s: %s", action, member)
}

// elbWireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func elbWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "elb", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// elbWireKey builds the scoped state key a named record is stored under.
func elbWireKey(prefix, name string) string {
	return prefix + ":" + elbWireAccount + "/" + elbWireRegion + "/" + name
}

// elbWireOnlyKey finds the one state key under prefix, for the two records whose key carries a
// minted ARN suffix rather than a name.
//
// A listener's and a rule's key is `listener:`/`rule:` plus a generateELBSuffix value, which is the
// reason the tagging API's resolver finds those keys rather than building them (#935). Finding it the
// same way keeps this test from encoding the mint's output as a literal.
func elbWireOnlyKey(t *testing.T, state emulator.StateManager, prefix string) string {
	t.Helper()
	keys, err := state.List(t.Context(), "elb", prefix+":"+elbWireAccount+"/"+elbWireRegion+"/")
	require.NoError(t, err, "state.List %s", prefix)
	require.Len(t, keys, 1, "expected exactly one %s record", prefix)
	return keys[0]
}

// elbWireScope requires that a record persists the account and Region that have no published home on
// any ELB shape.
//
// This is the anchor for a body rendered before any tag exists — a create's own response. Without it
// an assertion that the body omits AccountID would pass on a record that never carried one.
func elbWireScope(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	record := elbWireRecord(t, state, key)
	require.Equal(t, `"`+elbWireAccount+`"`, string(record["AccountID"]), "%s must persist AccountID", key)
	require.Equal(t, `"`+elbWireRegion+`"`, string(record["Region"]), "%s must persist Region", key)
}

// elbWireTag tags a resource through ELBv2's own AddTags and proves through DescribeTags that the
// tag landed, then requires that the stored record carries `ever_tagged`.
//
// This is the presence anchor the absence assertions need. `ever_tagged` is `,omitempty`, so it is
// absent from a record that has never been tagged — and an assertion that a response omits a member
// the record could not have supplied passes without testing anything. elbTagsForCreate deliberately
// does not set it (#938), so the anchor has to be an explicit tag call rather than a create carrying
// tags. Tagging first makes the member exist at the moment every later response is rendered.
func elbWireTag(t *testing.T, p *emulator.ELBPlugin, ctx *emulator.RequestContext, state emulator.StateManager, arn, key string) {
	t.Helper()
	elbWire(t, p, ctx, "AddTags", map[string]string{
		"ResourceArns.member.1": arn,
		"Tags.member.1.Key":     "wire",
		"Tags.member.1.Value":   "anchor",
	})

	// Read back through the tag operation, never out of the state store — #765's rule.
	listed := elbWire(t, p, ctx, "DescribeTags", map[string]string{"ResourceArns.member.1": arn})
	assert.Contains(t, string(listed), "<Key>wire</Key>", "AddTags stored no tag for %s", arn)

	record := elbWireRecord(t, state, key)
	require.Equal(t, "true", string(record["ever_tagged"]),
		"%s must carry ever_tagged before an absence assertion on it means anything", key)
}

// elbWireCreateLB creates the load balancer the listener and rule tests hang off, and returns its
// ARN. One subnet and one security group so both of the members #756 added are populated.
func elbWireCreateLB(t *testing.T, p *emulator.ELBPlugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	body := elbWire(t, p, ctx, "CreateLoadBalancer", map[string]string{
		"Name":                    name,
		"Type":                    "application",
		"Scheme":                  "internet-facing",
		"VpcId":                   "vpc-wire",
		"Subnets.member.1":        "subnet-wire",
		"SecurityGroups.member.1": "sg-wire",
	})
	arn := elbWireMembers(t, "CreateLoadBalancer", body, "member")["LoadBalancerArn"]
	require.NotEmpty(t, arn, "CreateLoadBalancer must report LoadBalancerArn")
	return arn
}

// elbWireCreateTG creates the target group the listener and rule tests forward to, and returns its
// ARN.
func elbWireCreateTG(t *testing.T, p *emulator.ELBPlugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	body := elbWire(t, p, ctx, "CreateTargetGroup", map[string]string{
		"Name":                name,
		"Protocol":            "HTTP",
		"Port":                "80",
		"VpcId":               "vpc-wire",
		"TargetType":          "instance",
		"HealthCheckPath":     "/health",
		"HealthCheckProtocol": "HTTP",
		"HealthCheckPort":     "traffic-port",
	})
	arn := elbWireMembers(t, "CreateTargetGroup", body, "member")["TargetGroupArn"]
	require.NotEmpty(t, arn, "CreateTargetGroup must report TargetGroupArn")
	return arn
}

func TestELBWire_LoadBalancerResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	created := elbWireAssertMembers(t, "CreateLoadBalancer",
		elbWire(t, p, ctx, "CreateLoadBalancer", map[string]string{
			"Name":                    "wire-alb",
			"Type":                    "application",
			"Scheme":                  "internet-facing",
			"VpcId":                   "vpc-wire",
			"Subnets.member.1":        "subnet-wire",
			"SecurityGroups.member.1": "sg-wire",
		}), "member", elbWireLoadBalancerMembers)
	elbWireTime(t, "CreateLoadBalancer", created, "CreatedTime")

	// The two members #756 added report the values the create derived and stored, rather than merely
	// being present: a projection that emitted the elements empty would satisfy the path set above.
	assert.Equal(t, elbWireRegion+"a", created["AvailabilityZones/member/ZoneName"],
		"the zone is derived from Subnets.member, and API_AvailabilityZone carries it as ZoneName")
	assert.Equal(t, "sg-wire", created["SecurityGroups/member"],
		"SecurityGroups.member.N is published as a list of strings")

	arn := created["LoadBalancerArn"]
	require.NotEmpty(t, arn, "CreateLoadBalancer must report LoadBalancerArn")
	key := elbWireKey("lb", "wire-alb")

	// Create's own body is rendered before any tag exists, so it anchors on the two members that are
	// always set and have no published home at all.
	elbWireScope(t, state, key)

	// Every response after this one is rendered from a record carrying ever_tagged.
	elbWireTag(t, p, ctx, state, arn, key)

	described := elbWireAssertMembers(t, "DescribeLoadBalancers",
		elbWire(t, p, ctx, "DescribeLoadBalancers", map[string]string{"Names.member.1": "wire-alb"}),
		"member", elbWireLoadBalancerMembers)
	elbWireTime(t, "DescribeLoadBalancers", described, "CreatedTime")
	assert.Equal(t, arn, described["LoadBalancerArn"], "DescribeLoadBalancers must report the created ARN")
}

func TestELBWire_TargetGroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	created := elbWireAssertMembers(t, "CreateTargetGroup",
		elbWire(t, p, ctx, "CreateTargetGroup", map[string]string{
			"Name":                "wire-tg",
			"Protocol":            "HTTP",
			"Port":                "80",
			"VpcId":               "vpc-wire",
			"TargetType":          "instance",
			"HealthCheckPath":     "/health",
			"HealthCheckProtocol": "HTTP",
			"HealthCheckPort":     "traffic-port",
		}), "member", elbWireTargetGroupMembers)

	arn := created["TargetGroupArn"]
	require.NotEmpty(t, arn, "CreateTargetGroup must report TargetGroupArn")
	key := elbWireKey("tg", "wire-tg")

	elbWireScope(t, state, key)
	elbWireTag(t, p, ctx, state, arn, key)

	described := elbWireAssertMembers(t, "DescribeTargetGroups",
		elbWire(t, p, ctx, "DescribeTargetGroups", map[string]string{"Names.member.1": "wire-tg"}),
		"member", elbWireTargetGroupMembers)
	assert.Equal(t, arn, described["TargetGroupArn"], "DescribeTargetGroups must report the created ARN")

	// ModifyTargetGroup answered an empty TargetGroups list until #756 — the record was found,
	// modified, written back and then discarded. So this asserts the modified value is reported as
	// well as the membership: the empty body it used to answer carried no element to be wrong.
	modified := elbWireAssertMembers(t, "ModifyTargetGroup",
		elbWire(t, p, ctx, "ModifyTargetGroup", map[string]string{
			"TargetGroupArn":  arn,
			"HealthCheckPath": "/ready",
		}), "member", elbWireTargetGroupMembers)
	assert.Equal(t, "/ready", modified["HealthCheckPath"],
		"ModifyTargetGroup must report the modified target group, not an empty list")
	assert.Equal(t, arn, modified["TargetGroupArn"], "ModifyTargetGroup must report the modified ARN")
}

func TestELBWire_ListenerResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	lbARN := elbWireCreateLB(t, p, ctx, "wire-alb")
	tgARN := elbWireCreateTG(t, p, ctx, "wire-tg")

	created := elbWireAssertMembers(t, "CreateListener",
		elbWire(t, p, ctx, "CreateListener", map[string]string{
			"LoadBalancerArn":                        lbARN,
			"Protocol":                               "HTTP",
			"Port":                                   "80",
			"DefaultActions.member.1.Type":           "forward",
			"DefaultActions.member.1.TargetGroupArn": tgARN,
		}), "member", elbWireListenerMembers)

	arn := created["ListenerArn"]
	require.NotEmpty(t, arn, "CreateListener must report ListenerArn")
	key := elbWireOnlyKey(t, state, "listener")

	elbWireScope(t, state, key)
	elbWireTag(t, p, ctx, state, arn, key)

	described := elbWireAssertMembers(t, "DescribeListeners",
		elbWire(t, p, ctx, "DescribeListeners", map[string]string{"ListenerArns.member.1": arn}),
		"member", elbWireListenerMembers)
	assert.Equal(t, tgARN, described["DefaultActions/member/TargetGroupArn"],
		"API_Action.TargetGroupArn sits inside the action, one element below the DefaultActions wrapper")

	modified := elbWireAssertMembers(t, "ModifyListener",
		elbWire(t, p, ctx, "ModifyListener", map[string]string{
			"ListenerArn": arn,
			"Port":        "8080",
		}), "member", elbWireListenerMembers)
	assert.Equal(t, "8080", modified["Port"], "ModifyListener must report the modified listener")
}

func TestELBWire_RuleResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	lbARN := elbWireCreateLB(t, p, ctx, "wire-alb")
	tgARN := elbWireCreateTG(t, p, ctx, "wire-tg")
	listenerBody := elbWire(t, p, ctx, "CreateListener", map[string]string{
		"LoadBalancerArn":                        lbARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	})
	listenerARN := elbWireMembers(t, "CreateListener", listenerBody, "member")["ListenerArn"]
	require.NotEmpty(t, listenerARN, "CreateListener must report ListenerArn")

	created := elbWireAssertMembers(t, "CreateRule",
		elbWire(t, p, ctx, "CreateRule", map[string]string{
			"ListenerArn":                         listenerARN,
			"Priority":                            "10",
			"Conditions.member.1.Field":           "path-pattern",
			"Conditions.member.1.Values.member.1": "/api/*",
			"Actions.member.1.Type":               "forward",
			"Actions.member.1.TargetGroupArn":     tgARN,
		}), "member", elbWireRuleMembers)
	assert.Equal(t, "/api/*", created["Conditions/member/Values/member"],
		"API_RuleCondition.Values is a list inside the condition, two elements below the wrapper")

	arn := created["RuleArn"]
	require.NotEmpty(t, arn, "CreateRule must report RuleArn")
	key := elbWireOnlyKey(t, state, "rule")

	elbWireScope(t, state, key)
	elbWireTag(t, p, ctx, state, arn, key)

	described := elbWireAssertMembers(t, "DescribeRules",
		elbWire(t, p, ctx, "DescribeRules", map[string]string{"RuleArns.member.1": arn}),
		"member", elbWireRuleMembers)
	assert.Equal(t, "10", described["Priority"], "DescribeRules must report the rule's priority")

	// SetRulePriorities answered an empty Rules list until #756, the same defect ModifyTargetGroup
	// carried, so the new priority is asserted as well as the membership.
	repriced := elbWireAssertMembers(t, "SetRulePriorities",
		elbWire(t, p, ctx, "SetRulePriorities", map[string]string{
			"RulePriorities.member.1.RuleArn":  arn,
			"RulePriorities.member.1.Priority": "5",
		}), "member", elbWireRuleMembers)
	assert.Equal(t, "5", repriced["Priority"],
		"SetRulePriorities must report the repriced rule, not an empty list")
	assert.Equal(t, arn, repriced["RuleArn"], "SetRulePriorities must report the repriced ARN")
}

// elbWireClassicParams is a classic CreateLoadBalancer carrying one of every list member the shape
// publishes and substrate persists, so no wrapper is absent for want of an input.
//
// It carries a tag as well, which is the only way a classic record comes to hold one inside this
// harness: elbResourceKindFromARN refuses a classic ARN, so ELBv2's AddTags cannot reach it. The tag
// is a second absence anchor — Tags is published by no classic response shape, and a record that
// never held one could not prove that.
func elbWireClassicParams(name string) map[string]string {
	return map[string]string{
		"Version":                             elbClassicVersion,
		"LoadBalancerName":                    name,
		"Scheme":                              "internet-facing",
		"Tags.member.1.Key":                   "wire",
		"Tags.member.1.Value":                 "anchor",
		"Listeners.member.1.Protocol":         "HTTP",
		"Listeners.member.1.LoadBalancerPort": "80",
		"Listeners.member.1.InstanceProtocol": "HTTP",
		"Listeners.member.1.InstancePort":     "8080",
		"AvailabilityZones.member.1":          elbWireRegion + "a",
		"Subnets.member.1":                    "subnet-wire",
		"SecurityGroups.member.1":             "sg-wire",
	}
}

func TestELBWire_ClassicResponsesCarryOnlyPublishedMembers(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	elbWire(t, p, ctx, "CreateLoadBalancer", elbWireClassicParams("wire-classic"))
	key := elbWireKey("classic_lb", "wire-classic")

	// The classic record cannot be tagged through ELBv2's AddTags at all: elbResourceKindFromARN
	// refuses a classic ARN by design, which is why this anchors on the account and Region the record
	// persists rather than on ever_tagged. The subtest below reaches it through the one door that can
	// stamp it.
	elbWireScope(t, state, key)

	described := elbWireAssertMembers(t, "DescribeLoadBalancers",
		elbWire(t, p, ctx, "DescribeLoadBalancers", map[string]string{
			"Version":                    elbClassicVersion,
			"LoadBalancerNames.member.1": "wire-classic",
		}), "member", elbWireClassicMembers)
	elbWireTime(t, "DescribeLoadBalancers", described, "CreatedTime")
	assert.Equal(t, "wire-classic", described["LoadBalancerName"])
	assert.Equal(t, "HTTP", described["ListenerDescriptions/member/Listener/Protocol"],
		"the listener is nested inside a ListenerDescription, which is the published depth")

	t.Run("a tag stamped through the tagging API cannot reach the body", func(t *testing.T) {
		// The Resource Groups Tagging API is the only writer that reaches a classic record, and it
		// edits the stored JSON through mergeResourceTags — so this is the only way to make
		// ever_tagged exist on the record whose body is then re-read. A full server rather than the
		// plugin alone, because the two APIs have to share one state store.
		ts := elbTaggingServer(t)
		dns := elbClassicCreate(t, ts.URL, "wire-classic-tagged", map[string]string{
			"Scheme": "internet-facing",
		})
		require.NotEmpty(t, dns)
		arn := "arn:aws:elasticloadbalancing:" + elbWireRegion + ":" + elbWireAccount +
			":loadbalancer/wire-classic-tagged"
		taggingTagResources(t, ts, arn, map[string]string{"wire": "anchor"})

		status, body := elbClassicRawBody(t, ts.URL, map[string]string{
			"Action":                     "DescribeLoadBalancers",
			"Version":                    elbClassicVersion,
			"LoadBalancerNames.member.1": "wire-classic-tagged",
		})
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, body, "wire-classic-tagged", "DescribeLoadBalancers must report the record")
		assert.NotContains(t, body, "ever_tagged", "a stamped ever_tagged must not reach the body")
		assert.NotContains(t, body, "AccountID", "AccountID has no published home on any ELB shape")
		assert.NotContains(t, body, "<Region>", "Region has no published home on any ELB shape")
	})
}

// TestELBWire_ProjectionLeavesTheRecordIntact is the other half of the pin: the fifteen baseline
// lines stay declared, so every one of them must still be persisted after a projection has dropped
// it from the body.
//
// This is what keeps scanELBKind and the tagging API's GetResources working, and it is the reason the
// lines are not removed from scripts/wire-bookkeeping-baseline.txt — ecr_wire.go's replay rule:
// `json:"-"` and retyping a field in place both change the format of every recorded run.
func TestELBWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupELBWirePlugin(t)

	lbARN := elbWireCreateLB(t, p, ctx, "wire-alb")
	tgARN := elbWireCreateTG(t, p, ctx, "wire-tg")
	listenerBody := elbWire(t, p, ctx, "CreateListener", map[string]string{
		"LoadBalancerArn":                        lbARN,
		"Protocol":                               "HTTP",
		"Port":                                   "80",
		"DefaultActions.member.1.Type":           "forward",
		"DefaultActions.member.1.TargetGroupArn": tgARN,
	})
	listenerARN := elbWireMembers(t, "CreateListener", listenerBody, "member")["ListenerArn"]
	ruleBody := elbWire(t, p, ctx, "CreateRule", map[string]string{
		"ListenerArn":                         listenerARN,
		"Priority":                            "10",
		"Conditions.member.1.Field":           "path-pattern",
		"Conditions.member.1.Values.member.1": "/api/*",
		"Actions.member.1.Type":               "forward",
		"Actions.member.1.TargetGroupArn":     tgARN,
	})
	ruleARN := elbWireMembers(t, "CreateRule", ruleBody, "member")["RuleArn"]
	elbWire(t, p, ctx, "CreateLoadBalancer", elbWireClassicParams("wire-classic"))

	for _, tc := range []struct {
		name string
		key  string
		arn  string
	}{
		{"load balancer", elbWireKey("lb", "wire-alb"), lbARN},
		{"target group", elbWireKey("tg", "wire-tg"), tgARN},
		{"listener", elbWireOnlyKey(t, state, "listener"), listenerARN},
		{"rule", elbWireOnlyKey(t, state, "rule"), ruleARN},
		{"classic load balancer", elbWireKey("classic_lb", "wire-classic"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := elbWireRecord(t, state, tc.key)
			assert.Equal(t, `"`+elbWireAccount+`"`, string(record["AccountID"]), "AccountID")
			assert.Equal(t, `"`+elbWireRegion+`"`, string(record["Region"]), "Region")

			// Tags and ever_tagged are both `,omitempty`, so each has to be made to exist before it
			// can be asserted. The classic record was created carrying a tag, which is the only door
			// that reaches it here; the four ELBv2 kinds are tagged through AddTags, which is also
			// what sets ever_tagged — elbTagsForCreate deliberately does not (#938).
			if tc.arn == "" {
				assert.Contains(t, record, "Tags", "Tags")
				return
			}
			elbWireTag(t, p, ctx, state, tc.arn, tc.key)
			assert.Contains(t, elbWireRecord(t, state, tc.key), "Tags", "Tags")
		})
	}
}
