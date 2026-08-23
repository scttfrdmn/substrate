package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The authorization decision for ELBv2 (#748), which had two defects at once.
//
// Every ELB request was decided against the literal string "*" — the default arm of
// buildResourceARN — so a policy scoped to one load balancer's ARN matched nothing:
// resourceMatches passes the *statement's* Resource to globMatch as the pattern, so a
// request resource of "*" matches only a statement whose Resource itself begins with "*".
// An ARN-scoped Allow therefore denied every call and an ARN-scoped Deny was inert.
//
// And a tagged create was authorized as the creating action alone, so AWS's documented
// tag-on-create grant — which is what the bundled ELBTaggingPolicy statement *is* —
// permitted more than it says.

const (
	elbAuthzAccount = "123456789012"
	elbAuthzRegion  = "us-east-1"
)

// The ARNs the fixture's resources carry, in substrate's own shapes. The listener and rule
// ARNs nest under the load balancer's (#774); the tagging code accepts AWS's flat spelling
// too, which elb_tags_test.go pins.
var (
	elbAuthzLBARN       = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":loadbalancer/app/web/0abc111"
	elbAuthzTGARN       = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":targetgroup/web-tg/0def222"
	elbAuthzListenerARN = elbAuthzLBARN + "/listener/0aaa333"
	elbAuthzRuleARN     = elbAuthzListenerARN + "/rule/0bbb444"

	// The type wildcards a tagged create's second pass is authorized against, which are
	// AWS's ARN resource types rather than substrate's nesting.
	elbAuthzLBWildcard       = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":loadbalancer/*"
	elbAuthzTGWildcard       = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":targetgroup/*"
	elbAuthzListenerWildcard = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":listener/*"
	elbAuthzRuleWildcard     = "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":listener-rule/*"
)

// elbAuthzFixture is an ELB state store plus an AuthController over the same store, which
// is what makes a resource-tag condition testable: the tag has to travel from the elb
// namespace into the condition context under the ARN it belongs to.
type elbAuthzFixture struct {
	state emulator.StateManager
	auth  *emulator.AuthController
	user  string
}

// newELBAuthzFixture builds a user carrying one policy plus a load balancer, target group,
// listener and rule, each optionally tagged.
//
// Records are written directly rather than through the plugin: the decision reads state,
// and a create call would drag its own authorization into the fixture for it.
func newELBAuthzFixture(t *testing.T, user string, doc emulator.PolicyDocument) *elbAuthzFixture {
	t.Helper()
	policyARN := "arn:aws:iam::" + elbAuthzAccount + ":policy/ELB-" + user
	state := newAuthTestState(t, user, policyARN, doc)
	f := &elbAuthzFixture{
		state: state,
		auth:  emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false)),
		user:  user,
	}
	f.putLoadBalancer(t, nil)
	f.putTargetGroup(t, nil)
	f.putListener(t, nil)
	f.putRule(t, nil)
	return f
}

// elbAuthzScope is the account/region segment every ELB state key carries.
func elbAuthzScope() string { return elbAuthzAccount + "/" + elbAuthzRegion }

// put writes one ELB record under key.
func (f *elbAuthzFixture) put(t *testing.T, key string, record any) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	if err := f.state.Put(context.Background(), "elb", key, raw); err != nil { //nolint:contextcheck
		t.Fatalf("store %s: %v", key, err)
	}
}

func (f *elbAuthzFixture) putLoadBalancer(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "lb:"+elbAuthzScope()+"/web", emulator.ELBLoadBalancer{
		Name:      "web",
		ARN:       elbAuthzLBARN,
		Type:      "application",
		AccountID: elbAuthzAccount,
		Region:    elbAuthzRegion,
		Tags:      elbAuthzTags(tags),
	})
}

func (f *elbAuthzFixture) putTargetGroup(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "tg:"+elbAuthzScope()+"/web-tg", emulator.ELBTargetGroup{
		Name:      "web-tg",
		ARN:       elbAuthzTGARN,
		Protocol:  "HTTP",
		Port:      80,
		AccountID: elbAuthzAccount,
		Region:    elbAuthzRegion,
		Tags:      elbAuthzTags(tags),
	})
}

func (f *elbAuthzFixture) putListener(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "listener:"+elbAuthzScope()+"/0aaa333", emulator.ELBListener{
		ARN:             elbAuthzListenerARN,
		LoadBalancerARN: elbAuthzLBARN,
		Protocol:        "HTTP",
		Port:            80,
		AccountID:       elbAuthzAccount,
		Region:          elbAuthzRegion,
		Suffix:          "0aaa333",
		Tags:            elbAuthzTags(tags),
	})
}

func (f *elbAuthzFixture) putRule(t *testing.T, tags map[string]string) {
	t.Helper()
	f.put(t, "rule:"+elbAuthzScope()+"/0bbb444", emulator.ELBRule{
		ARN:         elbAuthzRuleARN,
		ListenerARN: elbAuthzListenerARN,
		Priority:    "10",
		AccountID:   elbAuthzAccount,
		Region:      elbAuthzRegion,
		Suffix:      "0bbb444",
		Tags:        elbAuthzTags(tags),
	})
}

// elbAuthzTags converts a map to the []ELBTag shape the records store.
func elbAuthzTags(tags map[string]string) []emulator.ELBTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]emulator.ELBTag, 0, len(tags))
	for k, v := range tags {
		out = append(out, emulator.ELBTag{Key: k, Value: v})
	}
	return out
}

// call runs one ELB request through the authorization decision.
//
// Params, not Body: ELBv2 is a query-protocol service, so every resource a request names
// arrives as a form parameter.
func (f *elbAuthzFixture) call(t *testing.T, operation string, params map[string]string) error {
	t.Helper()
	reqCtx := newAuthTestReqCtx("arn:aws:iam::" + elbAuthzAccount + ":user/" + f.user)
	return f.auth.CheckAccess(reqCtx, &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: operation,
		Path:      "/",
		Params:    params,
	})
}

// setPolicy rewrites the user's single attached policy so its statements are exactly those
// given, which is how a test scopes a decision to a list of ARNs rather than to "*".
func (f *elbAuthzFixture) setPolicy(t *testing.T, statements ...emulator.PolicyStatement) {
	t.Helper()
	arn := "arn:aws:iam::" + elbAuthzAccount + ":policy/ELB-" + f.user
	pol := emulator.IAMPolicy{
		PolicyName:       "elb",
		PolicyID:         "ANPAELB",
		ARN:              arn,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         emulator.PolicyDocument{Version: "2012-10-17", Statement: statements},
	}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", emulator.IAMPolicyKeyForTest(arn), raw); err != nil { //nolint:contextcheck
		t.Fatalf("store policy: %v", err)
	}
}

// attachManagedPolicy points the user's attached-policy list at an AWS-managed ARN, so a
// test can run one of the policies substrate bundles rather than a hand-written copy of it.
func (f *elbAuthzFixture) attachManagedPolicy(t *testing.T, arn string) {
	t.Helper()
	raw, err := json.Marshal([]string{arn})
	if err != nil {
		t.Fatalf("marshal attached policies: %v", err)
	}
	key := emulator.IAMAttachedPoliciesKeyForTest(elbAuthzAccount, "user", f.user)
	if err := f.state.Put(context.Background(), "iam", key, raw); err != nil { //nolint:contextcheck
		t.Fatalf("store attached policies: %v", err)
	}
}

// elbAuthzAllow builds one Allow statement over the given action and resources.
func elbAuthzAllow(action string, resources ...string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{action},
		Resource: emulator.StringOrSlice(resources),
	}
}

// elbAuthzAllowCond is elbAuthzAllow with a condition block.
func elbAuthzAllowCond(action string, resources []string,
	cond map[string]map[string]emulator.StringOrSlice) emulator.PolicyStatement {
	stmt := elbAuthzAllow(action, resources...)
	stmt.Condition = cond
	return stmt
}

// elbCreateActionAllow is AWS's documented tagging grant, and the shape of the bundled
// ELBTaggingPolicy statement: elasticloadbalancing:AddTags, but only in the context of the
// named creating action.
func elbCreateActionAllow(createActions ...string) emulator.PolicyStatement {
	return elbAuthzAllowCond("elasticloadbalancing:AddTags", []string{"*"},
		map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"elasticloadbalancing:CreateAction": emulator.StringOrSlice(createActions)},
		})
}

// elbAuthzTagParams returns the Tags.member.N params carrying kv pairs, in the wire
// spelling ELB's query protocol uses.
func elbAuthzTagParams(kv ...string) map[string]string {
	out := make(map[string]string, len(kv))
	for i := 0; i+1 < len(kv); i += 2 {
		n := strconv.Itoa(i/2 + 1)
		out["Tags.member."+n+".Key"] = kv[i]
		out["Tags.member."+n+".Value"] = kv[i+1]
	}
	return out
}

// elbAuthzParams merges extra maps into a copy of base.
func elbAuthzParams(base map[string]string, extra ...map[string]string) map[string]string {
	out := make(map[string]string, len(base)+4)
	for k, v := range base {
		out[k] = v
	}
	for _, m := range extra {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// TestELB_Authz_ResourceScopedPolicyMatchesTheNamedARN is the first half of #748: a policy
// naming one resource's ARN now decides the request that names it.
//
// Before this the request resource was the literal "*", so each of these rows denied — the
// Allow never matched the resource it was written about.
func TestELB_Authz_ResourceScopedPolicyMatchesTheNamedARN(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		params    map[string]string
		action    string
		resource  string
	}{
		{
			name:      "DeleteLoadBalancer on LoadBalancerArn",
			operation: "DeleteLoadBalancer",
			params:    map[string]string{"LoadBalancerArn": elbAuthzLBARN},
			action:    "elasticloadbalancing:DeleteLoadBalancer",
			resource:  elbAuthzLBARN,
		},
		{
			name:      "RegisterTargets on TargetGroupArn",
			operation: "RegisterTargets",
			params:    map[string]string{"TargetGroupArn": elbAuthzTGARN},
			action:    "elasticloadbalancing:RegisterTargets",
			resource:  elbAuthzTGARN,
		},
		{
			name:      "ModifyListener on ListenerArn",
			operation: "ModifyListener",
			params:    map[string]string{"ListenerArn": elbAuthzListenerARN},
			action:    "elasticloadbalancing:ModifyListener",
			resource:  elbAuthzListenerARN,
		},
		{
			name:      "DeleteRule on RuleArn",
			operation: "DeleteRule",
			params:    map[string]string{"RuleArn": elbAuthzRuleARN},
			action:    "elasticloadbalancing:DeleteRule",
			resource:  elbAuthzRuleARN,
		},
		{
			name:      "DescribeTags on ResourceArns.member.N",
			operation: "DescribeTags",
			params:    map[string]string{"ResourceArns.member.1": elbAuthzLBARN},
			action:    "elasticloadbalancing:DescribeTags",
			resource:  elbAuthzLBARN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newELBAuthzFixture(t, "scoped", emulator.PolicyDocument{})
			f.setPolicy(t, elbAuthzAllow(tt.action, tt.resource))
			assert.NoError(t, f.call(t, tt.operation, tt.params),
				"a policy naming %s must permit the request that names it", tt.resource)

			// The other direction: the same grant scoped to a different ARN must not
			// permit it, which is what proves the resource is the one being compared.
			f.setPolicy(t, elbAuthzAllow(tt.action, elbAuthzTGARN+"-other"))
			require.Error(t, f.call(t, tt.operation, tt.params))
		})
	}
}

// TestELB_Authz_DenyOnOneNamedARNRefusesTheWholeRequest pins the multi-resource rule for the
// three tagging operations, which name up to 20 ARNs: every one must be allowed.
func TestELB_Authz_DenyOnOneNamedARNRefusesTheWholeRequest(t *testing.T) {
	f := newELBAuthzFixture(t, "multi", emulator.PolicyDocument{})
	f.setPolicy(t,
		elbAuthzAllow("elasticloadbalancing:*", "*"),
		emulator.PolicyStatement{
			Effect:   "Deny",
			Action:   emulator.StringOrSlice{"elasticloadbalancing:RemoveTags"},
			Resource: emulator.StringOrSlice{elbAuthzTGARN},
		},
	)

	// The load balancer alone is permitted.
	assert.NoError(t, f.call(t, "RemoveTags", map[string]string{
		"ResourceArns.member.1": elbAuthzLBARN,
		"TagKeys.member.1":      "env",
	}))

	// Naming the denied target group alongside it refuses the whole request, and the
	// denial names the resource that failed.
	err := f.call(t, "RemoveTags", map[string]string{
		"ResourceArns.member.1": elbAuthzLBARN,
		"ResourceArns.member.2": elbAuthzTGARN,
		"TagKeys.member.1":      "env",
	})
	require.Error(t, err)
	assert.Equal(t, elbAuthzTGARN, deniedResource(t, err))
}

// TestELB_Authz_UnresolvableARNIsStillTheResourceDecidedAbout pins that a bogus ARN is not
// silently replaced by "*": substituting it would let it reach a statement scoped to "*"
// that the real ARN would not have matched.
func TestELB_Authz_UnresolvableARNIsStillTheResourceDecidedAbout(t *testing.T) {
	f := newELBAuthzFixture(t, "ghost", emulator.PolicyDocument{})
	f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:AddTags", elbAuthzLBARN))

	ghost := "arn:aws:elasticloadbalancing:" + elbAuthzRegion + ":" + elbAuthzAccount + ":loadbalancer/app/ghost/0zzz999"
	err := f.call(t, "AddTags", elbAuthzParams(
		map[string]string{"ResourceArns.member.1": ghost},
		elbAuthzTagParams("env", "prod"),
	))
	require.Error(t, err)
	assert.Equal(t, ghost, deniedResource(t, err))
}

// TestELB_Authz_ResourceTagConditions pins both prefixes a resource's tags are reported
// under: the global aws:ResourceTag/ and the elasticloadbalancing:ResourceTag/ the ELB user
// guide publishes ("All mutating actions support this condition key").
func TestELB_Authz_ResourceTagConditions(t *testing.T) {
	for _, prefix := range []string{"aws:ResourceTag/", "elasticloadbalancing:ResourceTag/"} {
		t.Run(prefix, func(t *testing.T) {
			f := newELBAuthzFixture(t, "tagged", emulator.PolicyDocument{})
			f.putLoadBalancer(t, map[string]string{"env": "prod"})
			f.setPolicy(t, elbAuthzAllowCond("elasticloadbalancing:DeleteLoadBalancer",
				[]string{"*"},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {prefix + "env": {"prod"}},
				}))

			assert.NoError(t, f.call(t, "DeleteLoadBalancer",
				map[string]string{"LoadBalancerArn": elbAuthzLBARN}),
				"the resource's own tag must satisfy a condition written under %s", prefix)

			// The target group carries no tags, so the same statement must not permit it —
			// which is what shows the tags travel with the ARN they belong to.
			f.setPolicy(t, elbAuthzAllowCond("elasticloadbalancing:DeleteTargetGroup",
				[]string{"*"},
				map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {prefix + "env": {"prod"}},
				}))
			require.Error(t, f.call(t, "DeleteTargetGroup",
				map[string]string{"TargetGroupArn": elbAuthzTGARN}))
		})
	}
}

// TestELB_Authz_TaggedCreateRequiresTheTaggingGrant is the second half of #748. AWS: "If
// tags are specified in the resource-creating action, additional authorization is required
// on the elasticloadbalancing:AddTags action to verify if users have permissions to apply
// tags to the resources being created".
func TestELB_Authz_TaggedCreateRequiresTheTaggingGrant(t *testing.T) {
	tests := []struct {
		operation string
		wildcard  string
	}{
		{operation: "CreateLoadBalancer", wildcard: elbAuthzLBWildcard},
		{operation: "CreateTargetGroup", wildcard: elbAuthzTGWildcard},
		{operation: "CreateListener", wildcard: elbAuthzListenerWildcard},
		{operation: "CreateRule", wildcard: elbAuthzRuleWildcard},
	}

	for _, tt := range tests {
		t.Run(tt.operation, func(t *testing.T) {
			f := newELBAuthzFixture(t, "creator", emulator.PolicyDocument{})
			f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:"+tt.operation, "*"))

			// AWS: "a user that has permissions to create a resource … does not require
			// permissions to use the elasticloadbalancing:AddTags action if no tags are
			// specified in the request."
			assert.NoError(t, f.call(t, tt.operation, map[string]string{"Name": "new"}),
				"an untagged create must not require the tagging grant")

			// The same create carrying tags is refused, and the denial names the tagging
			// action against the created type's wildcard — which is how a caller learns
			// it is the tagging pass that failed rather than the create.
			err := f.call(t, tt.operation, elbAuthzParams(
				map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod")))
			require.Error(t, err)
			assert.Equal(t, "elasticloadbalancing:AddTags", deniedActionNamed(t, err))
			assert.Equal(t, tt.wildcard, deniedResource(t, err))

			// Adding the grant permits it.
			f.setPolicy(t,
				elbAuthzAllow("elasticloadbalancing:"+tt.operation, "*"),
				elbAuthzAllow("elasticloadbalancing:AddTags", "*"),
			)
			assert.NoError(t, f.call(t, tt.operation, elbAuthzParams(
				map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod"))))
		})
	}
}

// TestELB_Authz_CreateActionScopesTheTaggingGrant runs the bundled ELBTaggingPolicy's own
// statement shape, which is the reason elasticloadbalancing:CreateAction exists.
func TestELB_Authz_CreateActionScopesTheTaggingGrant(t *testing.T) {
	f := newELBAuthzFixture(t, "scoper", emulator.PolicyDocument{})
	f.setPolicy(t,
		elbAuthzAllow("elasticloadbalancing:CreateLoadBalancer", "*"),
		elbAuthzAllow("elasticloadbalancing:CreateTargetGroup", "*"),
		elbCreateActionAllow("CreateTargetGroup", "CreateRule", "CreateListener", "CreateLoadBalancer"),
	)

	tagged := elbAuthzTagParams("env", "prod")
	assert.NoError(t, f.call(t, "CreateLoadBalancer",
		elbAuthzParams(map[string]string{"Name": "new"}, tagged)),
		"a create named by the condition must be permitted to tag")

	// The whole point of the key: a standalone AddTags carries no
	// elasticloadbalancing:CreateAction, so the same grant must not permit it.
	err := f.call(t, "AddTags", elbAuthzParams(
		map[string]string{"ResourceArns.member.1": elbAuthzLBARN}, tagged))
	require.Error(t, err, "a grant conditioned on CreateAction permitted standalone tagging")

	// A create the condition does not name is refused too, which is what makes the four
	// values in the bundled statement mean something.
	f.setPolicy(t,
		elbAuthzAllow("elasticloadbalancing:CreateTargetGroup", "*"),
		elbCreateActionAllow("CreateLoadBalancer"),
	)
	require.Error(t, f.call(t, "CreateTargetGroup",
		elbAuthzParams(map[string]string{"Name": "new"}, tagged)))
}

// TestELB_Authz_CreateActionIsAbsentOutsideACreate pins the key's absence, which a Null
// condition is the only way to observe.
func TestELB_Authz_CreateActionIsAbsentOutsideACreate(t *testing.T) {
	nullTrue := func(action string) emulator.PolicyStatement {
		return elbAuthzAllowCond(action, []string{"*"},
			map[string]map[string]emulator.StringOrSlice{
				"Null": {"elasticloadbalancing:CreateAction": {"true"}},
			})
	}

	f := newELBAuthzFixture(t, "nuller", emulator.PolicyDocument{})

	// A standalone AddTags carries no CreateAction, so a grant requiring its absence
	// permits it.
	f.setPolicy(t, nullTrue("elasticloadbalancing:AddTags"))
	assert.NoError(t, f.call(t, "AddTags", elbAuthzParams(
		map[string]string{"ResourceArns.member.1": elbAuthzLBARN},
		elbAuthzTagParams("env", "prod"))))

	// The tagging pass of a tagged create does carry it, so the same grant refuses that.
	f.setPolicy(t,
		elbAuthzAllow("elasticloadbalancing:CreateLoadBalancer", "*"),
		nullTrue("elasticloadbalancing:AddTags"),
	)
	require.Error(t, f.call(t, "CreateLoadBalancer", elbAuthzParams(
		map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod"))),
		"the tagging pass of a tagged create carried no elasticloadbalancing:CreateAction")
}

// TestELB_Authz_RequestTagAndTagKeys pins the two request-level keys the new addRequestTags
// arm produces, which a "may only apply approved tags" policy is written against.
func TestELB_Authz_RequestTagAndTagKeys(t *testing.T) {
	t.Run("aws:RequestTag on AddTags", func(t *testing.T) {
		f := newELBAuthzFixture(t, "reqtag", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllowCond("elasticloadbalancing:AddTags", []string{"*"},
			map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {"aws:RequestTag/env": {"prod"}},
			}))

		assert.NoError(t, f.call(t, "AddTags", elbAuthzParams(
			map[string]string{"ResourceArns.member.1": elbAuthzLBARN},
			elbAuthzTagParams("env", "prod"))))
		require.Error(t, f.call(t, "AddTags", elbAuthzParams(
			map[string]string{"ResourceArns.member.1": elbAuthzLBARN},
			elbAuthzTagParams("env", "dev"))))
	})

	t.Run("aws:TagKeys on a tagged create", func(t *testing.T) {
		f := newELBAuthzFixture(t, "tagkeys", emulator.PolicyDocument{})
		f.setPolicy(t,
			elbAuthzAllow("elasticloadbalancing:CreateTargetGroup", "*"),
			elbAuthzAllowCond("elasticloadbalancing:AddTags", []string{"*"},
				map[string]map[string]emulator.StringOrSlice{
					"ForAllValues:StringEquals": {"aws:TagKeys": {"env", "team"}},
				}),
		)

		assert.NoError(t, f.call(t, "CreateTargetGroup", elbAuthzParams(
			map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod", "team", "platform"))))
		require.Error(t, f.call(t, "CreateTargetGroup", elbAuthzParams(
			map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod", "cost-center", "42"))))
	})

	// RemoveTags names keys and supplies no values, and those keys still reach aws:TagKeys
	// — the reading that makes a "may only remove approved tags" policy expressible.
	t.Run("aws:TagKeys on RemoveTags", func(t *testing.T) {
		f := newELBAuthzFixture(t, "removekeys", emulator.PolicyDocument{})
		f.setPolicy(t, elbAuthzAllowCond("elasticloadbalancing:RemoveTags", []string{"*"},
			map[string]map[string]emulator.StringOrSlice{
				"ForAllValues:StringEquals": {"aws:TagKeys": {"env"}},
			}))

		assert.NoError(t, f.call(t, "RemoveTags", map[string]string{
			"ResourceArns.member.1": elbAuthzLBARN,
			"TagKeys.member.1":      "env",
		}))
		require.Error(t, f.call(t, "RemoveTags", map[string]string{
			"ResourceArns.member.1": elbAuthzLBARN,
			"TagKeys.member.1":      "env",
			"TagKeys.member.2":      "owner",
		}))
	})
}

// TestELB_Authz_TaggedCreateHonoursThePermissionBoundary pins that the second pass is
// bounded like the first: a boundary that does not admit the tagging action refuses a tagged
// create even when the attached policy grants it.
func TestELB_Authz_TaggedCreateHonoursThePermissionBoundary(t *testing.T) {
	f := newELBAuthzFixture(t, "bounded", emulator.PolicyDocument{})
	f.setPolicy(t,
		elbAuthzAllow("elasticloadbalancing:CreateLoadBalancer", "*"),
		elbAuthzAllow("elasticloadbalancing:AddTags", "*"),
	)
	elbSetBoundary(t, f, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{elbAuthzAllow("elasticloadbalancing:CreateLoadBalancer", "*")},
	})

	assert.NoError(t, f.call(t, "CreateLoadBalancer", map[string]string{"Name": "new"}),
		"the untagged create is inside the boundary")

	err := f.call(t, "CreateLoadBalancer", elbAuthzParams(
		map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod")))
	require.Error(t, err)
	assert.Equal(t, "elasticloadbalancing:AddTags", deniedActionNamed(t, err))
}

// elbSetBoundary attaches a permission boundary to the fixture's user, which is how a
// boundary reaches CheckAccess: it is read off the IAM entity, not from the attached-policy
// list.
func elbSetBoundary(t *testing.T, f *elbAuthzFixture, doc emulator.PolicyDocument) {
	t.Helper()
	arn := "arn:aws:iam::" + elbAuthzAccount + ":policy/Boundary-" + f.user
	pol := emulator.IAMPolicy{
		PolicyName:       "boundary",
		PolicyID:         "ANPABOUNDELB",
		ARN:              arn,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         doc,
	}
	raw, err := json.Marshal(pol)
	if err != nil {
		t.Fatalf("marshal boundary: %v", err)
	}
	if err := f.state.Put(context.Background(), "iam", emulator.IAMPolicyKeyForTest(arn), raw); err != nil { //nolint:contextcheck
		t.Fatalf("store boundary: %v", err)
	}
	user := emulator.IAMUser{
		UserName:            f.user,
		UserID:              "AIDATEST",
		ARN:                 "arn:aws:iam::" + elbAuthzAccount + ":user/" + f.user,
		Path:                "/",
		PermissionsBoundary: &emulator.IAMAttachedPolicy{PolicyARN: arn, PolicyName: "boundary"},
	}
	userRaw, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("marshal user: %v", err)
	}
	key := emulator.IAMUserKeyForTest(elbAuthzAccount, f.user)
	if err := f.state.Put(context.Background(), "iam", key, userRaw); err != nil { //nolint:contextcheck
		t.Fatalf("store user: %v", err)
	}
}

// TestELB_Authz_BundledPolicyAllowsATaggedCreate runs the AWS-authored policy substrate
// bundles, whose ELBTaggingPolicy statement is the only citable-in-code source for the
// condition key. A tagged create under it must succeed end to end.
func TestELB_Authz_BundledPolicyAllowsATaggedCreate(t *testing.T) {
	f := newELBAuthzFixture(t, "bundled", emulator.PolicyDocument{})
	f.attachManagedPolicy(t, "arn:aws:iam::aws:policy/AmazonECS_FullAccess")

	for _, operation := range []string{"CreateLoadBalancer", "CreateTargetGroup", "CreateListener", "CreateRule"} {
		assert.NoError(t, f.call(t, operation, elbAuthzParams(
			map[string]string{"Name": "new"}, elbAuthzTagParams("env", "prod"))), operation)
	}
}

// TestELB_Authz_DescribesByNameStayAtStar pins the deliberate absence: Names.member.N is not
// resolved to an ARN, because whether ELB's describes support resource-level permissions
// could not be verified. Leaving them at "*" is the direction that cannot invent a grant.
func TestELB_Authz_DescribesByNameStayAtStar(t *testing.T) {
	f := newELBAuthzFixture(t, "bynames", emulator.PolicyDocument{})
	f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:DescribeLoadBalancers", "*"))

	assert.NoError(t, f.call(t, "DescribeLoadBalancers", map[string]string{"Names.member.1": "web"}))

	// An ARN-scoped grant does not cover it, because the request resource is "*" and not
	// the load balancer the name resolves to.
	f.setPolicy(t, elbAuthzAllow("elasticloadbalancing:DescribeLoadBalancers", elbAuthzLBARN))
	err := f.call(t, "DescribeLoadBalancers", map[string]string{"Names.member.1": "web"})
	require.Error(t, err)
	assert.Equal(t, "*", deniedResource(t, err))
}
