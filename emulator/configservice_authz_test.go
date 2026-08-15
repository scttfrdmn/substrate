package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWS Config authorization (#580).
//
// Without a Config arm in buildResourceARN every Config request authorizes against
// "*", so a policy scoped to one rule admits an operation on every rule and a test
// asserting a denial passes for the wrong reason. That is a **false allow**, the one
// direction a privilege boundary must never fail in, and it is silent: the policy looks
// right, the test goes green, and the scoping does nothing.
//
// So the cases below pin two things. First, that a resource-scoped statement narrows —
// naming one rule in a policy's Resource element must not admit an operation against
// another. Second, that the tags a decision reads come from the resource the request
// names, so an ABAC policy over Config is enforceable at all.
//
// Operations that name a *list* of resources, and the delivery-channel operations that
// have no resource type, resolve to "*" deliberately, and that is asserted too: an
// arbitrary member of a list would be the false-allow direction again, and inventing a
// delivery-channel ARN would name a resource no real policy could.

// configAuthzFixture is a state store plus an AuthController over it, which is what
// makes a tag-gated boundary testable: the tag must travel out of the Config namespace
// into the condition context.
type configAuthzFixture struct {
	state emulator.StateManager
	auth  *emulator.AuthController
	ctx   *emulator.RequestContext
}

// newConfigAuthzFixture builds a user carrying one policy and an AuthController reading
// the same store.
func newConfigAuthzFixture(t *testing.T, user string, doc emulator.PolicyDocument) *configAuthzFixture {
	t.Helper()
	state := newAuthTestState(t, user, "arn:aws:iam::123456789012:policy/ConfigGate-"+user, doc)
	return &configAuthzFixture{
		state: state,
		auth:  emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false)),
		ctx:   newAuthTestReqCtx("arn:aws:iam::123456789012:user/" + user),
	}
}

// check runs one Config request through the authorization decision.
func (f *configAuthzFixture) check(t *testing.T, operation string, body map[string]any) error {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return f.auth.CheckAccess(f.ctx, &emulator.AWSRequest{
		Service:   "config",
		Operation: operation,
		Path:      "/",
		Body:      raw,
	})
}

// tag stores tags under a resource ARN, as a Put or a TagResource would.
func (f *configAuthzFixture) tag(t *testing.T, arn string, tags map[string]string) {
	t.Helper()
	raw, err := json.Marshal(tags)
	require.NoError(t, err)
	require.NoError(t, f.state.Put(t.Context(), emulator.ConfigServiceNamespaceForTest,
		emulator.CfgsvcTagsKeyForTest(arn), raw))
}

// configAuthzDenied reports whether err is the denial a Config caller sees. Config
// speaks JSON 1.1, so the code carries the "Exception" suffix.
func configAuthzDenied(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
	assert.Equal(t, http.StatusForbidden, awsErr.HTTPStatus)
	return true
}

// configAuthzCtx is the request context the ARN builders are called with, matching the
// one the fixture authorizes under.
func configAuthzCtx() *emulator.RequestContext {
	return newAuthTestReqCtx("arn:aws:iam::123456789012:user/anyone")
}

func TestConfigAuthz_AResourceScopedStatementNarrows(t *testing.T) {
	// The decisive case. A policy allowing DeleteConfigRule on one rule must deny it on
	// another — which requires the request to resolve to *that rule's* ARN. Falling
	// through to "*" makes both requests match, so the second assertion is the one that
	// fails without the Config arm in buildResourceARN.
	ctx := configAuthzCtx()
	mine := emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted")

	f := newConfigAuthzFixture(t, "cfgdeleter", emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"config:DeleteConfigRule"},
			Resource: emulator.StringOrSlice{mine},
		}},
	})

	assert.NoError(t, f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"}),
		"the rule the policy names is allowed")
	assert.True(t, configAuthzDenied(t,
		f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "ebs-encrypted"})),
		"a rule the policy does not name is denied")
}

func TestConfigAuthz_EachResourceKindResolvesToItsOwnARN(t *testing.T) {
	// The three ARN shapes, each resolved from the member its operation uses. The
	// recorder's and the pack's names are nested or differently spelled, so a single
	// "look for a name-shaped member" implementation would get at least one wrong.
	ctx := configAuthzCtx()

	for _, tc := range []struct {
		name, operation string
		body            map[string]any
		want            string
	}{
		{
			name:      "a recorder Put, whose name is nested and lowerCamel",
			operation: "PutConfigurationRecorder",
			body: map[string]any{"ConfigurationRecorder": map[string]any{
				"name": "main", "roleARN": "arn:aws:iam::123456789012:role/config",
			}},
			want: emulator.CfgsvcRecorderARNForTest(ctx, "main"),
		},
		{
			name:      "a recorder Put omitting the name, which defaults to default",
			operation: "PutConfigurationRecorder",
			body: map[string]any{"ConfigurationRecorder": map[string]any{
				"roleARN": "arn:aws:iam::123456789012:role/config",
			}},
			want: emulator.CfgsvcRecorderARNForTest(ctx, "default"),
		},
		{
			name:      "a Start, whose name is top-level",
			operation: "StartConfigurationRecorder",
			body:      map[string]any{"ConfigurationRecorderName": "main"},
			want:      emulator.CfgsvcRecorderARNForTest(ctx, "main"),
		},
		{
			name:      "a Stop",
			operation: "StopConfigurationRecorder",
			body:      map[string]any{"ConfigurationRecorderName": "main"},
			want:      emulator.CfgsvcRecorderARNForTest(ctx, "main"),
		},
		{
			name:      "a recorder Delete",
			operation: "DeleteConfigurationRecorder",
			body:      map[string]any{"ConfigurationRecorderName": "main"},
			want:      emulator.CfgsvcRecorderARNForTest(ctx, "main"),
		},
		{
			name:      "a rule Put, whose name is nested and UpperCamel",
			operation: "PutConfigRule",
			body:      map[string]any{"ConfigRule": map[string]any{"ConfigRuleName": "s3-encrypted"}},
			want:      emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"),
		},
		{
			name:      "a rule Delete",
			operation: "DeleteConfigRule",
			body:      map[string]any{"ConfigRuleName": "s3-encrypted"},
			want:      emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"),
		},
		{
			name:      "a per-rule compliance read",
			operation: "GetComplianceDetailsByConfigRule",
			body:      map[string]any{"ConfigRuleName": "s3-encrypted"},
			want:      emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"),
		},
		{
			name:      "a pack Put",
			operation: "PutConformancePack",
			body:      map[string]any{"ConformancePackName": "ops", "TemplateBody": "{}"},
			want:      emulator.CfgsvcPackARNForTest(ctx, "ops"),
		},
		{
			name:      "a pack Delete",
			operation: "DeleteConformancePack",
			body:      map[string]any{"ConformancePackName": "ops"},
			want:      emulator.CfgsvcPackARNForTest(ctx, "ops"),
		},
		{
			name:      "a pack compliance read",
			operation: "DescribeConformancePackCompliance",
			body:      map[string]any{"ConformancePackName": "ops"},
			want:      emulator.CfgsvcPackARNForTest(ctx, "ops"),
		},
		{
			name:      "a tag operation, which names its resource outright",
			operation: "TagResource",
			body: map[string]any{
				"ResourceArn": emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"),
				"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
			},
			want: emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.body)
			require.NoError(t, err)
			got := emulator.CfgsvcAuthzResourceARNForTest(ctx, &emulator.AWSRequest{
				Service: "config", Operation: tc.operation, Path: "/", Body: raw,
			})
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestConfigAuthz_AnOperationNamingNoSingleResourceIsStar(t *testing.T) {
	// "*" is the honest answer for a request whose resource cannot be named, and each of
	// these has a reason.
	ctx := configAuthzCtx()

	for _, tc := range []struct {
		name, operation string
		body            map[string]any
	}{
		{
			// The describes take name *lists*. Resolving to one member's ARN would let a
			// policy scoped to that member admit a call about all of them.
			name: "a describe over a list of rules", operation: "DescribeConfigRules",
			body: map[string]any{"ConfigRuleNames": []string{"a", "b"}},
		},
		{
			name: "a describe over a list of packs", operation: "DescribeConformancePacks",
			body: map[string]any{"ConformancePackNames": []string{"a", "b"}},
		},
		{
			name: "a describe of every recorder", operation: "DescribeConfigurationRecorders",
			body: map[string]any{},
		},
		{
			// **There is no delivery-channel resource type.** The Service Authorization
			// Reference gives all four channel operations an empty resource list, which is
			// how it spells "authorizes against * only". Synthesizing an ARN would name a
			// resource no real policy could match.
			name: "a channel Put", operation: "PutDeliveryChannel",
			body: map[string]any{"DeliveryChannel": map[string]any{
				"name": "default", "s3BucketName": "cfg-logs",
			}},
		},
		{
			name: "a channel Delete", operation: "DeleteDeliveryChannel",
			body: map[string]any{"DeliveryChannelName": "default"},
		},
		{
			name: "a channel describe", operation: "DescribeDeliveryChannels", body: map[string]any{},
		},
		{
			name: "a channel status describe", operation: "DescribeDeliveryChannelStatus",
			body: map[string]any{},
		},
		{
			// PutEvaluations carries no rule name at all — its ResultToken is the only
			// thing that says which rule. Decoding a token to authorize would hand a
			// caller control over the resource its own request is checked against.
			name: "PutEvaluations, which carries no rule name", operation: "PutEvaluations",
			body: map[string]any{"ResultToken": "substrate-config-rule:czMtZW5jcnlwdGVk"},
		},
		{
			// A rule update may name its rule by Id or Arn. Converting either would need a
			// state lookup that can miss — and a miss would authorize against a
			// *different* rule's ARN.
			name: "a rule Put naming its rule by ID", operation: "PutConfigRule",
			body: map[string]any{"ConfigRule": map[string]any{"ConfigRuleId": "config-rule-abc123"}},
		},
		{
			name: "an operation with an unparseable body", operation: "DeleteConfigRule",
			body: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte("not json")
			if tc.body != nil {
				var err error
				raw, err = json.Marshal(tc.body)
				require.NoError(t, err)
			}
			got := emulator.CfgsvcAuthzResourceARNForTest(ctx, &emulator.AWSRequest{
				Service: "config", Operation: tc.operation, Path: "/", Body: raw,
			})
			assert.Equal(t, "*", got)
		})
	}
}

func TestConfigAuthz_ResourceTagGatesTheResourceNamed(t *testing.T) {
	// The aws:ResourceTag half. A policy allowing DeleteConfigRule only on a rule
	// already tagged Owner=platform must allow it for that rule and deny it for another
	// — which requires the tags to be loaded from the Config namespace at decision time.
	// Without the addResourceTags arm the condition key is never populated, the Allow
	// never matches, and the *first* assertion fails: a boundary that denies everything
	// looks correct until someone legitimate is blocked.
	ctx := configAuthzCtx()
	f := newConfigAuthzFixture(t, "cfgabac",
		newABACPolicy("Allow", "config:DeleteConfigRule", "*", "aws:ResourceTag/Owner", "platform"))

	f.tag(t, emulator.CfgsvcRuleARNForTest(ctx, "mine"), map[string]string{"Owner": "platform"})
	f.tag(t, emulator.CfgsvcRuleARNForTest(ctx, "theirs"), map[string]string{"Owner": "security"})

	assert.NoError(t, f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "mine"}),
		"the matching resource tag is allowed")
	assert.True(t, configAuthzDenied(t,
		f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "theirs"})),
		"a rule tagged with a different Owner is denied")
	assert.True(t, configAuthzDenied(t,
		f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "untagged"})),
		"an untagged rule has no Owner at all, which cannot satisfy the condition")
}

func TestConfigAuthz_ResourceTagsComeFromTheResourceTheOperationNames(t *testing.T) {
	// The tags must come from the resource *this* operation names, not from whichever
	// Config resource happens to be tagged. A recorder and a rule tagged differently
	// must decide their own operations — merging them, or reading the wrong one, would
	// let a tag on one satisfy a condition written about the other.
	ctx := configAuthzCtx()
	f := newConfigAuthzFixture(t, "cfgmixed", emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"config:*"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"StringEquals": {"aws:ResourceTag/Owner": {"platform"}},
			},
		}},
	})

	f.tag(t, emulator.CfgsvcRecorderARNForTest(ctx, "default"), map[string]string{"Owner": "platform"})
	f.tag(t, emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted"), map[string]string{"Owner": "security"})

	assert.NoError(t, f.check(t, "StopConfigurationRecorder",
		map[string]any{"ConfigurationRecorderName": "default"}),
		"the recorder's own tag decides the recorder's operation")
	assert.True(t, configAuthzDenied(t,
		f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"})),
		"the recorder's tag does not satisfy a condition about the rule")
}

func TestConfigAuthz_RequestTagGatesTagResource(t *testing.T) {
	// The aws:RequestTag half: a policy permitting tagging only with an approved tag.
	// This is about what the request *asks for* rather than what is stored, so it is the
	// arm addRequestTags populates.
	ctx := configAuthzCtx()
	f := newConfigAuthzFixture(t, "cfgtagger",
		newABACPolicy("Allow", "config:TagResource", "*", "aws:RequestTag/Owner", "platform"))
	arn := emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted")

	body := func(tags []map[string]any) map[string]any {
		return map[string]any{"ResourceArn": arn, "Tags": tags}
	}

	assert.NoError(t, f.check(t, "TagResource",
		body([]map[string]any{{"Key": "Owner", "Value": "platform"}})),
		"the approved tag is allowed")
	assert.True(t, configAuthzDenied(t, f.check(t, "TagResource",
		body([]map[string]any{{"Key": "Owner", "Value": "security"}}))),
		"a request carrying the wrong Owner is denied")
	assert.True(t, configAuthzDenied(t, f.check(t, "TagResource",
		map[string]any{"ResourceArn": arn})),
		"no tag at all leaves the condition key absent, which also fails to match")
}

func TestConfigAuthz_RequestTagGatesACreationTimeTagsList(t *testing.T) {
	// The same reader serves the creating Puts, whose Tags list has the same shape. A
	// policy requiring an approved tag on creation is a real control — "every Config
	// rule must name its owner" — and it is only expressible if the Put's own Tags list
	// reaches the condition context.
	f := newConfigAuthzFixture(t, "cfgcreator",
		newABACPolicy("Allow", "config:PutConfigRule", "*", "aws:RequestTag/Owner", "platform"))

	body := func(owner string) map[string]any {
		return map[string]any{
			"ConfigRule": map[string]any{"ConfigRuleName": "s3-encrypted"},
			"Tags":       []map[string]any{{"Key": "Owner", "Value": owner}},
		}
	}

	assert.NoError(t, f.check(t, "PutConfigRule", body("platform")))
	assert.True(t, configAuthzDenied(t, f.check(t, "PutConfigRule", body("security"))))
}

func TestConfigAuthz_AnUnreadableTagStoreDenies(t *testing.T) {
	// Tags that will not decode leave the condition key absent, so a policy requiring it
	// does not match and the request is denied — the failure mode a corrupted or
	// hand-edited state file produces.
	//
	// The **partially** decodable store is the case that matters, and it is not the
	// obvious one. `encoding/json` populates a map as it goes and returns its type error
	// at the member that failed, so a document whose first tag is a string and whose
	// second is an object yields *both* an error and a map containing the first tag. A
	// reader that ignored the error would hand the decision a map carrying
	// Owner=platform read out of a store it could not decode — an Allow granted from
	// corrupted state, which is the false-allow direction. Whole-file garbage does not
	// discriminate: both readings return an empty map for that, so only this input
	// distinguishes checking the error from ignoring it.
	ctx := configAuthzCtx()
	f := newConfigAuthzFixture(t, "cfgcorrupt",
		newABACPolicy("Allow", "config:DeleteConfigRule", "*", "aws:ResourceTag/Owner", "platform"))

	for _, tc := range []struct {
		name string
		raw  string
	}{
		{
			// The discriminating case: Owner decodes, Team does not.
			name: "a store whose first tag decodes and whose second does not",
			raw:  `{"Owner":"platform","Team":{"nested":"object"}}`,
		},
		{
			name: "a store that is not a JSON object at all",
			raw:  `["Owner","platform"]`,
		},
		{
			name: "a store that is not JSON",
			raw:  `not json at all`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arn := emulator.CfgsvcRuleARNForTest(ctx, "s3-encrypted")
			require.NoError(t, f.state.Put(t.Context(), emulator.ConfigServiceNamespaceForTest,
				emulator.CfgsvcTagsKeyForTest(arn), []byte(tc.raw)))

			assert.True(t, configAuthzDenied(t,
				f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"})))
		})
	}
}

func TestConfigAuthz_AnExplicitDenyOnOneRuleStillNarrows(t *testing.T) {
	// The other polarity: a broad Allow with a Deny scoped to one rule. If the request
	// resolved to "*", the Deny's Resource element would match every request and the
	// broad Allow would be useless — the failure that *looks* safe and is just as wrong,
	// because it blocks work that should proceed.
	ctx := configAuthzCtx()
	protected := emulator.CfgsvcRuleARNForTest(ctx, "do-not-touch")

	f := newConfigAuthzFixture(t, "cfgdenied", emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice{"config:*"},
				Resource: emulator.StringOrSlice{"*"},
			},
			{
				Effect:   "Deny",
				Action:   emulator.StringOrSlice{"config:DeleteConfigRule"},
				Resource: emulator.StringOrSlice{protected},
			},
		},
	})

	assert.True(t, configAuthzDenied(t,
		f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "do-not-touch"})))
	assert.NoError(t, f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "anything-else"}),
		"the Deny is scoped to one rule and must not reach another")
}

func TestConfigAuthz_TheARNIsMintedWhetherOrNotTheResourceExists(t *testing.T) {
	// Authorization runs before the handler, so a policy denying an operation on a rule
	// must deny it whether or not the rule is there. Reading state to build the ARN
	// would make a denial depend on the resource existing — so a caller could probe a
	// policy's shape by deleting, and an operation on an absent resource would authorize
	// against "*" and be allowed by a broad statement it should not match.
	//
	// The fixture's store holds no Config resources at all, and the decision still lands
	// on the right ARN.
	ctx := configAuthzCtx()
	f := newConfigAuthzFixture(t, "cfgghost", emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Deny",
			Action:   emulator.StringOrSlice{"config:DeleteConfigRule"},
			Resource: emulator.StringOrSlice{emulator.CfgsvcRuleARNForTest(ctx, "ghost")},
		}, {
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"config:*"},
			Resource: emulator.StringOrSlice{"*"},
		}},
	})

	err := f.check(t, "DeleteConfigRule", map[string]any{"ConfigRuleName": "ghost"})
	assert.True(t, configAuthzDenied(t, err), "a rule that does not exist is still named")
	var awsErr *emulator.AWSError
	require.True(t, errors.As(err, &awsErr))
}
