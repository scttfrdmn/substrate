package emulator_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The ForAllValues/ForAnyValue set qualifiers, the multivalued request context they
// quantify over, and aws:TagKeys — the key every tag-scoped AWS policy in the wild is
// written against (#690).
//
// The issue's premise was inverted, and the correction is what these tests pin. It
// reported that an unpopulated aws:TagKeys made a ForAllValues:StringEquals statement
// *allow*. In fact the qualifier was never parsed anywhere in the repo, so
// "ForAllValues:StringEquals" reached evaluateConditionKey whole, matched none of its
// nine operators, and fell through to its deny-by-default. Every statement carrying a
// set qualifier was therefore discarded before its key was looked at — which cuts both
// ways and is worse than the issue said: an Allow was a false *deny*, and a Deny was
// silently *inert*. Populating aws:TagKeys alone would have changed nothing.
//
// The policies below are AWS's own, from the "Creating a condition with multiple keys
// or values" and "Single-valued vs. multivalued condition keys" pages, so the expected
// decisions are AWS's documented ones rather than substrate's inference.

const (
	// setQualDeleteTags is the action AWS's two example policies are written against.
	setQualDeleteTags = "ec2:DeleteTags"

	// setQualResource is a concrete ARN rather than "*", so resourceMatches is doing
	// real work in every case below and no decision here can turn on the request
	// resource being a literal star.
	setQualResource = "arn:aws:ec2:us-east-1:123456789012:instance/i-0aaa1111bbbb2222c"
)

// setQualPolicy builds a one-statement policy testing aws:TagKeys under the given
// operator, optionally paired with a Null condition.
//
// The value list — Department and CostCenter — is AWS's, so the five-row tables below
// can be read straight off its documentation.
func setQualPolicy(effect, operator string, nullValue string) []emulator.PolicyDocument {
	cond := map[string]map[string]emulator.StringOrSlice{
		operator: {"aws:TagKeys": {"Department", "CostCenter"}},
	}
	if nullValue != "" {
		cond["Null"] = map[string]emulator.StringOrSlice{"aws:TagKeys": {nullValue}}
	}
	return []emulator.PolicyDocument{{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:    effect,
			Action:    emulator.StringOrSlice{setQualDeleteTags},
			Resource:  emulator.StringOrSlice{"*"},
			Condition: cond,
		}},
	}}
}

// setQualDecide evaluates one request whose aws:TagKeys is the given set. A nil set is
// a request that names no tag keys at all, which is the case the two qualifiers
// disagree about.
func setQualDecide(docs []emulator.PolicyDocument, tagKeys []string) emulator.EvaluationResult {
	req := emulator.EvaluationRequest{
		Action:   setQualDeleteTags,
		Resource: setQualResource,
	}
	if tagKeys != nil {
		req.MultiContext = map[string][]string{"aws:TagKeys": tagKeys}
	}
	return emulator.Evaluate(docs, req)
}

// TestIAMEval_ForAllValues_QuantifiesOverTheRequestValues walks AWS's own evaluation
// table for its ec2:DeleteTags example: the statement allows the call only when *every*
// tag key the request names is one the policy lists.
//
// The absent-key row is the one that matters most, and it is AWS's wording verbatim:
// "It also returns true if there are no context keys in the request." That vacuous
// truth is not substrate being lax — it is the documented behavior, and it is exactly
// why the next test's Null pairing exists.
func TestIAMEval_ForAllValues_QuantifiesOverTheRequestValues(t *testing.T) {
	t.Parallel()
	docs := setQualPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "")

	for _, tc := range []struct {
		name    string
		tagKeys []string
		want    string
	}{
		{
			name:    "one key from the policy's list",
			tagKeys: []string{"Department"},
			want:    emulator.DecisionAllow,
		},
		{
			name:    "every key from the policy's list",
			tagKeys: []string{"Department", "CostCenter"},
			want:    emulator.DecisionAllow,
		},
		{
			name:    "a listed key beside an unlisted one",
			tagKeys: []string{"Department", "Environment"},
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name:    "only an unlisted key",
			tagKeys: []string{"Environment"},
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name:    "the key is present with no values",
			tagKeys: []string{},
			want:    emulator.DecisionAllow,
		},
		{
			name:    "the key is absent from the request",
			tagKeys: nil,
			want:    emulator.DecisionAllow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setQualDecide(docs, tc.tagKeys).Decision)
		})
	}
}

// TestIAMEval_ForAllValues_PairedWithNullFalse pins AWS's recommended form of the same
// policy, and with it the second half of the fix.
//
// AWS's Important note: "You should always include the Null condition operator in your
// policy with a false value" beside ForAllValues, and all four of its aws:TagKeys
// examples do. Substrate's Null reads a single string, so before #690 a key carried
// only in MultiContext read as null and the recommended pattern denied every request it
// was written to allow — the fix would have looked like it worked while changing nothing
// for a real policy.
func TestIAMEval_ForAllValues_PairedWithNullFalse(t *testing.T) {
	t.Parallel()
	docs := setQualPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "false")

	for _, tc := range []struct {
		name    string
		tagKeys []string
		want    string
	}{
		{
			name:    "a listed key satisfies both operators",
			tagKeys: []string{"Department"},
			want:    emulator.DecisionAllow,
		},
		{
			name:    "an unlisted key still fails ForAllValues",
			tagKeys: []string{"Environment"},
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name: "no tag keys at all is where the pairing earns its place: " +
				"ForAllValues is vacuously true and Null:false is what refuses",
			tagKeys: nil,
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name:    "a present-but-valueless key is null too, having no value to report",
			tagKeys: []string{},
			want:    emulator.DecisionImplicitDeny,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setQualDecide(docs, tc.tagKeys).Decision)
		})
	}
}

// TestIAMEval_ForAnyValue_NeedsOneMatch pins the qualifier whose absent-key answer is
// the opposite one: "For no matching context key or if the key does not exist, the
// condition returns false".
func TestIAMEval_ForAnyValue_NeedsOneMatch(t *testing.T) {
	t.Parallel()
	docs := setQualPolicy(emulator.IAMEffectAllow, "ForAnyValue:StringEquals", "")

	for _, tc := range []struct {
		name    string
		tagKeys []string
		want    string
	}{
		{
			name:    "one listed key",
			tagKeys: []string{"Department"},
			want:    emulator.DecisionAllow,
		},
		{
			name: "a listed key beside an unlisted one — the row where the two " +
				"qualifiers disagree",
			tagKeys: []string{"Department", "Environment"},
			want:    emulator.DecisionAllow,
		},
		{
			name:    "no listed key",
			tagKeys: []string{"Environment"},
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name:    "the key is present with no values",
			tagKeys: []string{},
			want:    emulator.DecisionImplicitDeny,
		},
		{
			name:    "the key is absent from the request",
			tagKeys: nil,
			want:    emulator.DecisionImplicitDeny,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setQualDecide(docs, tc.tagKeys).Decision)
		})
	}
}

// TestIAMEval_UnrecognizedQualifierDoesNotMatch pins what is left of the old
// behavior, deliberately.
//
// AWS defines exactly two set qualifiers. Anything else in that position is a policy
// substrate cannot evaluate, and the safe answer is the one evaluateConditionKey
// already gives an unknown operator: no match. An Allow so written does not allow —
// which is what ForAllValues and ForAnyValue themselves used to get.
func TestIAMEval_UnrecognizedQualifierDoesNotMatch(t *testing.T) {
	t.Parallel()

	for _, operator := range []string{
		"ForSomeValues:StringEquals",
		"forallvalues:StringEquals", // the qualifier is case-sensitive
		"ForAllValues:ForAnyValue:StringEquals",
	} {
		t.Run(operator, func(t *testing.T) {
			docs := setQualPolicy(emulator.IAMEffectAllow, operator, "")
			assert.Equal(t, emulator.DecisionImplicitDeny,
				setQualDecide(docs, []string{"Department"}).Decision,
				"an unevaluable qualifier must not allow")
		})
	}
}

// TestIAMEval_Null_ConsultsMultiContext isolates the Null fix from the qualifier fix,
// because the two are independently breakable: Null carries no qualifier at all, so it
// reads the single-valued map, and a multivalued key lives in the other one.
func TestIAMEval_Null_ConsultsMultiContext(t *testing.T) {
	t.Parallel()

	nullPolicy := func(value string) []emulator.PolicyDocument {
		return []emulator.PolicyDocument{{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{setQualDeleteTags},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"Null": {"aws:TagKeys": {value}},
				},
			}},
		}}
	}

	for _, tc := range []struct {
		name      string
		nullValue string
		tagKeys   []string
		want      string
	}{
		{
			name:      "Null:false is satisfied by a key only MultiContext carries",
			nullValue: "false",
			tagKeys:   []string{"Department"},
			want:      emulator.DecisionAllow,
		},
		{
			name:      "Null:false still refuses a request that names no tag keys",
			nullValue: "false",
			tagKeys:   nil,
			want:      emulator.DecisionImplicitDeny,
		},
		{
			name:      "Null:true refuses a request that does carry them",
			nullValue: "true",
			tagKeys:   []string{"Department"},
			want:      emulator.DecisionImplicitDeny,
		},
		{
			name:      "Null:true is satisfied when the key is absent",
			nullValue: "true",
			tagKeys:   nil,
			want:      emulator.DecisionAllow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setQualDecide(nullPolicy(tc.nullValue), tc.tagKeys).Decision)
		})
	}
}

// TestIAMEval_SetQualifiedDenyIsNoLongerInert pins the direction the issue did not
// name, and the more dangerous of the two: a Deny that never matched refused nothing,
// so a guardrail written with a set qualifier was decoration.
//
// The second case is the vacuous truth of ForAllValues seen from the Deny side, and it
// is the reason AWS warns against combining the two: a request naming no tag keys
// satisfies the qualifier, so the Deny fires on the request that asked for least. That
// is AWS's documented semantics rather than a substrate quirk, and pinning it here is
// what stops a later reading of "surely an empty set cannot match" from changing it.
func TestIAMEval_SetQualifiedDenyIsNoLongerInert(t *testing.T) {
	t.Parallel()

	deny := func(operator string) []emulator.PolicyDocument {
		docs := []emulator.PolicyDocument{{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"ec2:*"},
				Resource: emulator.StringOrSlice{"*"},
			}},
		}}
		return append(docs, setQualPolicy(emulator.IAMEffectDeny, operator, "")...)
	}

	for _, tc := range []struct {
		name     string
		operator string
		tagKeys  []string
		want     string
	}{
		{
			name:     "ForAnyValue Deny fires on a key it names",
			operator: "ForAnyValue:StringEquals",
			tagKeys:  []string{"Environment", "Department"},
			want:     emulator.DecisionDeny,
		},
		{
			name:     "ForAnyValue Deny leaves a request naming no such key alone",
			operator: "ForAnyValue:StringEquals",
			tagKeys:  []string{"Environment"},
			want:     emulator.DecisionAllow,
		},
		{
			name:     "ForAllValues Deny fires on a request naming no tag keys at all",
			operator: "ForAllValues:StringEquals",
			tagKeys:  nil,
			want:     emulator.DecisionDeny,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setQualDecide(deny(tc.operator), tc.tagKeys).Decision)
		})
	}
}

// TestIAMEval_MultiContextKeyIsNotAMissingContextValue pins the simulator's half of the
// same correction at the evaluator level: a key the evaluation *used* must not be
// reported as one the request had no value for.
//
// MissingContextValues is how SimulatePrincipalPolicy tells a caller "your policy is
// conditional on something I was not told", so naming a key that in fact decided the
// call would make a simulation contradict the enforcement it exists to predict.
func TestIAMEval_MultiContextKeyIsNotAMissingContextValue(t *testing.T) {
	t.Parallel()

	docs := []emulator.SourcedPolicyDocument{{
		Document:   setQualPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "false")[0],
		SourceID:   "PolicyInputList.1",
		SourceType: emulator.PolicySourceIAMPolicy,
	}}

	supplied := emulator.EvaluateSourced(docs, emulator.EvaluationRequest{
		Action:       setQualDeleteTags,
		Resource:     setQualResource,
		MultiContext: map[string][]string{"aws:TagKeys": {"Department"}},
	})
	assert.Equal(t, emulator.DecisionAllow, supplied.Decision)
	assert.Empty(t, supplied.MissingContextValues,
		"the request carried the key, in the map a set qualifier reads")

	// The case that actually exercises the computation, and the one a caller meets: a
	// *refusal*. MissingContextValues is only gathered for a statement ruled out by its
	// condition, so the allow above never reaches that code — and telling a caller "you
	// gave me no value for aws:TagKeys" when the value it gave is precisely why the call
	// was refused would send it looking for the wrong bug.
	refused := emulator.EvaluateSourced(docs, emulator.EvaluationRequest{
		Action:       setQualDeleteTags,
		Resource:     setQualResource,
		MultiContext: map[string][]string{"aws:TagKeys": {"Environment"}},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, refused.Decision)
	assert.Empty(t, refused.MissingContextValues,
		"the key was supplied and evaluated; the value is what failed")

	// And the honest opposite: with neither map carrying it, it *is* missing.
	absent := emulator.EvaluateSourced(docs, emulator.EvaluationRequest{
		Action:   setQualDeleteTags,
		Resource: setQualResource,
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, absent.Decision)
	assert.Equal(t, []string{"aws:TagKeys"}, absent.MissingContextValues)
}

// TestIAMManaged_RDSPoliciesStillWithholdDevOpsGuru is the regression the fix could
// silently have widened.
//
// AmazonRDSFullAccess and AmazonRDSReadOnlyAccess each carry a real
// ForAllValues:StringEquals on devops-guru:ServiceNames — paired, as AWS recommends,
// with Null:false. Both statements were discarded before #690 and must *still* not
// match a request that names no service names, because the Null arm refuses it. Two
// bundled policies quietly granting devops-guru:SearchInsights would be the fix
// escaping its own blast radius.
func TestIAMManaged_RDSPoliciesStillWithholdDevOpsGuru(t *testing.T) {
	t.Parallel()

	for _, arn := range []string{
		"arn:aws:iam::aws:policy/AmazonRDSFullAccess",
		"arn:aws:iam::aws:policy/AmazonRDSReadOnlyAccess",
	} {
		t.Run(arn, func(t *testing.T) {
			policy, ok := emulator.GetManagedPolicy(arn)
			require.True(t, ok, "bundled policy %s", arn)
			docs := []emulator.PolicyDocument{policy.Document}

			decide := func(serviceNames []string) string {
				req := emulator.EvaluationRequest{
					Action:   "devops-guru:SearchInsights",
					Resource: "*",
				}
				if serviceNames != nil {
					req.MultiContext = map[string][]string{
						"devops-guru:ServiceNames": serviceNames,
					}
				}
				return emulator.Evaluate(docs, req).Decision
			}

			assert.Equal(t, emulator.DecisionImplicitDeny, decide(nil),
				"the key is absent, so Null:false refuses — the statement must not "+
					"start matching now that its qualifier is parsed")
			assert.Equal(t, emulator.DecisionImplicitDeny, decide([]string{"EC2"}),
				"a service name the policy does not list")
			assert.Equal(t, emulator.DecisionAllow, decide([]string{"RDS"}),
				"the grant the policy actually makes, reachable for the first time")
		})
	}
}

// TestRequestTagKeys_SortedAndDerivedFromRequestTag pins the shape of the derived key.
//
// aws:TagKeys is read out of the aws:RequestTag/ entries addRequestTags already
// produced rather than gathered a second time per service, so the two cannot disagree
// about what the request asked for. The sort is what makes the derivation replayable:
// three of those arms iterate a Go map, whose order is randomized per run, and a
// ForAllValues denial that depended on map order is the one thing an event log must
// never record.
func TestRequestTagKeys_SortedAndDerivedFromRequestTag(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"CostCenter", "Department", "Environment"},
		emulator.RequestTagKeysForTest(map[string]string{
			"aws:RequestTag/Environment": "prod",
			"aws:RequestTag/Department":  "eng",
			"aws:RequestTag/CostCenter":  "",
			"aws:PrincipalArn":           "arn:aws:iam::123456789012:user/alice",
			"aws:RequestTag/":            "a key with no name is not a tag key",
		}),
		"sorted, tag keys only, and an empty suffix is not a key")

	assert.Empty(t, emulator.RequestTagKeysForTest(map[string]string{}),
		"a request carrying no tags carries no tag keys")
}

// TestAddRequestTags_PopulatesTagKeysForEveryArm pins that the derivation covers every
// service arm, which is the payoff of deriving it rather than threading a second map
// through each of them: an arm added later populates aws:TagKeys without knowing the
// key exists.
func TestAddRequestTags_PopulatesTagKeysForEveryArm(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		req  emulator.AWSRequest
		want []string
	}{
		{
			name: "ec2 TagSpecification on a create",
			req: emulator.AWSRequest{Service: "ec2", Operation: "RunInstances", Params: map[string]string{
				"TagSpecification.1.ResourceType": "instance",
				"TagSpecification.1.Tag.1.Key":    "Environment",
				"TagSpecification.1.Tag.1.Value":  "prod",
				"TagSpecification.2.ResourceType": "volume",
				"TagSpecification.2.Tag.1.Key":    "CostCenter",
				"TagSpecification.2.Tag.1.Value":  "1234",
			}},
			want: []string{"CostCenter", "Environment"},
		},
		{
			name: "ec2 Tag.N on a direct CreateTags",
			req: emulator.AWSRequest{Service: "ec2", Operation: "CreateTags", Params: map[string]string{
				"ResourceId.1": "i-0aaa1111bbbb2222c",
				"Tag.1.Key":    "Department",
				"Tag.1.Value":  "eng",
				"Tag.2.Key":    "Backup",
				"Tag.2.Value":  "nightly",
			}},
			want: []string{"Backup", "Department"},
		},
		{
			name: "ec2 DeleteTags, whose values are optional",
			req: emulator.AWSRequest{Service: "ec2", Operation: "DeleteTags", Params: map[string]string{
				"ResourceId.1": "i-0aaa1111bbbb2222c",
				"Tag.1.Key":    "Department",
			}},
			want: []string{"Department"},
		},
		{
			name: "an iam body's Tags list",
			req: emulator.AWSRequest{Service: "iam", Operation: "CreateUser",
				Body: []byte(`{"UserName":"bob","Tags":[{"Key":"Team","Value":"eng"},{"Key":"Cost","Value":"x"}]}`)},
			want: []string{"Cost", "Team"},
		},
		{
			name: "a lambda body's Tags map",
			req: emulator.AWSRequest{Service: "lambda", Operation: "CreateFunction",
				Body: []byte(`{"FunctionName":"f","Tags":{"Owner":"eng","Stage":"dev"}}`)},
			want: []string{"Owner", "Stage"},
		},
		{
			name: "an organizations body's Tags list",
			req: emulator.AWSRequest{Service: "organizations", Operation: "TagResource",
				Body: []byte(`{"ResourceId":"123456789012","Tags":[{"Key":"Zone","Value":"a"},{"Key":"Alpha","Value":"b"}]}`)},
			want: []string{"Alpha", "Zone"},
		},
		{
			name: "an s3 x-amz-tagging header",
			req: emulator.AWSRequest{Service: "s3", Operation: "PutObject",
				Headers: map[string]string{"x-amz-tagging": "Env=prod&Class=archive"}},
			want: []string{"Class", "Env"},
		},
		{
			name: "a request carrying no tags",
			req: emulator.AWSRequest{Service: "ec2", Operation: "DescribeInstances",
				Params: map[string]string{"InstanceId.1": "i-0aaa1111bbbb2222c"}},
			want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req
			tags, keys := emulator.AddRequestTagsForTest(&req)
			if tc.want == nil {
				assert.Empty(t, keys)
				return
			}
			assert.Equal(t, tc.want, keys)
			for _, key := range tc.want {
				assert.Contains(t, tags, "aws:RequestTag/"+key,
					"the two halves read the same request")
			}
		})
	}
}

// TestCheckAccess_TagKeysGatesATaggedRequest is the end-to-end: AWS's documented
// tag-scoped policy, decided by CheckAccess against a real EC2 request.
//
// This is the case the issue was filed about and the one a consumer meets. Before #690
// every row here denied — the statement was discarded whatever the request carried —
// which made the policy a false deny rather than the false allow the issue predicted.
func TestCheckAccess_TagKeysGatesATaggedRequest(t *testing.T) {
	t.Parallel()

	// The policy AWS writes for "may only apply approved tags": every tag key in the
	// request must be listed, and the request must carry at least one.
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"ec2:*"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"ForAllValues:StringEquals": {"aws:TagKeys": {"Department", "CostCenter"}},
				"Null":                      {"aws:TagKeys": {"false"}},
			},
		}},
	}

	for _, tc := range []struct {
		name      string
		operation string
		params    map[string]string
		allowed   bool
	}{
		{
			name:      "a launch tagging with approved keys",
			operation: "RunInstances",
			params: map[string]string{
				"TagSpecification.1.ResourceType": "instance",
				"TagSpecification.1.Tag.1.Key":    "Department",
				"TagSpecification.1.Tag.1.Value":  "eng",
			},
			allowed: true,
		},
		{
			name:      "a launch tagging with an unapproved key",
			operation: "RunInstances",
			params: map[string]string{
				"TagSpecification.1.ResourceType": "instance",
				"TagSpecification.1.Tag.1.Key":    "Department",
				"TagSpecification.1.Tag.1.Value":  "eng",
				"TagSpecification.1.Tag.2.Key":    "Environment",
				"TagSpecification.1.Tag.2.Value":  "prod",
			},
			allowed: false,
		},
		{
			name:      "an untagged launch, which Null:false refuses",
			operation: "RunInstances",
			params:    map[string]string{},
			allowed:   false,
		},
		{
			name:      "a direct CreateTags naming approved keys",
			operation: "CreateTags",
			params: map[string]string{
				"ResourceId.1": ec2TagAuthzInstance,
				"Tag.1.Key":    "CostCenter",
				"Tag.1.Value":  "1234",
			},
			allowed: true,
		},
		{
			name:      "a direct CreateTags naming an unapproved key",
			operation: "CreateTags",
			params: map[string]string{
				"ResourceId.1": ec2TagAuthzInstance,
				"Tag.1.Key":    "Environment",
				"Tag.1.Value":  "prod",
			},
			allowed: false,
		},
		{
			name:      "a DeleteTags naming a key and no value",
			operation: "DeleteTags",
			params: map[string]string{
				"ResourceId.1": ec2TagAuthzInstance,
				"Tag.1.Key":    "Department",
			},
			allowed: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEC2AuthzFixture(t, "tagkeys", doc)
			f.putTagInstance(t, nil)

			params := map[string]string{}
			if tc.operation == "RunInstances" {
				params = nominalLaunch()
			}
			for k, v := range tc.params {
				params[k] = v
			}

			err := f.call(t, tc.operation, params)
			if tc.allowed {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not authorized")
		})
	}
}

// TestSimulate_ContextEntriesKeepEveryValue pins the third producer of a multivalued
// context: the simulator, which used to keep only ContextKeyValues.member.1.
//
// A ContextEntry is a value *list* by construction, so discarding all but the first
// meant a caller simulating aws:TagKeys was answered against a policy that had seen one
// of its values. An answer that can differ from what enforcement would decide is the
// one thing a simulator must not produce.
func TestSimulate_ContextEntriesKeepEveryValue(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"ec2:DeleteTags","Resource":"*","Condition":{` +
		`"ForAllValues:StringEquals":{"aws:TagKeys":["Department","CostCenter"]},` +
		`"Null":{"aws:TagKeys":"false"}}}]}`

	simulate := func(t *testing.T, values ...string) simulateXMLResult {
		t.Helper()
		params := map[string]string{
			"ActionNames.member.1":                   "ec2:DeleteTags",
			"PolicyInputList.member.1":               policy,
			"ContextEntries.member.1.ContextKeyName": "aws:TagKeys",
			"ContextEntries.member.1.ContextKeyType": "stringList",
		}
		for i, v := range values {
			params["ContextEntries.member.1.ContextKeyValues.member."+strconv.Itoa(i+1)] = v
		}
		got := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", params))
		require.Len(t, got.results(), 1)
		return got.results()[0]
	}

	t.Run("every supplied value satisfies the policy", func(t *testing.T) {
		result := simulate(t, "Department", "CostCenter")
		assert.Equal(t, "allowed", result.Decision)
		assert.Empty(t, result.MissingContextValues,
			"the caller supplied the key, so it is not missing")
	})

	t.Run("a second value the policy does not list refuses", func(t *testing.T) {
		result := simulate(t, "Department", "Environment")
		assert.Equal(t, "implicitDeny", result.Decision,
			"the value after the first is evaluated, not discarded")
	})

	t.Run("a key named with no values is set but empty", func(t *testing.T) {
		result := simulate(t)
		assert.Equal(t, "implicitDeny", result.Decision,
			"ForAllValues is vacuously true; Null:false is what refuses")
		assert.Empty(t, result.MissingContextValues,
			"a key named with no value was still supplied by the caller")
	})
}
