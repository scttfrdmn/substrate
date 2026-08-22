package emulator_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Condition-key names are matched case-insensitively (#704).
//
// AWS, in *IAM JSON policy elements: Condition*: "Context key *names* are not
// case-sensitive. For example, including the aws:SourceIP context key is equivalent to
// testing for AWS:SourceIp. Case-sensitivity of context key *values* depends on the
// condition operator that you use."
//
// Substrate matched names byte-for-byte before this pass, which failed in both
// directions: an Allow written with a differently-cased name was an implicit deny — a
// false refusal — and a Deny written that way was inert, allowing what AWS refuses. The
// second is the dangerous one, and it is what these tests exist to keep closed.

// iamCaseFoldPolicy builds the one-statement document these tests evaluate: a single
// operator testing a single key, against every resource and action, so the only thing
// that can decide the result is the condition.
func iamCaseFoldPolicy(effect, operator, condKey string, condValues ...string) emulator.PolicyDocument {
	return emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   effect,
			Action:   emulator.StringOrSlice{"*"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				operator: {condKey: emulator.StringOrSlice(condValues)},
			},
		}},
	}
}

// TestEvaluate_ConditionKeyNameFoldsCase walks the spellings of one key against one
// request, in both effect positions.
//
// The Allow rows assert the false refusal is gone; the Deny rows assert the inert Deny
// is gone. Both are needed: an implementation that folded only on the way to an Allow
// would leave every case-variant Deny unenforced, which is the failure a policy author
// cannot see from a successful test run.
func TestEvaluate_ConditionKeyNameFoldsCase(t *testing.T) {
	t.Parallel()

	const canonical = "ec2:CreateAction"
	spellings := []string{
		canonical,
		"ec2:createaction",
		"EC2:CREATEACTION",
		"Ec2:CreateAction",
	}

	for _, spelling := range spellings {
		t.Run("allow/"+spelling, func(t *testing.T) {
			t.Parallel()
			doc := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", spelling, "RunInstances")
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action:   "ec2:CreateTags",
				Resource: "*",
				Context:  map[string]string{canonical: "RunInstances"},
			})
			assert.Equal(t, emulator.DecisionAllow, result.Decision,
				"a grant naming the key as %q must read the value the producer wrote under %q", spelling, canonical)
		})

		t.Run("deny/"+spelling, func(t *testing.T) {
			t.Parallel()
			docs := []emulator.PolicyDocument{
				{
					Version: "2012-10-17",
					Statement: []emulator.PolicyStatement{{
						Effect:   emulator.IAMEffectAllow,
						Action:   emulator.StringOrSlice{"*"},
						Resource: emulator.StringOrSlice{"*"},
					}},
				},
				iamCaseFoldPolicy(emulator.IAMEffectDeny, "StringEquals", spelling, "RunInstances"),
			}
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "ec2:CreateTags",
				Resource: "*",
				Context:  map[string]string{canonical: "RunInstances"},
			})
			assert.Equal(t, emulator.DecisionDeny, result.Decision,
				"a Deny naming the key as %q must not be inert", spelling)
		})
	}
}

// TestEvaluate_ConditionValueStaysCaseSensitive is the other half of AWS's sentence,
// and the reason the fold lives in the lookup rather than in the operator arms.
//
// StringEquals is defined as an exact match, so folding the name must not fold what the
// name resolves to. AWS supplies StringEqualsIgnoreCase for callers who want the value
// folded too; a StringEquals that quietly behaved like it would make that operator
// meaningless and would allow requests a policy was written to exclude.
func TestEvaluate_ConditionValueStaysCaseSensitive(t *testing.T) {
	t.Parallel()

	doc := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", "AWS:RequestTag/Env", "prod")

	matching := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"aws:RequestTag/Env": "prod"},
	})
	assert.Equal(t, emulator.DecisionAllow, matching.Decision, "the name folds")

	cased := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"aws:RequestTag/Env": "PROD"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, cased.Decision,
		"the value does not: StringEquals is an exact match, which is why StringEqualsIgnoreCase exists")
}

// TestEvaluate_ConditionKeyTagSuffixFoldsToo pins the half of #704 that its own scope
// section got backwards.
//
// #704 asked that the tag-key suffix of aws:RequestTag/ and aws:ResourceTag/ stay
// case-sensitive. AWS documents the opposite, in the same breath as the key–value form
// itself: "Key names are not case-sensitive. This means that if you specify
// "aws:ResourceTag/TagKey1": "Value1" in the condition element of your policy, then the
// condition matches a resource tag key named either TagKey1 or tagkey1, but not both."
//
// The case-sensitive rule #704 remembers is real, but it governs a tag key used as a
// condition *value* — aws:TagKeys, and substrate's own tag rules — which is a different
// thing from a key name. Both halves are asserted here so the distinction cannot be
// collapsed by a later change to either.
func TestEvaluate_ConditionKeyTagSuffixFoldsToo(t *testing.T) {
	t.Parallel()

	suffixFolded := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", "aws:ResourceTag/tagkey1", "Value1")
	result := emulator.Evaluate([]emulator.PolicyDocument{suffixFolded}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"aws:ResourceTag/TagKey1": "Value1"},
	})
	assert.Equal(t, emulator.DecisionAllow, result.Decision,
		`AWS: the condition "matches a resource tag key named either TagKey1 or tagkey1"`)

	// aws:TagKeys carries tag keys as *values*, so the same string is compared exactly.
	// A policy listing "Env" does not admit a request tagging "env".
	asValue := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "aws:TagKeys", "Env")
	admitted := emulator.Evaluate([]emulator.PolicyDocument{asValue}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		MultiContext: map[string][]string{"aws:TagKeys": {"Env"}},
	})
	assert.Equal(t, emulator.DecisionAllow, admitted.Decision)

	refused := emulator.Evaluate([]emulator.PolicyDocument{asValue}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		MultiContext: map[string][]string{"aws:TagKeys": {"env"}},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, refused.Decision,
		"a tag key compared as a condition value stays case-sensitive")
}

// TestEvaluate_ConditionKeyFoldIsDeterministic pins the tie-break AWS leaves unstated.
//
// AWS says the condition matches either spelling "but not both", and names the hazard
// without resolving it: "you might tag an Amazon EC2 instance with ec2=test1 and
// EC2=test2 … the key name matches both tags, but only one value matches. This can
// result in unexpected condition failures." Substrate has to answer with one of them,
// so it answers with the first in sorted order.
//
// The loop is what makes this a test rather than a coincidence: a fold that returned
// whichever key Go's randomized map iteration reached first would pass a single run
// about half the time, and would put a *decision* at the mercy of map order — which an
// event log that must replay identically cannot tolerate.
func TestEvaluate_ConditionKeyFoldIsDeterministic(t *testing.T) {
	t.Parallel()

	// "EC2" sorts before "ec2" (upper case is lower in ASCII), so the sorted-first rule
	// answers with test2 and the StringEquals on test1 does not match.
	doc := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", "aws:ResourceTag/Ec2", "test1")
	req := emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{
			"aws:ResourceTag/ec2": "test1",
			"aws:ResourceTag/EC2": "test2",
		},
	}

	for range 200 {
		result := emulator.Evaluate([]emulator.PolicyDocument{doc}, req)
		require.Equal(t, emulator.DecisionImplicitDeny, result.Decision,
			"the fold must resolve to the sorted-first name on every run, never to whichever key map iteration reached")
	}

	// The mirror image: asking for the value the sorted-first name holds always matches.
	wins := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", "aws:ResourceTag/Ec2", "test2")
	for range 200 {
		result := emulator.Evaluate([]emulator.PolicyDocument{wins}, req)
		require.Equal(t, emulator.DecisionAllow, result.Decision)
	}
}

// TestEvaluate_ConditionOperatorStaysCaseSensitive pins the boundary of the fold.
//
// AWS's sentence is about context key *names*. Nothing documents an operator name as
// case-insensitive, and an implementation that folded the whole Condition map key —
// which is where the operator and its set qualifier live — would fold both. An
// unrecognized operator denies, which is the safe direction and the one substrate
// already took for a bare unknown operator.
func TestEvaluate_ConditionOperatorStaysCaseSensitive(t *testing.T) {
	t.Parallel()

	for _, operator := range []string{"stringequals", "STRINGEQUALS", "forallvalues:StringEquals"} {
		t.Run(operator, func(t *testing.T) {
			t.Parallel()
			doc := iamCaseFoldPolicy(emulator.IAMEffectAllow, operator, "ec2:CreateAction", "RunInstances")
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action: "ec2:CreateTags", Resource: "*",
				Context: map[string]string{"ec2:CreateAction": "RunInstances"},
			})
			assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision,
				"%q is not an operator substrate evaluates, and the key-name fold must not make it one", operator)
		})
	}
}

// TestEvaluate_NullResolvesTheSameWay covers the operator whose whole job is to tell an
// absent key from a present one.
//
// Null is where a lookup that missed on case would be worst: it does not compare a
// value, so a name that failed to resolve reads as a definite "this key is absent"
// rather than as a mismatch. AWS's recommended ForAllValues + `"Null": "false"` pairing
// is built on that answer, so a differently-cased name would flip the guard the pattern
// exists to provide.
func TestEvaluate_NullResolvesTheSameWay(t *testing.T) {
	t.Parallel()

	present := iamCaseFoldPolicy(emulator.IAMEffectAllow, "Null", "EC2:CreateAction", "false")
	result := emulator.Evaluate([]emulator.PolicyDocument{present}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"ec2:CreateAction": "RunInstances"},
	})
	assert.Equal(t, emulator.DecisionAllow, result.Decision,
		"the key is present, so Null:false is true however the policy spells the name")

	absent := emulator.Evaluate([]emulator.PolicyDocument{present}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, absent.Decision,
		"a key nothing in the request folds to is still absent")

	// Null:true is the same question inverted, and reaches through MultiContext, which
	// is the map condContextValue falls back to.
	nullTrue := iamCaseFoldPolicy(emulator.IAMEffectAllow, "Null", "AWS:TagKeys", "true")
	multi := emulator.Evaluate([]emulator.PolicyDocument{nullTrue}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		MultiContext: map[string][]string{"aws:TagKeys": {"Env"}},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, multi.Decision,
		"the multivalued key resolves through the fold too, so Null:true is false")
}

// TestEvaluate_SetQualifiersResolveTheSameWay covers the pair whose disagreement about
// an absent key makes an unresolved name dangerous in *both* directions at once.
//
// ForAllValues on an absent key is true — AWS: "It also returns true if there are no
// context keys in the request" — so a name that failed to fold would grant a
// ForAllValues Allow outright. ForAnyValue on an absent key is false, so the same
// failure would make a ForAnyValue Allow a false deny. One lookup fixes both.
func TestEvaluate_SetQualifiersResolveTheSameWay(t *testing.T) {
	t.Parallel()

	req := emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		MultiContext: map[string][]string{"aws:TagKeys": {"Env", "Owner"}},
	}

	// Owner is not in the policy's list, so ForAllValues is false — which is only
	// reachable if the differently-cased name resolved to the request's two values.
	forAll := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "aws:tagkeys", "Env")
	assert.Equal(t, emulator.DecisionImplicitDeny,
		emulator.Evaluate([]emulator.PolicyDocument{forAll}, req).Decision,
		"an unresolved name would have read as no values in the request, which ForAllValues calls true")

	forAllBoth := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals", "aws:tagkeys", "Env", "Owner")
	assert.Equal(t, emulator.DecisionAllow,
		emulator.Evaluate([]emulator.PolicyDocument{forAllBoth}, req).Decision)

	forAny := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAnyValue:StringEquals", "AWS:TAGKEYS", "Owner")
	assert.Equal(t, emulator.DecisionAllow,
		emulator.Evaluate([]emulator.PolicyDocument{forAny}, req).Decision,
		"an unresolved name would have read as no values, which ForAnyValue calls false")

	// A set qualifier over a key the request carries only in the *single-valued* map
	// degrades to a one-element set — AWS's rule is about how many values the request
	// context holds, and a producer that recorded one has still said the request carried
	// it. That fallback needs the fold too: every key substrate populates on the
	// enforcement path except aws:TagKeys is single-valued, so this is the shape a real
	// ForAnyValue policy on aws:RequestTag/<key> actually meets.
	single := emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"aws:RequestTag/Env": "prod"},
	}
	anyOnSingle := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAnyValue:StringEquals",
		"AWS:REQUESTTAG/ENV", "prod")
	assert.Equal(t, emulator.DecisionAllow,
		emulator.Evaluate([]emulator.PolicyDocument{anyOnSingle}, single).Decision,
		"the single-valued fallback resolves the name as well")

	allOnSingle := iamCaseFoldPolicy(emulator.IAMEffectAllow, "ForAllValues:StringEquals",
		"AWS:REQUESTTAG/ENV", "staging")
	assert.Equal(t, emulator.DecisionImplicitDeny,
		emulator.Evaluate([]emulator.PolicyDocument{allOnSingle}, single).Decision,
		"an unresolved name would have read as no values at all, which ForAllValues calls true")
}

// TestSimulate_MissingContextValuesFoldsTheName is the simulator's half of the same
// rule, and the one a caller sees.
//
// A key the evaluator found by folding case must not also be reported as missing: the
// simulation would then contradict the enforcement it exists to predict — an allowed
// decision alongside a claim that the answer depends on a value the request supplied.
// When the key genuinely is absent, the reported string is the **policy's** spelling,
// because that is what the caller submitted and what they will compare against.
func TestSimulate_MissingContextValuesFoldsTheName(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	// The policy spells the derived key in a different case from the one the simulation
	// fills in from CallerArn.
	folded := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"ArnEquals":` +
		`{"AWS:PRINCIPALARN":"arn:aws:iam::123456789012:user/alice"}}}]}`

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{folded},
		"CallerArn":       "arn:aws:iam::123456789012:user/alice",
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	assert.Empty(t, got.results()[0].MissingContextValues,
		"the evaluator resolved the name, so nothing about this simulation is incomplete")

	// The decisive case, because MissingContextValues is only *gathered* for a statement
	// ruled out by its condition: the name resolves, and the value it resolves to does
	// not match. The refusal is real and complete — supplying the key would change
	// nothing, since the request already carries it — so the key must not be listed. A
	// lookup that missed on case here would answer implicitDeny *and* claim the key was
	// never supplied, which is the two-way contradiction a preflight must never produce.
	mismatched := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"ArnEquals":` +
		`{"AWS:PRINCIPALARN":"arn:aws:iam::123456789012:user/bob"}}}]}`
	refused := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{mismatched},
		"CallerArn":       "arn:aws:iam::123456789012:user/alice",
	})
	require.Len(t, refused.results(), 1)
	assert.Equal(t, "implicitDeny", refused.results()[0].Decision)
	assert.Empty(t, refused.results()[0].MissingContextValues,
		"the request supplied the key under another spelling, so the refusal is not for want of a value")

	// A key nothing folds to is missing, and is reported exactly as the policy spelled
	// it — that string reaches the wire, and canonicalizing it would answer a caller
	// with a name they never wrote.
	unknown := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"Bool":` +
		`{"AWS:MultiFactorAuthPresent":"true"}}}]}`
	absent := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{unknown},
	})
	require.Len(t, absent.results(), 1)
	assert.Equal(t, "implicitDeny", absent.results()[0].Decision)
	assert.Equal(t, []string{"AWS:MultiFactorAuthPresent"}, absent.results()[0].MissingContextValues,
		"MissingContextValues echoes the submitted document, not substrate's preferred casing")
}

// TestSimulate_ContextEntriesFoldAgainstEachOther covers the one place a case-variant
// duplicate can enter substrate at all: the caller supplies these names.
//
// Every internal producer writes a canonical literal, so two spellings of one key can
// only arrive through ContextEntries. Letting both survive would leave the decision to
// condResolveKey's sorted tie-break rather than to what the caller last said, so the
// later entry overwrites the earlier under the earlier's spelling. Entries arrive in
// wire-index order, which is the caller's own ordering.
//
// **Each call is repeated**, because collapsing here is what makes the answer a function
// of the request at all. With both spellings in the map, the derived-key override in
// simulationConditionContext iterates them in Go map order and deletes whichever folded
// sibling it happens to meet first, so the surviving value — and the decision — differs
// run to run. A single round trip would pass about half the time.
func TestSimulate_ContextEntriesFoldAgainstEachOther(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	onProd := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":` +
		`{"aws:RequestTag/Env":"prod"}}}]}`

	for range 40 {
		got := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
			"ActionNames.member.1":                              "s3:GetObject",
			"PolicyInputList.member.1":                          onProd,
			"ContextEntries.member.1.ContextKeyName":            "AWS:RequestTag/Env",
			"ContextEntries.member.1.ContextKeyValues.member.1": "staging",
			"ContextEntries.member.2.ContextKeyName":            "aws:requesttag/env",
			"ContextEntries.member.2.ContextKeyValues.member.1": "prod",
		}))
		require.Len(t, got.results(), 1)
		require.Equal(t, "allowed", got.results()[0].Decision,
			"the last entry the caller sent is the value simulated with, on every run")
	}

	// Reversing the order reverses the answer, which is what "the caller's ordering
	// decides" means.
	for range 40 {
		reversed := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
			"ActionNames.member.1":                              "s3:GetObject",
			"PolicyInputList.member.1":                          onProd,
			"ContextEntries.member.1.ContextKeyName":            "aws:requesttag/env",
			"ContextEntries.member.1.ContextKeyValues.member.1": "prod",
			"ContextEntries.member.2.ContextKeyName":            "AWS:RequestTag/Env",
			"ContextEntries.member.2.ContextKeyValues.member.1": "staging",
		}))
		require.Len(t, reversed.results(), 1)
		require.Equal(t, "implicitDeny", reversed.results()[0].Decision)
	}
}

// TestSimulate_ContextEntryOverridesADerivedKeyWhateverItsCase pins the rule
// simulationConditionContext exists for, against a differently-cased entry.
//
// aws:PrincipalArn is derived from CallerArn because the simulation knows it. A caller
// who names the key is stating what to simulate with instead — and before #704 a
// differently-cased entry left the derived value in the map beside it, so which one the
// evaluator read was decided by a sort rather than by the caller.
//
// The entry is spelled `aws:principalarn` deliberately: it sorts *after* the derived
// `aws:PrincipalArn` (upper case is lower in ASCII), so an implementation that left both
// in the map would answer with alice and deny. A spelling that sorted first would pass
// this test whether or not the derived key was removed, and so would pin nothing.
func TestSimulate_ContextEntryOverridesADerivedKeyWhateverItsCase(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	onBob := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"ArnEquals":` +
		`{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/bob"}}}]}`

	got := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
		"ActionNames.member.1":                              "s3:GetObject",
		"PolicyInputList.member.1":                          onBob,
		"CallerArn":                                         "arn:aws:iam::123456789012:user/alice",
		"ContextEntries.member.1.ContextKeyName":            "aws:principalarn",
		"ContextEntries.member.1.ContextKeyValues.member.1": "arn:aws:iam::123456789012:user/bob",
	}))
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision,
		"the caller's entry replaces the derived key rather than sitting beside it")
}

// TestEvaluate_ConditionKeyExactMatchIsUnaffected is the regression guard for the fast
// path.
//
// Every policy substrate ships and every key any of its producers writes is spelled
// canonically, so the overwhelming majority of lookups must still be the single map
// read they were before #704 — and must still return the exact entry even when a
// folded-equal sibling exists and sorts earlier.
func TestEvaluate_ConditionKeyExactMatchIsUnaffected(t *testing.T) {
	t.Parallel()

	doc := iamCaseFoldPolicy(emulator.IAMEffectAllow, "StringEquals", "aws:ResourceTag/env", "exact")
	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{
			"aws:ResourceTag/ENV": "sorts-first",
			"aws:ResourceTag/env": "exact",
		},
	})
	assert.Equal(t, emulator.DecisionAllow, result.Decision,
		"an exact hit wins over a folded one, however the folded one sorts")

	// And the canonical spelling of a key present only once behaves as it always did,
	// which is the case the whole evaluator runs on.
	assert.False(t, strings.EqualFold("aws:ResourceTag/env", "aws:ResourceTag/other"),
		"guard against a fold so wide it matches unrelated names")
	unrelated := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Action: "ec2:CreateTags", Resource: "*",
		Context: map[string]string{"aws:ResourceTag/other": "exact"},
	})
	assert.Equal(t, emulator.DecisionImplicitDeny, unrelated.Decision)
}
