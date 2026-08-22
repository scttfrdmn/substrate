package emulator_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// iamOperatorPolicy builds a one-statement Allow conditioned on a single operator and
// key, which is the shape every operator-family case below needs.
func iamOperatorPolicy(operator, condKey string, condValues ...string) emulator.PolicyDocument {
	return emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"*"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				operator: {condKey: emulator.StringOrSlice(condValues)},
			},
		}},
	}
}

// iamOperatorAllows reports whether a one-key Allow conditioned on operator is granted
// for a request context carrying ctxVal — or carrying nothing, when ctxVal is absent.
func iamOperatorAllows(t *testing.T, operator string, condValues []string, ctxVal ...string) bool {
	t.Helper()
	const condKey = "test:Key"
	req := emulator.EvaluationRequest{Action: "s3:GetObject", Resource: "*"}
	if len(ctxVal) > 0 {
		req.Context = map[string]string{condKey: ctxVal[0]}
	}
	doc := iamOperatorPolicy(operator, condKey, condValues...)
	return emulator.Evaluate([]emulator.PolicyDocument{doc}, req).Decision == emulator.DecisionAllow
}

// TestEvaluate_UnrecognizedOperatorDeniesUnderASetQualifier pins the hole #714 was
// filed about: a set qualifier over an absent key returned true without ever consulting
// the operator, so an Allow written with an operator substrate cannot evaluate was
// granted.
//
// The vacuous truth itself is AWS's rule and stays for a recognized operator: "The
// ForAllValues qualifier returns true if there are no context keys in the request".
// What changed is that an operator name substrate does not evaluate no longer collects
// it. Real IAM rejects an unknown operator when the document is submitted, so a policy
// reaching this arm is one AWS would never have stored.
func TestEvaluate_UnrecognizedOperatorDeniesUnderASetQualifier(t *testing.T) {
	cases := []struct {
		operator string
		allowed  bool
		why      string
	}{
		{"ForAllValues:StringEquals", true, "recognized: AWS's vacuous truth applies"},
		{"ForAllValues:NumericLessThan", true, "recognized as of #714, so the same rule applies"},
		{"ForAllValues:NotAnOperator", false, "unrecognized: must not be granted vacuously"},
		{"ForAllValues:stringequals", false, "an operator's own name is case-sensitive"},
		{"ForAllValues:Null", false, "AWS defines no set-qualified Null"},
		{"ForAnyValue:Null", false, "nor a ForAnyValue one"},
		{"ForAnyValue:NotAnOperator", false, "ForAnyValue denies on an absent key regardless"},
		{"NotAnOperator", false, "unqualified, unrecognized: unchanged"},
		// IfExists is where the qualified unrecognized operator becomes a grant if the
		// operator is not checked: the suffix answers true on an absent key before the
		// value loop that would otherwise have found nothing to match.
		{"ForAnyValue:NotAnOperatorIfExists", false, "the suffix must not rescue an unevaluable operator"},
		{"ForAllValues:NotAnOperatorIfExists", false, "nor under the other qualifier"},
	}
	for _, tc := range cases {
		t.Run(tc.operator, func(t *testing.T) {
			doc := iamOperatorPolicy(tc.operator, "aws:TagKeys", "Env")
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action: "s3:GetObject", Resource: "*",
			})
			if tc.allowed {
				assert.Equal(t, emulator.DecisionAllow, result.Decision, tc.why)
				return
			}
			assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision, tc.why)
		})
	}
}

// TestEvaluate_ConditionAbsentKeyFollowsTheOperator pins AWS's Important note: an
// absent key is false for a positive operator and *true* for a negated one.
//
// AWS: "If the key that you specify in a policy condition is not present in the request
// context, the values do not match and the condition is false. If the policy condition
// requires that the key is not matched, such as StringNotLike or ArnNotLike, and the
// right key is not present, the condition is true." The rows below walk that sentence
// operator by operator, since the polarity is the only thing deciding each answer.
func TestEvaluate_ConditionAbsentKeyFollowsTheOperator(t *testing.T) {
	positive := []string{
		"StringEquals", "StringEqualsIgnoreCase", "StringLike", "ArnEquals", "ArnLike",
		"NumericEquals", "NumericLessThan", "NumericGreaterThanEquals",
		"DateEquals", "DateLessThan", "IpAddress", "Bool", "BinaryEquals",
	}
	negated := []string{
		"StringNotEquals", "StringNotEqualsIgnoreCase", "StringNotLike",
		"ArnNotEquals", "ArnNotLike", "NumericNotEquals", "DateNotEquals", "NotIpAddress",
	}
	for _, op := range positive {
		t.Run(op+"/absent is false", func(t *testing.T) {
			assert.False(t, iamOperatorAllows(t, op, []string{"anything"}),
				"%s on an absent key must not match", op)
		})
	}
	for _, op := range negated {
		t.Run(op+"/absent is true", func(t *testing.T) {
			assert.True(t, iamOperatorAllows(t, op, []string{"anything"}),
				"%s on an absent key must match, per AWS", op)
		})
	}
}

// TestEvaluate_ConditionNegatedOperatorsAreTheNotNames is the drift tripwire between
// the negated-operator list and the operator switch itself: every operator AWS names
// with "Not" must be treated as negated, and nothing else may be.
//
// It works by observation rather than by reading either list — an operator is negated
// exactly when an absent key satisfies it — so a new operator added to only one of the
// two places fails here.
func TestEvaluate_ConditionNegatedOperatorsAreTheNotNames(t *testing.T) {
	all := []string{
		"StringEquals", "StringNotEquals", "StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase",
		"StringLike", "StringNotLike", "ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike",
		"NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
		"NumericGreaterThan", "NumericGreaterThanEquals",
		"DateEquals", "DateNotEquals", "DateLessThan", "DateLessThanEquals",
		"DateGreaterThan", "DateGreaterThanEquals",
		"Bool", "BinaryEquals", "IpAddress", "NotIpAddress",
	}
	for _, op := range all {
		t.Run(op, func(t *testing.T) {
			assert.Equal(t, strings.Contains(op, "Not"), iamOperatorAllows(t, op, []string{"x"}),
				"%q: an absent key matches exactly the operators AWS names with Not", op)
		})
	}
}

// TestEvaluate_ConditionStringIgnoreCase covers the two operators AWS uses in its own
// ec2:CreateTags example and substrate did not implement.
//
// AWS: "When you append IgnoreCase to the operator, you allow any existing tag value
// capitalization, such as preprod, Preprod, and PreProd, to resolve to true." The
// case-sensitive rows are here too, since the pair is only useful if it is a pair.
func TestEvaluate_ConditionStringIgnoreCase(t *testing.T) {
	cases := []struct {
		operator string
		ctxVal   string
		want     bool
	}{
		{"StringEqualsIgnoreCase", "preprod", true},
		{"StringEqualsIgnoreCase", "PreProd", true},
		{"StringEqualsIgnoreCase", "PREPROD", true},
		{"StringEqualsIgnoreCase", "storage", true},
		{"StringEqualsIgnoreCase", "prod", false},
		{"StringNotEqualsIgnoreCase", "PreProd", false},
		{"StringNotEqualsIgnoreCase", "prod", true},
		// The case-sensitive operator is unaffected, which is the whole point of the pair.
		{"StringEquals", "PreProd", false},
		{"StringEquals", "preprod", true},
	}
	for _, tc := range cases {
		t.Run(tc.operator+"/"+tc.ctxVal, func(t *testing.T) {
			assert.Equal(t, tc.want,
				iamOperatorAllows(t, tc.operator, []string{"preprod", "storage"}, tc.ctxVal))
		})
	}
}

// TestEvaluate_ConditionNumericFamily covers the six numeric operators.
//
// AWS: numeric operators compare "a key to an integer or decimal value". The quoted
// and unquoted spellings of a number are the same value, so "10" is used throughout and
// TestPolicyDocument_UnquotedNumbersAndBooleans pins the other spelling.
func TestEvaluate_ConditionNumericFamily(t *testing.T) {
	cases := []struct {
		operator string
		ctxVal   string
		values   []string
		want     bool
	}{
		{"NumericEquals", "10", []string{"10"}, true},
		{"NumericEquals", "10.0", []string{"10"}, true},
		{"NumericEquals", "11", []string{"10"}, false},
		{"NumericEquals", "11", []string{"10", "11"}, true},
		{"NumericNotEquals", "11", []string{"10"}, true},
		{"NumericNotEquals", "10", []string{"10"}, false},
		{"NumericLessThan", "9", []string{"10"}, true},
		{"NumericLessThan", "10", []string{"10"}, false},
		{"NumericLessThanEquals", "10", []string{"10"}, true},
		{"NumericGreaterThan", "11", []string{"10"}, true},
		{"NumericGreaterThan", "10", []string{"10"}, false},
		{"NumericGreaterThanEquals", "10", []string{"10"}, true},
		{"NumericLessThanEquals", "2.5", []string{"2.75"}, true},
		// A context value that is not a number cannot be compared: the positive
		// operators are unsatisfied, the negated one is satisfied.
		{"NumericLessThan", "ten", []string{"10"}, false},
		{"NumericEquals", "", []string{"10"}, false},
		{"NumericNotEquals", "ten", []string{"10"}, true},
		// An unparseable *policy* value is skipped rather than deciding the condition.
		{"NumericEquals", "10", []string{"ten", "10"}, true},
		{"NumericNotEquals", "10", []string{"ten"}, true},
		// None of NaN, an infinity, or Go's hexadecimal float syntax is an integer or a
		// decimal value: "Inf" must not satisfy NumericLessThan against every number
		// there is, and "0x1p4" must not compare equal to 16.
		{"NumericLessThan", "10", []string{"Inf"}, false},
		{"NumericGreaterThan", "Inf", []string{"10"}, false},
		{"NumericEquals", "NaN", []string{"NaN"}, false},
		{"NumericEquals", "0x10", []string{"16"}, false},
		{"NumericEquals", "0x1p4", []string{"16"}, false},
		{"NumericEquals", "16", []string{"0x1p4"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.operator+"/"+tc.ctxVal, func(t *testing.T) {
			assert.Equal(t, tc.want, iamOperatorAllows(t, tc.operator, tc.values, tc.ctxVal))
		})
	}
}

// TestEvaluate_ConditionDateFamily covers the six date operators.
//
// AWS: "You must specify date/time values with one of the W3C implementations of the
// ISO 8601 date formats or in epoch (UNIX) time." Both notations appear below, on both
// sides of the comparison and against each other.
func TestEvaluate_ConditionDateFamily(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		ctxVal   string
		values   []string
		want     bool
	}{
		{"after", "DateGreaterThan", "2020-06-01T00:00:00Z", []string{"2020-01-01T00:00:01Z"}, true},
		{"before", "DateGreaterThan", "2019-06-01T00:00:00Z", []string{"2020-01-01T00:00:01Z"}, false},
		{"less than", "DateLessThan", "2019-06-01T00:00:00Z", []string{"2020-01-01T00:00:01Z"}, true},
		{"equal", "DateEquals", "2020-01-01T00:00:01Z", []string{"2020-01-01T00:00:01Z"}, true},
		{"equal at or before", "DateLessThanEquals", "2020-01-01T00:00:01Z", []string{"2020-01-01T00:00:01Z"}, true},
		{"equal at or after", "DateGreaterThanEquals", "2020-01-01T00:00:01Z", []string{"2020-01-01T00:00:01Z"}, true},
		{"not equals", "DateNotEquals", "2020-06-01T00:00:00Z", []string{"2020-01-01T00:00:01Z"}, true},
		// An offset is a W3C timezone designator, not just "Z".
		{"offset", "DateEquals", "2020-01-01T00:00:00Z", []string{"2019-12-31T19:00:00-05:00"}, true},
		// The shorter W3C forms.
		{"date only", "DateLessThan", "2019-12-31T23:59:59Z", []string{"2020-01-01"}, true},
		{"month only", "DateGreaterThan", "2020-02-01T00:00:00Z", []string{"2020-01"}, true},
		{"year only", "DateGreaterThan", "2020-06-01T00:00:00Z", []string{"2020"}, true},
		// Epoch on either side, and the two notations compared against each other.
		{"epoch policy value", "DateGreaterThan", "2020-06-01T00:00:00Z", []string{"1577836800"}, true},
		{"epoch context value", "DateLessThan", "1577836800", []string{"2020-06-01T00:00:00Z"}, true},
		{"fractional epoch", "DateGreaterThan", "1577836800.5", []string{"1577836800"}, true},
		// A four-digit value is read as a W3C year rather than as an epoch second, which
		// is the one place the two notations collide. Read as epoch, "2020" would be
		// 1970-01-01T00:33:40Z and this would answer false.
		{"four digits are a year", "DateGreaterThan", "2019-01-01T00:00:00Z", []string{"2020"}, false},
		// Unparseable values, in both directions.
		{"bad context value", "DateLessThan", "not-a-date", []string{"2020-01-01"}, false},
		{"bad context value negated", "DateNotEquals", "not-a-date", []string{"2020-01-01"}, true},
		{"bad policy value skipped", "DateEquals", "2020-01-01", []string{"nope", "2020-01-01"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.operator+"/"+tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, iamOperatorAllows(t, tc.operator, tc.values, tc.ctxVal))
		})
	}
}

// TestEvaluate_ConditionIPFamily covers IpAddress and NotIpAddress.
//
// AWS: "The value must be in the standard CIDR format (for example, 203.0.113.0/24 or
// 2001:DB8:1234:5678::/64). If you specify an IP address without the associated routing
// prefix, IAM uses the default prefix value of /32." Both address families are covered,
// including the IPv6 host route that sentence does not describe.
func TestEvaluate_ConditionIPFamily(t *testing.T) {
	cases := []struct {
		name     string
		operator string
		ctxVal   string
		values   []string
		want     bool
	}{
		{"in range", "IpAddress", "203.0.113.1", []string{"203.0.113.0/24"}, true},
		{"out of range", "IpAddress", "198.51.100.1", []string{"203.0.113.0/24"}, false},
		{"second value", "IpAddress", "198.51.100.1", []string{"203.0.113.0/24", "198.51.100.0/24"}, true},
		{"not in range", "NotIpAddress", "198.51.100.1", []string{"203.0.113.0/24"}, true},
		{"not, but in range", "NotIpAddress", "203.0.113.1", []string{"203.0.113.0/24"}, false},
		// A bare address is a host route: itself and nothing else.
		{"bare address matches itself", "IpAddress", "203.0.113.1", []string{"203.0.113.1"}, true},
		{"bare address matches nothing else", "IpAddress", "203.0.113.2", []string{"203.0.113.1"}, false},
		// IPv6, including the mixed list AWS recommends writing.
		{"v6 in range", "IpAddress", "2001:db8:1234:5678::1", []string{"2001:db8:1234:5678::/64"}, true},
		{"v6 out of range", "IpAddress", "2001:db8:1234:9999::1", []string{"2001:db8:1234:5678::/64"}, false},
		{"mixed list", "IpAddress", "2001:db8:1234:5678::1", []string{"203.0.113.0/24", "2001:db8:1234:5678::/64"}, true},
		// A bare IPv6 address is its own /128. Taking AWS's "/32" literally would make
		// this one address stand for 2^96 of them, and the second row would pass.
		{"bare v6 matches itself", "IpAddress", "2001:db8::1", []string{"2001:db8::1"}, true},
		{"bare v6 is not a /32", "IpAddress", "2001:db8:ffff::1", []string{"2001:db8::1"}, false},
		// Families do not cross.
		{"v4 against a v6 prefix", "IpAddress", "203.0.113.1", []string{"2001:db8::/32"}, false},
		// A malformed value on either side matches nothing, so a typo cannot satisfy
		// NotIpAddress either.
		{"malformed policy value", "IpAddress", "203.0.113.1", []string{"203.0.113.0/33"}, false},
		{"malformed context value", "IpAddress", "not-an-ip", []string{"203.0.113.0/24"}, false},
		{"malformed policy value, negated", "NotIpAddress", "203.0.113.1", []string{"not-a-cidr"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.operator+"/"+tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, iamOperatorAllows(t, tc.operator, tc.values, tc.ctxVal))
		})
	}
}

// TestEvaluate_ConditionBinaryEquals covers the base64 comparison.
//
// AWS: BinaryEquals "compares the value of the specified key byte for byte against a
// base-64 encoded representation of the binary value in the policy", and its own
// example table compares two base64 strings.
func TestEvaluate_ConditionBinaryEquals(t *testing.T) {
	const encoded = "QmluYXJ5VmFsdWVJbkJhc2U2NA=="
	assert.True(t, iamOperatorAllows(t, "BinaryEquals", []string{encoded}, encoded))
	assert.False(t, iamOperatorAllows(t, "BinaryEquals", []string{encoded}, "ASIAIOSFODNN7EXAMPLE"),
		"AWS's own no-match row")
	assert.False(t, iamOperatorAllows(t, "BinaryEquals", []string{"not base64!!"}, "not base64!!"),
		"a policy value that is not base64 encodes no binary value")
}

// TestEvaluate_ConditionARNComponentsAreCompared pins AWS's component rule: "Each of
// the six colon-delimited components of the ARN is checked separately."
//
// Substrate ran one glob over the whole string, so a `*` in one component matched
// across a colon into the next — the row named below granted a pattern that never
// mentioned the account it authorized (#714).
func TestEvaluate_ConditionARNComponentsAreCompared(t *testing.T) {
	const topic = "arn:aws:sns:us-east-1:123456789012:TOPIC-ID"
	cases := []struct {
		name    string
		pattern string
		want    bool
	}{
		{"exact", topic, true},
		{"account differs", "arn:aws:sns:us-east-1:777788889999:TOPIC-ID", false},
		{"wildcard in one component", "arn:aws:sns:us-east-1:*:TOPIC-ID", true},
		{"single-character wildcard", "arn:aws:sns:us-east-?:123456789012:TOPIC-ID", true},
		{"wildcard resource", "arn:aws:sns:us-east-1:123456789012:*", true},
		// The bug: five components, whose trailing `*` used to eat the colon between
		// the account and the resource.
		{"a star must not cross a colon", "arn:aws:sns:*:TOPIC-ID", false},
		{"nor may a leading one", "arn:aws:*:TOPIC-ID", false},
		// A pattern naming fewer components has the missing trailing ones filled with
		// `*`, so a deliberately broad pattern keeps working — and a Deny written that
		// way is not silently narrowed.
		{"incomplete pattern is completed", "arn:aws:sns:us-east-1", true},
		{"incomplete pattern still checks what it names", "arn:aws:sqs:us-east-1", false},
		{"partition only", "arn:aws", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"ArnEquals", "ArnLike"} {
				assert.Equal(t, tc.want, iamOperatorAllows(t, op, []string{tc.pattern}, topic),
					"%s %q: AWS documents ArnEquals and ArnLike as behaving identically", op, tc.pattern)
			}
			for _, op := range []string{"ArnNotEquals", "ArnNotLike"} {
				assert.Equal(t, !tc.want, iamOperatorAllows(t, op, []string{tc.pattern}, topic),
					"%s %q must be the negation of its positive pair", op, tc.pattern)
			}
		})
	}

	t.Run("a context value with too few components matches no full pattern", func(t *testing.T) {
		assert.False(t, iamOperatorAllows(t, "ArnEquals",
			[]string{"arn:aws:sns:us-east-1:123456789012:TOPIC-ID"}, "arn:aws:sns"))
	})
}

// TestEvaluate_ConditionArnNotEqualsHonoursWildcards is the second ARN bug: the negated
// operator compared with == , so a Deny written with a wildcard was inert.
//
// AWS: "The ArnNotEquals and ArnNotLike condition operators behave identically" —
// negated matching, wildcards and all.
func TestEvaluate_ConditionArnNotEqualsHonoursWildcards(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"*"}, Resource: emulator.StringOrSlice{"*"}},
			{
				Effect:   emulator.IAMEffectDeny,
				Action:   emulator.StringOrSlice{"*"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"ArnNotEquals": {"aws:PrincipalArn": emulator.StringOrSlice{"arn:aws:iam::123456789012:role/*"}},
				},
			},
		},
	}
	decide := func(principalArn string) string {
		return emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
			Action: "s3:GetObject", Resource: "*",
			Context: map[string]string{"aws:PrincipalArn": principalArn},
		}).Decision
	}
	assert.Equal(t, emulator.DecisionAllow, decide("arn:aws:iam::123456789012:role/admin"),
		"the wildcard must match, so the Deny does not fire")
	assert.Equal(t, emulator.DecisionDeny, decide("arn:aws:iam::123456789012:user/eve"),
		"a principal outside the pattern must still be denied")
	assert.Equal(t, emulator.DecisionDeny, decide("arn:aws:iam::777788889999:role/admin"),
		"a role in another account is outside the pattern")
}

// TestEvaluate_ConditionIfExists covers the suffix AWS allows on every operator but
// Null.
//
// AWS: "If the condition key is present in the context of the request, process the key
// as specified in the policy. If the key is not present, evaluate the condition element
// as true." Both halves are asserted: a suffix that ignored a present key would pass a
// test written only about absence.
func TestEvaluate_ConditionIfExists(t *testing.T) {
	t.Run("absent key is a match", func(t *testing.T) {
		assert.True(t, iamOperatorAllows(t, "StringLikeIfExists", []string{"t1.*", "t2.*"}))
	})
	t.Run("present key is still evaluated", func(t *testing.T) {
		assert.True(t, iamOperatorAllows(t, "StringLikeIfExists", []string{"t1.*", "t2.*"}, "t1.micro"),
			"AWS's own match row")
		assert.False(t, iamOperatorAllows(t, "StringLikeIfExists", []string{"t1.*", "t2.*"}, "m2.micro"),
			"AWS's own no-match row")
	})
	t.Run("every family takes the suffix", func(t *testing.T) {
		for _, op := range []string{
			"StringEqualsIfExists", "StringNotEqualsIfExists", "StringEqualsIgnoreCaseIfExists",
			"ArnEqualsIfExists", "ArnNotLikeIfExists", "NumericLessThanIfExists",
			"DateGreaterThanIfExists", "IpAddressIfExists", "NotIpAddressIfExists",
			"BoolIfExists", "BinaryEqualsIfExists",
		} {
			assert.True(t, iamOperatorAllows(t, op, []string{"anything"}),
				"%s on an absent key must match", op)
		}
	})
	t.Run("Null takes no suffix", func(t *testing.T) {
		// AWS: IfExists may be added to "any condition operator name except the Null
		// condition". Both spellings of the intent must fail, since either would
		// otherwise be a grant.
		assert.False(t, iamOperatorAllows(t, "NullIfExists", []string{"true"}))
		assert.False(t, iamOperatorAllows(t, "NullIfExists", []string{"false"}))
		assert.False(t, iamOperatorAllows(t, "NullIfExists", []string{"true"}, "present"))
	})
	t.Run("an unrecognized operator gains nothing from the suffix", func(t *testing.T) {
		assert.False(t, iamOperatorAllows(t, "NotAnOperatorIfExists", []string{"x"}))
	})
	t.Run("a Deny with a negated IfExists still fires on an absent key", func(t *testing.T) {
		// AWS: "If you are using an "Effect": "Deny" element with a negated condition
		// operator like StringNotEqualsIfExists, the request is still denied even if the
		// condition key is not present."
		doc := emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{
				{Effect: emulator.IAMEffectAllow, Action: emulator.StringOrSlice{"*"}, Resource: emulator.StringOrSlice{"*"}},
				{
					Effect:   emulator.IAMEffectDeny,
					Action:   emulator.StringOrSlice{"*"},
					Resource: emulator.StringOrSlice{"*"},
					Condition: map[string]map[string]emulator.StringOrSlice{
						"StringNotEqualsIfExists": {"test:Key": emulator.StringOrSlice{"expected"}},
					},
				},
			},
		}
		result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
			Action: "s3:GetObject", Resource: "*",
		})
		assert.Equal(t, emulator.DecisionDeny, result.Decision)
	})
}

// TestEvaluate_ConditionIfExistsUnderASetQualifier records the one resolution AWS does
// not publish.
//
// ForAnyValue says an absent key is false; IfExists says an absent key is true. AWS
// documents no answer, so substrate reads IfExists as the more specific annotation —
// the author wrote it about exactly this case, and AWS's gloss is emphatic that other
// elements "can still result in a nonmatch, but not a missing key when checked with
// ...IfExists".
func TestEvaluate_ConditionIfExistsUnderASetQualifier(t *testing.T) {
	absent := func(operator string) bool {
		doc := iamOperatorPolicy(operator, "aws:TagKeys", "Env")
		return emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
			Action: "s3:GetObject", Resource: "*",
		}).Decision == emulator.DecisionAllow
	}
	assert.False(t, absent("ForAnyValue:StringEquals"),
		"without the suffix, ForAnyValue's own rule holds")
	assert.True(t, absent("ForAnyValue:StringEqualsIfExists"),
		"with it, the more specific annotation wins")
	assert.True(t, absent("ForAllValues:StringEqualsIfExists"),
		"ForAllValues already answers true, so the suffix changes nothing")

	// A present key is evaluated either way — the suffix speaks only to absence.
	present := func(operator string, values ...string) bool {
		doc := iamOperatorPolicy(operator, "aws:TagKeys", values...)
		return emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
			Action: "s3:GetObject", Resource: "*",
			MultiContext: map[string][]string{"aws:TagKeys": {"Env", "Owner"}},
		}).Decision == emulator.DecisionAllow
	}
	assert.True(t, present("ForAnyValue:StringEqualsIfExists", "Env"))
	assert.False(t, present("ForAnyValue:StringEqualsIfExists", "Nope"))
	assert.False(t, present("ForAllValues:StringEqualsIfExists", "Env"),
		"Owner is not in the policy's list")

	// A set-qualified Null is refused whether or not the key is there. Quantifying an
	// existence test over the values whose existence it is testing has no meaning, and
	// AWS defines neither form — but `ForAnyValue:Null` with "false" would *match* a
	// present key if the qualifier arms evaluated Null like any other operator, which is
	// a grant nobody wrote.
	assert.False(t, present("ForAnyValue:Null", "false"))
	assert.False(t, present("ForAllValues:Null", "false"))
	assert.False(t, present("ForAnyValue:Null", "true"))
}

// TestEvaluate_ConditionPresentButEmptyIsAbsent pins the reading a key with an empty
// value gets.
//
// AWS speaks of a value that "resolves to a null dataset, such as an empty string", and
// Null's own wording is "the key exists and its value is not null". So an empty value is
// null, which is the reading substrate's Null operator has always had — and now the one
// the rest of the operators use.
func TestEvaluate_ConditionPresentButEmptyIsAbsent(t *testing.T) {
	assert.False(t, iamOperatorAllows(t, "StringEquals", []string{"prod"}, ""))
	assert.True(t, iamOperatorAllows(t, "StringNotEquals", []string{"prod"}, ""))
	assert.True(t, iamOperatorAllows(t, "StringEqualsIfExists", []string{"prod"}, ""))
	assert.True(t, iamOperatorAllows(t, "Null", []string{"true"}, ""),
		"Null must still see the empty value rather than being told the key is absent")
	assert.False(t, iamOperatorAllows(t, "Null", []string{"false"}, ""))
	assert.True(t, iamOperatorAllows(t, "Null", []string{"false"}, "prod"))
}

// TestPolicyDocument_UnquotedNumbersAndBooleans pins the IAM grammar's rule that
// substrate rejected outright: "Values are enclosed in quotation marks. Quotation marks
// are optional for numeric and Boolean values." Each document below is one real IAM
// accepts and substrate answered MalformedPolicyDocument for.
func TestPolicyDocument_UnquotedNumbersAndBooleans(t *testing.T) {
	cases := []struct {
		name   string
		policy string
		ctxVal string
		want   string
	}{
		{
			name: "unquoted Boolean",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
				`"Condition":{"Bool":{"test:Key":false}}}]}`,
			ctxVal: "false",
			want:   emulator.DecisionAllow,
		},
		{
			name: "unquoted Boolean, no match",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
				`"Condition":{"Bool":{"test:Key":false}}}]}`,
			ctxVal: "true",
			want:   emulator.DecisionImplicitDeny,
		},
		{
			name: "unquoted number",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
				`"Condition":{"NumericLessThanEquals":{"test:Key":10}}}]}`,
			ctxVal: "10",
			want:   emulator.DecisionAllow,
		},
		{
			name: "unquoted number, no match",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
				`"Condition":{"NumericLessThanEquals":{"test:Key":10}}}]}`,
			ctxVal: "11",
			want:   emulator.DecisionImplicitDeny,
		},
		{
			name: "a mixed list",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
				`"Condition":{"NumericEquals":{"test:Key":[10,"11",12]}}}]}`,
			ctxVal: "11",
			want:   emulator.DecisionAllow,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var doc emulator.PolicyDocument
			require.NoError(t, json.Unmarshal([]byte(tc.policy), &doc),
				"the IAM grammar declares this document legal")
			result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
				Action: "s3:GetObject", Resource: "*",
				Context: map[string]string{"test:Key": tc.ctxVal},
			})
			assert.Equal(t, tc.want, result.Decision)
		})
	}
}

// TestStringOrSlice_UnmarshalKeepsTheNumbersSpelling pins that a number reaches the
// comparison as the document spelled it. Routing it through a float64 would render 10
// as "1e+01" and a large integer as a rounded approximation.
func TestStringOrSlice_UnmarshalKeepsTheNumbersSpelling(t *testing.T) {
	cases := []struct {
		json string
		want emulator.StringOrSlice
	}{
		{`10`, emulator.StringOrSlice{"10"}},
		{`10.5`, emulator.StringOrSlice{"10.5"}},
		{`-3`, emulator.StringOrSlice{"-3"}},
		{`123456789012345678901234567890`, emulator.StringOrSlice{"123456789012345678901234567890"}},
		{`true`, emulator.StringOrSlice{"true"}},
		{`false`, emulator.StringOrSlice{"false"}},
		{`"already a string"`, emulator.StringOrSlice{"already a string"}},
		{`["a",1,true]`, emulator.StringOrSlice{"a", "1", "true"}},
		{`[]`, emulator.StringOrSlice{}},
	}
	for _, tc := range cases {
		t.Run(tc.json, func(t *testing.T) {
			var got emulator.StringOrSlice
			require.NoError(t, json.Unmarshal([]byte(tc.json), &got))
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("null, an object and a nested array are still refused", func(t *testing.T) {
		for _, bad := range []string{`null`, `{"k":"v"}`, `[["a"]]`, `[{"k":"v"}]`, `[null]`} {
			var got emulator.StringOrSlice
			assert.Error(t, json.Unmarshal([]byte(bad), &got), "%s is not a policy value", bad)
		}
	})
}
