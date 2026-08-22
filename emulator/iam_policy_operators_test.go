package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #730: a condition operator AWS does not recognize is refused when the document is
// submitted, rather than stored and then silently discarded at evaluation.
//
// The evaluator's reading does not change and is not in tension with this. A stored
// document carrying an unknown operator still matches nothing — conditionKeyMatches
// documents why that is the only safe answer, and #714 is what happens when it is not.
// What changes is that a caller now learns about the typo instead of watching an attached
// policy do nothing.

// iamOperatorDoc is a policy document whose single statement carries one condition
// operator, which is the only part these tests vary.
func iamOperatorDoc(operator string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject",` +
		`"Resource":"*","Condition":{"` + operator + `":{"aws:username":"alice"}}}]}`
}

// iamOperatorSubmission is one of the four paths a caller submits a policy document
// through. Each is here because each unmarshalled the document and checked only that it
// was JSON.
type iamOperatorSubmission struct {
	name      string
	operation string
	setup     func(t *testing.T, srv *emulator.Server)
	body      func(document string) map[string]any
}

// iamOperatorSubmissions returns the four submission paths, each already carrying the
// entity it needs.
//
// PutUserPolicy stands for all three inline-policy operations: PutRolePolicy and
// PutGroupPolicy reach the same putInlinePolicy, so covering one covers the shared
// validator rather than the parameter decoding around it.
func iamOperatorSubmissions() []iamOperatorSubmission {
	return []iamOperatorSubmission{
		{
			name:      "CreateRole",
			operation: "CreateRole",
			body: func(document string) map[string]any {
				return map[string]any{"RoleName": "r1", "AssumeRolePolicyDocument": document}
			},
		},
		{
			name:      "UpdateAssumeRolePolicy",
			operation: "UpdateAssumeRolePolicy",
			setup: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				require.Equal(t, http.StatusOK,
					iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "r1"}).StatusCode)
			},
			body: func(document string) map[string]any {
				return map[string]any{"RoleName": "r1", "PolicyDocument": document}
			},
		},
		{
			name:      "CreatePolicy",
			operation: "CreatePolicy",
			body: func(document string) map[string]any {
				return map[string]any{"PolicyName": "p1", "PolicyDocument": document}
			},
		},
		{
			name:      "PutUserPolicy",
			operation: "PutUserPolicy",
			setup: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				require.Equal(t, http.StatusOK,
					iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "u1"}).StatusCode)
			},
			body: func(document string) map[string]any {
				return map[string]any{
					"UserName": "u1", "PolicyName": "inline", "PolicyDocument": document,
				}
			},
		},
	}
}

// TestIAM_ConditionOperator_UnknownIsRefusedOnEverySubmissionPath is the issue's own
// reproduction, run through all four doors.
//
// AWS publishes MalformedPolicyDocument / 400 for every one of them — "The request was
// rejected because the policy document was malformed. The error message describes the
// specific error" — which is what licenses substrate naming the operator in the message.
func TestIAM_ConditionOperator_UnknownIsRefusedOnEverySubmissionPath(t *testing.T) {
	t.Parallel()

	for _, sub := range iamOperatorSubmissions() {
		t.Run(sub.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			if sub.setup != nil {
				sub.setup(t, srv)
			}

			resp := iamRequest(t, srv, sub.operation, sub.body(iamOperatorDoc("NotAnOperator")))
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "MalformedPolicyDocument", result["__type"])
			assert.Contains(t, result["message"], "NotAnOperator",
				"the caller has to be told which operator, or the message does not find the typo")
		})
	}
}

// TestIAM_ConditionOperator_RefusalStoresNothing pins that the refusal runs before the
// write.
//
// A stored document the caller was told was malformed is worse than no validation: a later
// GetUserPolicy would report a policy AWS never accepted, and the run would not replay
// against real IAM.
func TestIAM_ConditionOperator_RefusalStoresNothing(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	require.Equal(t, http.StatusOK,
		iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "u1"}).StatusCode)

	require.Equal(t, http.StatusBadRequest, iamRequest(t, srv, "PutUserPolicy", map[string]any{
		"UserName": "u1", "PolicyName": "inline",
		"PolicyDocument": iamOperatorDoc("NotAnOperator"),
	}).StatusCode)

	resp := iamRequest(t, srv, "GetUserPolicy", map[string]any{
		"UserName": "u1", "PolicyName": "inline",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "NoSuchEntity", result["__type"])

	// And a valid document through the same path is stored, so the refusal is about the
	// operator and not about the path.
	require.Equal(t, http.StatusOK, iamRequest(t, srv, "PutUserPolicy", map[string]any{
		"UserName": "u1", "PolicyName": "inline",
		"PolicyDocument": iamOperatorDoc("StringEquals"),
	}).StatusCode)
	assert.Equal(t, http.StatusOK, iamRequest(t, srv, "GetUserPolicy", map[string]any{
		"UserName": "u1", "PolicyName": "inline",
	}).StatusCode)
}

// TestIAM_ConditionOperator_AcceptedForms walks the vocabulary AWS documents, so a policy a
// consumer copies out of the AWS documentation submits.
//
// The suffix and qualifier cases are the ones a hand-written name list gets wrong:
// StringLikeIfExists and ForAnyValue:StringEquals are two operators AWS spells as one JSON
// key each, and both have to survive being split apart and recognized.
func TestIAM_ConditionOperator_AcceptedForms(t *testing.T) {
	t.Parallel()

	operators := []string{
		"StringEquals", "StringNotEquals", "StringEqualsIgnoreCase",
		"StringNotEqualsIgnoreCase", "StringLike", "StringNotLike",
		"NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
		"NumericGreaterThan", "NumericGreaterThanEquals",
		"DateEquals", "DateNotEquals", "DateLessThan", "DateLessThanEquals",
		"DateGreaterThan", "DateGreaterThanEquals",
		"Bool", "BinaryEquals", "IpAddress", "NotIpAddress",
		"ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike",
		"Null",
		// AWS: "You can add IfExists to the end of any condition operator name except
		// the Null condition — for example, StringLikeIfExists."
		"StringLikeIfExists", "ArnEqualsIfExists", "NumericLessThanIfExists",
		// The set operators, over the operators AWS's own multivalued tables list and
		// one it does not: its set-operator reference describes the qualifiers
		// generally, so refusing the untabulated combinations would be substrate
		// inventing a restriction.
		"ForAllValues:StringEquals", "ForAnyValue:StringLike",
		"ForAllValues:ArnNotLike", "ForAnyValue:Bool",
		"ForAllValues:NumericLessThan",
		// A qualifier and a suffix on one operator, which is the shape that has to
		// survive both splits.
		"ForAnyValue:StringEqualsIfExists",
	}

	for _, operator := range operators {
		t.Run(operator, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
				"PolicyName": "p1", "PolicyDocument": iamOperatorDoc(operator),
			})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}

// TestIAM_ConditionOperator_RefusedForms covers the names AWS defines rules against, each
// with the rule that refuses it.
func TestIAM_ConditionOperator_RefusedForms(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		operator string
		// wantMessage is a fragment naming the rule, so a refusal for the wrong reason
		// fails rather than passing on the status code alone.
		wantMessage string
	}{
		// AWS: IfExists may be added to any operator "except the Null condition".
		"NullIfExists": {"NullIfExists", "except Null"},
		// AWS defines no set-qualified Null, and quantifying an existence test over the
		// values whose existence it tests has no meaning.
		"ForAllValues:Null": {"ForAllValues:Null", "cannot be qualified"},
		"ForAnyValue:Null":  {"ForAnyValue:Null", "cannot be qualified"},
		// A qualifier that is not one of the two AWS defines.
		"unknown qualifier": {"ForSomeValues:StringEquals", "not a set operator"},
		// A recognized qualifier over an unrecognized operator: the qualifier must not
		// carry the operator past the check, which is the #714 shape at write time.
		"qualified unknown operator": {"ForAllValues:NotAnOperator", "not valid"},
		// Case matters: AWS's case-insensitivity covers condition key *names*, not
		// operator names (#704), and iam_set_qualifiers_test.go pins the same reading at
		// evaluation.
		"wrong case":     {"stringequals", "not valid"},
		"bare IfExists":  {"IfExists", "not valid"},
		"empty operator": {"", "not valid"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
				"PolicyName": "p1", "PolicyDocument": iamOperatorDoc(tc.operator),
			})
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)

			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "MalformedPolicyDocument", result["__type"])
			assert.Contains(t, result["message"], tc.wantMessage)
		})
	}
}

// TestIAM_ConditionOperator_RefusalIsDeterministic pins the sort.
//
// A statement's Condition is a map, so its iteration order is random, and the message names
// one operator. Without the sort a document with two bad operators would be refused with
// either message, so a recorded run would not replay to the same response — which is the
// property the whole event log rests on.
func TestIAM_ConditionOperator_RefusalIsDeterministic(t *testing.T) {
	t.Parallel()

	document := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject",` +
		`"Resource":"*","Condition":{` +
		`"ZzzOperator":{"aws:username":"alice"},` +
		`"AaaOperator":{"aws:username":"alice"},` +
		`"MmmOperator":{"aws:username":"alice"}}}]}`

	// Twenty submissions, because one pass over a three-key map has a good chance of
	// starting at the same key by luck.
	for range 20 {
		srv := newIAMTestServer(t)
		resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
			"PolicyName": "p1", "PolicyDocument": document,
		})
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result map[string]any
		decodeIAMXML(t, resp, &result)
		require.Contains(t, result["message"], "AaaOperator",
			"the lexicographically first bad operator, every time")
	}
}

// TestIAM_ConditionOperator_ValidationIsNotOnTheReadPath is the placement decision, pinned.
//
// PolicyDocument is unmarshalled on every read from state, so validating in
// UnmarshalJSON would make an already-stored document unloadable — a store written by an
// older substrate, or by a seed, would stop answering. Validation is on the submission
// paths only, and this asserts the consequence: a document that reached state before the
// rule existed still loads, evaluates, and matches nothing.
func TestIAM_ConditionOperator_ValidationIsNotOnTheReadPath(t *testing.T) {
	t.Parallel()

	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"s3:GetObject"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"NotAnOperator": {"aws:username": {"alice"}},
			},
		}},
	}

	result := emulator.Evaluate([]emulator.PolicyDocument{doc}, emulator.EvaluationRequest{
		Principal: "arn:aws:iam::123456789012:user/alice",
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::bucket/key",
		Context:   map[string]string{"aws:username": "alice"},
	})
	assert.NotEqual(t, emulator.DecisionAllow, result.Decision,
		"an operator substrate cannot evaluate must not grant, whatever the key says")
}
