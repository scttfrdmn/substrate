package emulator

import (
	"fmt"
	"net/http"
	"sort"
)

// iamValidateConditionOperators reports the first condition operator in doc that AWS
// would refuse when the document is submitted.
//
// Substrate accepted any operator name at write time, stored it, and discarded it at
// evaluation — [conditionKeyMatches] documents that reading and why it is the only safe
// one: a typo must not become a grant. But it leaves a caller with no way to learn about
// the typo. AWS validates the name when the document is submitted and answers
// MalformedPolicyDocument / 400 — "The request was rejected because the policy document
// was malformed. The error message describes the specific error" — so a `"NotAnOperator"`
// that real IAM rejects outright was stored here and then silently ignored, and a policy
// that looked attached did nothing (#730).
//
// The operator vocabulary is not restated here. It is [condOperatorRecognized], which
// asks [condEvaluate] itself, so an operator the evaluator learns is accepted at write
// time in the same commit — the drift a second list would introduce is the drift #714 was
// filed about, in the other direction.
//
// The two rules beyond "the name is recognized" are the two the evaluator applies:
//
//   - `NullIfExists` is refused. AWS: "You can add IfExists to the end of any condition
//     operator name except the Null condition." Null already answers the question
//     IfExists asks.
//   - A set-qualified `Null` — `ForAllValues:Null`, `ForAnyValue:Null` — is refused. AWS
//     defines neither, and quantifying an existence test over the values whose existence
//     it tests has no meaning.
//
// A set qualifier over any *other* recognized operator is accepted, including the
// combinations AWS's condition-operator page does not tabulate (it lists the String,
// Bool and ARN forms). Its set-operator reference describes the qualifiers generally
// rather than as an enumerated set, so refusing `ForAllValues:NumericLessThan` would be
// substrate inventing a restriction; the evaluator already treats it as recognized.
//
// Returns nil for a document with no conditions, which is most of them.
func iamValidateConditionOperators(doc PolicyDocument) error {
	for _, stmt := range doc.Statement {
		if len(stmt.Condition) == 0 {
			continue
		}
		// Sorted, because a Go map's iteration order is random and the message names
		// one operator: two submissions of the same bad document must be refused with
		// the same sentence, or a recorded run does not replay to the same response.
		operators := make([]string, 0, len(stmt.Condition))
		for operator := range stmt.Condition {
			operators = append(operators, operator)
		}
		sort.Strings(operators)

		for _, operator := range operators {
			if err := iamValidateConditionOperator(operator); err != nil {
				return err
			}
		}
	}
	return nil
}

// iamValidateConditionOperator reports whether one Condition key is an operator name AWS
// accepts, naming the operator in the refusal so the caller can find the typo.
func iamValidateConditionOperator(operator string) error {
	qualifier, qualified := splitConditionOperator(operator)
	if qualifier != "" && qualifier != condForAllValues && qualifier != condForAnyValue {
		return &AWSError{
			Code: "MalformedPolicyDocument",
			Message: fmt.Sprintf(
				"Condition operator %q is not valid: %q is not a set operator. The only set operators are %s and %s.",
				operator, qualifier, condForAllValues, condForAnyValue),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	base, ifExists := condSplitIfExists(qualified)
	if base == "Null" && ifExists {
		return &AWSError{
			Code: "MalformedPolicyDocument",
			Message: fmt.Sprintf(
				"Condition operator %q is not valid: %s may be added to any condition operator except Null.",
				operator, condIfExistsSuffix),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if base == "Null" && qualifier != "" {
		return &AWSError{
			Code: "MalformedPolicyDocument",
			Message: fmt.Sprintf(
				"Condition operator %q is not valid: Null cannot be qualified by %s.",
				operator, qualifier),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if !condOperatorRecognized(base) {
		return &AWSError{
			Code:       "MalformedPolicyDocument",
			Message:    fmt.Sprintf("Condition operator %q is not valid.", operator),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}
