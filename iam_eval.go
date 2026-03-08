package substrate

import "strings"

// Decision constants for IAM policy evaluation results.
const (
	// DecisionAllow indicates access was explicitly allowed.
	DecisionAllow = "Allow"

	// DecisionDeny indicates access was explicitly denied.
	DecisionDeny = "Deny"

	// DecisionImplicitDeny indicates no matching allow was found.
	DecisionImplicitDeny = "ImplicitDeny"
)

// EvaluationResult holds the outcome of an IAM policy evaluation.
type EvaluationResult struct {
	// Decision is one of DecisionAllow, DecisionDeny, or DecisionImplicitDeny.
	Decision string

	// Reason is a human-readable explanation of the decision.
	Reason string

	// MatchedStatements is the list of statement Sid values that matched.
	MatchedStatements []string
}

// EvaluationRequest contains the inputs for an IAM policy evaluation.
type EvaluationRequest struct {
	// Principal is the ARN of the caller.
	Principal string

	// Action is the AWS API action being requested (e.g., "s3:GetObject").
	Action string

	// Resource is the ARN of the target resource.
	Resource string

	// Context provides condition key values for condition evaluation.
	Context map[string]string
}

// Evaluate applies the AWS IAM policy evaluation algorithm across all provided
// policy documents and returns the access decision for req.
//
// The evaluation order follows the AWS rules:
//  1. Any matching Deny statement → DecisionDeny (returns immediately).
//  2. Any matching Allow statement → DecisionAllow.
//  3. No match → DecisionImplicitDeny.
//
// Allow matching never short-circuits — the full statement list is scanned
// to ensure no explicit Deny is present before returning Allow.
func Evaluate(documents []PolicyDocument, req EvaluationRequest) EvaluationResult {
	var allowed bool
	var allowedSids []string

	for _, doc := range documents {
		for _, stmt := range doc.Statement {
			if !statementMatches(stmt, req) {
				continue
			}
			if stmt.Effect == IAMEffectDeny {
				// Explicit deny always wins — return immediately.
				return EvaluationResult{
					Decision:          DecisionDeny,
					Reason:            "explicit deny in policy statement",
					MatchedStatements: []string{stmt.Sid},
				}
			}
			if stmt.Effect == IAMEffectAllow {
				allowed = true
				allowedSids = append(allowedSids, stmt.Sid)
			}
		}
	}

	if allowed {
		return EvaluationResult{
			Decision:          DecisionAllow,
			Reason:            "allowed by policy statement",
			MatchedStatements: allowedSids,
		}
	}

	return EvaluationResult{
		Decision: DecisionImplicitDeny,
		Reason:   "no matching allow statement",
	}
}

// statementMatches returns true if stmt covers the action, resource, and
// conditions specified in req.
func statementMatches(stmt PolicyStatement, req EvaluationRequest) bool {
	if !actionMatches(stmt, req.Action) {
		return false
	}
	if !resourceMatches(stmt, req.Resource) {
		return false
	}
	if !conditionMatches(stmt.Condition, req.Context) {
		return false
	}
	return true
}

// actionMatches returns true when req.Action is covered by stmt.Action or,
// for NotAction statements, when req.Action is NOT in stmt.NotAction.
func actionMatches(stmt PolicyStatement, action string) bool {
	lower := strings.ToLower(action)
	if len(stmt.NotAction) > 0 {
		for _, a := range stmt.NotAction {
			if globMatch(strings.ToLower(a), lower) {
				return false
			}
		}
		return true
	}
	for _, a := range stmt.Action {
		if globMatch(strings.ToLower(a), lower) {
			return true
		}
	}
	return false
}

// resourceMatches returns true when req.Resource is covered by stmt.Resource or,
// for NotResource statements, when req.Resource is NOT in stmt.NotResource.
func resourceMatches(stmt PolicyStatement, resource string) bool {
	if resource == "" {
		return true
	}
	if len(stmt.NotResource) > 0 {
		for _, r := range stmt.NotResource {
			if globMatch(r, resource) {
				return false
			}
		}
		return true
	}
	for _, r := range stmt.Resource {
		if globMatch(r, resource) {
			return true
		}
	}
	return false
}

// conditionMatches evaluates all condition operators against ctx.
// Multiple operators use AND semantics. Multiple keys within one operator
// use AND semantics. Multiple values for one key use OR semantics.
func conditionMatches(conditions map[string]map[string]StringOrSlice, ctx map[string]string) bool {
	for operator, keyValues := range conditions {
		for condKey, condValues := range keyValues {
			ctxVal := ctx[condKey]
			if !evaluateConditionKey(operator, ctxVal, condValues) {
				return false
			}
		}
	}
	return true
}

// evaluateConditionKey evaluates a single condition key against the context value.
// Multiple values for the same key use OR semantics.
func evaluateConditionKey(operator, ctxVal string, condValues StringOrSlice) bool {
	switch operator {
	case "StringEquals":
		for _, v := range condValues {
			if ctxVal == v {
				return true
			}
		}
		return false

	case "StringNotEquals":
		for _, v := range condValues {
			if ctxVal == v {
				return false
			}
		}
		return true

	case "StringLike":
		for _, v := range condValues {
			if globMatch(v, ctxVal) {
				return true
			}
		}
		return false

	case "StringNotLike":
		for _, v := range condValues {
			if globMatch(v, ctxVal) {
				return false
			}
		}
		return true

	case "ArnEquals", "ArnLike":
		for _, v := range condValues {
			if globMatch(v, ctxVal) {
				return true
			}
		}
		return false

	case "ArnNotEquals":
		for _, v := range condValues {
			if ctxVal == v {
				return false
			}
		}
		return true

	case "Bool":
		for _, v := range condValues {
			if ctxVal == v {
				return true
			}
		}
		return false

	case "Null":
		isNull := ctxVal == ""
		for _, v := range condValues {
			want := strings.ToLower(v) == "true"
			if want == isNull {
				return true
			}
		}
		return false
	}

	// Unknown operator — deny by default (safe).
	return false
}

// globMatch returns true if pattern matches value using AWS glob rules:
// '*' matches any sequence of characters, '?' matches exactly one character.
func globMatch(pattern, value string) bool {
	return globMatchRec(pattern, value)
}

// globMatchRec is the recursive implementation of globMatch.
func globMatchRec(pattern, value string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Skip consecutive stars.
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			// Try matching the rest of the pattern at every position.
			for i := 0; i <= len(value); i++ {
				if globMatchRec(pattern, value[i:]) {
					return true
				}
			}
			return false

		case '?':
			if len(value) == 0 {
				return false
			}
			pattern = pattern[1:]
			value = value[1:]

		default:
			if len(value) == 0 || pattern[0] != value[0] {
				return false
			}
			pattern = pattern[1:]
			value = value[1:]
		}
	}
	return len(value) == 0
}
