package emulator

import (
	"sort"
	"strings"
)

// Decision constants for IAM policy evaluation results.
const (
	// DecisionAllow indicates access was explicitly allowed.
	DecisionAllow = "Allow"

	// DecisionDeny indicates access was explicitly denied.
	DecisionDeny = "Deny"

	// DecisionImplicitDeny indicates no matching allow was found.
	DecisionImplicitDeny = "ImplicitDeny"
)

// Policy source types, as reported in a simulation's MatchedStatements.
//
// These are the values the SimulatePrincipalPolicy reference's sample responses
// emit — `<SourcePolicyType>IAM Policy</SourcePolicyType>` and `Resource Policy` —
// which **diverge from the `PolicySourceType` enum** in the API model
// (`user`/`group`/`role`/`aws-managed`/`user-managed`/`resource`/`none`). The doc
// values are what an SDK actually receives from the service, so those are what
// substrate emits.
const (
	// PolicySourceIAMPolicy marks a statement that came from an identity policy —
	// managed, inline, or supplied through PolicyInputList.
	PolicySourceIAMPolicy = "IAM Policy"

	// PolicySourceResourcePolicy marks a statement that came from a resource-based
	// policy.
	PolicySourceResourcePolicy = "Resource Policy"
)

// MatchedStatement identifies a policy statement that determined a decision.
//
// It mirrors the API model's Statement type, minus StartPosition/EndPosition:
// those are row/column offsets into the policy document *as submitted*, and
// substrate stores a parsed PolicyDocument, so the original text is gone and any
// offset would be fabricated. Omitting the members is honest — the reference's own
// sample response shows `<MatchedStatements/>` empty for an implicitDeny, so
// absence is a shape callers already handle.
type MatchedStatement struct {
	// SourcePolicyID identifies the policy the statement came from.
	//
	// It is the policy *name* rather than its ARN — `AmazonS3ReadOnlyAccess`, or
	// `PolicyInputList.1` for a document supplied as a string — per the reference's
	// sample responses.
	SourcePolicyID string

	// SourcePolicyType is PolicySourceIAMPolicy or PolicySourceResourcePolicy.
	SourcePolicyType string

	// Sid is the statement's own Sid, which AWS does not report.
	//
	// It is carried because substrate's own callers had it before MatchedStatements
	// grew a shape, and because a Sid names the statement far more usefully than a
	// policy name does when several statements in one document match. It is never
	// emitted over the wire.
	Sid string
}

// EvaluationResult holds the outcome of an IAM policy evaluation.
type EvaluationResult struct {
	// Decision is one of DecisionAllow, DecisionDeny, or DecisionImplicitDeny.
	Decision string

	// Reason is a human-readable explanation of the decision.
	Reason string

	// MatchedStatements identifies the statements that matched.
	MatchedStatements []MatchedStatement

	// MissingContextValues names the condition keys a matching statement tested
	// for which the request supplied no value.
	//
	// This is what stops a conditional grant reading as a clean allow: a statement
	// whose Condition names an unset key cannot match, so without this the caller
	// sees an implicitDeny with no indication that the answer would change given a
	// context value. AWS reports the same list for the same reason.
	MissingContextValues []string
}

// SourcedPolicyDocument is a policy document tagged with where it came from, so a
// matched statement can name its policy.
//
// It exists because Evaluate takes bare documents and therefore cannot report
// which one decided — enough for CheckAccess, which only needs allow-or-not, but
// not for a simulation, whose whole output is "which policy said so".
type SourcedPolicyDocument struct {
	// Document is the policy being evaluated.
	Document PolicyDocument

	// SourceID is the policy name reported as MatchedStatement.SourcePolicyID.
	SourceID string

	// SourceType is PolicySourceIAMPolicy or PolicySourceResourcePolicy.
	SourceType string
}

// EvaluationRequest contains the inputs for an IAM policy evaluation.
type EvaluationRequest struct {
	// Principal is the ARN of the caller.
	//
	// It is matched against a statement's Principal/NotPrincipal element, so it
	// only affects the outcome for a *resource* policy — an identity policy has
	// no Principal element and matches any caller. Before #593 this field was
	// populated by every call site and read by none, which is why a role's trust
	// policy could name one account and still admit every caller.
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
//
// Documents passed here carry no source identity, so the returned
// MatchedStatements name only a Sid. A caller that must report which policy
// decided — the policy simulator — uses [EvaluateSourced] instead.
func Evaluate(documents []PolicyDocument, req EvaluationRequest) EvaluationResult {
	sourced := make([]SourcedPolicyDocument, 0, len(documents))
	for _, doc := range documents {
		sourced = append(sourced, SourcedPolicyDocument{Document: doc, SourceType: PolicySourceIAMPolicy})
	}
	return EvaluateSourced(sourced, req)
}

// EvaluateSourced is [Evaluate] over documents that know where they came from, so
// each matched statement names its policy.
//
// The evaluation itself is identical — one implementation, so a simulated decision
// and an enforced one cannot disagree, which is the whole reason
// SimulatePrincipalPolicy is worth having.
//
// It also reports MissingContextValues: every condition key a matching-but-for-the-
// condition statement tested and the request had no value for. That is computed
// only for statements whose action and resource match, because a key named by a
// statement about some other action tells the caller nothing about this one.
func EvaluateSourced(documents []SourcedPolicyDocument, req EvaluationRequest) EvaluationResult {
	var allowed bool
	var allowedStatements []MatchedStatement
	missing := make(map[string]struct{})

	for _, doc := range documents {
		for _, stmt := range doc.Document.Statement {
			if !statementMatches(stmt, req) {
				// A statement that covers the action and resource but was ruled out by a
				// condition on an unset key is the reportable case: the answer would
				// change given a value.
				if actionResourcePrincipalMatch(stmt, req) {
					for _, key := range unsetConditionKeys(stmt.Condition, req.Context) {
						missing[key] = struct{}{}
					}
				}
				continue
			}
			matched := MatchedStatement{
				SourcePolicyID:   doc.SourceID,
				SourcePolicyType: sourceTypeOrDefault(doc.SourceType),
				Sid:              stmt.Sid,
			}
			if stmt.Effect == IAMEffectDeny {
				// Explicit deny always wins — return immediately.
				return EvaluationResult{
					Decision:             DecisionDeny,
					Reason:               "explicit deny in policy statement",
					MatchedStatements:    []MatchedStatement{matched},
					MissingContextValues: sortedKeys(missing),
				}
			}
			if stmt.Effect == IAMEffectAllow {
				allowed = true
				allowedStatements = append(allowedStatements, matched)
			}
		}
	}

	if allowed {
		return EvaluationResult{
			Decision:             DecisionAllow,
			Reason:               "allowed by policy statement",
			MatchedStatements:    allowedStatements,
			MissingContextValues: sortedKeys(missing),
		}
	}

	return EvaluationResult{
		Decision:             DecisionImplicitDeny,
		Reason:               "no matching allow statement",
		MissingContextValues: sortedKeys(missing),
	}
}

// sourceTypeOrDefault fills in the identity-policy type for a document that named
// none, so a MatchedStatement always carries a type an SDK can enum-decode.
func sourceTypeOrDefault(sourceType string) string {
	if sourceType == "" {
		return PolicySourceIAMPolicy
	}
	return sourceType
}

// sortedKeys returns set's keys in order, or nil when it is empty.
//
// Sorted because the map iteration order is randomized and a response a caller
// asserts on must not vary between two identical requests — the determinism
// substrate exists for.
func sortedKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// actionResourcePrincipalMatch reports whether stmt covers req apart from its
// conditions. It is the "would this statement apply, given the right context"
// question MissingContextValues answers.
func actionResourcePrincipalMatch(stmt PolicyStatement, req EvaluationRequest) bool {
	return principalMatches(stmt, req.Principal) &&
		actionMatches(stmt, req.Action) &&
		resourceMatches(stmt, req.Resource)
}

// unsetConditionKeys returns the condition keys stmt tests that ctx has no value
// for.
//
// A key present with an empty value is *set*: the Null operator exists precisely to
// test for absence, and treating "" as missing would report every Null condition as
// an incomplete simulation.
func unsetConditionKeys(conditions map[string]map[string]StringOrSlice, ctx map[string]string) []string {
	var out []string
	for _, keyValues := range conditions {
		for condKey := range keyValues {
			if _, ok := ctx[condKey]; !ok {
				out = append(out, condKey)
			}
		}
	}
	return out
}

// statementMatches returns true if stmt covers the principal, action, resource,
// and conditions specified in req.
func statementMatches(stmt PolicyStatement, req EvaluationRequest) bool {
	if !principalMatches(stmt, req.Principal) {
		return false
	}
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

// principalMatches reports whether principal is covered by stmt's Principal
// element, or for a NotPrincipal statement, whether it is NOT covered.
//
// A statement carrying neither element always matches. That is what makes this
// safe to run over an *identity* policy: Principal is meaningless in one — the
// principal is whoever the policy is attached to — and IAM rejects the element
// there, so an absent element cannot mean "matches nobody" without breaking every
// user, role and boundary evaluation. The element only appears in a *resource*
// policy, which is the case #593 needed: a role's trust policy answers "who may
// become this role", and until now nothing read the answer.
//
// A caller with no ARN never matches a statement that does name principals. An
// unauthenticated request resolves to no identity, so there is nothing for
// "Principal: {AWS: …}" to be true of; callers that must stay unenforced are
// skipped before evaluation rather than admitted here (see STSPlugin.assumeRole).
func principalMatches(stmt PolicyStatement, principal string) bool {
	// NotPrincipal is authoritative when both appear. IAM forbids the
	// combination, so there is no correct reading of it; taking the exclusion is
	// the direction that cannot widen access.
	if stmt.NotPrincipal != nil {
		return !principalCovered(stmt.NotPrincipal, principal)
	}
	if stmt.Principal == nil {
		return true
	}
	return principalCovered(stmt.Principal, principal)
}

// principalCovered reports whether p names principal.
func principalCovered(p *PolicyPrincipal, principal string) bool {
	if principal == "" {
		return false
	}
	if p.All {
		return true
	}
	for _, want := range p.AWS {
		if awsPrincipalCovers(want, principal) {
			return true
		}
	}
	// Service and Federated principals match literally. Substrate mints no
	// service-principal caller — a service ARN never appears as reqCtx.Principal
	// — so these entries can only be satisfied by a caller naming one exactly,
	// and pretending otherwise would let "Service: lambda.amazonaws.com" admit an
	// IAM user.
	for _, want := range p.Service {
		if want == principal {
			return true
		}
	}
	for _, want := range p.Federated {
		if want == principal {
			return true
		}
	}
	return false
}

// awsPrincipalCovers reports whether one "AWS" principal entry names principal.
//
// Three forms are accepted, all documented by IAM. An exact ARN matches itself. A
// bare 12-digit account ID and the equivalent root ARN
// ("arn:aws:iam::<account>:root") both delegate to the whole account: IAM treats
// them as identical, and "the account ID" form is what the ExternalId guide uses
// for a third-party trust policy.
func awsPrincipalCovers(want, principal string) bool {
	if want == principal {
		return true
	}
	account := want
	if strings.HasPrefix(want, "arn:") {
		if entityType, _ := parsePrincipalARN(want); entityType != "root" {
			// Any other ARN form is a specific entity, already handled by the
			// exact match above.
			return false
		}
		account = arnAccountID(want)
	} else if strings.ContainsAny(want, ":/") {
		// Not an ARN and not a bare account ID.
		return false
	}
	if account == "" {
		return false
	}
	return arnAccountID(principal) == account
}

// arnAccountID returns the account field of an ARN, or "" if arn is not one.
//
// The field is empty for the ARNs of account-less services (S3 buckets, for
// one), and an empty account must never be read as "same account" — every such
// ARN would then match every other.
func arnAccountID(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return ""
	}
	return parts[4]
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
