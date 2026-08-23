package emulator

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// Policy-ARN shape validation on attach (#499).
//
// AttachUserPolicy, AttachRolePolicy and AttachGroupPolicy accepted any PolicyArn and
// appended it to the entity's list unconditionally, so a consumer could attach a policy that
// does not exist and only find out later through GetPolicy — a different operation reporting
// NoSuchEntity for an ARN the attach had just accepted.
//
// #499 offered four paths and left the decision to be recorded. What ships is (1) + (2):
// refuse a *malformed* ARN, and log a warning for a well-formed one that resolves nowhere.
//
// Existence is deliberately not required. Substrate bundles 52 of roughly 1,200 AWS managed
// policies, so refusing an unresolvable ARN would refuse every attach of the other ~1,150: a
// consumer attaching AmazonAthenaFullAccess would get a hard failure where AWS succeeds. That
// trades a confusing success for a wrong failure, and the wrong failure is worse — it breaks
// working consumer code rather than merely failing to catch a typo. Shape validation catches
// the typo-shaped mistake (a bare policy name, an S3 bucket ARN, a missing "policy/") without
// requiring the catalog to be complete, and the WARN names the ARN so the confusing success
// is at least visible in a log.
//
// The two paths not taken: seeding all ~1,200 ARNs without documents (3) would make GetPolicy
// resolve policies whose documents are empty, which needs its own caveat and is a large data
// addition; a seedable strict mode (4) would be a second behavior for the same call with no
// consumer asking for it yet. Either remains open if one does.

// iamPolicyARNPattern is the shape a policy ARN must have.
//
// Built from the API model's own patterns rather than by eye:
//
//   - the account is "aws" for an AWS managed policy or accountIdType (\d{12}) for a
//     customer-managed one, and nothing else — an ARN naming a 13-digit account is a typo, not
//     an account substrate has not heard of;
//   - the region is empty, because IAM is global and every real policy ARN has an empty
//     region field;
//   - the path segments follow policyPathType and the name follows policyNameType
//     ([\w+=,.@-]+), which is what makes a bare name or a slash-terminated ARN refusable.
//
// The partition is a permissive [a-z0-9-]+ on purpose: aws, aws-cn, aws-us-gov and the
// several aws-iso variants exist, more may follow, and refusing an unknown partition would
// refuse an ARN that is perfectly well-formed for a region substrate does not model.
var iamPolicyARNPattern = regexp.MustCompile(
	`^arn:[a-z0-9-]+:iam::(aws|[0-9]{12}):policy(/[A-Za-z0-9.,+@=_-]+)*/[\w+=,.@-]+$`)

// iamValidatePolicyARN reports whether arn is a well-formed IAM policy ARN, returning the
// message to refuse it with when it is not.
//
// Shape only: a well-formed ARN naming a policy that does not exist is valid here, and the
// caller decides what to do about that (see iamWarnUnresolvedPolicyARN).
func iamValidatePolicyARN(arn string) (message string, ok bool) {
	// arnType is min 20, max 2048. The length check comes first so a 4 KB body is refused on
	// its size rather than run through the pattern.
	if len(arn) < 20 || len(arn) > 2048 {
		return fmt.Sprintf("The specified value for policyArn is invalid. "+
			"It must be between 20 and 2048 characters long (got %d).", len(arn)), false
	}
	if !iamPolicyARNPattern.MatchString(arn) {
		return fmt.Sprintf("ARN %s is not valid.", arn), false
	}
	return "", true
}

// iamPolicyARNIsAWSManaged reports whether arn names an AWS managed policy — the "::aws:"
// account — rather than a customer-managed one.
func iamPolicyARNIsAWSManaged(arn string) bool {
	return strings.HasPrefix(arn, "arn:") && strings.Contains(arn, ":iam::aws:policy/")
}

// iamWarnUnresolvedPolicyARN logs at WARN when a well-formed ARN resolves in neither the
// bundled catalog nor state.
//
// The attach still succeeds. The log is what makes the gap visible without breaking the
// consumer: an AWS managed ARN substrate does not bundle is the expected case (52 of ~1,200),
// while a customer-managed ARN that resolves nowhere means no CreatePolicy call ever made it,
// which is more likely to be a real mistake — so the two get different messages.
//
// It returns nothing on purpose. An earlier form reported whether the ARN resolved, and no
// caller used it — a bool no attach reads is a value a test cannot observe, so a mutation
// flipping it survived the whole suite. The log line is the entire observable effect.
func (p *IAMPlugin) iamWarnUnresolvedPolicyARN(goCtx context.Context, operation, arn string) {
	if p.logger == nil {
		return
	}
	if _, ok := GetManagedPolicy(arn); ok {
		return
	}
	// A state failure is treated as "does not resolve": the warning is advisory and the attach
	// proceeds either way, so a read error must not turn an accepted call into an error.
	raw, err := p.state.Get(goCtx, iamNamespace, iamPolicyKey(arn))
	if err == nil && raw != nil {
		return
	}

	if iamPolicyARNIsAWSManaged(arn) {
		p.logger.Warn("iam "+operation+": the policy ARN is a well-formed AWS managed ARN "+
			"substrate does not bundle; the attach succeeds and the policy contributes no "+
			"statements to an authorization decision",
			"policyArn", arn)
		return
	}
	p.logger.Warn("iam "+operation+": the policy ARN resolves in neither the bundled catalog "+
		"nor state; the attach succeeds and the policy contributes no statements to an "+
		"authorization decision",
		"policyArn", arn)
}
