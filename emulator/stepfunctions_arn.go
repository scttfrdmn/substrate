package emulator

// Step Functions ARN parsing: the one place a stateMachineArn, activityArn or executionArn
// parameter becomes an account, a Region and a name.
//
// Split out of stepfunctions_plugin.go and stepfunctions_tags.go because the parse now serves
// fourteen operations rather than three, and because the thing it replaced was four lines that got
// three things wrong at every one of them.
//
// extractSMNameFromARN returned the ARN's **last** colon-separated segment and nothing else, and
// each caller then supplied the account and Region from the caller's own request context. Three
// consequences compounded (#912), the same shape #826 closed for SQS and DynamoDB, #845 for the
// tagging API's resolver, #910 for these three tag operations, #918 for CloudFront, #922 for KMS
// and #925 for SNS:
//
//  1. arn:aws:states:eu-west-1:999988887777:stateMachine:orders presented by a us-east-1 caller in
//     account 111122223333 addressed that caller's own state machine named "orders".
//     DeleteStateMachine and DeleteActivity deleted it, UpdateStateMachine rewrote its definition
//     and role ARN, StopExecution aborted the caller's own execution, and the four Describe/List
//     operations disclosed a definition, an input, an output and an event history under an ARN that
//     named none of them. All of them answered 200 while doing it.
//
//  2. The resource-type segment was never read, so the operations told their two kinds apart by
//     accident: DescribeStateMachine on an activity ARN reported the state machine of that name,
//     because activity:orders' last segment is orders; an execution ARN at a state-machine
//     operation resolved the state machine named after the *execution*; and a non-states ARN was
//     never refused, only reported absent.
//
//  3. An execution ARN needed two names out of one string — arn:…:execution:{sm}:{exec} — and
//     extractSMNameFromExecARN got them by stripping one segment and taking the last of what
//     remained, which is right only for an ARN of exactly that arity.
//
// The account and Region now come from the ARN, by functions that take no *RequestContext at all,
// which is what makes the rule structural rather than a thing each of fourteen call sites has to
// remember. A *RequestContext is still the answer where it legitimately is: minting an ARN for a
// resource being created, which is CreateStateMachine and CreateActivity and no operation in this
// file's set.
//
// What substrate does not model, and so what these parsers refuse: a state machine **version** ARN
// (…:stateMachine:{name}:{version}) and an **alias** ARN (…:stateMachine:{name}:{alias}). Both are
// well-formed at AWS and both name a resource substrate has no record of, so a two-segment
// stateMachine resource portion is refused as a name containing a colon rather than resolved to the
// unqualified state machine — which would silently serve a caller a different resource from the one
// it asked for, the defect this file exists to end.

import (
	"strings"
)

// The resource-type segment of each Step Functions ARN substrate resolves.
//
// The literals are AWS's, and the comparison against them is case-sensitive on purpose: AWS
// distinguishes a state machine from an activity by the segment alone — stateMachine with a capital
// M against activity — so a case-folding match would let "statemachine:orders" address the same
// record as "stateMachine:orders" while AWS refuses the first outright.
const (
	sfnTypeStateMachine = "stateMachine"
	sfnTypeActivity     = "activity"
	sfnTypeExecution    = "execution"
	sfnTypeExpress      = "express"
)

// sfnARNTarget is the resource a Step Functions ARN names: the account and Region that own it, its
// resource type, and the one or two names that identify it.
type sfnARNTarget struct {
	// AccountID is the account segment of the ARN, never the caller's.
	AccountID string

	// Region is the Region segment of the ARN, never the caller's.
	Region string

	// Type is the ARN's resource-type segment, one of the sfnType* constants.
	Type string

	// Name is the state machine's or activity's name. For an execution ARN it is the name of the
	// state machine the execution belongs to, which is what the execution's state key is nested
	// under.
	Name string

	// ExecName is the execution's own name, and is empty unless Type is an execution type.
	ExecName string
}

// sfnParseARN parses a Step Functions ARN and returns the resource it names.
//
// The arity of the resource portion is checked per type, which is the discriminator the shape
// offers: a state machine and an activity carry one name and an execution carries two, so a single
// "contains no colon" rule cannot serve both. [elbChildARN]'s wantSegments parameter is the in-tree
// precedent for an arity discriminator.
//
// A malformed ARN, an ARN naming another service, and an ARN naming a resource type substrate does
// not resolve are all InvalidArn at HTTP 400, which every operation taking one of these three
// parameters publishes: "The provided Amazon Resource Name (ARN) is not valid." The status is 400
// rather than 404 at every Step Functions error, which is unusual enough that #910 recorded it for
// the tagging operations; the eleven audited here all published it too and all answered 404.
func sfnParseARN(arn string) (sfnARNTarget, *AWSError) {
	// arn:{partition}:states:{region}:{account}:{type}:{resource}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != "states" {
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}
	region, acct, resType, resource := parts[3], parts[4], parts[5], parts[6]
	if region == "" || acct == "" || resource == "" {
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}

	target := sfnARNTarget{AccountID: acct, Region: region, Type: resType}
	switch resType {
	case sfnTypeStateMachine, sfnTypeActivity:
		// AWS's Name pattern for both excludes ":" and "/", so the whole resource portion is the
		// name. Without the check, an execution ARN's trailing pair or a version qualifier built a
		// state key with a colon in the name, which addresses nothing and so reported the resource
		// absent rather than the ARN wrong: the wrong error to hand a caller and the wrong one to
		// read in a log.
		if strings.ContainsAny(resource, "/:") {
			return sfnARNTarget{}, sfnInvalidArnError(arn)
		}
		target.Name = resource

	case sfnTypeExecution, sfnTypeExpress:
		segs := strings.Split(resource, ":")
		// A standard execution ARN carries exactly {smName}:{execName}. An express one carries the
		// same pair and may carry a trailing identifier, so the extra segment is tolerated rather
		// than refused: substrate stores no express execution at all, so such an ARN resolves to a
		// key nothing is written at and answers ExecutionDoesNotExist — which is the honest answer,
		// where refusing the ARN outright would claim AWS rejects a shape it mints.
		if len(segs) < 2 || (resType == sfnTypeExecution && len(segs) != 2) {
			return sfnARNTarget{}, sfnInvalidArnError(arn)
		}
		if segs[0] == "" || segs[1] == "" || strings.ContainsAny(segs[0], "/") || strings.ContainsAny(segs[1], "/") {
			return sfnARNTarget{}, sfnInvalidArnError(arn)
		}
		target.Name, target.ExecName = segs[0], segs[1]

	default:
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}
	return target, nil
}

// sfnParseStateMachineARN parses an ARN that must name a state machine.
//
// An ARN naming an activity or an execution is well-formed and names a resource this operation does
// not accept, so it is InvalidArn rather than a not-found: the resource may well exist, and it is
// the ARN that does not belong here. That is the decision #910 recorded for an execution ARN at a
// tagging operation, applied to the rest of the plugin.
func sfnParseStateMachineARN(arn string) (sfnARNTarget, *AWSError) {
	target, err := sfnParseARN(arn)
	if err != nil {
		return sfnARNTarget{}, err
	}
	if target.Type != sfnTypeStateMachine {
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}
	return target, nil
}

// sfnParseActivityARN parses an ARN that must name an activity. See [sfnParseStateMachineARN] for
// why a well-formed ARN of another type is InvalidArn rather than a not-found.
func sfnParseActivityARN(arn string) (sfnARNTarget, *AWSError) {
	target, err := sfnParseARN(arn)
	if err != nil {
		return sfnARNTarget{}, err
	}
	if target.Type != sfnTypeActivity {
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}
	return target, nil
}

// sfnParseExecutionARN parses an ARN that must name an execution, standard or express.
//
// Both types are accepted because both name an execution; substrate mints an express ARN from
// StartSyncExecution and stores no record for it, so an express ARN resolves to a key nothing is
// written at rather than being refused for its shape.
func sfnParseExecutionARN(arn string) (sfnARNTarget, *AWSError) {
	target, err := sfnParseARN(arn)
	if err != nil {
		return sfnARNTarget{}, err
	}
	if target.Type != sfnTypeExecution && target.Type != sfnTypeExpress {
		return sfnARNTarget{}, sfnInvalidArnError(arn)
	}
	return target, nil
}
