package emulator

// stepfunctions_create.go holds what CreateStateMachine and CreateActivity check before they store
// anything, and what they answer when the same resource is created twice (#1072).
//
// Both handlers answered `InvalidParameterException` for an absent `name` — a code neither page
// publishes. `API_CreateStateMachine` publishes fifteen errors and `API_CreateActivity` seven, and
// it is among neither. Both answered 409 for a name already taken, where both pages publish 400.
// And both refused a second create unconditionally, where both pages publish the operation as
// idempotent.
//
// The name-constraint list is published identically on both pages, down to the wording, which is why
// one validator serves both. The choice of `InvalidName` over `ValidationException` for a name that
// breaks it is recorded on [sfnInvalidName].

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// sfnNameMaxLength is the published maximum for both a state machine's and an activity's name:
// "Length Constraints: Minimum length of 1. Maximum length of 80." The published minimum of 1 is the
// empty-name check in [sfnValidateName] rather than a constant, because an absent required member
// and an over-long one are different things to say.
const sfnNameMaxLength = 80

// sfnForbiddenNameChars are the characters both create pages forbid by name, in the order the pages
// list them: brackets `< > { } [ ]`, wildcard characters `? *`, and special characters
// `" # % \ ^ | ~ ` + "`" + ` $ & , ; : /`.
//
// White space is deliberately not in this set and is checked with [unicode.IsSpace] instead: the
// pages say "white space" rather than enumerating it, and a non-breaking space is white space
// without being a character either page lists.
const sfnForbiddenNameChars = "<>{}[]?*\"#%\\^|~`$&,;:/"

// sfnRoleArnMaxLength is the published maximum for `roleArn`, whose Length Constraints read
// "Minimum length of 1. Maximum length of 256".
const sfnRoleArnMaxLength = 256

// sfnValidateName reports a name neither create operation would accept, or nil for one they would.
//
// Both pages publish the same list, and substrate checked none of it — `createStateMachine` and
// `createActivity` each tested only for the empty string, and then with a code neither page
// publishes. The list is: white space; brackets `< > { } [ ]`; wildcard characters `? *`; special
// characters `" # % \ ^ | ~ ` + "`" + ` $ & , ; : /`; control characters (`U+0000-001F`,
// `U+007F-009F`, `U+FFFE-FFFF`); surrogates (`U+D800-DFFF`); invalid characters (`U+10FFFF`); and a
// length of 1 to 80.
//
// The surrogate clause is recorded rather than enforced, and that is a property of the wire rather
// than a gap: a lone surrogate cannot reach this function as a surrogate, because Go's JSON decoder
// substitutes U+FFFD for an unpaired `\uD800`-`\uDFFF` escape and for any byte sequence that is not
// valid UTF-8. There is no input a caller can send that would make the check fire.
//
// The page's further sentence — "To enable logging with CloudWatch Logs, the name should only
// contain 0-9, A-Z, a-z, - and _" — is not enforced either, and must not be: it is a condition on
// logging, stated with "should", not a constraint on the name, and substrate models no logging
// configuration for it to interact with.
func sfnValidateName(name string) *AWSError {
	if name == "" {
		return sfnInvalidName("a name is required")
	}
	if n := utf8.RuneCountInString(name); n > sfnNameMaxLength {
		return sfnInvalidName(fmt.Sprintf("a name is at most %d characters, and this one is %d",
			sfnNameMaxLength, n))
	}
	for _, r := range name {
		switch {
		case unicode.IsSpace(r):
			return sfnInvalidName("a name contains no white space")
		case strings.ContainsRune(sfnForbiddenNameChars, r):
			return sfnInvalidName(fmt.Sprintf("a name contains none of %s, and this one contains %q",
				sfnForbiddenNameChars, r))
		case r <= 0x1F, r >= 0x7F && r <= 0x9F, r == 0xFFFE, r == 0xFFFF, r == 0x10FFFF:
			return sfnInvalidName(fmt.Sprintf(
				"a name contains no control or invalid character, and this one contains U+%04X", r))
		}
	}
	return nil
}

// sfnValidateRoleArn reports a `roleArn` CreateStateMachine would not accept, or nil.
//
// The member is `Required: Yes` and substrate stored whatever arrived, including an empty string, so
// a state machine could exist with no role at all. The code is `InvalidArn` — "The provided Amazon
// Resource Name (ARN) is not valid." — published on this page, and the one that fits a member
// documented as "The Amazon Resource Name (ARN) of the IAM role to use for this state machine" when
// the value is not one.
//
// Three things are checked and the third is the narrowest reading rather than a structural one: the
// page publishes **no Pattern** for this member, so splitting the ARN into its six fields and
// refusing a value that does not name an IAM role would invent a validation AWS does not document —
// the line [cfnValidateRoleARN] draws for CloudFormation's own `RoleARN`, where the model is also a
// bare length-bounded string. What a published `InvalidArn` does establish is that the value is
// meant to be an ARN, and a string with no `arn:` prefix is not one under any reading.
//
// A role that does not exist is deliberately not an error: the page publishes no code for it, and
// substrate does not resolve the role at create time.
func sfnValidateRoleArn(roleArn string) *AWSError {
	switch {
	case roleArn == "":
		return sfnInvalidRoleArn("roleArn is required")
	case utf8.RuneCountInString(roleArn) > sfnRoleArnMaxLength:
		return sfnInvalidRoleArn(fmt.Sprintf("roleArn is at most %d characters", sfnRoleArnMaxLength))
	case !strings.HasPrefix(roleArn, "arn:"):
		return sfnInvalidRoleArn(roleArn)
	}
	return nil
}

// sfnValidateStateMachineType reports a `type` outside the published Valid Values, or nil.
//
// `API_CreateStateMachine` publishes `Valid Values: STANDARD | EXPRESS` and substrate stored any
// string that arrived, which left the record's `type` reportable by DescribeStateMachine as a value
// AWS does not publish — and left the execution path branching on it undecided, since
// StartSyncExecution's own refusal tests for STANDARD rather than for EXPRESS.
// `StateMachineTypeNotSupported` is published on this page for exactly this, and
// [sfnStateMachineTypeNotSupported] already existed for StartSyncExecution's use of it.
//
// The caller applies the published default first ("Determines whether a Standard or Express state
// machine is created. The default is `STANDARD`"), so an absent `type` reaches here as `STANDARD`
// and is accepted rather than refused as unsupported.
func sfnValidateStateMachineType(smType string) *AWSError {
	switch smType {
	case "STANDARD", "EXPRESS":
		return nil
	}
	return sfnStateMachineTypeNotSupported(smType)
}

// sfnStateMachineIsIdempotentCreate reports whether a second CreateStateMachine against an existing
// name is the idempotent repeat AWS publishes rather than a conflict.
//
// The published rule is a Note on the page:
//
//	CreateStateMachine is an idempotent API. Subsequent requests won't create a duplicate resource if
//	it was already created. CreateStateMachine's idempotency check is based on the state machine
//	name, definition, type, LoggingConfiguration, TracingConfiguration, and EncryptionConfiguration
//	The check is also based on the publish and versionDescription parameters. If a following request
//	has a different roleArn or tags, Step Functions will ignore these differences and treat it as an
//	idempotent request of the previous. In this case, roleArn and tags will not be updated, even if
//	they are different.
//
// Of the eight inputs to that check, substrate models three — `name` (the lookup itself),
// `definition` and `type`. The other five are request members no handler in this plugin decodes, so
// they cannot differ between two requests substrate has seen and comparing them would compare
// nothing. That is why this takes two arguments rather than a whole request.
//
// **The page contradicts itself here, and substrate follows the Note.** `StateMachineAlreadyExists`
// is glossed "A state machine with the same name but a different definition **or role ARN** already
// exists", which would make a differing `roleArn` a refusal — while the Note says a differing
// `roleArn` is ignored and "will not be updated, even if [it is] different". The Note is the more
// specific statement: it enumerates the check's inputs and names `roleArn` as excluded twice. So a
// repeat that differs only in `roleArn` or `tags` answers the existing state machine's ARN, and
// neither value is updated — which returning the stored record unchanged achieves exactly.
func sfnStateMachineIsIdempotentCreate(existing *StateMachineState, definition, smType string) bool {
	return existing.Definition == definition && existing.Type == smType
}
