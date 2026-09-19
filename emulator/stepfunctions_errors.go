package emulator

// Step Functions error construction: one helper per published code, so a code's HTTP status is
// decided once rather than at each call site that answers it.
//
// The file exists for the reason kms_errors.go does, and the finding that produced it is the same
// one: **fifteen sites answered InvalidRequest for a request body that would not parse, and that
// string appears nowhere in Step Functions' documentation** — not on an operation page, not on
// CommonErrors.html. A caller matching on it matched something no SDK models. Twelve of the fifteen
// were in stepfunctions_plugin.go and three in stepfunctions_tags.go, one per handler, each with its
// own copy of the same literal (#950).
//
// The replacement is ValidationError at 400, from CommonErrors.html: "The input doesn't meet the
// required format or constraints. Check that all required parameters are included and that values
// are valid." It has to come from the common page because a body that will not parse belongs to no
// single operation — the fifteen sites span fifteen operations — and every one of those fifteen
// pages ends its own Errors section by deferring to that page.
//
// The wider finding of #950, recorded here because it is what makes the KMS fix transfer rather than
// coincide: that fifteen-entry page is AWS boilerplate shared across services. Step Functions',
// Systems Manager's and KMS's are byte-identical, in the same order and with the same statuses, and
// the code is spelled **ValidationError** with no "Exception" suffix.
//
// Two near misses, both declined, and the first is the one this service has and KMS did not:
//
//   - ValidationException. Step Functions really does publish it, at 400, glossed "The input does
//     not satisfy the constraints specified by an AWS service" — but on only **five** of the fifteen
//     operations that carry a parse guard: CreateStateMachine, UpdateStateMachine,
//     DeleteStateMachine, StartExecution and StopExecution. The other ten do not list it, so
//     answering it everywhere would leave ten sites reporting a code their own operation does not
//     publish, which is the defect above relocated rather than fixed, and answering it at only five
//     would make one failure produce two codes inside one plugin. Four pages compound the trap by
//     naming ValidationException in *prose* without listing it as an error — "if you provide a
//     qualified state machine ARN that refers to a Distributed Map state, the request fails with
//     ValidationException" — so the pattern a reader infers from the prose is not what the Errors
//     sections publish.
//
//   - MalformedHttpRequestException, on CommonErrors.html at 400, is declined for the reason
//     [kmsInvalidBody] declines it: its published scope is the transport layer — "this typically
//     happens when the request body can't be decompressed using the specified content encoding
//     algorithm" — and a body that arrived intact and then failed to parse is not that.
//
// Every refusal *in this file* is 400, and two of them replaced a 404: no Step Functions endpoint
// answers 404 for a resource that does not exist, so there the status carries nothing a caller can
// branch on and the code is the whole signal (#910).
//
// This used to read "every refusal here is 400" with "here" meaning the service, and #1064's citation
// sweep found that false on the service's own pages. API_CreateStateMachine publishes
// ConflictException at **409**, API_UpdateStateMachine publishes ConflictException at 409 *and*
// ServiceQuotaExceededException at **402**, and both pages list those two codes under a 400 entry as
// well — AWS documents each of them twice, at two statuses. So the narrow claim above is about this
// file's own constructors and must not be generalised to the plugin.
//
// #1072 then corrected the statuses the plugin itself answered: createStateMachine's 409 became the
// published 400 ([sfnStateMachineAlreadyExists]) and createActivity's 409 went away entirely, because
// modeling CreateActivity's published idempotency leaves ActivityAlreadyExists with no reachable
// condition. Every constructor in this file is still 400, and the two 409s it now has a replacement
// for were both outside it.
//
// Messages are substrate's throughout, except where a helper quotes AWS's own gloss because it is
// already the whole of what the refusal has to say.

import (
	"net/http"
)

// sfnInvalidBody reports that a request body could not be parsed as JSON.
//
// ValidationError at 400, for the reasons this file's preamble gives. It takes no argument for
// [kmsInvalidBody]'s reason: encoding/json's error text describes the emulator's own decoder rather
// than anything a caller can act on, and the actionable fact — that the body was not JSON — is what
// the message states. Two Systems Manager sites answered err.Error() before #950 and leaked exactly
// that; see [ssmInvalidBody].
func sfnInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnInvalidArnError reports that an ARN is not one this operation accepts.
//
// TagResource, UntagResource and ListTagsForResource each publish InvalidArn — "The provided
// Amazon Resource Name (ARN) is not valid." — at HTTP 400.
func sfnInvalidArnError(arn string) *AWSError {
	return &AWSError{
		Code:       "InvalidArn",
		Message:    "The provided Amazon Resource Name (ARN) is not valid: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnResourceNotFoundError reports that the resource a resolved ARN addresses does not exist.
//
// The status is 400, not 404. All three tagging operations publish ResourceNotFound — "Could not
// find the referenced resource." — with "HTTP Status Code: 400", which is unusual enough to be
// worth stating: this path answered 404 before, so a consumer branching on the status rather than
// the code saw something no AWS Step Functions endpoint returns.
func sfnResourceNotFoundError(arn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFound",
		Message:    "Could not find the referenced resource: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnStateMachineDoesNotExist reports that a well-formed ARN names no state machine.
//
// API_DescribeStateMachine, API_UpdateStateMachine, API_StartExecution, API_StartSyncExecution and
// API_ListExecutions each publish StateMachineDoesNotExist — "The specified state machine does not
// exist." — at HTTP 400. The status is the part substrate had wrong: it answered 404, which no Step
// Functions endpoint returns, so a consumer branching on the status rather than the code saw
// something AWS never sends. #910 established the same correction for the tagging operations'
// ResourceNotFound.
func sfnStateMachineDoesNotExist(arn string) *AWSError {
	return &AWSError{
		Code:       "StateMachineDoesNotExist",
		Message:    "The specified state machine does not exist: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnActivityDoesNotExist reports that a well-formed ARN names no activity.
//
// API_DescribeActivity publishes ActivityDoesNotExist — "The specified activity does not exist." —
// at HTTP 400, and it is one of only two errors on that page. See [sfnStateMachineDoesNotExist] for
// the status.
func sfnActivityDoesNotExist(arn string) *AWSError {
	return &AWSError{
		Code:       "ActivityDoesNotExist",
		Message:    "The specified activity does not exist: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnExecutionDoesNotExist reports that a well-formed ARN names no execution.
//
// API_DescribeExecution, API_GetExecutionHistory and API_StopExecution each publish
// ExecutionDoesNotExist — "The specified execution does not exist." — at HTTP 400. See
// [sfnStateMachineDoesNotExist] for the status.
func sfnExecutionDoesNotExist(arn string) *AWSError {
	return &AWSError{
		Code:       "ExecutionDoesNotExist",
		Message:    "The specified execution does not exist: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnStateMachineTypeNotSupported reports that an operation does not serve this state machine's type.
//
// API_StartSyncExecution publishes StateMachineTypeNotSupported — "State machine type is not
// supported." — at HTTP 400, and it is the code for the one restriction that page states outright:
// "StartSyncExecution is not available for STANDARD workflows." Substrate answered InvalidDefinition
// here, which is a real Step Functions code at CreateStateMachine and UpdateStateMachine, where a
// definition arrives in the request — but not on this page, and not about this fact. A consumer
// branching on the code saw a claim about the ASL document when what was wrong was the workflow type
// (#996).
//
// The message is AWS's verbatim, with the offending type appended: the published sentence does not say
// which type was rejected, and a caller holding one ARN of each needs to know.
func sfnStateMachineTypeNotSupported(smType string) *AWSError {
	return &AWSError{
		Code:       "StateMachineTypeNotSupported",
		Message:    "State machine type is not supported: " + smType,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnInvalidName reports a name outside the constraints both create pages publish.
//
// API_CreateStateMachine and API_CreateActivity both publish InvalidName — "The provided name is not
// valid." — at HTTP 400. Both handlers answered InvalidParameterException for an absent name, which
// is on neither page's Errors list: CreateStateMachine publishes fifteen codes and CreateActivity
// seven, and it is among neither (#1072).
//
// **The choice between InvalidName and ValidationException is recorded here because both are
// citable on one of the two pages and only one is citable on both.** ValidationException — "The input
// does not satisfy the constraints specified by an AWS service." — is published on
// API_CreateStateMachine and *not* on API_CreateActivity, whose seven errors do not include it. So
// answering ValidationException would make one plugin report two different codes for the same
// failure, and on CreateActivity it would report a code that page does not publish, which is the
// defect #1072 exists to remove rather than relocate. InvalidName is also the narrower fit: its gloss
// is about the name specifically, and every constraint checked here is a name constraint.
//
// The reason is appended to AWS's sentence rather than replacing it, so a caller matching on the
// published text still matches; see [sfnInvalidDefinition], which does the same.
func sfnInvalidName(reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidName",
		Message:    "The provided name is not valid: " + reason,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnInvalidRoleArn reports a roleArn that is absent, over-long, or not an ARN.
//
// API_CreateStateMachine publishes InvalidArn — "The provided Amazon Resource Name (ARN) is not
// valid." — at HTTP 400, and roleArn is Required: Yes there. Nothing checked it, so a state machine
// could be stored with no role at all (#1072). [sfnValidateRoleArn] states what is checked and why
// that is less than parsing the ARN.
//
// This is a second constructor for the same code as [sfnInvalidArnError] rather than a reuse of it,
// because that one appends the offending ARN and there is no ARN to append when the member is absent.
func sfnInvalidRoleArn(reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArn",
		Message:    "The provided Amazon Resource Name (ARN) is not valid: " + reason,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnStateMachineAlreadyExists reports a name held by a state machine that differs from the one being
// created.
//
// API_CreateStateMachine publishes StateMachineAlreadyExists — "A state machine with the same name
// but a different definition or role ARN already exists." — at **HTTP 400**. Substrate answered 409,
// which is the #910/#912 class one step further along: those swept four codes from 404 to 400 and
// missed this one because it was a 409 rather than a 404. A consumer branching on the status saw
// something this endpoint does not send (#1072).
//
// The message is AWS's verbatim, with the name appended, and it names a condition narrower than the
// one substrate refuses on — see [sfnStateMachineIsIdempotentCreate] for the contradiction between
// this sentence and the page's own idempotency Note, and for why the Note governs.
func sfnStateMachineAlreadyExists(name string) *AWSError {
	return &AWSError{
		Code: "StateMachineAlreadyExists",
		Message: "A state machine with the same name but a different definition or role ARN " +
			"already exists: " + name,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnInvalidDefinition reports that a definition is not one substrate will store.
//
// API_CreateStateMachine and API_UpdateStateMachine both publish InvalidDefinition — "The provided
// Amazon States Language definition is not valid." — at HTTP 400. These are the only two operations
// that publish it, and they are exactly the two where a definition arrives in the request, which is
// why it is wrong anywhere else; see [sfnStateMachineTypeNotSupported].
//
// What substrate checks is narrower than what AWS checks, and [sfnValidateDefinition] states the
// boundary. The reason is appended to AWS's sentence rather than replacing it, so a caller matching on
// the published text still matches.
func sfnInvalidDefinition(reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidDefinition",
		Message:    "The provided Amazon States Language definition is not valid: " + reason,
		HTTPStatus: http.StatusBadRequest,
	}
}
