package emulator

import "net/http"

// EventBridge's refusals for a request substrate could not use.
//
// #950 found thirteen sites in eventbridge_plugin.go answering InvalidParameterException — seven
// body-decode guards and six missing-member complaints. The code appears in the Errors section of
// none of the seven guarded operations, and a grep of their raw pages finds it nowhere on them at
// all, not even in prose.
//
// Unlike Kinesis, EventBridge has no per-operation code to move to. The intersection across the seven
// operations' Errors sections is InternalException/500 alone, which is wrong for a client's mistake,
// and the service publishes **no** validation-shaped code anywhere: API_PutRule, API_PutTargets,
// API_CreateArchive, API_TagResource, API_CreateConnection, API_ListRules and API_CreateEventBus were
// all checked for ValidationException, ValidationError and InvalidParameterException and carry none.
//
// So the code comes from the common-errors page, which is the fifteen-entry boilerplate every JSON
// service publishes: ValidationError, HTTP 400, "The input doesn't meet the required format or
// constraints. Check that all required parameters are included and that values are valid." That is
// the same fallback and the same reasoning #950 applied to Step Functions and Systems Manager, taken
// for the same reason — a refusal that belongs to no single operation has to come from the page that
// belongs to no single operation.
//
// The status is unchanged: all thirteen sites already answered 400, and 400 is what the common page
// publishes.
//
// **Correction from #1086: the sweep above missed one code, because it read Errors sections.**
// API_ListRules publishes no pagination code in its Errors section — that list is InternalException
// 500 and ResourceNotFoundException 400 — and InvalidToken is absent from EventBridge's
// CommonErrors.html as well. But the page names it twice in prose, in the NextToken member's own
// description: "Using an expired pagination token results in an HTTP 400 InvalidToken error." So a
// bad pagination token is the one client mistake in this service that does have a published code,
// and the common-errors fallback above is not what it should answer. See [ebInvalidToken].

// ebInvalidToken reports a NextToken that is not one substrate issued.
//
// InvalidToken at 400, from the prose quoted in this file's preamble rather than from an Errors
// section — the only provenance EventBridge offers, and enough: the code, the status and the
// condition are all stated by the page, in the description of the very member being refused.
//
// The condition AWS names is an *expired* token and substrate's tokens do not expire, which is worth
// being exact about. A substrate token is a base64 array offset, valid for as long as the process
// holds the listing, so what this refuses is the other way a token fails: one the encoder could not
// have produced at all (see [decodeOffsetPaginationToken]). Both are the same
// observation for a caller — a token this service will not resume from — and answering page one
// instead is the failure #884 named, where a loop that never terminates is indistinguishable from a
// correct answer.
func ebInvalidToken() *AWSError {
	return &AWSError{
		Code:       "InvalidToken",
		Message:    "the NextToken is not a pagination token this service issued",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ebInvalidBody reports that a request body would not decode.
//
// It takes no argument so that encoding/json's own text cannot reach a caller; see
// [kinesisInvalidBody] for the same rule.
func ebInvalidBody() *AWSError {
	return ebValidationError("the request body is not valid JSON")
}

// ebValidationError reports that a request does not satisfy the constraints its operation states.
func ebValidationError(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
