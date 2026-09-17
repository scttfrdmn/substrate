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
