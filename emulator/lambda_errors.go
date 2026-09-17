package emulator

import "net/http"

// Lambda's refusals for a request substrate could not use.
//
// #950 found seven sites in lambda_plugin.go answering ValidationException — four body-decode guards
// and three missing-member complaints. Lambda publishes that code nowhere: it is in the Errors
// section of none of the four guarded operations, and Lambda's common-errors page is the fifteen-entry
// boilerplate, which lists ValidationError and not ValidationException. So a caller matching on it
// matched something no SDK models.
//
// InvalidParameterValueException/400 — "One of the parameters in the request is not valid." — is in
// the Errors section of **all four**: CreateFunction, AddPermission, TagResource and
// CreateEventSourceMapping. That makes it better evidence than the common-errors fallback #950 had to
// use for Step Functions, Systems Manager, EventBridge, SageMaker, Firehose and ACM: those services
// had no per-operation code covering every guarded site, and Lambda does. The rule the audit settled
// on is to prefer a per-operation code where one covers every site, and to fall back to the common
// page only where none does.
//
// The status is unchanged — all seven already answered 400, which is what the pages give the code —
// so this is a code-string correction, invisible to #923's status audit.

// lambdaInvalidBody reports that a request body would not decode.
//
// It takes no argument so that encoding/json's own text cannot reach a caller; see
// [kinesisInvalidBody] for the same rule.
func lambdaInvalidBody() *AWSError {
	return lambdaInvalidParameterValue("the request body is not valid JSON")
}

// lambdaInvalidParameterValue reports that a request parameter cannot be used as given.
func lambdaInvalidParameterValue(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValueException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
