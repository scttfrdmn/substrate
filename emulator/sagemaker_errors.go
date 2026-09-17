package emulator

import "net/http"

// SageMaker's refusals for a request substrate could not use.
//
// #950 found six sites in sagemaker_plugin.go answering InvalidParameterValue. That code is in the
// Errors section of none of the six guarded operations — and there is nothing to move it to, because
// **not one of the six publishes any input-validation error at all**. Every Errors section is
// resource-lifecycle only:
//
//	CreateApp           ResourceInUse 400, ResourceLimitExceeded 400
//	DeleteApp           ResourceInUse 400, ResourceNotFound 400
//	DescribeApp         ResourceNotFound 400
//	CreateTrainingJob   ResourceInUse 400, ResourceLimitExceeded 400, ResourceNotFound 400
//	DescribeTrainingJob ResourceNotFound 400
//	StopTrainingJob     ResourceNotFound 400
//
// So the code comes from SageMaker's common-errors page — the fifteen-entry boilerplate — exactly as
// it does for Step Functions, Systems Manager and EventBridge: ValidationError, HTTP 400, "The input
// doesn't meet the required format or constraints. Check that all required parameters are included
// and that values are valid." The status is unchanged; all six already answered 400.
//
// The second half of the fix is the message. Four of the six sites read
//
//	if err := json.Unmarshal(req.Body, &body); err != nil || body.TrainingJobName == "" {
//
// and answered "TrainingJobName is required", so a body that would not parse was reported as a
// missing member — a true code with a false message, telling the caller to add a member to a document
// substrate never read. The two conditions are now separate refusals, which is why this file carries
// two constructors rather than one.

// sagemakerInvalidBody reports that a request body would not decode.
//
// It takes no argument so that encoding/json's own text cannot reach a caller; see
// [kinesisInvalidBody] for the same rule.
func sagemakerInvalidBody() *AWSError {
	return sagemakerValidationError("the request body is not valid JSON")
}

// sagemakerValidationError reports that a request does not satisfy the constraints its operation
// states.
func sagemakerValidationError(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
