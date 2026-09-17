package emulator

import "net/http"

// Bedrock Runtime's refusals for a request substrate could not use.
//
// The code was already right — ValidationException/400 is in the Errors section of both
// guarded operations, Converse and CreateModelInvocationJob, glossed "Input validation
// failed. Check your request parameters and retry the request." — so #950 changed no
// code string here. What it changed is that CreateModelInvocationJob's guard read
//
//	if err := json.Unmarshal(req.Body, &body); err != nil || body.JobName == "" {
//		… "jobName is required" …
//	}
//
// so a body that would not parse at all was reported as a missing member. That is the
// conflation #950 split at four SageMaker sites for the same reason: the two conditions
// call for different fixes by the caller — one sends different bytes, the other adds a
// member — and telling a caller their jobName is missing when their JSON is truncated
// sends them to look at the wrong thing. The remaining conflated sites, in Athena and
// Secrets Manager, are recorded in docs/services.md rather than changed, because there
// the required member is the only member and the two answers coincide.

// bedrockInvalidBody reports that a request body would not decode.
//
// It takes no argument so that encoding/json's own text cannot reach a caller; see
// [kinesisInvalidBody] for the same rule.
func bedrockInvalidBody() *AWSError {
	return bedrockValidationError("the request body is not valid JSON")
}

// bedrockValidationError reports that a request does not satisfy the constraints its
// operation states.
func bedrockValidationError(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
