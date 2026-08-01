package emulator

import "net/http"

// sqsQueueDoesNotExist returns the error SQS raises for an operation naming a
// queue that does not exist.
//
// The code is QueueDoesNotExist, not the legacy
// "AWS.SimpleQueueService.NonExistentQueue". The AWS GetQueueUrl and
// GetQueueAttributes references document QueueDoesNotExist, and the legacy string
// appears nowhere in the current API reference: SQS is an
// awsQueryCompatible/JSON service, and the dotted form is the query-compatibility
// alias AWS sends in an x-amzn-query-error header rather than in __type.
//
// The distinction decides whether a consumer can catch the error at all:
//
//   - botocore derives the exception class from the resolved error code, so
//     __type: "AWS.SimpleQueueService.NonExistentQueue" resolves to a bare
//     ClientError and `except sqs.exceptions.QueueDoesNotExist` never matches,
//     while "QueueDoesNotExist" resolves to the modeled class.
//   - aws-sdk-go-v2 dispatches on strings.EqualFold("QueueDoesNotExist",
//     errorCode); the legacy string appears nowhere in the sqs module, so
//     errors.As(&types.QueueDoesNotExist{}) never matched either.
//
// Because writeError derives x-amzn-ErrorType from Code, changing the code alone
// corrects both SDKs with no additional plumbing.
func sqsQueueDoesNotExist() *AWSError {
	return &AWSError{
		Code:       "QueueDoesNotExist",
		Message:    "The specified queue does not exist",
		HTTPStatus: http.StatusBadRequest,
	}
}
