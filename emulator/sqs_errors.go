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

// sqsQueueNameExists returns the error CreateQueue raises when the named queue
// already exists with attribute values differing from the ones requested.
//
// The code and message are the CreateQueue reference's own, verbatim: QueueNameExists
// / "A queue with this name already exists. Amazon SQS returns this error only if
// the request includes attributes whose values differ from those of the existing
// queue." / HTTP 400. AWS publishes no separate runtime wording for it, so the
// documented description is the most faithful message available — the same basis
// [sqsQueueDoesNotExist] uses.
//
// Verified to be the string both SDKs dispatch on, because #413 proved a plausible
// guess is not good enough here: botocore's SQS model declares the exception shape
// QueueNameExists, and aws-sdk-go-v2 matches
// strings.EqualFold("QueueNameExists", errorCode). Neither carries a legacy alias
// for this one.
//
// The offending attribute is named in the message beyond AWS's text, because the
// condition is otherwise near-undiagnosable: a consumer holding a 20-attribute
// template learns only that *something* differs. That is additive — a caller
// matching the code still matches, and one matching the documented prefix still
// matches.
func sqsQueueNameExists(attribute string) *AWSError {
	msg := "A queue with this name already exists. Amazon SQS returns this error " +
		"only if the request includes attributes whose values differ from those of " +
		"the existing queue."
	if attribute != "" {
		msg += " The attribute " + attribute + " differs."
	}
	return &AWSError{
		Code:       "QueueNameExists",
		Message:    msg,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsQueueDeletedRecently returns the error CreateQueue raises when a queue of the
// same name was deleted too recently to be recreated.
//
// Code, message and status are the CreateQueue reference's, verbatim:
// QueueDeletedRecently / "You must wait 60 seconds after deleting a queue before
// you can create another queue with the same name." / HTTP 400. Both SDKs dispatch
// on the plain code — botocore models the QueueDeletedRecently exception shape and
// aws-sdk-go-v2 matches it with EqualFold — so no alias handling is needed.
//
// Substrate raises this only when seeded. The real condition is a 60-second
// wall-clock window, and a duration-based window cannot be modeled here without
// making assertions wall-clock dependent; see [sqsConsistencySeed] for why the
// budget is counted instead.
func sqsQueueDeletedRecently() *AWSError {
	return &AWSError{
		Code: "QueueDeletedRecently",
		Message: "You must wait 60 seconds after deleting a queue before you can " +
			"create another queue with the same name.",
		HTTPStatus: http.StatusBadRequest,
	}
}
