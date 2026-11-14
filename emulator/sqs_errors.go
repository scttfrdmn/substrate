package emulator

import (
	"net/http"
	"strconv"
)

// sqsInvalidBody returns the refusal for a JSON request body that will not parse (#1007).
//
// No SQS operation page publishes an error for an undecodable body, so this comes from the service's
// own Common Errors page, which lists ValidationError at HTTP 400 and glosses it "The input fails to
// satisfy the constraints specified by an AWS service" — the step-2 rule #950 established for a class
// of caller error no operation page names. The page's nearer-looking codes are deliberately not used:
// InvalidParameterValue names "the input parameter", and a body that will not parse has no parameter to
// name; MalformedQueryString is published at 404 and describes the query string, not a JSON body;
// MissingParameter asserts which parameter is absent, which is unknowable when nothing decoded.
//
// Fourteen SQS sites discarded this error before #1007: thirteen are refused here, and the fourteenth
// (sqsRequestedAttributeNames) is unreachable behind ReceiveMessage's own guard and records that rather
// than refusing twice. Four of the thirteen — GetQueueAttributes, DeleteQueue,
// ListQueueTags and PurgeQueue — took the discarded value straight into a queue lookup, so a JSON
// syntax error answered QueueDoesNotExist: the emulator told a caller its queue was missing when the
// queue was fine and the body was not.
//
// The same code answers a body that parses but contradicts a member's type, which is the only shape that
// reaches the second decode in the nine handlers that read the queue URL through the helper first — a
// body that will not parse has already been refused one call earlier. The member is not named, because
// naming it would mean answering InvalidParameterValue for a type mismatch and ValidationError for a
// syntax error on the same operation, which is the one-plugin-two-codes split #950 removed; the
// common-errors gloss above is true of both.
func sqsInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "The input fails to satisfy the constraints specified by an AWS service",
		HTTPStatus: http.StatusBadRequest,
	}
}

// The errors below are the codes the GetQueueAttributes, ChangeMessageVisibility and
// ChangeMessageVisibilityBatch references publish, each at HTTP 400 (#1441, #1462). The
// code is the plain one under both protocols, as [sqsQueueDoesNotExist] records for
// the same reason: it is what botocore and aws-sdk-go-v2 dispatch on.

// sqsInvalidAttributeName is GetQueueAttributes' refusal of an attribute name it does
// not define: "The specified attribute doesn't exist".
func sqsInvalidAttributeName(name string) *AWSError {
	return &AWSError{
		Code:       "InvalidAttributeName",
		Message:    "Unknown Attribute " + name + ".",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsReceiptHandleIsInvalid is the refusal of a receipt handle that names no message
// of the queue: "The specified receipt handle isn't valid".
func sqsReceiptHandleIsInvalid(handle string) *AWSError {
	return &AWSError{
		Code:       "ReceiptHandleIsInvalid",
		Message:    "The input receipt handle \"" + handle + "\" is not a valid receipt handle.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsMessageNotInflight is the refusal of a visibility change for a message that is
// not in flight — never received, or received and already visible again: "The
// specified message isn't in flight".
func sqsMessageNotInflight() *AWSError {
	return &AWSError{
		Code:       "MessageNotInflight",
		Message:    "The specified message isn't in flight.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsVisibilityTimeoutOutOfRange is the refusal of a VisibilityTimeout outside the
// documented range "0 to 43200. Maximum: 12 hours".
func sqsVisibilityTimeoutOutOfRange(v int) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value " + strconv.Itoa(v) + " for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200, if provided.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsEmptyBatchRequest is the refusal of a batch request with no entries: "The batch
// request doesn't contain any entries".
func sqsEmptyBatchRequest(entryName string) *AWSError {
	return &AWSError{
		Code:       "EmptyBatchRequest",
		Message:    "There should be at least one " + entryName + " in the request.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsTooManyEntriesInBatchRequest is the refusal of a batch of more than ten entries,
// the maximum the reference states for every SQS batch operation.
func sqsTooManyEntriesInBatchRequest(n int) *AWSError {
	return &AWSError{
		Code:       "TooManyEntriesInBatchRequest",
		Message:    "Maximum number of entries per request are 10. You have sent " + strconv.Itoa(n) + ".",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsBatchEntryIDsNotDistinct is the refusal of a batch in which "Two or more batch
// entries in the request have the same Id".
func sqsBatchEntryIDsNotDistinct(id string) *AWSError {
	return &AWSError{
		Code:       "BatchEntryIdsNotDistinct",
		Message:    "Id " + id + " repeated.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsInvalidBatchEntryID is the refusal of a batch entry Id that "doesn't abide by the
// specification": up to 80 alphanumeric characters, hyphens and underscores.
func sqsInvalidBatchEntryID() *AWSError {
	return &AWSError{
		Code:       "InvalidBatchEntryId",
		Message:    "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
		HTTPStatus: http.StatusBadRequest,
	}
}

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
