package emulator

import "net/http"

// dynamodbStreamsValidationError reports that a DynamoDB Streams request omits a member its
// page marks Required: Yes.
//
// The code is the JSON-protocol common list's **ValidationError** at 400 — "The input doesn't
// meet the required format or constraints. Check that all required parameters are included and
// that values are valid." — and it is the one refusal in dynamodb_plugin.go that does not
// answer ValidationException. That is deliberate, and it is the whole reason this constructor
// exists in its own file rather than beside the eighteen others.
//
// API_streams_GetRecords publishes five errors and not one of them is about a parameter's
// shape: ExpiredIteratorException, InternalServerError, LimitExceededException,
// ResourceNotFoundException and TrimmedDataAccessException. The page goes as far as assigning
// an out-of-range Limit to LimitExceededException, so there is no gap where a validation code
// was merely left unlisted — DynamoDB Streams genuinely publishes none.
//
// ValidationException was therefore **declined**, and the reason is #671's binding scope
// decision rather than a preference. It is published on DynamoDB's *control-plane* pages —
// PutItem, Query and the rest — which is a different API surface reached through the same
// substrate plugin. Borrowing it here would be exactly the analogy from a sibling operation
// #671 forbids, and would make the plugin's eighteen ValidationException sites read as
// evidence for a nineteenth that no Streams page supports.
//
// So one plugin answers three codes, and each is sourced: SerializationException for a body
// that will not parse ([ddbInvalidBody]), ValidationException for the control-plane
// operations, and ValidationError for the Streams ones. Two APIs behind one plugin is the
// thing being modeled, not an inconsistency — and nothing on the wire separates them here,
// because substrate routes the Streams operations through DynamoDB's own host and
// DynamoDB_20120810 target prefix rather than through the streams.dynamodb endpoint. The
// operation name is the only discriminator, which is why the code is chosen per operation.
//
// Scope note: only an **absent** ShardIterator is refused. An iterator that is present but
// will not base64-decode, or decodes to something that is not a cursor, still answers 200 with
// an empty Records list — a deliberate accommodation of the stub iterators earlier releases
// minted, and the present-but-invalid axis, which needs its own reading of which published
// code applies.
func dynamodbStreamsValidationError(detail string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "The input doesn't meet the required format or constraints: " + detail,
		HTTPStatus: http.StatusBadRequest,
	}
}
