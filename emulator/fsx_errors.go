package emulator

import "net/http"

// FSx's refusals for a request substrate could not use.
//
// #950 corrected FSx's three body-parse guards from a bare InvalidRequest — an Amazon S3
// code, the likely provenance of the mistake — to BadRequest, and deferred the one guard
// that complains about a member rather than about the body. #1063 is that guard, and the
// issue misattributed it: it is in deleteFileSystem, not describeFileSystems.
//
// Which page governs it is the whole of the question, because the two pages do not answer
// the same way about whether the check should exist at all. On API_DeleteFileSystem
// FileSystemId is **Required: Yes**, so refusing an absent one is what AWS does; on
// API_DescribeFileSystems FileSystemIds is Required: No and an absent list means "describe
// them all", which is what substrate's describeFileSystems already does. So the check is
// correct and only its code was wrong.
//
// BadRequest at 400, "A generic error indicating a failure with a client request.", is the
// first entry in API_DeleteFileSystem's own Errors section, and that page publishes no
// InvalidRequest — its five errors are BadRequest, FileSystemNotFound,
// IncompatibleParameterError, InternalServerError and ServiceLimitExceeded. The status was
// already right; the code was the S3 string.
//
// The stem is what was wrong, not the suffix: FSx's Java class is BadRequestException but
// the wire code carries no suffix, so #950's no-suffix instinct was right.
//
// Recorded and not fixed here: API_DeleteFileSystem publishes FileSystemNotFound at **400**,
// where substrate answers 404. That is a status divergence rather than a code one and it is
// shared with Glue's twelve EntityNotFoundException sites and WAFv2's three
// WAFNonexistentItemException sites, so it belongs to one sweep rather than to this change.

// fsxBadRequest reports that a request is malformed or cannot be used as given.
//
// One constructor for every such refusal in the plugin, so the code and the status cannot
// come to differ between the operation that creates a file system and the one that deletes
// it.
func fsxBadRequest(message string) *AWSError {
	return &AWSError{
		Code:       "BadRequest",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
