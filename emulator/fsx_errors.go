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
// FSx is no longer part of the not-found status sweep this file once said it belonged to, and the
// note has been corrected rather than deleted (#1234), because the grouping still has members.
// All three FileSystemNotFound sites answer 400, which is what API_DeleteFileSystem publishes:
// fsx_plugin.go:230 and :244 in describeFileSystems, :300 in deleteFileSystem. What remains of the
// grouping is Glue's twelve EntityNotFoundException sites (glue_plugin.go:171 through :898) and
// WAFv2's three WAFNonexistentItemException sites (wafv2_plugin.go:454, :695, :722), all of which
// still answer 404 — so the class stays findable from here without FSx being a phantom site in it.
// Whether 404 is a divergence at either service is that sweep's question to settle against their
// own pages, not one this file can answer for them.

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
