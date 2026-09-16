package emulator

// KMS error construction: one helper per published code, so a code's HTTP status is decided once
// rather than at each of the thirty-nine call sites that answered it.
//
// The reason for the file is a finding that holds across every KMS operation substrate models, and
// it is worth stating before the helpers because it is what makes them one-liners: **KMS publishes
// no 404 for any resource.** Across the fifteen operation pages reached from this package, exactly
// two statuses appear — 500 for DependencyTimeoutException, KMSInternalException and
// KeyUnavailableException, and 400 for everything else, NotFoundException included. The only 404
// anywhere in KMS's documentation is UnknownOperationException on CommonErrors.html, which reports
// that the *action name* was not recognized. So a KMS status carries nothing a caller can branch on
// and the code is the whole signal — the shape #933 recorded for Systems Manager, arrived at from
// the other direction.
//
// Three codes were wrong before #923, each in a different way:
//
//   - NotFoundException answered HTTP 404 at fifteen sites. Every one of the fifteen belongs to an
//     operation whose own reference page gives it 400: "The request was rejected because the
//     specified entity or resource could not be found. HTTP Status Code: 400". That is the status
//     kms_tags.go's own refusals already used, so the two halves of the package disagreed.
//
//   - DisabledException answered HTTP 409 at three sites. API_Encrypt, API_Decrypt and
//     API_GenerateDataKey each publish it at 400: "The request was rejected because the specified
//     KMS key is not enabled".
//
//   - InvalidRequest was answered at twenty-one sites for an unparseable request body, and the
//     string appears nowhere in KMS's documentation — not on an operation page, not on
//     CommonErrors.html. A caller matching on it matched something no SDK models. The replacement
//     is ValidationError, published on CommonErrors.html at 400: "The input doesn't meet the
//     required format or constraints. Check that all required parameters are included and that
//     values are valid." Being a common error, it applies to every operation, which the
//     twenty-one sites need and TagException does not give: TagException is also 400 but is
//     glossed "one or more tags are not valid", which is the content of a Tags member, and
//     eighteen of the twenty-one operations take no tags at all.
//
// Three codes deliberately not reached for, recorded because each is a plausible guess that would
// repeat the defect above. MalformedHttpRequestException is on CommonErrors.html at 400 but its
// published scope is the transport layer — "This typically happens when the request body can't be
// decompressed using the specified content encoding algorithm" — and a body that decompressed and
// then failed to parse is not that. SerializationException and ValidationException appear nowhere
// in KMS's documentation at all; KMS spells it ValidationError with no "Exception" suffix, unlike
// most JSON-protocol services, so a caller matching ValidationException would match nothing.
//
// Messages are substrate's throughout. AWS publishes codes and statuses, not message text.

import (
	"fmt"
	"net/http"
)

// kmsNotFound reports that a KeyId, an alias or a destination key names nothing KMS holds.
//
// The code is NotFoundException and the status is 400, which every operation that can answer it
// publishes; see this file's preamble for why no KMS refusal is a 404. The detail is passed through
// rather than composed here because the callers name different things — a key, an alias, the
// destination key of a ReEncrypt — and a caller reading a log needs to know which.
func kmsNotFound(detail string) *AWSError {
	return &AWSError{
		Code:       "NotFoundException",
		Message:    detail,
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsKeyDisabled reports that a cryptographic operation named a key that exists and is not enabled.
//
// The code is DisabledException at 400, published on API_Encrypt, API_Decrypt and
// API_GenerateDataKey — the three operations substrate refuses this way. Note that
// API_EnableKeyRotation and API_DisableKeyRotation publish it too and substrate does not check
// there; that is a missing refusal rather than a wrong status, recorded on #923 and filed
// separately rather than folded into a status change.
func kmsKeyDisabled(keyID string) *AWSError {
	return &AWSError{
		Code:       "DisabledException",
		Message:    fmt.Sprintf("the KMS key %q is not enabled", keyID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidBody reports that a request body could not be parsed as JSON.
//
// The code is ValidationError at 400, from CommonErrors.html, which is where a failure that belongs
// to no single operation has to come from — the twenty-one sites that call this span twenty-one
// operations. It takes no argument because the encoding/json error text describes the emulator's
// decoder rather than anything a caller can act on: the actionable fact is that the body was not
// JSON, which the message states.
func kmsInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidARN reports that an ARN is not one KMS accepts, naming the reason so a caller can tell
// a wrong service from a wrong resource type.
//
// InvalidArnException at 400, which API_TagResource, API_UntagResource and API_ListResourceTags
// each publish and gloss as "the request was rejected because a specified ARN, or an ARN in a key
// policy, is not valid". Moved here from kms_tags.go by #923 so that every KMS refusal is
// constructed in one file.
func kmsInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArnException",
		Message:    fmt.Sprintf("the ARN %q is not valid: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
