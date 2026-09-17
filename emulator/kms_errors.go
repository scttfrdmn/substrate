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

// kmsKeyDisabled reports that an operation named a key that exists and is not enabled.
//
// The code is DisabledException at 400, published identically on API_Encrypt, API_Decrypt,
// API_GenerateDataKey, API_GenerateDataKeyWithoutPlaintext, API_ReEncrypt, API_EnableKeyRotation and
// API_DisableKeyRotation — the seven operations substrate refuses this way, every one of which gives a
// Disabled key footnote [1] in the developer guide's key-state table. The rotation pair was added by
// #949; before it they wrote RotationEnabled against a disabled key and answered 200, which #923
// recorded as a missing refusal rather than a wrong status. GenerateDataKeyWithoutPlaintext and
// ReEncrypt were added by #961, which found they refused a disabled key nowhere at all.
//
// Every caller reaches this through [kmsKeyStateError], which is what rules out PendingDeletion first —
// the key-state table gives that state a different code, and ScheduleKeyDeletion clears Enabled as it
// writes the state, so a bare !key.Enabled test at a call site would answer this for a key pending
// deletion. See [kmsInvalidKeyState].
func kmsKeyDisabled(keyID string) *AWSError {
	return &AWSError{
		Code:       "DisabledException",
		Message:    fmt.Sprintf("the KMS key %q is not enabled", keyID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidKeyState reports that a key exists but is in a key state the operation does not permit.
//
// The code is KMSInvalidStateException at 400 — "the request was rejected because the state of the
// specified resource is not valid for this request" — published on every operation reached from this
// package that reads or writes a key, API_EnableKeyRotation and API_DisableKeyRotation included.
//
// It is a separate code from DisabledException rather than a synonym for it, and the developer
// guide's "Key states of AWS KMS keys" table is where the two divide: for both rotation operations a
// Disabled key is footnote [1], "DisabledException: <key ARN> is disabled", while a key pending
// deletion is footnote [3], "KMSInvalidStateException: <key ARN> is pending deletion". So a caller
// distinguishing "enable the key and retry" from "cancel the deletion and retry" reads the code, and
// answering DisabledException for both would collapse two different remedies into one. The state is
// named in the message for the same reason.
//
// For the five cryptographic operations #961 brought here the same table cell reads "[2] or [3]", and
// footnote [2] is that same sentence under **DisabledException** — so AWS admits either code there and
// the DisabledException substrate answered before #961 was not outside what the table publishes.
// Choosing this one anyway is substrate's reading, argued in [kmsKeyStateError]: it is the only
// choice under which one key state produces one code across the plugin, and the only one a caller can
// branch on to tell the two remedies apart.
//
// PendingDeletion is the only state substrate can be in here: ScheduleKeyDeletion is the sole writer
// of anything but Enabled or Disabled, so PendingImport, Unavailable, Creating and Updating — which
// the table also refuses — are unreachable and are recorded as such rather than guarded against.
//
// For the inverse refusal — an operation that requires PendingDeletion and did not find it — see
// [kmsKeyNotPendingDeletion], which carries the same code and a different message because AWS's
// footnote for it is a negation.
func kmsInvalidKeyState(keyID, state string) *AWSError {
	return &AWSError{
		Code:       "KMSInvalidStateException",
		Message:    fmt.Sprintf("the KMS key %q is in state %q, which this operation does not permit", keyID, state),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsKeyNotPendingDeletion reports that CancelKeyDeletion named a key that is not scheduled for
// deletion, so there is no deletion to cancel.
//
// The code is KMSInvalidStateException at 400, the same as [kmsInvalidKeyState], and this is a
// separate helper only because of the message. CancelKeyDeletion is the one operation in the
// developer guide's key-state table whose permitted set is a single state: every row but
// PendingDeletion is footnote [4], and that footnote is phrased as a negation —
// "KMSInvalidStateException: <key ARN> is not pending deletion" — where every other footnote names
// the offending state. Naming the state here would read as though some other state were the problem,
// when the problem is the absence of the one state the operation needs.
//
// Answering this at all is the point of #963: before it, CancelKeyDeletion on a key that had never
// been scheduled answered 200 and enabled the key, which is EnableKey reached through an operation a
// caller may hold no iam:EnableKey permission for.
func kmsKeyNotPendingDeletion(keyID string) *AWSError {
	return &AWSError{
		Code:       "KMSInvalidStateException",
		Message:    fmt.Sprintf("the KMS key %q is not pending deletion, so there is no deletion to cancel", keyID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidPendingWindow reports a ScheduleKeyDeletion waiting period outside the published range.
//
// API_ScheduleKeyDeletion gives PendingWindowInDays a Valid Range of 7 to 30 and states it twice —
// "you can specify a waiting period of 7-30 days" in the prose and "if you include a value, it must
// be between 7 and 30, inclusive" on the parameter — but publishes no error code for violating it:
// its list is DependencyTimeoutException, InvalidArnException, KMSInternalException,
// KMSInvalidStateException and NotFoundException, none of which describes a bad parameter value. So
// the code here is substrate's reading, taken from CommonErrors.html for the same reason
// [kmsInvalidBody] takes it: a failure that belongs to no operation-specific code has to come from
// the common set, and ValidationError at 400 is the one that fits a value out of range.
//
// The bound is stated in the message rather than left implicit, because a caller that sent 1 or 365
// has no way to discover 7-30 from a bare refusal.
func kmsInvalidPendingWindow(days int) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"PendingWindowInDays is %d, which is outside the valid range of %d to %d",
			days, kmsMinPendingWindowInDays, kmsMaxPendingWindowInDays),
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
