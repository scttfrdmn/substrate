package emulator

// Systems Manager error construction: one helper per published code, so a code's HTTP status is
// decided once rather than at each call site that answers it.
//
// The file exists for the reason kms_errors.go does, and it collects a finding #950 made across
// twelve handlers: **every body-parse guard in the plugin answered a code Systems Manager does not
// publish, and it answered two different ones.** Ten sites answered InvalidRequest, which appears
// nowhere in Systems Manager's documentation — not on an operation page, not on CommonErrors.html —
// and two more, sendCommand and getCommandInvocation, answered SerializationException, which appears
// nowhere either. The issue counted ten; the two SerializationException sites are the same defect
// wearing a second wrong code, which is what made them easy to miss.
//
// The two also passed encoding/json's own error text through as the message. That is a second thing
// wrong with them rather than a detail: the text describes the emulator's decoder, so a caller was
// being told about Go's json package by an endpoint that is meant to look like AWS. See
// [ssmInvalidBody] for what replaced it.
//
// The replacement is ValidationError at 400, from CommonErrors.html: "The input doesn't meet the
// required format or constraints. Check that all required parameters are included and that values
// are valid." It has to come from the common page, and for this service that is not an argument from
// convenience — it is what the operation pages leave. All twelve were read, and each publishes only
// narrow resource-specific 400s plus InternalServerError at 500: PutParameter's fifteen are about a
// hierarchy, a policy, a key ID, a pattern or a quota; GetParameter's four are InvalidKeyId,
// ParameterNotFound, ParameterVersionNotFound and InternalServerError; the three tag operations
// publish InvalidResourceId, InvalidResourceType, TooManyTagsError and TooManyUpdates. Not one of
// the twelve describes a request that could not be read at all.
//
// Two near misses, declined:
//
//   - ValidationException. Five of the twelve pages name it — GetParameter's is "if the specified
//     name for a parameter contains spaces between characters, the request fails with a
//     ValidationException error" — but every one of the five names it in *prose* and none lists it in
//     an Errors section, so it is not a shape any of those operations publishes. This is the trap
//     #950 flagged before implementation and it survives verification: Systems Manager spells its
//     published input error ValidationError, with no "Exception" suffix, exactly as KMS does.
//
//   - MalformedHttpRequestException, on CommonErrors.html at 400, is declined for the reason
//     [kmsInvalidBody] declines it: its published scope is the transport layer — "the request body
//     can't be processed. This typically happens when the request body can't be decompressed using
//     the specified content encoding algorithm" — and a body that arrived intact and then failed to
//     parse is not that.
//
// The wider finding of #950, recorded in stepfunctions_errors.go rather than repeated here: that
// fifteen-entry common-errors page is AWS boilerplate shared across services, byte-identical between
// Systems Manager, Step Functions and KMS.
//
// [ssmInvalidResourceID] and [ssmInvalidResourceType] moved here from ssm_tags.go, unchanged, so
// that every Systems Manager refusal that is more than a one-off is constructed in one file —
// kms_errors.go's own arrangement, reached the same way (#923).

import (
	"fmt"
	"net/http"
	"strings"
)

// ssmInvalidBody reports that a request body could not be parsed as JSON.
//
// ValidationError at 400, for the reasons this file's preamble gives. It takes no argument, which is
// the deliberate half of the change at the two sites that answered SerializationException with
// err.Error(): encoding/json's message names the emulator's decoder and the Go types it was
// unmarshalling into, and none of that is something a caller can act on. The actionable fact is that
// the body was not JSON, which the message states.
func ssmInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ssmInvalidResourceID reports that an identifier names no Systems Manager resource, naming the
// reason so a caller can tell a wrong service from a wrong resource type from an absent resource.
//
// The code is InvalidResourceId, which all three tag operations publish and which is how Systems
// Manager reports a nonexistent resource — it publishes no distinct not-found code for them. The
// status is 400, which is what all three reference pages give it: "The resource ID isn't valid. Verify
// that you entered the correct ID and try again. HTTP Status Code: 400". Systems Manager answers no
// 404 on any of the three, so the status carries no information a caller can branch on and the code is
// the whole signal (#933). One helper means one status, which is why it lives here rather than at each
// call site.
func ssmInvalidResourceID(id, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidResourceId",
		Message:    fmt.Sprintf("the resource ID %q is not valid: %s", id, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ssmInvalidResourceType reports that a string is not one of AWS's ResourceType values, listing the
// ones that are.
//
// The code and status are AWS's own: InvalidResourceType at 400, published by all three tag
// operations. The message names the valid values because the reference does, and because a caller who
// sent "parameter" for "Parameter" has no other way to see the difference.
func ssmInvalidResourceType(resourceType string) *AWSError {
	return &AWSError{
		Code: "InvalidResourceType",
		Message: fmt.Sprintf("the resource type %q is not valid: valid values are %s",
			resourceType, strings.Join(ssmResourceTypes, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}
