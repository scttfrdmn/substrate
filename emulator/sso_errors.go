package emulator

import "net/http"

// IAM Identity Center's refusals for a request substrate could not use.
//
// ValidationException at 400 is published on all three of API_CreateAccountAssignment,
// API_DeleteAccountAssignment and API_ListAccountAssignments, glossed "The request failed
// because it contains a syntax error." It is also what the plugin's two already-checked
// guards answered before #1062 — createPermissionSet's Name check and loadPermissionSet's
// PermissionSetArn check — so the code is the plugin's own established answer rather than
// something this change introduces, and [ssoInvalidBody] carries it too.
//
// The gloss says "syntax error" rather than "missing parameter", which is worth recording
// because it is the weaker of the two readings available. The alternative was the JSON
// common list's ValidationError, whose sentence names required parameters outright. It was
// declined on the order #950 settled: a code published on the operation's own page takes
// precedence over the common list, and ValidationException is the only input-validation code
// any of the three pages publishes. Answering the common list's code here would also have
// split one plugin across two codes for one class of caller error — the defect #1063 spent
// eleven sites undoing in Glue, FSx and WAFv2.
//
// What #1062 did *not* add, recorded so the omission is deliberate: TargetType's single
// published value AWS_ACCOUNT and PrincipalType's USER | GROUP are not enforced. An
// unrecognized value is the present-but-invalid case, which is a different axis from an
// absent member and needs its own reading of which code applies — see
// [wafv2ValidateCreateIPSet] and [wafv2ValidateScope], the two places substrate has drawn that
// line so far, both in a service that publishes a separate code for it.

// ssoValidationException reports that an IAM Identity Center request cannot be used as given.
//
// One constructor for every such refusal in the plugin, so the code and the status cannot
// come to differ between the operation that creates a permission set and the one that
// assigns it.
func ssoValidationException(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ssoMember pairs a request member's published name with the value a request carried, so
// [ssoRequireMembers] can name the one it refuses.
type ssoMember struct {
	name  string
	value string
}

// ssoRequireMembers returns the refusal for the first of members that is empty, or nil when
// every one carries a value.
//
// The three account-assignment operations mark six, six and three members Required: Yes and
// checked none of them before #1062, so CreateAccountAssignment with no body answered 200
// and a SUCCEEDED status for an assignment whose permission set, target and principal were
// all the empty string — and DeleteAccountAssignment answered SUCCEEDED for deleting it.
// Taking the members in the order the page lists them makes the refusal reproducible when a
// caller omits several.
func ssoRequireMembers(members ...ssoMember) *AWSError {
	for _, m := range members {
		if m.value == "" {
			return ssoValidationException(m.name + " is required")
		}
	}
	return nil
}
