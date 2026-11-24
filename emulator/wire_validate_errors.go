package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// The services request validation covers, and what each answers (#1612).
//
// The models say which constraints an input carries; they do not say what a service answers
// when one fails, so each validated service has an entry here naming the code and status for
// each kind of failure and the form of the message, each with its source.

// wireMessageStyle is the form a service's validation message takes.
type wireMessageStyle uint8

const (
	// wireStyleQuoted is the form most JSON services answer with: "1 validation error
	// detected: Value 'x' at 'member' failed to satisfy constraint: Member must …", and for
	// several failures "N validation errors detected: …; …". A missing member renders as
	// "Value null at 'member'".
	wireStyleQuoted wireMessageStyle = iota + 1
	// wireStyleOfTheInput is [wireStyleQuoted] with the value withheld: "Value of the input at
	// 'member' failed to satisfy constraint: …". A missing member still renders as "Value null
	// at 'member'".
	wireStyleOfTheInput
)

// wireErrorCode is a code and HTTP status.
type wireErrorCode struct {
	code   string
	status int
}

// wireServiceRules is one validated service's entry.
type wireServiceRules struct {
	// ops limits validation to these operations; nil validates every operation the tables
	// hold for the service.
	ops map[string]bool
	// missing answers an absent required member, and invalid every other failure.
	missing, invalid wireErrorCode
	// style is the message's form.
	style wireMessageStyle
	// enum and pattern opt the service into enum and pattern checks, each only on a source
	// showing the service enforces them.
	enum, pattern bool
}

// covers reports whether the rules validate op.
func (r wireServiceRules) covers(op string) bool {
	return r.ops == nil || r.ops[op]
}

// validatedServices lists the services whose requests are validated, by plugin name. A
// service absent from it is not validated at all, whatever the tables hold for it.
var validatedServices = map[string]wireServiceRules{ //nolint:gochecknoglobals // immutable table
	// ACM, JSON 1.1. Every ACM operation that takes a certificate publishes ValidationException
	// at HTTP 400, "The supplied input failed to satisfy constraints of an AWS service", in its
	// own Errors section (API_DescribeCertificate, API_DeleteCertificate,
	// API_AddTagsToCertificate, API_RemoveTagsFromCertificate, API_ListTagsForCertificate).
	// RequestCertificate's section omits it, and ACM answers it there anyway; the message
	// forms below are ACM's observed answers on RequestCertificate:
	//
	//   - "1 validation error detected: Value null at 'domainName' failed to satisfy
	//     constraint: Member must not be null" for an absent DomainName, captured against
	//     real ACM in ministackorg/ministack#2588 (2026-11-04), and the same form at
	//     'domainValidationOptions.1.member.validationDomain' in go-to-k/cdkd#5281;
	//   - "Value of the input at 'domainName' failed to satisfy constraint: Member must have
	//     length less than or equal to 253" for a 254-character domain (mulgadc/spinifex#1181);
	//   - "Value of the input at 'tags.2.member.value' failed to satisfy constraint: Member
	//     must satisfy regular expression pattern: …", an AWS answer quoted in
	//     kubernetes-sigs/aws-load-balancer-controller#4836, which is the source for the
	//     pattern opt-in and for the element path form;
	//   - "Value of the input at 'options.certificateTransparencyLoggingPreference' failed to
	//     satisfy constraint: Member must satisfy enum value set: [ENABLED, DISABLED]",
	//     captured in ministackorg/ministack#2588, the source for the enum opt-in.
	"acm": {
		missing: wireErrorCode{"ValidationException", http.StatusBadRequest},
		invalid: wireErrorCode{"ValidationException", http.StatusBadRequest},
		style:   wireStyleOfTheInput,
		enum:    true,
		pattern: true,
	},
}

// wireRefusal renders one failure in service's validation form. A plugin calls it for a check it
// keeps because a request the gate never sees reaches it — a stack's or a schedule's dispatch —
// so that request is answered exactly as the gate answers a client's.
func wireRefusal(service string, f wireFailure) *AWSError {
	return validatedServices[service].refusal([]wireFailure{f})
}

// wireMissingMember is the failure for an absent required member at path; wire is its name
// on the wire, which the EC2 form reports.
func wireMissingMember(path, wire string) wireFailure {
	return wireFailure{kind: wireMissing, path: path, wire: wire, constraint: "Member must not be null"}
}

// refusal renders failures into the service's error.
//
// The count and separators follow AWS's published form for several failures, "2 validation
// errors detected: Value at 'name.givenName' failed to satisfy constraint: Member must satisfy
// regular expression pattern: …; Value at 'name.givenName' failed to satisfy constraint: Member
// must have length greater than or equal to 1" (IAM Identity Center User Guide,
// "Troubleshooting IAM Identity Center issues"). Failures are listed in the model's member
// order, depth first, and each member's in the order length, range, enum, pattern; AWS
// publishes no order, so that order is substrate's.
//
// The code is the first failure's: a missing member's code when the first failure is one, the
// invalid code otherwise.
func (r wireServiceRules) refusal(failures []wireFailure) *AWSError {
	parts := make([]string, len(failures))
	for i, f := range failures {
		parts[i] = r.clause(f)
	}
	noun := "validation errors"
	if len(failures) == 1 {
		noun = "validation error"
	}
	code := r.invalid
	if failures[0].kind == wireMissing {
		code = r.missing
	}
	return &AWSError{
		Code:       code.code,
		Message:    fmt.Sprintf("%d %s detected: %s", len(failures), noun, strings.Join(parts, "; ")),
		HTTPStatus: code.status,
	}
}

// clause renders one failure in the service's style.
func (r wireServiceRules) clause(f wireFailure) string {
	var subject string
	switch {
	case f.kind == wireMissing:
		subject = "Value null"
	case r.style == wireStyleOfTheInput:
		subject = "Value of the input"
	case f.hasValue:
		subject = fmt.Sprintf("Value '%s'", f.value)
	default:
		subject = "Value"
	}
	return fmt.Sprintf("%s at '%s' failed to satisfy constraint: %s", subject, f.path, f.constraint)
}
