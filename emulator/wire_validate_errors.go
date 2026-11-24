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
	// wireStyleElided is [wireStyleQuoted] with the value left out: "Value at 'member' failed to
	// satisfy constraint: …".
	wireStyleElided
	// wireStyleEC2 is the EC2 API's form, which names one parameter rather than listing
	// failures: "Value (x) for parameter Name is invalid." for a value, and the rules'
	// missingSentence for an absent parameter.
	wireStyleEC2
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
	// missingSentence, when set, is the service's own sentence for an absent required member,
	// with a %s for the parameter's wire name. A request missing one is refused with that
	// sentence alone, naming the first such member, rather than with the validator's list.
	missingSentence string
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

	// SESv2, REST-JSON. BadRequestException at HTTP 400, "The input you provided is invalid.", is
	// in the Errors section of every SESv2 operation substrate routes (see sesv2_errors.go). The
	// message form is SESv2's observed answer, which leaves the value out: "BadRequestException: 1
	// validation error detected: Value at 'deliveryOptions.maxDeliverySeconds' failed to satisfy
	// constraint: Member must have value greater than or equal to 300"
	// (hashicorp/terraform-provider-aws#40591). The enum opt-in is from "Value at
	// 'suppressionOptions.suppressedReasons' failed to satisfy constraint: Member must satisfy
	// constraint: [Member must satisfy enum value set: [BOUNCE, COMPLAINT]]"
	// (hashicorp/terraform-provider-aws#38207). No source shows SESv2's answer for a missing member,
	// so its "Value null at" form is the one every other service in this table answers, and is
	// substrate's reading for SESv2.
	"sesv2": {
		missing: wireErrorCode{"BadRequestException", http.StatusBadRequest},
		invalid: wireErrorCode{"BadRequestException", http.StatusBadRequest},
		style:   wireStyleElided,
		enum:    true,
	},

	// Route 53, REST-XML. InvalidInput at HTTP 400, "The input is not valid.", is in the Errors
	// section of every Route 53 operation substrate routes (API_CreateHostedZone,
	// API_ChangeResourceRecordSets, API_GetHostedZone, API_DeleteHostedZone, API_ListHostedZones,
	// API_ListResourceRecordSets). The message form, value quoted, is Route 53's observed answer:
	// "InvalidInput: 1 validation error detected: Value '/hostedzone/…' at 'resourceId' failed to
	// satisfy constraint: Member must have length less than or equal to 32"
	// (hashicorp/terraform-provider-aws#37817). The enum opt-in is from "InvalidInput: 1 validation
	// error detected: Value 'to' at 'startRecordType' failed to satisfy constraint: Member must
	// satisfy enum value set: [AAAA, A, CAA, NS, SOA, SPF, MX, PTR, CNAME, TXT, SRV, NAPTR]"
	// (hashicorp/terraform-provider-aws#11677). Route 53 lists the values in an order of its own;
	// substrate lists them in the model's.
	"route53": {
		missing: wireErrorCode{"InvalidInput", http.StatusBadRequest},
		invalid: wireErrorCode{"InvalidInput", http.StatusBadRequest},
		style:   wireStyleQuoted,
		enum:    true,
	},

	// ElastiCache, Query. Its common-errors page publishes InvalidParameterValue at 400, "An
	// invalid or out-of-range value was supplied for the input parameter", and a missing member
	// is answered with it too, in ElastiCache's own sentence: "InvalidParameterValue: The parameter
	// CacheSubnetGroupName must be provided and must not be blank." (Stack Overflow, "The parameter
	// CacheSubnetGroupName must be provided and must not be blank", quoting AWS through Terraform;
	// cloudposse/terraform-aws-elasticache-redis#89 for CacheParameterGroupName). No source shows
	// ElastiCache's message for a length, so that form is the quoted one, and is substrate's own.
	"elasticache": {
		missing:         wireErrorCode{"InvalidParameterValue", http.StatusBadRequest},
		invalid:         wireErrorCode{"InvalidParameterValue", http.StatusBadRequest},
		style:           wireStyleQuoted,
		missingSentence: "The parameter %s must be provided and must not be blank.",
	},

	// EC2, key pairs and placement groups only: the slice #1612 pilots, chosen because their
	// handlers' own checks are few and their tests exercise them over the wire. The rest of EC2
	// follows with the protocol batches. The codes and sentences are [wireEC2MissingSentence]'s.
	"ec2": {
		ops: map[string]bool{
			"CreateKeyPair": true, "DeleteKeyPair": true, "DescribeKeyPairs": true, "ImportKeyPair": true,
			"CreatePlacementGroup": true, "DeletePlacementGroup": true, "DescribePlacementGroups": true,
		},
		missing:         wireErrorCode{"MissingParameter", http.StatusBadRequest},
		invalid:         wireErrorCode{"InvalidParameterValue", http.StatusBadRequest},
		style:           wireStyleEC2,
		missingSentence: wireEC2MissingSentence,
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

// wireRequireMember refuses the top-level member name, which a plugin found absent or blank, as
// the gate does. An absent member, and a blank one the model gives a minimum length, are refused
// through the gate's own walk over that member ([wireCheckMember]), so the answer is the gate's
// byte for byte. A blank one the model does not bound, which the gate lets through, is refused
// in the same form as an absent one. name is both the model's member name and, for the Query
// services that call this, the parameter's wire name.
func wireRequireMember(req *AWSRequest, name string) *AWSError {
	if err := wireCheckMember(req, name, wireMissing, wireTooShort); err != nil {
		return err
	}
	return wireRefusal(req.Service, wireMissingMember(lowerFirst(name), name))
}

// The EC2 API's sentences. MissingParameter and InvalidParameterValue are the codes the EC2 API
// reference's error-codes page publishes for "The request is missing a required parameter" and "A
// value specified in a parameter is not valid", both at 400. The wording is EC2's observed answer:
// "The request must contain the parameter instanceId (Service: AmazonEC2; Status Code: 400; Error
// Code: MissingParameter …)" (Amazon developer forums, "EC2 Missing parameter") and "MissingParameter:
// The request must contain the parameter ebs" (hashicorp/packer#3799), which [ec2MissingParameter]
// already answers. The value sentence is the form EC2's KeyType refusal takes in
// [EC2Plugin.createKeyPair]; no source publishes it for a model constraint, so it is substrate's.
const (
	wireEC2MissingSentence = "The request must contain the parameter %s"
	wireEC2InvalidSentence = "Value (%s) for parameter %s is invalid."
)

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
	first := failures[0]
	if r.missingSentence != "" && first.kind == wireMissing {
		return &AWSError{Code: r.missing.code, Message: fmt.Sprintf(r.missingSentence, first.wire), HTTPStatus: r.missing.status}
	}
	if r.style == wireStyleEC2 {
		return &AWSError{Code: r.invalid.code, Message: fmt.Sprintf(wireEC2InvalidSentence, first.value, first.wire),
			HTTPStatus: r.invalid.status}
	}
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
	case r.style == wireStyleElided:
		subject = "Value"
	case f.hasValue:
		subject = fmt.Sprintf("Value '%s'", f.value)
	default:
		subject = "Value"
	}
	return fmt.Sprintf("%s at '%s' failed to satisfy constraint: %s", subject, f.path, f.constraint)
}
