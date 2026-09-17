package emulator

// CertificateArn validation and the two refusals that follow from it (#921).
//
// Every ACM operation that takes a certificate answered one of two things before this: an
// InvalidParameterException for an absent CertificateArn, or a ResourceNotFoundException at HTTP
// 404 for anything the state lookup missed. Both are wrong against the reference, in different
// ways, and the second was wrong in the way that matters more.
//
//  1. ResourceNotFoundException is published at HTTP 400, not 404. AddTagsToCertificate,
//     RemoveTagsFromCertificate, ListTagsForCertificate, DescribeCertificate and DeleteCertificate
//     all give it 400, with the description "The specified certificate cannot be found in the
//     caller's account or the caller's account cannot be found." Every code ACM publishes on those
//     pages is 400. This is the same finding #910 made for Step Functions and #933 for Systems
//     Manager: the status comes from the operation's own reference page, not from what the code
//     name suggests.
//
//  2. A malformed ARN reached the state lookup and was reported absent. That answers the wrong
//     question — the caller is told the certificate is not there when the string they sent could
//     not name a certificate in the first place, so retrying against a real certificate is the
//     obvious next move and it will not help.
//
// The refusal is in three tiers, and they do not share a provenance, so each is named:
//
//   - A value breaking a documented constraint on the member is ValidationException/400, whose own
//     description is exactly that: "The supplied input failed to satisfy constraints of an AWS
//     service." CertificateArn publishes Length 20-2048 and the Pattern below, so this tier is
//     AWS's own. It also covers an absent CertificateArn, which fails the minimum length — that
//     is why there is no separate required-member check any more, and it replaces an
//     InvalidParameterException that ListTagsForCertificate and DescribeCertificate do not
//     publish at all.
//
//   - An ARN that satisfies the Pattern but names no certificate is InvalidArnException/400.
//     This tier is **substrate's reading**. AWS publishes the code on all five operations but
//     describes it as "The requested Amazon Resource Name (ARN) does not refer to an existing
//     resource" — a statement about non-existence, not about syntax — so the mapping from "this
//     ARN cannot name a certificate" to this code is an inference, not a quotation. It is the
//     closest published statement: an ARN naming an ACM resource type substrate does not model
//     cannot refer to an existing certificate no matter what state holds.
//
//   - A well-formed certificate ARN with no record is ResourceNotFoundException/400, per the
//     description quoted above.
//
// What #921 deliberately left alone, and #950 settled: an unparseable request body answered
// InvalidParameterException. That is a protocol-level failure rather than a member-constraint
// violation, so its code belongs to ACM's common errors rather than to any one operation's list,
// and settling it was a separate question. It is settled in [acmInvalidBody], which confirmed that
// sentence's guess.
//
// AccessDeniedException is also published at 400 by ACM while substrate answers it at 403. That is
// not fixed here: the 403 comes from the central authorization check every service shares, and
// making it per-service would have the emulator answer two statuses for one decision. It is
// recorded in docs/services.md rather than silently skipped.

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// acmCertificateARNPattern is the Pattern AWS publishes for CertificateArn, anchored.
//
// Taken verbatim from the reference — arn:[\w+=/,.@-]+:acm:[\w+=/,.@-]*:[0-9]+:[\w+=,.@-]+(/[\w+=,.@-]+)*
// — and anchored at both ends, because an AWS Pattern constrains the whole value while Go's
// MatchString reports a match anywhere in it.
//
// Two things it does not say carry weight, and both are why [acmValidateCertificateARN] has a
// second tier. The Region field is starred, so an ARN with an empty Region satisfies the pattern
// while naming no Region at all. And the resource field is any slash-separated run of tag
// characters, so the pattern alone does not restrict an ARN to the certificate type — that
// restriction is published in prose on each page ("This action applies only to the certificate
// resource type"), and ACM does have other types with their own shapes. See acm_tags.go's
// preamble.
var acmCertificateARNPattern = regexp.MustCompile(
	`^arn:[\w+=/,.@-]+:acm:[\w+=/,.@-]*:[0-9]+:[\w+=,.@-]+(/[\w+=,.@-]+)*$`)

// The published Length range for CertificateArn. Named rather than written inline because the
// message quotes them, and a message disagreeing with the check it explains is worse than no
// message.
const (
	acmCertificateARNMinLen = 20
	acmCertificateARNMaxLen = 2048
)

// acmValidateCertificateARN reports whether arn can name an ACM certificate, returning the
// [AWSError] to refuse it with when it cannot.
//
// Shape and type only: a well-formed certificate ARN naming a certificate that does not exist is
// valid here, and the caller answers [acmCertificateNotFound] for that after its state lookup.
// The two are kept apart on purpose, because they tell a caller different things — one says the
// string is wrong and retrying is pointless, the other says the string is fine and the resource
// is not there. See this file's preamble for which tier is AWS's and which is substrate's reading.
//
// Length is checked before the pattern so an oversized value is refused on its size rather than
// run through a regexp, following [iamValidatePolicyARN].
func acmValidateCertificateARN(arn string) *AWSError {
	if len(arn) < acmCertificateARNMinLen || len(arn) > acmCertificateARNMaxLen {
		return acmValidationError(fmt.Sprintf(
			"CertificateArn must be between %d and %d characters long, and this one is %d",
			acmCertificateARNMinLen, acmCertificateARNMaxLen, len(arn)))
	}
	if !acmCertificateARNPattern.MatchString(arn) {
		return acmValidationError(fmt.Sprintf(
			"CertificateArn %q does not satisfy the pattern an ACM ARN must match", arn))
	}

	// The pattern guarantees six colon-separated fields and that the last of them holds no
	// colon, so the split below cannot come up short and its tail is the whole resource.
	fields := strings.SplitN(arn, ":", 6)
	if fields[3] == "" {
		return acmInvalidARN(arn, "it names no Region, and a certificate is regional")
	}
	resourceType, id, ok := strings.Cut(fields[5], "/")
	if !ok || id == "" {
		return acmInvalidARN(arn, "it names an ACM resource type with no identifier after it")
	}
	if resourceType != acmCertificateResourceType {
		return acmInvalidARN(arn, fmt.Sprintf(
			"it names a %q, and only a %s can be addressed here",
			resourceType, acmCertificateResourceType))
	}
	if strings.Contains(id, "/") {
		return acmInvalidARN(arn, "it names something nested under a certificate")
	}
	return nil
}

// acmCertificateARNAccountRegion returns the account and Region a certificate ARN names.
//
// It is only correct for an ARN [acmValidateCertificateARN] has accepted, which is what guarantees
// the six fields it indexes. Split out so the tagging API's resolver takes both from the ARN
// without re-parsing it a second, subtly different way — the rule #826 through #937 established,
// made structural rather than remembered.
func acmCertificateARNAccountRegion(arn string) (accountID, region string) {
	fields := strings.SplitN(arn, ":", 6)
	return fields[4], fields[3]
}

// acmValidationError refuses a CertificateArn that breaks one of its published constraints.
//
// ValidationException/400 is AWS's own code for this, described as "The supplied input failed to
// satisfy constraints of an AWS service", and it is published on every ACM operation that takes a
// certificate — including the two, ListTagsForCertificate and DescribeCertificate, whose published
// error list is only three codes long and does not include InvalidParameterException.
func acmValidationError(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// acmInvalidARN refuses an ARN that satisfies the published pattern but cannot name a certificate,
// naming the reason.
//
// InvalidArnException/400. The code is AWS's and the status is the one all five pages give it; the
// decision that this is the code for a pattern-valid ARN naming something else is substrate's
// reading, for the reason set out in this file's preamble.
func acmInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArnException",
		Message:    fmt.Sprintf("the ARN %q does not refer to an ACM certificate: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// acmInvalidBody reports that a request body would not decode.
//
// ValidationError/400, from ACM's common-errors page rather than from any operation's own list, and
// this is the one refusal in the plugin where that is the right provenance. The six sites answered
// InvalidParameterException, which ACM publishes on only **three** of the six guarded operations —
// RequestCertificate, AddTagsToCertificate and RemoveTagsFromCertificate — and omits from
// DescribeCertificate, DeleteCertificate and ListTagsForCertificate. Answering it everywhere leaves
// three sites reporting a code their own operation does not publish (#950's defect relocated);
// answering it only where published makes one failure produce two codes inside one plugin.
//
// [acmValidationError]'s ValidationException is not the answer either, near as the name is: it is
// published on five of the six and **not** on RequestCertificate, so it has the same problem one
// operation smaller. The two codes sitting side by side in this file is therefore deliberate. A
// CertificateArn breaking a published constraint is ValidationException, because every operation
// taking a certificate publishes it; a body that would not parse belongs to no operation, so it takes
// the code from the page that belongs to no operation.
//
// It takes no argument so that encoding/json's own text cannot reach a caller; see
// [kinesisInvalidBody] for the same rule.
func acmInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// acmCertificateNotFound reports that a well-formed certificate ARN names no certificate substrate
// holds.
//
// ResourceNotFoundException at HTTP **400**, which is what all five reference pages give it —
// "The specified certificate cannot be found in the caller's account or the caller's account
// cannot be found. HTTP Status Code: 400". Substrate answered 404, a status ACM publishes nowhere.
//
// One helper for all five call sites, so the status cannot come to differ between the operation
// that reads a certificate and the one that tags it (#921).
func acmCertificateNotFound(arn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "Certificate not found: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}
