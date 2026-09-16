package emulator

// CloudFront tagging: the ARN resolver TagResource, UntagResource and ListTagsForResource all
// three key through.
//
// Split out of cloudfront_plugin.go for the reason stepfunctions_tags.go was split out of
// stepfunctions_plugin.go: the resolver is the load-bearing part, and here it was two lines
// of substring search that got both halves wrong.
//
// The three operations shared one resolver already — resolveTagTarget, which #883 introduced so
// they could not drift on which resource an ARN addresses — but that resolver read the
// distribution *ID* out of the ARN and took the *account* from the caller's own request context.
// So arn:aws:cloudfront::999988887777:distribution/E1EXAMPLE addressed the caller's own
// E1EXAMPLE. UntagResource is the damaging direction, and it is the same argument #826 made for
// SQS and DynamoDB, #845 for the tagging resolver and #910 for Step Functions: stripping a tag
// can turn an aws:ResourceTag Deny into an allow, and it answered 204 while doing it (#918).
//
// The account is in the ARN and the reference says so. ListTagsForResource publishes the
// Resource parameter's pattern verbatim — "arn:aws(-cn)?:cloudfront::[0-9]+:.* Required: Yes" —
// in which [0-9]+ is the account and the empty segment between "cloudfront" and it is the
// Region, absent because CloudFront is global.
//
// The other half was the type match. It was strings.LastIndex(arn, "distribution/"), which is
// unanchored, so arn:aws:cloudfront::123456789012:streaming-distribution/E1EXAMPLE resolved to
// the web distribution E1EXAMPLE — a different resource type reaching a record it does not name.
// That is the anchoring mistake #910 found one layer up in strings.Contains(arn, ":stateMachine:").
// The type is now the resource portion's first "/"-delimited segment and is compared whole.
//
// A CloudFront ARN of any other resource type is refused rather than resolved. Two of them are
// refused on the reference's own authority: the developer guide's tagging page states "You can
// tag distributions, but you can't tag origin access identities or invalidations", and substrate
// stores invalidations in this namespace (cfinval:) while CloudFormation mints origin access
// identities (#859), so both are reachable-looking targets. The rest — a function, a cache
// policy, a key group, a streaming distribution — are resource types substrate does not model at
// all, so there is no record for an ARN naming one to address.

import (
	"fmt"
	"net/http"
	"strings"
)

// cfDistributionResourceType is the one CloudFront ARN resource type substrate resolves for
// tagging. See this file's preamble for the types deliberately refused and why.
const cfDistributionResourceType = "distribution"

// cfTagTarget is the distribution a CloudFront tagging ARN names: the account that owns it and
// its identifier, both taken from the ARN.
type cfTagTarget struct {
	// AccountID is the account segment of the ARN, never the caller's.
	AccountID string

	// DistID is the distribution identifier, e.g. E1EXAMPLE.
	DistID string
}

// cfParseDistributionARN parses a CloudFront tagging ARN and returns the distribution it names.
//
// It takes no *RequestContext, which is what makes "the account comes from the ARN" structural
// rather than a thing each of the three tagging operations has to remember — the arrangement
// [sfnResolveARN] settled on for Step Functions.
//
// The two refusals answer different published codes, because they are different failures. A
// malformed ARN — not an ARN, not CloudFront, carrying a Region CloudFront ARNs do not have, or
// naming no resource — is InvalidArgument/400, which the reference glosses as "an argument is
// invalid" and the ARN is the argument. A well-formed CloudFront ARN naming a resource type
// substrate does not resolve is NoSuchResource/404: the ARN is fine and there is simply no
// record of that kind here. Both are among the four codes the three tagging operations publish
// (AccessDenied 403, InvalidArgument 400, InvalidTagging 400, NoSuchResource 404). Neither
// message is AWS's; the reference publishes codes for these operations and not message text.
//
// An ARN naming a distribution that does not exist is not this function's business — it parses,
// and the absence is reported by the load, which keeps answering NoSuchDistribution/404 for the
// reason [CloudFrontPlugin.resolveTagTarget] records.
func cfParseDistributionARN(arn string) (cfTagTarget, *AWSError) {
	// arn:aws:cloudfront::{account}:{type}/{id}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "cloudfront" {
		return cfTagTarget{}, cfInvalidARN(arn, "not a CloudFront ARN")
	}
	// CloudFront is global and its ARNs carry an empty Region segment, which the published
	// Resource pattern spells as the "::" in arn:aws(-cn)?:cloudfront::[0-9]+:.*. A Region
	// here means the caller built the ARN for a regional service, and resolving it anyway
	// would answer for a distribution the ARN does not name.
	if parts[3] != "" {
		return cfTagTarget{}, cfInvalidARN(arn, "CloudFront ARNs carry no Region")
	}
	account := parts[4]
	if account == "" {
		return cfTagTarget{}, cfInvalidARN(arn, "names no account")
	}

	resType, id, ok := strings.Cut(parts[5], "/")
	if !ok || id == "" {
		return cfTagTarget{}, cfInvalidARN(arn, "names no resource")
	}
	if resType != cfDistributionResourceType {
		return cfTagTarget{}, &AWSError{
			Code:       "NoSuchResource",
			Message:    fmt.Sprintf("CloudFront resource type %q is not taggable here: %s", resType, arn),
			HTTPStatus: http.StatusNotFound,
		}
	}
	// A distribution identifier is a single path segment of uppercase alphanumerics. Anything
	// carrying a further "/" or a ":" names something nested under the distribution or is a
	// longer ARN misread as one, and it would build a state key nothing is stored at — which
	// reports the distribution absent rather than the ARN wrong, the wrong error to hand a
	// caller and the wrong one to see in a log.
	if strings.ContainsAny(id, "/:") {
		return cfTagTarget{}, cfInvalidARN(arn, "names something nested under a distribution")
	}

	return cfTagTarget{AccountID: account, DistID: id}, nil
}

// cfInvalidARN reports that an ARN is not one CloudFront's tagging operations accept, naming the
// reason so a caller can tell a wrong partition from a wrong resource type.
func cfInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArgument",
		Message:    fmt.Sprintf("the Resource ARN %q is not valid: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
