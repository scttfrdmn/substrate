package emulator

// ACM tagging: the ARN resolver the Resource Groups Tagging API keys through, and the state-key
// prefixes it and ACM's own tag operations share.
//
// Split into its own file for the reason stepfunctions_tags.go and cloudfront_tags.go were: the
// resolver is the load-bearing part of reaching one resource rather than a same-named other one,
// and the account and Region it derives have to come from the ARN (#826, #845, #910, #918).
//
// AWS publishes the certificate ARN shape on all three of ACM's tag operations, identically:
//
//	arn:aws:acm:region:123456789012:certificate/12345678-1234-1234-1234-123456789012
//
// with Pattern arn:[\w+=/,.@-]+:acm:[\w+=/,.@-]*:[0-9]+:[\w+=,.@-]+(/[\w+=,.@-]+)* and
// Required: Yes. Note what that pattern does *not* say: the resource tail is any
// slash-separated run of tag characters, so the pattern alone does not restrict an ARN to the
// certificate type. The restriction is published in prose instead, on each of the three pages —
// "This action applies only to the certificate resource type. For all other ACM resource types,
// use TagResource instead." ACM does have other types with their own ARN shapes; the ACME
// endpoint publishes arn:aws[a-z-]*:acm:[a-z0-9-]+:[0-9]{12}:acme-endpoint/[a-zA-Z0-9-]+.
// Substrate models the certificate and no other ACM resource, so [acmResolveARN] resolves
// "certificate" and refuses every other type by name rather than resolving whatever it is
// handed — the anchored-segment rule #910 established.
//
// ACM's own three tag operations key by the caller's account and Region while the ARN carries
// both, which is the shape #918 fixed in CloudFront. It is *not* a defect here, and the
// difference is worth stating because it looks like one. [acmCertKey] embeds the whole ARN in
// the key, so a lookup keyed by the caller's account can only ever find a record whose ARN
// names that same account — ACM mints the ARN from the request context it stored the record
// under, so no key exists where the two disagree. That redundancy also makes the reference's own
// account scoping fall out: ResourceNotFoundException is documented as "The specified
// certificate cannot be found in the caller's account or the caller's account cannot be found",
// so a foreign-account ARN must not resolve through ACM's own operations, and it does not.
//
// The tagging API's arm is the other way round by rule: it takes all three key components from
// the ARN, because #845 requires an ARN naming another account to resolve that account's
// resource or none, and because [TaggingPlugin.resolveARN] receives no request context to reach
// for. Both sides build the key through [acmCertKey], which is the cross-readability criterion
// #765 asks for — a tag written by either is read back by the other.

import (
	"fmt"
	"strings"
)

// ACM state-key prefixes. The namespace holds exactly these two kinds: a certificate record and
// the per-account/Region index of certificate ARNs. The index stores no tags and no ARN
// addresses it, hence [acmKeyIsTaggable] in front of the merge.
//
// Both are colon-terminated, and the guard tests them that way on purpose: "cert" alone is a
// prefix of "cert_arns", so a bare-prefix test would report the index taggable and merge a tags
// member into a JSON array of ARN strings.
const (
	acmCertKeyPrefix     = "cert:"
	acmCertARNsKeyPrefix = "cert_arns:"
)

// acmCertificateResourceType is the one ACM ARN resource type substrate resolves. See this
// file's preamble for the types deliberately refused and the prose that scopes the three tag
// operations to this one.
const acmCertificateResourceType = "certificate"

// acmTagsJSONMember is the JSON member an ACM certificate record stores its tags in.
//
// [ACMCertificate] declares `json:"Tags,omitempty"` (acm_types.go). It is named rather than
// written inline because [mergeRecordStringMapTags] writes whichever member it is given, and a
// misspelling would add a second tags member while leaving the real one untouched — a tag call
// that answers 200 and stores nothing.
const acmTagsJSONMember = "Tags"

// acmResolveARN parses an ACM certificate ARN and returns the namespace and state key it
// addresses.
//
// The account and Region come from the ARN, never from the caller's request context, and the
// function takes no *RequestContext at all so that is structural rather than a thing each call
// site has to remember — the arrangement [sfnResolveARN] and [cfParseDistributionARN] settled
// on. See this file's preamble for why ACM's own operations legitimately key the other way.
//
// The whole ARN goes into the key as [acmCertKey] takes it, so the ARN a caller passes must be
// the one ACM reported. That is the same constraint ACM's own operations are under, and it is
// what #765 asks of a tagging arm: the identifier the owning service hands out is the identifier
// the tagging API accepts.
func acmResolveARN(arn string) (ns, key string, err error) {
	// arn:aws:acm:{region}:{acct}:certificate/{id}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "acm" {
		return "", "", fmt.Errorf("invalid ACM ARN: %q", arn)
	}
	region := parts[3]
	acct := parts[4]
	if region == "" {
		return "", "", fmt.Errorf("ACM ARN names no Region: %q", arn)
	}
	if acct == "" {
		return "", "", fmt.Errorf("ACM ARN names no account: %q", arn)
	}

	resType, id, ok := strings.Cut(parts[5], "/")
	if !ok || id == "" {
		return "", "", fmt.Errorf("ACM ARN names no resource: %q", arn)
	}
	if resType != acmCertificateResourceType {
		return "", "", fmt.Errorf("unsupported ACM ARN resource type: %q", resType)
	}
	// A certificate identifier is a single path segment. Anything carrying a further "/" or a
	// ":" names something nested under the certificate or is a longer ARN misread as one, and it
	// would build a key nothing is stored at — reporting the certificate absent rather than the
	// ARN wrong.
	if strings.ContainsAny(id, "/:") {
		return "", "", fmt.Errorf("ACM ARN names something nested under a certificate: %q", arn)
	}

	return acmNamespace, acmCertKey(acct, region, arn), nil
}

// acmKeyIsTaggable reports whether an ACM state key names a record that stores tags.
//
// Only the certificate record does. The prefix is tested colon-terminated because the index key
// begins with the same four letters; see the const block above.
func acmKeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, acmCertKeyPrefix)
}
