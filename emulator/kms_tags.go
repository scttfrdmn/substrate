package emulator

// KMS resource resolution: the one place a KeyId parameter or a key ARN becomes a state key.
//
// Split out of kms_plugin.go for the reason cloudfront_tags.go was split out of
// cloudfront_plugin.go: the resolver is the load-bearing part, and here it was four lines that got
// two things wrong for every one of the eighteen operations that call it.
//
// The old resolver took the last "/"-delimited component of an ARN and handed it back as a key ID,
// and its callers then keyed the load and the store by the **caller's own** account and Region. So
// arn:aws:kms:eu-west-1:999988887777:key/{id} addressed the caller's own {id} in the caller's own
// Region, and UntagResource — the damaging direction — stripped tags from it while answering 200.
// That is the rule #826 established for SQS and DynamoDB, #845 carried through the tagging API's
// resolver, #910 applied to Step Functions and #918 to CloudFront: the account comes from the ARN,
// never from the request context, and a resolver that cannot reach for the context does not have to
// remember not to.
//
// The type match was unanchored in the same way. An alias ARN's resource portion is
// "alias/{name}", so taking the last "/" component turned arn:aws:kms:…:alias/prod into the key ID
// "prod" — a different resource type addressing a record it does not name, the mistake #910 found
// in strings.Contains(arn, ":stateMachine:"). The type is now the resource portion's first
// "/"-delimited segment and is compared whole.
//
// A bare identifier is still accepted, because AWS's KeyId parameter documents four forms and two
// of them are not ARNs. An "alias/" prefix is still resolved through the alias pointer. What
// changed is that an ARN is parsed as an ARN rather than string-scanned, and that its account and
// Region are the ones used.

import (
	"fmt"
	"net/http"
	"strings"
)

// KMS state-key prefixes. The namespace holds five kinds: a key record, the per-account index of
// key IDs, an alias-to-key-ID pointer, the index of alias names, and a key policy document. Only
// the key record stores tags, hence [kmsKeyIsTaggable] in front of the merge.
//
// Every prefix is tested colon-terminated, because "key" is a prefix of both "key_ids" and
// "key_policy", and "alias" of "alias_names". A bare-prefix test would report the key-ID index
// taggable and merge a tags member into a JSON array of identifier strings.
const (
	kmsKeyKeyPrefix        = "key:"
	kmsKeyIDsKeyPrefix     = "key_ids:"
	kmsAliasKeyPrefix      = "alias:"
	kmsAliasNamesKeyPrefix = "alias_names:"
	kmsKeyPolicyKeyPrefix  = "key_policy:"
)

// The two resource types a KMS ARN can name. Both are resolvable through a KeyId parameter — AWS
// documents four accepted forms, of which two are ARNs — but only a key ARN addresses a record
// directly; an alias ARN addresses the pointer that names one.
const (
	kmsKeyResourceType   = "key"
	kmsAliasResourceType = "alias"
)

// kmsTagsJSONMember is the JSON member a KMS key record stores its tags in.
//
// [KMSKey] declares `json:"Tags,omitempty"` (kms_types.go). It is named rather than written inline
// because [mergeRecordTagListTags] writes whichever member it is given, and a misspelling would add
// a second tags member while leaving the real one untouched — a tag call that answers 200 and
// stores nothing.
const kmsTagsJSONMember = "Tags"

// KMS spells a tag's two fields TagKey and TagValue, not Key and Value. [KMSTag] follows the
// service model, and AWS's own reference publishes the Tag shape's members as "TagKey" and
// "TagValue" for KMS alone among the four services #835's remaining rows cover — which is why
// [mergeRecordTagListTags] takes both field names as parameters.
const (
	kmsTagKeyField   = "TagKey"
	kmsTagValueField = "TagValue"
)

// kmsKeyIsTaggable reports whether a KMS state key names a record that stores tags.
//
// Only the key record does. A single positive test rather than an enumeration of the four
// refusals, which is the safe direction: a key kind added later is refused by default, and refusing
// is the conservative outcome — merging tags into a record that does not model them writes a member
// nothing reads and reports success.
func kmsKeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, kmsKeyKeyPrefix)
}

// kmsTagTarget is the key a KMS ARN names: the account and Region that own it and its identifier,
// all three taken from the ARN.
type kmsTagTarget struct {
	// AccountID is the account segment of the ARN, never the caller's.
	AccountID string

	// Region is the Region segment of the ARN, never the caller's.
	Region string

	// KeyID is the key identifier, e.g. 1234abcd-12ab-34cd-56ef-1234567890ab.
	KeyID string
}

// kmsParseARN parses any KMS ARN and returns the account and Region it names, its resource type,
// and the identifier within that type.
//
// It takes no *RequestContext, which is what makes "the account and Region come from the ARN"
// structural rather than a thing each of eighteen operations has to remember — the arrangement
// [sfnResolveARN] settled on for Step Functions and [cfParseDistributionARN] for CloudFront. The
// resource type is returned rather than checked here, because the two callers accept different
// sets: a KeyId parameter accepts a key ARN and an alias ARN, while the Resource Groups Tagging
// API names a key.
//
// A malformed ARN — not an ARN, not KMS, naming no Region, account or resource — is
// InvalidArnException/400, which API_TagResource, API_UntagResource and API_ListResourceTags each
// publish at HTTP 400 and gloss as "the request was rejected because a specified ARN, or an ARN in a
// key policy, is not valid". The message text is substrate's; the reference publishes codes and not
// messages.
func kmsParseARN(arn string) (target kmsTagTarget, resType string, err *AWSError) {
	// arn:aws:kms:{region}:{account}:{type}/{id}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "kms" {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "not a KMS ARN")
	}
	region, account := parts[3], parts[4]
	if region == "" {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "names no Region")
	}
	if account == "" {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "names no account")
	}

	resType, id, ok := strings.Cut(parts[5], "/")
	if !ok || id == "" {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "names no resource")
	}
	// The cut is on the FIRST "/" and the remainder is not required to be a single segment,
	// because an alias name may contain one: AWS's own example of an AWS managed key's alias is
	// alias/aws/s3, whose ARN is arn:aws:kms:{region}:{account}:alias/aws/s3. A parser that
	// refused a slashed identifier outright, or that took the last component, would resolve that
	// ARN to "s3".
	//
	// A key ID is a UUID, or "mrk-" followed by one with the hyphens removed for a multi-Region
	// key, and carries no "/" in either form — so for a key the extra segment is refused, which is
	// the case the old last-component scan silently accepted.
	if resType == kmsKeyResourceType && strings.Contains(id, "/") {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "names something nested under a key")
	}
	// A ":" in the identifier is a longer ARN misread as this one. It would build a state key
	// nothing is stored at, which reports the resource absent rather than the ARN wrong — the
	// wrong error to hand a caller and the wrong one to see in a log.
	if strings.Contains(id, ":") {
		return kmsTagTarget{}, "", kmsInvalidARN(arn, "identifier contains a colon")
	}

	return kmsTagTarget{AccountID: account, Region: region, KeyID: id}, resType, nil
}

// kmsParseKeyARN parses a KMS ARN that must name a key, for the Resource Groups Tagging API and for
// a KeyId that is a key ARN.
//
// An alias ARN is refused here rather than followed, because the two callers that need an alias
// followed do it themselves through the alias pointer: resolving it inside the key parser would let
// two different ARNs address one record with no way for a caller to see which. The refusal is
// NotFoundException rather than InvalidArnException — the ARN is well formed and there is simply no
// key of that name.
//
// The status is 400. All three KMS tagging operations publish NotFoundException at HTTP 400, not
// 404. Fifteen pre-existing KMS sites answer it at 404 and are left alone here, filed as #923 so one
// diff does not carry both a resolution fix and a status change — the split #921 took for ACM.
func kmsParseKeyARN(arn string) (kmsTagTarget, *AWSError) {
	target, resType, err := kmsParseARN(arn)
	if err != nil {
		return kmsTagTarget{}, err
	}
	if resType != kmsKeyResourceType {
		detail := fmt.Sprintf("KMS resource type %q is not a key: %s", resType, arn)
		if resType == kmsAliasResourceType {
			detail = fmt.Sprintf("an alias ARN does not name a key: %s", arn)
		}
		return kmsTagTarget{}, &AWSError{
			Code:       "NotFoundException",
			Message:    detail,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return target, nil
}

// kmsResolveARN parses a KMS key ARN and returns the namespace and state key it addresses, for the
// Resource Groups Tagging API.
//
// A thin wrapper over [kmsParseKeyARN] rather than a second parser, so the tagging API and KMS's
// own operations cannot disagree about which key an ARN names — the arrangement #826 established
// through ecsTagStateKey and #910 through [sfnResolveARN]. It exists at all because the two callers
// need different error *shapes*: KMS's own operations answer an [AWSError] carrying a published
// code, while [TaggingPlugin.resolveARN] returns a plain error that its caller renders into a
// FailedResourcesMap entry.
func kmsResolveARN(arn string) (ns, key string, err error) {
	target, arnErr := kmsParseKeyARN(arn)
	if arnErr != nil {
		return "", "", fmt.Errorf("%s: %s", arnErr.Code, arnErr.Message)
	}
	return kmsNamespace, kmsKeyStateKey(target.AccountID, target.Region, target.KeyID), nil
}

// kmsRequireLocal refuses a key that the ARN places in another account or another Region, for the
// operations AWS publishes as single-account.
//
// All three KMS tagging operations state "Cross-account use: No. You cannot perform this operation on
// a KMS key in a different AWS account", and the developer guide's tagging page repeats it: "You
// cannot tag ... KMS keys in other AWS accounts". This check exists **because** [kmsParseARN] fixed
// the resolution: while the account came from the request context, a foreign-account key ARN reached
// the caller's own same-named key and the cross-account case could not arise. Now that the ARN's
// account is honored, the operation would succeed against the real foreign key — which is the one
// thing AWS says it must not do. So the two changes belong in one commit, not two.
//
// A Region mismatch is refused on the same footing. A KMS key is Region-scoped and its ARN names the
// Region, so a key ARN from another Region names a key this endpoint does not serve.
//
// The code is NotFoundException at HTTP 400, which all three operations publish: from the caller's
// side a key it may not address is indistinguishable from one that is not there, and that
// indistinguishability is the point of the refusal. AWS publishes the prohibition and the code but
// does not join them, so the mapping is substrate's reading.
func kmsRequireLocal(reqCtx *RequestContext, target kmsTagTarget, operation string) *AWSError {
	if target.AccountID == reqCtx.AccountID && target.Region == reqCtx.Region {
		return nil
	}
	return &AWSError{
		Code: "NotFoundException",
		Message: fmt.Sprintf("%s cannot reach a KMS key in %s/%s from %s/%s", operation,
			target.AccountID, target.Region, reqCtx.AccountID, reqCtx.Region),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidARN reports that an ARN is not one KMS accepts, naming the reason so a caller can tell
// a wrong service from a wrong resource type.
func kmsInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArnException",
		Message:    fmt.Sprintf("the ARN %q is not valid: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
