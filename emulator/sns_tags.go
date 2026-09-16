package emulator

// SNS resource resolution: the one place a TopicArn or ResourceArn parameter becomes a state key.
//
// Split out of sns_plugin.go for the reason kms_tags.go was split out of kms_plugin.go and
// cloudfront_tags.go out of cloudfront_plugin.go: the resolver is the load-bearing part, and here it
// was four lines that got three things wrong for every one of the ten operations that called it.
//
// snsNameFromARN split an ARN on ":" and returned the LAST segment, and its callers then keyed the
// load and the store by the caller's own account and Region. Three consequences compounded (#925):
//
//  1. arn:aws:sns:eu-west-1:999988887777:orders presented by a us-east-1 caller in account
//     111122223333 addressed that caller's own topic named "orders". UntagResource is the damaging
//     direction and answered 200 while stripping tags from it; Publish published to the wrong topic
//     and DeleteTopic deleted it. This is the rule #826 established for SQS and DynamoDB, #845
//     carried through the tagging API's resolver, #910 applied to Step Functions, #918 to CloudFront
//     and #922 to KMS: the account and Region come from the ARN, never from the request context.
//
//  2. A subscription ARN is arn:aws:sns:{region}:{account}:{topic}:{sub-id}, so the last segment of
//     one is the subscription's own identifier — returned as a topic name. SNS's topic ARN carries no
//     type keyword and no separator, the resource portion *is* the name, so the anchored rule of
//     #910 here is that a resource portion containing a ":" is not a topic.
//
//  3. The length guard fell through to returning the argument, so any string at all — "arn:aws:sns",
//     a bare name, a URL — became a topic name and was looked up rather than refused.
//
// What is deliberately still keyed by the calling account: the subscription record, the account-wide
// subscription index, and the per-topic subscription index. A cross-account subscription is not
// modeled at all — nothing mints one and no operation distinguishes a subscriber's account from a
// topic's — so moving those three to the topic's account would put the index under an account the
// subscriptions substrate does have are not stored in, and Publish would silently stop delivering.
// The topic *record* is what an ARN addresses, and it is the record tags live on.

import (
	"fmt"
	"net/http"
	"strings"
)

// SNS state-key prefixes. The namespace holds five kinds: a topic record, the per-account index of
// topic names, a subscription record, the account-wide index of subscription ARNs, and the per-topic
// index of them. Only the topic record stores tags, hence [snsKeyIsTaggable] in front of the merge.
//
// Every prefix is tested colon-terminated, because "topic" is a prefix of "topic_names" — the fourth
// namespace to need that after cert/cert_arns and cfdist/cfdist_ids (#835) and key/key_ids (#924). A
// bare-prefix test would report the topic-name index taggable and merge a tags member into a JSON
// array of names.
const (
	snsTopicKeyPrefix       = "topic:"
	snsTopicNamesKeyPrefix  = "topic_names:"
	snsSubKeyPrefix         = "subscription:"
	snsSubAllIDsKeyPrefix   = "sub_all_ids:"
	snsSubTopicIDsKeyPrefix = "sub_ids:"
)

// snsTagsJSONMember is the JSON member an SNS topic record stores its tags in.
//
// [SNSTopic] declares `json:"Tags,omitempty"` (sns_types.go). It is named rather than written inline
// because [mergeRecordTagListTags] writes whichever member it is given, and a misspelling would add a
// second tags member while leaving the real one untouched — a tag call that answers 200 and stores
// nothing.
const snsTagsJSONMember = "Tags"

// SNS spells a tag's two fields Key and Value, as [SNSTag] and AWS's own Tag shape do. Named for the
// same reason as the member above, and passed to [mergeRecordTagListTags], which takes both field
// names as parameters because KMS alone among #835's rows spells them TagKey/TagValue.
const (
	snsTagKeyField   = "Key"
	snsTagValueField = "Value"
)

// snsKeyIsTaggable reports whether an SNS state key names a record that stores tags.
//
// Only the topic record does. A single positive test rather than an enumeration of the four
// refusals, which is the safe direction: a key kind added later is refused by default, and refusing
// is the conservative outcome — merging tags into a record that does not model them writes a member
// nothing reads and reports success.
func snsKeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, snsTopicKeyPrefix)
}

// snsTopicTarget is the topic an SNS ARN names: the account and Region that own it and its name, all
// three taken from the ARN.
type snsTopicTarget struct {
	// AccountID is the account segment of the ARN, never the caller's.
	AccountID string

	// Region is the Region segment of the ARN, never the caller's.
	Region string

	// Name is the topic name, which is the whole resource portion of an SNS topic ARN.
	Name string
}

// snsParseTopicARN parses an SNS topic ARN and returns the account, Region and name it identifies.
//
// It takes no *RequestContext, which is what makes "the account and Region come from the ARN"
// structural rather than a thing each of ten operations has to remember — the arrangement
// [sfnResolveARN] settled on for Step Functions, [cfParseDistributionARN] for CloudFront and
// [kmsParseARN] for KMS.
//
// An SNS topic ARN is arn:{partition}:sns:{region}:{account}:{topic-name}, with no type keyword and
// no separator before the name, so there is no segment to anchor a type match against. The
// discriminator the shape does offer is the colon: a subscription ARN appends ":{sub-id}", so a
// resource portion containing a ":" names a subscription and not a topic. Refusing it is what stops
// a subscription ARN from resolving to its own identifier as a topic name.
//
// A malformed ARN is InvalidParameter/400, which API_TagResource, API_UntagResource,
// API_ListTagsForResource, API_GetTopicAttributes and API_Publish each publish. The message text is
// substrate's; the reference publishes codes and not messages.
func snsParseTopicARN(arn string) (snsTopicTarget, *AWSError) {
	// arn:aws:sns:{region}:{account}:{topic-name}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "sns" {
		return snsTopicTarget{}, snsInvalidARN(arn, "not an SNS ARN")
	}
	region, account, name := parts[3], parts[4], parts[5]
	if region == "" {
		return snsTopicTarget{}, snsInvalidARN(arn, "names no Region")
	}
	if account == "" {
		return snsTopicTarget{}, snsInvalidARN(arn, "names no account")
	}
	if name == "" {
		return snsTopicTarget{}, snsInvalidARN(arn, "names no topic")
	}
	if strings.Contains(name, ":") {
		return snsTopicTarget{}, snsInvalidARN(arn, "names a subscription, not a topic")
	}
	return snsTopicTarget{AccountID: account, Region: region, Name: name}, nil
}

// snsResolveARN parses an SNS topic ARN and returns the namespace and state key it addresses, for
// the Resource Groups Tagging API.
//
// A thin wrapper over [snsParseTopicARN] rather than a second parser, so the tagging API and SNS's
// own three tag operations cannot disagree about which topic an ARN names — the arrangement #826
// established through ecsTagStateKey, #910 through [sfnResolveARN] and #924 through [kmsResolveARN].
// It exists at all because the two callers need different error *shapes*: SNS's own operations answer
// an [AWSError] carrying a published code, while [TaggingPlugin.resolveARN] returns a plain error
// that its caller renders into a FailedResourcesMap entry.
//
// Account isolation at this arm is emergent rather than guarded, as ACM's is: the state key carries
// the ARN's account and Region, so an ARN naming another account builds a key nothing is stored at
// and the merge reports the resource absent. Unlike KMS (#922) there is nothing to refuse
// explicitly — no cross-account statement appears on any of SNS's three tagging pages, so inventing
// a prohibition would be substrate's invention rather than its reading.
func snsResolveARN(arn string) (ns, key string, err error) {
	target, arnErr := snsParseTopicARN(arn)
	if arnErr != nil {
		return "", "", fmt.Errorf("%s: %s", arnErr.Code, arnErr.Message)
	}
	return snsNamespace, snsTopicStateKey(target.AccountID, target.Region, target.Name), nil
}

// The two spellings a query-protocol caller may use for SNS's tag list, and for the key list
// UntagResource takes.
//
// AWS's reference names the members Tags.member.N and TagKeys.member.N, but AWS's own request
// examples on the same pages wire Tags.Tag.1.Key, Tags.Tag.1.Value and TagKeys.TagKey.1. Substrate
// decoded only the first form, so a caller sending the form AWS's own example shows got 200 with
// nothing written or nothing removed — the "tag parameter decoded by nobody" class of #925. Both are
// accepted rather than one being chosen, because the reference and its example disagree and a caller
// may reasonably have followed either.
var (
	snsTagParamForms    = []string{"Tags.member.%d", "Tags.Tag.%d"}
	snsTagKeyParamForms = []string{"TagKeys.member.%d", "TagKeys.TagKey.%d"}
)

// snsTagParams decodes a tag list from a query-protocol request, in both accepted spellings.
//
// A tag with an empty Key ends the list, following the convention every indexed query-protocol
// decoder in the tree uses: the numbering is 1-based and dense, so the first absent index is the end.
// A duplicate key across the two spellings is left in the list; the caller merges by key, so the
// later entry wins exactly as a repeated key within one spelling does.
func snsTagParams(params map[string]string) []SNSTag {
	var out []SNSTag
	for _, form := range snsTagParamForms {
		for i := 1; ; i++ {
			key := params[fmt.Sprintf(form+".Key", i)]
			if key == "" {
				break
			}
			out = append(out, SNSTag{Key: key, Value: params[fmt.Sprintf(form+".Value", i)]})
		}
	}
	return out
}

// snsTagKeyParams decodes a tag-key list from a query-protocol request, in both accepted spellings.
func snsTagKeyParams(params map[string]string) []string {
	var out []string
	for _, form := range snsTagKeyParamForms {
		for i := 1; ; i++ {
			key := params[fmt.Sprintf(form, i)]
			if key == "" {
				break
			}
			out = append(out, key)
		}
	}
	return out
}

// snsInvalidARN reports that a string is not an ARN SNS accepts, naming the reason so a caller can
// tell a wrong service from a wrong resource type.
func snsInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameter",
		Message:    fmt.Sprintf("the ARN %q is not valid: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// snsTopicNotFound reports that a well-formed ARN names no topic, for SNS's three tag operations.
//
// The code is ResourceNotFound, which API_TagResource, API_UntagResource and API_ListTagsForResource
// each publish at HTTP 404. It differs from the plain NotFound the topic operations publish and that
// substrate answered here before #925 — the status was right and the code name was not, and a caller
// matching on the code saw something no SDK models.
func snsTopicNotFound(arn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFound",
		Message:    fmt.Sprintf("no topic found for the ARN %q", arn),
		HTTPStatus: http.StatusNotFound,
	}
}
