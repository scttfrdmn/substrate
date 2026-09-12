package emulator

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"
)

// IAM tag validation.
//
// The twelve IAM tagging paths accepted anything at all before #806: more than fifty tags, a
// key beginning with the reserved `aws:` prefix, an empty key, a character outside AWS's set.
// This file is the one place those rules live, and every one of them is quoted from an
// authoritative AWS page rather than inferred.
//
// The structural model is EC2's — `ec2CheckTagLimit` counting a **post-merge** key set,
// `ec2CheckTagLengths` counting **runes** rather than bytes — but not a line of it is shared.
// IAM's codes differ (`InvalidInput` where EC2 raises `InvalidParameterValue`, `LimitExceeded`
// where EC2 raises `TagLimitExceeded`), IAM's character set is a published regex where EC2's is
// prose, and IAM does **not** exempt reserved keys from its count the way EC2 does — EC2's
// restrictions list says "Tags with the aws: prefix do not count against your tags per resource
// limit" and no IAM page says anything of the kind. Nothing in substrate stamps an IAM entity,
// and a caller cannot set a reserved key at all, so an IAM entity holds no reserved tags for the
// question to arise.
//
// **Which code answers which rule is substrate's mapping, and is worth stating because AWS
// publishes no per-rule code.** The split follows what each operation's Errors list makes
// available:
//
//   - A constraint stated on the shape — the two patterns, the length limits, the fifty-member
//     array cap — answers `ValidationError`/400, the code the IAM plugin already returns for
//     every other shape violation ("UserName is required"). It is also the only code available
//     on the untag operations, whose Errors lists carry `ConcurrentModification`, `NoSuchEntity`
//     and `ServiceFailure` and **not** `InvalidInput`.
//   - A rule stated only in prose and inexpressible in the shape — the reserved `aws:` prefix —
//     answers `InvalidInput`/400, which every tag-writing and create operation documents.
//   - Exceeding the fifty-tag total answers `LimitExceeded`/409, documented as "The request was
//     rejected because it attempted to create resources beyond the current AWS account limits."
//
// The *messages* are substrate's own wording throughout; no captured real-AWS IAM tag rejection
// was available, and none of the pages publishes message text. SDKs dispatch on the code, so the
// code is the part a consumer's error branch turns on.
//
// `ConcurrentModification`/409 and `ServiceFailure`/500 are declared on all of these operations
// and are **unreachable in substrate by construction** (#806's fourth criterion). A tagging
// handler's read-modify-write runs synchronously inside one request against a [StateManager]
// that serializes its own access, so no second request can interleave and produce the
// simultaneous-change condition `ConcurrentModification` reports. And a state failure is
// returned as a Go error from the plugin — the server answers it as an internal error rather
// than as an IAM-shaped `ServiceFailure` body — so no code path constructs one.

// iamTagCase says how one IAM entity type compares tag keys.
//
// AWS splits the entity types, from the User Guide's *Tagging IAM resources*:
//
//	Case sensitivity for tag keys differs depending on the type of IAM resource that is
//	tagged. Tag key values for IAM users and roles are not case sensitive, but case is
//	preserved. This means that you cannot have separate Department and department tag keys.
//	[…] For other IAM resource types, tag key values are case sensitive.
//
// The same page lists the two sides explicitly: not case sensitive for IAM roles and IAM users;
// case sensitive for customer managed policies, instance profiles, and the identity-provider,
// server-certificate and virtual-MFA types substrate does not tag. So this is a per-entity-type
// constant rather than a global one, and it has to reach both [iamMergeTagSet] and
// [iamRemoveTagKeys] — an untag of DEPARTMENT must remove a user's Department, and must not
// remove a policy's.
type iamTagCase bool

// The two tag-key comparison rules, one per side of AWS's split.
const (
	// iamTagKeysCaseSensitive is the rule for customer managed policies and instance profiles:
	// Costcenter and costcenter are two tags.
	iamTagKeysCaseSensitive iamTagCase = true

	// iamTagKeysCaseInsensitive is the rule for users and roles: Department and department are
	// one tag, and the stored spelling is the one preserved.
	iamTagKeysCaseInsensitive iamTagCase = false
)

// canonical returns the form of key this rule compares by. Case folding is [strings.ToLower],
// which is Unicode-aware — the character set is `\p{L}`-based, so a key can be cased in a script
// other than Latin.
func (c iamTagCase) canonical(key string) string {
	if c == iamTagKeysCaseSensitive {
		return key
	}
	return strings.ToLower(key)
}

// The IAM tag limits, all four from the API model rather than from prose.
//
// The count is the `Tags.member.N` / `TagKeys.member.N` array cap every tagging and create
// operation declares — "Array Members: Maximum number of 50 items" — which coincides with the
// per-resource total the quotas page states for session tags ("You can pass up to 50 session
// tags") and which `LimitExceeded` reports on a resource already holding tags.
//
// The lengths are the `Tag` type's Length Constraints: key "Minimum length of 1. Maximum length
// of 128", value "Minimum length of 0. Maximum length of 256". The unit is characters, not
// bytes: the quotas page calls 128 the "Tag key" limit alongside policy limits measured in
// characters, and the pattern's character classes are Unicode, so a 128-rune key of non-ASCII
// letters is legal and a byte-counting check would refuse it. Hence [utf8.RuneCountInString].
//
// The value's minimum of 0 is load-bearing rather than trivia — "You can create a tag with an
// empty value such as phoneNumber = . You cannot create an empty tag key." — so an empty value
// is accepted and an empty key is not.
const (
	iamMaxTagsPerResource = 50
	iamMinTagKeyLength    = 1
	iamMaxTagKeyLength    = 128
	iamMaxTagValueLength  = 256
)

// iamReservedTagPrefix is the tag prefix AWS reserves, from *Tagging IAM resources*: "You
// cannot create a tag key or value that begins with the text aws:. This tag prefix is reserved
// for AWS internal use."
//
// Two details that a looser reading would get wrong. The rule covers the **value** as well as
// the key — IAM states both where EC2's equivalent restriction names keys only. And the match is
// **case-sensitive**, so `AWS:billing` is an ordinary caller tag: the prohibition names "the
// text aws:", the reserved keys the same page lists are all lowercase
// (`aws:cloudformation:stack-name`, `aws:ec2spot:fleet-request-id`), and case-folding it would
// refuse a key real IAM accepts. That is the same reading, and the same reasoning, as
// [ec2ReservedTagPrefix].
const iamReservedTagPrefix = "aws:"

// AWS's two `Tag` patterns, compiled verbatim from the `Tag` data type:
//
//	Key   — Pattern: [\p{L}\p{Z}\p{N}_.:/=+\-@]+
//	Value — Pattern: [\p{L}\p{Z}\p{N}_.:/=+\-@]*
//
// Compiling the published pattern rather than hand-rolling a whitelist is what "only what the
// API model states" requires (#671), and it is not merely tidier: the classes are **Unicode**,
// so `Abteilung=Zürich` and a key containing a non-breaking space are legal, where an ASCII
// whitelist derived from the User Guide's prose rendering ("letters, numbers, spaces, and
// _ . : / = + - @") would refuse them. Go's regexp supports `\p{L}`, `\p{Z}` and `\p{N}`
// directly, so no translation is needed.
//
// The anchors are substrate's: AWS publishes the pattern unanchored, and the SDKs and the
// service validate it against the whole member. An unanchored match would accept any string
// containing one legal character, which is plainly not the intent — a key of "a\tb" would pass.
//
// The key pattern's `+` quantifier is why an empty key needs no separate rule from the model's
// point of view; [iamValidateTagSet] still checks emptiness first, so the message names the
// actual problem instead of reporting a character-set failure.
var (
	iamTagKeyPattern   = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]+$`)
	iamTagValuePattern = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)
)

// iamTagShapeError returns the ValidationError a violated shape constraint answers.
func iamTagShapeError(message string) *AWSResponse {
	return iamErrorResponse("ValidationError", message, http.StatusBadRequest)
}

// iamReservedTagError returns the InvalidInput a reserved key or value answers. what names the
// part that carried the prefix ("key" or "value").
func iamReservedTagError(what, value string) *AWSResponse {
	return iamErrorResponse("InvalidInput", fmt.Sprintf(
		"Tag %s %q begins with the reserved prefix %q, which is reserved for AWS internal use.",
		what, value, iamReservedTagPrefix), http.StatusBadRequest)
}

// iamTagLimitError returns the LimitExceeded an over-limit tag set answers.
//
// The status is 409, as the operations document it — which differs from EC2's 400 for the
// equivalent refusal, and is the reason IAM cannot reuse EC2's constructor.
func iamTagLimitError(count int) *AWSResponse {
	return iamErrorResponse("LimitExceeded", fmt.Sprintf(
		"Cannot exceed %d tags on an IAM resource; the request would leave %d.",
		iamMaxTagsPerResource, count), http.StatusConflict)
}

// iamValidateTagSet returns the response to answer with when any tag in tags is illegal, or nil
// when every one of them is legal.
//
// Every tag-writing path calls this, including the four creates — `CreateUser`'s `Tags.member.N`
// is explicit that a bad tag is fatal to the whole request: "If any one of the tags is invalid
// or if you exceed the allowed maximum number of tags, then the entire request fails and the
// resource is not created." So a create must call this **before** it writes the entity, not
// after.
//
// Tags are checked in slice order and each tag's parts in a fixed order, so a request with more
// than one problem is refused on the same one on every run — the determinism [ec2CheckTagLengths]
// avoids a map for.
//
// What this does **not** enforce, deliberately: the `Tag` type marks both `Key` and `Value`
// `Required: Yes`, but an absent `Tags.member.N.Value` arrives here as the empty string, which
// the value pattern and the minimum length of 0 both permit. Substrate cannot distinguish an
// omitted value from an empty one on the query wire, and the empty value is documented as legal,
// so it accepts it rather than guessing.
func iamValidateTagSet(tags []IAMTag) *AWSResponse {
	if len(tags) > iamMaxTagsPerResource {
		return iamTagShapeError(fmt.Sprintf(
			"Tags can contain at most %d members; %d were supplied.",
			iamMaxTagsPerResource, len(tags)))
	}
	for _, t := range tags {
		if resp := iamValidateTagKey(t.Key); resp != nil {
			return resp
		}
		if n := utf8.RuneCountInString(t.Value); n > iamMaxTagValueLength {
			return iamTagShapeError(fmt.Sprintf(
				"Tag value for key %q must be no more than %d characters; the supplied value is %d.",
				t.Key, iamMaxTagValueLength, n))
		}
		if !iamTagValuePattern.MatchString(t.Value) {
			return iamTagShapeError(fmt.Sprintf(
				"Tag value for key %q contains a character that is not permitted.", t.Key))
		}
		if strings.HasPrefix(t.Value, iamReservedTagPrefix) {
			return iamReservedTagError("value", t.Value)
		}
	}
	return nil
}

// iamValidateTagKey returns the response to answer with when key is not a legal tag key, or nil.
//
// Shared by [iamValidateTagSet] and [iamValidateTagKeys] so a key is judged identically whether
// it arrives to be written or to be removed.
func iamValidateTagKey(key string) *AWSResponse {
	if key == "" {
		return iamTagShapeError("A tag key cannot be empty.")
	}
	if n := utf8.RuneCountInString(key); n < iamMinTagKeyLength || n > iamMaxTagKeyLength {
		return iamTagShapeError(fmt.Sprintf(
			"Tag key must be between %d and %d characters; the supplied key is %d.",
			iamMinTagKeyLength, iamMaxTagKeyLength, n))
	}
	if !iamTagKeyPattern.MatchString(key) {
		return iamTagShapeError(fmt.Sprintf(
			"Tag key %q contains a character that is not permitted.", key))
	}
	if strings.HasPrefix(key, iamReservedTagPrefix) {
		return iamReservedTagError("key", key)
	}
	return nil
}

// iamValidateTagKeys returns the response to answer with when any key in keys is illegal, or nil.
//
// The untag operations' `TagKeys.member.N` carries the same constraints as a tag key — "Array
// Members: Maximum number of 50 items", "Minimum length of 1. Maximum length of 128", and the
// key pattern — so the same checks apply.
//
// Including the reserved-prefix check, which is a choice worth naming: the prohibition is on
// *creating* such a tag, and no IAM entity in substrate can hold one, so refusing to remove one
// costs a caller nothing and keeps `aws:`-prefixed keys uniformly inexpressible through the IAM
// tagging surface. A caller who names one is asking to remove a tag that cannot exist.
func iamValidateTagKeys(keys []string) *AWSResponse {
	if len(keys) > iamMaxTagsPerResource {
		return iamTagShapeError(fmt.Sprintf(
			"TagKeys can contain at most %d members; %d were supplied.",
			iamMaxTagsPerResource, len(keys)))
	}
	for _, k := range keys {
		if resp := iamValidateTagKey(k); resp != nil {
			return resp
		}
	}
	return nil
}

// iamCreateTagSet validates a create operation's tag-on-create list and returns the set to store,
// or the response to answer with when any tag is illegal.
//
// The four creates share this rather than calling [iamValidateTagSet] directly, because a create
// has to do one thing the tag-writing paths get from their merge: collapse a duplicate key
// inside the request itself, under the entity type's case rule, so a user created with both
// `Department` and `department` holds the one tag the entity invariant allows rather than two
// that no later `TagUser` could ever produce. Merging against an empty set also sorts, which is
// the order the listing operations document.
//
// [iamCheckTagLimit] is deliberately not called here and is not needed: [iamValidateTagSet]
// already refuses more than [iamMaxTagsPerResource] members, a create starts from no tags, and a
// merge cannot grow a set — so the cap AWS states for a create ("if you exceed the allowed
// maximum number of tags, then the entire request fails and the resource is not created") is
// enforced by the array check alone.
//
// Empty in, nil out: an entity created without tags stores no `Tags` at all rather than an empty
// list, which is what keeps [iamEntityTagsXML] omitting the member.
func iamCreateTagSet(tags []IAMTag, caseRule iamTagCase) ([]IAMTag, *AWSResponse) {
	if resp := iamValidateTagSet(tags); resp != nil {
		return nil, resp
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return iamMergeTagSet(nil, tags, caseRule), nil
}

// iamCheckTagLimit returns the LimitExceeded response when merged holds more than
// [iamMaxTagsPerResource] tags, or nil.
//
// The argument is the **post-merge** set — the output of [iamMergeTagSet], not the request's
// tags — which is what gets the documented behavior right in one comparison, the same way
// [ec2CheckTagLimit] does: overwriting a key an entity already carries adds nothing, so a
// request that rewrites a value on an entity already holding fifty tags succeeds, while a
// request adding a fifty-first key is refused. Counting the request against the stored total
// would refuse both.
//
// Every reachable caller has already merged by the time it calls this, so the count is also
// case-correct for free: two keys that collide under the entity type's [iamTagCase] have
// already collapsed into one.
func iamCheckTagLimit(merged []IAMTag) *AWSResponse {
	if len(merged) > iamMaxTagsPerResource {
		return iamTagLimitError(len(merged))
	}
	return nil
}
