package emulator

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// The limits on a CloudFormation stack tag, from the `Tag` data type's own
// constraints:
//
//	Key    Length Constraints: Minimum length of 1. Maximum length of 128.
//	Value  Length Constraints: Minimum length of 1. Maximum length of 256.
//
// Both are `Required: Yes`, and the value's minimum is **1** — unlike IAM's and
// ELB's tag values, which document a minimum of 0 and so accept an empty string.
// A caller sending `Key=team` with no value is therefore refused here and accepted
// by those two services, which is AWS's own inconsistency rather than substrate's.
//
// `CreateStack`'s own prose supplies the count: "A maximum number of 50 tags can be
// specified." The Template Reference's resource-tag page agrees with the model on both
// lengths ("1 to 128 Unicode characters", "1 to 256 characters"); the *console* page
// documents 127, 255 and "up to 50 tag key pairs", so it disagrees on the two lengths by
// one character each. The model wins, on the reasoning #671 settled — only what the API
// model states — and `docs/services.md` records the disagreement so a consumer reading
// the console page is not surprised by the extra character.
//
// The model publishes no `Pattern` for either member, unlike IAM's `Tag` and AWS Config's
// `TagsList`, so no character set is enforced here — inventing one would refuse requests
// AWS accepts. The Template Reference page does list a character set, but for a
// *template's* resource-level `Tags` property rather than for `CreateStack`'s stack-level
// list, and the model for the operation being emulated is the citation that governs.
const (
	cfnMaxStackTags      = 50
	cfnMaxTagKeyLength   = 128
	cfnMaxTagValueLength = 256
)

// cfnReservedTagPrefix is the prefix CloudFormation reserves for its own tags.
//
// The `Tag` type's Key description says it outright — "Tags owned by AWS have the
// reserved prefix: `aws:`" — the Template Reference's resource-tag page names the three
// keys CloudFormation itself creates with it, and its Key description repeats that a key
// "can't be prefixed with `aws:`".
//
// Matched **case-insensitively**, unlike EC2's, ELB's and IAM's checks, because that same
// page states the rule outright: "The `aws:` prefix is reserved for AWS use. This prefix
// is case-insensitive." So `AWS:owner` is refused here and accepted by those three
// services. That is AWS's own divergence, and [cfgsvcCheckTag] already lowercases for a
// service whose guide says the same thing.
//
// The prefix is refused in a key only. The Template Reference says of a value carrying it
// merely that "you can't update or delete the tag", which is a consequence rather than a
// refusal, and the `Tag` type's Value description prohibits nothing — so a value beginning
// `aws:` is stored, and IAM's stricter treatment of the same string is not copied here.
const cfnReservedTagPrefix = "aws:"

// cfnStackTags decodes an indexed `Tags.member.N` list of {Key, Value} into the map
// [CFNStackState] stores, distinguishing an omitted parameter from an empty list.
//
// The distinction is the whole reason this returns a map rather than a slice, because
// `UpdateStack` turns on it: "If you don't specify this parameter, CloudFormation
// doesn't modify the stack's tags. If you specify an empty value, CloudFormation
// removes all associated tags." So an absent list is nil and an explicitly empty one is
// an empty non-nil map, and the caller can tell "leave them alone" from "remove them
// all".
//
// An empty list is recognized from a bare `Tags` parameter, which is how the query
// protocol puts one on the wire: botocore's query serializer writes `Tags=` for an
// empty list ("The query protocol serializes empty lists", `serialize.py`), and there
// is no `Tags.member.1` to walk. A hand-built request that omits the parameter
// entirely reads as omitted, which is what it is.
//
// Unlike [extractELBTags] the walk ends only on an **absent** index, not on an empty
// key: a key of "" is below the type's minimum length and has to reach the validator to
// be refused, where terminating on it would silently drop the rest of the caller's list
// and accept the request. This is the same correction #806 made to `iamMemberTags`.
//
// Two members naming the same key collapse, last one winning. AWS publishes no error
// code for a duplicate key and the map is what the stack's tags *are*, so there is
// nothing a second entry could mean other than an overwrite.
func cfnStackTags(params map[string]string) map[string]string {
	out := make(map[string]string)
	for i := 1; ; i++ {
		key, ok := params[fmt.Sprintf("Tags.member.%d.Key", i)]
		if !ok {
			break
		}
		out[key] = params[fmt.Sprintf("Tags.member.%d.Value", i)]
	}
	if len(out) == 0 {
		if _, present := params["Tags"]; !present {
			return nil
		}
	}
	return out
}

// cfnValidateStackTags checks a decoded stack-tag set against the limits AWS documents,
// returning an [ErrCFNInvalidTag] error naming the first rule broken.
//
// Keys are checked in sorted order so the message a caller sees for a request breaking
// several rules is the same on every run — a validation error that names a different tag
// each time is one a test cannot assert against.
//
// Lengths count runes rather than bytes, as [ec2CheckTagLengths] does: the constraint is
// documented in characters, and counting bytes would refuse a legal tag whose characters
// are multi-byte.
func cfnValidateStackTags(tags map[string]string) error {
	if len(tags) > cfnMaxStackTags {
		return cfnErrf(ErrCFNInvalidTag,
			"Tags may contain at most %d entries; the request supplies %d",
			cfnMaxStackTags, len(tags))
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if n := utf8.RuneCountInString(key); n < 1 || n > cfnMaxTagKeyLength {
			return cfnErrf(ErrCFNInvalidTag,
				"Tag key must be between 1 and %d characters; the supplied key is %d",
				cfnMaxTagKeyLength, n)
		}
		if strings.HasPrefix(strings.ToLower(key), cfnReservedTagPrefix) {
			return cfnErrf(ErrCFNInvalidTag,
				"Tag key %q uses the reserved prefix %q", key, cfnReservedTagPrefix)
		}
		if n := utf8.RuneCountInString(tags[key]); n < 1 || n > cfnMaxTagValueLength {
			return cfnErrf(ErrCFNInvalidTag,
				"Tag value for key %q must be between 1 and %d characters; the supplied value is %d",
				key, cfnMaxTagValueLength, n)
		}
	}
	return nil
}

// cfnStackTagsXML renders a stack's tags for DescribeStacks, sorted by key so a
// response is byte-identical across calls — the property [cfnParametersXML] exists for.
//
// A stack with no tags renders an empty `<Tags></Tags>`, which is what its two siblings
// on the same shape already do: `encoding/xml` writes the parent element of a nested
// path (`Tags>member`) whether or not the slice has members, so Parameters and Outputs
// come back empty rather than absent too. Nothing observable rides on the difference —
// every SDK decodes an absent list and an empty one to the same empty slice — and
// matching the neighbors beats a pointer indirection whose only effect is on bytes no
// consumer reads differently.
func cfnStackTagsXML(tags map[string]string) []cfnTagXML {
	if len(tags) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]cfnTagXML, 0, len(keys))
	for _, k := range keys {
		out = append(out, cfnTagXML{Key: k, Value: tags[k]})
	}
	return out
}

// cfnTagXML is one stack tag as DescribeStacks reports it.
type cfnTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}
