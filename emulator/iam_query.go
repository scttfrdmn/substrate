package emulator

import (
	"strconv"
	"strings"
)

// IAM speaks the AWS query protocol, which has no JSON: a list is sent as
// numbered form parameters and a struct member as a dotted suffix on one. This
// file decodes that encoding, mirroring iam_xml.go, which encodes the responses.
//
// It exists because nothing decoded it. ParseAWSRequest flattens a form body into
// AWSRequest.Params, a map[string]string (parser.go), and server.go rebuilds
// req.Body for the query-protocol services as json.Marshal(req.Params). A client
// sending
//
//	ActionNames.member.1=s3:GetObject&ActionNames.member.2=s3:PutObject
//
// therefore produces a JSON body whose keys are *literally* "ActionNames.member.1"
// and "ActionNames.member.2", so a handler field declared `ActionNames []string`
// matched nothing and unmarshaled to nil. TagUser, UntagUser, TagRole and UntagRole
// shipped that way: 200 OK, nothing stored (#639).
//
// The unit suite could not see it, because iam_plugin_test.go's iamRequest
// hand-marshals a body containing real JSON arrays and never traverses the
// query → params → JSON path a real client takes. Only an SDK-level journey does
// (test/e2e/journey_iam_tags_test.go).
//
// The precedent for the walk itself is parseMemberList (cloudwatch_plugin.go); IAM
// gets its own so an IAM-shaped refusal never depends on CloudWatch's file, and so
// the empty-value rule below can differ.

// iamMemberSep is the infix the query protocol puts between a list parameter's
// name and its 1-based index.
const iamMemberSep = ".member."

// iamMemberList decodes a flat query-protocol list: prefix.member.1,
// prefix.member.2, and so on. It returns nil when the list is absent.
//
// Indices are walked from 1 — AWS numbers members from 1, contiguously — and the
// first absent index ends the list. A gap therefore truncates rather than being
// skipped over, which is deliberate: a client that produced one is not speaking the
// protocol, and guessing at the intended length would invent members.
//
// Unlike parseMemberList, a present-but-empty value is a member holding the empty
// string rather than the end of the list. That is the same present-vs-absent
// distinction parser.go is careful about (#412): a client that sent an empty value
// sent something, and a required-member check must be able to see it.
func iamMemberList(params map[string]string, prefix string) []string {
	if params == nil {
		return nil
	}
	var out []string
	for i := 1; ; i++ {
		v, ok := params[prefix+iamMemberSep+strconv.Itoa(i)]
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

// iamMemberStructs decodes a query-protocol list of structures: for each index it
// collects every parameter under prefix.member.N. into one map, keyed by the
// remaining suffix. It returns nil when the list is absent.
//
// So Tags.member.1.Key=env&Tags.member.1.Value=prod yields
// [{"Key": "env", "Value": "prod"}].
//
// The suffix is kept verbatim rather than split further, which is what makes a
// nested list work: ContextEntries.member.1.ContextKeyValues.member.1=arn yields a
// member map holding "ContextKeyValues.member.1", so iamMemberList can be applied
// to that map directly with prefix "ContextKeyValues". SimulatePrincipalPolicy's
// ContextEntries is exactly that shape (#579).
//
// An index exists if any parameter sits under it; as in iamMemberList, the first
// absent index ends the list.
func iamMemberStructs(params map[string]string, prefix string) []map[string]string {
	if params == nil {
		return nil
	}
	// One pass over params per index would be O(n·len), so group by index first.
	byIndex := make(map[int]map[string]string)
	root := prefix + iamMemberSep
	for k, v := range params {
		if !strings.HasPrefix(k, root) {
			continue
		}
		rest := k[len(root):]
		dot := strings.IndexByte(rest, '.')
		if dot <= 0 {
			// prefix.member.N with no suffix: a flat member, not a struct one. It
			// belongs to iamMemberList, so leave it out rather than recording an
			// index with no fields.
			continue
		}
		idx, err := strconv.Atoi(rest[:dot])
		if err != nil || idx < 1 {
			continue
		}
		fields, ok := byIndex[idx]
		if !ok {
			fields = make(map[string]string)
			byIndex[idx] = fields
		}
		fields[rest[dot+1:]] = v
	}
	if len(byIndex) == 0 {
		return nil
	}
	var out []map[string]string
	for i := 1; ; i++ {
		fields, ok := byIndex[i]
		if !ok {
			return out
		}
		out = append(out, fields)
	}
}

// iamMemberTags decodes a Tags list from query parameters into the shape the
// tagging handlers store. It returns nil when no Tags list was sent, which is what
// lets a caller fall back to a JSON body.
//
// A member with neither Key nor Value is dropped: an empty tag would otherwise
// merge into an entity's tag set under the key "", which no client asked for. An
// empty *Value* under a real Key is kept, because AWS accepts a tag with an empty
// value.
func iamMemberTags(params map[string]string) []IAMTag {
	members := iamMemberStructs(params, "Tags")
	if members == nil {
		return nil
	}
	out := make([]IAMTag, 0, len(members))
	for _, m := range members {
		key, value := m["Key"], m["Value"]
		if key == "" && value == "" {
			continue
		}
		out = append(out, IAMTag{Key: key, Value: value})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
