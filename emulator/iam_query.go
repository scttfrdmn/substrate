package emulator

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

// iamInt is an integer parameter that decodes from either a JSON number or a JSON
// string.
//
// It exists because the query protocol has no types: every value in the JSON body
// server.go rebuilds is a string, so `MaxItems=1` arrives as `{"MaxItems":"1"}` and
// a field declared `int` made encoding/json refuse the whole request. Every
// paginated IAM operation answered `400 ValidationError` to a caller who passed
// MaxItems at all, and CreateRole rejected MaxSessionDuration (#642) — the louder
// half of the defect #639 fixed for lists, which failed silently instead.
//
// Both forms are accepted rather than only the string, because iam_plugin_test.go's
// iamRequest hand-marshals a body holding a real JSON number. That is exactly the
// asymmetry that hid this: the unit path sends a number, every real client sends a
// string, and only the first was ever exercised.
type iamInt int

// UnmarshalJSON decodes a JSON number or a numeric JSON string.
//
// An empty string is zero, matching an absent parameter: a client that sent
// `MaxItems=` expressed no limit, and refusing it would fail a request AWS accepts.
func (v *iamInt) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == "null" {
		return nil
	}
	if unquoted, ok := iamUnquote(s); ok {
		if unquoted == "" {
			*v = 0
			return nil
		}
		s = unquoted
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return &iamParamError{message: fmt.Sprintf("must be an integer, got %q", s)}
	}
	*v = iamInt(n)
	return nil
}

// Int returns the value as a plain int, for handing to a helper that takes one.
func (v iamInt) Int() int { return int(v) }

// iamBool is a boolean parameter that decodes from either a JSON bool or a JSON
// string, for the same reason as [iamInt].
//
// Only "true" and "false" are accepted, case-insensitively, which is what the query
// protocol sends. A numeric 1/0 is not: no AWS client emits it, and accepting it
// would mean guessing at a value the caller never wrote.
type iamBool bool

// UnmarshalJSON decodes a JSON bool or a "true"/"false" JSON string. An empty
// string is false, matching an absent parameter.
func (v *iamBool) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == "null" {
		return nil
	}
	if unquoted, ok := iamUnquote(s); ok {
		if unquoted == "" {
			*v = false
			return nil
		}
		s = strings.ToLower(unquoted)
	}
	switch s {
	case "true":
		*v = true
	case "false":
		*v = false
	default:
		return &iamParamError{message: fmt.Sprintf("must be true or false, got %q", s)}
	}
	return nil
}

// iamParamError is a scalar-decoding refusal from [iamInt] or [iamBool]. It carries
// the sentence fragment that follows the parameter's name, which [iamParamMessage]
// turns into "MaxItems must be an integer, got \"abc\"".
//
// It is a dedicated type rather than a *json.UnmarshalTypeError carrying a smuggled
// message, which is what this was until #787. UnmarshalTypeError is the one error the
// decoder itself annotates with the field it was decoding, so encoding/json filled in
// the name for free — until Go 1.27, whose encoding/json leaves Field empty for an
// error returned from a custom UnmarshalJSON. Every caller on that toolchain read
// "json: cannot unmarshal must be an integer, got \"abc\" into Go value of type int",
// the internal the mechanism existed to avoid. The name is now resolved explicitly,
// which depends on no decoder behavior at all.
type iamParamError struct {
	// message is the fragment that follows the parameter's name.
	message string
}

// Error implements the error interface, returning the fragment with no name. A caller
// that never resolves one still sees something meaningful.
func (e *iamParamError) Error() string { return e.message }

// iamParamMessage turns a body-decoding error into a parameter-shaped message, so a
// caller sees "MaxItems must be an integer, got \"abc\"" rather than an
// encoding/json internal.
//
// body and dst must be the values that were handed to json.Unmarshal; they are what
// let the offending parameter be named. An error from any other cause is returned
// as-is: this is presentation for the one case that has a parameter name, not a
// general error rewrite.
func iamParamMessage(err error, body []byte, dst any) string {
	var paramErr *iamParamError
	if !errors.As(err, &paramErr) {
		return err.Error()
	}
	if name := iamParamField(body, dst, paramErr); name != "" {
		return name + " " + paramErr.message
	}
	return paramErr.message
}

// iamParamField reports which of dst's members refused to decode, by decoding each one
// on its own and matching the refusal against the one that failed.
//
// It walks dst's fields rather than the body's keys so the answer is deterministic —
// struct declaration order, not Go's randomized map iteration — and it considers only
// members the body actually carries, so a field the caller never sent cannot be blamed.
//
// The name is the outermost member carrying the refusal, not the full path to it: a
// bad scalar inside a list member is reported as "Nested", not "Nested.1.Count". That
// is enough for the protocol this file decodes, which delivers every scalar at the top
// level — parser.go flattens "Tags.member.1.Key" into a key of that literal name (see
// iamMemberStructs), so a genuinely nested scalar arrives only from a hand-marshaled
// test body, never from a client. Walking deeper to build a path no client can provoke
// would be speculative.
func iamParamField(body []byte, dst any, want *iamParamError) string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	v := reflect.ValueOf(dst)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	structType := v.Type()
	for i := range structType.NumField() {
		field := structType.Field(i)
		value, sent := raw[iamFieldName(field)]
		if !sent {
			continue
		}
		probe := reflect.New(field.Type)
		var probeErr *iamParamError
		if err := json.Unmarshal(value, probe.Interface()); err != nil &&
			errors.As(err, &probeErr) && probeErr.message == want.message {
			return iamFieldName(field)
		}
	}
	return ""
}

// iamFieldName returns the wire name of a struct field: its json tag if it has one,
// otherwise the Go field name, which is what encoding/json itself matches on.
func iamFieldName(field reflect.StructField) string {
	tag, ok := field.Tag.Lookup("json")
	if !ok {
		return field.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" || name == "-" {
		return field.Name
	}
	return name
}

// Bool returns the value as a plain bool.
func (v iamBool) Bool() bool { return bool(v) }

// iamUnquote unwraps a JSON string literal, reporting whether data was one.
//
// json.Unmarshal is used rather than trimming quotes so an escaped value decodes
// correctly; a value that will not decode is reported as not-a-string and falls
// through to the numeric parse, which produces the parameter-shaped error.
func iamUnquote(data string) (string, bool) {
	if len(data) < 2 || data[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal([]byte(data), &s); err != nil {
		return "", false
	}
	return s, true
}

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
// This is a **pure decoder**: every member that arrived is returned, including one with an empty
// key, and nothing here judges it. It dropped an empty member before #806 — an empty tag would
// otherwise have merged into an entity's tag set under the key "" — but silently discarding it
// is exactly the wrong answer now that a validator exists, because AWS refuses the request
// ("You cannot create an empty tag key") rather than ignoring the tag. Dropping it here would
// hide it from [iamValidateTagSet] and leave substrate accepting a request real IAM rejects.
//
// An empty *Value* under a real Key is legal and is kept, because AWS accepts a tag with an
// empty value; the query wire cannot distinguish that from an omitted `Value`, and the `Tag`
// type's minimum value length of 0 makes both acceptable.
func iamMemberTags(params map[string]string) []IAMTag {
	members := iamMemberStructs(params, "Tags")
	if members == nil {
		return nil
	}
	out := make([]IAMTag, 0, len(members))
	for _, m := range members {
		out = append(out, IAMTag{Key: m["Key"], Value: m["Value"]})
	}
	return out
}
