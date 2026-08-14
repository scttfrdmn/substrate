package emulator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestIAMMemberList covers the flat query-protocol list decoder (#639). The
// numbering rules are the point: AWS numbers members from 1 contiguously, so index
// 0 is not a member and a gap ends the list rather than being skipped.
func TestIAMMemberList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params map[string]string
		prefix string
		want   []string
	}{
		{
			name:   "nil params",
			params: nil,
			prefix: "ActionNames",
			want:   nil,
		},
		{
			name:   "absent list",
			params: map[string]string{"UserName": "jill"},
			prefix: "ActionNames",
			want:   nil,
		},
		{
			name:   "single member",
			params: map[string]string{"ActionNames.member.1": "s3:GetObject"},
			prefix: "ActionNames",
			want:   []string{"s3:GetObject"},
		},
		{
			name: "three members in order",
			params: map[string]string{
				"ActionNames.member.3": "s3:PutObject",
				"ActionNames.member.1": "s3:GetObject",
				"ActionNames.member.2": "s3:DeleteObject",
			},
			prefix: "ActionNames",
			want:   []string{"s3:GetObject", "s3:DeleteObject", "s3:PutObject"},
		},
		{
			// The walk starts at 1. A member numbered 0 is not something AWS sends, and
			// treating it as one would shift every index.
			name: "index zero is not a member",
			params: map[string]string{
				"ActionNames.member.0": "ignored",
				"ActionNames.member.1": "s3:GetObject",
			},
			prefix: "ActionNames",
			want:   []string{"s3:GetObject"},
		},
		{
			// A gap truncates. Guessing at the intended length would invent members a
			// client did not send.
			name: "a gap ends the list",
			params: map[string]string{
				"ActionNames.member.1": "s3:GetObject",
				"ActionNames.member.3": "s3:PutObject",
			},
			prefix: "ActionNames",
			want:   []string{"s3:GetObject"},
		},
		{
			// Present-but-empty is a member, unlike parseMemberList's rule. A client
			// that sent an empty value sent something, and a required-member check has
			// to be able to see it (the #412 distinction).
			name: "an empty value is a present member",
			params: map[string]string{
				"ActionNames.member.1": "",
				"ActionNames.member.2": "s3:PutObject",
			},
			prefix: "ActionNames",
			want:   []string{"", "s3:PutObject"},
		},
		{
			// A prefix must not match a longer sibling parameter name.
			name: "a longer sibling prefix does not leak in",
			params: map[string]string{
				"Tags.member.1":    "a",
				"TagKeys.member.1": "env",
			},
			prefix: "TagKeys",
			want:   []string{"env"},
		},
		{
			// Struct members belong to iamMemberStructs; the flat decoder must not
			// mistake the dotted suffix for a value.
			name:   "struct members are not flat members",
			params: map[string]string{"Tags.member.1.Key": "env"},
			prefix: "Tags",
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, emulator.IAMMemberListForTest(tc.params, tc.prefix))
		})
	}
}

// TestIAMMemberStructs covers the struct-list decoder, including the nested-list
// case SimulatePrincipalPolicy's ContextEntries needs (#579): a member map keeps its
// suffix verbatim, so iamMemberList applies to it directly.
func TestIAMMemberStructs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params map[string]string
		prefix string
		want   []map[string]string
	}{
		{
			name:   "nil params",
			params: nil,
			prefix: "Tags",
			want:   nil,
		},
		{
			name:   "absent list",
			params: map[string]string{"UserName": "jill"},
			prefix: "Tags",
			want:   nil,
		},
		{
			name: "two members, fields grouped by index",
			params: map[string]string{
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "prod",
				"Tags.member.2.Key":   "team",
				"Tags.member.2.Value": "infra",
			},
			prefix: "Tags",
			want: []map[string]string{
				{"Key": "env", "Value": "prod"},
				{"Key": "team", "Value": "infra"},
			},
		},
		{
			name: "a gap ends the list",
			params: map[string]string{
				"Tags.member.1.Key": "env",
				"Tags.member.3.Key": "team",
			},
			prefix: "Tags",
			want:   []map[string]string{{"Key": "env"}},
		},
		{
			name:   "index zero is not a member",
			params: map[string]string{"Tags.member.0.Key": "env"},
			prefix: "Tags",
			want:   nil,
		},
		{
			// The suffix is kept whole, which is what makes a list inside a struct
			// member decodable by a second iamMemberList call.
			name: "a nested member list keeps its suffix",
			params: map[string]string{
				"ContextEntries.member.1.ContextKeyName":            "aws:username",
				"ContextEntries.member.1.ContextKeyType":            "string",
				"ContextEntries.member.1.ContextKeyValues.member.1": "jill",
				"ContextEntries.member.1.ContextKeyValues.member.2": "jack",
			},
			prefix: "ContextEntries",
			want: []map[string]string{{
				"ContextKeyName":            "aws:username",
				"ContextKeyType":            "string",
				"ContextKeyValues.member.1": "jill",
				"ContextKeyValues.member.2": "jack",
			}},
		},
		{
			// A flat member under the same prefix records no index, so the two
			// decoders cannot corrupt each other's view of the same parameter set.
			name:   "a flat member is not a struct member",
			params: map[string]string{"Tags.member.1": "env"},
			prefix: "Tags",
			want:   nil,
		},
		{
			name:   "a non-numeric index is skipped",
			params: map[string]string{"Tags.member.x.Key": "env"},
			prefix: "Tags",
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := emulator.IAMMemberStructsForTest(tc.params, tc.prefix)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestIAMMemberStructs_NestedListDecodesWithMemberList closes the loop on the
// nested case: the member map iamMemberStructs returns must be usable as input to
// iamMemberList, since that is how ContextKeyValues is read.
func TestIAMMemberStructs_NestedListDecodesWithMemberList(t *testing.T) {
	t.Parallel()

	members := emulator.IAMMemberStructsForTest(map[string]string{
		"ContextEntries.member.1.ContextKeyName":            "aws:PrincipalTag/team",
		"ContextEntries.member.1.ContextKeyValues.member.1": "infra",
		"ContextEntries.member.1.ContextKeyValues.member.2": "platform",
	}, "ContextEntries")
	assert.Len(t, members, 1)
	assert.Equal(t, []string{"infra", "platform"},
		emulator.IAMMemberListForTest(members[0], "ContextKeyValues"))
}

// TestIAMMemberTags covers the tagging shim, whose job is to answer nil when no
// Tags list was sent so a handler can fall back to its JSON body.
func TestIAMMemberTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params map[string]string
		want   []emulator.IAMTag
	}{
		{
			name:   "no tags sent",
			params: map[string]string{"UserName": "jill"},
			want:   nil,
		},
		{
			name: "two tags",
			params: map[string]string{
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "prod",
				"Tags.member.2.Key":   "team",
				"Tags.member.2.Value": "infra",
			},
			want: []emulator.IAMTag{{Key: "env", Value: "prod"}, {Key: "team", Value: "infra"}},
		},
		{
			// AWS accepts a tag with an empty value, so the key alone is enough to
			// make a member real.
			name:   "an empty value under a real key is kept",
			params: map[string]string{"Tags.member.1.Key": "env", "Tags.member.1.Value": ""},
			want:   []emulator.IAMTag{{Key: "env", Value: ""}},
		},
		{
			// A wholly empty member would otherwise merge into the tag set under the
			// key "", which no client asked for.
			name: "a wholly empty member is dropped",
			params: map[string]string{
				"Tags.member.1.Key":   "",
				"Tags.member.1.Value": "",
				"Tags.member.2.Key":   "team",
				"Tags.member.2.Value": "infra",
			},
			want: []emulator.IAMTag{{Key: "team", Value: "infra"}},
		},
		{
			// Every member empty means nothing to apply, and nil is what lets the
			// handler fall through to its JSON body rather than clearing the field.
			name:   "all members empty answers nil",
			params: map[string]string{"Tags.member.1.Key": "", "Tags.member.1.Value": ""},
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, emulator.IAMMemberTagsForTest(tc.params))
		})
	}
}

// TestIAMScalarParams covers the string-tolerant integer and boolean decoders
// (#642).
//
// Both forms must work, and the reason is the defect: the query protocol has no
// types, so every real client sends "1", while the unit suite's iamRequest
// hand-marshals a real JSON number. Accepting only one of the two shapes is exactly
// how MaxItems came to answer 400 to every SDK caller while the unit tests stayed
// green.
func TestIAMScalarParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		wantCount int
		wantFlag  bool
		wantErr   string
	}{
		{
			name:      "the wire form: every value is a string",
			body:      `{"Count":"25","Flag":"true"}`,
			wantCount: 25,
			wantFlag:  true,
		},
		{
			// The shape iamRequest sends. It must keep working, or every existing unit
			// test that passes MaxItems breaks.
			name:      "the unit-test form: real JSON scalars",
			body:      `{"Count":25,"Flag":true}`,
			wantCount: 25,
			wantFlag:  true,
		},
		{
			name:      "absent members stay zero",
			body:      `{"UserName":"jill"}`,
			wantCount: 0,
			wantFlag:  false,
		},
		{
			// A client that sent "MaxItems=" expressed no limit. Refusing it would fail
			// a request AWS accepts.
			name:      "an empty string is the zero value, not an error",
			body:      `{"Count":"","Flag":""}`,
			wantCount: 0,
			wantFlag:  false,
		},
		{
			name:      "explicit null is the zero value",
			body:      `{"Count":null,"Flag":null}`,
			wantCount: 0,
			wantFlag:  false,
		},
		{
			name:      "a negative integer decodes, so a range check can refuse it",
			body:      `{"Count":"-1"}`,
			wantCount: -1,
		},
		{
			name:     "false decodes as false, not as absent",
			body:     `{"Flag":"false"}`,
			wantFlag: false,
		},
		{
			// The query protocol lower-cases neither, and the CLI sends "true".
			name:     "a boolean is case-insensitive",
			body:     `{"Flag":"True"}`,
			wantFlag: true,
		},
		{
			// The message must name the parameter. Before this, a caller saw
			// "cannot unmarshal string into Go struct field .MaxItems of type int".
			name:    "a non-numeric integer names its parameter",
			body:    `{"Count":"abc"}`,
			wantErr: `invalid request body: Count must be an integer, got "abc"`,
		},
		{
			name:    "a non-boolean names its parameter",
			body:    `{"Flag":"yes"}`,
			wantErr: `invalid request body: Flag must be true or false, got "yes"`,
		},
		{
			// 1/0 is not a boolean in any AWS client, and accepting it would mean
			// guessing at a value the caller never wrote.
			name:    "a numeric boolean is refused",
			body:    `{"Flag":"1"}`,
			wantErr: `invalid request body: Flag must be true or false, got "1"`,
		},
		{
			// A malformed body has no parameter to name, so it must still report
			// something a caller can act on rather than an empty message.
			name:    "a malformed body still reports an error",
			body:    `{"Count":`,
			wantErr: "invalid request body: unexpected end of JSON input",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var params emulator.IAMScalarParamsForTest
			err := emulator.ParseIAMBodyForTest([]byte(tc.body), &params)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tc.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
			count, flag := params.IAMScalarValuesForTest()
			assert.Equal(t, tc.wantCount, count)
			assert.Equal(t, tc.wantFlag, flag)
		})
	}
}

// TestIAMScalarParams_EmptyBodyIsNotAnError pins parseIAMBody's contract that an
// absent body decodes to the zero value, which is what lets an operation with only
// optional parameters be called with nothing at all.
func TestIAMScalarParams_EmptyBodyIsNotAnError(t *testing.T) {
	t.Parallel()

	var params emulator.IAMScalarParamsForTest
	require.NoError(t, emulator.ParseIAMBodyForTest(nil, &params))
	count, flag := params.IAMScalarValuesForTest()
	assert.Zero(t, count)
	assert.False(t, flag)
}
