package emulator_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A stack's own tags (#764).
//
// `CreateStack`'s `Tags.member.N` was decoded by nothing at all: a caller sending
// `--tags Key=team,Value=platform` got a 200 and a stack that reported no tags and
// propagated none, so a cost-allocation or policy assertion keyed on a stack tag had
// nothing to read. These tests cover recording and reporting them; the propagation onto
// the stack's resources is #765 and #764's second half.
//
// The update semantics are AWS's, quoted: "If you don't specify this parameter,
// CloudFormation doesn't modify the stack's tags. If you specify an empty value,
// CloudFormation removes all associated tags." That is the one distinction the wire
// decoder has to preserve, and the reason a stack's tags are a map that can be nil
// rather than a list.

// cfnStackTagParams renders a tag map as the indexed Tags.member.N parameters an SDK
// sends, in the order the keys are given so a test can assert on a specific index.
func cfnStackTagParams(pairs ...string) map[string]string {
	params := make(map[string]string, len(pairs))
	for i := 0; i < len(pairs); i += 2 {
		n := i/2 + 1
		params[fmt.Sprintf("Tags.member.%d.Key", n)] = pairs[i]
		params[fmt.Sprintf("Tags.member.%d.Value", n)] = pairs[i+1]
	}
	return params
}

// cfnDescribeStackTags reads one stack's tags off DescribeStacks, preserving the order
// they were reported in so the sorting can be asserted.
func cfnDescribeStackTags(t *testing.T, ts *cfnTestServer, stackName string) []string {
	t.Helper()
	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": stackName})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Stacks []struct {
			Tags []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tags>member"`
		} `xml:"DescribeStacksResult>Stacks>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	require.Len(t, doc.Stacks, 1)

	out := make([]string, 0, len(doc.Stacks[0].Tags))
	for _, tag := range doc.Stacks[0].Tags {
		out = append(out, tag.Key+"="+tag.Value)
	}
	return out
}

// cfnCreateStackWithParams creates a stack with the template and the extra parameters,
// requiring success.
func cfnCreateStackWithParams(t *testing.T, ts *cfnTestServer, name, template string, extra map[string]string) {
	t.Helper()
	params := map[string]string{"StackName": name, "TemplateBody": template}
	for k, v := range extra {
		params[k] = v
	}
	code, body := cfnAction(t, ts, "CreateStack", params)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
}

func TestCFN_CreateStackRecordsItsTagsAndDescribeStacksReportsThem(t *testing.T) {
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "tagged", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform", "cost-center", "4471", "env", "test"))

	// Sorted by key, which is [cfnParametersXML]'s property and for the same reason: a
	// response whose member order follows Go's map iteration differs run to run, and an
	// assertion on it would flake in an emulator whose whole claim is that it does not.
	assert.Equal(t, []string{"cost-center=4471", "env=test", "owner=platform"},
		cfnDescribeStackTags(t, ts, "tagged"))
}

func TestCFN_AStackWithNoTagsReportsAnEmptyTagList(t *testing.T) {
	// An empty `<Tags></Tags>` with no members, exactly as the Parameters and Outputs
	// members on the same shape already render: `encoding/xml` writes the parent element
	// of a nested path whether or not the slice has members. The assertion is on the raw
	// body because a decoded slice cannot tell an empty element from an absent one — and
	// the point is that a stack with no tags reports no *member*, not that the element
	// disappears.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "untagged", cfnEmptyTemplate, nil)

	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "untagged"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<Tags></Tags>")
	assert.Empty(t, cfnDescribeStackTags(t, ts, "untagged"))
}

func TestCFN_ATagOutsideTheLimitsAWSDocumentsIsRefused(t *testing.T) {
	// Every limit is the `Tag` data type's own — key 1-128 characters, value 1-256, and
	// "a maximum number of 50 tags can be specified" from CreateStack's prose. The value's
	// minimum of **1** is what makes an empty value illegal here and legal in IAM and
	// ELBv2, whose Tag types document a minimum of 0. That is AWS's inconsistency, not
	// substrate's, and it is asserted rather than smoothed over.
	//
	// The reserved prefix is matched case-insensitively, which is also AWS's own rule and
	// only CloudFormation's: "The `aws:` prefix is reserved for AWS use. This prefix is
	// case-insensitive."
	t.Parallel()

	fiftyOne := make(map[string]string, 51*2)
	for i := 1; i <= 51; i++ {
		fiftyOne[fmt.Sprintf("Tags.member.%d.Key", i)] = fmt.Sprintf("key%d", i)
		fiftyOne[fmt.Sprintf("Tags.member.%d.Value", i)] = "v"
	}

	tests := []struct {
		name   string
		params map[string]string
	}{
		{"fifty-one tags", fiftyOne},
		{"an empty key", cfnStackTagParams("", "platform")},
		{"a key of 129 characters", cfnStackTagParams(strings.Repeat("k", 129), "v")},
		{"an empty value", cfnStackTagParams("owner", "")},
		{"a value of 257 characters", cfnStackTagParams("owner", strings.Repeat("v", 257))},
		{"the reserved aws: prefix", cfnStackTagParams("aws:cloudformation:stack-name", "mine")},
		{"the reserved prefix in a different case", cfnStackTagParams("AWS:owner", "platform")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newCFNTestServer(t)
			params := map[string]string{"StackName": "refused", "TemplateBody": cfnEmptyTemplate}
			for k, v := range tt.params {
				params[k] = v
			}
			code, body := cfnAction(t, ts, "CreateStack", params)
			assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

			// The refusal is of the request, so no stack exists afterwards — the same rule
			// CreateUser states for a bad tag: "the entire request fails and the resource is
			// not created".
			code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "refused"})
			assert.Equal(t, http.StatusBadRequest, code, "no stack was created; body was %s", body)
		})
	}

	t.Run("fifty tags are accepted", func(t *testing.T) {
		// The boundary from the other side, so the cap cannot drift to 49 unnoticed.
		t.Parallel()
		ts := newCFNTestServer(t)
		params := map[string]string{}
		for i := 1; i <= 50; i++ {
			params[fmt.Sprintf("Tags.member.%d.Key", i)] = fmt.Sprintf("key%d", i)
			params[fmt.Sprintf("Tags.member.%d.Value", i)] = "v"
		}
		cfnCreateStackWithParams(t, ts, "fifty", cfnEmptyTemplate, params)
		assert.Len(t, cfnDescribeStackTags(t, ts, "fifty"), 50)
	})

	t.Run("a 128-character key and a 256-character value are accepted", func(t *testing.T) {
		t.Parallel()
		ts := newCFNTestServer(t)
		cfnCreateStackWithParams(t, ts, "edges", cfnEmptyTemplate,
			cfnStackTagParams(strings.Repeat("k", 128), strings.Repeat("v", 256)))
		assert.Len(t, cfnDescribeStackTags(t, ts, "edges"), 1)
	})

	t.Run("the reserved prefix in a value is accepted", func(t *testing.T) {
		// The prefix rule AWS states is on the key: it "can't be prefixed with `aws:`". Of a
		// *value* carrying it the same page says only that "you can't update or delete the
		// tag" — a consequence, not a refusal — and the `Tag` type's Value description
		// prohibits nothing. So this is stored, where IAM refuses the identical string.
		t.Parallel()
		ts := newCFNTestServer(t)
		cfnCreateStackWithParams(t, ts, "prefixed-value", cfnEmptyTemplate,
			cfnStackTagParams("owner", "aws:platform"))
		assert.Equal(t, []string{"owner=aws:platform"},
			cfnDescribeStackTags(t, ts, "prefixed-value"))
	})
}

func TestCFN_AnUpdateOmittingTagsPreservesThemAndAnEmptyListClearsThem(t *testing.T) {
	// AWS, verbatim: "If you don't specify this parameter, CloudFormation doesn't modify
	// the stack's tags. If you specify an empty value, CloudFormation removes all
	// associated tags."
	//
	// The empty list is not "no parameters" on the wire. botocore's query serializer
	// writes a bare `Tags=` for an empty list — "The query protocol serializes empty
	// lists", `serialize.py` — so that is what an `--tags '[]'` request arrives as, and it
	// is what the second subtest sends.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "evolving", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform"))

	t.Run("an update that omits Tags preserves them", func(t *testing.T) {
		code, body := cfnAction(t, ts, "UpdateStack", map[string]string{
			"StackName":    "evolving",
			"TemplateBody": cfnEmptyTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Equal(t, []string{"owner=platform"}, cfnDescribeStackTags(t, ts, "evolving"))
	})

	t.Run("an update that supplies Tags replaces them wholesale", func(t *testing.T) {
		params := map[string]string{"StackName": "evolving", "TemplateBody": cfnEmptyTemplate}
		for k, v := range cfnStackTagParams("env", "staging") {
			params[k] = v
		}
		code, body := cfnAction(t, ts, "UpdateStack", params)
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Equal(t, []string{"env=staging"}, cfnDescribeStackTags(t, ts, "evolving"),
			"the tag set is replaced, not merged: `owner` is gone")
	})

	t.Run("an empty Tags list clears them", func(t *testing.T) {
		code, body := cfnAction(t, ts, "UpdateStack", map[string]string{
			"StackName":    "evolving",
			"TemplateBody": cfnEmptyTemplate,
			"Tags":         "",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Empty(t, cfnDescribeStackTags(t, ts, "evolving"))
	})

	t.Run("and an update is held to the same limits as a create", func(t *testing.T) {
		code, body := cfnAction(t, ts, "UpdateStack", map[string]string{
			"StackName":           "evolving",
			"TemplateBody":        cfnEmptyTemplate,
			"Tags.member.1.Key":   "aws:cloudformation:stack-name",
			"Tags.member.1.Value": "mine",
			"Tags.member.2.Key":   "owner",
			"Tags.member.2.Value": "platform",
		})
		assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
		assert.Empty(t, cfnDescribeStackTags(t, ts, "evolving"),
			"a refused update changes nothing, including the tags it would have set")
	})
}

func TestCFN_ATagIsAddedChangedAndRemovedAcrossUpdates(t *testing.T) {
	// The three transitions a consumer's tagging code has to survive, in one stack's
	// history, because each is a different path through the resolver: a key appearing, a
	// key whose value changes, and a key that stops being sent.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "drifting", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform", "env", "test"))
	require.Equal(t, []string{"env=test", "owner=platform"},
		cfnDescribeStackTags(t, ts, "drifting"))

	update := func(t *testing.T, pairs ...string) {
		t.Helper()
		params := map[string]string{"StackName": "drifting", "TemplateBody": cfnEmptyTemplate}
		for k, v := range cfnStackTagParams(pairs...) {
			params[k] = v
		}
		code, body := cfnAction(t, ts, "UpdateStack", params)
		require.Equal(t, http.StatusOK, code, "body was %s", body)
	}

	update(t, "owner", "platform", "env", "prod", "team", "core")
	assert.Equal(t, []string{"env=prod", "owner=platform", "team=core"},
		cfnDescribeStackTags(t, ts, "drifting"), "env changed value and team was added")

	update(t, "env", "prod")
	assert.Equal(t, []string{"env=prod"}, cfnDescribeStackTags(t, ts, "drifting"),
		"owner and team stopped being sent, so they are gone")
}
