package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A change set's own tags (#824).
//
// `CreateChangeSet` publishes a `Tags` parameter with the same description `CreateStack`
// carries — "key-value pairs to associate with this stack. CloudFormation also propagates
// these tags to resources in the stack" — and substrate decoded none of it: the parameter
// reached a handler that never read it, so a change set answered 200, reported no tags, and
// executed as though the caller had asked for none. That was the one hole left in #764,
// which fixed `CreateStack` and `UpdateStack`.
//
// `ExecuteChangeSet`'s page mentions tags nowhere at all — no request parameter, no response
// element, an empty result body — so the documented warrant for applying them on execution is
// `DescribeChangeSet`'s description of the member it reports: "if you execute the change set,
// the tags that will be associated with the stack". That sentence is the only statement AWS
// makes about what executing a change set does with tags, and it is what these tests assert.
//
// The three-way meaning is the family's, quoted from `UpdateStack` because that is the
// operation execution routes through: "If you don't specify this parameter, CloudFormation
// doesn't modify the stack's tags. If you specify an empty value, CloudFormation removes all
// associated tags."

// cfnDescribeChangeSetTags reads one change set's tags off DescribeChangeSet, preserving the
// reported order so the sorting can be asserted.
func cfnDescribeChangeSetTags(t *testing.T, ts *cfnTestServer, stackName, changeSetName string) []string {
	t.Helper()
	code, body := cfnAction(t, ts, "DescribeChangeSet", map[string]string{
		"StackName":     stackName,
		"ChangeSetName": changeSetName,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"DescribeChangeSetResult>Tags>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)

	out := make([]string, 0, len(doc.Tags))
	for _, tag := range doc.Tags {
		out = append(out, tag.Key+"="+tag.Value)
	}
	return out
}

// cfnBucketTagsOverTheWire reads a bucket's tags through S3's own GetBucketTagging, which is
// the only reader that proves a propagated tag reached somewhere a caller can see it — #765's
// standing rule that a tag written to a key the service does not read satisfies a state
// assertion and no caller. A bucket with no tags answers NoSuchTagSet, which reads as none.
func cfnBucketTagsOverTheWire(t *testing.T, ts *cfnTestServer, bucket string) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://s3.amazonaws.com/"+bucket+"?tagging", nil)
	req.Host = "s3.amazonaws.com"
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, req)
	body := w.Body.String()
	if w.Code == http.StatusNotFound && strings.Contains(body, "NoSuchTagSet") {
		return nil
	}
	require.Equal(t, http.StatusOK, w.Code, "body was %s", body)

	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"TagSet>Tag"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)

	out := make([]string, 0, len(doc.Tags))
	for _, tag := range doc.Tags {
		out = append(out, tag.Key+"="+tag.Value)
	}
	return out
}

// cfnCreateChangeSetWithParams creates a change set with the template and extra parameters,
// requiring success.
func cfnCreateChangeSetWithParams(t *testing.T, ts *cfnTestServer, stackName, changeSetName, template string, extra map[string]string) {
	t.Helper()
	params := map[string]string{
		"StackName":     stackName,
		"ChangeSetName": changeSetName,
		"TemplateBody":  template,
	}
	for k, v := range extra {
		params[k] = v
	}
	code, body := cfnAction(t, ts, "CreateChangeSet", params)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
}

func TestCFN_AChangeSetRecordsItsTagsAndDescribeChangeSetReportsThem(t *testing.T) {
	// Sorted by key, which is what `DescribeStacks` already does with the stack's own tags
	// and for the same reason: a response ordered by Go's map iteration differs run to run.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "cs-tagged", cfnEmptyTemplate, nil)
	cfnCreateChangeSetWithParams(t, ts, "cs-tagged", "with-tags", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform", "cost-center", "4471", "env", "test"))

	assert.Equal(t, []string{"cost-center=4471", "env=test", "owner=platform"},
		cfnDescribeChangeSetTags(t, ts, "cs-tagged", "with-tags"))
}

func TestCFN_AChangeSetWithNoTagsReportsNoTagMember(t *testing.T) {
	// An empty `<Tags></Tags>` carrying no member, which is what the Parameters and Changes
	// members beside it already render and what `DescribeStacks` renders for a stack with no
	// tags: `encoding/xml` writes the parent element of a nested path whether or not the
	// slice has members, so `omitempty` suppresses the members and not the wrapper. The
	// point is that a change set with no tags reports no *member*, not that the element
	// disappears — asserted against the raw body, because a decoded slice cannot tell an
	// empty element from an absent one.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "cs-untagged", cfnEmptyTemplate, nil)
	cfnCreateChangeSetWithParams(t, ts, "cs-untagged", "no-tags", cfnEmptyTemplate, nil)

	code, body := cfnAction(t, ts, "DescribeChangeSet", map[string]string{
		"StackName":     "cs-untagged",
		"ChangeSetName": "no-tags",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<Tags></Tags>")
	assert.Empty(t, cfnDescribeChangeSetTags(t, ts, "cs-untagged", "no-tags"))
}

func TestCFN_ExecutingAChangeSetAppliesItsTagsToTheStackAndItsResources(t *testing.T) {
	// The load-bearing assertion: the tags survive being written to state, read back at
	// execution, and threaded through the update onto the resource the change set created.
	// A resource's tags are read through S3's own GetBucketTagging rather than out of the
	// state store, so nothing here passes on a tag no caller could see.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "cs-applies", cfnEmptyTemplate, nil)
	cfnCreateChangeSetWithParams(t, ts, "cs-applies", "add-bucket", cfnBucketTemplate,
		cfnStackTagParams("owner", "platform"))

	// Nothing is applied until it executes: neither the stack nor the bucket exists yet.
	assert.Empty(t, cfnDescribeStackTags(t, ts, "cs-applies"),
		"a described change set has not been executed")

	code, body := cfnAction(t, ts, "ExecuteChangeSet", map[string]string{
		"StackName":     "cs-applies",
		"ChangeSetName": "add-bucket",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	assert.Equal(t, []string{"owner=platform"}, cfnDescribeStackTags(t, ts, "cs-applies"))
	assert.Contains(t, cfnBucketTagsOverTheWire(t, ts, "cfn-wire-bucket"), "owner=platform",
		"the change set's tags propagate to the resources it created, beside the aws:cloudformation:* stamp")
}

func TestCFN_AChangeSetWithoutTagsLeavesTheStacksOwnTagsAlone(t *testing.T) {
	// #824's fourth criterion, and the compatibility guarantee: a nil Tags is the omitted
	// parameter, so a change set created without one executes exactly as it did before the
	// field existed. A record written by an earlier version decodes to nil too, which is why
	// the field is not `omitempty` in state — see [emulator.CFNChangeSet].
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "cs-preserves", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform"))
	cfnCreateChangeSetWithParams(t, ts, "cs-preserves", "untagged", cfnBucketTemplate, nil)

	code, body := cfnAction(t, ts, "ExecuteChangeSet", map[string]string{
		"StackName":     "cs-preserves",
		"ChangeSetName": "untagged",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	assert.Equal(t, []string{"owner=platform"}, cfnDescribeStackTags(t, ts, "cs-preserves"))
}

func TestCFN_AChangeSetWithAnEmptyTagsListClearsTheStacksTags(t *testing.T) {
	// The other half of the distinction, and the reason the recorded field cannot be
	// `omitempty`: an empty list has to survive the round trip through state as an empty
	// non-nil map, or it would read back as an omitted parameter and preserve where the
	// caller asked to clear.
	//
	// A bare `Tags=` is what an empty list arrives as: botocore's query serializer "serializes
	// empty lists", so `--tags '[]'` puts the parameter on the wire with no members.
	t.Parallel()

	ts := newCFNTestServer(t)
	cfnCreateStackWithParams(t, ts, "cs-clears", cfnEmptyTemplate,
		cfnStackTagParams("owner", "platform"))
	cfnCreateChangeSetWithParams(t, ts, "cs-clears", "clear-tags", cfnEmptyTemplate,
		map[string]string{"Tags": ""})

	require.Empty(t, cfnDescribeChangeSetTags(t, ts, "cs-clears", "clear-tags"),
		"an empty list records no tags, which is not the same as recording nothing")

	code, body := cfnAction(t, ts, "ExecuteChangeSet", map[string]string{
		"StackName":     "cs-clears",
		"ChangeSetName": "clear-tags",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	assert.Empty(t, cfnDescribeStackTags(t, ts, "cs-clears"),
		"an empty Tags list removes all associated tags")
}

func TestCFN_ACreateChangeSetTagOutsideTheLimitsIsRefused(t *testing.T) {
	// The same limits `CreateStack` is held to, checked at creation rather than deferred to
	// execution: `CreateChangeSet` publishes the same `Tags` constraints, so a change set
	// that could never execute is refused rather than recorded. The refusal is of the whole
	// request, so no change set exists afterwards.
	t.Parallel()

	tests := []struct {
		name   string
		params map[string]string
	}{
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
			cfnCreateStackWithParams(t, ts, "cs-refused", cfnEmptyTemplate, nil)

			params := map[string]string{
				"StackName":     "cs-refused",
				"ChangeSetName": "bad-tags",
				"TemplateBody":  cfnEmptyTemplate,
			}
			for k, v := range tt.params {
				params[k] = v
			}
			code, body := cfnAction(t, ts, "CreateChangeSet", params)
			assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

			code, body = cfnAction(t, ts, "DescribeChangeSet", map[string]string{
				"StackName":     "cs-refused",
				"ChangeSetName": "bad-tags",
			})
			assert.Equal(t, http.StatusNotFound, code, "no change set was created; body was %s", body)
			assert.Equal(t, "ChangeSetNotFound", cfnErrorCode(t, body))
		})
	}

	t.Run("fifty-one tags are refused and fifty are accepted", func(t *testing.T) {
		// The cap from both sides, so it cannot drift to 49 unnoticed.
		t.Parallel()
		ts := newCFNTestServer(t)
		cfnCreateStackWithParams(t, ts, "cs-fifty", cfnEmptyTemplate, nil)

		pairs := make([]string, 0, 51*2)
		for i := 1; i <= 51; i++ {
			pairs = append(pairs, "key"+strings.Repeat("x", i), "v")
		}
		params := map[string]string{
			"StackName":     "cs-fifty",
			"ChangeSetName": "too-many",
			"TemplateBody":  cfnEmptyTemplate,
		}
		for k, v := range cfnStackTagParams(pairs...) {
			params[k] = v
		}
		code, body := cfnAction(t, ts, "CreateChangeSet", params)
		assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

		cfnCreateChangeSetWithParams(t, ts, "cs-fifty", "exactly-fifty", cfnEmptyTemplate,
			cfnStackTagParams(pairs[:100]...))
		assert.Len(t, cfnDescribeChangeSetTags(t, ts, "cs-fifty", "exactly-fifty"), 50)
	})
}
