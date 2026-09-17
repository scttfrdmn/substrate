package emulator_test

// GetResources reports what has been tagged, not what is tagged (#938).
//
// AWS_GetResources states two rules that substrate could not tell apart, because both end with a
// record whose tag set is empty: "GetResources does not return untagged resources", and, on
// TagFilters, "If you don't specify a TagFilter, the response includes all resources that are
// currently tagged or ever had a tag. Resources that were previously tagged, but do not currently have
// tags, are shown with an empty tag set, like this: "Tags": []."
//
// So there are three states to hold apart, and each test below is one of them: never tagged (absent),
// tagged (reported with its tags), and previously tagged (reported with an empty list, and only when
// no TagFilter is given). The third is the one that needs state beyond the tags, and the flag each
// scanned record now carries is where that state lives.
//
// Two directions are covered deliberately. The tagging API's own TagResources/UntagResources is one
// writer; each owning service's native tag operation is another, and two of those — S3's
// PutBucketTagging and DeleteBucketTagging — replace a bucket's whole tag set rather than merging into
// it, which is the one shape that would drop the flag if it did not read the set it replaces. Every
// resource here is created and tagged through real wire calls, per #765: a helper writing state
// directly would choose the very field the rule is read from.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// everTaggedServer starts a server callable as [taggingTestAccount].
func everTaggedServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// everTaggedTable creates a DynamoDB table and returns the ARN CreateTable itself reports.
//
// tags is passed through to the create call, so a caller can choose between a resource tagged at
// creation and one tagged afterwards — the distinction the flag's rule turns on, since a resource
// holding a tag is reported for holding it and only a removal has to record the history.
func everTaggedTable(t *testing.T, ts *emulator.TestServer, name string, tags map[string]string) string {
	t.Helper()
	body := map[string]any{
		"TableName":            name,
		"AttributeDefinitions": []map[string]string{{"AttributeName": "id", "AttributeType": "S"}},
		"KeySchema":            []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
		"BillingMode":          "PAY_PER_REQUEST",
	}
	if len(tags) > 0 {
		list := make([]map[string]string, 0, len(tags))
		for k, v := range tags {
			list = append(list, map[string]string{"Key": k, "Value": v})
		}
		body["Tags"] = list
	}
	var out struct {
		TableDescription struct {
			TableARN string `json:"TableArn"`
		} `json:"TableDescription"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, dynamodbTarget, taggingTestAccount, "CreateTable", body), &out)
	require.Emptyf(t, errCode, "CreateTable %s", name)
	require.Equalf(t, http.StatusOK, status, "CreateTable %s", name)
	require.NotEmptyf(t, out.TableDescription.TableARN, "CreateTable %s reports an ARN", name)
	return out.TableDescription.TableARN
}

// everTaggedBucket creates an S3 bucket in us-east-1.
func everTaggedBucket(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+name, nil, "")
	require.Equalf(t, http.StatusOK, status, "CreateBucket %s: %s", name, raw)
}

// everTaggedPutBucketTagging replaces a bucket's whole tag set through S3's own operation.
func everTaggedPutBucketTagging(t *testing.T, ts *emulator.TestServer, name, key, value string) {
	t.Helper()
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+name+"?tagging",
		[]byte(`<Tagging><TagSet><Tag><Key>`+key+`</Key><Value>`+value+
			`</Value></Tag></TagSet></Tagging>`), "application/xml")
	require.Equalf(t, http.StatusNoContent, status, "PutBucketTagging %s: %s", name, raw)
}

// everTaggedDeleteBucketTagging empties a bucket's tag set through S3's own operation.
func everTaggedDeleteBucketTagging(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodDelete,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+name+"?tagging", nil, "")
	require.Equalf(t, http.StatusNoContent, status, "DeleteBucketTagging %s: %s", name, raw)
}

// everTaggedRawTags calls GetResources with the given request members and returns each reported ARN's
// Tags member as raw JSON, plus the PaginationToken.
//
// Raw, because the difference between an empty tag set and an absent one is exactly what AWS's rule
// publishes — "Tags": [] — and a decoded []Tag cannot tell [] from null.
func everTaggedRawTags(t *testing.T, ts *emulator.TestServer, body map[string]any) (map[string]string, string) {
	t.Helper()
	var out struct {
		ResourceTagMappingList []map[string]json.RawMessage `json:"ResourceTagMappingList"`
		PaginationToken        string                       `json:"PaginationToken"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, "GetResources", body), &out)
	require.Empty(t, errCode, "GetResources")
	require.Equal(t, http.StatusOK, status, "GetResources")

	tags := make(map[string]string, len(out.ResourceTagMappingList))
	for _, rm := range out.ResourceTagMappingList {
		var arn string
		require.NoError(t, json.Unmarshal(rm["ResourceARN"], &arn), "decode ResourceARN")
		require.NotContainsf(t, tags, arn, "GetResources reports %s once", arn)
		tags[arn] = string(rm["Tags"])
	}
	return tags, out.PaginationToken
}

// everTaggedReported is [everTaggedRawTags] for the calls that assert membership rather than a shape.
func everTaggedReported(t *testing.T, ts *emulator.TestServer, body map[string]any) []string {
	t.Helper()
	tags, _ := everTaggedRawTags(t, ts, body)
	arns := make([]string, 0, len(tags))
	for arn := range tags {
		arns = append(arns, arn)
	}
	return arns
}

// TestTaggingEverTagged_ANeverTaggedResourceIsAbsent is the first of AWS's two rules: "GetResources
// does not return untagged resources".
//
// A tagged table is created alongside, because "the untagged ones are absent" would pass just as well
// against a scanner that reported nothing at all — which is how this rule could be satisfied for the
// wrong reason.
func TestTaggingEverTagged_ANeverTaggedResourceIsAbsent(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	tagged := everTaggedTable(t, ts, "ever-tagged", map[string]string{"env": "prod"})
	untagged := everTaggedTable(t, ts, "never-tagged", nil)
	everTaggedBucket(t, ts, "never-tagged-bucket")

	reported := everTaggedReported(t, ts, map[string]any{})
	assert.Contains(t, reported, tagged, "the tagged table is reported")
	assert.NotContains(t, reported, untagged, "the never-tagged table is not")
	assert.Emptyf(t, scanScopeMatches(reported, "never-tagged-bucket"),
		"nor the never-tagged bucket: %v", reported)
}

// TestTaggingEverTagged_AnEmptiedTagSetIsReportedAsAnEmptyList is the second rule, and the one that
// needs state the tags do not carry: "Resources that were previously tagged, but do not currently have
// tags, are shown with an empty tag set, like this: "Tags": []".
//
// The assertion is on the raw member rather than on a decoded slice, because null and [] decode alike
// and only one of them is what AWS publishes.
func TestTaggingEverTagged_AnEmptiedTagSetIsReportedAsAnEmptyList(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	arn := everTaggedTable(t, ts, "emptied", nil)
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})
	untagResourcesWith(t, ts, arn, "env")

	tags, _ := everTaggedRawTags(t, ts, map[string]any{})
	require.Containsf(t, tags, arn, "a previously tagged resource is still reported: %v", tags)
	assert.Equal(t, "[]", tags[arn], "with an empty tag set, rendered as [] rather than null")
}

// TestTaggingEverTagged_AnEmptiedTagSetIsAbsentUnderAnyTagFilter holds the other half of the same
// sentence: the empty-set rendering applies only when no TagFilter is given.
//
// The filter naming the key the resource used to carry is the interesting case — a caller asking for
// env=prod must not be handed a resource whose env tag was removed, even though substrate now
// remembers that it once had one.
func TestTaggingEverTagged_AnEmptiedTagSetIsAbsentUnderAnyTagFilter(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	arn := everTaggedTable(t, ts, "emptied", nil)
	held := everTaggedTable(t, ts, "still-tagged", nil)
	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})
	tagResourcesWith(t, ts, held, map[string]string{"env": "prod"})
	untagResourcesWith(t, ts, arn, "env")

	for _, tc := range []struct {
		name   string
		filter map[string]any
	}{
		{"the key it used to carry", map[string]any{"Key": "env"}},
		{"that key and its old value", map[string]any{"Key": "env", "Values": []string{"prod"}}},
		{"a key it never carried", map[string]any{"Key": "team"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reported := everTaggedReported(t, ts, map[string]any{
				"TagFilters": []map[string]any{tc.filter},
			})
			assert.NotContainsf(t, reported, arn,
				"a previously tagged resource is absent under a TagFilter: %v", reported)
		})
	}

	// The control, which is why the three assertions above measure the filter rather than an empty
	// scan: the table that still holds env=prod is reported for the first two filters.
	reported := everTaggedReported(t, ts, map[string]any{
		"TagFilters": []map[string]any{{"Key": "env", "Values": []string{"prod"}}},
	})
	assert.Equal(t, []string{held}, reported, "the resource that still holds the tag is reported")
}

// TestTaggingEverTagged_TheOwningServicesOwnRemovalCounts is the second writer direction.
//
// S3 is the case that has to be asserted rather than assumed: PutBucketTagging and
// DeleteBucketTagging replace a bucket's whole tag set rather than merging into it, so each is a
// writer that would leave the flag unwritten if it did not read the set it is replacing. That is the
// one gap the tagging API's shared merge helpers do not cover for it.
func TestTaggingEverTagged_TheOwningServicesOwnRemovalCounts(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	const bucket = "ever-tagged-bucket"
	everTaggedBucket(t, ts, bucket)
	everTaggedPutBucketTagging(t, ts, bucket, "env", "prod")

	arns := everTaggedReported(t, ts, map[string]any{})
	matches := scanScopeMatches(arns, bucket)
	require.Lenf(t, matches, 1, "the tagged bucket is reported: %v", arns)

	everTaggedDeleteBucketTagging(t, ts, bucket)

	tags, _ := everTaggedRawTags(t, ts, map[string]any{})
	require.Containsf(t, tags, matches[0],
		"the bucket is still reported after its whole tag set was deleted: %v", tags)
	assert.Equal(t, "[]", tags[matches[0]], "with an empty tag set")
}

// TestTaggingEverTagged_ATagWrittenAtCreationCounts records the design decision that a
// creation-with-tags path writes no flag and does not need to.
//
// The flag is read only when the tag set is empty, so a resource holding a tag is reported for
// holding it; the first writer that empties the set is the one that has to remember, because it is
// looking at the set it is about to empty. A table tagged at creation and then untagged is therefore
// the case that would fail if that reasoning were wrong.
func TestTaggingEverTagged_ATagWrittenAtCreationCounts(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	arn := everTaggedTable(t, ts, "tagged-at-creation", map[string]string{"env": "prod"})

	require.Contains(t, everTaggedReported(t, ts, map[string]any{}), arn,
		"a resource tagged at creation is reported")

	untagResourcesWith(t, ts, arn, "env")

	tags, _ := everTaggedRawTags(t, ts, map[string]any{})
	require.Containsf(t, tags, arn, "and still reported once the tag is removed: %v", tags)
	assert.Equal(t, "[]", tags[arn], "with an empty tag set")
}

// TestTaggingEverTagged_APageIsNotUnderFilledByTheNeverTaggedFilter is why the rule is applied inside
// the scan rather than over its result.
//
// A filter applied after pagination cuts a page from a list that still holds never-tagged records, so
// a caller asking for one resource per page is handed a short page — or an empty one — for a reason
// they cannot see. Walking the cursor at ResourcesPerPage=1 over five tagged tables interleaved with
// four never-tagged ones is the case that catches it: the ARNs are sorted, so the untagged ones fall
// between the tagged ones rather than after them.
func TestTaggingEverTagged_APageIsNotUnderFilledByTheNeverTaggedFilter(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	var want []string
	for _, name := range []string{"a", "c", "e", "g", "i"} {
		want = append(want, everTaggedTable(t, ts, "page-"+name, map[string]string{"env": "prod"}))
	}
	for _, name := range []string{"b", "d", "f", "h"} {
		everTaggedTable(t, ts, "page-"+name, nil)
	}

	var got []string
	token := ""
	for page := 1; ; page++ {
		body := map[string]any{
			"ResourcesPerPage":    1,
			"ResourceTypeFilters": []string{"dynamodb:table"},
		}
		if token != "" {
			body["PaginationToken"] = token
		}
		tags, next := everTaggedRawTags(t, ts, body)
		require.Lenf(t, tags, 1, "page %d holds exactly one resource, not a filtered-out remainder", page)
		for arn := range tags {
			got = append(got, arn)
		}
		if next == "" {
			break
		}
		token = next
		require.LessOrEqual(t, page, len(want), "the walk terminates rather than repeating a page")
	}

	assert.ElementsMatch(t, want, got, "every tagged table is reported exactly once across the walk")
}

// TestTaggingEverTagged_AnOverwriteDoesNotLoseTheFlag pins the one writer that rebuilds a record
// instead of editing it.
//
// PutParameter constructs a whole new SSMParameter, so an overwrite carrying no tags would write a
// record with the flag zeroed and make a previously tagged parameter never-tagged again. The order
// here is the one that reaches it: tag, remove the tag through Systems Manager's own operation so the
// stored set is empty, then overwrite. An overwrite before the removal proves nothing, because
// putParameter carries a non-empty stored set forward on its own.
func TestTaggingEverTagged_AnOverwriteDoesNotLoseTheFlag(t *testing.T) {
	t.Parallel()
	ts := ssmTagServer(t)

	const name = "/ever/tagged"
	arn := ssmPutParameter(t, ts, name, map[string]string{"env": "prod"})

	status, code := ssmRemoveTags(t, ts, ssmTagTarget, "Parameter", name, "env")
	require.Empty(t, code, "RemoveTagsFromResource")
	require.Equal(t, http.StatusOK, status, "RemoveTagsFromResource")

	// Overwrite: true rather than ssmPutParameterIn, which omits the member and would answer
	// ParameterAlreadyExists here. The overwrite is the whole subject.
	status, _, code = ssmRawCall(t, ts, ssmTagTarget, "PutParameter", map[string]any{
		"Name":      name,
		"Value":     "overwritten",
		"Type":      "String",
		"Overwrite": true,
	})
	require.Empty(t, code, "PutParameter overwrite")
	require.Equal(t, http.StatusOK, status, "PutParameter overwrite")

	tags, _ := everTaggedRawTags(t, ts, map[string]any{})
	require.Containsf(t, tags, arn, "the overwritten parameter is still reported: %v", tags)
	assert.Equal(t, "[]", tags[arn], "with an empty tag set")
}
