package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1004 and #1010: GetResources honors all eight of its published request members.
//
// Four were decoded and four were dropped, and of the four decoded, two were not honored as
// published either — `ResourcesPerPage` was clamped where AWS refuses, and `PaginationToken`
// discarded both of its decode errors so a token substrate never issued meant page one.
//
// `ResourceARNList` is the member these assertions spend the most on, because it is the one whose
// absence was not merely an omission: the page publishes it as mutually exclusive with three other
// members, so substrate accepted three request shapes AWS refuses and served the account-wide scan
// for all three. A caller asking for the tags on five ARNs got a superset at HTTP 200.
//
// Everything goes over the wire (#765) and every refusal asserts the status alongside the code
// (#923). `InvalidParameterException`/400 is the operation's only published code for a bad request,
// so the *message* is what distinguishes these refusals from each other, and it is asserted wherever
// two of them could otherwise be confused.

// taggingServer pairs the tagging test server with the state manager that backs it, so a test can
// create a resource and then call the API against it without carrying two values through.
type taggingServer struct {
	srv   *httptest.Server
	state emulator.StateManager
}

// newTaggingServer builds the server of newTaggingTestServer in the paired form.
func newTaggingServer(t *testing.T) *taggingServer {
	t.Helper()
	srv, state := newTaggingTestServer(t)
	return &taggingServer{srv: srv, state: state}
}

// taggingBucketName names the nth test bucket.
//
// The names are numbered rather than lettered so that lexicographic order — which is the order
// GetResources sorts by, and therefore the order an offset token is an offset into — matches the
// order they were created in. That keeps a paging assertion readable for fewer than ten buckets.
func taggingBucketName(n int) string {
	return fmt.Sprintf("tagging-bucket-%d", n)
}

// taggingPutBuckets creates n tagged S3 buckets.
//
// Each carries one tag, because GetResources does not report a resource that never had one (#938)
// and a bucket with an empty tag map would simply be absent from every assertion below. One tag also
// makes each bucket cost exactly one against a TagsPerPage budget.
func taggingPutBuckets(t *testing.T, ts *taggingServer, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		name := taggingBucketName(i)
		putTestS3Bucket(t, ts.state, name, map[string]string{"Name": name})
	}
}

// taggingPutBucketWithTags creates an S3 bucket carrying exactly tagCount tags.
func taggingPutBucketWithTags(t *testing.T, ts *taggingServer, name string, tagCount int) {
	t.Helper()
	tags := make(map[string]string, tagCount)
	for i := 0; i < tagCount; i++ {
		tags[fmt.Sprintf("key-%03d", i)] = "v"
	}
	putTestS3Bucket(t, ts.state, name, tags)
}

// taggingEverTagged drives a bucket to the previously-tagged state, which is the only state in which
// GetResources reports a resource whose tag set is empty (#938).
func taggingEverTagged(t *testing.T, ts *taggingServer, name string) {
	t.Helper()
	arn := taggingARNOfBucket(name)
	for _, call := range []struct {
		op   string
		body map[string]any
	}{
		{"TagResources", map[string]any{
			"ResourceARNList": []string{arn},
			"Tags":            map[string]string{"Team": "alpha"},
		}},
		{"UntagResources", map[string]any{
			"ResourceARNList": []string{arn},
			"TagKeys":         []string{"Team"},
		}},
	} {
		resp := taggingRequest(t, ts.srv, call.op, call.body)
		require.Equal(t, http.StatusOK, resp.StatusCode, call.op)
		resp.Body.Close() //nolint:errcheck
	}
}

// taggingARNOfBucket renders the ARN GetResources reports for a bucket.
//
// An S3 bucket ARN carries neither an account nor a Region segment, which is stated at
// scanS3Buckets. That matters here because ResourceARNList matches exactly, so a test that guessed
// the usual six-segment shape would assert an empty result and pass for the wrong reason.
func taggingARNOfBucket(name string) string {
	return "arn:aws:s3:::" + name
}

// taggingGetResources posts a GetResources request, requires success, and returns the decoded body.
func taggingGetResources(t *testing.T, ts *taggingServer, body map[string]any) map[string]any {
	t.Helper()
	resp := taggingRequest(t, ts.srv, "GetResources", body)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, "GetResources %v", body)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// taggingGetResourcesRefusal posts a GetResources request and returns the status, the error code and
// the message.
func taggingGetResourcesRefusal(t *testing.T, ts *taggingServer, body map[string]any) (int, string, string) {
	t.Helper()
	resp := taggingRequest(t, ts.srv, "GetResources", body)
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, awsErrorCode(out.Type), out.Message
}

// taggingARNsOf reports the ResourceARN of every mapping in a GetResources response, in order.
func taggingARNsOf(t *testing.T, out map[string]any) []string {
	t.Helper()
	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok, "ResourceTagMappingList is an array: %v", out)
	arns := make([]string, 0, len(list))
	for _, item := range list {
		rm, ok := item.(map[string]any)
		require.True(t, ok, "each mapping is an object")
		arn, ok := rm["ResourceARN"].(string)
		require.True(t, ok, "each mapping carries a ResourceARN")
		arns = append(arns, arn)
	}
	return arns
}

// taggingFirstMapping returns the first mapping of a GetResources response as raw JSON.
//
// Raw rather than decoded, because two of the assertions below are about a member that is *absent*,
// which a decoded struct cannot express — the same reason iam_shape_members_test.go reads raw JSON.
func taggingFirstMapping(t *testing.T, out map[string]any) map[string]any {
	t.Helper()
	list, ok := out["ResourceTagMappingList"].([]any)
	require.True(t, ok, "ResourceTagMappingList is an array: %v", out)
	require.NotEmpty(t, list, "at least one mapping")
	rm, ok := list[0].(map[string]any)
	require.True(t, ok, "each mapping is an object")
	return rm
}

// TestTaggingGetResources_ResourcesPerPageOutsideItsRangeIsRefused is #1004.
//
// The two out-of-range cases are the whole issue: `0` and `-5` were rewritten to 100, and `5000` was
// honored, so one call could return every resource in the account on a page size AWS refuses. The
// in-range cases are here because a range check written with the wrong comparison satisfies every
// refusal assertion and no acceptance one.
func TestTaggingGetResources_ResourcesPerPageOutsideItsRangeIsRefused(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 3)

	for _, perPage := range []int{0, -5, 101, 5000} {
		t.Run(fmt.Sprintf("%d", perPage), func(t *testing.T) {
			status, code, message := taggingGetResourcesRefusal(t, ts,
				map[string]any{"ResourcesPerPage": perPage})
			assert.Equal(t, "InvalidParameterException", code,
				"the operation's published code for a value out of range")
			assert.Equal(t, http.StatusBadRequest, status, "published at 400")
			assert.Contains(t, message, "ResourcesPerPage", "the message names the parameter")
			assert.Contains(t, message, "100", "and the published maximum")
		})
	}

	// Both ends of the published range are accepted, which is what a `<` written for a `<=` fails.
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"ResourcesPerPage": 1})), 1, "a page size of 1 is in range")
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"ResourcesPerPage": 100})), 3, "and so is 100")

	// An absent member is still the default, which is the case the old `<= 0` arm conflated with an
	// explicit zero and is the reason the field had to become a pointer.
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts, map[string]any{})), 3,
		"an absent ResourcesPerPage defaults to 100 and is not refused")
}

// TestTaggingGetResources_AnUnissuedPaginationTokenIsRefused is the token site folded into this PR.
//
// Both discarded errors are exercised — a token that is not base64, and one that is base64 but does
// not decode to an offset — plus the forms strconv.Atoi accepts that the encoder never emits, which
// is what decodeOffsetPaginationToken's round-trip rule exists to refuse. Before this change every
// one of them meant page one, so a consumer whose cursor handling was broken saw a working pager that
// silently looped.
func TestTaggingGetResources_AnUnissuedPaginationTokenIsRefused(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 3)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"not base64", "!!!not-base64!!!"},
		{"base64 of a word", "bm90LWEtbnVtYmVy"}, // "not-a-number"
		{"base64 of a negative offset", "LTE="},  // "-1"
		{"base64 of a padded offset", "MDU="},    // "05", which the encoder never emits
		{"base64 of a signed offset", "KzU="},    // "+5", likewise
		{"another service's cursor shape", "1"},  // EC2's decimal token, not this one
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := taggingGetResourcesRefusal(t, ts,
				map[string]any{"PaginationToken": tc.token})
			assert.Equal(t, "InvalidParameterException", code,
				"a malformed string parameter, which the code's own gloss names")
			assert.Equal(t, http.StatusBadRequest, status, "published at 400")
			assert.Contains(t, message, "PaginationToken", "the message names the parameter")
		})
	}

	// Over the published maximum length, which has its own message so a caller can tell a token that
	// is too long from one that is merely wrong.
	status, code, message := taggingGetResourcesRefusal(t, ts,
		map[string]any{"PaginationToken": strings.Repeat("A", 2049)})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, message, "2048", "the message names the published maximum")

	// A token this API issued still works, and an empty one is the initial request rather than a
	// refusal — the member's published minimum length is 0.
	first := taggingGetResources(t, ts, map[string]any{"ResourcesPerPage": 2})
	token, ok := first["PaginationToken"].(string)
	require.True(t, ok, "a partial result carries a token")
	require.NotEmpty(t, token, "three resources at a page size of two is a partial result")
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"ResourcesPerPage": 2, "PaginationToken": token})), 1, "the token resumes")
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"PaginationToken": ""})), 3, "an empty token is the first page")
}

// TestTaggingGetResources_PaginationTokenIsAlwaysPresent is #1010's response-side reading.
//
// `omitempty` meant a final page carried no `PaginationToken` member at all, so a consumer indexing
// `response["PaginationToken"]` rather than using a defaulting read raised against substrate and
// worked against AWS. Asserted on the *key set* rather than the value, since an empty string and an
// absent member decode to the same Go value through a map[string]any.
func TestTaggingGetResources_PaginationTokenIsAlwaysPresent(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 2)

	out := taggingGetResources(t, ts, map[string]any{})
	require.Contains(t, out, "PaginationToken",
		"AWS's own sample response emits the member on a complete result")
	assert.Equal(t, "", out["PaginationToken"], "empty, because this is the last page")

	// And in a resource-free account, which is the other shape a consumer's first call can see.
	out = taggingGetResources(t, newTaggingServer(t), map[string]any{})
	assert.Contains(t, out, "PaginationToken", "including when there is nothing to page")
}

// TestTaggingGetResources_ResourceARNListSelectsTheNamedResources is #1010's main assertion.
//
// Three things at once, because they are one behavior: the named resources come back, the unnamed
// ones do not, and an ARN that names nothing is not an error. The last is published verbatim — *"if
// a resource specified by this parameter doesn't exist, it doesn't generate an error; it simply isn't
// included in the response"* — and is the difference between this and a not-found refusal.
func TestTaggingGetResources_ResourceARNListSelectsTheNamedResources(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 4)

	out := taggingGetResources(t, ts, map[string]any{
		"ResourceARNList": []string{
			taggingARNOfBucket(taggingBucketName(1)),
			taggingARNOfBucket(taggingBucketName(3)),
		},
	})
	assert.Equal(t, []string{
		taggingARNOfBucket(taggingBucketName(1)),
		taggingARNOfBucket(taggingBucketName(3)),
	}, taggingARNsOf(t, out), "exactly the two named, and not the account-wide scan")

	// An ARN that names nothing is silently absent rather than an error, and does not suppress the
	// one that does exist.
	out = taggingGetResources(t, ts, map[string]any{
		"ResourceARNList": []string{
			taggingARNOfBucket(taggingBucketName(2)),
			taggingARNOfBucket("no-such-bucket-anywhere"),
		},
	})
	assert.Equal(t, []string{taggingARNOfBucket(taggingBucketName(2))}, taggingARNsOf(t, out),
		"the absent ARN is simply not included")

	// A list of nothing but absent ARNs is an empty result at 200, not a refusal.
	out = taggingGetResources(t, ts, map[string]any{
		"ResourceARNList": []string{
			taggingARNOfBucket("nope-one"), taggingARNOfBucket("nope-two"),
		},
	})
	assert.Empty(t, taggingARNsOf(t, out), "no matches is an empty list, not an error")

	// The match is exact. A prefix of a real ARN selects nothing, which is what a loose comparison
	// would get wrong in the direction that returns resources the caller did not name.
	out = taggingGetResources(t, ts, map[string]any{
		"ResourceARNList": []string{"arn:aws:s3:::tagging-bucket-"},
	})
	assert.Empty(t, taggingARNsOf(t, out), "a prefix is not an ARN")
}

// TestTaggingGetResources_ResourceARNListReturnsAPreviouslyTaggedResource pins the reading the page
// does not settle.
//
// #938 established that a resource which once held a tag is reported with `"Tags": []`. The page
// scopes that to the no-`TagFilters` case and says nothing about `ResourceARNList`, so this is
// substrate's reading, argued from the operation's own scoping sentence — *"GetResources does not
// return untagged resources"* — on the ground that a resource which ever held a tag is not untagged.
// It is asserted rather than left implicit because the opposite reading is equally implementable, and
// a future change should have to argue with a failing test rather than with a comment.
func TestTaggingGetResources_ResourceARNListReturnsAPreviouslyTaggedResource(t *testing.T) {
	ts := newTaggingServer(t)
	const name = "ever-tagged-bucket"
	putTestS3Bucket(t, ts.state, name, nil)
	taggingEverTagged(t, ts, name)
	arn := taggingARNOfBucket(name)

	out := taggingGetResources(t, ts, map[string]any{"ResourceARNList": []string{arn}})
	require.Equal(t, []string{arn}, taggingARNsOf(t, out),
		"a previously-tagged resource is eligible when named by ARN")

	tags, ok := taggingFirstMapping(t, out)["Tags"].([]any)
	require.True(t, ok, "Tags is an array, per #938")
	assert.Empty(t, tags, "and it is empty, which is how AWS reports the history")
}

// TestTaggingGetResources_ResourceARNListBoundsAreEnforced covers the member's published array and
// item constraints.
//
// The empty-array case is the one worth arguing: `ResourceARNList: []` is a *present* member whose
// array minimum is 1, so it is refused rather than treated as absent. A caller that sent the member
// asked to filter by ARN, and filtering by none of them is not the same request as the account-wide
// scan — which is exactly what substrate would otherwise answer.
func TestTaggingGetResources_ResourceARNListBoundsAreEnforced(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 2)

	status, code, message := taggingGetResourcesRefusal(t, ts,
		map[string]any{"ResourceARNList": []string{}})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, message, "ResourceARNList", "the message names the parameter")

	// 101 items, one past the published array maximum.
	tooMany := make([]string, 101)
	for i := range tooMany {
		tooMany[i] = taggingARNOfBucket(taggingBucketName(1))
	}
	status, code, message = taggingGetResourcesRefusal(t, ts,
		map[string]any{"ResourceARNList": tooMany})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, message, "100", "the message names the published maximum")

	// Exactly 100 is in range, which no over-the-limit case can demonstrate.
	exactly := make([]string, 100)
	for i := range exactly {
		exactly[i] = taggingARNOfBucket(taggingBucketName(1))
	}
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"ResourceARNList": exactly})), 1,
		"100 items is accepted, and naming one ARN a hundred times still selects it once")

	// An entry past the published 1011-character maximum, and an empty one, which is the other end.
	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"an empty entry", ""},
		{"an entry over 1011 characters", "arn:aws:s3:::" + strings.Repeat("b", 1011)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := taggingGetResourcesRefusal(t, ts,
				map[string]any{"ResourceARNList": []string{tc.arn}})
			assert.Equal(t, "InvalidParameterException", code)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, message, "1011", "the message names the published length range")
		})
	}
}

// TestTaggingGetResources_ResourceARNListIsMutuallyExclusive is #1010's three published refusals.
//
// Each has its own sentence on the page, and all three were accepted before this change with the
// parameter silently ignored — so a caller asking for two ARNs received the account-wide scan at
// HTTP 200 with nothing in the response to say so. The messages are asserted because all three share
// one code, and a caller that could not read the message could not tell which of its two parameters
// to remove.
func TestTaggingGetResources_ResourceARNListIsMutuallyExclusive(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 2)

	arn := taggingARNOfBucket(taggingBucketName(1))

	for _, tc := range []struct {
		name  string
		body  map[string]any
		other string
	}{
		{
			"with ResourceTypeFilters",
			map[string]any{"ResourceARNList": []string{arn}, "ResourceTypeFilters": []string{"s3"}},
			"ResourceTypeFilters",
		},
		{
			"with TagFilters",
			map[string]any{
				"ResourceARNList": []string{arn},
				"TagFilters":      []map[string]any{{"Key": "Name"}},
			},
			"TagFilters",
		},
		{
			"with ResourcesPerPage",
			map[string]any{"ResourceARNList": []string{arn}, "ResourcesPerPage": 10},
			"ResourcesPerPage",
		},
		{
			"with TagsPerPage",
			map[string]any{"ResourceARNList": []string{arn}, "TagsPerPage": 100},
			"TagsPerPage",
		},
		{
			"with PaginationToken",
			map[string]any{"ResourceARNList": []string{arn}, "PaginationToken": "MQ=="},
			"PaginationToken",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := taggingGetResourcesRefusal(t, ts, tc.body)
			assert.Equal(t, "InvalidParameterException", code,
				"the code the sentences' `Invalid Parameter` exception resolves to")
			assert.Equal(t, http.StatusBadRequest, status, "published at 400")
			assert.Contains(t, message, "ResourceARNList", "the message names both parameters")
			assert.Contains(t, message, tc.other, "the message names both parameters")
		})
	}

	// A pagination member sent as an explicit zero is still *sent*, which is the distinction the
	// pointer exists for: under the old `int` field this request was indistinguishable from one that
	// omitted the member, so the exclusion could not have been enforced at all.
	status, code, _ := taggingGetResourcesRefusal(t, ts,
		map[string]any{"ResourceARNList": []string{arn}, "ResourcesPerPage": 0})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestTaggingGetResources_AMemberIsCheckedBeforeTheRequestAsAWhole pins the validation order.
//
// No order avoids every two-round-trip case, so the choice is the one #991 and #983 already made:
// what is wrong with a single member is named before what is wrong with the request as a whole,
// because the first is fixable from that member's own documentation and the second makes the caller
// decide which of two features it wanted. A request that is both is the only way to see the order.
func TestTaggingGetResources_AMemberIsCheckedBeforeTheRequestAsAWhole(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 1)

	status, code, message := taggingGetResourcesRefusal(t, ts, map[string]any{
		"ResourceARNList":  []string{taggingARNOfBucket(taggingBucketName(1))},
		"ResourcesPerPage": 5000,
	})
	assert.Equal(t, "InvalidParameterException", code,
		"one code covers both, so only the message differs")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, message, "must be between",
		"the range violation is named, not the exclusion")

	// And the validation runs before the scan, which is why a refused request needs no resource to
	// exist: this account holds none and the refusal is the same.
	status, code, _ = taggingGetResourcesRefusal(t, newTaggingServer(t),
		map[string]any{"ResourcesPerPage": 5000})
	assert.Equal(t, "InvalidParameterException", code,
		"refused in an account with no resources at all")
	assert.Equal(t, http.StatusBadRequest, status)
}

// TestTaggingGetResources_TagsPerPageCutsThePageByTagCount is #1010's TagsPerPage decision.
//
// Implemented rather than refused, because the page publishes exact and deterministic behavior: the
// 100–500 range, *"a resource with no tags is counted as having one tag"*, *"does not split a
// resource and its associated tags across pages"*, and a worked example that pins the comparison as
// inclusive. The budget here is the minimum 100 against buckets of 25 tags each, so four resources
// fit exactly and the fifth starts a new page — the same arithmetic as AWS's own example (TagsPerPage
// 100 against 22 resources of 10 tags each, yielding 10, 10 and 2) at a smaller scale.
func TestTaggingGetResources_TagsPerPageCutsThePageByTagCount(t *testing.T) {
	ts := newTaggingServer(t)
	for i := 1; i <= 6; i++ {
		taggingPutBucketWithTags(t, ts, taggingBucketName(i), 25)
	}

	out := taggingGetResources(t, ts, map[string]any{"TagsPerPage": 100})
	assert.Len(t, taggingARNsOf(t, out), 4,
		"four resources of 25 tags is exactly the 100-tag budget, so the fifth waits")
	token, _ := out["PaginationToken"].(string)
	require.NotEmpty(t, token, "and a token is issued at the shorter boundary")

	// The token resumes where the tag budget stopped, not where ResourcesPerPage would have.
	out = taggingGetResources(t, ts, map[string]any{"TagsPerPage": 100, "PaginationToken": token})
	assert.Len(t, taggingARNsOf(t, out), 2, "the remaining two")
	assert.Equal(t, "", out["PaginationToken"], "and the listing is complete")

	// Both members apply when both are sent, the page breaking at whichever is reached first.
	out = taggingGetResources(t, ts, map[string]any{"TagsPerPage": 500, "ResourcesPerPage": 2})
	assert.Len(t, taggingARNsOf(t, out), 2, "ResourcesPerPage is the tighter limit here")
	out = taggingGetResources(t, ts, map[string]any{"TagsPerPage": 100, "ResourcesPerPage": 100})
	assert.Len(t, taggingARNsOf(t, out), 4, "and TagsPerPage is the tighter limit here")
}

// TestTaggingGetResources_AnUntaggedResourceCostsOneTag is the published counting rule.
//
// *"A resource with no tags is counted as having one tag (one key and value pair)."* The resource
// with no tags that GetResources still reports is the previously-tagged one #938 added, so this is
// where that form meets the tag budget. One bucket of 100 tags exhausts a 100-tag budget exactly; the
// empty-tag bucket that sorts after it therefore does not fit, where a cost of zero would have let it
// on to the same page. That is the whole distinction, in two resources.
func TestTaggingGetResources_AnUntaggedResourceCostsOneTag(t *testing.T) {
	ts := newTaggingServer(t)
	const full, bare = "tagbudget-a-full", "tagbudget-b-bare"
	taggingPutBucketWithTags(t, ts, full, 100)
	putTestS3Bucket(t, ts.state, bare, nil)
	taggingEverTagged(t, ts, bare)

	// Both are reported when no budget is set, which is what makes the cut below meaningful.
	require.Equal(t, []string{taggingARNOfBucket(full), taggingARNOfBucket(bare)},
		taggingARNsOf(t, taggingGetResources(t, ts, map[string]any{})),
		"both resources are in the listing, in ARN order")

	out := taggingGetResources(t, ts, map[string]any{"TagsPerPage": 100})
	assert.Equal(t, []string{taggingARNOfBucket(full)}, taggingARNsOf(t, out),
		"the empty-tag resource costs one tag, so it does not fit the exhausted budget")
	assert.NotEmpty(t, out["PaginationToken"], "and the page is cut, so a token resumes it")
}

// TestTaggingGetResources_TagsPerPageOutsideItsRangeIsRefused covers the member's published bounds.
//
// The minimum is 100 rather than 1, which is the surprise on this member and the reason the range is
// asserted from both ends: a value of 1 or 50 looks like a small page size and is out of range.
func TestTaggingGetResources_TagsPerPageOutsideItsRangeIsRefused(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 2)

	for _, n := range []int{0, 1, 99, 501} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			status, code, message := taggingGetResourcesRefusal(t, ts,
				map[string]any{"TagsPerPage": n})
			assert.Equal(t, "InvalidParameterException", code)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, message, "TagsPerPage", "the message names the parameter")
			assert.Contains(t, message, "100", "and the published minimum")
			assert.Contains(t, message, "500", "and the published maximum")
		})
	}

	// Both ends are accepted.
	for _, n := range []int{100, 500} {
		assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
			map[string]any{"TagsPerPage": n})), 2, "%d is in range", n)
	}
}

// TestTaggingGetResources_ComplianceDetailsAreReportedOnlyWhenAskedFor is #1010's compliance half.
//
// `ComplianceDetails` was `*struct{}`, so the only two documents it could produce were "absent" and
// `{}`, neither of which is the published shape. It is now the published type — minus
// `ComplianceStatus`, whose omission is the decision this test pins.
func TestTaggingGetResources_ComplianceDetailsAreReportedOnlyWhenAskedFor(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 1)

	// Absent by default: the member exists to be opted into.
	assert.NotContains(t, taggingFirstMapping(t, taggingGetResources(t, ts, map[string]any{})),
		"ComplianceDetails", "no ComplianceDetails unless IncludeComplianceDetails asked for it")

	out := taggingGetResources(t, ts, map[string]any{"IncludeComplianceDetails": true})
	rm := taggingFirstMapping(t, out)
	details, ok := rm["ComplianceDetails"].(map[string]any)
	require.True(t, ok, "ComplianceDetails is an object: %v", rm)

	// The three derivable members, each an empty array rather than null — every one is defined
	// against the effective tag policy, and substrate models no organization with one.
	for _, member := range []string{
		"KeysWithNoncompliantValues", "MissingTagKeys", "NoncompliantKeys",
	} {
		value, present := details[member]
		require.True(t, present, "%s is published in the Response Syntax", member)
		assert.Equal(t, []any{}, value,
			"%s is empty, not null — there is no policy for a key to violate", member)
	}

	// ComplianceStatus is omitted, and that is the decision rather than an oversight: the
	// Organizations guide says a tag outside the policy is not evaluated for compliance at all, so
	// reporting `true` would claim an evaluation that never happened.
	assert.NotContains(t, details, "ComplianceStatus",
		"substrate does not report a compliance verdict it did not reach")
}

// TestTaggingGetResources_ExcludeCompliantResourcesNeedsIncludeComplianceDetails is the one
// compliance constraint that needs no tag-policy model.
//
// *"You can use this parameter only if the `IncludeComplianceDetails` parameter is also set to
// `true`."* An explicit `false` is accepted, which is substrate's reading: it asks for nothing, and
// AWS's own Sample Request sends the member with a non-meaningful value.
func TestTaggingGetResources_ExcludeCompliantResourcesNeedsIncludeComplianceDetails(t *testing.T) {
	ts := newTaggingServer(t)
	taggingPutBuckets(t, ts, 2)

	status, code, message := taggingGetResourcesRefusal(t, ts,
		map[string]any{"ExcludeCompliantResources": true})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, message, "IncludeComplianceDetails",
		"the message names the parameter that has to accompany it")

	// With the companion set it is accepted — and excludes nothing, since no resource is evaluated as
	// compliant. Recorded in docs/services.md rather than left to be discovered.
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts, map[string]any{
		"ExcludeCompliantResources": true,
		"IncludeComplianceDetails":  true,
	})), 2, "nothing is excluded, because nothing is evaluated as compliant")

	// An explicit false with no companion is accepted.
	assert.Len(t, taggingARNsOf(t, taggingGetResources(t, ts,
		map[string]any{"ExcludeCompliantResources": false})), 2,
		"an explicit false asks for nothing and is not a use of the parameter")
}
