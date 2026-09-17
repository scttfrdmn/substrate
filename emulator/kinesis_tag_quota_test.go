package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// How many tags a Kinesis stream may carry — #965.
//
// `API_AddTagsToStream` publishes two different limits inside one parameter entry: the `Tags` member's
// description says fifty twice, and the constraint lines beneath it say *"Map Entries: Maximum number
// of 200 items."* Substrate enforced neither, so a stream could hold three hundred tags and
// `ListTagsForStream` would answer an array longer than its own published maximum.
//
// Both are enforced, at different codes, and the tests below turn on the difference: 201 entries in one
// request is a **shape** violation (`InvalidArgumentException`, checked against the request alone) while
// fifty-one tags on the stream is a **quota** violation (`LimitExceededException`, checked against the
// merged result). A test that only asserted "too many tags is refused" would not notice the two being
// collapsed into one code.
//
// The quota being a property of the stream rather than of the request is the part a single oversized
// request cannot prove, so the accumulation cases are the load-bearing ones: thirty tags then thirty
// more, and a refused request read back through `ListTagsForStream` to show it wrote nothing.

const (
	// kinesisQuotaStream is the stream every case tags.
	kinesisQuotaStream = "quota-stream"

	// kinesisQuotaLimit is the published per-stream tag quota, spelled out here rather than imported
	// from the package under test so a change to the constant cannot silently change the expectation.
	kinesisQuotaLimit = 50

	// kinesisQuotaMapEntries is the published maximum number of entries in one AddTagsToStream request.
	kinesisQuotaMapEntries = 200
)

// kinesisQuotaServer starts a server and creates the stream every case in this file tags.
func kinesisQuotaServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := kinesisARNServer(t)
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisQuotaStream, 1)
	return ts
}

// kinesisQuotaTags builds n tags whose keys are prefix-0000 … prefix-nnnn, so two calls with different
// prefixes add disjoint keys and two with the same prefix overlap exactly.
func kinesisQuotaTags(prefix string, n int) map[string]any {
	tags := make(map[string]any, n)
	for i := range n {
		tags[fmt.Sprintf("%s-%04d", prefix, i)] = "v"
	}
	return tags
}

// kinesisQuotaAdd posts one AddTagsToStream and returns the status and the error code.
func kinesisQuotaAdd(t *testing.T, ts *emulator.TestServer, tags map[string]any) (int, string) {
	t.Helper()
	status, code, _ := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Tags": tags})
	return status, code
}

// kinesisQuotaTagCount reports how many tags ListTagsForStream says the stream carries, read at the
// maximum Limit and asserted not to be truncated — so the count is the whole set rather than a page.
//
// Read back through the owning service's own operation rather than out of state, per #765: a helper
// that inspected the record could not show that a refused request left the *observable* tag set alone.
func kinesisQuotaTagCount(t *testing.T, ts *emulator.TestServer) int {
	t.Helper()
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListTagsForStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Limit": kinesisQuotaLimit})
	var page struct {
		Tags        []struct{ Key, Value string } `json:"Tags"`
		HasMoreTags bool                          `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &page), raw)
	require.Falsef(t, page.HasMoreTags,
		"the stream holds more than the quota, so the count below would be a page rather than the set: %s", raw)
	return len(page.Tags)
}

// TestKinesisTagQuota_UpToFiftyTagsIsAccepted holds the lower side of the boundary, so the refusal
// cannot be off by one.
func TestKinesisTagQuota_UpToFiftyTagsIsAccepted(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaLimit))
	require.Equalf(t, http.StatusOK, status, "%d tags answered %s", kinesisQuotaLimit, code)
	assert.Equal(t, kinesisQuotaLimit, kinesisQuotaTagCount(t, ts))
}

// TestKinesisTagQuota_TheFiftyFirstTagIsRefused is the upper side of the same boundary, and it is
// reached by **accumulation** — one accepted request of fifty and then a single further tag — which is
// what shows the limit is a function of the stream's state rather than of the request's size.
func TestKinesisTagQuota_TheFiftyFirstTagIsRefused(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaLimit))
	require.Equalf(t, http.StatusOK, status, "%d tags answered %s", kinesisQuotaLimit, code)

	status, code = kinesisQuotaAdd(t, ts, map[string]any{"one-too-many": "v"})
	assert.Equal(t, "LimitExceededException", code)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equalf(t, kinesisQuotaLimit, kinesisQuotaTagCount(t, ts),
		"the refused request wrote a tag anyway")
}

// TestKinesisTagQuota_TheLimitIsOverTheMergedSetNotOneRequest is the accumulation case in its plainest
// form: two requests, each well under the quota, whose sum is not.
func TestKinesisTagQuota_TheLimitIsOverTheMergedSetNotOneRequest(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("first", 30))
	require.Equalf(t, http.StatusOK, status, "the first 30 tags answered %s", code)

	status, code = kinesisQuotaAdd(t, ts, kinesisQuotaTags("second", 30))
	assert.Equal(t, "LimitExceededException", code, "60 tags across two requests was accepted")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equalf(t, 30, kinesisQuotaTagCount(t, ts),
		"a refused request wrote part of its tag set")
}

// TestKinesisTagQuota_ARefusedRequestWritesNothing is the all-or-nothing assertion #949 established the
// ordering for: the quota is checked before the merge, so none of a rejected request's tags land — not
// even the ones that would have fit.
func TestKinesisTagQuota_ARefusedRequestWritesNothing(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", 48))
	require.Equalf(t, http.StatusOK, status, "48 tags answered %s", code)

	// Five more against a stream with two slots left: a handler that merged first and checked after,
	// or that stopped at the limit, would leave two or five of these behind.
	status, code = kinesisQuotaAdd(t, ts, kinesisQuotaTags("late", 5))
	require.Equal(t, "LimitExceededException", code)
	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, 48, kinesisQuotaTagCount(t, ts))

	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListTagsForStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Limit": kinesisQuotaLimit})
	assert.NotContainsf(t, raw, "late-", "a tag from the refused request is readable: %s", raw)
}

// TestKinesisTagQuota_RewritingAnExistingTagAtTheQuotaSucceeds is the case the merged count exists to
// get right: AWS states that *"AddTagsToStream overwrites any existing tags that correspond to the
// specified tag keys"*, so re-tagging a full stream with a key it already carries is a rewrite rather
// than a fifty-first tag. Counting `len(existing) + len(request)` would refuse it.
func TestKinesisTagQuota_RewritingAnExistingTagAtTheQuotaSucceeds(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaLimit))
	require.Equalf(t, http.StatusOK, status, "%d tags answered %s", kinesisQuotaLimit, code)

	status, code = kinesisQuotaAdd(t, ts, map[string]any{"k-0000": "rewritten"})
	assert.Equalf(t, http.StatusOK, status, "rewriting an existing tag at the quota answered %s", code)
	assert.Equal(t, kinesisQuotaLimit, kinesisQuotaTagCount(t, ts))

	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListTagsForStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Limit": kinesisQuotaLimit})
	assert.Containsf(t, raw, "rewritten", "the rewrite was accepted but did not take: %s", raw)
}

// TestKinesisTagQuota_RemovingTagsFreesTheQuota shows the limit is read off current state rather than
// off anything the stream remembers about how many tags it has ever held.
func TestKinesisTagQuota_RemovingTagsFreesTheQuota(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaLimit))
	require.Equalf(t, http.StatusOK, status, "%d tags answered %s", kinesisQuotaLimit, code)

	status, code = kinesisQuotaAdd(t, ts, map[string]any{"extra": "v"})
	require.Equalf(t, "LimitExceededException", code, "status %d", status)

	kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "RemoveTagsFromStream",
		map[string]any{"StreamName": kinesisQuotaStream, "TagKeys": []string{"k-0000", "k-0001"}})
	require.Equal(t, kinesisQuotaLimit-2, kinesisQuotaTagCount(t, ts))

	status, code = kinesisQuotaAdd(t, ts, map[string]any{"extra": "v", "another": "v"})
	assert.Equalf(t, http.StatusOK, status, "two freed slots did not admit two tags: %s", code)
	assert.Equal(t, kinesisQuotaLimit, kinesisQuotaTagCount(t, ts))
}

// TestKinesisTagQuota_MoreThanTwoHundredEntriesIsAShapeRefusal keeps the two published numbers apart.
//
// 201 entries violates the request's own `Map Entries` bound, which is a different failure from a full
// stream and answers a different code — and it does so on an **empty** stream, where no quota is in
// play at all, so the two cannot be confused.
func TestKinesisTagQuota_MoreThanTwoHundredEntriesIsAShapeRefusal(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaMapEntries+1))
	assert.Equalf(t, "InvalidArgumentException", code,
		"%d entries is a shape violation, not a quota one", kinesisQuotaMapEntries+1)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Zero(t, kinesisQuotaTagCount(t, ts))
}

// TestKinesisTagQuota_BetweenFiftyOneAndTwoHundredIsTheQuotaCode is the other side of the same
// distinction: a request the shape admits but the quota does not.
func TestKinesisTagQuota_BetweenFiftyOneAndTwoHundredIsTheQuotaCode(t *testing.T) {
	t.Parallel()

	for _, n := range []int{kinesisQuotaLimit + 1, 100, kinesisQuotaMapEntries} {
		t.Run(fmt.Sprintf("%d entries", n), func(t *testing.T) {
			t.Parallel()
			ts := kinesisQuotaServer(t)

			status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", n))
			assert.Equalf(t, "LimitExceededException", code,
				"%d entries is within the 200-entry shape bound and over the 50-tag quota", n)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Zero(t, kinesisQuotaTagCount(t, ts))
		})
	}
}

// TestKinesisTagQuota_TheRefusalNamesWhatItCounted covers the criterion that a caller can act on the
// message: which stream, how many tags it holds, how many the request adds, and the limit.
func TestKinesisTagQuota_TheRefusalNamesWhatItCounted(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code := kinesisQuotaAdd(t, ts, kinesisQuotaTags("k", kinesisQuotaLimit))
	require.Equalf(t, http.StatusOK, status, "%d tags answered %s", kinesisQuotaLimit, code)

	_, _, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Tags": map[string]any{"extra": "v"}})
	assert.Contains(t, raw, kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, kinesisQuotaStream),
		raw)
	assert.Contains(t, raw, "50", raw)
}

// TestKinesisTagQuota_ATagsMemberOutsideItsPublishedLengths is refused per constraint, each under the
// code whose description is *"a specified parameter exceeds its restrictions"*.
//
// The zero-length **key** and the zero-length **value** are the pair worth keeping apart: AWS's own
// wording is that *"a tag consists of a required key and an optional value"*, and the published
// minimums say the same — key 1, value 0 — so an empty value must be accepted where an empty key is
// not.
func TestKinesisTagQuota_ATagsMemberOutsideItsPublishedLengths(t *testing.T) {
	t.Parallel()

	longKey := strings.Repeat("k", 129)
	longValue := strings.Repeat("v", 257)

	cases := map[string]struct {
		tags map[string]any
		code string
	}{
		"a key of 129 characters":   {tags: map[string]any{longKey: "v"}, code: "InvalidArgumentException"},
		"a key of 128 characters":   {tags: map[string]any{longKey[:128]: "v"}},
		"an empty key":              {tags: map[string]any{"": "v"}, code: "InvalidArgumentException"},
		"a value of 257 characters": {tags: map[string]any{"k": longValue}, code: "InvalidArgumentException"},
		"a value of 256 characters": {tags: map[string]any{"k": longValue[:256]}},
		"an empty value":            {tags: map[string]any{"k": ""}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := kinesisQuotaServer(t)

			status, code := kinesisQuotaAdd(t, ts, tc.tags)
			if tc.code == "" {
				assert.Equalf(t, http.StatusOK, status, "answered %s", code)
				assert.Equal(t, 1, kinesisQuotaTagCount(t, ts))
				return
			}
			assert.Equal(t, tc.code, code)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Zero(t, kinesisQuotaTagCount(t, ts))
		})
	}
}

// TestKinesisTagQuota_AnAbsentTagsMemberIsRefusedAndAnEmptyOneIsANoOp records the pair AWS's shape
// distinguishes and its prose does not.
//
// `Tags` is the operation's one Required: Yes member other than the stream reference, so an absent one
// is refused. An **empty** map is accepted as a no-op, which is **substrate's reading**: the member
// publishes a maximum entry count and no minimum, where the sibling `RemoveTagsFromStream`'s `TagKeys`
// publishes *"Minimum number of 1 item"* — AWS states a minimum where it means one, and reading one in
// here anyway would refuse a request the shape admits.
func TestKinesisTagQuota_AnAbsentTagsMemberIsRefusedAndAnEmptyOneIsANoOp(t *testing.T) {
	t.Parallel()
	ts := kinesisQuotaServer(t)

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": kinesisQuotaStream})
	assert.Equal(t, "InvalidArgumentException", code, raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)

	status, code = kinesisQuotaAdd(t, ts, map[string]any{})
	assert.Equalf(t, http.StatusOK, status, "an empty Tags map answered %s", code)
	assert.Zero(t, kinesisQuotaTagCount(t, ts))
}

// TestKinesisTagQuota_TheShapeIsCheckedBeforeTheStreamIsLooked keeps this operation's refusal ordering
// the same as ListTagsForStream's, where a parameter violation does not depend on the stream existing.
//
// The reverse ordering is also asserted, and it is the one that could go wrong silently: a *valid*
// request against an absent stream must still answer ResourceNotFoundException rather than being
// admitted because its tags were fine.
func TestKinesisTagQuota_TheShapeIsCheckedBeforeTheStreamIsLooked(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": "no-such-stream", "Tags": map[string]any{strings.Repeat("k", 129): "v"}})
	assert.Equalf(t, "InvalidArgumentException", code,
		"an over-long key against an absent stream answered for the stream instead: %s", raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)

	status, code, raw = kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": "no-such-stream", "Tags": map[string]any{"k": "v"}})
	assert.Equal(t, "ResourceNotFoundException", code, raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)
}

// TestKinesisTagQuota_TheQuotaFollowsTheARNsAccount is the #966 guarantee applied to the new refusal:
// the tags counted are the ARN's stream's, not the caller's own same-named one's.
//
// The caller's own stream is left empty and the ARN's is filled to the quota, so a quota read from the
// wrong record would answer 200 where this requires a refusal.
func TestKinesisTagQuota_TheQuotaFollowsTheARNsAccount(t *testing.T) {
	t.Parallel()
	ts := kinesisARNServer(t)

	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, kinesisQuotaStream, 1)
	kinesisARNCreate(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, kinesisQuotaStream, 1)
	foreign := kinesisStreamARNFor(kinesisARNOtherAccount, kinesisARNEastRegion, kinesisQuotaStream)

	full := kinesisQuotaTags("k", kinesisQuotaLimit)
	kinesisARNOK(t, ts, kinesisARNOtherAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamName": kinesisQuotaStream, "Tags": full})

	status, code, raw := kinesisARNCall(t, ts, taggingTestAccount, kinesisARNEastRegion, "AddTagsToStream",
		map[string]any{"StreamARN": foreign, "Tags": map[string]any{"extra": "v"}})
	assert.Equalf(t, "LimitExceededException", code,
		"the quota was counted against the caller's own empty stream: %s", raw)
	assert.Equal(t, http.StatusBadRequest, status, raw)

	// The caller's own stream is empty, so the same tag by name succeeds — which is what shows the
	// refusal above came from the ARN's record rather than from a limit applied to the wrong one.
	status, code = kinesisQuotaAdd(t, ts, map[string]any{"extra": "v"})
	assert.Equalf(t, http.StatusOK, status, "the caller's own empty stream refused one tag: %s", code)
}
