package emulator_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListTagsForStream's cursor — #954.
//
// The operation publishes both halves of a cursor over the tag key and substrate implemented neither:
// Limit and ExclusiveStartTagKey were decoded by nothing, and HasMoreTags was the literal false on
// every call. A consumer's paging loop therefore read "there is nothing more" from a response that had
// never looked, which is the same defect shape as a token substrate never issued (#915).
//
// Two things follow for how this is tested, and a single-page assertion makes neither:
//
//  1. **The walk is the assertion, not the page.** A cursor is correct when repeated calls partition
//     the set — every tag reported exactly once, none repeated, none omitted — and that is a property
//     of the sequence of pages. So the tests below walk to exhaustion and compare the concatenation
//     with the whole sorted set, at every Limit from 1 up to and past the size of the set. A test that
//     read one page could pass against a cursor that dropped a tag at every boundary.
//
//  2. **Termination is part of correctness.** HasMoreTags is what stops the loop, so a walk that never
//     reported false would hang rather than fail; each walk carries a runaway guard and each case
//     asserts the final page reports false. This is also where substrate reads AWS against itself — see
//     [TestKinesisListTags_HasMoreTagsIsTrueExactlyWhenTagsWereWithheld].
//
// Every call goes over the wire and every tag is written with AddTagsToStream, per #765: a helper that
// wrote a stream's tag map into state directly could not show that the cursor pages what the owning
// service's own tag call stored.

// kinesisTagTargetPrefix is the X-Amz-Target prefix Kinesis's JSON-1.1 protocol carries.
const kinesisTagTargetPrefix = "Kinesis_20131202"

// kinesisTagKeysWritten is the six keys each case below writes.
//
// They are already in sorted order, which is deliberate: the expectation is spelled out rather than
// computed with the same comparison the code under test uses, so a broken sort cannot agree with a
// broken expectation. That the response is sorted at all is #946's; this file is about which tags a
// page contains, given that order.
var kinesisTagKeysWritten = []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}

// kinesisTagPage is one ListTagsForStream response.
type kinesisTagPage struct {
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
	HasMoreTags bool `json:"HasMoreTags"`
}

// kinesisTagKeys is the keys a page reported, in the order it reported them.
func kinesisTagKeys(page kinesisTagPage) []string {
	keys := make([]string, 0, len(page.Tags))
	for _, tag := range page.Tags {
		keys = append(keys, tag.Key)
	}
	return keys
}

// kinesisTagStream starts a server, creates one stream and tags it with [kinesisTagKeysWritten].
//
// Each value is derived from its key so a page reporting the right keys with shuffled values cannot
// pass, and the tags go on through AddTagsToStream rather than into state.
func kinesisTagStream(t *testing.T, stream string) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))

	scanScopeJSON(t, ts, taggingTestAccount, "kinesis", scanScopeEast, kinesisTagTargetPrefix,
		"CreateStream", map[string]any{"StreamName": stream, "ShardCount": 1}, nil)

	tags := make(map[string]string, len(kinesisTagKeysWritten))
	for _, key := range kinesisTagKeysWritten {
		tags[key] = "v-" + key
	}
	scanScopeJSON(t, ts, taggingTestAccount, "kinesis", scanScopeEast, kinesisTagTargetPrefix,
		"AddTagsToStream", map[string]any{"StreamName": stream, "Tags": tags}, nil)

	return ts
}

// kinesisListTags posts one ListTagsForStream and returns the status, the decoded page and the error
// code, which is empty on success.
//
// The code comes out of the body's "__type" member, which is where a JSON-1.1 error carries it, with
// the shape namespace stripped.
func kinesisListTags(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, kinesisTagPage, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err, "marshal ListTagsForStream body")

	raw, status := scanScopeSignedTarget(t, ts, taggingTestAccount,
		"kinesis."+scanScopeEast+".amazonaws.com", "kinesis", scanScopeEast, kinesisTagTargetPrefix,
		"ListTagsForStream", data)

	if status != http.StatusOK {
		var doc map[string]any
		require.NoErrorf(t, json.Unmarshal(raw, &doc), "decode error body: %s", raw)
		code, _ := doc["__type"].(string)
		if i := strings.LastIndex(code, "#"); i >= 0 {
			code = code[i+1:]
		}
		return status, kinesisTagPage{}, code
	}

	var page kinesisTagPage
	require.NoErrorf(t, json.Unmarshal(raw, &page), "decode ListTagsForStream page: %s", raw)
	return status, page, ""
}

// kinesisWalkTags pages a stream's tags to exhaustion at one Limit and returns every key it was
// reported, in order, including any repeat.
//
// Repeats are returned rather than deduplicated on purpose: the caller compares against the whole
// expected set, so a cursor that reported one tag twice fails there rather than being hidden here. The
// walk is the loop AWS itself describes — "to list additional tags, set ExclusiveStartTagKey to the
// last key in the response" — and the page cap guards against a runaway HasMoreTags.
func kinesisWalkTags(t *testing.T, ts *emulator.TestServer, stream string, limit int) []string {
	t.Helper()

	var seen []string
	start := ""
	for page := 1; page <= len(kinesisTagKeysWritten)+2; page++ {
		body := map[string]any{"StreamName": stream, "Limit": limit}
		if start != "" {
			body["ExclusiveStartTagKey"] = start
		}

		status, out, code := kinesisListTags(t, ts, body)
		require.Emptyf(t, code, "page %d at Limit %d", page, limit)
		require.Equal(t, http.StatusOK, status)

		keys := kinesisTagKeys(out)
		require.LessOrEqualf(t, len(keys), limit, "page %d returned more than Limit %d", page, limit)
		seen = append(seen, keys...)

		if !out.HasMoreTags {
			return seen
		}
		require.NotEmptyf(t, keys, "page %d reports more tags but returned none, so the walk "+
			"cannot advance", page)
		start = keys[len(keys)-1]
	}

	require.Failf(t, "the walk did not terminate",
		"HasMoreTags stayed true past every tag at Limit %d, reporting %v", limit, seen)
	return seen
}

func TestKinesisListTags_AWalkAtEveryLimitReportsEveryTagExactlyOnce(t *testing.T) {
	t.Parallel()

	// Every Limit from one tag per page to more than the whole set. Limit 2 is the case #954 names, and 1,
	// 2, 3 and 6 divide six exactly, so the final page is *full* and must still report false — what a
	// "the page filled up, so there must be more" test gets wrong. 4 and 5 leave a partial final page,
	// and 50 is the published maximum, larger than the set, so it is a single page.
	for _, limit := range []int{1, 2, 3, 4, 5, 6, 50} {
		t.Run(kinesisLimitName(limit), func(t *testing.T) {
			t.Parallel()

			stream := "cursor-limit-" + kinesisLimitName(limit)
			ts := kinesisTagStream(t, stream)

			assert.Equal(t, kinesisTagKeysWritten, kinesisWalkTags(t, ts, stream, limit),
				"a walk at Limit %d partitions the tag set: every key exactly once, in order, with "+
					"no repeat and no omission", limit)
		})
	}
}

// kinesisLimitName renders a Limit as a subtest name, since a bare integer makes an opaque one.
func kinesisLimitName(limit int) string {
	return "Limit" + strconv.Itoa(limit)
}

func TestKinesisListTags_AnAbsentLimitReturnsEveryTag(t *testing.T) {
	t.Parallel()

	const stream = "cursor-no-limit"
	ts := kinesisTagStream(t, stream)

	// Limit is "Required: No", and the page states no default, so an absent Limit returns everything —
	// which is also the behavior every caller had before #954 and the reason this is the one path the
	// change does not alter.
	status, page, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	assert.Equal(t, kinesisTagKeysWritten, kinesisTagKeys(page), "an absent Limit withholds nothing")
	assert.False(t, page.HasMoreTags, "nothing was withheld, so nothing more is available")
}

func TestKinesisListTags_HasMoreTagsIsTrueExactlyWhenTagsWereWithheld(t *testing.T) {
	t.Parallel()

	const stream = "cursor-has-more"
	ts := kinesisTagStream(t, stream)

	// AWS's two sentences about HasMoreTags do not agree, and this is the assertion that records which
	// one substrate implements. Limit's says HasMoreTags is set "if this number is less than the total
	// number of tags associated with the stream" — literally, a Limit of 5 against six tags would report
	// true on the second page too, since 5 is still less than 6, and the walk AWS itself describes would
	// never terminate. HasMoreTags' own description is the coherent reading, "if set to true, more tags
	// are available", so it reports whether anything remains *after this page*.
	status, first, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream, "Limit": 5})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, kinesisTagKeysWritten[:5], kinesisTagKeys(first))
	assert.True(t, first.HasMoreTags, "one tag was withheld")

	_, second, code := kinesisListTags(t, ts, map[string]any{
		"StreamName": stream, "Limit": 5, "ExclusiveStartTagKey": "echo",
	})
	require.Empty(t, code)
	assert.Equal(t, kinesisTagKeysWritten[5:], kinesisTagKeys(second))
	assert.False(t, second.HasMoreTags,
		"the last page withheld nothing, although Limit 5 is still less than the stream's six tags — "+
			"the reading Limit's own sentence would forbid and the walk requires")

	// A Limit equal to the set is not a truncation either, which is the boundary the comparison sits on.
	_, exact, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream, "Limit": 6})
	require.Empty(t, code)
	assert.Equal(t, kinesisTagKeysWritten, kinesisTagKeys(exact))
	assert.False(t, exact.HasMoreTags, "a Limit equal to the tag count withholds nothing")
}

func TestKinesisListTags_ExclusiveStartTagKeyIsStrictlyExclusive(t *testing.T) {
	t.Parallel()

	const stream = "cursor-start-key"
	ts := kinesisTagStream(t, stream)

	// "Gets all tags that occur after ExclusiveStartTagKey" — after, so the named key is never in the
	// answer. Without that a walk would repeat its own cursor key on every page.
	cases := []struct {
		name  string
		start string
		want  []string
	}{
		{"a key that exists is excluded", "bravo", []string{"charlie", "delta", "echo", "foxtrot"}},
		{"the first key is excluded", "alpha", kinesisTagKeysWritten[1:]},
		// A key no tag has still positions the walk, since the cursor is over the key space rather than
		// over the tags: "bz" sorts between bravo and charlie.
		{"a key no tag has still positions the walk", "bz", []string{"charlie", "delta", "echo", "foxtrot"}},
		{"the last key ends the walk", "foxtrot", nil},
		{"a key after every tag reports none", "zulu", nil},
		// An empty string is what an omitted member decodes to and what a caller starting a walk sends,
		// so it is read as absent rather than as a violation of the published minimum length of 1.
		{"an empty key is read as absent", "", kinesisTagKeysWritten},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, page, code := kinesisListTags(t, ts, map[string]any{
				"StreamName": stream, "ExclusiveStartTagKey": tc.start,
			})
			require.Empty(t, code)
			require.Equal(t, http.StatusOK, status)

			if tc.want == nil {
				assert.Empty(t, kinesisTagKeys(page), "no tag sorts after %q", tc.start)
			} else {
				assert.Equal(t, tc.want, kinesisTagKeys(page))
			}
			assert.False(t, page.HasMoreTags, "no Limit was given, so nothing was withheld")
		})
	}
}

func TestKinesisListTags_ALimitOutsideOneToFiftyIsRefused(t *testing.T) {
	t.Parallel()

	const stream = "cursor-limit-range"
	ts := kinesisTagStream(t, stream)

	// Limit's published Valid Range is 1–50, and InvalidArgumentException/400 — one of this operation's
	// four published errors — describes exactly this: "a specified parameter exceeds its restrictions".
	// 0 is in the table because it is what an int-typed decode would turn an absent Limit into, so a
	// handler that collapsed the two would answer 200 here.
	for _, limit := range []int{0, -1, 51, 1000} {
		t.Run(kinesisLimitName(limit), func(t *testing.T) {
			status, _, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream, "Limit": limit})
			assert.Equal(t, "InvalidArgumentException", code,
				"Limit %d is outside the published 1–50 range", limit)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}

	// Both ends of the range are accepted, without which the refusal above could be off by one.
	for _, limit := range []int{1, 50} {
		t.Run("accepted/"+kinesisLimitName(limit), func(t *testing.T) {
			status, page, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream, "Limit": limit})
			require.Empty(t, code, "Limit %d is inside the published range", limit)
			require.Equal(t, http.StatusOK, status)
			assert.NotEmpty(t, kinesisTagKeys(page))
		})
	}
}

func TestKinesisListTags_AnOverLongExclusiveStartTagKeyIsRefused(t *testing.T) {
	t.Parallel()

	const stream = "cursor-start-key-length"
	ts := kinesisTagStream(t, stream)

	// ExclusiveStartTagKey publishes a maximum length of 128, which is also the maximum length of a tag
	// key, so a longer value cannot name a tag at all. It is refused under the same
	// InvalidArgumentException rather than silently matching nothing, because "exceeds its restrictions"
	// is what the error reports and a silent empty answer would look like an exhausted walk.
	status, _, code := kinesisListTags(t, ts, map[string]any{
		"StreamName": stream, "ExclusiveStartTagKey": strings.Repeat("k", 129),
	})
	assert.Equal(t, "InvalidArgumentException", code)
	assert.Equal(t, http.StatusBadRequest, status)

	// 128 exactly is accepted, and reports nothing, since no tag key sorts after it here.
	status, page, code := kinesisListTags(t, ts, map[string]any{
		"StreamName": stream, "ExclusiveStartTagKey": strings.Repeat("k", 128),
	})
	require.Empty(t, code, "128 characters is the published maximum, not one past it")
	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, kinesisTagKeys(page))
}

func TestKinesisListTags_AStreamWithNoTagsReportsAnEmptyPage(t *testing.T) {
	t.Parallel()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const stream = "cursor-untagged"
	scanScopeJSON(t, ts, taggingTestAccount, "kinesis", scanScopeEast, kinesisTagTargetPrefix,
		"CreateStream", map[string]any{"StreamName": stream, "ShardCount": 1}, nil)

	// Tags publishes a minimum of 0 items, so an untagged stream is an empty list rather than an error,
	// and a Limit against it is not a truncation. The walk has to terminate immediately here or a
	// caller's loop would spin on a stream it has not tagged.
	status, page, code := kinesisListTags(t, ts, map[string]any{"StreamName": stream, "Limit": 2})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, kinesisTagKeys(page))
	assert.False(t, page.HasMoreTags, "there is nothing to withhold")
}
