package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The shared cursor primitive (pagination.go): ordering, resuming strictly after the cursor under a
// listing that changes between pages, truncation, and refusing a token not issued for the request.

// pgItems builds items whose Sort is each name and whose Tie is empty.
func pgItems(names ...string) []emulator.PageItemForTest {
	out := make([]emulator.PageItemForTest, 0, len(names))
	for _, n := range names {
		out = append(out, emulator.PageItemForTest{Sort: n})
	}
	return out
}

// pgSorts is the Sort of each item, in order.
func pgSorts(items []emulator.PageItemForTest) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.Sort+it.Tie)
	}
	return out
}

// pgWalk walks items a page at a time, letting mutate change the listing after each page, and
// returns every item returned, in order.
func pgWalk(t *testing.T, items []emulator.PageItemForTest, req emulator.PageRequestForTest,
	mutate func(page int, items []emulator.PageItemForTest) []emulator.PageItemForTest) []string {
	t.Helper()
	var walked []string
	for page := 0; ; page++ {
		require.Less(t, page, 50, "the walk terminates")
		got, next, truncated, err := emulator.PageForTest(items, req)
		require.NoError(t, err)
		assert.Equal(t, truncated, next != "", "a token is issued exactly when the page is truncated")
		if req.Size > 0 {
			assert.LessOrEqual(t, len(got), req.Size)
		}
		walked = append(walked, pgSorts(got)...)
		if next == "" {
			return walked
		}
		req.Token = next
		if mutate != nil {
			items = mutate(page, items)
		}
	}
}

func TestPagination_WalkOrdersAndTruncates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		items      []emulator.PageItemForTest
		size       int
		descending bool
		want       []string
	}{
		{"empty input", nil, 2, false, nil},
		{"unsorted input is sorted", pgItems("c", "a", "b"), 2, false, []string{"a", "b", "c"}},
		{"an exact multiple ends without an empty page", pgItems("a", "b", "c", "d"), 2, false, []string{"a", "b", "c", "d"}},
		{"size 0 is the whole listing", pgItems("b", "a"), 0, false, []string{"a", "b"}},
		{"size 1", pgItems("b", "a", "c"), 1, false, []string{"a", "b", "c"}},
		{"descending", pgItems("a", "c", "b"), 2, true, []string{"c", "b", "a"}},
		{"ties order by Tie", []emulator.PageItemForTest{{"s", "2"}, {"s", "1"}, {"r", "9"}, {"s", "3"}}, 1, false,
			[]string{"r9", "s1", "s2", "s3"}},
		{"ties descending", []emulator.PageItemForTest{{"s", "2"}, {"s", "1"}, {"r", "9"}}, 2, true,
			[]string{"s2", "s1", "r9"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := pgWalk(t, tc.items, emulator.PageRequestForTest{Op: "Op", Size: tc.size, Descending: tc.descending}, nil)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPagination_FirstPageReportsTruncation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		items         []emulator.PageItemForTest
		size          int
		wantTruncated bool
	}{
		{"empty", nil, 1, false},
		{"fewer than a page", pgItems("a"), 2, false},
		{"exactly a page", pgItems("a", "b"), 2, false},
		{"one more than a page", pgItems("a", "b", "c"), 2, true},
	} {
		_, next, truncated, err := emulator.PageForTest(tc.items, emulator.PageRequestForTest{Op: "Op", Size: tc.size})
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.wantTruncated, truncated, tc.name)
		assert.Equal(t, tc.wantTruncated, next != "", tc.name)
	}
}

// TestPagination_MutationMidWalk changes the listing between the first and second pages, before, at
// and after the cursor, and asserts every item present for the whole walk is returned exactly once
// and nothing is returned twice.
func TestPagination_MutationMidWalk(t *testing.T) {
	t.Parallel()
	base := []string{"b", "d", "f", "h", "j", "l"}
	without := func(items []emulator.PageItemForTest, name string) []emulator.PageItemForTest {
		return slices.DeleteFunc(slices.Clone(items), func(it emulator.PageItemForTest) bool { return it.Sort == name })
	}
	with := func(items []emulator.PageItemForTest, name string) []emulator.PageItemForTest {
		return append(slices.Clone(items), emulator.PageItemForTest{Sort: name})
	}
	// The first page at size 2 is b, d: the cursor is d.
	for _, tc := range []struct {
		name       string
		descending bool
		change     func([]emulator.PageItemForTest) []emulator.PageItemForTest
		want       []string
	}{
		{"delete before the cursor", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return without(i, "b") },
			[]string{"b", "d", "f", "h", "j", "l"}},
		{"delete the cursor record", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return without(i, "d") },
			[]string{"b", "d", "f", "h", "j", "l"}},
		{"delete after the cursor", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return without(i, "f") },
			[]string{"b", "d", "h", "j", "l"}},
		{"insert before the cursor", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return with(i, "a") },
			[]string{"b", "d", "f", "h", "j", "l"}},
		{"insert at the cursor's place", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest {
			return with(without(i, "d"), "d")
		}, []string{"b", "d", "f", "h", "j", "l"}},
		{"insert just after the cursor", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return with(i, "e") },
			[]string{"b", "d", "e", "f", "h", "j", "l"}},
		{"delete everything after the cursor", false, func(i []emulator.PageItemForTest) []emulator.PageItemForTest {
			return pgItems("b", "d")
		}, []string{"b", "d"}},
		// Descending, the first page is l, j: the cursor is j.
		{"descending, delete the cursor record", true, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return without(i, "j") },
			[]string{"l", "j", "h", "f", "d", "b"}},
		{"descending, insert after the cursor", true, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return with(i, "i") },
			[]string{"l", "j", "i", "h", "f", "d", "b"}},
		{"descending, insert before the cursor", true, func(i []emulator.PageItemForTest) []emulator.PageItemForTest { return with(i, "k") },
			[]string{"l", "j", "h", "f", "d", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := pgWalk(t, pgItems(base...), emulator.PageRequestForTest{Op: "Op", Size: 2, Descending: tc.descending},
				func(page int, items []emulator.PageItemForTest) []emulator.PageItemForTest {
					if page == 0 {
						return tc.change(items)
					}
					return items
				})
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPagination_TokenRoundTripsAndIsBound(t *testing.T) {
	t.Parallel()
	filter := map[string][]string{"Names": {"x", "y"}, "Arn": {"a"}}
	req := emulator.PageRequestForTest{Op: "ListThings", Filter: filter, Size: 1}
	_, next, _, err := emulator.PageForTest(pgItems("a", "b", "c"), req)
	require.NoError(t, err)
	require.NotEmpty(t, next)

	raw, err := base64.StdEncoding.DecodeString(next)
	require.NoError(t, err)
	var tok map[string]string
	require.NoError(t, json.Unmarshal(raw, &tok))
	assert.Equal(t, "ListThings", tok["o"])
	assert.Equal(t, "a", tok["s"])
	assert.Empty(t, tok["t"])
	assert.Len(t, tok["h"], 16, "the filter hash is sixteen hex digits")

	accepted := func(r emulator.PageRequestForTest) bool { return emulator.PageValidateForTest(r) == nil }
	ok := req
	ok.Token = next
	assert.True(t, accepted(ok), "the token round-trips")

	reordered := ok
	reordered.Filter = map[string][]string{"Arn": {"a"}, "Names": {"y", "x"}, "Empty": nil, "Blank": {""}}
	assert.True(t, accepted(reordered), "member order, value order and empty members do not change the filter")
	duplicated := ok
	duplicated.Filter = map[string][]string{"Arn": {"a", "a"}, "Names": {"x", "y", "x"}}
	assert.True(t, accepted(duplicated), "a value repeated is the same set")

	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	canonical := string(raw)
	for _, tc := range []struct {
		name string
		req  func(r emulator.PageRequestForTest) emulator.PageRequestForTest
	}{
		{"a foreign operation", func(r emulator.PageRequestForTest) emulator.PageRequestForTest { r.Op = "ListOthers"; return r }},
		{"a different filter value", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Filter = map[string][]string{"Names": {"x"}, "Arn": {"a"}}
			return r
		}},
		{"a filter dropped", func(r emulator.PageRequestForTest) emulator.PageRequestForTest { r.Filter = nil; return r }},
		{"a filter added", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Filter = map[string][]string{"Names": {"x", "y"}, "Arn": {"a"}, "Type": {"t"}}
			return r
		}},
		{"not base64", func(r emulator.PageRequestForTest) emulator.PageRequestForTest { r.Token = "not a token!"; return r }},
		{"unpadded base64", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = base64.RawStdEncoding.EncodeToString(raw)
			return r
		}},
		{"base64 of text", func(r emulator.PageRequestForTest) emulator.PageRequestForTest { r.Token = enc("page-two"); return r }},
		{"base64 of an offset", func(r emulator.PageRequestForTest) emulator.PageRequestForTest { r.Token = enc("1"); return r }},
		{"whitespace in the JSON", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = enc(" " + canonical)
			return r
		}},
		{"an unknown member", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = enc(canonical[:len(canonical)-1] + `,"x":"1"}`)
			return r
		}},
		{"a missing member", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = enc(`{"o":"ListThings","h":"` + tok["h"] + `","s":"a"}`)
			return r
		}},
		{"members reordered", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = enc(`{"h":"` + tok["h"] + `","o":"ListThings","s":"a","t":""}`)
			return r
		}},
		{"a forged hash", func(r emulator.PageRequestForTest) emulator.PageRequestForTest {
			r.Token = enc(`{"o":"ListThings","h":"0000000000000000","s":"a","t":""}`)
			return r
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.req(ok)
			err := emulator.PageValidateForTest(r)
			require.Error(t, err)
			assert.True(t, emulator.IsPageTokenErrorForTest(err), "the refusal wraps the sentinel")
			_, _, _, pageErr := emulator.PageForTest(pgItems("a", "b"), r)
			assert.True(t, emulator.IsPageTokenErrorForTest(pageErr), "page refuses what validate refuses")
		})
	}
}

// TestPagination_TheZeroKeyIsAPosition pins the zero key's reading: it sorts first, a page ending on
// it carries a token like any other, and that token resumes at the first non-zero key.
func TestPagination_TheZeroKeyIsAPosition(t *testing.T) {
	t.Parallel()
	items := []emulator.PageItemForTest{{Sort: "b"}, {}, {Sort: "a"}}
	req := emulator.PageRequestForTest{Op: "Op", Size: 1}
	got, next, truncated, err := emulator.PageForTest(items, req)
	require.NoError(t, err)
	require.Equal(t, []emulator.PageItemForTest{{}}, got)
	require.True(t, truncated)
	require.NotEmpty(t, next, "a truncated page on the zero key still carries a token")
	assert.Equal(t, []string{"", "a", "b"}, pgWalk(t, items, req, nil))

	req.Token = next
	got, _, _, err = emulator.PageForTest(items, req)
	require.NoError(t, err)
	assert.Equal(t, pgItems("a"), got)
}
