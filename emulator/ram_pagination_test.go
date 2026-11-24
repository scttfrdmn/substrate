package emulator_test

// GetResourceShares, ListPrincipals and ListResources page on maxResults/nextToken, and
// GetResourceShares applies tagFilters (#1542). Every assertion is over the wire.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ramPageDoc is one page of any of the three listings: the items under their member, and the token.
type ramPageDoc struct {
	NextToken string
	Items     []json.RawMessage
}

// ramPageCall reads one page of path's listing, whose items are under member.
func ramPageCall(t *testing.T, ts *emulator.TestServer, path, member string, body map[string]any) ramPageDoc {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, path, body)
	require.Equal(t, http.StatusOK, status, "%s: %s", path, raw)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	var out ramPageDoc
	if tok, ok := doc["nextToken"]; ok {
		require.NoError(t, json.Unmarshal(tok, &out.NextToken))
		require.NotEmpty(t, out.NextToken, "a nextToken that is present is not empty")
	}
	require.NoError(t, json.Unmarshal(doc[member], &out.Items), "%s carries %s: %s", path, member, raw)
	return out
}

// ramWalk follows nextToken to the end with pages of size, returning every item and the page count.
func ramWalk(t *testing.T, ts *emulator.TestServer, path, member string, size int) ([]json.RawMessage, int) {
	t.Helper()
	var all []json.RawMessage
	token := ""
	for pages := 1; ; pages++ {
		body := map[string]any{"resourceOwner": "SELF", "maxResults": size}
		if token != "" {
			body["nextToken"] = token
		}
		page := ramPageCall(t, ts, path, member, body)
		require.LessOrEqual(t, len(page.Items), size, "a page holds at most maxResults items")
		all = append(all, page.Items...)
		if page.NextToken == "" {
			return all, pages
		}
		token = page.NextToken
		require.Less(t, pages, 100, "the walk terminates")
	}
}

func TestRAMPagination_EveryListingPages(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 5 {
		ramOwnershipShare(t, ts, fmt.Sprintf("page-%d", i),
			[]string{fmt.Sprintf("11111111111%d", i)},
			[]string{fmt.Sprintf("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-%d", i)})
	}

	for _, tc := range []struct{ path, member string }{
		{"/getresourceshares", "resourceShares"},
		{"/listprincipals", "principals"},
		{"/listresources", "resources"},
	} {
		t.Run(tc.member, func(t *testing.T) {
			whole := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF"})
			assert.Len(t, whole.Items, 5, "an absent maxResults answers everything on one page")
			assert.Empty(t, whole.NextToken)

			for _, size := range []int{1, 2, 5, 500} {
				items, pages := ramWalk(t, ts, tc.path, tc.member, size)
				assert.Equal(t, whole.Items, items, "maxResults %d: the walk visits every item once, in order", size)
				assert.Equal(t, (5+size-1)/size, pages, "maxResults %d: the last full page carries no token", size)
			}
		})
	}
}

func TestRAMPagination_RefusesWhatItCannotPage(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	const ownShare = "arn:aws:ram:us-east-1:123456789012:resource-share/0000"
	ops := map[string]string{
		"/getresourceshares": "GetResourceShares", "/listprincipals": "ListPrincipals", "/listresources": "ListResources",
	}
	for path, op := range ops {
		for _, tc := range []struct {
			name string
			body map[string]any
		}{
			{"maxResults 0", map[string]any{"maxResults": 0}},
			{"maxResults 501", map[string]any{"maxResults": 501}},
			{"maxResults -1", map[string]any{"maxResults": -1}},
			{"a token that is not base64", map[string]any{"nextToken": "not a token!"}},
			{"a token naming no operation", map[string]any{"nextToken": b64("abc")}},
			{"a bare offset, the shared helper's form", map[string]any{"nextToken": b64("1")}},
			{"a bare share ARN", map[string]any{"nextToken": b64(ownShare)}},
			{"the operation with no position", map[string]any{"nextToken": b64(op + ":")}},
			{"offset 0, which is never emitted", map[string]any{"nextToken": b64(op + ":0")}},
			{"an offset in a form never emitted", map[string]any{"nextToken": b64(op + ":05")}},
			{"a share ARN in another account", map[string]any{"nextToken": b64(op + ":arn:aws:ram:us-east-1:210987654321:resource-share/0000")}},
			{"a share ARN in another Region", map[string]any{"nextToken": b64(op + ":arn:aws:ram:eu-west-1:123456789012:resource-share/0000")}},
		} {
			tc.body["resourceOwner"] = "SELF"
			want := "InvalidNextTokenException"
			if _, ok := tc.body["maxResults"]; ok {
				want = "InvalidParameterException"
			}
			status, code := ramOwnershipErrorCode(t, ts, path, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s %s", path, tc.name)
			assert.Equal(t, want, code, "%s %s", path, tc.name)
		}
	}

	// Each listing's token is its own: a token one operation really issued is refused by the other two.
	for i := range 2 {
		ramOwnershipShare(t, ts, fmt.Sprintf("tok-%d", i),
			[]string{fmt.Sprintf("11111111111%d", i)},
			[]string{fmt.Sprintf("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-%d", i)})
	}
	issued := map[string]string{}
	members := map[string]string{"/getresourceshares": "resourceShares", "/listprincipals": "principals", "/listresources": "resources"}
	for path, member := range members {
		page := ramPageCall(t, ts, path, member, map[string]any{"resourceOwner": "SELF", "maxResults": 1})
		require.NotEmpty(t, page.NextToken, path)
		issued[path] = page.NextToken
		again := ramPageCall(t, ts, path, member, map[string]any{"resourceOwner": "SELF", "maxResults": 1, "nextToken": page.NextToken})
		assert.Len(t, again.Items, 1, "%s accepts its own token", path)
	}
	for from, token := range issued {
		for to := range members {
			if to == from {
				continue
			}
			status, code := ramOwnershipErrorCode(t, ts, to, map[string]any{"resourceOwner": "SELF", "nextToken": token})
			assert.Equal(t, http.StatusBadRequest, status, "%s token on %s", from, to)
			assert.Equal(t, "InvalidNextTokenException", code, "%s token on %s", from, to)
		}
	}
}

// TestRAMPagination_AnExpiryMidWalkSkipsNoShare pins why GetResourceShares pages on a value cursor: a
// DELETED share expiring between two pages removes it from the listing, and the walk resumes after its
// ARN rather than at a position that has moved.
func TestRAMPagination_AnExpiryMidWalkSkipsNoShare(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, name := range []string{"a", "b", "c"} {
		ramOwnershipShare(t, ts, name, nil, nil)
	}
	all := ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF"}).ResourceShares
	require.Len(t, all, 3)
	first := all[0].ResourceShareArn
	status, raw := ramOwnershipCall(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": first})
	require.Equal(t, http.StatusOK, status, "%s", raw)

	page1 := ramOwnershipGetPage(t, ts, map[string]any{"resourceOwner": "SELF", "maxResults": 1})
	require.Equal(t, []string{first}, page1.arns(), "the deleted share is still listed, first")
	require.NotEmpty(t, page1.NextToken)

	// Its window passes between the two pages, and the next listing prunes it from the index.
	ts.AdvanceTime(2*time.Hour + time.Second)
	page2 := ramOwnershipGetPage(t, ts, map[string]any{"resourceOwner": "SELF", "maxResults": 2, "nextToken": page1.NextToken})
	assert.Equal(t, []string{all[1].ResourceShareArn, all[2].ResourceShareArn}, page2.arns(),
		"the walk resumes after the expired share; an offset would have skipped the second share")
	assert.Empty(t, page2.NextToken)
}

// ramEntry is one ListPrincipals or ListResources item's cursor pair.
type ramEntry struct {
	Share string `json:"resourceShareArn"`
	ID    string `json:"id"`
	ARN   string `json:"arn"`
}

// key is the entry's (share, entity) pair, which is also the listing's order.
func (e ramEntry) key() string { return e.Share + "|" + e.ID + e.ARN }

// ramEntries decodes a page's items.
func ramEntries(t *testing.T, items []json.RawMessage) []ramEntry {
	t.Helper()
	out := make([]ramEntry, 0, len(items))
	for _, raw := range items {
		var e ramEntry
		require.NoError(t, json.Unmarshal(raw, &e))
		out = append(out, e)
	}
	return out
}

// TestRAMPagination_ListingsRenderInCursorOrder pins the order ListPrincipals and ListResources
// compare in: resource-share ARN, then principal ID or resource ARN, whatever the association order.
func TestRAMPagination_ListingsRenderInCursorOrder(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	ramOwnershipShare(t, ts, "ordered", []string{"333333333333", "111111111111", "222222222222"},
		[]string{
			"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-c",
			"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-a",
		})
	ramOwnershipShare(t, ts, "second", []string{"444444444444"}, nil)

	for _, tc := range []struct{ path, member string }{
		{"/listprincipals", "principals"},
		{"/listresources", "resources"},
	} {
		page := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF"})
		keys := []string{}
		for _, e := range ramEntries(t, page.Items) {
			keys = append(keys, e.key())
		}
		assert.True(t, sort.StringsAreSorted(keys), "%s lists in (share, entity) order: %v", tc.path, keys)
	}
}

// TestRAMPagination_AWalkSurvivesMidWalkChanges walks each listing a page at a time and, between the
// first and second pages, deletes a share, disassociates the entity the token names, and associates a
// new one on each side of the cursor. Every item that survives is returned exactly once.
func TestRAMPagination_AWalkSurvivesMidWalkChanges(t *testing.T) {
	t.Parallel()
	subnet := func(s string) string { return "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-" + s }
	for _, tc := range []struct {
		path, member, assocMember string
		entity                    func(ramEntry) string
		values                    func(...string) []string
	}{
		{"/listprincipals", "principals", "principals", func(e ramEntry) string { return e.ID },
			func(s ...string) []string {
				out := []string{}
				for _, v := range s {
					out = append(out, "1111111111"+v)
				}
				return out
			}},
		{"/listresources", "resources", "resourceArns", func(e ramEntry) string { return e.ARN },
			func(s ...string) []string {
				out := []string{}
				for _, v := range s {
					out = append(out, subnet(v))
				}
				return out
			}},
	} {
		t.Run(tc.member, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			create := func(name string, values []string) string {
				if tc.member == "principals" {
					return ramOwnershipShare(t, ts, name, values, nil)
				}
				return ramOwnershipShare(t, ts, name, nil, values)
			}
			create("s1", tc.values("10", "20", "30"))
			create("s2", tc.values("40", "50"))
			create("s3", tc.values("60"))
			whole := ramEntries(t, ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF"}).Items)
			require.Len(t, whole, 6)

			page1 := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF", "maxResults": 2})
			first := ramEntries(t, page1.Items)
			require.Len(t, first, 2)
			require.NotEmpty(t, page1.NextToken)
			cursor := first[1]
			// The deleted share is the last in the listing that is not the cursor's.
			doomed := ""
			for _, e := range whole {
				if e.Share != cursor.Share {
					doomed = e.Share
				}
			}
			require.NotEmpty(t, doomed)

			// The cursor's own entity is disassociated, one is added before the cursor and one after,
			// and a later share is deleted outright.
			change := func(path string, values []string) {
				status, raw := ramOwnershipCall(t, ts, path, map[string]any{
					"resourceShareArn": cursor.Share, tc.assocMember: values,
				})
				require.Equal(t, http.StatusOK, status, "%s", raw)
			}
			change("/disassociateresourceshare", []string{tc.entity(cursor)})
			change("/associateresourceshare", tc.values("00", "99"))
			status, raw := ramOwnershipCall(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": doomed})
			require.Equal(t, http.StatusOK, status, "%s", raw)

			walked := append([]ramEntry(nil), first...)
			token := page1.NextToken
			for pages := 0; token != ""; pages++ {
				require.Less(t, pages, 10, "the walk terminates")
				pg := ramPageCall(t, ts, tc.path, tc.member, map[string]any{
					"resourceOwner": "SELF", "maxResults": 2, "nextToken": token,
				})
				walked = append(walked, ramEntries(t, pg.Items)...)
				token = pg.NextToken
			}

			seen := map[string]int{}
			for _, e := range walked {
				seen[e.key()]++
				assert.Equal(t, 1, seen[e.key()], "%s returned twice", e.key())
			}
			for _, e := range whole[2:] {
				if e.Share == doomed {
					assert.Zero(t, seen[e.key()], "an item of the deleted share is not listed")
					continue
				}
				assert.Equal(t, 1, seen[e.key()], "the surviving item %s is returned exactly once", e.key())
			}
			added := tc.values("99")[0]
			assert.Equal(t, 1, seen[cursor.Share+"|"+added], "an item added after the cursor is reached")
			assert.Zero(t, seen[cursor.Share+"|"+tc.values("00")[0]], "an item added before the cursor is not")
		})
	}
}

// TestRAMPagination_ATokenIsBoundToItsFilters refuses a ListPrincipals or ListResources token reused
// with different filter members, and a forged token, with InvalidNextTokenException/400.
func TestRAMPagination_ATokenIsBoundToItsFilters(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := range 3 {
		ramOwnershipShare(t, ts, fmt.Sprintf("bound-%d", i),
			[]string{fmt.Sprintf("11111111111%d", i)},
			[]string{fmt.Sprintf("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-%d", i)})
	}
	forged := base64.StdEncoding.EncodeToString([]byte(`{"o":"ListPrincipals","h":"0000000000000000","s":"x","t":"y"}`))
	for _, tc := range []struct {
		path, member string
		filter       map[string]any
	}{
		{"/listprincipals", "principals", map[string]any{"principals": []string{"111111111111"}}},
		{"/listresources", "resources", map[string]any{"resourceType": "ec2:Subnet"}},
	} {
		page := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF", "maxResults": 1})
		require.NotEmpty(t, page.NextToken)

		reused := map[string]any{"resourceOwner": "SELF", "nextToken": page.NextToken}
		for k, v := range tc.filter {
			reused[k] = v
		}
		for name, body := range map[string]map[string]any{
			"a token reused with another filter": reused,
			"a forged token":                     {"resourceOwner": "SELF", "nextToken": forged},
		} {
			status, code := ramOwnershipErrorCode(t, ts, tc.path, body)
			assert.Equal(t, http.StatusBadRequest, status, "%s %s", tc.path, name)
			assert.Equal(t, "InvalidNextTokenException", code, "%s %s", tc.path, name)
		}
	}
}

// ramOwnershipPage is one GetResourceShares page with its token.
type ramOwnershipPage struct {
	NextToken      string `json:"nextToken"`
	ResourceShares []struct {
		ResourceShareArn string `json:"resourceShareArn"`
	} `json:"resourceShares"`
}

// arns is the page's share ARNs in the order listed.
func (p ramOwnershipPage) arns() []string {
	out := []string{}
	for _, s := range p.ResourceShares {
		out = append(out, s.ResourceShareArn)
	}
	return out
}

// ramOwnershipGetPage reads one GetResourceShares page.
func ramOwnershipGetPage(t *testing.T, ts *emulator.TestServer, body map[string]any) ramOwnershipPage {
	t.Helper()
	status, raw := ramOwnershipCall(t, ts, "/getresourceshares", body)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	var out ramOwnershipPage
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func TestRAMPagination_TagFiltersFilterGetResourceShares(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	tagged := func(name string, tags map[string]string) string {
		var list []map[string]string
		for k, v := range tags {
			list = append(list, map[string]string{"key": k, "value": v})
		}
		return ramCreate(t, ts, map[string]any{"name": name, "tags": list}).ResourceShare.ResourceShareArn
	}
	prod := tagged("prod", map[string]string{"env": "prod"})
	dev := tagged("dev", map[string]string{"env": "dev", "team": "a"})
	tagged("bare", nil)

	names := func(filters []map[string]any) []string {
		got := ramOwnershipGet(t, ts, map[string]any{"resourceOwner": "SELF", "tagFilters": filters})
		out := []string{}
		for _, s := range got.ResourceShares {
			out = append(out, s.ResourceShareArn)
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		filters []map[string]any
		want    []string
	}{
		{"a key and a value", []map[string]any{{"tagKey": "env", "tagValues": []string{"prod"}}}, []string{prod}},
		{"a key with no values matches any value", []map[string]any{{"tagKey": "env"}}, []string{prod, dev}},
		{"either of two values", []map[string]any{{"tagKey": "env", "tagValues": []string{"prod", "dev"}}}, []string{prod, dev}},
		{"two filters are an OR", []map[string]any{
			{"tagKey": "env", "tagValues": []string{"prod"}}, {"tagKey": "team"},
		}, []string{prod, dev}},
		{"a key nothing carries", []map[string]any{{"tagKey": "owner"}}, []string{}},
	} {
		assert.ElementsMatch(t, tc.want, names(tc.filters), tc.name)
	}

	status, code := ramOwnershipErrorCode(t, ts, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF", "tagFilters": []map[string]any{{"tagValues": []string{"x"}}},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", code, "a filter with no tagKey is refused")
}
