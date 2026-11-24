package emulator_test

// GetResourceShares, ListPrincipals and ListResources page on maxResults/nextToken, and
// GetResourceShares applies tagFilters (#1542). Every assertion is over the wire.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
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

// ramEntry is one ListPrincipals or ListResources item.
type ramEntry struct {
	Share string `json:"resourceShareArn"`
	ID    string `json:"id"`
	ARN   string `json:"arn"`
}

// entity is the item's principal ID or resource ARN, which is the listing's order and its cursor.
func (e ramEntry) entity() string { return e.ID + e.ARN }

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

// ramListCase is ListPrincipals or ListResources, with how to name its entities.
type ramListCase struct {
	path, member, assocMember string
	value                     func(string) string
}

// ramListCases are the two entity listings.
func ramListCases() []ramListCase {
	return []ramListCase{
		{"/listprincipals", "principals", "principals", func(s string) string { return "1111111111" + s }},
		{"/listresources", "resources", "resourceArns", func(s string) string {
			return "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-" + s
		}},
	}
}

// ramShareWith creates a share holding tc's entities named by suffixes.
func ramShareWith(t *testing.T, ts *emulator.TestServer, tc ramListCase, name string, suffixes ...string) string {
	t.Helper()
	values := []string{}
	for _, s := range suffixes {
		values = append(values, tc.value(s))
	}
	if tc.member == "principals" {
		return ramOwnershipShare(t, ts, name, values, nil)
	}
	return ramOwnershipShare(t, ts, name, nil, values)
}

// ramChange associates or disassociates tc's entities named by suffixes on share.
func ramChange(t *testing.T, ts *emulator.TestServer, tc ramListCase, path, share string, suffixes ...string) {
	t.Helper()
	values := []string{}
	for _, s := range suffixes {
		values = append(values, tc.value(s))
	}
	status, raw := ramOwnershipCall(t, ts, path, map[string]any{"resourceShareArn": share, tc.assocMember: values})
	require.Equal(t, http.StatusOK, status, "%s", raw)
}

// ramWalkFrom follows token to the end at page size 2, returning every entity listed.
func ramWalkFrom(t *testing.T, ts *emulator.TestServer, tc ramListCase, token string) []string {
	t.Helper()
	var out []string
	for pages := 0; token != ""; pages++ {
		require.Less(t, pages, 10, "the walk terminates")
		pg := ramPageCall(t, ts, tc.path, tc.member, map[string]any{
			"resourceOwner": "SELF", "maxResults": 2, "nextToken": token,
		})
		for _, e := range ramEntries(t, pg.Items) {
			out = append(out, e.entity())
		}
		token = pg.NextToken
	}
	return out
}

// TestRAMPagination_ListingsRenderInEntityOrder pins the order ListPrincipals and ListResources
// compare in: principal ID or resource ARN, across shares and whatever the association order. An
// entity of several shares is listed once, under the first share in ARN order.
func TestRAMPagination_ListingsRenderInEntityOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range ramListCases() {
		t.Run(tc.member, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			one := ramShareWith(t, ts, tc, "one", "30", "10", "50")
			two := ramShareWith(t, ts, tc, "two", "40", "20", "10")
			first := one
			if two < one {
				first = two
			}

			page := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF"})
			got := ramEntries(t, page.Items)
			var entities []string
			for _, e := range got {
				entities = append(entities, e.entity())
				if e.entity() == tc.value("10") {
					assert.Equal(t, first, e.Share, "a shared entity is listed under the first share in ARN order")
				}
			}
			assert.Equal(t, []string{tc.value("10"), tc.value("20"), tc.value("30"), tc.value("40"), tc.value("50")},
				entities, "each entity once, in entity order")
		})
	}
}

// TestRAMPagination_AWalkSurvivesMidWalkChanges walks each listing a page at a time and changes it
// between the first and second pages. Every entity listed both before and after the change is
// returned exactly once, and nothing is returned twice.
func TestRAMPagination_AWalkSurvivesMidWalkChanges(t *testing.T) {
	t.Parallel()
	for _, tc := range ramListCases() {
		for _, change := range []struct {
			name string
			// apply changes the listing; the token's entity is 20.
			apply func(t *testing.T, ts *emulator.TestServer, early, late string)
			want  []string
		}{
			{
				// The cursor's entity moves to an earlier share: its position does not move.
				name: "the cursor's entity associated to an earlier share",
				apply: func(t *testing.T, ts *emulator.TestServer, early, late string) {
					ramChange(t, ts, tc, "/associateresourceshare", early, "20", "40")
				},
				want: []string{"10", "20", "30", "40", "50"},
			},
			{
				name: "the cursor's entity disassociated",
				apply: func(t *testing.T, ts *emulator.TestServer, early, late string) {
					ramChange(t, ts, tc, "/disassociateresourceshare", late, "20")
				},
				want: []string{"10", "20", "30", "40", "50"},
			},
			{
				name: "an entity after the cursor disassociated, one added on each side",
				apply: func(t *testing.T, ts *emulator.TestServer, early, late string) {
					ramChange(t, ts, tc, "/disassociateresourceshare", late, "30")
					ramChange(t, ts, tc, "/associateresourceshare", late, "15", "45")
				},
				want: []string{"10", "20", "40", "45", "50"},
			},
			{
				name: "a share deleted",
				apply: func(t *testing.T, ts *emulator.TestServer, early, late string) {
					status, raw := ramOwnershipCall(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": early})
					require.Equal(t, http.StatusOK, status, "%s", raw)
				},
				want: []string{"10", "20", "30"},
			},
		} {
			t.Run(tc.member+"/"+change.name, func(t *testing.T) {
				t.Parallel()
				ts := emulator.StartTestServer(t)
				a := ramOwnershipShare(t, ts, "a", nil, nil)
				b := ramOwnershipShare(t, ts, "b", nil, nil)
				early, late := a, b
				if b < a {
					early, late = b, a
				}
				// The cursor's entity, 20, is in the later share.
				ramChange(t, ts, tc, "/associateresourceshare", early, "10", "40", "50")
				ramChange(t, ts, tc, "/associateresourceshare", late, "20", "30")

				page1 := ramPageCall(t, ts, tc.path, tc.member, map[string]any{"resourceOwner": "SELF", "maxResults": 2})
				first := ramEntries(t, page1.Items)
				require.Equal(t, []string{tc.value("10"), tc.value("20")}, []string{first[0].entity(), first[1].entity()})
				require.NotEmpty(t, page1.NextToken)

				change.apply(t, ts, early, late)
				walked := []string{first[0].entity(), first[1].entity()}
				walked = append(walked, ramWalkFrom(t, ts, tc, page1.NextToken)...)

				want := []string{}
				for _, s := range change.want {
					want = append(want, tc.value(s))
				}
				assert.Equal(t, want, walked, "every surviving entity once, nothing repeated")
				for _, e := range walked {
					assert.NotEqual(t, tc.value("15"), e, "an entity added before the cursor is not reached")
				}
			})
		}
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
