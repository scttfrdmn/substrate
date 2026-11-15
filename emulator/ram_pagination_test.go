package emulator_test

// GetResourceShares, ListPrincipals and ListResources page on maxResults/nextToken, and
// GetResourceShares applies tagFilters (#1542). Every assertion is over the wire.

import (
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
	for _, path := range []string{"/getresourceshares", "/listprincipals", "/listresources"} {
		for _, tc := range []struct {
			name string
			body map[string]any
			code string
		}{
			{"maxResults 0", map[string]any{"maxResults": 0}, "InvalidParameterException"},
			{"maxResults 501", map[string]any{"maxResults": 501}, "InvalidParameterException"},
			{"maxResults -1", map[string]any{"maxResults": -1}, "InvalidParameterException"},
			{"a token that is not base64", map[string]any{"nextToken": "not a token!"}, "InvalidNextTokenException"},
			{"a token naming no offset", map[string]any{"nextToken": "YWJj"}, "InvalidNextTokenException"},
			{"a token substrate never emits", map[string]any{"nextToken": "MDU="}, "InvalidNextTokenException"},
		} {
			tc.body["resourceOwner"] = "SELF"
			status, code := ramOwnershipErrorCode(t, ts, path, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s %s", path, tc.name)
			assert.Equal(t, tc.code, code, "%s %s", path, tc.name)
		}
	}
}

// TestRAMPagination_AnExpiryMidWalkClampsRatherThanFails records the offset cursor's behaviour when the
// listing shrinks between pages: a token past the shrunken end is still one substrate issued, and
// answers an empty final page.
func TestRAMPagination_AnExpiryMidWalkClampsRatherThanFails(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	doomed := ramOwnershipShare(t, ts, "doomed", nil, nil)
	ramOwnershipShare(t, ts, "kept", nil, nil)
	status, raw := ramOwnershipCall(t, ts, "/deleteresourceshare", map[string]any{"resourceShareArn": doomed})
	require.Equal(t, http.StatusOK, status, "%s", raw)

	first := ramPageCall(t, ts, "/getresourceshares", "resourceShares", map[string]any{"resourceOwner": "SELF", "maxResults": 1})
	require.Len(t, first.Items, 1)
	require.NotEmpty(t, first.NextToken)

	// The deleted share's window passes between the two pages, and the listing shrinks to one share.
	ts.AdvanceTime(2*time.Hour + time.Second)
	second := ramPageCall(t, ts, "/getresourceshares", "resourceShares", map[string]any{
		"resourceOwner": "SELF", "maxResults": 1, "nextToken": first.NextToken,
	})
	assert.Empty(t, second.Items, "the offset past the shrunken end clamps to an empty page")
	assert.Empty(t, second.NextToken, "and the walk ends")
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
		assert.Equal(t, tc.want, names(tc.filters), tc.name)
	}

	status, code := ramOwnershipErrorCode(t, ts, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF", "tagFilters": []map[string]any{{"tagValues": []string{"x"}}},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", code, "a filter with no tagKey is refused")
}
