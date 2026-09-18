package emulator_test

// The API Gateway v1 collections that publish "limit" and "position" and now read them (#1025).
//
// Seven v1 collections publish the pair. GetBasePathMappings was made to read it under #917 and is
// asserted in apigateway_basepath_pagination_test.go, which is also where the server and request
// helpers this file uses live. The other six read neither parameter and answered the whole collection
// with no cursor: a paging consumer's loop terminated on its first response, and the walk it exists to
// perform first ran for real against an account holding more elements than one page.
//
// The six are added to [apigwPagedCollections] as they land, and every assertion below runs over the
// whole table, so a collection that pages differently from its siblings fails here rather than in a
// test written only for it. The assertions are the ones the basepath operation already carries, for
// the reason those were chosen: the published default of 25 is what distinguishes these operations
// from an EC2 describe, a walk that repeats or omits an element is the defect a cursor introduces,
// and a "position" substrate never issued must be refused rather than answered with page one (#915).
//
// Every element is created through the service's own wire call rather than written to state (#765).
//
// What the order assertions can and cannot say is decided by the order these collections are in.
// Unlike the base path mappings, which are keyed by a caller-chosen base path, these are walked in
// ascending element ID, because that is what the index [updateStringIndex] maintains holds — and an ID
// is generated, not chosen. So a test cannot assert that the walk matches the order the creating calls
// were made in; it would be asserting over crypto/rand. What it asserts instead is the property that
// actually matters to a paging consumer: the paged walk agrees, element for element and in order, with
// the same collection read whole, and between them they account for exactly what was created. That is
// checked by reading the collection at the published maximum first and paging against that answer.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// apigwPagedCollection describes one paginated v1 collection: how to fill it, where to read it, and
// which of an element's members names it in an assertion.
type apigwPagedCollection struct {
	// op is the operation name, used to label the subtests.
	op string
	// member is the element member that identifies an element. It is a member AWS publishes on the
	// element, whose value the test controls, so a failure names the element a reader can find.
	member string
	// seed fills the collection with exactly n elements on a fresh server and returns the path to
	// list it at, together with the members it created, in creation order — which is not the order
	// the collection reports them in.
	seed func(t *testing.T, srv *emulator.Server, n int) (path string, created []string)
	// emptyPath lists the collection without anything having been created, for the assertion that a
	// malformed request is refused before any state is read. It cannot come from seed, whose calls a
	// sealed store would fail.
	emptyPath string
}

// apigwPagedCollections is the table every assertion in this file runs over. PRs adding a collection
// add a row here rather than a test.
var apigwPagedCollections = []apigwPagedCollection{
	{
		op:        "GetRestApis",
		member:    "name",
		seed:      apigwSeedRestAPIs,
		emptyPath: "/restapis",
	},
	{
		op:        "GetResources",
		member:    "path",
		seed:      apigwSeedResources,
		emptyPath: "/restapis/abcde12345/resources",
	},
}

// apigwSeedRestAPIs creates n REST APIs, whose names are zero-padded so a failure reads clearly, and
// returns the account-level collection path.
func apigwSeedRestAPIs(t *testing.T, srv *emulator.Server, n int) (string, []string) {
	t.Helper()
	created := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("api-%03d", i)
		status, raw := apigwPagingCall(t, srv, http.MethodPost, "/restapis", map[string]any{"name": name})
		require.Equal(t, http.StatusCreated, status, raw)
		created = append(created, name)
	}
	return "/restapis", created
}

// apigwSeedResources creates one REST API and n-1 child resources under its root.
//
// The root resource CreateRestApi writes is part of the collection and cannot be created or deleted
// separately, so it counts towards n and its path, "/", is one of the members returned.
func apigwSeedResources(t *testing.T, srv *emulator.Server, n int) (string, []string) {
	t.Helper()
	require.Positive(t, n, "a resource collection always holds at least its root")

	status, raw := apigwPagingCall(t, srv, http.MethodPost, "/restapis", map[string]any{"name": "paging"})
	require.Equal(t, http.StatusCreated, status, raw)
	var api struct {
		ID             string `json:"id"`
		RootResourceID string `json:"rootResourceId"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &api), raw)
	require.NotEmpty(t, api.ID)
	require.NotEmpty(t, api.RootResourceID)

	created := []string{"/"}
	for i := 0; i < n-1; i++ {
		pathPart := fmt.Sprintf("rp-%03d", i)
		underRoot := "/restapis/" + api.ID + "/resources/" + api.RootResourceID
		status, raw = apigwPagingCall(t, srv, http.MethodPost, underRoot, map[string]any{"pathPart": pathPart})
		require.Equal(t, http.StatusCreated, status, raw)
		created = append(created, "/"+pathPart)
	}
	return "/restapis/" + api.ID + "/resources", created
}

// apigwCollectionPage is one v1 collection response, decoded loosely: the elements are read as maps so
// that one table can assert over collections whose elements share no Go type.
type apigwCollectionPage struct {
	Item     []map[string]any `json:"item"`
	Position string           `json:"position"`
}

// members returns the named member of every element on the page, in the order the page carries them.
func (p apigwCollectionPage) members(t *testing.T, member string) []string {
	t.Helper()
	out := make([]string, 0, len(p.Item))
	for _, element := range p.Item {
		value, ok := element[member].(string)
		require.True(t, ok, "element %v has no string %q", element, member)
		out = append(out, value)
	}
	return out
}

// apigwCollectionListRaw lists a collection and returns the status and the raw body.
//
// The query is built with url.Values so a position containing "+" or "=" survives the round trip
// rather than arriving corrupted, which would make the test assert against its own damage.
func apigwCollectionListRaw(t *testing.T, srv *emulator.Server, path string, pairs ...string) (int, string) {
	t.Helper()
	query := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			query.Set(pairs[i], pairs[i+1])
		}
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return apigwPagingCall(t, srv, http.MethodGet, path, nil)
}

// apigwCollectionList lists a collection, requires a 200 and returns the decoded page.
func apigwCollectionList(t *testing.T, srv *emulator.Server, path string, pairs ...string) apigwCollectionPage {
	t.Helper()
	status, raw := apigwCollectionListRaw(t, srv, path, pairs...)
	require.Equal(t, http.StatusOK, status, raw)
	var page apigwCollectionPage
	require.NoError(t, json.Unmarshal([]byte(raw), &page), raw)
	return page
}

// apigwCollectionWhole reads the collection in one page at the published maximum and returns the order
// it reports, having first checked that it holds exactly what was created.
//
// This is the reference the paged walks below are compared against. It is also the only place the
// created set is checked against the reported one: everything after it asserts that paging agrees with
// reading whole, which is the property a paging consumer depends on and the one a cursor can break.
func apigwCollectionWhole(t *testing.T, srv *emulator.Server, c apigwPagedCollection, path string, created []string) []string {
	t.Helper()
	whole := apigwCollectionList(t, srv, path, "limit", "500")
	reported := whole.members(t, c.member)
	require.ElementsMatch(t, created, reported, "the collection read whole is not what was created")
	assert.Empty(t, whole.Position, "a collection inside one page must carry no position")
	return reported
}

// TestAPIGatewayCollections_AbsentLimitPagesAtThePublishedDefault asserts the bound that
// distinguishes these operations from every EC2 describe: a request naming no limit still pages, at
// the published default of 25, where each of them used to answer the whole collection.
func TestAPIGatewayCollections_AbsentLimitPagesAtThePublishedDefault(t *testing.T) {
	for _, collection := range apigwPagedCollections {
		t.Run(collection.op, func(t *testing.T) {
			srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
			const total = 26
			path, created := collection.seed(t, srv, total)
			want := apigwCollectionWhole(t, srv, collection, path, created)

			page1 := apigwCollectionList(t, srv, path)
			require.Len(t, page1.Item, 25, "an absent limit must page at the published default")
			assert.Equal(t, want[:25], page1.members(t, collection.member))
			require.NotEmpty(t, page1.Position, "a collection longer than the default must carry a position")

			page2 := apigwCollectionList(t, srv, path, "position", page1.Position)
			assert.Equal(t, want[25:], page2.members(t, collection.member))
			assert.Empty(t, page2.Position, "the last page carries no position")
		})
	}
}

// TestAPIGatewayCollections_WalkReportsEveryElementExactlyOnce is the assertion the cursor exists to
// satisfy: a caller looping until position comes back empty sees the whole collection, in the order it
// reads whole, with no repeat and no omission.
func TestAPIGatewayCollections_WalkReportsEveryElementExactlyOnce(t *testing.T) {
	for _, collection := range apigwPagedCollections {
		t.Run(collection.op, func(t *testing.T) {
			srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
			const total = 7
			path, created := collection.seed(t, srv, total)
			want := apigwCollectionWhole(t, srv, collection, path, created)

			var walked []string
			position := ""
			for pages := 0; ; pages++ {
				require.Less(t, pages, total+2, "the walk did not terminate")
				page := apigwCollectionList(t, srv, path, "limit", "3", "position", position)
				assert.LessOrEqual(t, len(page.Item), 3)
				walked = append(walked, page.members(t, collection.member)...)
				if page.Position == "" {
					break
				}
				position = page.Position
			}
			assert.Equal(t, want, walked)
		})
	}
}

// TestAPIGatewayCollections_FullFinalPageCarriesNoPosition covers the exact-multiple case: the
// position names where the next page starts, so a full final page must not carry one.
func TestAPIGatewayCollections_FullFinalPageCarriesNoPosition(t *testing.T) {
	for _, collection := range apigwPagedCollections {
		t.Run(collection.op, func(t *testing.T) {
			srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
			path, created := collection.seed(t, srv, 6)
			want := apigwCollectionWhole(t, srv, collection, path, created)

			page1 := apigwCollectionList(t, srv, path, "limit", "3")
			require.Equal(t, want[:3], page1.members(t, collection.member))
			require.NotEmpty(t, page1.Position)

			page2 := apigwCollectionList(t, srv, path, "limit", "3", "position", page1.Position)
			assert.Equal(t, want[3:], page2.members(t, collection.member))
			assert.Empty(t, page2.Position, "a full final page must not carry a token")
		})
	}
}

// TestAPIGatewayCollections_LimitOutsideThePublishedRangeIsRefused asserts the published maximum of
// 500 and the floor of one, which is substrate's reading — AWS publishes no minimum, and a page of
// zero elements describes a walk that answers nothing and hands back a position forever.
func TestAPIGatewayCollections_LimitOutsideThePublishedRangeIsRefused(t *testing.T) {
	for _, collection := range apigwPagedCollections {
		t.Run(collection.op, func(t *testing.T) {
			srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
			path, created := collection.seed(t, srv, 3)

			for _, tc := range []struct {
				name  string
				limit string
			}{
				{"zero", "0"},
				{"negative", "-1"},
				{"not an integer", "many"},
				{"one above the published maximum", "501"},
			} {
				t.Run("refused: "+tc.name, func(t *testing.T) {
					status, raw := apigwCollectionListRaw(t, srv, path, "limit", tc.limit)
					assert.Equal(t, http.StatusBadRequest, status, raw)
					assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
					assert.NotContains(t, raw, created[1], "a refused limit must not be answered with a page")
				})
			}

			t.Run("one is accepted", func(t *testing.T) {
				page := apigwCollectionList(t, srv, path, "limit", "1")
				assert.Len(t, page.Item, 1)
				assert.NotEmpty(t, page.Position)
			})

			t.Run("the published maximum is accepted", func(t *testing.T) {
				page := apigwCollectionList(t, srv, path, "limit", "500")
				assert.Len(t, page.Item, 3)
				assert.Empty(t, page.Position)
			})
		})
	}
}

// TestAPIGatewayCollections_RefuseAPositionTheyDidNotIssue is #915 at these operations: a token
// substrate could not have issued is refused rather than answered with a well-formed page one.
func TestAPIGatewayCollections_RefuseAPositionTheyDidNotIssue(t *testing.T) {
	for _, collection := range apigwPagedCollections {
		t.Run(collection.op, func(t *testing.T) {
			srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
			path, created := collection.seed(t, srv, 3)
			want := apigwCollectionWhole(t, srv, collection, path, created)

			// A position the operation issued still resumes the walk. Asserted first, so a refusal
			// that swallowed every token could not pass the rest of this test.
			page1 := apigwCollectionList(t, srv, path, "limit", "1")
			require.Equal(t, want[:1], page1.members(t, collection.member))
			issued := page1.Position
			require.NotEmpty(t, issued)

			page2 := apigwCollectionList(t, srv, path, "limit", "1", "position", issued)
			assert.Equal(t, want[1:2], page2.members(t, collection.member),
				"an issued position must resume after the first page")

			for _, tc := range tokenRefusalBadTokens {
				t.Run(tc.name, func(t *testing.T) {
					status, raw := apigwCollectionListRaw(t, srv, path, "limit", "1", "position", tc.token)
					assert.Equal(t, http.StatusBadRequest, status, raw)
					assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
					assert.NotContains(t, raw, want[0], "a refused position must not be answered with a page")
				})
			}

			// A position past the end is one substrate did issue, over a collection that has since
			// shrunk, so it clamps to an empty final page rather than being refused.
			t.Run("a position past the end clamps to an empty page", func(t *testing.T) {
				beyond := base64.StdEncoding.EncodeToString([]byte("99"))
				page := apigwCollectionList(t, srv, path, "limit", "1", "position", beyond)
				assert.Empty(t, page.Item)
				assert.Empty(t, page.Position)
			})

			// Both refusals precede the read of any state, per #887: the answer to a malformed request
			// must not depend on how many elements exist. Sealed against reads, the handler still
			// refuses rather than reporting the store's failure as a 500.
			for _, tc := range []struct {
				name  string
				pairs []string
			}{
				{"position", []string{"position", "!!not-base64!!"}},
				{"limit", []string{"limit", "0"}},
			} {
				t.Run(tc.name+" is refused before any state is read", func(t *testing.T) {
					sealed := &tokenRefusalSealedState{
						inner:     emulator.NewMemoryStateManager(),
						sealGets:  true,
						sealLists: true,
					}
					sealedSrv := apigwPagingServer(t, sealed)
					status, raw := apigwCollectionListRaw(t, sealedSrv, collection.emptyPath, tc.pairs...)
					assert.Equal(t, http.StatusBadRequest, status, raw)
					assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
				})
			}
		})
	}
}
