package emulator_test

// Offset pagination for the two catalog describes (#1024).
//
// API_DescribeInstanceTypes and API_DescribeInstanceTypeOfferings publish MaxResults and NextToken
// and substrate read neither, so each answered its whole listing with no token: a caller paging the
// catalog found one page here and several in production. Both convert onto the shared paginator
// (ec2_pagination.go), and both publish a floor of five with ceilings that differ — a hundred at
// DescribeInstanceTypes and a thousand at DescribeInstanceTypeOfferings.
//
// They get their own cases rather than rows in ec2_pagination_test.go's table for the reason that
// file's preamble gives about DescribeSpotPriceHistory: every case there begins by *creating* the
// listing it walks, and neither of these listings can be created. Both are the instance-type
// catalog — the offerings one crossed with the locations of the request's Region — so both exist in
// a fresh account at a size no test chose. The five properties the table asserts are asserted here
// against those fixed listings instead, plus two the table cannot reach: that the offset is stable
// over a listing built from no state at all, and that neither page has an ID list for MaxResults to
// conflict with.
//
// Everything is driven through the query protocol, per #765.

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2CatalogPagedTypes are ten catalog types, used where a case needs a listing whose size it can
// state exactly.
//
// Ten because both pages publish a floor of five, so the smallest listing that walks in more than
// one page and ends on a full page is exactly twice that floor. They are two whole families rather
// than a scattering, which is how [ec2InstanceTypeFamilies] is organized, and every one is a real
// catalog member — a type the catalog does not carry would be refused outright by
// DescribeInstanceTypes and silently drop out of the offerings answer.
var ec2CatalogPagedTypes = []string{
	"t3.nano", "t3.micro", "t3.small", "t3.medium", "t3.large", "t3.xlarge", "t3.2xlarge",
	"t3a.nano", "t3a.micro", "t3a.small",
}

// ec2CatalogPagedWalkSize is the MaxResults the walks below are driven at.
//
// Twenty-five rather than the floor of five, because the whole catalog is the listing: at five it
// would take nineteen requests per walk for the types and fifty-seven for the offerings, and what
// the walk asserts — every item once, in order, terminating — does not depend on the page being
// small. The floors and ceilings themselves are asserted by
// TestEC2_CatalogPagination_MaxResultsOutsideTheRangeIsRefused.
const ec2CatalogPagedWalkSize = 25

// ec2CatalogPagedOp is one catalog-backed operation converted onto the shared offset paginator.
type ec2CatalogPagedOp struct {
	// name is the operation, used as the subtest name.
	name string
	// minMaxResults and maxMaxResults are the MaxResults range its own page publishes:
	// API_DescribeInstanceTypes states "Valid Range: Minimum value of 5. Maximum value of 100." and
	// API_DescribeInstanceTypeOfferings the same floor with "Maximum value of 1000."
	minMaxResults int
	maxMaxResults int
	// narrow adds the parameters that cut the listing to [ec2CatalogPagedTypes], so a case can
	// state its size, and merges extra on top.
	//
	// The two do it with different parameters, which is the point of the coexistence case:
	// DescribeInstanceTypes has InstanceType.N and DescribeInstanceTypeOfferings has only an
	// instance-type filter.
	narrow func(extra map[string]string) map[string]string
	// describe sends the operation with extra params and returns one key per item, in the order
	// reported, plus the token the page carried.
	describe func(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string)
	// competing is a parameter set the operation refuses for a reason of its own, and
	// competingCode and competingMarker are what that refusal answers. They exist so the ordering
	// case can show the token is read first, rather than merely that a token refusal happens.
	competing       map[string]string
	competingCode   string
	competingMarker string
}

// maxResultsMessage is the refusal message this operation's published range produces.
func (op ec2CatalogPagedOp) maxResultsMessage() string {
	return ec2MaxResultsMessage(op.minMaxResults, op.maxMaxResults)
}

// ec2CatalogPagedOps is the two operations whose listing is the instance-type catalog.
//
// DescribeSpotPriceHistory is the third and has its own file, because its listing is the catalog
// crossed with the Availability Zones and its cases turn on a price key rather than a type name.
func ec2CatalogPagedOps() []ec2CatalogPagedOp {
	return []ec2CatalogPagedOp{
		{
			name:          "DescribeInstanceTypes",
			minMaxResults: 5,
			maxMaxResults: 100,
			narrow:        ec2CatalogNarrowByTypeList,
			describe:      ec2DescribeCatalogInstanceTypes,
			// An absent type is refused with InvalidInstanceType (ec2CheckInstanceTypesExist), a
			// different code from the token refusal, so this ordering case is decided by the code
			// and not only by the message.
			competing:       map[string]string{"InstanceType.1": "zz9.nonexistent"},
			competingCode:   "InvalidInstanceType",
			competingMarker: "zz9.nonexistent",
		},
		{
			name:          "DescribeInstanceTypeOfferings",
			minMaxResults: 5,
			maxMaxResults: 1000,
			narrow:        ec2CatalogNarrowByTypeFilter,
			describe:      ec2DescribeCatalogOfferings,
			// outpost is a LocationType real EC2 accepts and substrate does not model
			// (ec2UnmodelledLocationTypeError). It carries InvalidParameterValue, the same code as
			// the token refusal, so this one is decided by the message alone.
			competing:       map[string]string{"LocationType": "outpost"},
			competingCode:   "InvalidParameterValue",
			competingMarker: "parameter LocationType",
		},
	}
}

// ec2CatalogNarrowByTypeList cuts DescribeInstanceTypes to [ec2CatalogPagedTypes] with
// InstanceType.N, the parameter that asserts the types exist.
func ec2CatalogNarrowByTypeList(extra map[string]string) map[string]string {
	params := map[string]string{}
	for i, name := range ec2CatalogPagedTypes {
		params["InstanceType."+strconv.Itoa(i+1)] = name
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// ec2CatalogNarrowByTypeFilter cuts DescribeInstanceTypeOfferings to [ec2CatalogPagedTypes] with its
// instance-type filter, and to one location per type with LocationType=region.
//
// The Region arm is what makes the narrowed listing ten items rather than thirty: an offering is a
// type crossed with a location, and only `region` reports exactly one location. Ten is then a listing
// whose size a case can state without knowing how many Availability Zones the Region seeds.
func ec2CatalogNarrowByTypeFilter(extra map[string]string) map[string]string {
	params := map[string]string{
		"LocationType":  "region",
		"Filter.1.Name": "instance-type",
	}
	for i, name := range ec2CatalogPagedTypes {
		params["Filter.1.Value."+strconv.Itoa(i+1)] = name
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// ec2DescribeCatalogInstanceTypes reads a DescribeInstanceTypes page and returns one key per type.
func ec2DescribeCatalogInstanceTypes(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		Types []struct {
			InstanceType string `xml:"instanceType"`
		} `xml:"instanceTypeSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeInstanceTypes", extra), &decoded)
	keys := make([]string, 0, len(decoded.Types))
	for _, item := range decoded.Types {
		keys = append(keys, item.InstanceType)
	}
	return keys, decoded.NextToken
}

// ec2DescribeCatalogOfferings reads a DescribeInstanceTypeOfferings page and returns one key per
// offering.
//
// The key is instanceType@location, which is what an offering is: the same type appears once per
// location, so a key on the type alone would make a repeat across pages invisible.
func ec2DescribeCatalogOfferings(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		Offerings []struct {
			InstanceType string `xml:"instanceType"`
			Location     string `xml:"location"`
		} `xml:"instanceTypeOfferingSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeInstanceTypeOfferings", extra), &decoded)
	keys := make([]string, 0, len(decoded.Offerings))
	for _, item := range decoded.Offerings {
		keys = append(keys, item.InstanceType+"@"+item.Location)
	}
	return keys, decoded.NextToken
}

// TestEC2_CatalogPagination_AbsentMaxResultsReportsTheWholeListing pins the compatibility half of
// the conversion: a caller that never sent the parameter sees no wire change, token included.
//
// It matters more at these two than at the created listings, because the catalog is larger than any
// page a caller is likely to ask for — so an accidental default page size here would truncate the
// answer every existing consumer already gets whole.
func TestEC2_CatalogPagination_AbsentMaxResultsReportsTheWholeListing(t *testing.T) {
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			whole, token := op.describe(t, ts, nil)
			require.Greater(t, len(whole), ec2CatalogPagedWalkSize,
				"the seeded catalog must be larger than a page for the walk cases to mean anything")
			assert.Empty(t, token, "an unpaginated answer must carry no token")
			assert.NotContains(t, ec2DescribeBody(t, ts, ec2PagedParams(op.name, nil)), "<nextToken")
		})
	}
}

// TestEC2_CatalogPagination_AWalkReportsEveryItemExactlyOnce walks the whole listing and compares it
// with the unpaginated answer element for element.
//
// That comparison is the strong one, and what makes it worth stating here is that no state backs
// either listing: the order is the catalog slice's, built once by buildEC2InstanceTypeCatalog, where
// every other converted describe borrows its order from StateManager.List. An off-by-one in the
// offset shows up as a repeat or an omission, and a page cut at the wrong point in the handler shows
// up as a reordering — at the offerings operation that means a cut inside the type/location product,
// which is the one nested loop in the pair.
func TestEC2_CatalogPagination_AWalkReportsEveryItemExactlyOnce(t *testing.T) {
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			whole, _ := op.describe(t, ts, nil)
			var walked []string
			token := ""
			for page := 0; page <= len(whole); page++ {
				extra := map[string]string{"MaxResults": strconv.Itoa(ec2CatalogPagedWalkSize)}
				if token != "" {
					extra["NextToken"] = token
				}
				keys, next := op.describe(t, ts, extra)
				require.LessOrEqual(t, len(keys), ec2CatalogPagedWalkSize,
					"page %d holds more than MaxResults items", page)
				walked = append(walked, keys...)
				if next == "" {
					break
				}
				token = next
			}
			assert.Equal(t, whole, walked, "paging must not change what is reported or its order")
		})
	}
}

// TestEC2_CatalogPagination_AFullLastPageCarriesNoToken covers the listing whose size is an exact
// multiple of the page size, over the ten-type narrowing so the multiple is exact by construction
// rather than by catalog size.
//
// The token is emitted from whether a further item exists, not from whether the page filled up. A
// caller told to keep calling until the token is null would otherwise make one extra request per
// walk, and the empty page it got could be mistaken for a truncated listing.
func TestEC2_CatalogPagination_AFullLastPageCarriesNoToken(t *testing.T) {
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			whole, _ := op.describe(t, ts, op.narrow(nil))
			require.Len(t, whole, len(ec2CatalogPagedTypes),
				"the narrowing must select one item per named type")

			half := len(ec2CatalogPagedTypes) / 2
			first, token := op.describe(t, ts, op.narrow(map[string]string{
				"MaxResults": strconv.Itoa(half),
			}))
			require.Len(t, first, half)
			require.NotEmpty(t, token)

			second, token := op.describe(t, ts, op.narrow(map[string]string{
				"MaxResults": strconv.Itoa(half),
				"NextToken":  token,
			}))
			assert.Len(t, second, half)
			assert.Empty(t, token, "a full last page must not carry a token")
			assert.Equal(t, whole, append(first, second...))

			onePage, token := op.describe(t, ts, op.narrow(map[string]string{
				"MaxResults": strconv.Itoa(len(ec2CatalogPagedTypes)),
			}))
			assert.Equal(t, whole, onePage)
			assert.Empty(t, token, "a page holding the whole listing must not carry a token")
		})
	}
}

// TestEC2_CatalogPagination_AnInventedTokenIsRefused covers the defect #915 named elsewhere: a token
// substrate could not have issued must not select page one.
//
// An offset past the end is the deliberate exception and answers an empty page, for the reason
// ec2NextTokenOffset records — except that here nothing can be deleted, so the case pins the clamp
// itself rather than a caller's recovery from a deletion.
func TestEC2_CatalogPagination_AnInventedTokenIsRefused(t *testing.T) {
	bad := []struct {
		name  string
		token string
	}{
		{"not a number", "abc"},
		{"a negative offset", "-1"},
		{"a number with trailing text", "3x"},
		{"an empty-looking token", " "},
	}
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			for _, tc := range bad {
				t.Run(tc.name, func(t *testing.T) {
					status, code, message := ec2ErrorDetail(t, ts,
						ec2PagedParams(op.name, map[string]string{"NextToken": tc.token}))
					assert.Equal(t, http.StatusBadRequest, status)
					assert.Equal(t, "InvalidParameterValue", code)
					assert.Contains(t, message, "is invalid")
				})
			}

			t.Run("an offset past the end answers an empty page", func(t *testing.T) {
				keys, token := op.describe(t, ts, map[string]string{"NextToken": "99999"})
				assert.Empty(t, keys)
				assert.Empty(t, token)
			})

			t.Run("a token the operation issued resumes the walk", func(t *testing.T) {
				whole, _ := op.describe(t, ts, op.narrow(nil))
				half := len(ec2CatalogPagedTypes) / 2
				first, token := op.describe(t, ts, op.narrow(map[string]string{
					"MaxResults": strconv.Itoa(half),
				}))
				require.Len(t, first, half)
				require.NotEmpty(t, token)
				rest, _ := op.describe(t, ts, op.narrow(map[string]string{"NextToken": token}))
				assert.Equal(t, whole, append(first, rest...))
			})
		})
	}
}

// TestEC2_CatalogPagination_TokenIsRefusedBeforeTheRequestIsRead pins the ordering.
//
// The sealed-store idiom the created listings use is unavailable here — neither handler reads state
// at all — so the ordering is asserted against each operation's other refusal instead: a request
// carrying both a malformed token and something the operation would refuse on its own is answered
// about the token. Each competing refusal is checked to fire on its own first, so a case cannot pass
// by the operation having stopped refusing it. Which of the two AWS answers first is not published,
// so this is substrate's ordering, and it is the one every converted describe uses: the pagination
// parameters are read before anything about the rest of the request.
func TestEC2_CatalogPagination_TokenIsRefusedBeforeTheRequestIsRead(t *testing.T) {
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			status, code, message := ec2ErrorDetail(t, ts, ec2PagedParams(op.name, op.competing))
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, op.competingCode, code, "the competing refusal must fire on its own")
			require.Contains(t, message, op.competingMarker)

			withToken := map[string]string{"NextToken": "abc"}
			for k, v := range op.competing {
				withToken[k] = v
			}
			status, code, message = ec2ErrorDetail(t, ts, ec2PagedParams(op.name, withToken))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "The token 'abc' is invalid")
			assert.NotContains(t, message, op.competingMarker,
				"the token refusal, not the operation's own — at DescribeInstanceTypeOfferings both "+
					"carry InvalidParameterValue, so the message is the only thing that says which "+
					"check ran")
		})
	}
}

// TestEC2_CatalogPagination_MaxResultsOutsideTheRangeIsRefused asserts each page's bound is the one
// it publishes, and that the two are not collapsed into one.
//
// Both publish a floor of five, so four is refused at both where DescribeVolumes accepts one. The
// ceilings differ — a hundred at DescribeInstanceTypes, a thousand at DescribeInstanceTypeOfferings
// — and MaxResults=1000 is asserted in both directions because that is the value the two disagree
// about: accepted at the offerings operation and refused at its sibling. Borrowing either ceiling
// for the other is the borrowing #671 forbids, and it would be a one-line edit in the handler
// that nothing else in the suite would notice.
func TestEC2_CatalogPagination_MaxResultsOutsideTheRangeIsRefused(t *testing.T) {
	refusedEverywhere := []struct {
		name       string
		maxResults string
	}{
		{"a page of zero items", "0"},
		{"a negative page", "-1"},
		{"not a number", "many"},
	}
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			refuse := func(t *testing.T, maxResults string) {
				t.Helper()
				status, code, got := ec2ErrorDetail(t, ts,
					ec2PagedParams(op.name, map[string]string{"MaxResults": maxResults}))
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterValue", code)
				assert.Contains(t, got, op.maxResultsMessage())
			}
			accept := func(t *testing.T, maxResults string) {
				t.Helper()
				keys, _ := op.describe(t, ts, map[string]string{"MaxResults": maxResults})
				assert.NotEmpty(t, keys)
			}

			for _, tc := range refusedEverywhere {
				t.Run(tc.name, func(t *testing.T) { refuse(t, tc.maxResults) })
			}
			t.Run("the published floor itself", func(t *testing.T) {
				accept(t, strconv.Itoa(op.minMaxResults))
			})
			t.Run("one below the published floor", func(t *testing.T) {
				refuse(t, strconv.Itoa(op.minMaxResults-1))
			})
			t.Run("the published ceiling itself", func(t *testing.T) {
				accept(t, strconv.Itoa(op.maxMaxResults))
			})
			t.Run("one above the published ceiling", func(t *testing.T) {
				refuse(t, strconv.Itoa(op.maxMaxResults+1))
			})
			t.Run("a page of 1000, which the two pages disagree about", func(t *testing.T) {
				if op.maxMaxResults == 1000 {
					accept(t, "1000")
					return
				}
				refuse(t, "1000")
			})
		})
	}
}

// TestEC2_CatalogPagination_TheTypeSelectorAndMaxResultsCoexist is the case that exists because of a
// rule these two operations are *outside* of.
//
// The service-wide prohibition is stated against "a list of IDs" — "If you call a describe API
// action with both a list of IDs and MaxResults, the request fails with the error
// InvalidParameterCombination" — and neither page has an ID-list parameter.
// DescribeInstanceTypeOfferings has no list parameter at all: its request is DryRun, Filter.N,
// LocationType, MaxResults and NextToken. DescribeInstanceTypes has InstanceType.N, which is a
// stronger parameter than the spot-price operation's namesake — it asserts the types exist rather
// than filtering by them — but what it names are catalog members, not resources the account holds,
// so an instance type is still not a resource ID.
//
// Asserted rather than assumed, because the nine other converted describes with an ID list all do
// refuse the combination and a sweep is exactly where a published rule gets applied one operation
// too far.
func TestEC2_CatalogPagination_TheTypeSelectorAndMaxResultsCoexist(t *testing.T) {
	for _, op := range ec2CatalogPagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			half := len(ec2CatalogPagedTypes) / 2
			keys, token := op.describe(t, ts, op.narrow(map[string]string{
				"MaxResults": strconv.Itoa(half),
			}))
			assert.Len(t, keys, half, "the type selector and MaxResults must both be read")
			assert.NotEmpty(t, token, "a narrowing larger than the page must still hand back a token")
		})
	}
}
