package emulator_test

// Offset pagination for DescribeSpotPriceHistory (#1024).
//
// API_DescribeSpotPriceHistory publishes MaxResults and NextToken and substrate read neither, so
// the operation answered its whole assembled history with no token: a caller paging it found one
// page here and several in production.
//
// It gets its own cases rather than a row in ec2_pagination_test.go's table for one reason — that
// table's every case begins by *creating* the listing it walks, and this listing cannot be created.
// It is the instance-type catalog crossed with the Availability Zones of the request's Region, so it
// exists in a fresh account, at a size no test chose. The five properties the table asserts are
// asserted here against that fixed listing instead, and two further ones the table cannot reach:
// that the offset is stable over a listing built from no state at all, and that this is the one
// converted operation with no ID-list parameter for MaxResults to conflict with.
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

// ec2SpotPricePagedType and its sibling are two catalog types used to cut the listing down to a
// size a case can state exactly.
//
// Two types across the three seeded Availability Zones is six prices, because every price carries
// the one product description substrate models. Both are real catalog members, so the filter
// narrows rather than empties: InstanceType.N here is documented as "Filters the results by the
// specified instance types", which is why an unknown type would answer nothing rather than refuse.
const (
	ec2SpotPricePagedType      = "t3.micro"
	ec2SpotPricePagedOtherType = "c5.large"
	ec2SpotPricePagedTotal     = 6
)

// ec2DescribeSpotPrices reads a DescribeSpotPriceHistory page and returns one key per price, in the
// order reported, plus the token the page carried.
//
// The key is instanceType@availabilityZone, which is unique across the whole listing: it is the pair
// the answer is a product of, and the only other member that varies with it — the price — is a
// function of the type alone.
func ec2DescribeSpotPrices(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		Prices []struct {
			InstanceType     string `xml:"instanceType"`
			AvailabilityZone string `xml:"availabilityZone"`
		} `xml:"spotPriceHistorySet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeSpotPriceHistory", extra), &decoded)
	keys := make([]string, 0, len(decoded.Prices))
	for _, price := range decoded.Prices {
		keys = append(keys, price.InstanceType+"@"+price.AvailabilityZone)
	}
	return keys, decoded.NextToken
}

// ec2SpotPricePagedParams narrows the listing to the two types above, plus extra.
func ec2SpotPricePagedParams(extra map[string]string) map[string]string {
	params := map[string]string{
		"InstanceType.1": ec2SpotPricePagedType,
		"InstanceType.2": ec2SpotPricePagedOtherType,
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// TestEC2_SpotPriceHistoryPagination_AbsentMaxResultsReportsTheWholeHistory pins the compatibility
// half of the conversion: a caller that never sent the parameter sees no wire change, token
// included.
//
// The element is omitted rather than emitted empty, which is a choice this page in particular
// invites the other way: its Example Response shows `<nextToken/>` on a last page and the member is
// documented as "an empty string ("") or null when there are no more items". Both shapes are
// published; substrate answers the one every other converted describe answers, and a caller
// decoding into a string reads "" from either.
func TestEC2_SpotPriceHistoryPagination_AbsentMaxResultsReportsTheWholeHistory(t *testing.T) {
	ts := newEC2TestServer(t)

	whole, token := ec2DescribeSpotPrices(t, ts, nil)
	require.Greater(t, len(whole), ec2PagedPageSize,
		"the seeded catalog must be larger than a page for the walk below to mean anything")
	assert.Empty(t, token, "an unpaginated answer must carry no token")
	assert.NotContains(t, ec2DescribeBody(t, ts, ec2PagedParams("DescribeSpotPriceHistory", nil)),
		"<nextToken")
}

// TestEC2_SpotPriceHistoryPagination_AWalkReportsEveryPriceExactlyOnce walks the whole history and
// compares it with the unpaginated answer element for element.
//
// That comparison is the strong one, and it is what the absence of any state makes worth stating:
// the offset is only meaningful because the answer is assembled in a fixed order — the catalog slice
// crossed with a fixed Availability Zone list — where every other converted describe borrows its
// order from StateManager.List. An off-by-one shows up as a repeat or an omission, and a page cut at
// the wrong point in the loop shows up as a reordering.
func TestEC2_SpotPriceHistoryPagination_AWalkReportsEveryPriceExactlyOnce(t *testing.T) {
	ts := newEC2TestServer(t)

	whole, _ := ec2DescribeSpotPrices(t, ts, nil)
	pageSize := 25

	var walked []string
	token := ""
	for page := 0; page <= len(whole); page++ {
		extra := map[string]string{"MaxResults": strconv.Itoa(pageSize)}
		if token != "" {
			extra["NextToken"] = token
		}
		keys, next := ec2DescribeSpotPrices(t, ts, extra)
		require.LessOrEqual(t, len(keys), pageSize, "page %d holds more than MaxResults items", page)
		walked = append(walked, keys...)
		if next == "" {
			break
		}
		token = next
	}
	assert.Equal(t, whole, walked, "paging must not change what is reported or its order")
}

// TestEC2_SpotPriceHistoryPagination_AFullLastPageCarriesNoToken covers the history whose size is an
// exact multiple of the page size, over the two-type listing so the multiple is exact by
// construction rather than by catalog size.
//
// The token is emitted from whether a further price exists, not from whether the page filled up. A
// caller told to keep calling until the token is null would otherwise make one extra request per
// walk, and the empty page it got could be mistaken for a truncated history.
func TestEC2_SpotPriceHistoryPagination_AFullLastPageCarriesNoToken(t *testing.T) {
	ts := newEC2TestServer(t)

	whole, _ := ec2DescribeSpotPrices(t, ts, ec2SpotPricePagedParams(nil))
	require.Len(t, whole, ec2SpotPricePagedTotal,
		"two catalog types across the seeded zones is a fixed listing size")

	half := ec2SpotPricePagedTotal / 2
	first, token := ec2DescribeSpotPrices(t, ts,
		ec2SpotPricePagedParams(map[string]string{"MaxResults": strconv.Itoa(half)}))
	require.Len(t, first, half)
	require.NotEmpty(t, token)

	second, token := ec2DescribeSpotPrices(t, ts, ec2SpotPricePagedParams(
		map[string]string{"MaxResults": strconv.Itoa(half), "NextToken": token}))
	assert.Len(t, second, half)
	assert.Empty(t, token, "a full last page must not carry a token")
	assert.Equal(t, whole, append(first, second...))

	onePage, token := ec2DescribeSpotPrices(t, ts,
		ec2SpotPricePagedParams(map[string]string{"MaxResults": strconv.Itoa(ec2SpotPricePagedTotal)}))
	assert.Equal(t, whole, onePage)
	assert.Empty(t, token, "a page that holds the whole history must not carry a token")
}

// TestEC2_SpotPriceHistoryPagination_AnInventedTokenIsRefused covers the defect #915 named
// elsewhere: a token substrate could not have issued must not select page one.
//
// An offset past the end is the deliberate exception and answers an empty page, for the reason
// [ec2NextTokenOffset] records — except that here no price can be deleted, so the case pins the
// clamp itself rather than a caller's recovery from a deletion.
func TestEC2_SpotPriceHistoryPagination_AnInventedTokenIsRefused(t *testing.T) {
	ts := newEC2TestServer(t)

	for _, tc := range []struct {
		name  string
		token string
	}{
		{"not a number", "abc"},
		{"a negative offset", "-1"},
		{"a number with trailing text", "3x"},
		{"an empty-looking token", " "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := ec2ErrorDetail(t, ts,
				ec2PagedParams("DescribeSpotPriceHistory", map[string]string{"NextToken": tc.token}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "is invalid")
		})
	}

	t.Run("an offset past the end answers an empty page", func(t *testing.T) {
		keys, token := ec2DescribeSpotPrices(t, ts, map[string]string{"NextToken": "99999"})
		assert.Empty(t, keys)
		assert.Empty(t, token)
	})

	t.Run("a token the operation issued resumes the walk", func(t *testing.T) {
		whole, _ := ec2DescribeSpotPrices(t, ts, ec2SpotPricePagedParams(nil))
		first, token := ec2DescribeSpotPrices(t, ts,
			ec2SpotPricePagedParams(map[string]string{"MaxResults": "2"}))
		require.Len(t, first, 2)
		require.NotEmpty(t, token)
		rest, _ := ec2DescribeSpotPrices(t, ts,
			ec2SpotPricePagedParams(map[string]string{"NextToken": token}))
		assert.Equal(t, whole, append(first, rest...))
	})
}

// TestEC2_SpotPriceHistoryPagination_TokenIsRefusedBeforeTheFilterNamesAreChecked pins the ordering.
//
// The sealed-store idiom the other converted describes use is unavailable here — this operation
// reads no state at all — so what the ordering is asserted against instead is the operation's other
// refusal: a request carrying both an unsupported filter name and a malformed token is answered
// about the token. Which refusal AWS answers first is not published, so this is substrate's
// ordering, and it is the same one every converted describe uses: the pagination parameters are read
// before anything about the request's own shape.
func TestEC2_SpotPriceHistoryPagination_TokenIsRefusedBeforeTheFilterNamesAreChecked(t *testing.T) {
	ts := newEC2TestServer(t)

	status, code, message := ec2ErrorDetail(t, ts, ec2PagedParams("DescribeSpotPriceHistory",
		map[string]string{
			"NextToken":        "abc",
			"Filter.1.Name":    "no-such-filter",
			"Filter.1.Value.1": "anything",
		}))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Contains(t, message, "The token 'abc' is invalid")
	assert.NotContains(t, message, "no-such-filter",
		"the token refusal, not the unsupported-filter one — both carry InvalidParameterValue, "+
			"so the message is the only thing that says which check ran")
}

// TestEC2_SpotPriceHistoryPagination_MaxResultsOutsideTheRangeIsRefused asserts this page's bound is
// the one it publishes, which is none.
//
// It states only "The maximum number of items to return for this request", type Integer, with no
// Valid Range line, so substrate does not borrow the 5–1000 three sibling describes publish (#671):
// MaxResults=1 and MaxResults=5000 are both accepted. The floor of one is substrate's reading and
// the only bound the published pagination rule forces, since a page of zero items describes a walk
// that can never advance.
func TestEC2_SpotPriceHistoryPagination_MaxResultsOutsideTheRangeIsRefused(t *testing.T) {
	ts := newEC2TestServer(t)

	for _, tc := range []struct {
		name       string
		maxResults string
	}{
		{"a page of zero items", "0"},
		{"a negative page", "-1"},
		{"not a number", "many"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := ec2ErrorDetail(t, ts, ec2PagedParams("DescribeSpotPriceHistory",
				map[string]string{"MaxResults": tc.maxResults}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "MaxResults must be at least 1")
		})
	}

	for _, accepted := range []string{"1", "5000"} {
		t.Run("a page of "+accepted+" is accepted where no range is published", func(t *testing.T) {
			keys, _ := ec2DescribeSpotPrices(t, ts, map[string]string{"MaxResults": accepted})
			assert.NotEmpty(t, keys)
		})
	}
}

// TestEC2_SpotPriceHistoryPagination_InstanceTypesAndMaxResultsCoexist is the one case that exists
// because of a rule this operation is *outside* of.
//
// The service-wide prohibition is stated against "a list of IDs" — "If you call a describe API
// action with both a list of IDs and MaxResults, the request fails with the error
// InvalidParameterCombination" — and this page has no ID-list parameter. InstanceType.N is a filter,
// and an instance type is not a resource ID, so applying the refusal here would extend a published
// rule to a parameter it does not name. Asserted rather than assumed, because the seven other
// converted flat describes all do refuse the combination and a sweep is exactly where that gets
// applied one operation too far.
func TestEC2_SpotPriceHistoryPagination_InstanceTypesAndMaxResultsCoexist(t *testing.T) {
	ts := newEC2TestServer(t)

	keys, token := ec2DescribeSpotPrices(t, ts,
		ec2SpotPricePagedParams(map[string]string{"MaxResults": "2"}))
	assert.Len(t, keys, 2, "the filter and MaxResults must both be read")
	assert.NotEmpty(t, token)
}
