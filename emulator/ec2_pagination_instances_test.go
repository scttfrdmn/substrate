package emulator_test

// Offset pagination for DescribeInstances (#917).
//
// This operation is the one converted describe whose answer is nested — reservationSet > item >
// instancesSet — so it pages through ec2PageReservations rather than the flat ec2Page every other
// listing uses, and it gets its own cases rather than a row in ec2_pagination_test.go's table.
//
// The decision those cases exist to pin is what MaxResults counts. AWS publishes only "the maximum
// number of items to return for this request" and never says which of the two nested lists an item
// is, so substrate reads it as **instances**: counting reservations would leave the parameter unable
// to bound a response at all, because one RunInstances with MinCount=500 is a single reservation, and
// NextToken's own text — "Pagination continues from the end of the items returned by the previous
// request" — describes a position in a flat sequence. The visible consequence, that a reservation
// straddling a page boundary is reported on both pages, is substrate's reading too and is asserted
// here rather than left to be discovered.
//
// Every case drives real RunInstances calls and the query protocol, per #765's rule that state
// written directly cannot prove a value is observable through the owning service's own call.

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2PagedReservation is one reservationSet member as a caller decodes it, reduced to the two
// things every case here asserts on: which reservation it is, and which instances it reported.
type ec2PagedReservation struct {
	// ReservationID is the reservation's ID, which a straddling reservation repeats across pages.
	ReservationID string
	// InstanceIDs are the instances this page reported under that reservation, in order.
	InstanceIDs []string
}

// ec2RunPagedInstances launches one reservation of count instances and returns it.
//
// MinCount and MaxCount are both count, so the reservation is a single launch: that is the shape
// that makes a reservation larger than a page reachable at all, and it is what a caller asking for
// capacity actually sends.
func ec2RunPagedInstances(t *testing.T, ts *httptest.Server, count int) ec2PagedReservation {
	t.Helper()
	var launched struct {
		ReservationID string `xml:"reservationId"`
		Instances     []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  ec2TestImage,
		"MinCount": strconv.Itoa(count),
		"MaxCount": strconv.Itoa(count),
	}, &launched)
	require.NotEmpty(t, launched.ReservationID)
	require.Len(t, launched.Instances, count)
	reservation := ec2PagedReservation{ReservationID: launched.ReservationID}
	for _, inst := range launched.Instances {
		reservation.InstanceIDs = append(reservation.InstanceIDs, inst.InstanceID)
	}
	return reservation
}

// ec2DescribePagedInstances reads a DescribeInstances page.
//
// Both IDs are read as direct children of their own element, which matters for the same reason
// [ec2DescribePagedVolumes]' does: an instance item nests further identifiers below itself, so a
// decoder matching an element name at any depth would over-collect.
func ec2DescribePagedInstances(t *testing.T, ts *httptest.Server, extra map[string]string) ([]ec2PagedReservation, string) {
	t.Helper()
	var decoded struct {
		XMLName      xml.Name `xml:"DescribeInstancesResponse"`
		Reservations []struct {
			ReservationID string `xml:"reservationId"`
			Instances     []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeInstances", extra), &decoded)
	out := make([]ec2PagedReservation, 0, len(decoded.Reservations))
	for _, res := range decoded.Reservations {
		item := ec2PagedReservation{ReservationID: res.ReservationID}
		for _, inst := range res.Instances {
			item.InstanceIDs = append(item.InstanceIDs, inst.InstanceID)
		}
		out = append(out, item)
	}
	return out, decoded.NextToken
}

// ec2FlattenPagedInstances is every instance ID in reservations, in the order reported.
func ec2FlattenPagedInstances(reservations []ec2PagedReservation) []string {
	var ids []string
	for _, res := range reservations {
		ids = append(ids, res.InstanceIDs...)
	}
	return ids
}

// ec2WalkPagedInstances pages DescribeInstances at pageSize and returns each page's reservations.
//
// It asserts the two rules the walk itself has to obey: no page reports more than pageSize
// *instances* — which is the substrate reading under test, since a page counting reservations would
// blow that bound the moment one reservation held more — and the walk ends with a page carrying no
// token. The iteration cap is a guard rather than an expectation: a paginator re-issuing one token
// would otherwise hang the suite instead of failing it.
func ec2WalkPagedInstances(t *testing.T, ts *httptest.Server, pageSize, wantInstances int) [][]ec2PagedReservation {
	t.Helper()
	var pages [][]ec2PagedReservation
	token := ""
	for page := 0; page <= wantInstances+2; page++ {
		extra := map[string]string{"MaxResults": strconv.Itoa(pageSize)}
		if token != "" {
			extra["NextToken"] = token
		}
		reservations, next := ec2DescribePagedInstances(t, ts, extra)
		require.LessOrEqual(t, len(ec2FlattenPagedInstances(reservations)), pageSize,
			"page %d reports more instances than MaxResults", page)
		pages = append(pages, reservations)
		if next == "" {
			return pages
		}
		token = next
	}
	t.Fatalf("DescribeInstances did not stop paging at %d instances per page", pageSize)
	return nil
}

// TestEC2_DescribeInstances_AbsentMaxResultsReportsTheWholeListing pins the compatibility half of
// the conversion: a caller that never sent MaxResults sees exactly what it saw before this
// operation paginated, token included — the element is omitted rather than emitted empty.
func TestEC2_DescribeInstances_AbsentMaxResultsReportsTheWholeListing(t *testing.T) {
	ts := newEC2TestServer(t)
	first := ec2RunPagedInstances(t, ts, 2)
	second := ec2RunPagedInstances(t, ts, 3)

	reservations, token := ec2DescribePagedInstances(t, ts, nil)
	assert.Len(t, reservations, 2)
	assert.ElementsMatch(t, append(first.InstanceIDs, second.InstanceIDs...),
		ec2FlattenPagedInstances(reservations))
	assert.Empty(t, token, "an unpaginated answer must carry no token")
	assert.NotContains(t, ec2DescribeBody(t, ts, ec2PagedParams("DescribeInstances", nil)), "<nextToken")
}

// TestEC2_DescribeInstances_AWalkReportsEveryInstanceExactlyOnce is #917's first acceptance criterion
// at the nested listing: a listing larger than the page size is walked and every record is reported
// once.
//
// Three reservations of different sizes, so the page boundary lands inside one of them rather than
// conveniently between two. The walk is compared against the unpaginated answer element for element
// rather than against the launch order, because the order is the state store's (#865's
// lexicographic List guarantee) and the reservations' own order is by reservation ID.
func TestEC2_DescribeInstances_AWalkReportsEveryInstanceExactlyOnce(t *testing.T) {
	ts := newEC2TestServer(t)
	total := 0
	for _, count := range []int{3, 1, 4} {
		total += len(ec2RunPagedInstances(t, ts, count).InstanceIDs)
	}

	whole, _ := ec2DescribePagedInstances(t, ts, nil)
	flat := ec2FlattenPagedInstances(whole)
	require.Len(t, flat, total)

	var walked []string
	for _, page := range ec2WalkPagedInstances(t, ts, 3, total) {
		walked = append(walked, ec2FlattenPagedInstances(page)...)
	}
	assert.Equal(t, flat, walked, "paging must not change which instances are reported or their order")
}

// TestEC2_DescribeInstances_AStraddlingReservationIsReportedOnBothPages records the visible
// consequence of counting instances rather than reservations.
//
// One reservation of four instances at three per page: the reservation appears on both pages,
// carrying three instances and then one. AWS publishes nothing about this, so it is substrate's
// reading — and the alternative, never splitting a reservation, would have to answer either more
// instances than MaxResults asked for or fewer than are available with a token. What a caller must
// be able to rely on is that the instances do not repeat, which is the assertion below.
func TestEC2_DescribeInstances_AStraddlingReservationIsReportedOnBothPages(t *testing.T) {
	ts := newEC2TestServer(t)
	launched := ec2RunPagedInstances(t, ts, 4)

	first, token := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "3"})
	require.Len(t, first, 1)
	require.Equal(t, launched.ReservationID, first[0].ReservationID)
	require.Len(t, first[0].InstanceIDs, 3)
	require.NotEmpty(t, token)

	second, token := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "3", "NextToken": token})
	require.Len(t, second, 1)
	assert.Equal(t, launched.ReservationID, second[0].ReservationID,
		"a straddling reservation is reported under its own ID on both pages")
	assert.Len(t, second[0].InstanceIDs, 1)
	assert.Empty(t, token)

	assert.ElementsMatch(t, launched.InstanceIDs,
		append(first[0].InstanceIDs, second[0].InstanceIDs...),
		"an instance must be reported on exactly one page")
}

// TestEC2_DescribeInstances_AFullLastPageCarriesNoToken covers the listing whose instance count is an
// exact multiple of the page size.
//
// The token is emitted from whether a further instance exists, not from whether the page filled up,
// so six instances at three per page is two pages and not three-with-an-empty-tail — a caller told
// to keep calling until the token is null would otherwise make one extra request per walk.
func TestEC2_DescribeInstances_AFullLastPageCarriesNoToken(t *testing.T) {
	ts := newEC2TestServer(t)
	launched := append(ec2RunPagedInstances(t, ts, 3).InstanceIDs,
		ec2RunPagedInstances(t, ts, 3).InstanceIDs...)

	first, token := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "3"})
	require.Len(t, ec2FlattenPagedInstances(first), 3)
	require.NotEmpty(t, token)

	second, token := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "3", "NextToken": token})
	assert.Len(t, ec2FlattenPagedInstances(second), 3)
	assert.Empty(t, token, "a full last page must not carry a token")
	assert.ElementsMatch(t, launched,
		append(ec2FlattenPagedInstances(first), ec2FlattenPagedInstances(second)...))
}

// TestEC2_DescribeInstances_AnInventedTokenIsRefused covers the defect #915 named at four other
// operations: a token substrate could not have issued must not select page one.
//
// A token past the end is the deliberate exception and answers an empty page, because a caller
// resuming a walk after an instance was terminated holds a token that was valid when issued.
func TestEC2_DescribeInstances_AnInventedTokenIsRefused(t *testing.T) {
	ts := newEC2TestServer(t)
	launched := ec2RunPagedInstances(t, ts, 3)

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
				ec2PagedParams("DescribeInstances", map[string]string{"NextToken": tc.token}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "is invalid")
		})
	}

	t.Run("an offset past the end answers an empty page", func(t *testing.T) {
		reservations, token := ec2DescribePagedInstances(t, ts, map[string]string{"NextToken": "9999"})
		assert.Empty(t, reservations)
		assert.Empty(t, token)
	})

	t.Run("a token the operation issued resumes the walk", func(t *testing.T) {
		first, token := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "1"})
		require.Len(t, ec2FlattenPagedInstances(first), 1)
		require.NotEmpty(t, token)
		rest, _ := ec2DescribePagedInstances(t, ts, map[string]string{"NextToken": token})
		assert.ElementsMatch(t, launched.InstanceIDs,
			append(ec2FlattenPagedInstances(first), ec2FlattenPagedInstances(rest)...))
	})
}

// TestEC2_DescribeInstances_TokenIsRefusedBeforeStateIsRead pins the ordering that makes the refusal
// predictable: a caller gets the same answer to a malformed token whether the account holds no
// instances or ten thousand.
//
// Asserted by sealing the state store against reads and requiring the refusal anyway — the idiom
// #887 established and #915's tests use. This operation reads state in three places before it
// renders, so a handler validating late would answer 500 from the sealed store instead.
func TestEC2_DescribeInstances_TokenIsRefusedBeforeStateIsRead(t *testing.T) {
	sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
	ts := newEC2TestServerWithState(t, sealed)
	ec2RunPagedInstances(t, ts, 2)
	sealed.sealGets = true
	sealed.sealLists = true

	status, code, _ := ec2ErrorDetail(t, ts,
		ec2PagedParams("DescribeInstances", map[string]string{"NextToken": "abc"}))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
}

// TestEC2_DescribeInstances_MaxResultsIsBoundedOnlyBySubstratesFloor covers the range at the page
// that publishes none.
//
// API_DescribeInstances says only "The maximum number of items to return for this request", type
// Integer, with no Valid Range and no Default — where API_DescribeVpcs, API_DescribeSubnets and
// API_DescribeSecurityGroups all publish 5 to 1000. Per #671 that range is not borrowed by analogy,
// which the accepted 5000 case pins: it would fail against a ceiling nothing published. The floor of
// one is substrate's reading and is forced by the published pagination rule — a page of zero items
// describes a walk that answers nothing and hands back a token forever.
func TestEC2_DescribeInstances_MaxResultsIsBoundedOnlyBySubstratesFloor(t *testing.T) {
	ts := newEC2TestServer(t)
	launched := ec2RunPagedInstances(t, ts, 2)

	for _, tc := range []struct {
		name       string
		maxResults string
		refused    bool
	}{
		{"a page of zero instances", "0", true},
		{"a negative page", "-1", true},
		{"not a number", "many", true},
		{"the floor substrate reads", "1", false},
		{"above the range the siblings publish", "5000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := ec2PagedParams("DescribeInstances", map[string]string{"MaxResults": tc.maxResults})
			if !tc.refused {
				reservations, _ := ec2DescribePagedInstances(t, ts,
					map[string]string{"MaxResults": tc.maxResults})
				reported := ec2FlattenPagedInstances(reservations)
				assert.NotEmpty(t, reported)
				assert.LessOrEqual(t, len(reported), len(launched.InstanceIDs))
				return
			}
			status, code, message := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "MaxResults must be at least 1")
		})
	}
}

// TestEC2_DescribeInstances_AnInstanceIDListWithMaxResultsIsRefused covers the rule this page states
// in its own words rather than only service-wide.
//
// Query-Requests.html publishes it for every describe — "If you call a describe API action with both
// a list of IDs and MaxResults, the request fails with the error InvalidParameterCombination" — and
// API_DescribeInstances is the one page of the nine that repeats it against its own parameter: "You
// cannot specify this parameter and the instance IDs parameter in the same request." So the code is
// published twice over and only the message wording is substrate's.
//
// Each parameter is also asserted accepted on its own, because a handler that refused the ID list
// outright would satisfy the first assertion.
func TestEC2_DescribeInstances_AnInstanceIDListWithMaxResultsIsRefused(t *testing.T) {
	ts := newEC2TestServer(t)
	launched := ec2RunPagedInstances(t, ts, 3)

	t.Run("together", func(t *testing.T) {
		status, code, message := ec2ErrorDetail(t, ts, ec2PagedParams("DescribeInstances", map[string]string{
			"InstanceId.1": launched.InstanceIDs[0],
			"MaxResults":   "5",
		}))
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterCombination", code)
		assert.Contains(t, message, "InstanceId")
		assert.Contains(t, message, "MaxResults")
	})

	t.Run("an ID list alone", func(t *testing.T) {
		reservations, token := ec2DescribePagedInstances(t, ts,
			map[string]string{"InstanceId.1": launched.InstanceIDs[0]})
		assert.Equal(t, []string{launched.InstanceIDs[0]}, ec2FlattenPagedInstances(reservations))
		assert.Empty(t, token)
	})

	t.Run("MaxResults alone", func(t *testing.T) {
		reservations, _ := ec2DescribePagedInstances(t, ts, map[string]string{"MaxResults": "2"})
		assert.Len(t, ec2FlattenPagedInstances(reservations), 2)
	})

	// The refusal is checked before the ID list's own syntax, because whether two parameters may
	// appear together does not depend on either being well formed. Which of the two AWS answers
	// first is not published, so this pins substrate's ordering rather than AWS's.
	t.Run("before the ID list's own syntax check", func(t *testing.T) {
		_, code, _ := ec2ErrorDetail(t, ts, ec2PagedParams("DescribeInstances", map[string]string{
			"InstanceId.1": "not-an-id",
			"MaxResults":   "5",
		}))
		assert.Equal(t, "InvalidParameterCombination", code)
	})
}
