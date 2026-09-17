package emulator_test

// Offset pagination shared by the EC2 describes (#917).
//
// DescribeVolumes and DescribeSnapshots published MaxResults and NextToken and implemented
// neither: a request naming either was answered with the whole listing and no token, so a caller
// paging in one page against substrate found a second page in production. The two operations that
// already paginated — DescribeTags and DescribeLaunchTemplateVersions — carried a private copy of
// the same three rules each, and their behavior is unchanged by the conversion, which
// TestEC2_DescribeTags_Pagination and TestEC2_DescribeLaunchTemplateVersions' own MaxResults cases
// are the regression guard for.
//
// What the shared helpers must get right comes from Query-Requests.html → Pagination, which is
// where AWS publishes the mechanism once rather than per operation:
//
//	"With pagination, you continue to call the action until nextToken is null, even if you receive
//	less than MaxResults items, including zero items."
//
// So the assertion that matters most is not "a page holds MaxResults items" but that a *walk*
// reports every record exactly once and terminates: the last page must carry no token, or a
// conforming caller loops forever. Each case below runs against both converted operations through
// the query protocol, over resources created by real calls, per #765's rule that state written
// directly cannot prove a value is observable through the owning service's own call.

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

// ec2PagedOp is one operation #917 converted onto the shared offset paginator, with everything a
// case needs to drive it: how to create a listing, how to read the answer, and the name of the
// ID-list parameter MaxResults may not accompany.
//
// The two are exercised from one table because the rules under test are the helpers' and not the
// operations' — an assertion written against volumes alone would pass while snapshots, whose page
// is cut at a different point in the handler, reported a record twice.
type ec2PagedOp struct {
	// name is the operation, used as the subtest name.
	name string
	// idParam is the resource-ID list parameter, unindexed as AWS names it.
	idParam string
	// create makes n records through real calls and returns their IDs.
	create func(t *testing.T, ts *httptest.Server, n int) []string
	// describe sends the operation with extra params and returns the IDs it reported, in the
	// order it reported them, and the token the page carried.
	describe func(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string)
}

// ec2PagedOps is every operation #917's first part converted.
func ec2PagedOps() []ec2PagedOp {
	return []ec2PagedOp{
		{
			name:     "DescribeVolumes",
			idParam:  "VolumeId",
			create:   ec2CreatePagedVolumes,
			describe: ec2DescribePagedVolumes,
		},
		{
			name:     "DescribeSnapshots",
			idParam:  "SnapshotId",
			create:   ec2CreatePagedSnapshots,
			describe: ec2DescribePagedSnapshots,
		},
	}
}

// ec2CreatePagedVolumes creates n volumes and returns their IDs.
func ec2CreatePagedVolumes(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, ec2CreateVolume(t, ts, strconv.Itoa(8+i)))
	}
	return ids
}

// ec2CreatePagedSnapshots creates n snapshots of one volume and returns their IDs.
//
// One volume rather than n is deliberate: it is the listing a real caller pages, and it means the
// volume the snapshots are taken from cannot be mistaken for one of the records being counted.
func ec2CreatePagedSnapshots(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	volumeID := ec2CreateVolume(t, ts, "8")
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var created struct {
			SnapshotID string `xml:"snapshotId"`
		}
		ec2DescribeXML(t, ts, map[string]string{
			"Action":      "CreateSnapshot",
			"VolumeId":    volumeID,
			"Description": "snapshot " + strconv.Itoa(i),
		}, &created)
		require.NotEmpty(t, created.SnapshotID)
		ids = append(ids, created.SnapshotID)
	}
	return ids
}

// ec2DescribePagedVolumes reads a DescribeVolumes page.
//
// The volume ID is read as a direct child of volumeSet>item, which matters: a volume item nests a
// second volumeId inside attachmentSet>item, so a decoder matching the element name at any depth
// would over-collect and report an attached volume twice.
func ec2DescribePagedVolumes(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeVolumesResponse"`
		Volumes []struct {
			VolumeID string `xml:"volumeId"`
		} `xml:"volumeSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeVolumes", extra), &decoded)
	ids := make([]string, 0, len(decoded.Volumes))
	for _, vol := range decoded.Volumes {
		ids = append(ids, vol.VolumeID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedSnapshots reads a DescribeSnapshots page.
func ec2DescribePagedSnapshots(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName   xml.Name `xml:"DescribeSnapshotsResponse"`
		Snapshots []struct {
			SnapshotID string `xml:"snapshotId"`
		} `xml:"snapshotSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeSnapshots", extra), &decoded)
	ids := make([]string, 0, len(decoded.Snapshots))
	for _, snap := range decoded.Snapshots {
		ids = append(ids, snap.SnapshotID)
	}
	return ids, decoded.NextToken
}

// ec2PagedParams is extra with the action added, so a case can name only what it is testing.
func ec2PagedParams(action string, extra map[string]string) map[string]string {
	params := map[string]string{"Action": action}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

// ec2PagedWalk pages an operation at pageSize and returns every ID it reported, in order.
//
// It asserts the two rules the walk itself has to obey: no page holds more than pageSize items,
// and the walk ends with a page carrying no token. The iteration cap is a guard rather than an
// expectation — a paginator that re-issued the same token would otherwise hang the suite instead
// of failing it.
func ec2PagedWalk(t *testing.T, ts *httptest.Server, op ec2PagedOp, pageSize, want int) []string {
	t.Helper()
	var seen []string
	token := ""
	for page := 0; page <= want+2; page++ {
		extra := map[string]string{"MaxResults": strconv.Itoa(pageSize)}
		if token != "" {
			extra["NextToken"] = token
		}
		ids, next := op.describe(t, ts, extra)
		require.LessOrEqual(t, len(ids), pageSize, "page %d holds more than MaxResults items", page)
		seen = append(seen, ids...)
		if next == "" {
			return seen
		}
		token = next
	}
	t.Fatalf("%s did not stop paging at %d items per page", op.name, pageSize)
	return nil
}

// TestEC2_OffsetPagination_AbsentMaxResultsReportsTheWholeListing pins the compatibility half of
// the conversion.
//
// API_DescribeVolumes and API_DescribeSnapshots publish no unpaginated default — the sentence
// belongs to API_DescribeSecurityGroups, "If this parameter is not specified, then all items are
// returned" — so substrate reads an absent MaxResults as the whole listing at both, which is also
// what each answered before it paginated. A caller that never sent MaxResults therefore sees no
// wire change at all, token included: the element is omitted rather than emitted empty.
func TestEC2_OffsetPagination_AbsentMaxResultsReportsTheWholeListing(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 7)

			ids, token := op.describe(t, ts, nil)
			assert.ElementsMatch(t, created, ids)
			assert.Empty(t, token, "an unpaginated answer must carry no token")
			assert.NotContains(t, ec2DescribeBody(t, ts, ec2PagedParams(op.name, nil)), "<nextToken")
		})
	}
}

// TestEC2_OffsetPagination_AWalkReportsEveryRecordExactlyOnce is #917's first acceptance
// criterion: a listing larger than the page size is walked and every record is reported once.
//
// The walk is compared against the unpaginated answer element for element rather than against the
// creation order, because the order is the state store's (#865's lexicographic List guarantee) and
// not the order the records were made in. That comparison is the strong one: an off-by-one in the
// offset shows up as a repeat or an omission, and a page cut at the wrong point in the handler
// shows up as a reordering.
func TestEC2_OffsetPagination_AWalkReportsEveryRecordExactlyOnce(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 7)

			whole, _ := op.describe(t, ts, nil)
			require.Len(t, whole, len(created))

			walked := ec2PagedWalk(t, ts, op, 3, len(created))
			assert.Equal(t, whole, walked, "paging must not change what is reported or its order")
		})
	}
}

// TestEC2_OffsetPagination_AFullLastPageCarriesNoToken covers the listing whose size is an exact
// multiple of the page size.
//
// The token is emitted from whether a further record exists, not from whether the page filled up,
// so six records at three per page is two pages and not three-with-an-empty-tail. Getting this
// backwards is not a crash: a caller told to keep calling until the token is null would make one
// extra request per walk, which only shows up as a wasted round trip until the empty page is
// mistaken for a truncated listing.
func TestEC2_OffsetPagination_AFullLastPageCarriesNoToken(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 6)

			first, token := op.describe(t, ts, map[string]string{"MaxResults": "3"})
			require.Len(t, first, 3)
			require.NotEmpty(t, token)

			second, token := op.describe(t, ts, map[string]string{"MaxResults": "3", "NextToken": token})
			assert.Len(t, second, 3)
			assert.Empty(t, token, "a full last page must not carry a token")
			assert.ElementsMatch(t, created, append(first, second...))
		})
	}
}

// TestEC2_OffsetPagination_AnInventedTokenIsRefused covers the defect #915 named at four other
// operations: a token substrate could not have issued must not select page one.
//
// A token past the end of the listing is the deliberate exception and is answered with an empty
// page rather than an error, because a caller resuming a walk after a record was deleted holds a
// token that was valid when it was issued.
func TestEC2_OffsetPagination_AnInventedTokenIsRefused(t *testing.T) {
	bad := []struct {
		name  string
		token string
	}{
		{"not a number", "abc"},
		{"a negative offset", "-1"},
		{"a number with trailing text", "3x"},
		{"an empty-looking token", " "},
	}
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 3)

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
				ids, token := op.describe(t, ts, map[string]string{"NextToken": "9999"})
				assert.Empty(t, ids)
				assert.Empty(t, token)
			})

			t.Run("a token the operation issued resumes the walk", func(t *testing.T) {
				first, token := op.describe(t, ts, map[string]string{"MaxResults": "1"})
				require.Len(t, first, 1)
				require.NotEmpty(t, token)
				rest, _ := op.describe(t, ts, map[string]string{"NextToken": token})
				assert.ElementsMatch(t, created, append(first, rest...))
			})
		})
	}
}

// TestEC2_OffsetPagination_TokenIsRefusedBeforeStateIsRead pins the ordering, which is what makes
// the refusal predictable: a caller must get the same answer to a malformed token whether the
// account holds no volumes or ten thousand.
//
// It is asserted by sealing the state store against reads and requiring the refusal to arrive
// anyway — the idiom #887 established and #915's tests use. A handler validating after the scan
// would answer 500 from the sealed store instead, which is what DescribeLaunchTemplateVersions did
// before this conversion moved its token parse ahead of the template lookup.
func TestEC2_OffsetPagination_TokenIsRefusedBeforeStateIsRead(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			sealed := &tokenRefusalSealedState{inner: emulator.NewMemoryStateManager()}
			ts := newEC2TestServerWithState(t, sealed)
			op.create(t, ts, 2)
			sealed.sealGets = true
			sealed.sealLists = true

			status, code, _ := ec2ErrorDetail(t, ts,
				ec2PagedParams(op.name, map[string]string{"NextToken": "abc"}))
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
		})
	}
}

// TestEC2_OffsetPagination_MaxResultsOutsideTheRangeIsRefused covers the bound at the two
// operations whose page publishes none.
//
// API_DescribeVolumes and API_DescribeSnapshots say only "The maximum number of items to return
// for this request", type Integer, with no Valid Range line — where API_DescribeVpcs,
// API_DescribeSubnets and API_DescribeSecurityGroups all publish 5 to 1000. Per #671 substrate
// does not borrow the siblings' range by analogy, which is what the accepted 5000 case pins: it
// would fail against a ceiling nothing published. The floor of one is substrate's reading and is
// forced by the published pagination rule — a page of zero items describes a walk that answers
// nothing and hands back a token forever.
func TestEC2_OffsetPagination_MaxResultsOutsideTheRangeIsRefused(t *testing.T) {
	cases := []struct {
		name       string
		maxResults string
		refused    bool
	}{
		{"a page of zero items", "0", true},
		{"a negative page", "-1", true},
		{"not a number", "many", true},
		{"the floor substrate reads", "1", false},
		{"above the range the siblings publish", "5000", false},
	}
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 3)

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					params := ec2PagedParams(op.name, map[string]string{"MaxResults": tc.maxResults})
					if !tc.refused {
						ids, _ := op.describe(t, ts, map[string]string{"MaxResults": tc.maxResults})
						assert.NotEmpty(t, ids)
						assert.LessOrEqual(t, len(ids), len(created))
						return
					}
					status, code, message := ec2ErrorDetail(t, ts, params)
					assert.Equal(t, http.StatusBadRequest, status)
					assert.Equal(t, "InvalidParameterValue", code)
					assert.Contains(t, message, "MaxResults must be at least 1")
				})
			}
		})
	}
}

// TestEC2_OffsetPagination_AnIDListWithMaxResultsIsRefused covers the one rule in this set that is
// published for the service rather than for an operation.
//
// Query-Requests.html → Pagination: "If you call a describe API action with both a list of IDs and
// MaxResults, the request fails with the error InvalidParameterCombination." Of the nine describes
// #917 names only API_DescribeInstances repeats it against its own parameter, so the code is
// published and the message wording is substrate's.
//
// This is the refusal that matters most in the divergence direction — without it the combination
// answers 200 here and fails in production — which is why each parameter is also asserted accepted
// on its own: a handler refusing the ID list outright would satisfy the first assertion.
func TestEC2_OffsetPagination_AnIDListWithMaxResultsIsRefused(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 3)

			t.Run("together", func(t *testing.T) {
				status, code, message := ec2ErrorDetail(t, ts, ec2PagedParams(op.name, map[string]string{
					op.idParam + ".1": created[0],
					"MaxResults":      "5",
				}))
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidParameterCombination", code)
				assert.Contains(t, message, op.idParam)
				assert.Contains(t, message, "MaxResults")
			})

			t.Run("an ID list alone", func(t *testing.T) {
				ids, token := op.describe(t, ts, map[string]string{op.idParam + ".1": created[0]})
				assert.Equal(t, []string{created[0]}, ids)
				assert.Empty(t, token)
			})

			t.Run("MaxResults alone", func(t *testing.T) {
				ids, _ := op.describe(t, ts, map[string]string{"MaxResults": "2"})
				assert.Len(t, ids, 2)
			})

			// The refusal is checked before the ID list's own syntax, because whether two
			// parameters may appear together does not depend on either being well formed. Which
			// of the two AWS answers first is not published, so this pins substrate's ordering
			// rather than AWS's.
			t.Run("before the ID list's own syntax check", func(t *testing.T) {
				_, code, _ := ec2ErrorDetail(t, ts, ec2PagedParams(op.name, map[string]string{
					op.idParam + ".1": "not-an-id",
					"MaxResults":      "5",
				}))
				assert.Equal(t, "InvalidParameterCombination", code)
			})
		})
	}
}
