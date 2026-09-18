package emulator_test

// Offset pagination shared by the EC2 describes (#917, #1024).
//
// Sixteen routed EC2 describes published MaxResults and NextToken and implemented neither: a
// request naming either was answered with the whole listing and no token, so a caller paging in one
// page against substrate found a second page in production. Sixteen is audited rather than
// estimated — this comment used to say "roughly twenty", which #1024 corrects here and in
// ec2_pagination.go, because an estimate invites the reader to assume the sweep was complete.
//
// The flat listings converted so far are the table below: DescribeVolumes and DescribeSnapshots in
// #917's first part, DescribeImages, DescribeVpcs, DescribeSubnets and DescribeSecurityGroups in
// its second, DescribeInstanceStatus and DescribeFleets in #1024's first, and
// DescribeInternetGateways, DescribeNatGateways and DescribeRouteTables in its second. The two
// operations that already paginated — DescribeTags and DescribeLaunchTemplateVersions — carried a
// private copy of the same three rules each, and their behavior is unchanged by the conversion,
// which TestEC2_DescribeTags_Pagination and TestEC2_DescribeLaunchTemplateVersions' own MaxResults
// cases are the regression guard for.
//
// Three converted operations are **not** in the table, because every case here creates the listing
// it walks and theirs cannot be created: all three are assembled from the instance-type catalog.
// DescribeSpotPriceHistory's cases are in ec2_spotpricehistory_pagination_test.go, and
// DescribeInstanceTypes' and DescribeInstanceTypeOfferings' in ec2_catalog_pagination_test.go; each
// file asserts the same five properties against the listing it is given rather than one it built.
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

// ec2PagedOp is one operation converted onto the shared offset paginator, with everything a case
// needs to drive it: how to create a listing, how to read the answer, the name of the ID-list
// parameter MaxResults may not accompany, and the MaxResults range its own page publishes.
//
// They are exercised from one table because the rules under test are the helpers' and not the
// operations' — an assertion written against volumes alone would pass while snapshots, whose page
// is cut at a different point in the handler, reported a record twice.
type ec2PagedOp struct {
	// name is the operation, used as the subtest name.
	name string
	// idParam is the resource-ID list parameter, unindexed as AWS names it.
	idParam string
	// minMaxResults and maxMaxResults are the MaxResults range the operation's own page
	// publishes, with maxMaxResults of [ec2PagedNoCeiling] meaning it publishes no maximum.
	//
	// API_DescribeVpcs, API_DescribeSubnets, API_DescribeSecurityGroups, API_DescribeInternetGateways
	// and API_DescribeNatGateways publish "Valid Range: Minimum value of 5. Maximum value of 1000.";
	// API_DescribeRouteTables publishes the same floor and a ceiling of **100**; and
	// API_DescribeVolumes, API_DescribeSnapshots, API_DescribeImages, API_DescribeInstanceStatus and
	// API_DescribeFleets publish no range at all, only "The maximum number of items to return for
	// this request", where the floor of one is substrate's reading (see ec2MinUnpublishedMaxResults).
	//
	// A pair rather than the boolean this column started as (#1024): two published ranges now appear
	// in this table alongside the pages that publish none, and DescribeLaunchTemplates' 1–200 is a
	// third still to convert, so a boolean would force them to collapse into one bound, which #671
	// forbids. Naming each operation's bounds here is also what lets the cases derive their values
	// from the bounds instead of hardcoding one range's edges — which is how one table asserts that
	// MaxResults=1000 is accepted at DescribeNatGateways and refused at DescribeRouteTables.
	minMaxResults int
	maxMaxResults int
	// create makes n records through real calls and returns their IDs.
	create func(t *testing.T, ts *httptest.Server, n int) []string
	// describe sends the operation with extra params and returns the IDs it reported, in the
	// order it reported them, and the token the page carried.
	describe func(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string)
}

// ec2PagedNoCeiling is the maxMaxResults meaning the operation's page publishes no maximum, and
// mirrors ec2NoMaxResultsCeiling in the package under test.
const ec2PagedNoCeiling = 0

// maxResultsMessage is the refusal message the operation's own range produces.
func (op ec2PagedOp) maxResultsMessage() string {
	return ec2MaxResultsMessage(op.minMaxResults, op.maxMaxResults)
}

// ec2MaxResultsMessage is the refusal message a published range of minResults..maxResults produces,
// where maxResults of [ec2PagedNoCeiling] means the page publishes no maximum.
//
// The wording is substrate's — no page publishes one — so it is asserted literally rather than
// derived: a message naming the wrong range would tell a caller to send a value the operation then
// refuses. It is a free function rather than a method because the catalog describes' cases
// (ec2_catalog_pagination_test.go) assert the same messages from their own table.
func ec2MaxResultsMessage(minResults, maxResults int) string {
	if maxResults == ec2PagedNoCeiling {
		return "MaxResults must be at least " + strconv.Itoa(minResults)
	}
	return "MaxResults must be between " + strconv.Itoa(minResults) +
		" and " + strconv.Itoa(maxResults)
}

// ec2PagedPageSize is the MaxResults every walk below is driven at.
//
// Five, because it is the smallest value the whole table accepts: six of the eleven pages publish a
// floor of five and the other five a floor of one (substrate's reading, see
// ec2MinUnpublishedMaxResults), so one page size exercises the walk at every operation without the
// cases having to know which range each carries. That the floors really do differ is asserted
// separately, by TestEC2_OffsetPagination_MaxResultsOutsideTheRangeIsRefused.
const ec2PagedPageSize = 5

// ec2PagedOps is every flat listing converted onto the shared offset paginator whose records a
// caller can create.
//
// DescribeInstances is deliberately absent: its answer nests reservationSet > item > instancesSet,
// so it pages through ec2PageReservations rather than ec2Page and its cases live in
// ec2_pagination_instances_test.go. DescribeSpotPriceHistory, DescribeInstanceTypes and
// DescribeInstanceTypeOfferings are absent for the reason this file's preamble gives — their
// listings are assembled from a fixed catalog, not created.
func ec2PagedOps() []ec2PagedOp {
	return []ec2PagedOp{
		{
			name:          "DescribeVolumes",
			idParam:       "VolumeId",
			minMaxResults: 1,
			maxMaxResults: ec2PagedNoCeiling,
			create:        ec2CreatePagedVolumes,
			describe:      ec2DescribePagedVolumes,
		},
		{
			name:          "DescribeSnapshots",
			idParam:       "SnapshotId",
			minMaxResults: 1,
			maxMaxResults: ec2PagedNoCeiling,
			create:        ec2CreatePagedSnapshots,
			describe:      ec2DescribePagedSnapshots,
		},
		{
			name:          "DescribeImages",
			idParam:       "ImageId",
			minMaxResults: 1,
			maxMaxResults: ec2PagedNoCeiling,
			create:        ec2CreatePagedImages,
			describe:      ec2DescribePagedImages,
		},
		{
			name:          "DescribeVpcs",
			idParam:       "VpcId",
			minMaxResults: 5,
			maxMaxResults: 1000,
			create:        ec2CreatePagedVPCs,
			describe:      ec2DescribePagedVPCs,
		},
		{
			name:          "DescribeSubnets",
			idParam:       "SubnetId",
			minMaxResults: 5,
			maxMaxResults: 1000,
			create:        ec2CreatePagedSubnets,
			describe:      ec2DescribePagedSubnets,
		},
		{
			name:          "DescribeSecurityGroups",
			idParam:       "GroupId",
			minMaxResults: 5,
			maxMaxResults: 1000,
			create:        ec2CreatePagedSecurityGroups,
			describe:      ec2DescribePagedSecurityGroups,
		},
		{
			name:          "DescribeInstanceStatus",
			idParam:       "InstanceId",
			minMaxResults: 1,
			maxMaxResults: ec2PagedNoCeiling,
			create:        ec2CreatePagedInstanceStatuses,
			describe:      ec2DescribePagedInstanceStatuses,
		},
		{
			name:          "DescribeFleets",
			idParam:       "FleetId",
			minMaxResults: 1,
			maxMaxResults: ec2PagedNoCeiling,
			create:        ec2CreatePagedFleets,
			describe:      ec2DescribePagedFleets,
		},
		{
			name:          "DescribeInternetGateways",
			idParam:       "InternetGatewayId",
			minMaxResults: 5,
			maxMaxResults: 1000,
			create:        ec2CreatePagedInternetGateways,
			describe:      ec2DescribePagedInternetGateways,
		},
		{
			name:          "DescribeNatGateways",
			idParam:       "NatGatewayId",
			minMaxResults: 5,
			maxMaxResults: 1000,
			create:        ec2CreatePagedNatGateways,
			describe:      ec2DescribePagedNatGateways,
		},
		{
			// The one row whose ceiling is a hundred: API_DescribeRouteTables publishes "Minimum
			// value of 5. Maximum value of 100." where its two siblings in this part publish a
			// thousand, so this row is what makes the boundary cases prove the bound is per
			// operation rather than per family (#671).
			name:          "DescribeRouteTables",
			idParam:       "RouteTableId",
			minMaxResults: 5,
			maxMaxResults: 100,
			create:        ec2CreatePagedRouteTables,
			describe:      ec2DescribePagedRouteTables,
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

// ec2CreatePagedImages registers n AMIs and returns their IDs.
//
// RegisterImage rather than CreateImage: the listing wanted is n AMIs and nothing else, and
// CreateImage would need an instance per AMI, each of which is itself a record another operation in
// this table pages. A bundled public AMI is not part of the answer — those are absent from state
// (#733) and reachable only by name — so the registered set is the whole listing.
func ec2CreatePagedImages(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, ec2RegisterImageID(t, ts, "paged-ami-"+strconv.Itoa(i)))
	}
	return ids
}

// ec2CreatePagedVPCs creates n VPCs and returns their IDs.
func ec2CreatePagedVPCs(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, ec2CreateVPC(t, ts, "10."+strconv.Itoa(i)+".0.0/16"))
	}
	return ids
}

// ec2CreatePagedSubnets creates n subnets in one VPC and returns their IDs.
func ec2CreatePagedSubnets(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, ec2CreateTaggedSubnet(t, ts, vpcID,
			"10.0."+strconv.Itoa(i)+".0/24", "us-east-1a", nil))
	}
	return ids
}

// ec2CreatePagedSecurityGroups creates n security groups and returns their IDs.
//
// No VPC, because CreateVpc mints no default security group here and a group needs none: the
// listing is then exactly the n groups created, with nothing the account acquired implicitly.
func ec2CreatePagedSecurityGroups(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, ec2CreateSG(t, ts, "paged-"+strconv.Itoa(i), "paged group", ""))
	}
	return ids
}

// ec2CreatePagedInstanceStatuses launches n instances and returns their IDs.
//
// One RunInstances of n rather than n of one: DescribeInstanceStatus reports a flat
// instanceStatusSet with no reservation grouping at all, so the launch shape cannot affect its
// answer, and one call is the cheapest way to reach a listing larger than a page. That its sibling
// DescribeInstances *does* group them is what ec2_pagination_instances_test.go covers.
func ec2CreatePagedInstanceStatuses(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	return ec2RunPagedInstances(t, ts, n).InstanceIDs
}

// ec2CreatePagedFleets creates n maintain fleets from one launch template and returns their IDs.
//
// Maintain rather than instant, and it is not a stylistic choice: an instant fleet is reported only
// when its ID is named, and naming an ID list refuses MaxResults, so an instant fleet can never
// appear on a paginated page. A listing of them would page as empty at every offset.
func ec2CreatePagedFleets(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ltID := newFleetLaunchTemplate(t, ts, "paged-fleets")
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var created createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "maintain",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &created)
		require.NotEmpty(t, created.FleetID)
		ids = append(ids, created.FleetID)
	}
	return ids
}

// ec2DescribePagedImages reads a DescribeImages page.
func ec2DescribePagedImages(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeImagesResponse"`
		Images  []struct {
			ImageID string `xml:"imageId"`
		} `xml:"imagesSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeImages", extra), &decoded)
	ids := make([]string, 0, len(decoded.Images))
	for _, img := range decoded.Images {
		ids = append(ids, img.ImageID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedVPCs reads a DescribeVpcs page.
func ec2DescribePagedVPCs(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeVpcsResponse"`
		Vpcs    []struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpcSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeVpcs", extra), &decoded)
	ids := make([]string, 0, len(decoded.Vpcs))
	for _, vpc := range decoded.Vpcs {
		ids = append(ids, vpc.VpcID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedSubnets reads a DescribeSubnets page.
func ec2DescribePagedSubnets(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeSubnetsResponse"`
		Subnets []struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnetSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeSubnets", extra), &decoded)
	ids := make([]string, 0, len(decoded.Subnets))
	for _, subnet := range decoded.Subnets {
		ids = append(ids, subnet.SubnetID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedSecurityGroups reads a DescribeSecurityGroups page.
//
// The group ID is read as a direct child of securityGroupInfo>item for the reason
// [ec2DescribePagedVolumes] gives about volumeId: a permission's groups>item carries a groupId too,
// so an unanchored match would report a group that referenced another one twice.
func ec2DescribePagedSecurityGroups(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeSecurityGroupsResponse"`
		Groups  []struct {
			GroupID string `xml:"groupId"`
		} `xml:"securityGroupInfo>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeSecurityGroups", extra), &decoded)
	ids := make([]string, 0, len(decoded.Groups))
	for _, group := range decoded.Groups {
		ids = append(ids, group.GroupID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedInstanceStatuses reads a DescribeInstanceStatus page.
//
// The instance ID is read as a direct child of instanceStatusSet>item, per the reason
// [ec2DescribePagedVolumes] gives.
func ec2DescribePagedInstanceStatuses(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName  xml.Name `xml:"DescribeInstanceStatusResponse"`
		Statuses []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instanceStatusSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeInstanceStatus", extra), &decoded)
	ids := make([]string, 0, len(decoded.Statuses))
	for _, status := range decoded.Statuses {
		ids = append(ids, status.InstanceID)
	}
	return ids, decoded.NextToken
}

// ec2CreatePagedInternetGateways creates n internet gateways and returns their IDs.
//
// Unattached, because an attachment is not part of the listing: CreateInternetGateway takes no
// arguments at all, so these n gateways are the whole answer with nothing the account acquired
// implicitly — the property [ec2CreatePagedSecurityGroups] needed a comment to arrange.
func ec2CreatePagedInternetGateways(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var created struct {
			InternetGatewayID string `xml:"internetGateway>internetGatewayId"`
		}
		ec2DescribeXML(t, ts, map[string]string{"Action": "CreateInternetGateway"}, &created)
		require.NotEmpty(t, created.InternetGatewayID)
		ids = append(ids, created.InternetGatewayID)
	}
	return ids
}

// ec2CreatePagedNatGateways creates n NAT gateways in one subnet and returns their IDs.
//
// One subnet and one elastic IP per gateway: a public NAT gateway names an AllocationId, and reusing
// one would leave every gateway reporting the same publicIp — harmless to the offset but it would
// make a duplicated record indistinguishable from a correct one in the walk's comparison.
func ec2CreatePagedNatGateways(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	subnetID := ec2CreateTaggedSubnet(t, ts, vpcID, "10.0.1.0/24", "us-east-1a", nil)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		var addr struct {
			AllocationID string `xml:"allocationId"`
		}
		ec2DescribeXML(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"}, &addr)
		require.NotEmpty(t, addr.AllocationID)
		var created struct {
			NatGatewayID string `xml:"natGateway>natGatewayId"`
		}
		ec2DescribeXML(t, ts, map[string]string{
			"Action":       "CreateNatGateway",
			"SubnetId":     subnetID,
			"AllocationId": addr.AllocationID,
		}, &created)
		require.NotEmpty(t, created.NatGatewayID)
		ids = append(ids, created.NatGatewayID)
	}
	return ids
}

// ec2CreatePagedRouteTables returns the IDs of n route tables in one VPC, one of which the VPC
// brought with it.
//
// Unlike every other row, the listing is not empty before this helper creates anything: CreateVpc
// mints the VPC's main route table (createRouteTableForVPC with main true), and that table is a
// genuine member of DescribeRouteTables' answer rather than an artifact — so n-1 are created here
// and the main one is read back rather than guessed. Returning it is what keeps the listing exactly
// n, which is what lets a walk be compared against the whole answer element for element.
func ec2CreatePagedRouteTables(t *testing.T, ts *httptest.Server, n int) []string {
	t.Helper()
	require.Positive(t, n, "the main route table alone makes a listing of one")
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	ids, token := ec2DescribePagedRouteTables(t, ts, nil)
	require.Len(t, ids, 1, "a fresh VPC holds exactly one route table, its main one")
	require.Empty(t, token)
	for i := 0; i < n-1; i++ {
		ids = append(ids, ec2CreateRouteTableID(t, ts, vpcID))
	}
	return ids
}

// ec2DescribePagedInternetGateways reads a DescribeInternetGateways page.
func ec2DescribePagedInternetGateways(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeInternetGatewaysResponse"`
		IGWs    []struct {
			InternetGatewayID string `xml:"internetGatewayId"`
		} `xml:"internetGatewaySet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeInternetGateways", extra), &decoded)
	ids := make([]string, 0, len(decoded.IGWs))
	for _, igw := range decoded.IGWs {
		ids = append(ids, igw.InternetGatewayID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedNatGateways reads a DescribeNatGateways page.
func ec2DescribePagedNatGateways(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName     xml.Name `xml:"DescribeNatGatewaysResponse"`
		NatGateways []struct {
			NatGatewayID string `xml:"natGatewayId"`
		} `xml:"natGatewaySet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeNatGateways", extra), &decoded)
	ids := make([]string, 0, len(decoded.NatGateways))
	for _, gw := range decoded.NatGateways {
		ids = append(ids, gw.NatGatewayID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedRouteTables reads a DescribeRouteTables page.
//
// The ID is read as a direct child of routeTableSet>item for the reason
// [ec2DescribePagedVolumes] gives: an association carries a routeTableAssociationId, and a decoder
// matching loosely would over-collect.
func ec2DescribePagedRouteTables(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName     xml.Name `xml:"DescribeRouteTablesResponse"`
		RouteTables []struct {
			RouteTableID string `xml:"routeTableId"`
		} `xml:"routeTableSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeRouteTables", extra), &decoded)
	ids := make([]string, 0, len(decoded.RouteTables))
	for _, rtb := range decoded.RouteTables {
		ids = append(ids, rtb.RouteTableID)
	}
	return ids, decoded.NextToken
}

// ec2DescribePagedFleets reads a DescribeFleets page.
func ec2DescribePagedFleets(t *testing.T, ts *httptest.Server, extra map[string]string) ([]string, string) {
	t.Helper()
	var decoded struct {
		XMLName xml.Name `xml:"DescribeFleetsResponse"`
		Fleets  []struct {
			FleetID string `xml:"fleetId"`
		} `xml:"fleetSet>item"`
		NextToken string `xml:"nextToken"`
	}
	ec2DescribeXML(t, ts, ec2PagedParams("DescribeFleets", extra), &decoded)
	ids := make([]string, 0, len(decoded.Fleets))
	for _, fleet := range decoded.Fleets {
		ids = append(ids, fleet.FleetID)
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
// Only API_DescribeSecurityGroups publishes what an absent MaxResults means — "If this parameter is
// not specified, then all items are returned" — and substrate reads it that way at every operation
// in the table, which is also what each answered before it paginated. A caller that never sent
// MaxResults therefore sees no wire change at all, token included: the element is omitted rather
// than emitted empty.
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
			created := op.create(t, ts, 2*ec2PagedPageSize+1)

			whole, _ := op.describe(t, ts, nil)
			require.Len(t, whole, len(created))

			walked := ec2PagedWalk(t, ts, op, ec2PagedPageSize, len(created))
			assert.Equal(t, whole, walked, "paging must not change what is reported or its order")
		})
	}
}

// TestEC2_OffsetPagination_AFullLastPageCarriesNoToken covers the listing whose size is an exact
// multiple of the page size.
//
// The token is emitted from whether a further record exists, not from whether the page filled up,
// so ten records at five per page is two pages and not three-with-an-empty-tail. Getting this
// backwards is not a crash: a caller told to keep calling until the token is null would make one
// extra request per walk, which only shows up as a wasted round trip until the empty page is
// mistaken for a truncated listing.
func TestEC2_OffsetPagination_AFullLastPageCarriesNoToken(t *testing.T) {
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, 2*ec2PagedPageSize)
			pageSize := strconv.Itoa(ec2PagedPageSize)

			first, token := op.describe(t, ts, map[string]string{"MaxResults": pageSize})
			require.Len(t, first, ec2PagedPageSize)
			require.NotEmpty(t, token)

			second, token := op.describe(t, ts, map[string]string{"MaxResults": pageSize, "NextToken": token})
			assert.Len(t, second, ec2PagedPageSize)
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
			created := op.create(t, ts, ec2PagedPageSize+1)

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
				first, token := op.describe(t, ts,
					map[string]string{"MaxResults": strconv.Itoa(ec2PagedPageSize)})
				require.Len(t, first, ec2PagedPageSize)
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

// TestEC2_OffsetPagination_MaxResultsOutsideTheRangeIsRefused asserts each operation's bound is the
// one its own page publishes, and nothing wider.
//
// Five pages in the table publish "Valid Range: Minimum value of 5. Maximum value of 1000.";
// API_DescribeRouteTables publishes that floor with a ceiling of **100**; and the other five say
// only "The maximum number of items to return for this request", type Integer, with no Valid Range
// line at all. Per #671 substrate does not borrow the published range by analogy, and both
// directions are asserted because that is what pins it: 1 and 5000 are **accepted** where no range
// is published and **refused** where 5–1000 is, and 1000 itself is accepted at DescribeNatGateways
// and refused at DescribeRouteTables — so a helper that had defaulted to one range for the family
// would fail on most of the rows.
//
// The boundary values come from each operation's own [ec2PagedOp] bounds rather than from literals,
// so the table asserts the edges of whichever range the operation publishes — which is also what
// DescribeLaunchTemplates' 1–200 needs when #1024's last part converts it.
//
// Where no range is published the floor of one is substrate's reading, forced by the published
// pagination rule — a page of zero items describes a walk that answers nothing and hands back a
// token forever. A value that is not a number is refused at every operation, since AWS types the
// parameter Integer everywhere.
func TestEC2_OffsetPagination_MaxResultsOutsideTheRangeIsRefused(t *testing.T) {
	// Values no published range admits, whatever the operation: no page publishes a floor below
	// one, and none types the parameter as anything but Integer.
	refusedEverywhere := []struct {
		name       string
		maxResults string
	}{
		{"a page of zero items", "0"},
		{"a negative page", "-1"},
		{"not a number", "many"},
	}
	for _, op := range ec2PagedOps() {
		t.Run(op.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			created := op.create(t, ts, ec2PagedPageSize+1)

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
				ids, _ := op.describe(t, ts, map[string]string{"MaxResults": maxResults})
				assert.NotEmpty(t, ids)
				assert.LessOrEqual(t, len(ids), len(created))
			}

			for _, tc := range refusedEverywhere {
				t.Run(tc.name, func(t *testing.T) { refuse(t, tc.maxResults) })
			}
			t.Run("the published floor itself", func(t *testing.T) {
				accept(t, strconv.Itoa(op.minMaxResults))
			})
			if op.minMaxResults > 1 {
				t.Run("one below the published floor", func(t *testing.T) {
					refuse(t, strconv.Itoa(op.minMaxResults-1))
				})
			}
			if op.maxMaxResults == ec2PagedNoCeiling {
				// The direction that matters most: a value far above every range published
				// anywhere in the family is accepted here, because this page publishes none.
				t.Run("a page of 5000 where no ceiling is published", func(t *testing.T) {
					accept(t, "5000")
				})
				return
			}
			t.Run("the published ceiling itself", func(t *testing.T) {
				accept(t, strconv.Itoa(op.maxMaxResults))
			})
			t.Run("one above the published ceiling", func(t *testing.T) {
				refuse(t, strconv.Itoa(op.maxMaxResults+1))
			})
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
			created := op.create(t, ts, ec2PagedPageSize+1)

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
				ids, _ := op.describe(t, ts,
					map[string]string{"MaxResults": strconv.Itoa(ec2PagedPageSize)})
				assert.Len(t, ids, ec2PagedPageSize)
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
