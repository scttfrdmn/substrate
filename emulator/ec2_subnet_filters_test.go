package emulator_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// describedSubnet is the subset of a DescribeSubnets item these tests read. Every member
// below arrived with #685 except subnetId, vpcId, cidrBlock, availabilityZone, state and
// mapPublicIpOnLaunch.
type describedSubnet struct {
	SubnetID            string `xml:"subnetId"`
	SubnetARN           string `xml:"subnetArn"`
	State               string `xml:"state"`
	OwnerID             string `xml:"ownerId"`
	VpcID               string `xml:"vpcId"`
	CIDRBlock           string `xml:"cidrBlock"`
	AvailabilityZone    string `xml:"availabilityZone"`
	DefaultForAz        bool   `xml:"defaultForAz"`
	MapPublicIPOnLaunch bool   `xml:"mapPublicIpOnLaunch"`
	Tags                []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

// ec2DescribeSubnets sends DescribeSubnets with params and returns the subnets.
func ec2DescribeSubnets(t *testing.T, ts *httptest.Server, params map[string]string) []describedSubnet {
	t.Helper()
	full := map[string]string{"Action": "DescribeSubnets"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Subnets []describedSubnet `xml:"subnetSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Subnets
}

// ec2SubnetIDsFor is [ec2DescribeSubnets] reduced to the IDs a filter selected.
func ec2SubnetIDsFor(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	subnets := ec2DescribeSubnets(t, ts, params)
	ids := make([]string, 0, len(subnets))
	for _, s := range subnets {
		ids = append(ids, s.SubnetID)
	}
	return ids
}

// ec2CreateTaggedSubnet creates one subnet, tagging it on creation through
// TagSpecification.N — the parameter #685 added to CreateSubnet, and the only way CDK or
// Terraform sets the tags a caller then filters on.
func ec2CreateTaggedSubnet(t *testing.T, ts *httptest.Server, vpcID, cidr, az string, tags map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action":                          "CreateSubnet",
		"VpcId":                           vpcID,
		"CidrBlock":                       cidr,
		"AvailabilityZone":                az,
		"TagSpecification.1.ResourceType": "subnet",
	}
	i := 1
	for k, v := range tags {
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Key"] = k
		params["TagSpecification.1.Tag."+strconv.Itoa(i)+".Value"] = v
		i++
	}
	var created struct {
		Subnet describedSubnet `xml:"subnet"`
	}
	ec2FleetXML(t, ts, params, &created)
	require.NotEmpty(t, created.Subnet.SubnetID)
	return created.Subnet.SubnetID
}

// subnetFixture is the three-subnet fixture the filter tests share: the account's default
// subnet, which a launch with no SubnetId mints, plus two tagged subnets in a VPC of their
// own. The default subnet is what makes default-for-az and map-public-ip-on-launch
// testable in both directions — nothing else in substrate sets either to true.
type subnetFixture struct {
	vpcID          string
	defaultSubnet  string
	alpha, beta    string
	owner          string
	defaultSubnetV string
}

// newSubnetFixture builds [subnetFixture].
func newSubnetFixture(t *testing.T, ts *httptest.Server) subnetFixture {
	t.Helper()
	ec2TagTestInstance(t, ts) // no SubnetId, so this mints the default VPC and subnet

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.9.0.0/16"}, &vpc)
	require.NotEmpty(t, vpc.VPCID)

	f := subnetFixture{
		vpcID: vpc.VPCID,
		alpha: ec2CreateTaggedSubnet(t, ts, vpc.VPCID, "10.9.1.0/24", "us-east-1b",
			map[string]string{"Scope": "alpha", "Tier": "web"}),
		beta: ec2CreateTaggedSubnet(t, ts, vpc.VPCID, "10.9.2.0/24", "us-east-1c",
			map[string]string{"Scope": "beta"}),
	}
	for _, s := range ec2DescribeSubnets(t, ts, nil) {
		f.owner = s.OwnerID
		if s.DefaultForAz {
			f.defaultSubnet = s.SubnetID
			f.defaultSubnetV = s.VpcID
		}
	}
	require.NotEmpty(t, f.defaultSubnet, "the launch should have left a default subnet")
	require.NotEmpty(t, f.owner, "ownerId is what the owner-id filter compares against")
	return f
}

// TestEC2_DescribeSubnets_FiltersNarrowTheAnswer covers the eleven filter names #685 taught
// describeSubnets to evaluate, and the four alias spellings AWS documents inline.
//
// This operation parsed no Filter.N at all before #685: it selected on SubnetId.N and
// nothing else, so every case below returned all three subnets. "The subnets of vpc-x"
// answering with a neighbor's subnets is the worst shape of the ignored-filter failure,
// because the response carries nothing that says the filter was not applied.
func TestEC2_DescribeSubnets_FiltersNarrowTheAnswer(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)
	all := []string{f.defaultSubnet, f.alpha, f.beta}
	created := []string{f.alpha, f.beta}

	tests := []struct {
		name   string
		filter map[string]string
		want   []string
	}{
		{"vpc-id selects one VPC's subnets", ec2Filter("vpc-id", f.vpcID), created},
		{"vpc-id values OR", ec2Filter("vpc-id", f.vpcID, f.defaultSubnetV), all},
		{"vpc-id miss", ec2Filter("vpc-id", "vpc-0000000000000dead"), nil},
		{"availability-zone", ec2Filter("availability-zone", "us-east-1b"), []string{f.alpha}},
		{"availabilityZone alias", ec2Filter("availabilityZone", "us-east-1b"), []string{f.alpha}},
		{"cidr-block is exact", ec2Filter("cidr-block", "10.9.1.0/24"), []string{f.alpha}},
		{"cidr alias", ec2Filter("cidr", "10.9.2.0/24"), []string{f.beta}},
		{"cidrBlock alias", ec2Filter("cidrBlock", "10.9.2.0/24"), []string{f.beta}},
		{"cidr-block does not match a containing block",
			ec2Filter("cidr-block", "10.9.0.0/16"), nil},
		{"default-for-az true", ec2Filter("default-for-az", "true"), []string{f.defaultSubnet}},
		{"default-for-az false", ec2Filter("default-for-az", "false"), created},
		{"defaultForAz alias", ec2Filter("defaultForAz", "true"), []string{f.defaultSubnet}},
		{"map-public-ip-on-launch true",
			ec2Filter("map-public-ip-on-launch", "true"), []string{f.defaultSubnet}},
		{"map-public-ip-on-launch false", ec2Filter("map-public-ip-on-launch", "false"), created},
		{"owner-id self", ec2Filter("owner-id", f.owner), all},
		{"owner-id another account", ec2Filter("owner-id", "999999999999"), nil},
		{"state available", ec2Filter("state", "available"), all},
		{"state pending", ec2Filter("state", "pending"), nil},
		{"subnet-id", ec2Filter("subnet-id", f.alpha), []string{f.alpha}},
		{"tag-key shared by two", ec2Filter("tag-key", "Scope"), created},
		{"tag-key on one", ec2Filter("tag-key", "Tier"), []string{f.alpha}},
		{"tag-key absent", ec2Filter("tag-key", "Absent"), nil},
		{"tag value selects one", ec2Filter("tag:Scope", "alpha"), []string{f.alpha}},
		{"tag values OR", ec2Filter("tag:Scope", "alpha", "beta"), created},
		{"tag key absent", ec2Filter("tag:Absent", "alpha"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ec2SubnetIDsFor(t, ts, tc.filter))
		})
	}
}

// TestEC2_DescribeSubnets_SubnetArnFilterMatchesTheRenderedARN pins the two halves of
// subnet-arn against each other: the filter compares the same string the response renders,
// so a caller can round-trip what it read. Two spellings of the ARN would break that
// silently, which is why [ec2SubnetARN] is the only one.
func TestEC2_DescribeSubnets_SubnetArnFilterMatchesTheRenderedARN(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)

	one := ec2DescribeSubnets(t, ts, map[string]string{"SubnetId.1": f.alpha})
	require.Len(t, one, 1)
	require.Equal(t, "arn:aws:ec2:us-east-1:"+f.owner+":subnet/"+f.alpha, one[0].SubnetARN)

	assert.Equal(t, []string{f.alpha},
		ec2SubnetIDsFor(t, ts, ec2Filter("subnet-arn", one[0].SubnetARN)))
	assert.Empty(t, ec2SubnetIDsFor(t, ts,
		ec2Filter("subnet-arn", "arn:aws:ec2:us-east-1:"+f.owner+":subnet/subnet-0000000000000dead")))
}

// TestEC2_DescribeSubnets_FiltersAND pins AWS's composition rule on this operation:
// separate filters AND, values within one OR.
func TestEC2_DescribeSubnets_FiltersAND(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)

	assert.Equal(t, []string{f.alpha}, ec2SubnetIDsFor(t, ts, map[string]string{
		"Filter.1.Name": "vpc-id", "Filter.1.Value.1": f.vpcID,
		"Filter.2.Name": "tag:Scope", "Filter.2.Value.1": "alpha",
	}))
	assert.Empty(t, ec2SubnetIDsFor(t, ts, map[string]string{
		"Filter.1.Name": "vpc-id", "Filter.1.Value.1": f.vpcID,
		"Filter.2.Name": "default-for-az", "Filter.2.Value.1": "true",
	}), "no subnet in the created VPC is a default subnet, so the AND is empty")
}

// TestEC2_DescribeSubnets_InertFiltersReturnEverything pins the fourteen names AWS
// documents that substrate accepts and cannot answer — sampled here, listed in full in
// docs/services.md. They constrain nothing rather than being refused, because refusing a
// filter real EC2 accepts would be a false deny.
func TestEC2_DescribeSubnets_InertFiltersReturnEverything(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)
	want := []string{f.defaultSubnet, f.alpha, f.beta}

	for name, value := range map[string]string{
		"availability-zone-id":       "use1-az1",
		"available-ip-address-count": "251",
		"ipv6-native":                "false",
		"outpost-arn":                "arn:aws:outposts:us-east-1:1:outpost/op-1",
		"private-dns-name-options-on-launch.hostname-type": "ip-name",
	} {
		t.Run(name, func(t *testing.T) {
			assert.ElementsMatch(t, want, ec2SubnetIDsFor(t, ts, ec2Filter(name, value)),
				"an inert filter constrains nothing, so every subnet is still returned")
		})
	}
}

// TestEC2_DescribeSubnets_AFilterDoesNotUnresolveANamedID pins the same ordering rule
// describeSnapshots follows: a subnet a filter excluded still counts as resolved, so naming
// an existing subnet and filtering it out is an empty 200 rather than a not-found error.
func TestEC2_DescribeSubnets_AFilterDoesNotUnresolveANamedID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)

	assert.Empty(t, ec2SubnetIDsFor(t, ts, map[string]string{
		"SubnetId.1":    f.alpha,
		"Filter.1.Name": "state", "Filter.1.Value.1": "pending",
	}), "an existing subnet a filter excluded is an empty answer, not a not-found error")

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":        "DescribeSubnets",
		"SubnetId.1":    "subnet-0000000000000dead",
		"Filter.1.Name": "state", "Filter.1.Value.1": "available",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidSubnetID.NotFound", code)
}

// TestEC2_DescribeSubnets_TagSetIsAbsentWhenUntagged pins the decision that an untagged
// subnet omits tagSet entirely rather than rendering it empty.
//
// Both of AWS's DescribeSubnets samples omit the element on an untagged subnet, which is
// per-operation evidence — describeSnapshots' present-but-empty tagSet is left as it is,
// because its own page shows that shape. Asserting on the raw body is the only way to see
// the difference: a decoder reports an absent element and an empty one identically.
func TestEC2_DescribeSubnets_TagSetIsAbsentWhenUntagged(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	f := newSubnetFixture(t, ts)

	body := func(subnetID string) string {
		resp := ec2Request(t, ts, map[string]string{
			"Action": "DescribeSubnets", "SubnetId.1": subnetID,
		})
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusOK, resp.StatusCode)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return string(raw)
	}

	assert.NotContains(t, body(f.defaultSubnet), "tagSet",
		"an untagged subnet omits tagSet, as both of AWS's samples do")
	assert.Contains(t, body(f.alpha), "<tagSet>",
		"a tagged subnet renders it")
}

// TestEC2_CreateSubnet_TagsOnCreateAndAgreesWithDescribe pins both halves of the create
// path: TagSpecification.N is honored, and the create response describes the same subnet
// the describe does.
//
// The two rendered different member subsets before #685 — CreateSubnet omitted
// mapPublicIpOnLaunch, and neither carried tags, an ARN, an owner or defaultForAz — so a
// caller reading the create response saw a different subnet from the one it could then
// filter on. One shared renderer is what keeps them from drifting again.
func TestEC2_CreateSubnet_TagsOnCreateAndAgreesWithDescribe(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.9.0.0/16"}, &vpc)

	var created struct {
		Subnet describedSubnet `xml:"subnet"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                          "CreateSubnet",
		"VpcId":                           vpc.VPCID,
		"CidrBlock":                       "10.9.1.0/24",
		"AvailabilityZone":                "us-east-1b",
		"TagSpecification.1.ResourceType": "subnet",
		"TagSpecification.1.Tag.1.Key":    "Scope",
		"TagSpecification.1.Tag.1.Value":  "alpha",
	}, &created)
	require.NotEmpty(t, created.Subnet.SubnetID)
	require.Len(t, created.Subnet.Tags, 1, "TagSpecification.N must be honored on create")
	assert.Equal(t, "Scope", created.Subnet.Tags[0].Key)
	assert.Equal(t, "alpha", created.Subnet.Tags[0].Value)

	described := ec2DescribeSubnets(t, ts, map[string]string{"SubnetId.1": created.Subnet.SubnetID})
	require.Len(t, described, 1)
	assert.Equal(t, created.Subnet, described[0],
		"CreateSubnet and DescribeSubnets render one Subnet type, so the two must agree")

	assert.Equal(t, []string{created.Subnet.SubnetID},
		ec2SubnetIDsFor(t, ts, ec2Filter("tag:Scope", "alpha")),
		"a tag set at create time is immediately filterable, which is the point of the parameter")
}

// TestEC2_CreateSubnet_RejectsReservedTagKeys pins that the shared tag rules apply to this
// new tag-on-create path too, and that a rejected request leaves no subnet behind.
func TestEC2_CreateSubnet_RejectsReservedTagKeys(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.9.0.0/16"}, &vpc)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "CreateSubnet",
		"VpcId":                           vpc.VPCID,
		"CidrBlock":                       "10.9.1.0/24",
		"TagSpecification.1.ResourceType": "subnet",
		"TagSpecification.1.Tag.1.Key":    "aws:foo",
		"TagSpecification.1.Tag.1.Value":  "v",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, ec2ReservedTagMessage, message)

	assert.Empty(t, ec2DescribeSubnets(t, ts, nil),
		"a rejected CreateSubnet must not create a subnet")
}
