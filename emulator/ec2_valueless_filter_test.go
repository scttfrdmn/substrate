package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file pins #696: a Filter.N entry naming no Value.N at all matches nothing, on every
// operation.
//
// Nine of substrate's eleven filter-value matchers already answered that way, by calling
// containsStr with no guard on the value list. Two did not — ec2FilterAccepts and the three
// hand-written DescribeSecurityGroups arms — and returned *every* resource instead, reading a
// valueless filter as an absent one. Three operations diverged, because DescribeTags shares
// ec2FilterAccepts: DescribeSecurityGroups, DescribeTags and DescribeInstanceTypeOfferings.
//
// AWS documents no rule here — Using_Filtering says only that "You can't specify a filter
// value of null" — so both answers were readings. What made the split a defect is that the
// same request meant two different things depending on which operation received it, and the
// permissive reading is the more dangerous silence of the two: a caller who asked for a
// subset and got everything cannot tell that answer from a genuine match on every resource.
// #686 had already settled it for tag:<key> on DescribeImages; this converges the rest.
//
// Throughout, note what is *not* being asserted. A filter carrying one empty value
// (Filter.1.Value.1=) asks for the empty string and is a value like any other — DescribeTags'
// own Example 6 does exactly that, and TestEC2_DescribeTags_FiltersNarrowTheAnswer pins it.
// An *absent* filter still constrains nothing.

// ec2SecurityGroupNames returns the group names DescribeSecurityGroups reports for params.
func ec2SecurityGroupNames(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	full := map[string]string{"Action": "DescribeSecurityGroups"}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var doc struct {
		Groups []struct {
			GroupName string `xml:"groupName"`
		} `xml:"securityGroupInfo>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&doc))
	names := make([]string, 0, len(doc.Groups))
	for _, g := range doc.Groups {
		names = append(names, g.GroupName)
	}
	return names
}

// TestEC2_DescribeSecurityGroups_ValuelessFilterMatchesNothing is the first of #696's two
// permissive sites: the three hand-written arms carried an `ok && len(vals) > 0 &&` guard, so
// a caller who named a filter and no values got every security group in the account.
//
// Every evaluated name is exercised, because the guard was written out three times rather
// than shared — dropping it from two of the three would have left the divergence in place on
// the third.
func TestEC2_DescribeSecurityGroups_ValuelessFilterMatchesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.9.0.0/16"}, &vpc)
	require.NotEmpty(t, vpc.VPCID)
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "CreateSecurityGroup",
		"GroupName":        "valueless-fixture",
		"GroupDescription": "valueless-fixture",
		"VpcId":            vpc.VPCID,
	}, nil)

	// The fixture is only meaningful if the unfiltered answer is non-empty, which is also
	// the assertion that an absent filter still constrains nothing.
	all := ec2SecurityGroupNames(t, ts, nil)
	require.Contains(t, all, "valueless-fixture",
		"fixture setup: the group must be visible without a filter")

	for _, name := range []string{"group-name", "vpc-id", "group-id"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := ec2SecurityGroupNames(t, ts, map[string]string{"Filter.1.Name": name})
			assert.Empty(t, got,
				"a valueless %s filter returned groups; it must select none of them", name)
		})
	}
}

// TestEC2_DescribeTags_ValuelessFilterMatchesNothing covers the site that inherited the
// permissive rule rather than choosing it: ec2TagMatchesFilter routes every value comparison
// through ec2FilterAccepts, so all five of AWS's documented names changed together.
//
// tag:<key> is included even though #686 settled that spelling on DescribeImages, because
// DescribeTags reached it through the other matcher and so answered the opposite way — the
// two spellings of the same question are now one answer.
func TestEC2_DescribeTags_ValuelessFilterMatchesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	instID, _ := ec2SeedTaggedResources(t, ts)

	require.Contains(t, ec2TagKeysFor(t, ts, nil), instID+"/Env=prod",
		"fixture setup: the tag must be visible without a filter")

	for _, name := range []string{"key", "value", "resource-id", "resource-type", "tag:Env"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := ec2TagKeysFor(t, ts, map[string]string{"Filter.1.Name": name})
			assert.Empty(t, got,
				"a valueless %s filter returned tags; it must select none of them", name)
		})
	}
}

// TestEC2_DescribeInstanceTypeOfferings_ValuelessFilterMatchesNothing covers the operation
// ec2FilterAccepts was written for.
//
// Its two call sites index the filter map, so they need the two-value lookup form to keep
// telling an absent filter from a valueless one — with a bare index both arrive as the same
// nil slice, and the whole catalog would disappear from an unfiltered query. The no-filter
// case in TestEC2_DescribeInstanceTypeOfferings_InstanceTypeFilter is the other half of that
// guard; this asserts the two answers differ.
func TestEC2_DescribeInstanceTypeOfferings_ValuelessFilterMatchesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	require.NotEmpty(t, ec2Offerings(t, ts, map[string]string{}),
		"fixture setup: the catalog must be non-empty without a filter")

	for _, name := range []string{"instance-type", "location"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := ec2Offerings(t, ts, map[string]string{"Filter.1.Name": name})
			assert.Empty(t, got,
				"a valueless %s filter returned offerings; it must select none of them", name)
		})
	}
}

// TestEC2_ValuelessFilter_MajorityOperationsUnchanged is the other direction of #696: the nine
// matchers that already answered "nothing" still do.
//
// The convergence went towards these, so a regression here would mean the fix had been
// applied backwards — and because the two permissive sites are the ones that moved, nothing in
// this test needed to change to make it pass.
func TestEC2_ValuelessFilter_MajorityOperationsUnchanged(t *testing.T) {
	t.Parallel()

	t.Run("DescribeSnapshots", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2SeedTaggedSnapshots(t, ts, "majority-unchanged")
		require.NotEmpty(t, ec2SnapshotIDsFor(t, ts, map[string]string{}))
		assert.Empty(t, ec2SnapshotIDsFor(t, ts, map[string]string{"Filter.1.Name": "status"}))
	})

	t.Run("DescribeImages", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2ImageFilterFixture(t, ts)
		require.NotEmpty(t, ec2FilteredImageIDs(t, ts, map[string]string{}))
		assert.Empty(t, ec2FilteredImageIDs(t, ts, map[string]string{"Filter.1.Name": "image-id"}))
	})

	t.Run("DescribeSubnets", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		var vpc struct {
			VPCID string `xml:"vpc>vpcId"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.11.0.0/16"}, &vpc)
		ec2CreateTaggedSubnet(t, ts, vpc.VPCID, "10.11.1.0/24", "us-east-1a", nil)
		require.NotEmpty(t, ec2SubnetIDsFor(t, ts, map[string]string{}))
		assert.Empty(t, ec2SubnetIDsFor(t, ts, map[string]string{"Filter.1.Name": "state"}))
	})
}
