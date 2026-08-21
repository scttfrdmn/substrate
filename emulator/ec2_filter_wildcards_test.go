package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file pins #697: EC2's documented filter-value wildcards work on every describe, not
// on the two operations that happened to have them.
//
// AWS states the rule once, for the whole family: "An asterisk (*) matches zero or more
// characters, and a question mark (?) matches zero or one character." Before #697 nine of
// substrate's eleven matchers compared with containsStr, so `t3.*` selected nothing at all —
// the silent-narrowing shape of an ignored filter, where a consumer sees a legitimate-looking
// empty set instead of an error. DescribeInstanceTypeOfferings and DescribeTags matched
// through ec2FilterValueMatches and so already worked, which is what made the split a defect:
// one documented value meant two different things depending on the operation.
//
// Three things are deliberately *not* globbed, and the last test here pins each: identifier
// parameter lists (KeyName.N, GroupName.N, FleetId.N, InstanceType.N), which assert a resource
// exists rather than narrowing a set, and filter *names*, which are one of a fixed documented
// set. See containsStr's doc comment for the whole surviving list.

// ec2WildcardSubnetNames returns the Name tags of the subnets a tag:Name filter selected,
// sorted, so a case asserts on the names it seeded rather than on generated IDs.
func ec2WildcardSubnetNames(t *testing.T, ts *httptest.Server, value string) []string {
	t.Helper()
	names := make([]string, 0, 4)
	for _, s := range ec2DescribeSubnets(t, ts, ec2Filter("tag:Name", value)) {
		for _, tag := range s.Tags {
			if tag.Key == "Name" {
				names = append(names, tag.Value)
			}
		}
	}
	sort.Strings(names)
	return names
}

// TestEC2_FilterWildcards_ValueSemantics walks AWS's own worked example through a real
// request.
//
// EC2's "Wildcard search" section works the rule through on exactly this data set: "if you
// have a data set with the values prod, prods, and production, a search of prod* matches all
// values, whereas prod? matches only prod and prods". So the fixture is prod / prods /
// production plus a fourth subnet whose Name is the literal string `prod*`, which is what
// makes the escape rule assertable — `prod\*` must select that one and only that one.
//
// The prod? row is the one that matters most, because AWS's page contradicts itself about it.
// Two normative statements and three worked examples say '?' matches zero **or one**
// character; one sentence in the CLI examples says "exactly 1" and is refuted by its own next
// example. Substrate follows the majority reading, so `prod?` selects prod — with the '?'
// matching nothing — and that is asserted here rather than left to ec2globMatch's internals.
func TestEC2_FilterWildcards_ValueSemantics(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.7.0.0/16"}, &vpc)
	require.NotEmpty(t, vpc.VPCID)

	for i, name := range []string{"prod", "prods", "production", `prod*`} {
		ec2CreateTaggedSubnet(t, ts, vpc.VPCID,
			"10.7."+strconv.Itoa(i+1)+".0/24", "us-east-1a",
			map[string]string{"Name": name})
	}

	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{"no wildcard is still exact", "prod", []string{"prod"}},
		{"'*' matches zero or more", "prod*", []string{"prod", `prod*`, "prods", "production"}},
		{"'*' matches zero characters", "production*", []string{"production"}},
		{"'*' matches in the middle", "*duct*", []string{"production"}},
		{"leading '*'", "*s", []string{"prods"}},
		{"'?' matches zero or one", "prod?", []string{"prod", `prod*`, "prods"}},
		{"'?' matches exactly the one it needs", "pro?", []string{"prod"}},
		{"'?' does not match two", "prod??", []string{"prod", `prod*`, "prods"}},
		{"an escaped '*' is a literal", `prod\*`, []string{`prod*`}},
		{"values are case sensitive", "PROD*", []string{}},
		{"a wildcard matching nothing is an empty answer", "stag*", []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ec2WildcardSubnetNames(t, ts, tt.value))
		})
	}
}

// TestEC2_FilterWildcards_EveryOperation is #697's headline: one wildcard case per matcher
// that compared exactly before it.
//
// Each case selects with a pattern that is not a legal exact value, so an exact comparison
// answers with the empty set — the fixture always holds a resource that must be excluded, so
// a matcher that wrongly matched everything fails too. Together they cover all nine
// previously-exact matchers: instances, volumes, images, snapshots, subnets, security groups,
// route tables, NAT gateways and fleets.
func TestEC2_FilterWildcards_EveryOperation(t *testing.T) {
	t.Parallel()

	t.Run("DescribeInstances", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		wanted := bdmRunInstances(t, ts, map[string]string{"InstanceType": "t3.micro"})
		bdmRunInstances(t, ts, map[string]string{"InstanceType": "m5.large"})
		require.Len(t, wanted, 1)

		var desc describedInstances
		ec2FleetXML(t, ts, ec2WithAction("DescribeInstances",
			ec2Filter("instance-type", "t3.*")), &desc)
		require.Len(t, desc.Instances, 1, "t3.* should select only the t3 instance")
		assert.Equal(t, wanted[0], desc.Instances[0].InstanceID)
	})

	t.Run("DescribeVolumes", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		gp3 := volCreate(t, ts, map[string]string{"VolumeType": "gp3"})
		volCreate(t, ts, map[string]string{"VolumeType": "io1", "Iops": "100"})

		assert.Equal(t, []string{gp3.VolumeID},
			volIDs(volDescribe(t, ts, ec2Filter("volume-type", "gp?"))),
			"gp? should select gp3 and not io1")
	})

	t.Run("DescribeImages", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		tagged, untagged := ec2ImageFilterFixture(t, ts)

		got := ec2FilteredImageIDs(t, ts, ec2Filter("tag:Env", "pro*"))
		assert.Equal(t, []string{tagged}, got)
		assert.NotContains(t, got, untagged)
	})

	t.Run("DescribeSnapshots", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		snaps := ec2SeedTaggedSnapshots(t, ts, "alpha", "beta")

		assert.Equal(t, []string{snaps["alpha"]},
			ec2SnapshotIDsFor(t, ts, ec2Filter("tag:Scope", "alph?")))
	})

	t.Run("DescribeSubnets", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		f := newSubnetFixture(t, ts)

		assert.Equal(t, []string{f.alpha},
			ec2SubnetIDsFor(t, ts, ec2Filter("tag:Scope", "*pha")))
	})

	t.Run("DescribeSecurityGroups", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		var vpc struct {
			VPCID string `xml:"vpc>vpcId"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.8.0.0/16"}, &vpc)
		for _, name := range []string{"web-tier", "db-tier"} {
			ec2FleetXML(t, ts, map[string]string{
				"Action": "CreateSecurityGroup", "GroupName": name,
				"GroupDescription": name, "VpcId": vpc.VPCID,
			}, nil)
		}

		assert.Equal(t, []string{"web-tier"},
			ec2SecurityGroupNames(t, ts, ec2Filter("group-name", "web-*")))
	})

	t.Run("DescribeRouteTables", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		// newSubnetFixture's launch mints the default VPC, whose own route table is
		// associated with a subnet — so both filters below have a route table to exclude.
		f := newSubnetFixture(t, ts)
		rtbID := ec2CreateRouteTableID(t, ts, f.vpcID)
		ec2FleetXML(t, ts, map[string]string{
			"Action": "AssociateRouteTable", "RouteTableId": rtbID, "SubnetId": f.alpha,
		}, nil)

		// association.subnet-id is a filter value, so it globs — where the same string in
		// SubnetId.N would be a malformed ID (see routeTableHasSubnet).
		assert.Equal(t, []string{rtbID},
			ec2RouteTableIDsFor(t, ts, ec2Filter("association.subnet-id", f.alpha+"*")))

		// vpc-id is asserted against the exact filter's own answer rather than a literal
		// list, because CreateVpc mints a main route table alongside the one created
		// above. The property under test is that a wildcard answers what the exact value
		// answers, and that it is still narrower than no filter at all.
		exact := ec2RouteTableIDsFor(t, ts, ec2Filter("vpc-id", f.vpcID))
		require.NotEmpty(t, exact)
		require.Less(t, len(exact), len(ec2RouteTableIDsFor(t, ts, nil)),
			"the default VPC's route table is what the filter must exclude")
		assert.Equal(t, exact, ec2RouteTableIDsFor(t, ts, ec2Filter("vpc-id", f.vpcID+"*")))
	})

	t.Run("DescribeNatGateways", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		natID := ec2CreateNatGatewayID(t, ts)

		assert.Equal(t, []string{natID}, ec2NatGatewayIDsFor(t, ts, ec2Filter("state", "avail*")))
		assert.Empty(t, ec2NatGatewayIDsFor(t, ts, ec2Filter("state", "pend*")))
	})

	t.Run("DescribeFleets", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ltID := newFleetLaunchTemplate(t, ts, "fleet-wildcards")
		var maintain createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "maintain",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &maintain)

		var got describeFleetsResp
		ec2FleetXML(t, ts, ec2WithAction("DescribeFleets", ec2Filter("type", "main*")), &got)
		require.Len(t, got.Fleets, 1)
		assert.Equal(t, maintain.FleetID, got.Fleets[0].FleetID)

		var none describeFleetsResp
		ec2FleetXML(t, ts, ec2WithAction("DescribeFleets", ec2Filter("type", "spot*")), &none)
		assert.Empty(t, none.Fleets)
	})
}

// TestEC2_DescribeVolumes_DeleteOnTerminationIsCaseSensitive pins the one behavior change
// #697 makes to an answer a caller could already have been getting.
//
// attachment.delete-on-termination was the tree's only case-*insensitive* filter-value
// comparison: a hand-rolled loop lowercased the request value, so "True" selected a
// delete-on-termination attachment. AWS documents the opposite twice — Using_Filtering's
// "Filter values are case sensitive" and the Filter type's "Filter values are
// case-sensitive" — so it now selects nothing, and the lowercase form that AWS's own
// examples use still works.
func TestEC2_DescribeVolumes_DeleteOnTerminationIsCaseSensitive(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ids := bdmRunInstances(t, ts, map[string]string{
		"BlockDeviceMapping.1.DeviceName":              "/dev/xvda",
		"BlockDeviceMapping.1.Ebs.VolumeSize":          "30",
		"BlockDeviceMapping.2.DeviceName":              "/dev/sdf",
		"BlockDeviceMapping.2.Ebs.VolumeSize":          "40",
		"BlockDeviceMapping.2.Ebs.DeleteOnTermination": "false",
	})
	require.Len(t, ids, 1)

	deleted := bdmDescribeVolumes(t, ts, ec2Filter("attachment.delete-on-termination", "true"))
	require.Len(t, deleted, 1, "the lowercase form AWS's examples use still selects")
	assert.Equal(t, "/dev/xvda", deleted[0].bdmDevice())

	assert.Empty(t, bdmDescribeVolumes(t, ts,
		ec2Filter("attachment.delete-on-termination", "True")),
		`"True" is not "true"; AWS documents filter values as case-sensitive`)

	// A wildcard still reaches both, which is what makes the case rule a rule about the
	// comparison rather than about the boolean rendering.
	assert.Len(t, bdmDescribeVolumes(t, ts,
		ec2Filter("attachment.delete-on-termination", "*e")), 2)
}

// TestEC2_FilterWildcards_IdentifierParametersStayExact pins what #697 must not reach.
//
// AWS documents wildcards for Filter.N.Value.N only. An identifier parameter list asserts
// that the resources it names exist, so globbing one would let `i-*` stand in for an ID and
// quietly turn an Invalid*.NotFound contract into a match. Filter *names* are excluded for a
// different reason: they are one of a fixed documented set, and the Filter type says "Filter
// names are case-sensitive", so a name is either in the set or refused.
func TestEC2_FilterWildcards_IdentifierParametersStayExact(t *testing.T) {
	t.Parallel()

	t.Run("KeyName.N on DescribeKeyPairs", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2FleetXML(t, ts, map[string]string{"Action": "CreateKeyPair", "KeyName": "alpha-key"}, nil)

		require.Equal(t, []string{"alpha-key"}, ec2KeyPairNamesFor(t, ts, "alpha-key"))
		assert.Empty(t, ec2KeyPairNamesFor(t, ts, "alpha-*"),
			"KeyName.N names a key pair; it is not a pattern")
	})

	t.Run("GroupName.N on DescribePlacementGroups", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreatePlacementGroup", "GroupName": "alpha-pg", "Strategy": "cluster",
		}, nil)

		require.Equal(t, []string{"alpha-pg"}, ec2PlacementGroupNamesFor(t, ts, "alpha-pg"))
		assert.Empty(t, ec2PlacementGroupNamesFor(t, ts, "alpha-*"))
	})

	t.Run("FleetId.N on DescribeFleets", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ltID := newFleetLaunchTemplate(t, ts, "fleet-ids-exact")
		var fleet createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "maintain",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &fleet)

		var got describeFleetsResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "DescribeFleets", "FleetId.1": fleet.FleetID + "*",
		}, &got)
		assert.Empty(t, got.Fleets)
	})

	t.Run("InstanceType.N on DescribeInstanceTypes", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		status, code := ec2ErrorCode(t, ts, map[string]string{
			"Action": "DescribeInstanceTypes", "InstanceType.1": "t3.*",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidInstanceType", code,
			"InstanceType.N asserts the type exists, so a pattern is a bad request")
	})

	t.Run("the filter name itself", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		status, code := ec2ErrorCode(t, ts, map[string]string{
			"Action":           "DescribeSnapshots",
			"Filter.1.Name":    "tag-ke?",
			"Filter.1.Value.1": "Scope",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})
}

// TestEC2_Filter_RequestLimits covers the three limits AWS publishes for Filter.N, enforced
// in ec2FilterSpec.check so that every operation with a spec inherits them.
//
// Using_Filtering states all three: "You can specify up to 50 filters and up to 200 total
// filter values in a single request" and "Filter strings can be up to 255 characters in
// length." The Filter type's own reference page publishes none of them, which is why the
// numbers are cited from the filtering guide.
//
// Provenance: no EC2 error code exists for a filter limit. Every *LimitExceeded in the
// client-error table names a resource quota — KeyPairLimitExceeded, NatGatewayLimitExceeded
// and so on — so substrate answers InvalidParameterValue, whose documented gloss is "A value
// specified in a parameter is not valid, is unsupported, or cannot be used." The code is
// substrate's reading; the limits are AWS's.
func TestEC2_Filter_RequestLimits(t *testing.T) {
	t.Parallel()

	// Two operations, to show the limits come from the shared check rather than a handler.
	for _, op := range []struct{ action, filter string }{
		{"DescribeSnapshots", "tag-key"},
		{"DescribeTags", "key"},
	} {
		t.Run(op.action, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)

			tests := []struct {
				name     string
				params   map[string]string
				wantCode string
			}{
				{"50 filters is accepted", ec2RepeatedFilters(op.filter, 50), ""},
				{"51 filters is refused", ec2RepeatedFilters(op.filter, 51), "InvalidParameterValue"},
				{"200 values is accepted", ec2FilterWithValues(op.filter, 200), ""},
				{"201 values is refused", ec2FilterWithValues(op.filter, 201), "InvalidParameterValue"},
				{
					"a 255-character value is accepted",
					ec2Filter(op.filter, ec2RepeatChar('a', 255)), "",
				},
				{
					"a 256-character value is refused",
					ec2Filter(op.filter, ec2RepeatChar('a', 256)), "InvalidParameterValue",
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					status, code := ec2ErrorCode(t, ts, ec2WithAction(op.action, tt.params))
					assert.Equal(t, tt.wantCode, code)
					if tt.wantCode == "" {
						assert.Equal(t, http.StatusOK, status)
					} else {
						assert.Equal(t, http.StatusBadRequest, status)
					}
				})
			}
		})
	}
}

// ec2WithAction returns params with Action set, leaving the caller's map untouched — several
// helpers here take a filter map built by ec2Filter and must not mutate it.
func ec2WithAction(action string, params map[string]string) map[string]string {
	full := map[string]string{"Action": action}
	for k, v := range params {
		full[k] = v
	}
	return full
}

// ec2RepeatedFilters builds n Filter.N entries all naming the same filter, each with one
// value. Repeating one documented name is what isolates the count limit: ec2FilterSpec.check
// tests the count before it validates the name, so an undocumented name would be refused for
// the wrong reason.
func ec2RepeatedFilters(name string, n int) map[string]string {
	params := make(map[string]string, n*2)
	for i := 1; i <= n; i++ {
		params["Filter."+strconv.Itoa(i)+".Name"] = name
		params["Filter."+strconv.Itoa(i)+".Value.1"] = "v" + strconv.Itoa(i)
	}
	return params
}

// ec2FilterWithValues builds one filter carrying n values, for the total-values limit.
func ec2FilterWithValues(name string, n int) map[string]string {
	params := map[string]string{"Filter.1.Name": name}
	for i := 1; i <= n; i++ {
		params["Filter.1.Value."+strconv.Itoa(i)] = "v" + strconv.Itoa(i)
	}
	return params
}

// ec2RepeatChar returns a string of n copies of c, for the value-length limit.
func ec2RepeatChar(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}

// ec2CreateRouteTableID creates a route table in vpcID and returns its ID.
func ec2CreateRouteTableID(t *testing.T, ts *httptest.Server, vpcID string) string {
	t.Helper()
	var res struct {
		RouteTableID string `xml:"routeTable>routeTableId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID}, &res)
	require.NotEmpty(t, res.RouteTableID)
	return res.RouteTableID
}

// ec2RouteTableIDsFor returns the route table IDs DescribeRouteTables reports for params,
// sorted.
//
// Sorted here rather than asserted in response order because describeRouteTables walks
// StateManager.List, which documents no ordering — unlike describeTags, which sorts its own
// answer for the reason ec2SortTagDescriptions records. What these cases assert is which
// route tables a filter selected, not the sequence they arrived in.
func ec2RouteTableIDsFor(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	var res struct {
		IDs []string `xml:"routeTableSet>item>routeTableId"`
	}
	ec2FleetXML(t, ts, ec2WithAction("DescribeRouteTables", params), &res)
	sort.Strings(res.IDs)
	return res.IDs
}

// ec2CreateNatGatewayID builds the subnet and elastic IP a NAT gateway needs and returns the
// gateway's ID.
func ec2CreateNatGatewayID(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.6.0.0/16"}, &vpc)
	var subnet struct {
		SubnetID string `xml:"subnet>subnetId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc.VPCID, "CidrBlock": "10.6.1.0/24",
	}, &subnet)
	var addr struct {
		AllocationID string `xml:"allocationId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"}, &addr)

	var nat struct {
		NatGatewayID string `xml:"natGateway>natGatewayId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": subnet.SubnetID, "AllocationId": addr.AllocationID,
	}, &nat)
	require.NotEmpty(t, nat.NatGatewayID)
	return nat.NatGatewayID
}

// ec2NatGatewayIDsFor returns the gateway IDs DescribeNatGateways reports for params,
// sorted for the reason [ec2RouteTableIDsFor] gives.
func ec2NatGatewayIDsFor(t *testing.T, ts *httptest.Server, params map[string]string) []string {
	t.Helper()
	var res struct {
		IDs []string `xml:"natGatewaySet>item>natGatewayId"`
	}
	ec2FleetXML(t, ts, ec2WithAction("DescribeNatGateways", params), &res)
	sort.Strings(res.IDs)
	return res.IDs
}

// ec2KeyPairNamesFor returns the key-pair names DescribeKeyPairs reports for one KeyName.1.
func ec2KeyPairNamesFor(t *testing.T, ts *httptest.Server, keyName string) []string {
	t.Helper()
	var res struct {
		Names []string `xml:"keySet>item>keyName"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeKeyPairs", "KeyName.1": keyName}, &res)
	return res.Names
}

// ec2PlacementGroupNamesFor returns the names DescribePlacementGroups reports for one
// GroupName.1.
func ec2PlacementGroupNamesFor(t *testing.T, ts *httptest.Server, groupName string) []string {
	t.Helper()
	var res struct {
		Names []string `xml:"placementGroupSet>item>groupName"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "DescribePlacementGroups", "GroupName.1": groupName,
	}, &res)
	return res.Names
}
