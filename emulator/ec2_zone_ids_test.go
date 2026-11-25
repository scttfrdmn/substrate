package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Zone IDs on subnets and per-zone instance-type offerings (#1621).

type ec2SubnetZoneDoc struct {
	ID     string `xml:"subnetId"`
	Zone   string `xml:"availabilityZone"`
	ZoneID string `xml:"availabilityZoneId"`
}

func TestEC2_Subnets_AvailabilityZoneID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpc := ec2CreateVPC(t, ts, "10.0.0.0/16")

	var byID struct {
		Subnet ec2SubnetZoneDoc `xml:"subnet"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc, "CidrBlock": "10.0.1.0/24", "AvailabilityZoneId": "use1-az2",
	}, &byID)
	assert.Equal(t, "us-east-1b", byID.Subnet.Zone, "the zone ID resolves to its name")
	assert.Equal(t, "use1-az2", byID.Subnet.ZoneID)

	var byName struct {
		Subnet ec2SubnetZoneDoc `xml:"subnet"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpc, "CidrBlock": "10.0.2.0/24", "AvailabilityZone": "us-east-1c",
	}, &byName)
	assert.Equal(t, "use1-az3", byName.Subnet.ZoneID)

	var described struct {
		Subnets []ec2SubnetZoneDoc `xml:"subnetSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSubnets"}, &described)
	require.NotEmpty(t, described.Subnets)
	for _, s := range described.Subnets {
		assert.NotEmpty(t, s.ZoneID, "subnet %s in %s carries its zone ID, default subnets included", s.ID, s.Zone)
	}

	for _, name := range []string{"availability-zone-id", "availabilityZoneId"} {
		var filtered struct {
			Subnets []ec2SubnetZoneDoc `xml:"subnetSet>item"`
		}
		params := ec2Filter(name, "use1-az2")
		params["Action"] = "DescribeSubnets"
		params["Filter.2.Name"] = "vpc-id"
		params["Filter.2.Value.1"] = vpc
		ec2FleetXML(t, ts, params, &filtered)
		require.Len(t, filtered.Subnets, 1, name)
		assert.Equal(t, byID.Subnet.ID, filtered.Subnets[0].ID)
	}

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"both name and ID", map[string]string{"AvailabilityZone": "us-east-1a", "AvailabilityZoneId": "use1-az1"}, "InvalidParameterCombination"},
		{"an ID of no zone", map[string]string{"AvailabilityZoneId": "use1-az9"}, "InvalidParameterValue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params["Action"] = "CreateSubnet"
			tc.params["VpcId"] = vpc
			tc.params["CidrBlock"] = "10.0.9.0/24"
			status, code := ec2ErrorCode(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestEC2_Offerings_GravitonAbsentFromUse1AZ3(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	for _, tc := range []struct {
		locationType, excluded string
	}{
		{"availability-zone", "us-east-1c"},
		{"availability-zone-id", "use1-az3"},
	} {
		t.Run(tc.locationType, func(t *testing.T) {
			graviton := ec2Offerings(t, ts, map[string]string{
				"LocationType": tc.locationType, "Filter.1.Name": "instance-type", "Filter.1.Value.1": "m7g.large",
			})
			require.Len(t, graviton, 2)
			for _, o := range graviton {
				assert.NotEqual(t, tc.excluded, o.Location)
			}
			x86 := ec2Offerings(t, ts, map[string]string{
				"LocationType": tc.locationType, "Filter.1.Name": "instance-type", "Filter.1.Value.1": "m5.large",
			})
			assert.Len(t, x86, 3, "an x86 type is offered in every zone")
		})
	}
	region := ec2Offerings(t, ts, map[string]string{
		"LocationType": "region", "Filter.1.Name": "instance-type", "Filter.1.Value.1": "m7g.large",
	})
	assert.Len(t, region, 1, "the region offers what any of its zones does")
}
