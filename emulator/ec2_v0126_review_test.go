package emulator_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Review follow-ups to #1464, #1440, #1453 and #1436.

// TestEC2_Spot_OpenRequestIsFulfilledOnceTheSeedClears pins that a wait-for-fulfillment loop
// terminates: a request left open by a capacity seed is fulfilled by the first describe after
// the seed is cleared, and not before.
func TestEC2_Spot_OpenRequestIsFulfilledOnceTheSeedClears(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedSpotOutcome(t, ts, `{"statusCode":"capacity-not-available"}`)

	var created struct {
		Requests []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "Type": "persistent", "LaunchSpecification.ImageId": ec2TestImage,
	}, &created)
	require.Len(t, created.Requests, 1)
	id := created.Requests[0].ID

	r := ec2SpotRequest(t, ts, id)
	assert.Equal(t, "open", r.State, "still seeded: the describe launches nothing")
	assert.Empty(t, r.InstanceID)

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/ec2/spot-request-outcomes", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	r = ec2SpotRequest(t, ts, id)
	assert.Equal(t, "active", r.State)
	assert.Equal(t, "fulfilled", r.StatusCode)
	require.NotEmpty(t, r.InstanceID)
	got := ec2SpotInstances(t, ts, map[string]string{"InstanceId.1": r.InstanceID})
	require.Len(t, got, 1)
	assert.Equal(t, "spot", got[0].Lifecycle)
	assert.Equal(t, id, got[0].RequestID)

	assert.Equal(t, r.InstanceID, ec2SpotRequest(t, ts, id).InstanceID, "a second describe launches nothing more")
}

// TestEC2_Spot_OpenRequestPastValidUntilExpires pins the other way out of open.
func TestEC2_Spot_OpenRequestPastValidUntilExpires(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedSpotOutcome(t, ts, `{"statusCode":"price-too-low"}`)
	var created struct {
		Requests []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "Type": "persistent", "ValidUntil": "2001-01-01T00:00:00Z",
		"LaunchSpecification.ImageId": ec2TestImage,
	}, &created)
	require.Len(t, created.Requests, 1)
	r := ec2SpotRequest(t, ts, created.Requests[0].ID)
	assert.Equal(t, "closed", r.State)
	assert.Equal(t, "schedule-expired", r.StatusCode)
}

// TestEC2_CreateFleet_SpotPoolInstancesAreSpot pins that a fleet's Spot pool launches Spot
// Instances, so instance-lifecycle=spot selects them, and that no Spot Instance request is
// written for them.
func TestEC2_CreateFleet_SpotPoolInstancesAreSpot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-spot-lifecycle")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet", "Type": "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
		"TargetCapacitySpecification.SpotTargetCapacity":                       "2",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "spot",
	}, &got)
	require.Len(t, got.Instances, 1)
	require.Len(t, got.Instances[0].InstanceIDs, 2)

	spot := ec2SpotInstances(t, ts, ec2Filter("instance-lifecycle", "spot"))
	require.Len(t, spot, 2, "the fleet's Spot instances are selected by instance-lifecycle=spot")
	for _, inst := range spot {
		assert.Empty(t, inst.RequestID, "a fleet Spot Instance has no sir- request")
	}
	requests, _ := ec2DescribeSpotRequests(t, ts, nil)
	assert.Empty(t, requests)
}

// TestEC2_SecurityGroup_PairNamesResolveToIDs pins that a rule authorized by group name is
// revoked by group ID, and the other way round.
func TestEC2_SecurityGroup_PairNamesResolveToIDs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		authorize, revoke func(id string) map[string]string
	}{
		{"authorize by name, revoke by ID",
			func(string) map[string]string { return map[string]string{"IpPermissions.1.Groups.1.GroupName": "peer"} },
			func(id string) map[string]string { return map[string]string{"IpPermissions.1.Groups.1.GroupId": id} }},
		{"authorize by ID, revoke by name",
			func(id string) map[string]string { return map[string]string{"IpPermissions.1.Groups.1.GroupId": id} },
			func(string) map[string]string { return map[string]string{"IpPermissions.1.Groups.1.GroupName": "peer"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
			peer := ec2CreateSG(t, ts, "peer", "peer", vpcID)
			groupID := ec2CreateSG(t, ts, "app", "app", vpcID)
			call := func(action string, source map[string]string) {
				params := map[string]string{
					"Action": action, "GroupId": groupID,
					"IpPermissions.1.IpProtocol": "tcp", "IpPermissions.1.FromPort": "5432", "IpPermissions.1.ToPort": "5432",
				}
				for k, v := range source {
					params[k] = v
				}
				ec2FleetXML(t, ts, params, nil)
			}
			call("AuthorizeSecurityGroupIngress", tc.authorize(peer))
			groups := ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": groupID})
			require.Len(t, groups[0].Ingress, 1)
			require.Len(t, groups[0].Ingress[0].Groups, 1)
			assert.Equal(t, peer, groups[0].Ingress[0].Groups[0].GroupID, "a named source is stored with its ID")

			call("RevokeSecurityGroupIngress", tc.revoke(peer))
			assert.Empty(t, ec2DescribeSGs(t, ts, map[string]string{"GroupId.1": groupID})[0].Ingress)
		})
	}
}

// TestEC2_CreateSecurityGroup_RefusesTheReservedName pins EC2's observed refusal of "default",
// in any case.
func TestEC2_CreateSecurityGroup_RefusesTheReservedName(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	vpcID := ec2CreateVPC(t, ts, "10.0.0.0/16")
	for _, name := range []string{"default", "Default"} {
		status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
			"Action": "CreateSecurityGroup", "GroupName": name, "GroupDescription": "x", "VpcId": vpcID,
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Equal(t, "Cannot use reserved security group name: "+name, msg)
	}
}

// TestEC2_DefaultVPC_LaunchUsesThePlacementZonesSubnet pins that a subnet-less launch naming a
// zone lands in that zone's default subnet, and is refused when the zone has none.
func TestEC2_DefaultVPC_LaunchUsesThePlacementZonesSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	view := ec2ReadDefaultVPC(t, call)

	var run struct {
		SubnetID string `xml:"instancesSet>item>subnetId"`
		Zone     string `xml:"instancesSet>item>placement>availabilityZone"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"Placement.AvailabilityZone": "us-east-1b",
	}, &run)
	assert.Equal(t, view.subnets[1].SubnetID, run.SubnetID, "the zone-b default subnet")
	assert.Equal(t, "us-east-1b", run.Zone)

	call(map[string]string{"Action": "DeleteSubnet", "SubnetId": view.subnets[2].SubnetID})
	status, code, msg := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"Placement.AvailabilityZone": "us-east-1c",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidInput", code)
	assert.Equal(t, "No default subnet for availability zone: 'us-east-1c'.", msg)
}

// TestEC2_CreateDefaultSubnet_AvoidsOverlapAndRaces pins the CIDR choice against a caller's own
// subnet in the default VPC, and that concurrent creates for one zone make one subnet.
func TestEC2_CreateDefaultSubnet_AvoidsOverlapAndRaces(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	view := ec2ReadDefaultVPC(t, call)

	call(map[string]string{"Action": "DeleteSubnet", "SubnetId": view.subnets[1].SubnetID})
	// A /24 inside the freed 172.31.16.0/20: the /20 is no longer free.
	ec2CreateTaggedSubnet(t, ts, view.vpcID, "172.31.17.0/24", "us-east-1b", nil)

	const callers = 6
	var wg sync.WaitGroup
	statuses := make([]int, callers)
	bodies := make([]string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			form := url.Values{"Action": {"CreateDefaultSubnet"}, "AvailabilityZone": {"us-east-1b"}}
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
			if err != nil {
				return
			}
			req.Host = "ec2.us-east-1.amazonaws.com"
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			statuses[i], bodies[i] = resp.StatusCode, string(raw)
		}(i)
	}
	wg.Wait()

	created := 0
	for i := range statuses {
		if statuses[i] == http.StatusOK {
			created++
			assert.Contains(t, bodies[i], "<cidrBlock>172.31.48.0/20</cidrBlock>",
				"the first /20 overlapping no subnet of the VPC")
		} else {
			assert.Contains(t, bodies[i], "DefaultSubnetAlreadyExistsInAvailabilityZone")
		}
	}
	assert.Equal(t, 1, created, "one default subnet per zone, however many callers race")
}

// TestEC2_DefaultVPC_TimeTravelHonorsTheSwitch pins that a time-travel replay registers its
// plugins with the server's default-VPC setting, so a server started without one does not
// show one in the past either.
func TestEC2_DefaultVPC_TimeTravelHonorsTheSwitch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []emulator.TestServerOption
		want bool
	}{
		{"seeded", []emulator.TestServerOption{emulator.WithRecordedBodies()}, true},
		{"switched off", []emulator.TestServerOption{emulator.WithRecordedBodies(), emulator.WithoutDefaultVPC()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t, tc.opts...)
			ec2ReplayCall(t, ts, "DescribeVpcs", url.Values{})
			resp, err := http.Get(ts.URL + "/v1/debug/events/1000/state")
			require.NoError(t, err)
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
			assert.Equal(t, tc.want, strings.Contains(string(raw), "default_vpc:"), string(raw))
		})
	}
}

// TestEC2_DefaultVPC_ARecordingReplaysWithZeroDifferences pins that a recording made under this
// release — a subnet-less launch into the seeded default VPC, a group placed there without a
// VpcId, and a Spot request replaced on terminate — replays identically, state hashes included.
func TestEC2_DefaultVPC_ARecordingReplaysWithZeroDifferences(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	ts.FreezeTime()

	ec2ReplayCall(t, ts, "RunInstances", url.Values{
		"ImageId": {ec2TestImage}, "MinCount": {"1"}, "MaxCount": {"1"},
	})
	ec2ReplayCall(t, ts, "CreateSecurityGroup", url.Values{"GroupName": {"workers"}, "GroupDescription": {"w"}})
	body := ec2ReplayCall(t, ts, "RunInstances", url.Values{
		"ImageId": {ec2TestImage}, "MinCount": {"1"}, "MaxCount": {"1"},
		"InstanceMarketOptions.MarketType":                               {"spot"},
		"InstanceMarketOptions.SpotOptions.SpotInstanceType":             {"persistent"},
		"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": {"stop"},
	})
	instanceID := xmlElement(t, body, "instanceId")
	ec2ReplayCall(t, ts, "TerminateInstances", url.Values{"InstanceId.1": {instanceID}})
	ec2ReplayCall(t, ts, "DescribeSpotInstanceRequests", url.Values{})

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid, "state_hash_after matches: %v", results.StateErrors)
}

// xmlElement returns the text of the first <name> element in body.
func xmlElement(t *testing.T, body []byte, name string) string {
	t.Helper()
	s := string(body)
	start := strings.Index(s, "<"+name+">")
	require.GreaterOrEqual(t, start, 0, "no <%s> in %s", name, s)
	start += len(name) + 2
	end := strings.Index(s[start:], "</"+name+">")
	require.GreaterOrEqual(t, end, 0)
	return s[start : start+end]
}
