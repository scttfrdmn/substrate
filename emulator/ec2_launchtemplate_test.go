package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ltNetwork is the networking a launch-template test asks for and then asserts
// landed on the instance.
type ltNetwork struct {
	vpcID    string
	subnetID string
	sgID     string
}

// newLTNetwork creates a non-default VPC, subnet and security group. Non-default
// is what makes the assertions meaningful: a default subnet assigns a public IP
// and resolves a security group on its own, so a dropped template value would
// still produce a plausible-looking instance.
func newLTNetwork(t *testing.T, ts *httptest.Server, cidr, name string) ltNetwork {
	t.Helper()
	vpcID := createVPC(t, ts, cidr)
	return ltNetwork{
		vpcID:    vpcID,
		subnetID: createSubnet(t, ts, vpcID, cidr[:len(cidr)-6]+"1.0/24"),
		sgID:     createSecurityGroup(t, ts, vpcID, name),
	}
}

// createLaunchTemplate creates a launch template from the given
// LaunchTemplateData.* params and returns its ID.
func createLaunchTemplate(t *testing.T, ts *httptest.Server, name string, data map[string]string) string {
	t.Helper()
	params := map[string]string{
		"Action":             "CreateLaunchTemplate",
		"LaunchTemplateName": name,
	}
	for k, v := range data {
		params[k] = v
	}
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateLaunchTemplate: status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		LaunchTemplateID string `xml:"launchTemplate>launchTemplateId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode CreateLaunchTemplate: %v", err)
	}
	if out.LaunchTemplateID == "" {
		t.Fatal("CreateLaunchTemplate returned no launchTemplateId")
	}
	return out.LaunchTemplateID
}

// launchedInstance is the instance state a launch-template test inspects.
type launchedInstance struct {
	InstanceID      string `xml:"instanceId"`
	SubnetID        string `xml:"subnetId"`
	VpcID           string `xml:"vpcId"`
	PublicIPAddress string `xml:"publicIpAddress"`
	Groups          []struct {
		GroupID   string `xml:"groupId"`
		GroupName string `xml:"groupName"`
	} `xml:"groupSet>item"`
}

// describeInstance runs DescribeInstances for one instance ID.
func describeInstance(t *testing.T, ts *httptest.Server, id string) launchedInstance {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Instances []launchedInstance `xml:"reservationSet>item>instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	if len(out.Instances) != 1 {
		t.Fatalf("DescribeInstances returned %d instances, want 1", len(out.Instances))
	}
	return out.Instances[0]
}

// runInstance sends RunInstances with the given params and returns the launched
// instance ID.
func runInstance(t *testing.T, ts *httptest.Server, params map[string]string) string {
	t.Helper()
	full := map[string]string{"Action": "RunInstances", "MinCount": "1", "MaxCount": "1"}
	for k, v := range params {
		full[k] = v
	}
	resp := ec2Request(t, ts, full)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	if len(out.Instances) != 1 {
		t.Fatalf("RunInstances launched %d instances, want 1", len(out.Instances))
	}
	return out.Instances[0].InstanceID
}

// TestEC2_LaunchTemplate_SubnetSources walks the four ways a subnet can reach a
// launch, which is #444's own reproduction table. Cases A and D were the bug: a
// template's NetworkInterface was parsed and discarded, so an instance landed in
// a substrate-chosen subnet instead. B and C already worked and are regression
// guards here — the fix reorders the resolution, which is exactly the kind of
// change that breaks the paths that were already correct.
func TestEC2_LaunchTemplate_SubnetSources(t *testing.T) {
	t.Run("A: template network interface, via RunInstances", func(t *testing.T) {
		ts := newEC2TestServer(t)
		net := newLTNetwork(t, ts, "10.1.0.0/16", "lt-case-a")
		ltID := createLaunchTemplate(t, ts, "case-a", map[string]string{
			"LaunchTemplateData.ImageId":                        "ami-0case00000000001",
			"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
			"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
		})

		id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
		if got := describeInstance(t, ts, id); got.SubnetID != net.subnetID {
			t.Errorf("subnetId = %q, want %q from the template's network interface",
				got.SubnetID, net.subnetID)
		}
	})

	t.Run("B: call-level SubnetId", func(t *testing.T) {
		ts := newEC2TestServer(t)
		net := newLTNetwork(t, ts, "10.2.0.0/16", "lt-case-b")
		ltID := createLaunchTemplate(t, ts, "case-b", map[string]string{
			"LaunchTemplateData.ImageId": "ami-0case00000000002",
		})

		id := runInstance(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateId": ltID,
			"SubnetId":                        net.subnetID,
		})
		if got := describeInstance(t, ts, id); got.SubnetID != net.subnetID {
			t.Errorf("subnetId = %q, want %q from the call", got.SubnetID, net.subnetID)
		}
	})

	t.Run("C: CreateFleet override", func(t *testing.T) {
		ts := newEC2TestServer(t)
		net := newLTNetwork(t, ts, "10.3.0.0/16", "lt-case-c")
		ltID := createLaunchTemplate(t, ts, "case-c", map[string]string{
			"LaunchTemplateData.ImageId": "ami-0case00000000003",
		})

		var fleet createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "instant",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"LaunchTemplateConfigs.1.Overrides.1.SubnetId":                         net.subnetID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &fleet)

		ids := fleet.instanceIDs()
		if len(ids) != 1 {
			t.Fatalf("fleet launched %d instances, want 1", len(ids))
		}
		if got := describeInstance(t, ts, ids[0]); got.SubnetID != net.subnetID {
			t.Errorf("subnetId = %q, want %q from the fleet override", got.SubnetID, net.subnetID)
		}
	})

	t.Run("D: template network interface, via CreateFleet with no override", func(t *testing.T) {
		ts := newEC2TestServer(t)
		net := newLTNetwork(t, ts, "10.4.0.0/16", "lt-case-d")
		ltID := createLaunchTemplate(t, ts, "case-d", map[string]string{
			"LaunchTemplateData.ImageId":                        "ami-0case00000000004",
			"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
			"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
		})

		var fleet createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "instant",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &fleet)

		ids := fleet.instanceIDs()
		if len(ids) != 1 {
			t.Fatalf("fleet launched %d instances, want 1", len(ids))
		}
		if got := describeInstance(t, ts, ids[0]); got.SubnetID != net.subnetID {
			t.Errorf("subnetId = %q, want %q from the template's network interface",
				got.SubnetID, net.subnetID)
		}
	})
}

// TestEC2_LaunchTemplate_SubnetPrecedence pins AWS's ordering: a value named in
// the request wins over the template's, and a fleet override wins over both.
// Without an explicit assertion the fix could satisfy the four cases above while
// letting a template silently override an explicit request.
func TestEC2_LaunchTemplate_SubnetPrecedence(t *testing.T) {
	ts := newEC2TestServer(t)
	templateNet := newLTNetwork(t, ts, "10.5.0.0/16", "lt-prec-template")
	callNet := newLTNetwork(t, ts, "10.6.0.0/16", "lt-prec-call")
	overrideNet := newLTNetwork(t, ts, "10.7.0.0/16", "lt-prec-override")

	ltID := createLaunchTemplate(t, ts, "precedence", map[string]string{
		"LaunchTemplateData.ImageId":                        "ami-0prec00000000001",
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":    templateNet.subnetID,
	})

	t.Run("call beats template", func(t *testing.T) {
		id := runInstance(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateId": ltID,
			"SubnetId":                        callNet.subnetID,
		})
		if got := describeInstance(t, ts, id); got.SubnetID != callNet.subnetID {
			t.Errorf("subnetId = %q, want the call's %q, not the template's %q",
				got.SubnetID, callNet.subnetID, templateNet.subnetID)
		}
	})

	t.Run("nested call network interface beats template", func(t *testing.T) {
		id := runInstance(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateId": ltID,
			"NetworkInterface.1.DeviceIndex":  "0",
			"NetworkInterface.1.SubnetId":     callNet.subnetID,
		})
		if got := describeInstance(t, ts, id); got.SubnetID != callNet.subnetID {
			t.Errorf("subnetId = %q, want the call's %q, not the template's %q",
				got.SubnetID, callNet.subnetID, templateNet.subnetID)
		}
	})

	t.Run("fleet override beats template", func(t *testing.T) {
		var fleet createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "instant",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"LaunchTemplateConfigs.1.Overrides.1.SubnetId":                         overrideNet.subnetID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		}, &fleet)

		ids := fleet.instanceIDs()
		if len(ids) != 1 {
			t.Fatalf("fleet launched %d instances, want 1", len(ids))
		}
		if got := describeInstance(t, ts, ids[0]); got.SubnetID != overrideNet.subnetID {
			t.Errorf("subnetId = %q, want the override's %q, not the template's %q",
				got.SubnetID, overrideNet.subnetID, templateNet.subnetID)
		}
	})
}

// TestEC2_LaunchTemplate_SecurityGroups covers both spellings of the network
// interface's group list. Real SDKs send SecurityGroupId.N — the AWS model gives
// that member the locationName "SecurityGroupId" — while #444 suggested Groups.N;
// substrate accepts either, so a hand-built request works too.
//
// The group is asserted through RunInstances' own cross-VPC validation rather than
// a read of the instance: a group that does not belong to the subnet's VPC is
// rejected with InvalidGroup.NotFound, which can only happen if the template's
// list was actually read. Instances do not yet echo a groupSet at all — that is
// the other half of #444, and its tests assert the template's groups directly.
func TestEC2_LaunchTemplate_SecurityGroups(t *testing.T) {
	tests := []struct {
		name  string
		param string
	}{
		{name: "SecurityGroupId.N (what SDKs send)", param: "LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1"},
		{name: "Groups.N (hand-built form)", param: "LaunchTemplateData.NetworkInterface.1.Groups.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			net := newLTNetwork(t, ts, "10.8.0.0/16", "lt-sg")
			other := newLTNetwork(t, ts, "10.11.0.0/16", "lt-sg-other")

			// A group from the template's own VPC launches.
			sameVPC := createLaunchTemplate(t, ts, "sg-same-"+tt.param, map[string]string{
				"LaunchTemplateData.ImageId":                        "ami-0sg000000000001",
				"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
				"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
				tt.param: net.sgID,
			})
			id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": sameVPC})
			if got := describeInstance(t, ts, id); got.SubnetID != net.subnetID {
				t.Errorf("subnetId = %q, want %q", got.SubnetID, net.subnetID)
			}

			// A group from a different VPC than the template's subnet must be
			// rejected — which proves the list was read rather than ignored.
			crossVPC := createLaunchTemplate(t, ts, "sg-cross-"+tt.param, map[string]string{
				"LaunchTemplateData.ImageId":                        "ami-0sg000000000002",
				"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
				"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
				tt.param: other.sgID,
			})
			status, code := ec2ErrorCode(t, ts, map[string]string{
				"Action":                          "RunInstances",
				"MinCount":                        "1",
				"MaxCount":                        "1",
				"LaunchTemplate.LaunchTemplateId": crossVPC,
			})
			if status != http.StatusBadRequest || code != "InvalidGroup.NotFound" {
				t.Errorf("cross-VPC group: status/code = %d/%q, want 400/InvalidGroup.NotFound — "+
					"the template's group list was not read", status, code)
			}
		})
	}
}

// TestEC2_LaunchTemplate_AssociatePublicIPAddress is why the stored preference is
// a string rather than a bool: absent, "true" and "false" are three distinct
// observations, and only a non-default subnet without MapPublicIPOnLaunch can
// tell them apart.
func TestEC2_LaunchTemplate_AssociatePublicIPAddress(t *testing.T) {
	tests := []struct {
		name          string
		preference    string
		wantPublicIP  bool
		wantPublicWhy string
	}{
		{
			name:          "true forces a public IP on a non-default subnet",
			preference:    "true",
			wantPublicIP:  true,
			wantPublicWhy: "the template requested one",
		},
		{
			name:          "absent uses the subnet default",
			preference:    "",
			wantPublicIP:  false,
			wantPublicWhy: "the subnet is non-default and does not map public IPs",
		},
		{
			name:          "false suppresses one",
			preference:    "false",
			wantPublicIP:  false,
			wantPublicWhy: "the template declined one",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			net := newLTNetwork(t, ts, "10.9.0.0/16", "lt-public-ip")
			data := map[string]string{
				"LaunchTemplateData.ImageId":                        "ami-0pub00000000001",
				"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
				"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
			}
			if tt.preference != "" {
				data["LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress"] = tt.preference
			}
			ltID := createLaunchTemplate(t, ts, "public-ip", data)

			id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
			got := describeInstance(t, ts, id)
			if hasPublicIP := got.PublicIPAddress != ""; hasPublicIP != tt.wantPublicIP {
				t.Errorf("publicIpAddress = %q, want present = %v (%s)",
					got.PublicIPAddress, tt.wantPublicIP, tt.wantPublicWhy)
			}
		})
	}
}

// TestEC2_LaunchTemplate_NoNetworkInterface pins that a template without a
// network interface still falls through to the auto-created default VPC, which is
// how every launch-template test that predates #444 worked.
func TestEC2_LaunchTemplate_NoNetworkInterface(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := createLaunchTemplate(t, ts, "no-ni", map[string]string{
		"LaunchTemplateData.ImageId":      "ami-0noni0000000001",
		"LaunchTemplateData.InstanceType": "t3.small",
	})

	id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
	got := describeInstance(t, ts, id)
	if got.SubnetID == "" {
		t.Error("subnetId is empty; want the auto-created default subnet")
	}
	if got.VpcID == "" {
		t.Error("vpcId is empty; want the auto-created default VPC")
	}
	// A default subnet assigns a public IP, which is what distinguishes this
	// fall-through from the non-default subnets the tests above use.
	if got.PublicIPAddress == "" {
		t.Error("publicIpAddress is empty; the default subnet should assign one")
	}
}

// TestEC2_LaunchTemplate_DescribeEchoesNetworking pins that the stored template
// round-trips: DescribeLaunchTemplates is how a consumer confirms what it
// created, so a template whose networking is parsed but never persisted would
// still read back wrong.
func TestEC2_LaunchTemplate_DescribeEchoesNetworking(t *testing.T) {
	ts := newEC2TestServer(t)
	net := newLTNetwork(t, ts, "10.10.0.0/16", "lt-echo")
	ltID := createLaunchTemplate(t, ts, "echo", map[string]string{
		"LaunchTemplateData.ImageId":                                     "ami-0echo0000000001",
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":              "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":                 net.subnetID,
		"LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1":        net.sgID,
		"LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress": "true",
	})

	// Launching twice from the same template must be stable — the second launch
	// reads the template back out of state rather than from the create request.
	for i := range 2 {
		id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
		got := describeInstance(t, ts, id)
		if got.SubnetID != net.subnetID {
			t.Errorf("launch %d: subnetId = %q, want %q", i+1, got.SubnetID, net.subnetID)
		}
		if got.PublicIPAddress == "" {
			t.Errorf("launch %d: no public IP, want one from the template", i+1)
		}
	}
}
