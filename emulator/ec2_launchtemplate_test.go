package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	ImageID         string `xml:"imageId"`
	InstanceType    string `xml:"instanceType"`
	KeyName         string `xml:"keyName"`
	SubnetID        string `xml:"subnetId"`
	VpcID           string `xml:"vpcId"`
	PublicIPAddress string `xml:"publicIpAddress"`
	Groups          []struct {
		GroupID   string `xml:"groupId"`
		GroupName string `xml:"groupName"`
	} `xml:"groupSet>item"`
}

// groupIDs flattens the instance's security group IDs.
func (l launchedInstance) groupIDs() []string {
	ids := make([]string, 0, len(l.Groups))
	for _, g := range l.Groups {
		ids = append(ids, g.GroupID)
	}
	return ids
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
			"LaunchTemplateData.ImageId":                        ec2TestImage,
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
			"LaunchTemplateData.ImageId": ec2TestImageArm,
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
			"LaunchTemplateData.ImageId": ec2TestImageMinimal,
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
			"LaunchTemplateData.ImageId":                        ec2TestImageMinimalArm,
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
		"LaunchTemplateData.ImageId":                        ec2TestImage,
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
// Each spelling is asserted twice over: the group lands in the launched
// instance's groupSet, and a group from a different VPC than the template's
// subnet is rejected with InvalidGroup.NotFound. The rejection matters
// independently — it can only happen if the list reached validation, so it holds
// even if the groupSet emission were to regress.
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
				"LaunchTemplateData.ImageId":                        ec2TestImage,
				"LaunchTemplateData.NetworkInterface.1.DeviceIndex": "0",
				"LaunchTemplateData.NetworkInterface.1.SubnetId":    net.subnetID,
				tt.param: net.sgID,
			})
			id := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": sameVPC})
			got := describeInstance(t, ts, id)
			if got.SubnetID != net.subnetID {
				t.Errorf("subnetId = %q, want %q", got.SubnetID, net.subnetID)
			}
			if len(got.Groups) != 1 || got.Groups[0].GroupID != net.sgID {
				t.Errorf("groupSet = %+v, want the template's group %s", got.Groups, net.sgID)
			}

			// A group from a different VPC than the template's subnet must be
			// rejected — which proves the list was read rather than ignored.
			crossVPC := createLaunchTemplate(t, ts, "sg-cross-"+tt.param, map[string]string{
				"LaunchTemplateData.ImageId":                        ec2TestImageArm,
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
				"LaunchTemplateData.ImageId":                        ec2TestImage,
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
		"LaunchTemplateData.ImageId":      ec2TestImage,
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

// TestEC2_LaunchTemplate_MergesWithRequestParams is #453's headline case: a
// request that names both an ImageId and a launch template.
//
// runInstances gated its entire template block on imageID == "", so such a
// request never read the template at all — its InstanceType, KeyName, subnet,
// security groups and public-IP preference were silently dropped and the instance
// landed in a substrate-chosen default VPC on a default instance type. The launch
// succeeded, so nothing failed; the instance simply was not the one asked for.
//
// AWS's RunInstances reference states the rule this asserts: "Any additional
// parameters that you specify for the new instance overwrite the corresponding
// parameters included in the launch template." So the request's AMI wins *and*
// every field it did not name comes from the template — one launch, all of it.
func TestEC2_LaunchTemplate_MergesWithRequestParams(t *testing.T) {
	ts := newEC2TestServer(t)
	net := newLTNetwork(t, ts, "10.20.0.0/16", "lt-merge")
	ltID := createLaunchTemplate(t, ts, "merge", map[string]string{
		"LaunchTemplateData.ImageId":                                     ec2TestImage,
		"LaunchTemplateData.InstanceType":                                "c5.xlarge",
		"LaunchTemplateData.KeyName":                                     "template-key",
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":              "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":                 net.subnetID,
		"LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1":        net.sgID,
		"LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress": "true",
	})

	id := runInstance(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateId": ltID,
		// The only field the request names. Before #453 its presence alone was
		// enough to discard everything else above.
		"ImageId": ec2TestImageArm,
	})
	got := describeInstance(t, ts, id)

	if got.ImageID != ec2TestImageArm {
		t.Errorf("imageId = %q, want the request's AMI", got.ImageID)
	}
	if got.InstanceType != "c5.xlarge" {
		t.Errorf("instanceType = %q, want the template's c5.xlarge", got.InstanceType)
	}
	if got.KeyName != "template-key" {
		t.Errorf("keyName = %q, want the template's template-key", got.KeyName)
	}
	if got.SubnetID != net.subnetID {
		t.Errorf("subnetId = %q, want the template's %q", got.SubnetID, net.subnetID)
	}
	if ids := got.groupIDs(); len(ids) != 1 || ids[0] != net.sgID {
		t.Errorf("groupSet = %v, want the template's group %s", ids, net.sgID)
	}
	// The subnet is non-default and does not map public IPs, so a public IP can
	// only have come from the template's preference.
	if got.PublicIPAddress == "" {
		t.Error("publicIpAddress is empty; want one from the template's preference")
	}
}

// TestEC2_LaunchTemplate_FieldPrecedence walks each mergeable field three ways:
// the request wins when it names a value, the template fills the gap when it does
// not, and the built-in default applies when neither does.
//
// The InstanceType rows are the ones that matter most. The template fallback used
// to test instanceType == "t3.micro" as a proxy for "the request named nothing",
// because the default was applied before the template was read — so a request
// explicitly asking for t3.micro was indistinguishable from one asking for
// nothing, and the template overrode it. That inverts AWS's precedence on the one
// value most likely to be spelled out in a cost-sensitive launch.
func TestEC2_LaunchTemplate_FieldPrecedence(t *testing.T) {
	tests := []struct {
		name string
		// templateData is merged into the launch template.
		templateData map[string]string
		// requestParams is merged into the RunInstances call.
		requestParams map[string]string
		check         func(t *testing.T, got launchedInstance)
	}{
		{
			name:          "request InstanceType wins over the template's",
			templateData:  map[string]string{"LaunchTemplateData.InstanceType": "c5.xlarge"},
			requestParams: map[string]string{"InstanceType": "m5.large"},
			check: func(t *testing.T, got launchedInstance) {
				if got.InstanceType != "m5.large" {
					t.Errorf("instanceType = %q, want m5.large", got.InstanceType)
				}
			},
		},
		{
			// The sentinel bug, stated directly: t3.micro is also the default, so
			// an implementation that compares against the default cannot tell this
			// request from one that named no type at all.
			name:          "an explicit t3.micro wins over the template's type",
			templateData:  map[string]string{"LaunchTemplateData.InstanceType": "m5.large"},
			requestParams: map[string]string{"InstanceType": "t3.micro"},
			check: func(t *testing.T, got launchedInstance) {
				if got.InstanceType != "t3.micro" {
					t.Errorf("instanceType = %q, want the explicitly requested t3.micro", got.InstanceType)
				}
			},
		},
		{
			name:          "the template fills an absent InstanceType",
			templateData:  map[string]string{"LaunchTemplateData.InstanceType": "m5.large"},
			requestParams: map[string]string{},
			check: func(t *testing.T, got launchedInstance) {
				if got.InstanceType != "m5.large" {
					t.Errorf("instanceType = %q, want the template's m5.large", got.InstanceType)
				}
			},
		},
		{
			name:          "neither names an InstanceType, so the default applies",
			templateData:  map[string]string{},
			requestParams: map[string]string{},
			check: func(t *testing.T, got launchedInstance) {
				if got.InstanceType != "t3.micro" {
					t.Errorf("instanceType = %q, want the t3.micro default", got.InstanceType)
				}
			},
		},
		{
			name:          "request KeyName wins over the template's",
			templateData:  map[string]string{"LaunchTemplateData.KeyName": "template-key"},
			requestParams: map[string]string{"KeyName": "request-key"},
			check: func(t *testing.T, got launchedInstance) {
				if got.KeyName != "request-key" {
					t.Errorf("keyName = %q, want request-key", got.KeyName)
				}
			},
		},
		{
			name:          "the template fills an absent KeyName",
			templateData:  map[string]string{"LaunchTemplateData.KeyName": "template-key"},
			requestParams: map[string]string{},
			check: func(t *testing.T, got launchedInstance) {
				if got.KeyName != "template-key" {
					t.Errorf("keyName = %q, want the template's template-key", got.KeyName)
				}
			},
		},
		{
			name:          "the template fills an absent ImageId",
			templateData:  map[string]string{"LaunchTemplateData.ImageId": ec2TestImageArm},
			requestParams: map[string]string{},
			check: func(t *testing.T, got launchedInstance) {
				if got.ImageID != ec2TestImageArm {
					t.Errorf("imageId = %q, want the template's", got.ImageID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)

			// Every template carries an AMI so the #412 check is satisfied even in
			// the cases whose request names one; a case asserting on the AMI
			// overrides it explicitly.
			data := map[string]string{"LaunchTemplateData.ImageId": ec2TestImage}
			for k, v := range tt.templateData {
				data[k] = v
			}
			ltID := createLaunchTemplate(t, ts, "precedence", data)

			params := map[string]string{"LaunchTemplate.LaunchTemplateId": ltID}
			for k, v := range tt.requestParams {
				params[k] = v
			}
			tt.check(t, describeInstance(t, ts, runInstance(t, ts, params)))
		})
	}
}

// TestEC2_LaunchTemplate_NetworkingPrecedenceWithRequestImageID is the #444
// networking precedence re-asserted for the case #453 opened up.
//
// Those fallbacks were previously unreachable whenever the request named an
// ImageId, so their guards had never actually been exercised against a
// call-level value. This runs both directions with an ImageId present: a
// request-level subnet and security group win, and the template's public-IP
// preference still fills a gap the request left.
func TestEC2_LaunchTemplate_NetworkingPrecedenceWithRequestImageID(t *testing.T) {
	ts := newEC2TestServer(t)
	tmplNet := newLTNetwork(t, ts, "10.30.0.0/16", "lt-net-tmpl")
	reqNet := newLTNetwork(t, ts, "10.31.0.0/16", "lt-net-req")

	ltID := createLaunchTemplate(t, ts, "net-precedence", map[string]string{
		"LaunchTemplateData.ImageId":                                     ec2TestImage,
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":              "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":                 tmplNet.subnetID,
		"LaunchTemplateData.NetworkInterface.1.SecurityGroupId.1":        tmplNet.sgID,
		"LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress": "true",
	})

	got := describeInstance(t, ts, runInstance(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateId": ltID,
		"ImageId":                         ec2TestImageArm,
		"SubnetId":                        reqNet.subnetID,
		"SecurityGroupId.1":               reqNet.sgID,
	}))

	if got.SubnetID != reqNet.subnetID {
		t.Errorf("subnetId = %q, want the request's %q", got.SubnetID, reqNet.subnetID)
	}
	if ids := got.groupIDs(); len(ids) != 1 || ids[0] != reqNet.sgID {
		t.Errorf("groupSet = %v, want only the request's group %s", ids, reqNet.sgID)
	}
	// The request named no public-IP preference, so the template's still applies —
	// on a non-default subnet, which would otherwise assign none.
	if got.PublicIPAddress == "" {
		t.Error("publicIpAddress is empty; the template's preference should still fill the gap")
	}
}

// TestEC2_LaunchTemplate_RequestFalseBeatsTemplateTrue pins the direction of the
// public-IP preference that a bool cannot express.
//
// AssociatePublicIpAddress is carried as a three-state string ("" / "true" /
// "false") precisely so an explicit false can override a template's true. With
// the template now read even when the request names an ImageId, this is the case
// where collapsing the states would visibly assign an unwanted public IP.
func TestEC2_LaunchTemplate_RequestFalseBeatsTemplateTrue(t *testing.T) {
	ts := newEC2TestServer(t)
	net := newLTNetwork(t, ts, "10.40.0.0/16", "lt-public-false")
	ltID := createLaunchTemplate(t, ts, "public-false", map[string]string{
		"LaunchTemplateData.ImageId":                                     ec2TestImage,
		"LaunchTemplateData.NetworkInterface.1.DeviceIndex":              "0",
		"LaunchTemplateData.NetworkInterface.1.SubnetId":                 net.subnetID,
		"LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress": "true",
	})

	got := describeInstance(t, ts, runInstance(t, ts, map[string]string{
		"LaunchTemplate.LaunchTemplateId": ltID,
		"ImageId":                         ec2TestImageArm,
		"NetworkInterface.1.AssociatePublicIpAddress": "false",
		"NetworkInterface.1.SubnetId":                 net.subnetID,
	}))

	if got.PublicIPAddress != "" {
		t.Errorf("publicIpAddress = %q, want none: the request explicitly asked for false",
			got.PublicIPAddress)
	}
}

// ltTagParams returns the LaunchTemplateData.TagSpecification.N params that scope
// tags to instances, in the request's key order.
//
// Note the singular member name: the *request* spells it TagSpecification.N while
// the response spells it tagSpecificationSet. Getting that backwards silently
// produces a template with no tags, which is exactly #471's defect.
func ltTagParams(tags ...[2]string) map[string]string {
	params := map[string]string{
		"LaunchTemplateData.TagSpecification.1.ResourceType": "instance",
	}
	for i, tag := range tags {
		params[fmt.Sprintf("LaunchTemplateData.TagSpecification.1.Tag.%d.Key", i+1)] = tag[0]
		params[fmt.Sprintf("LaunchTemplateData.TagSpecification.1.Tag.%d.Value", i+1)] = tag[1]
	}
	return params
}

// instanceTagMap returns an instance's tags as DescribeInstances reports them.
func instanceTagMap(t *testing.T, ts *httptest.Server, instID string) map[string]string {
	t.Helper()
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instID,
	}, &desc)
	require.Len(t, desc.Instances, 1)
	tags := map[string]string{}
	for _, tag := range desc.Instances[0].Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// instanceIDsMatchingTag returns the instance IDs a DescribeInstances tag: filter
// returns, which is the observation a consumer's IaC actually depends on.
func instanceIDsMatchingTag(t *testing.T, ts *httptest.Server, key, value string) []string {
	t.Helper()
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:" + key,
		"Filter.1.Value.1": value,
	}, &desc)
	ids := make([]string, 0, len(desc.Instances))
	for _, inst := range desc.Instances {
		ids = append(ids, inst.InstanceID)
	}
	return ids
}

// instanceProfileARN returns the iamInstanceProfile ARN DescribeInstances reports.
func instanceProfileARN(t *testing.T, ts *httptest.Server, instID string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instID,
	})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Instances []struct {
			Profile struct {
				ARN string `xml:"arn"`
			} `xml:"iamInstanceProfile"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Instances, 1)
	return out.Instances[0].Profile.ARN
}

// TestEC2_LaunchTemplate_RoundTripsTagsAndProfile covers the read-back half of
// #471: both fields were accepted by CreateLaunchTemplate and stored nowhere, so a
// template that tagged its instances and named a role read back as carrying neither.
//
// DescribeLaunchTemplateVersions is the only operation that can make this
// assertion — DescribeLaunchTemplates' response has no launchTemplateData at all —
// which is why #456 landed in the same release rather than this criterion being
// weakened.
func TestEC2_LaunchTemplate_RoundTripsTagsAndProfile(t *testing.T) {
	tests := []struct {
		name string
		// profileParam is the member the request uses; both are valid and AWS's
		// LaunchTemplateIamInstanceProfileSpecificationRequest has exactly these two.
		profileParam string
		profile      string
		wantARN      string
		wantName     string
	}{
		{
			name:         "Name form",
			profileParam: "LaunchTemplateData.IamInstanceProfile.Name",
			profile:      "tmpl-profile",
			wantName:     "tmpl-profile",
		},
		{
			name:         "Arn form",
			profileParam: "LaunchTemplateData.IamInstanceProfile.Arn",
			profile:      "arn:aws:iam::123456789012:instance-profile/tmpl-profile",
			wantARN:      "arn:aws:iam::123456789012:instance-profile/tmpl-profile",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			data := map[string]string{
				"LaunchTemplateData.ImageId": ec2TestImage,
				tt.profileParam:              tt.profile,
			}
			for k, v := range ltTagParams([2]string{"Env", "prod"}, [2]string{"Team", "core"}) {
				data[k] = v
			}
			ltID := createLaunchTemplate(t, ts, "roundtrip-"+tt.name, data)

			versions, _ := describeLTVersions(t, ts, map[string]string{"LaunchTemplateId": ltID})
			require.Len(t, versions, 1)

			// The profile is echoed in whichever member it arrived in. Synthesizing the
			// other would report the template as naming something the caller never wrote.
			assert.Equal(t, tt.wantARN, versions[0].Data.IamInstanceProfile.ARN)
			assert.Equal(t, tt.wantName, versions[0].Data.IamInstanceProfile.Name)

			require.Len(t, versions[0].Data.TagSpecifications, 1)
			spec := versions[0].Data.TagSpecifications[0]
			assert.Equal(t, "instance", spec.ResourceType)
			require.Len(t, spec.Tags, 2)
			// Request order is preserved, so the assertion is deterministic.
			assert.Equal(t, "Env", spec.Tags[0].Key)
			assert.Equal(t, "prod", spec.Tags[0].Value)
			assert.Equal(t, "Team", spec.Tags[1].Key)
			assert.Equal(t, "core", spec.Tags[1].Value)
		})
	}
}

// TestEC2_LaunchTemplate_TagsReachTheInstance is #471's headline case: an instance
// launched from a tagging template came up untagged, so DescribeInstances reported
// no tags and a tag: filter returned nothing — with no error to explain why.
//
// The filter is asserted as well as the tagSet, because the filter is the half a
// consumer's IaC actually depends on: it is how a stack finds the instances it just
// created, and it can fail even when the tagSet is right.
func TestEC2_LaunchTemplate_TagsReachTheInstance(t *testing.T) {
	ts := newEC2TestServer(t)
	data := map[string]string{"LaunchTemplateData.ImageId": ec2TestImage}
	for k, v := range ltTagParams([2]string{"Env", "prod"}, [2]string{"Team", "core"}) {
		data[k] = v
	}
	ltID := createLaunchTemplate(t, ts, "tags-reach", data)

	instID := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})

	tags := instanceTagMap(t, ts, instID)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "core", tags["Team"])

	assert.Equal(t, []string{instID}, instanceIDsMatchingTag(t, ts, "Env", "prod"),
		"a tag: filter must find an instance tagged through its launch template")
}

// TestEC2_LaunchTemplate_ProfileReachesTheInstance pins the other dropped field.
// A consumer asserting which role its instances carry had an assertion that could
// not fail: the instance reported no profile at all.
//
// DescribeInstances reports the profile as an ARN — AWS's IamInstanceProfile
// response shape has no name member — so a bare template name is surfaced as the
// ARN it implies, matching what a call-level IamInstanceProfile.Name already did.
func TestEC2_LaunchTemplate_ProfileReachesTheInstance(t *testing.T) {
	const arn = "arn:aws:iam::123456789012:instance-profile/tmpl-profile"
	tests := []struct {
		name  string
		param string
		value string
	}{
		{
			name:  "Name is surfaced as the ARN it implies",
			param: "LaunchTemplateData.IamInstanceProfile.Name",
			value: "tmpl-profile",
		},
		{
			name:  "an ARN is surfaced verbatim",
			param: "LaunchTemplateData.IamInstanceProfile.Arn",
			value: arn,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			ltID := createLaunchTemplate(t, ts, "profile-"+tt.param, map[string]string{
				"LaunchTemplateData.ImageId": ec2TestImage,
				tt.param:                     tt.value,
			})
			instID := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
			assert.Equal(t, arn, instanceProfileARN(t, ts, instID))
		})
	}
}

// TestEC2_LaunchTemplate_TagAndProfilePrecedence extends #453's matrix with the two
// fields #471 adds, three ways each: the request wins, the template fills the gap,
// and neither names one.
//
// Before this, the request's value won by *default* rather than by rule — the
// template's was never parsed — which happens to match AWS's precedence for the
// wrong reason. Parsing the fields without writing the fallback would invert it
// silently, which is what this table exists to catch.
//
// The tags row that matters is the first: template tags **replace** rather than
// merge. The reference gives no TagSpecifications-specific merge semantics, only the
// general "overwrite the corresponding parameters" rule, so replacement is that rule
// applied — a request naming Env=req does not also inherit the template's Team=x.
func TestEC2_LaunchTemplate_TagAndProfilePrecedence(t *testing.T) {
	const reqARN = "arn:aws:iam::123456789012:instance-profile/p-req"
	const tmplARN = "arn:aws:iam::123456789012:instance-profile/p-tmpl"

	tests := []struct {
		name          string
		templateData  map[string]string
		requestParams map[string]string
		wantTags      map[string]string
		// wantAbsentTags are keys that must not be present at all, which is how the
		// replace-not-merge rule is asserted.
		wantAbsentTags []string
		wantProfile    string
	}{
		{
			name:         "request tags replace the template's, not merge with them",
			templateData: ltTagParams([2]string{"Env", "tmpl"}, [2]string{"Team", "x"}),
			requestParams: map[string]string{
				"TagSpecification.1.ResourceType": "instance",
				"TagSpecification.1.Tag.1.Key":    "Env",
				"TagSpecification.1.Tag.1.Value":  "req",
			},
			wantTags:       map[string]string{"Env": "req"},
			wantAbsentTags: []string{"Team"},
		},
		{
			name:          "the template fills absent tags",
			templateData:  ltTagParams([2]string{"Env", "tmpl"}),
			requestParams: map[string]string{},
			wantTags:      map[string]string{"Env": "tmpl"},
		},
		{
			name:           "neither names tags",
			templateData:   map[string]string{},
			requestParams:  map[string]string{},
			wantAbsentTags: []string{"Env", "Team"},
		},
		{
			name:          "request IamInstanceProfile wins",
			templateData:  map[string]string{"LaunchTemplateData.IamInstanceProfile.Name": "p-tmpl"},
			requestParams: map[string]string{"IamInstanceProfile.Name": "p-req"},
			wantProfile:   reqARN,
		},
		{
			name:          "the template fills an absent IamInstanceProfile",
			templateData:  map[string]string{"LaunchTemplateData.IamInstanceProfile.Name": "p-tmpl"},
			requestParams: map[string]string{},
			wantProfile:   tmplARN,
		},
		{
			name:          "neither names an IamInstanceProfile",
			templateData:  map[string]string{},
			requestParams: map[string]string{},
			wantProfile:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			data := map[string]string{"LaunchTemplateData.ImageId": ec2TestImage}
			for k, v := range tt.templateData {
				data[k] = v
			}
			ltID := createLaunchTemplate(t, ts, "tag-precedence", data)

			params := map[string]string{"LaunchTemplate.LaunchTemplateId": ltID}
			for k, v := range tt.requestParams {
				params[k] = v
			}
			instID := runInstance(t, ts, params)

			tags := instanceTagMap(t, ts, instID)
			for key, want := range tt.wantTags {
				assert.Equal(t, want, tags[key], "tag %q", key)
			}
			for _, key := range tt.wantAbsentTags {
				assert.NotContains(t, tags, key, "tag %q must not be present", key)
			}
			assert.Equal(t, tt.wantProfile, instanceProfileARN(t, ts, instID))
		})
	}
}

// TestEC2_LaunchTemplate_TagsAreSubjectToTheTagRules is #471's own interaction
// criterion: a template's tags must not become a second, unrestricted path for
// reserved keys or for exceeding the 50-tag limit (#468, #469).
//
// The rejection happens at CreateLaunchTemplate, as it does on real EC2 — both
// rules are on the key and the count, not on the operation.
func TestEC2_LaunchTemplate_TagsAreSubjectToTheTagRules(t *testing.T) {
	t.Run("a reserved key is rejected at template creation", func(t *testing.T) {
		ts := newEC2TestServer(t)
		params := map[string]string{
			"Action":                     "CreateLaunchTemplate",
			"LaunchTemplateName":         "reserved-tag",
			"LaunchTemplateData.ImageId": ec2TestImage,
		}
		for k, v := range ltTagParams([2]string{"Name", "ok"}, [2]string{"aws:foo", "v"}) {
			params[k] = v
		}
		status, code, message := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Equal(t, ec2ReservedTagMessage, message)

		// Nothing is created, so the name is still free.
		_ = createLaunchTemplate(t, ts, "reserved-tag", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImageArm,
		})
	})

	t.Run("SourceVersion inheritance does not outrank the new version's tags", func(t *testing.T) {
		ts := newEC2TestServer(t)
		v1 := map[string]string{"LaunchTemplateData.ImageId": ec2TestImageMinimal}
		for k, v := range ltTagParams([2]string{"Env", "one"}) {
			v1[k] = v
		}
		v1["LaunchTemplateData.IamInstanceProfile.Name"] = "p-one"
		ltID := createLaunchTemplate(t, ts, "overlay", v1)

		v2 := map[string]string{
			"LaunchTemplateId": ltID,
			"SourceVersion":    "1",
			"LaunchTemplateData.IamInstanceProfile.Name": "p-two",
		}
		for k, v := range ltTagParams([2]string{"Env", "two"}) {
			v2[k] = v
		}
		got := createLTVersion(t, ts, v2)

		// The overlay direction is what this asserts: the request overwrites the
		// inherited value rather than the inherited value winning. Both fields are
		// separate overlay arms, so each can be dropped independently.
		require.Len(t, got.Data.TagSpecifications, 1)
		require.Len(t, got.Data.TagSpecifications[0].Tags, 1)
		assert.Equal(t, "two", got.Data.TagSpecifications[0].Tags[0].Value)
		assert.Equal(t, "p-two", got.Data.IamInstanceProfile.Name)

		// And the launch takes the new version's values, not version 1's.
		instID := runInstance(t, ts, map[string]string{
			"LaunchTemplate.LaunchTemplateId": ltID,
			"LaunchTemplate.Version":          "2",
		})
		assert.Equal(t, "two", instanceTagMap(t, ts, instID)["Env"])
		assert.Contains(t, instanceProfileARN(t, ts, instID), "instance-profile/p-two")
	})

	t.Run("a new version carrying a reserved key is rejected", func(t *testing.T) {
		ts := newEC2TestServer(t)
		ltID := createLaunchTemplate(t, ts, "reserved-version", map[string]string{
			"LaunchTemplateData.ImageId": ec2TestImageMinimalArm,
		})
		params := map[string]string{
			"Action":                     "CreateLaunchTemplateVersion",
			"LaunchTemplateId":           ltID,
			"LaunchTemplateData.ImageId": ec2TestImageWindows,
		}
		for k, v := range ltTagParams([2]string{"aws:foo", "v"}) {
			params[k] = v
		}
		status, code, _ := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
		assert.Equal(t, int64(1), describeLT(t, ts, ltID).LatestVersionNum,
			"the rejected version must not have been appended")
	})

	t.Run("more than the tag limit is rejected at template creation", func(t *testing.T) {
		ts := newEC2TestServer(t)
		tags := make([][2]string, 0, ec2TagLimit+1)
		for i := 1; i <= ec2TagLimit+1; i++ {
			tags = append(tags, [2]string{fmt.Sprintf("key%d", i), "v"})
		}
		params := map[string]string{
			"Action":                     "CreateLaunchTemplate",
			"LaunchTemplateName":         "too-many-tags",
			"LaunchTemplateData.ImageId": ec2TestImageECS,
		}
		for k, v := range ltTagParams(tags...) {
			params[k] = v
		}
		status, code, message := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "TagLimitExceeded", code)
		assert.Equal(t, ec2TagLimitMessage, message)
	})

	t.Run("exactly the tag limit launches", func(t *testing.T) {
		ts := newEC2TestServer(t)
		tags := make([][2]string, 0, ec2TagLimit)
		for i := 1; i <= ec2TagLimit; i++ {
			tags = append(tags, [2]string{fmt.Sprintf("key%d", i), "v"})
		}
		data := map[string]string{"LaunchTemplateData.ImageId": ec2TestImageECS2}
		for k, v := range ltTagParams(tags...) {
			data[k] = v
		}
		ltID := createLaunchTemplate(t, ts, "at-limit", data)
		instID := runInstance(t, ts, map[string]string{"LaunchTemplate.LaunchTemplateId": ltID})
		assert.Len(t, instanceTagMap(t, ts, instID), ec2TagLimit)
	})
}

// storeTemplateWithTags writes a launch template straight into state with the given
// instance-scoped tags and returns a server reading it.
//
// Written by hand rather than created through the API, because CreateLaunchTemplate
// can no longer produce these templates — that is exactly the point. A template
// stored before the tag checks existed, or one arriving from a replayed event log,
// can still carry a violation, and the launch is where it would otherwise be applied.
func storeTemplateWithTags(t *testing.T, name, ltID string, tags []map[string]string) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.EC2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	ts := httptest.NewServer(emulator.NewServer(*emulator.DefaultConfig(), registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	const acct, region = "123456789012", "us-east-1"
	raw, err := json.Marshal(map[string]any{
		"launchTemplateId":     ltID,
		"launchTemplateName":   name,
		"defaultVersionNumber": 1,
		"latestVersionNumber":  1,
		"createdBy":            acct,
		"createTime":           "2026-01-01T00:00:00Z",
		"latestData": map[string]any{
			"imageId":           ec2TestImage,
			"tagSpecifications": tags,
		},
		"accountID": acct,
		"region":    region,
	})
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, state.Put(ctx, "ec2", "lt:"+acct+"/"+region+"/"+ltID, raw))
	require.NoError(t, state.Put(ctx, "ec2", "lt_by_name:"+acct+"/"+region+"/"+name, []byte(ltID)))
	return ts
}

// TestEC2_LaunchTemplate_StoredTagsAreCheckedAtLaunch is the other half of the
// interaction. The creation-time checks cannot see a template that predates them, so
// the launch checks again — both rules, not just the reserved-key one.
func TestEC2_LaunchTemplate_StoredTagsAreCheckedAtLaunch(t *testing.T) {
	tests := []struct {
		name        string
		ltID        string
		tags        []map[string]string
		wantCode    string
		wantMessage string
	}{
		{
			name:        "a reserved key",
			ltID:        "lt-0stored000000001",
			tags:        []map[string]string{{"key": "aws:foo", "value": "v"}},
			wantCode:    "InvalidParameterValue",
			wantMessage: ec2ReservedTagMessage,
		},
		{
			name:        "more than the tag limit",
			ltID:        "lt-0stored000000002",
			tags:        storedNumberedTags(ec2TagLimit + 1),
			wantCode:    "TagLimitExceeded",
			wantMessage: ec2TagLimitMessage,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := storeTemplateWithTags(t, "stored-"+tt.wantCode, tt.ltID, tt.tags)
			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action":                          "RunInstances",
				"MinCount":                        "1",
				"MaxCount":                        "1",
				"LaunchTemplate.LaunchTemplateId": tt.ltID,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMessage, message)
		})
	}
}

// storedNumberedTags builds count distinct user tags in the stored JSON's shape.
func storedNumberedTags(count int) []map[string]string {
	tags := make([]map[string]string, 0, count)
	for i := 1; i <= count; i++ {
		tags = append(tags, map[string]string{"key": fmt.Sprintf("key%d", i), "value": "v"})
	}
	return tags
}

// TestEC2_Fleet_TemplateTagsReachFleetInstances pins that a fleet launched from a
// tagging template gets the template's tags *and* substrate's fleet stamp.
//
// This is why the fallback tests for a non-reserved tag rather than for an empty
// slice: by the time a fleet launch reaches the merge, aws:ec2:fleet-id has already
// been appended, so a plain len(tags) == 0 would read the stamp as "the caller named
// tags" and drop the template's — losing exactly the tags #471 exists to deliver, on
// exactly the path #443 added.
func TestEC2_Fleet_TemplateTagsReachFleetInstances(t *testing.T) {
	ts := newEC2TestServer(t)
	data := map[string]string{"LaunchTemplateData.ImageId": ec2TestImage}
	for k, v := range ltTagParams([2]string{"Env", "prod"}) {
		data[k] = v
	}
	ltID := createLaunchTemplate(t, ts, "fleet-tags", data)

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &fleet)

	ids := fleet.instanceIDs()
	require.Len(t, ids, 1)
	tags := instanceTagMap(t, ts, ids[0])
	assert.Equal(t, "prod", tags["Env"], "the template's tag must survive the fleet stamp")
	assert.Equal(t, fleet.FleetID, tags[fleetIDTagKey], "the fleet stamp must survive the template's tags")
}

// TestEC2_LaunchTemplate_DescribeEchoesNetworking pins that the stored template
// round-trips: DescribeLaunchTemplates is how a consumer confirms what it
// created, so a template whose networking is parsed but never persisted would
// still read back wrong.
func TestEC2_LaunchTemplate_DescribeEchoesNetworking(t *testing.T) {
	ts := newEC2TestServer(t)
	net := newLTNetwork(t, ts, "10.10.0.0/16", "lt-echo")
	ltID := createLaunchTemplate(t, ts, "echo", map[string]string{
		"LaunchTemplateData.ImageId":                                     ec2TestImage,
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
		if len(got.Groups) != 1 || got.Groups[0].GroupID != net.sgID {
			t.Errorf("launch %d: groupSet = %+v, want the template's group %s", i+1, got.Groups, net.sgID)
		}
	}
}
