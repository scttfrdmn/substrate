package substrate_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestEC2_RunInstances_NetworkInterface verifies that RunInstances reads the
// subnet, security group, and AssociatePublicIpAddress from a nested
// NetworkInterface.1.* spec (the form the AWS SDK uses when a public-IP
// preference is set) — not just the top-level params. Tools like spawn always
// nest these, so without this the caller's networking is silently dropped.
func TestEC2_RunInstances_NetworkInterface(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create a non-default VPC + subnet + SG so we can prove the nested fields
	// are honored (a default subnet would assign a public IP regardless).
	vpcID := createVPC(t, ts, "10.1.0.0/16")
	subnetID := createSubnet(t, ts, vpcID, "10.1.1.0/24")
	sgID := createSecurityGroup(t, ts, vpcID, "ni-sg")

	resp := ec2Request(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "t3.small",
		"MinCount":     "1",
		"MaxCount":     "1",
		// Networking specified ONLY in the nested NetworkInterface form.
		"NetworkInterface.1.DeviceIndex":              "0",
		"NetworkInterface.1.SubnetId":                 subnetID,
		"NetworkInterface.1.SecurityGroupId.1":        sgID,
		"NetworkInterface.1.AssociatePublicIpAddress": "true",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", resp.StatusCode)
	}
	var run struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&run); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	resp.Body.Close() //nolint:errcheck
	if len(run.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(run.Instances))
	}
	id := run.Instances[0].InstanceID

	// Describe and confirm the nested subnet was used AND a public IP was
	// assigned because AssociatePublicIpAddress=true (the subnet is non-default).
	d := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
	var desc struct {
		Reservations []struct {
			Instances []struct {
				SubnetID        string `xml:"subnetId"`
				PublicIPAddress string `xml:"publicIpAddress"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(d.Body).Decode(&desc); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	d.Body.Close() //nolint:errcheck

	if len(desc.Reservations) != 1 || len(desc.Reservations[0].Instances) != 1 {
		t.Fatalf("expected 1 reservation/instance, got %+v", desc)
	}
	got := desc.Reservations[0].Instances[0]
	if got.SubnetID != subnetID {
		t.Errorf("subnet from NetworkInterface.1.SubnetId not honored: got %q want %q", got.SubnetID, subnetID)
	}
	if got.PublicIPAddress == "" {
		t.Errorf("AssociatePublicIpAddress=true did not assign a public IP on a non-default subnet")
	}
}

// --- small helpers (EC2 query protocol) ---

func createVPC(t *testing.T, ts *httptest.Server, cidr string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": cidr})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		VPC struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	if out.VPC.VPCID == "" {
		t.Fatal("CreateVpc returned empty vpcId")
	}
	return out.VPC.VPCID
}

func createSubnet(t *testing.T, ts *httptest.Server, vpcID, cidr string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": cidr,
	})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode CreateSubnet: %v", err)
	}
	if out.Subnet.SubnetID == "" {
		t.Fatal("CreateSubnet returned empty subnetId")
	}
	return out.Subnet.SubnetID
}

func createSecurityGroup(t *testing.T, ts *httptest.Server, vpcID, name string) string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "VpcId": vpcID, "GroupName": name, "GroupDescription": name,
	})
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		GroupID string `xml:"groupId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode CreateSecurityGroup: %v", err)
	}
	if out.GroupID == "" {
		t.Fatal("CreateSecurityGroup returned empty groupId")
	}
	return out.GroupID
}
