package substrate_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
	"github.com/stretchr/testify/assert"
)

// newEC2TestServer builds a minimal Server with the EC2 plugin registered.
func newEC2TestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.EC2Plugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize ec2 plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// ec2Request sends an EC2 query-protocol request to ts and returns the response.
func ec2Request(t *testing.T, ts *httptest.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestEC2_RunDescribeTerminate(t *testing.T) {
	ts := newEC2TestServer(t)

	// RunInstances.
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", resp.StatusCode)
	}
	defer resp.Body.Close() //nolint:errcheck

	var runResult struct {
		XMLName   xml.Name `xml:"RunInstancesResponse"`
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances response: %v", err)
	}
	if len(runResult.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(runResult.Instances))
	}
	instanceID := runResult.Instances[0].InstanceID
	if !strings.HasPrefix(instanceID, "i-") {
		t.Errorf("expected instance ID starting with i-, got %q", instanceID)
	}

	// DescribeInstances.
	resp2 := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instanceID,
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances: expected 200, got %d", resp2.StatusCode)
	}
	resp2.Body.Close() //nolint:errcheck

	// TerminateInstances.
	resp3 := ec2Request(t, ts, map[string]string{
		"Action":       "TerminateInstances",
		"InstanceId.1": instanceID,
	})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("TerminateInstances: expected 200, got %d", resp3.StatusCode)
	}
	resp3.Body.Close() //nolint:errcheck
}

func TestEC2_StopStartInstances(t *testing.T) {
	ts := newEC2TestServer(t)

	// Launch instance.
	resp := ec2Request(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-00000001",
		"MinCount": "1",
		"MaxCount": "1",
	})
	defer resp.Body.Close() //nolint:errcheck
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	_ = xml.NewDecoder(resp.Body).Decode(&runResult)
	id := runResult.Instances[0].InstanceID

	// Stop.
	stopResp := ec2Request(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": id})
	if stopResp.StatusCode != http.StatusOK {
		t.Fatalf("StopInstances: expected 200, got %d", stopResp.StatusCode)
	}
	stopResp.Body.Close() //nolint:errcheck

	// Start.
	startResp := ec2Request(t, ts, map[string]string{"Action": "StartInstances", "InstanceId.1": id})
	if startResp.StatusCode != http.StatusOK {
		t.Fatalf("StartInstances: expected 200, got %d", startResp.StatusCode)
	}
	startResp.Body.Close() //nolint:errcheck
}

func TestEC2_VPC_CreateDescribeDelete(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC.
	resp := ec2Request(t, ts, map[string]string{
		"Action":    "CreateVpc",
		"CidrBlock": "10.0.0.0/16",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateVpc: expected 200, got %d", resp.StatusCode)
	}
	var createResult struct {
		Vpc struct {
			VpcID     string `xml:"vpcId"`
			CIDRBlock string `xml:"cidrBlock"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close() //nolint:errcheck
	vpcID := createResult.Vpc.VpcID
	if !strings.HasPrefix(vpcID, "vpc-") {
		t.Errorf("expected VPC ID starting with vpc-, got %q", vpcID)
	}

	// Describe.
	resp2 := ec2Request(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("DescribeVpcs: expected 200, got %d", resp2.StatusCode)
	}
	resp2.Body.Close() //nolint:errcheck

	// Delete.
	resp3 := ec2Request(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DeleteVpc: expected 200, got %d", resp3.StatusCode)
	}
	resp3.Body.Close() //nolint:errcheck
}

func TestEC2_Subnet_CreateDescribeDelete(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC first.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.1.0.0/16"})
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcResp.Body.Close() //nolint:errcheck
	vpcID := vpcResult.Vpc.VpcID

	// Create subnet.
	resp := ec2Request(t, ts, map[string]string{
		"Action":    "CreateSubnet",
		"VpcId":     vpcID,
		"CidrBlock": "10.1.1.0/24",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateSubnet: expected 200, got %d", resp.StatusCode)
	}
	var subnetResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(resp.Body).Decode(&subnetResult)
	resp.Body.Close() //nolint:errcheck
	subnetID := subnetResult.Subnet.SubnetID
	if !strings.HasPrefix(subnetID, "subnet-") {
		t.Errorf("expected subnet ID starting with subnet-, got %q", subnetID)
	}

	// Describe.
	resp2 := ec2Request(t, ts, map[string]string{"Action": "DescribeSubnets", "SubnetId.1": subnetID})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("DescribeSubnets: expected 200, got %d", resp2.StatusCode)
	}
	resp2.Body.Close() //nolint:errcheck

	// Delete.
	resp3 := ec2Request(t, ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": subnetID})
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DeleteSubnet: expected 200, got %d", resp3.StatusCode)
	}
	resp3.Body.Close() //nolint:errcheck
}

func TestEC2_SecurityGroup_AuthorizeRevoke(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC and security group.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.2.0.0/16"})
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcResp.Body.Close() //nolint:errcheck

	sgResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateSecurityGroup",
		"GroupName":        "test-sg",
		"GroupDescription": "test security group",
		"VpcId":            vpcResult.Vpc.VpcID,
	})
	if sgResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateSecurityGroup: expected 200, got %d", sgResp.StatusCode)
	}
	var sgResult struct {
		GroupID string `xml:"groupId"`
	}
	_ = xml.NewDecoder(sgResp.Body).Decode(&sgResult)
	sgResp.Body.Close() //nolint:errcheck
	sgID := sgResult.GroupID
	if !strings.HasPrefix(sgID, "sg-") {
		t.Errorf("expected sg ID starting with sg-, got %q", sgID)
	}

	// Authorize ingress.
	authResp := ec2Request(t, ts, map[string]string{
		"Action":                            "AuthorizeSecurityGroupIngress",
		"GroupId":                           sgID,
		"IpPermissions.1.IpProtocol":        "tcp",
		"IpPermissions.1.FromPort":          "80",
		"IpPermissions.1.ToPort":            "80",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	if authResp.StatusCode != http.StatusOK {
		t.Fatalf("AuthorizeSecurityGroupIngress: expected 200, got %d", authResp.StatusCode)
	}
	authResp.Body.Close() //nolint:errcheck

	// Revoke ingress.
	revokeResp := ec2Request(t, ts, map[string]string{
		"Action":                            "RevokeSecurityGroupIngress",
		"GroupId":                           sgID,
		"IpPermissions.1.IpProtocol":        "tcp",
		"IpPermissions.1.FromPort":          "80",
		"IpPermissions.1.ToPort":            "80",
		"IpPermissions.1.IpRanges.1.CidrIp": "0.0.0.0/0",
	})
	if revokeResp.StatusCode != http.StatusOK {
		t.Fatalf("RevokeSecurityGroupIngress: expected 200, got %d", revokeResp.StatusCode)
	}
	revokeResp.Body.Close() //nolint:errcheck
}

func TestEC2_InternetGateway_AttachDetach(t *testing.T) {
	ts := newEC2TestServer(t)

	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.3.0.0/16"})
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcResp.Body.Close() //nolint:errcheck
	vpcID := vpcResult.Vpc.VpcID

	igwResp := ec2Request(t, ts, map[string]string{"Action": "CreateInternetGateway"})
	if igwResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateInternetGateway: expected 200, got %d", igwResp.StatusCode)
	}
	var igwResult struct {
		IGW struct {
			InternetGatewayID string `xml:"internetGatewayId"`
		} `xml:"internetGateway"`
	}
	_ = xml.NewDecoder(igwResp.Body).Decode(&igwResult)
	igwResp.Body.Close() //nolint:errcheck
	igwID := igwResult.IGW.InternetGatewayID
	if !strings.HasPrefix(igwID, "igw-") {
		t.Errorf("expected IGW ID starting with igw-, got %q", igwID)
	}

	// Attach.
	attachResp := ec2Request(t, ts, map[string]string{
		"Action":            "AttachInternetGateway",
		"InternetGatewayId": igwID,
		"VpcId":             vpcID,
	})
	if attachResp.StatusCode != http.StatusOK {
		t.Fatalf("AttachInternetGateway: expected 200, got %d", attachResp.StatusCode)
	}
	attachResp.Body.Close() //nolint:errcheck

	// Detach.
	detachResp := ec2Request(t, ts, map[string]string{
		"Action":            "DetachInternetGateway",
		"InternetGatewayId": igwID,
		"VpcId":             vpcID,
	})
	if detachResp.StatusCode != http.StatusOK {
		t.Fatalf("DetachInternetGateway: expected 200, got %d", detachResp.StatusCode)
	}
	detachResp.Body.Close() //nolint:errcheck
}

func TestEC2_RouteTable_AssociateRoute(t *testing.T) {
	ts := newEC2TestServer(t)

	// Setup VPC + subnet + IGW.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.4.0.0/16"})
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcResp.Body.Close() //nolint:errcheck
	vpcID := vpcResult.Vpc.VpcID

	subnetResp := ec2Request(t, ts, map[string]string{
		"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": "10.4.1.0/24",
	})
	var subnetResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(subnetResp.Body).Decode(&subnetResult)
	subnetResp.Body.Close() //nolint:errcheck
	subnetID := subnetResult.Subnet.SubnetID

	igwResp := ec2Request(t, ts, map[string]string{"Action": "CreateInternetGateway"})
	var igwResult struct {
		IGW struct {
			InternetGatewayID string `xml:"internetGatewayId"`
		} `xml:"internetGateway"`
	}
	_ = xml.NewDecoder(igwResp.Body).Decode(&igwResult)
	igwResp.Body.Close() //nolint:errcheck
	igwID := igwResult.IGW.InternetGatewayID

	// Create route table.
	rtbResp := ec2Request(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID})
	if rtbResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateRouteTable: expected 200, got %d", rtbResp.StatusCode)
	}
	var rtbResult struct {
		RouteTable struct {
			RouteTableID string `xml:"routeTableId"`
		} `xml:"routeTable"`
	}
	_ = xml.NewDecoder(rtbResp.Body).Decode(&rtbResult)
	rtbResp.Body.Close() //nolint:errcheck
	rtbID := rtbResult.RouteTable.RouteTableID
	if !strings.HasPrefix(rtbID, "rtb-") {
		t.Errorf("expected RTB ID starting with rtb-, got %q", rtbID)
	}

	// Associate route table with subnet.
	assocResp := ec2Request(t, ts, map[string]string{
		"Action":       "AssociateRouteTable",
		"RouteTableId": rtbID,
		"SubnetId":     subnetID,
	})
	if assocResp.StatusCode != http.StatusOK {
		t.Fatalf("AssociateRouteTable: expected 200, got %d", assocResp.StatusCode)
	}
	var assocResult struct {
		AssociationID string `xml:"associationId"`
	}
	_ = xml.NewDecoder(assocResp.Body).Decode(&assocResult)
	assocResp.Body.Close() //nolint:errcheck

	// Create route via IGW.
	routeResp := ec2Request(t, ts, map[string]string{
		"Action":               "CreateRoute",
		"RouteTableId":         rtbID,
		"DestinationCidrBlock": "0.0.0.0/0",
		"GatewayId":            igwID,
	})
	if routeResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateRoute: expected 200, got %d", routeResp.StatusCode)
	}
	routeResp.Body.Close() //nolint:errcheck

	// Disassociate.
	if assocResult.AssociationID != "" {
		disassocResp := ec2Request(t, ts, map[string]string{
			"Action":        "DisassociateRouteTable",
			"AssociationId": assocResult.AssociationID,
		})
		if disassocResp.StatusCode != http.StatusOK {
			t.Fatalf("DisassociateRouteTable: expected 200, got %d", disassocResp.StatusCode)
		}
		disassocResp.Body.Close() //nolint:errcheck
	}
}

func TestEC2_DefaultVPCAutoCreate(t *testing.T) {
	ts := newEC2TestServer(t)

	// Launch without specifying a subnet — should auto-create default VPC/subnet.
	resp := ec2Request(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-auto",
		"MinCount": "1",
		"MaxCount": "1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances (auto VPC): expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			SubnetID   string `xml:"subnetId"`
		} `xml:"instancesSet>item"`
	}
	_ = xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close() //nolint:errcheck

	if len(result.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(result.Instances))
	}
	if result.Instances[0].SubnetID == "" {
		t.Error("expected instance to be in a subnet (default VPC auto-created)")
	}

	// Default VPC should now exist.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeVpcs"})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeVpcs: expected 200, got %d", descResp.StatusCode)
	}
	var vpcs struct {
		Vpcs []struct {
			VpcID     string `xml:"vpcId"`
			IsDefault bool   `xml:"isDefault"`
		} `xml:"vpcSet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&vpcs)
	descResp.Body.Close() //nolint:errcheck

	if len(vpcs.Vpcs) == 0 {
		t.Error("expected at least one VPC after auto-creation")
	}
}

func TestEC2_DescribeInstances_Filter(t *testing.T) {
	ts := newEC2TestServer(t)

	// Launch 2 instances.
	r1 := ec2Request(t, ts, map[string]string{"Action": "RunInstances", "ImageId": "ami-1", "MinCount": "1", "MaxCount": "1"})
	r1.Body.Close() //nolint:errcheck
	r2 := ec2Request(t, ts, map[string]string{"Action": "RunInstances", "ImageId": "ami-2", "MinCount": "1", "MaxCount": "1"})
	r2.Body.Close() //nolint:errcheck

	// Describe all — should get at least 2.
	allResp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
	if allResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances (all): expected 200, got %d", allResp.StatusCode)
	}
	allResp.Body.Close() //nolint:errcheck

	// Filter by state name = running.
	filterResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "instance-state-name",
		"Filter.1.Value.1": "running",
	})
	if filterResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances (filtered): expected 200, got %d", filterResp.StatusCode)
	}
	filterResp.Body.Close() //nolint:errcheck
}

func TestEC2_DescribeInstanceStatus(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-status", "MinCount": "1", "MaxCount": "1",
	})
	resp.Body.Close() //nolint:errcheck

	statusResp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstanceStatus"})
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstanceStatus: expected 200, got %d", statusResp.StatusCode)
	}
	statusResp.Body.Close() //nolint:errcheck
}

func TestEC2_SecurityGroup_DescribeDelete(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC first.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	var vpcResult struct {
		VPC struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(vpcResp.Body).Decode(&vpcResult); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	vpcResp.Body.Close() //nolint:errcheck
	vpcID := vpcResult.VPC.VPCID

	// CreateSecurityGroup.
	sgResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateSecurityGroup",
		"GroupName":        "test-sg",
		"GroupDescription": "test sg",
		"VpcId":            vpcID,
	})
	var sgResult struct {
		GroupID string `xml:"groupId"`
	}
	if err := xml.NewDecoder(sgResp.Body).Decode(&sgResult); err != nil {
		t.Fatalf("decode CreateSecurityGroup: %v", err)
	}
	sgResp.Body.Close() //nolint:errcheck
	sgID := sgResult.GroupID

	// DescribeSecurityGroups.
	dsgResp := ec2Request(t, ts, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": sgID})
	if dsgResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeSecurityGroups: expected 200, got %d", dsgResp.StatusCode)
	}
	dsgResp.Body.Close() //nolint:errcheck

	// DeleteSecurityGroup.
	delResp := ec2Request(t, ts, map[string]string{"Action": "DeleteSecurityGroup", "GroupId": sgID})
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteSecurityGroup: expected 200, got %d", delResp.StatusCode)
	}
	delResp.Body.Close() //nolint:errcheck
}

// TestEC2_SecurityGroup_GroupDescription verifies that CreateSecurityGroup reads
// the description from the GroupDescription parameter (not Description), matching
// the AWS EC2 query protocol wire format. Regression test for #219.
func TestEC2_SecurityGroup_GroupDescription(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.5.0.0/16"})
	var vpcResult struct {
		VPC struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(vpcResp.Body).Decode(&vpcResult); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	vpcResp.Body.Close() //nolint:errcheck

	sgResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateSecurityGroup",
		"GroupName":        "desc-test-sg",
		"GroupDescription": "my group description",
		"VpcId":            vpcResult.VPC.VPCID,
	})
	if sgResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateSecurityGroup: expected 200, got %d", sgResp.StatusCode)
	}
	var sgResult struct {
		GroupID string `xml:"groupId"`
	}
	if err := xml.NewDecoder(sgResp.Body).Decode(&sgResult); err != nil {
		t.Fatalf("decode CreateSecurityGroup: %v", err)
	}
	sgResp.Body.Close() //nolint:errcheck

	dResp := ec2Request(t, ts, map[string]string{
		"Action":    "DescribeSecurityGroups",
		"GroupId.1": sgResult.GroupID,
	})
	if dResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeSecurityGroups: expected 200, got %d", dResp.StatusCode)
	}
	var dResult struct {
		Groups []struct {
			Description string `xml:"groupDescription"`
		} `xml:"securityGroupInfo>item"`
	}
	if err := xml.NewDecoder(dResp.Body).Decode(&dResult); err != nil {
		t.Fatalf("decode DescribeSecurityGroups: %v", err)
	}
	dResp.Body.Close() //nolint:errcheck
	if len(dResult.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(dResult.Groups))
	}
	if dResult.Groups[0].Description != "my group description" {
		t.Errorf("groupDescription = %q; want %q", dResult.Groups[0].Description, "my group description")
	}
}

func TestEC2_SecurityGroup_EgressRules(t *testing.T) {
	ts := newEC2TestServer(t)

	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	var vpcResult struct {
		VPC struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(vpcResp.Body).Decode(&vpcResult); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	vpcResp.Body.Close() //nolint:errcheck

	sgResp := ec2Request(t, ts, map[string]string{
		"Action": "CreateSecurityGroup", "GroupName": "egress-sg",
		"GroupDescription": "egress test", "VpcId": vpcResult.VPC.VPCID,
	})
	var sgResult struct {
		GroupID string `xml:"groupId"`
	}
	if err := xml.NewDecoder(sgResp.Body).Decode(&sgResult); err != nil {
		t.Fatalf("decode CreateSecurityGroup: %v", err)
	}
	sgResp.Body.Close() //nolint:errcheck
	sgID := sgResult.GroupID

	// AuthorizeSecurityGroupEgress.
	authResp := ec2Request(t, ts, map[string]string{
		"Action":                     "AuthorizeSecurityGroupEgress",
		"GroupId":                    sgID,
		"IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort":   "443",
		"IpPermissions.1.ToPort":     "443",
	})
	if authResp.StatusCode != http.StatusOK {
		t.Fatalf("AuthorizeSecurityGroupEgress: expected 200, got %d", authResp.StatusCode)
	}
	authResp.Body.Close() //nolint:errcheck

	// RevokeSecurityGroupEgress.
	revResp := ec2Request(t, ts, map[string]string{
		"Action":                     "RevokeSecurityGroupEgress",
		"GroupId":                    sgID,
		"IpPermissions.1.IpProtocol": "tcp",
		"IpPermissions.1.FromPort":   "443",
		"IpPermissions.1.ToPort":     "443",
	})
	if revResp.StatusCode != http.StatusOK {
		t.Fatalf("RevokeSecurityGroupEgress: expected 200, got %d", revResp.StatusCode)
	}
	revResp.Body.Close() //nolint:errcheck
}

func TestEC2_InternetGateway_DescribeDelete(t *testing.T) {
	ts := newEC2TestServer(t)

	// CreateInternetGateway.
	createResp := ec2Request(t, ts, map[string]string{"Action": "CreateInternetGateway"})
	var igwResult struct {
		IGW struct {
			IGWID string `xml:"internetGatewayId"`
		} `xml:"internetGateway"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&igwResult); err != nil {
		t.Fatalf("decode CreateInternetGateway: %v", err)
	}
	createResp.Body.Close() //nolint:errcheck
	igwID := igwResult.IGW.IGWID

	// DescribeInternetGateways.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeInternetGateways", "InternetGatewayId.1": igwID})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInternetGateways: expected 200, got %d", descResp.StatusCode)
	}
	descResp.Body.Close() //nolint:errcheck

	// DeleteInternetGateway.
	delResp := ec2Request(t, ts, map[string]string{"Action": "DeleteInternetGateway", "InternetGatewayId": igwID})
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteInternetGateway: expected 200, got %d", delResp.StatusCode)
	}
	delResp.Body.Close() //nolint:errcheck
}

func TestEC2_RouteTable_DescribeDeleteRoute(t *testing.T) {
	ts := newEC2TestServer(t)

	// CreateVPC then RouteTable.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.1.0.0/16"})
	var vpcResult struct {
		VPC struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(vpcResp.Body).Decode(&vpcResult); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	vpcResp.Body.Close() //nolint:errcheck

	rtbResp := ec2Request(t, ts, map[string]string{"Action": "CreateRouteTable", "VpcId": vpcResult.VPC.VPCID})
	var rtbResult struct {
		RouteTable struct {
			RTBID string `xml:"routeTableId"`
		} `xml:"routeTable"`
	}
	if err := xml.NewDecoder(rtbResp.Body).Decode(&rtbResult); err != nil {
		t.Fatalf("decode CreateRouteTable: %v", err)
	}
	rtbResp.Body.Close() //nolint:errcheck
	rtbID := rtbResult.RouteTable.RTBID

	// DescribeRouteTables.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeRouteTables", "RouteTableId.1": rtbID})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeRouteTables: expected 200, got %d", descResp.StatusCode)
	}
	descResp.Body.Close() //nolint:errcheck

	// CreateRoute then DeleteRoute.
	crResp := ec2Request(t, ts, map[string]string{
		"Action":               "CreateRoute",
		"RouteTableId":         rtbID,
		"DestinationCidrBlock": "0.0.0.0/0",
		"GatewayId":            "igw-00000000",
	})
	if crResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateRoute: expected 200, got %d", crResp.StatusCode)
	}
	crResp.Body.Close() //nolint:errcheck

	drResp := ec2Request(t, ts, map[string]string{
		"Action":               "DeleteRoute",
		"RouteTableId":         rtbID,
		"DestinationCidrBlock": "0.0.0.0/0",
	})
	if drResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteRoute: expected 200, got %d", drResp.StatusCode)
	}
	drResp.Body.Close() //nolint:errcheck

	// DeleteRouteTable.
	delRTBResp := ec2Request(t, ts, map[string]string{"Action": "DeleteRouteTable", "RouteTableId": rtbID})
	if delRTBResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteRouteTable: expected 200, got %d", delRTBResp.StatusCode)
	}
	delRTBResp.Body.Close() //nolint:errcheck
}

func TestEC2_KeyPair_CreateDescribeDelete(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// CreateKeyPair.
	createResp := ec2Request(t, ts, map[string]string{
		"Action":  "CreateKeyPair",
		"KeyName": "my-test-key",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateKeyPair: expected 200, got %d", createResp.StatusCode)
	}
	var createResult struct {
		XMLName        xml.Name `xml:"CreateKeyPairResponse"`
		KeyPairID      string   `xml:"keyPairId"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyMaterial    string   `xml:"keyMaterial"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode CreateKeyPair response: %v", err)
	}
	createResp.Body.Close() //nolint:errcheck
	if createResult.KeyName != "my-test-key" {
		t.Errorf("KeyName = %q; want %q", createResult.KeyName, "my-test-key")
	}
	if createResult.KeyPairID == "" {
		t.Error("KeyPairId is empty")
	}
	if createResult.KeyFingerprint == "" {
		t.Error("KeyFingerprint is empty")
	}
	if createResult.KeyMaterial == "" {
		t.Error("KeyMaterial is empty")
	}

	// Duplicate CreateKeyPair should return 400.
	dupResp := ec2Request(t, ts, map[string]string{
		"Action":  "CreateKeyPair",
		"KeyName": "my-test-key",
	})
	if dupResp.StatusCode != http.StatusBadRequest {
		t.Errorf("duplicate CreateKeyPair: expected 400, got %d", dupResp.StatusCode)
	}
	dupResp.Body.Close() //nolint:errcheck

	// DescribeKeyPairs — no filter → returns our key.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeKeyPairs"})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeKeyPairs: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		XMLName  xml.Name `xml:"DescribeKeyPairsResponse"`
		KeyPairs []struct {
			KeyName string `xml:"keyName"`
		} `xml:"keySet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeKeyPairs: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.KeyPairs) != 1 || descResult.KeyPairs[0].KeyName != "my-test-key" {
		t.Errorf("DescribeKeyPairs returned %v; want [my-test-key]", descResult.KeyPairs)
	}

	// DescribeKeyPairs with name filter.
	filtResp := ec2Request(t, ts, map[string]string{"Action": "DescribeKeyPairs", "KeyName.1": "my-test-key"})
	if filtResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeKeyPairs filter: expected 200, got %d", filtResp.StatusCode)
	}
	filtResp.Body.Close() //nolint:errcheck

	// RunInstances referencing the key pair records KeyName on the instance.
	runResp := ec2Request(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
		"KeyName":      "my-test-key",
	})
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", runResp.StatusCode)
	}
	var runResult struct {
		Instances []struct {
			KeyName string `xml:"keyName"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	if len(runResult.Instances) == 0 || runResult.Instances[0].KeyName != "my-test-key" {
		t.Errorf("RunInstances KeyName = %q; want %q", func() string {
			if len(runResult.Instances) > 0 {
				return runResult.Instances[0].KeyName
			}
			return ""
		}(), "my-test-key")
	}

	// DeleteKeyPair.
	delResp := ec2Request(t, ts, map[string]string{
		"Action":  "DeleteKeyPair",
		"KeyName": "my-test-key",
	})
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteKeyPair: expected 200, got %d", delResp.StatusCode)
	}
	delResp.Body.Close() //nolint:errcheck

	// After deletion, DescribeKeyPairs returns empty set.
	afterResp := ec2Request(t, ts, map[string]string{"Action": "DescribeKeyPairs"})
	if afterResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeKeyPairs after delete: expected 200, got %d", afterResp.StatusCode)
	}
	var afterResult struct {
		KeyPairs []struct {
			KeyName string `xml:"keyName"`
		} `xml:"keySet>item"`
	}
	if err := xml.NewDecoder(afterResp.Body).Decode(&afterResult); err != nil {
		t.Fatalf("decode DescribeKeyPairs after delete: %v", err)
	}
	afterResp.Body.Close() //nolint:errcheck
	if len(afterResult.KeyPairs) != 0 {
		t.Errorf("expected 0 key pairs after delete; got %d", len(afterResult.KeyPairs))
	}
}

func TestEC2_KeyPair_Import(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// ImportKeyPair with a base64-encoded public key.
	pubKeyB64 := "c3NoLWVkMjU1MTkgQUFBQUM=" // base64 of "ssh-ed25519 AAAAC" (fake but valid base64)
	importResp := ec2Request(t, ts, map[string]string{
		"Action":            "ImportKeyPair",
		"KeyName":           "imported-key",
		"PublicKeyMaterial": pubKeyB64,
	})
	if importResp.StatusCode != http.StatusOK {
		t.Fatalf("ImportKeyPair: expected 200, got %d", importResp.StatusCode)
	}
	var importResult struct {
		KeyPairID      string `xml:"keyPairId"`
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
	}
	if err := xml.NewDecoder(importResp.Body).Decode(&importResult); err != nil {
		t.Fatalf("decode ImportKeyPair: %v", err)
	}
	importResp.Body.Close() //nolint:errcheck
	if importResult.KeyName != "imported-key" {
		t.Errorf("KeyName = %q; want %q", importResult.KeyName, "imported-key")
	}
	if importResult.KeyPairID == "" {
		t.Error("KeyPairId is empty")
	}
	if importResult.KeyFingerprint == "" {
		t.Error("KeyFingerprint is empty")
	}
}

func TestEC2_RebootInstances(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch an instance first.
	runResp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-1", "MinCount": "1", "MaxCount": "1",
	})
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	id := runResult.Instances[0].InstanceID

	resp := ec2Request(t, ts, map[string]string{"Action": "RebootInstances", "InstanceId.1": id})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RebootInstances: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close() //nolint:errcheck
}

func TestEC2_CreateDeleteTags(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch an instance.
	runResp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-1", "MinCount": "1", "MaxCount": "1",
	})
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	id := runResult.Instances[0].InstanceID

	// CreateTags.
	tagResp := ec2Request(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": id,
		"Tag.1.Key":    "Name",
		"Tag.1.Value":  "my-instance",
		"Tag.2.Key":    "Env",
		"Tag.2.Value":  "test",
	})
	if tagResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateTags: expected 200, got %d", tagResp.StatusCode)
	}
	tagResp.Body.Close() //nolint:errcheck

	// DescribeInstances should reflect updated tags.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances"})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		Reservations []struct {
			Instances []struct {
				Tags []struct {
					Key   string `xml:"key"`
					Value string `xml:"value"`
				} `xml:"tagSet>item"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.Reservations) == 0 || len(descResult.Reservations[0].Instances) == 0 {
		t.Fatal("expected instance in DescribeInstances response")
	}
	tags := descResult.Reservations[0].Instances[0].Tags
	found := false
	for _, tag := range tags {
		if tag.Key == "Name" && tag.Value == "my-instance" {
			found = true
		}
	}
	if !found {
		t.Errorf("Name=my-instance tag not found in %v", tags)
	}

	// DeleteTags — remove the Name tag.
	delTagResp := ec2Request(t, ts, map[string]string{
		"Action":       "DeleteTags",
		"ResourceId.1": id,
		"Tag.1.Key":    "Name",
	})
	if delTagResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteTags: expected 200, got %d", delTagResp.StatusCode)
	}
	delTagResp.Body.Close() //nolint:errcheck
}

func TestEC2_ModifyInstanceAttribute(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch a t3.micro instance.
	runResp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-1",
		"InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	id := runResult.Instances[0].InstanceID

	// Modify to t3.medium.
	modResp := ec2Request(t, ts, map[string]string{
		"Action":             "ModifyInstanceAttribute",
		"InstanceId":         id,
		"InstanceType.Value": "t3.medium",
	})
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyInstanceAttribute: expected 200, got %d", modResp.StatusCode)
	}
	modResp.Body.Close() //nolint:errcheck

	// DescribeInstances should show t3.medium.
	descResp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": id,
	})
	var descResult struct {
		Reservations []struct {
			Instances []struct {
				InstanceType string `xml:"instanceType"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.Reservations) == 0 || len(descResult.Reservations[0].Instances) == 0 {
		t.Fatal("no instance in response")
	}
	if got := descResult.Reservations[0].Instances[0].InstanceType; got != "t3.medium" {
		t.Errorf("InstanceType = %q; want %q", got, "t3.medium")
	}
}

func TestEC2_AMI_CreateDescribeDeregister(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch an instance so we have a valid source instance ID.
	runResp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-base",
		"InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	if len(runResult.Instances) == 0 {
		t.Fatal("no instance returned by RunInstances")
	}
	instanceID := runResult.Instances[0].InstanceID

	// CreateImage with a tag.
	createResp := ec2Request(t, ts, map[string]string{
		"Action":                          "CreateImage",
		"InstanceId":                      instanceID,
		"Name":                            "my-snapshot",
		"Description":                     "test AMI",
		"NoReboot":                        "true",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "canopy:managed",
		"TagSpecification.1.Tag.1.Value":  "true",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateImage: expected 200, got %d", createResp.StatusCode)
	}
	var createResult struct {
		ImageID string `xml:"imageId"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode CreateImage response: %v", err)
	}
	createResp.Body.Close() //nolint:errcheck
	if !strings.HasPrefix(createResult.ImageID, "ami-") {
		t.Errorf("ImageID = %q; want prefix ami-", createResult.ImageID)
	}
	imageID := createResult.ImageID

	// DescribeImages with tag filter should return the AMI.
	descResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeImages",
		"Owner.1":          "self",
		"Filter.1.Name":    "tag:canopy:managed",
		"Filter.1.Value.1": "true",
	})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeImages: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		Images []struct {
			ImageID string `xml:"imageId"`
			Name    string `xml:"name"`
			State   string `xml:"imageState"`
			Tags    []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"imagesSet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeImages response: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.Images) != 1 {
		t.Fatalf("expected 1 image, got %d", len(descResult.Images))
	}
	if descResult.Images[0].ImageID != imageID {
		t.Errorf("ImageID = %q; want %q", descResult.Images[0].ImageID, imageID)
	}
	if descResult.Images[0].Name != "my-snapshot" {
		t.Errorf("Name = %q; want %q", descResult.Images[0].Name, "my-snapshot")
	}
	if descResult.Images[0].State != "available" {
		t.Errorf("State = %q; want available", descResult.Images[0].State)
	}
	if len(descResult.Images[0].Tags) != 1 || descResult.Images[0].Tags[0].Key != "canopy:managed" {
		t.Errorf("tags = %v; want [{canopy:managed true}]", descResult.Images[0].Tags)
	}

	// DeregisterImage removes the AMI.
	deregResp := ec2Request(t, ts, map[string]string{
		"Action":  "DeregisterImage",
		"ImageId": imageID,
	})
	if deregResp.StatusCode != http.StatusOK {
		t.Fatalf("DeregisterImage: expected 200, got %d", deregResp.StatusCode)
	}
	deregResp.Body.Close() //nolint:errcheck

	// DescribeImages should return empty list now.
	descResp2 := ec2Request(t, ts, map[string]string{
		"Action":  "DescribeImages",
		"Owner.1": "self",
	})
	var descResult2 struct {
		Images []struct {
			ImageID string `xml:"imageId"`
		} `xml:"imagesSet>item"`
	}
	if err := xml.NewDecoder(descResp2.Body).Decode(&descResult2); err != nil {
		t.Fatalf("decode DescribeImages (post-deregister) response: %v", err)
	}
	descResp2.Body.Close() //nolint:errcheck
	if len(descResult2.Images) != 0 {
		t.Errorf("expected 0 images after DeregisterImage, got %d", len(descResult2.Images))
	}
}

// TestEC2_KeyPair_CreateTime verifies that DescribeKeyPairs includes a non-empty
// createTime field. Regression test for #218.
func TestEC2_KeyPair_CreateTime(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	createResp := ec2Request(t, ts, map[string]string{
		"Action":  "CreateKeyPair",
		"KeyName": "key-with-time",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateKeyPair: expected 200, got %d", createResp.StatusCode)
	}
	createResp.Body.Close() //nolint:errcheck

	descResp := ec2Request(t, ts, map[string]string{
		"Action":    "DescribeKeyPairs",
		"KeyName.1": "key-with-time",
	})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeKeyPairs: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		KeyPairs []struct {
			CreateTime string `xml:"createTime"`
		} `xml:"keySet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeKeyPairs: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.KeyPairs) != 1 {
		t.Fatalf("expected 1 key pair, got %d", len(descResult.KeyPairs))
	}
	if descResult.KeyPairs[0].CreateTime == "" {
		t.Error("createTime is empty; want RFC3339 timestamp")
	}
}

// TestEC2_KeyPair_KeyType_Default verifies that CreateKeyPair without an explicit
// KeyType defaults to "rsa" and that DescribeKeyPairs echoes the keyType field.
// Regression test for #215.
func TestEC2_KeyPair_KeyType_Default(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	createResp := ec2Request(t, ts, map[string]string{
		"Action":  "CreateKeyPair",
		"KeyName": "key-rsa-default",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateKeyPair: expected 200, got %d", createResp.StatusCode)
	}
	var createResult struct {
		KeyType string `xml:"keyType"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode CreateKeyPair response: %v", err)
	}
	createResp.Body.Close() //nolint:errcheck
	if createResult.KeyType != "rsa" {
		t.Errorf("keyType = %q; want %q", createResult.KeyType, "rsa")
	}

	descResp := ec2Request(t, ts, map[string]string{
		"Action":    "DescribeKeyPairs",
		"KeyName.1": "key-rsa-default",
	})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeKeyPairs: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		KeyPairs []struct {
			KeyType string `xml:"keyType"`
		} `xml:"keySet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeKeyPairs: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.KeyPairs) != 1 {
		t.Fatalf("expected 1 key pair, got %d", len(descResult.KeyPairs))
	}
	if descResult.KeyPairs[0].KeyType != "rsa" {
		t.Errorf("DescribeKeyPairs keyType = %q; want %q", descResult.KeyPairs[0].KeyType, "rsa")
	}
}

// TestEC2_KeyPair_KeyType_Ed25519 verifies that an explicit KeyType=ed25519 is
// stored and returned. Regression test for #215.
func TestEC2_KeyPair_KeyType_Ed25519(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	createResp := ec2Request(t, ts, map[string]string{
		"Action":  "CreateKeyPair",
		"KeyName": "key-ed25519",
		"KeyType": "ed25519",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateKeyPair ed25519: expected 200, got %d", createResp.StatusCode)
	}
	var createResult struct {
		KeyType string `xml:"keyType"`
	}
	if err := xml.NewDecoder(createResp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode CreateKeyPair response: %v", err)
	}
	createResp.Body.Close() //nolint:errcheck
	if createResult.KeyType != "ed25519" {
		t.Errorf("keyType = %q; want %q", createResult.KeyType, "ed25519")
	}
}

// TestEC2_Image_CreationDate verifies that DescribeImages includes a non-empty
// creationDate field. Regression test for #214.
func TestEC2_Image_CreationDate(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch an instance so we have a valid source instance ID.
	runResp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": "ami-base",
		"InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	if len(runResult.Instances) == 0 {
		t.Fatal("no instance returned by RunInstances")
	}
	instanceID := runResult.Instances[0].InstanceID

	createResp := ec2Request(t, ts, map[string]string{
		"Action":     "CreateImage",
		"InstanceId": instanceID,
		"Name":       "test-ami-date",
	})
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateImage: expected 200, got %d", createResp.StatusCode)
	}
	createResp.Body.Close() //nolint:errcheck

	descResp := ec2Request(t, ts, map[string]string{
		"Action":  "DescribeImages",
		"Owner.1": "self",
	})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeImages: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		Images []struct {
			CreationDate string `xml:"creationDate"`
		} `xml:"imagesSet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeImages: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.Images) == 0 {
		t.Fatal("no images returned")
	}
	if descResult.Images[0].CreationDate == "" {
		t.Error("creationDate is empty; want RFC3339 timestamp")
	}
}

func TestEC2_RunInstances_TagSpecifications(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	// Launch with TagSpecifications for ResourceType=instance.
	runResp := ec2Request(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-1",
		"InstanceType":                    "t3.micro",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "Name",
		"TagSpecification.1.Tag.1.Value":  "my-instance",
		"TagSpecification.1.Tag.2.Key":    "canopy:managed",
		"TagSpecification.1.Tag.2.Value":  "true",
	})
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", runResp.StatusCode)
	}
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			Tags       []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	runResp.Body.Close() //nolint:errcheck
	if len(runResult.Instances) == 0 {
		t.Fatal("no instance returned")
	}
	instanceID := runResult.Instances[0].InstanceID

	// DescribeInstances should return the launch tags.
	descResp := ec2Request(t, ts, map[string]string{
		"Action": "DescribeInstances", "InstanceId.1": instanceID,
	})
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances: expected 200, got %d", descResp.StatusCode)
	}
	var descResult struct {
		Reservations []struct {
			Instances []struct {
				Tags []struct {
					Key   string `xml:"key"`
					Value string `xml:"value"`
				} `xml:"tagSet>item"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(descResp.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeInstances: %v", err)
	}
	descResp.Body.Close() //nolint:errcheck
	if len(descResult.Reservations) == 0 || len(descResult.Reservations[0].Instances) == 0 {
		t.Fatal("no instance in describe response")
	}
	tags := descResult.Reservations[0].Instances[0].Tags
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags from launch, got %d: %v", len(tags), tags)
	}
	tagMap := make(map[string]string, len(tags))
	for _, tg := range tags {
		tagMap[tg.Key] = tg.Value
	}
	if tagMap["Name"] != "my-instance" {
		t.Errorf("Name tag = %q; want %q", tagMap["Name"], "my-instance")
	}
	if tagMap["canopy:managed"] != "true" {
		t.Errorf("canopy:managed tag = %q; want %q", tagMap["canopy:managed"], "true")
	}
}

// TestEC2_DescribeSecurityGroups_Filters verifies that DescribeSecurityGroups
// correctly filters by group-name, vpc-id, and group-id.  Regression test for #211.
func TestEC2_DescribeSecurityGroups_Filters(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create two VPCs.
	makeVPC := func(cidr string) string {
		t.Helper()
		r := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": cidr})
		defer r.Body.Close() //nolint:errcheck
		var res struct {
			VPC struct {
				VPCID string `xml:"vpcId"`
			} `xml:"vpc"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&res); err != nil {
			t.Fatalf("decode CreateVpc: %v", err)
		}
		return res.VPC.VPCID
	}
	makeSG := func(name, vpcID string) string {
		t.Helper()
		r := ec2Request(t, ts, map[string]string{
			"Action":           "CreateSecurityGroup",
			"GroupName":        name,
			"GroupDescription": name,
			"VpcId":            vpcID,
		})
		defer r.Body.Close() //nolint:errcheck
		var res struct {
			GroupID string `xml:"groupId"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&res); err != nil {
			t.Fatalf("decode CreateSecurityGroup: %v", err)
		}
		return res.GroupID
	}
	countSGs := func(resp *http.Response) int {
		t.Helper()
		defer resp.Body.Close() //nolint:errcheck
		var res struct {
			Groups []struct {
				GroupID   string `xml:"groupId"`
				GroupName string `xml:"groupName"`
			} `xml:"securityGroupInfo>item"`
		}
		if err := xml.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("decode DescribeSecurityGroups: %v", err)
		}
		return len(res.Groups)
	}
	firstSGName := func(resp *http.Response) string {
		t.Helper()
		defer resp.Body.Close() //nolint:errcheck
		var res struct {
			Groups []struct {
				GroupName string `xml:"groupName"`
			} `xml:"securityGroupInfo>item"`
		}
		if err := xml.NewDecoder(resp.Body).Decode(&res); err != nil {
			t.Fatalf("decode DescribeSecurityGroups: %v", err)
		}
		if len(res.Groups) == 0 {
			return ""
		}
		return res.Groups[0].GroupName
	}

	vpc1 := makeVPC("10.1.0.0/16")
	vpc2 := makeVPC("10.2.0.0/16")
	sg1ID := makeSG("canopy-default", vpc1)
	_ = makeSG("other-sg", vpc1)
	_ = makeSG("canopy-default", vpc2) // same name, different VPC

	// No filters: all SGs returned (including auto-created "default" SGs).
	allResp := ec2Request(t, ts, map[string]string{"Action": "DescribeSecurityGroups"})
	total := countSGs(allResp)
	if total < 3 {
		t.Fatalf("expected at least 3 SGs without filter, got %d", total)
	}

	// Filter by group-name=canopy-default: should return 2 (one per VPC).
	nameResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSecurityGroups",
		"Filter.1.Name":    "group-name",
		"Filter.1.Value.1": "canopy-default",
	})
	if n := countSGs(nameResp); n != 2 {
		t.Errorf("group-name filter: expected 2, got %d", n)
	}

	// Filter by group-name + vpc-id: should return exactly 1.
	bothResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSecurityGroups",
		"Filter.1.Name":    "group-name",
		"Filter.1.Value.1": "canopy-default",
		"Filter.2.Name":    "vpc-id",
		"Filter.2.Value.1": vpc1,
	})
	if n := countSGs(bothResp); n != 1 {
		t.Errorf("group-name+vpc-id filter: expected 1, got %d", n)
	}

	// Filter by group-name=canopy-default + vpc-id=vpc1: name must be canopy-default.
	checkResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSecurityGroups",
		"Filter.1.Name":    "group-name",
		"Filter.1.Value.1": "canopy-default",
		"Filter.2.Name":    "vpc-id",
		"Filter.2.Value.1": vpc1,
	})
	if name := firstSGName(checkResp); name != "canopy-default" {
		t.Errorf("expected group-name=canopy-default, got %q", name)
	}

	// Filter by group-id: should return exactly the requested SG.
	idResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSecurityGroups",
		"Filter.1.Name":    "group-id",
		"Filter.1.Value.1": sg1ID,
	})
	if n := countSGs(idResp); n != 1 {
		t.Errorf("group-id filter: expected 1, got %d", n)
	}

	// Filter by group-name that doesn't exist: empty result.
	emptyResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSecurityGroups",
		"Filter.1.Name":    "group-name",
		"Filter.1.Value.1": "nonexistent-sg",
	})
	if n := countSGs(emptyResp); n != 0 {
		t.Errorf("nonexistent group-name filter: expected 0, got %d", n)
	}
}

func TestEC2_PublicIP(t *testing.T) {
	ts := newEC2TestServer(t)

	// RunInstances into the default VPC/subnet (no SubnetId specified).
	resp := ec2Request(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", resp.StatusCode)
	}
	defer resp.Body.Close() //nolint:errcheck

	var runResult struct {
		XMLName   xml.Name `xml:"RunInstancesResponse"`
		Instances []struct {
			InstanceID   string `xml:"instanceId"`
			PublicIPAddr string `xml:"publicIpAddress"`
			PublicDNS    string `xml:"dnsName"`
			PrivateDNS   string `xml:"privateDnsName"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances response: %v", err)
	}
	if len(runResult.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(runResult.Instances))
	}
	inst := runResult.Instances[0]

	// Public IP must be in 54.x.x.x range.
	if !strings.HasPrefix(inst.PublicIPAddr, "54.") {
		t.Errorf("expected public IP starting with 54., got %q", inst.PublicIPAddr)
	}
	// Public DNS must start with "ec2-" and end with ".amazonaws.com".
	if !strings.HasPrefix(inst.PublicDNS, "ec2-") || !strings.HasSuffix(inst.PublicDNS, ".amazonaws.com") {
		t.Errorf("unexpected public DNS name: %q", inst.PublicDNS)
	}
	// Private DNS must end with ".compute.internal".
	if !strings.HasSuffix(inst.PrivateDNS, ".compute.internal") {
		t.Errorf("unexpected private DNS name: %q", inst.PrivateDNS)
	}

	// DescribeInstances must return the same fields.
	resp2 := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": inst.InstanceID,
	})
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("DescribeInstances: expected 200, got %d", resp2.StatusCode)
	}
	defer resp2.Body.Close() //nolint:errcheck

	var descResult struct {
		XMLName      xml.Name `xml:"DescribeInstancesResponse"`
		Reservations []struct {
			Instances []struct {
				PublicIPAddr string `xml:"publicIpAddress"`
				PublicDNS    string `xml:"dnsName"`
				PrivateDNS   string `xml:"privateDnsName"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(resp2.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode DescribeInstances response: %v", err)
	}
	if len(descResult.Reservations) == 0 || len(descResult.Reservations[0].Instances) == 0 {
		t.Fatal("DescribeInstances returned no instances")
	}
	di := descResult.Reservations[0].Instances[0]
	if di.PublicIPAddr != inst.PublicIPAddr {
		t.Errorf("DescribeInstances public IP %q != RunInstances %q", di.PublicIPAddr, inst.PublicIPAddr)
	}
	if di.PublicDNS != inst.PublicDNS {
		t.Errorf("DescribeInstances public DNS %q != RunInstances %q", di.PublicDNS, inst.PublicDNS)
	}
	if !strings.HasSuffix(di.PrivateDNS, ".compute.internal") {
		t.Errorf("DescribeInstances private DNS %q missing .compute.internal suffix", di.PrivateDNS)
	}
}

func TestEC2_NoPublicIP_NonDefaultSubnet(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create a VPC and a non-default subnet.
	vpcResp := ec2Request(t, ts, map[string]string{
		"Action":    "CreateVpc",
		"CidrBlock": "10.0.0.0/16",
	})
	if vpcResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateVpc: expected 200, got %d", vpcResp.StatusCode)
	}
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		XMLName xml.Name `xml:"CreateVpcResponse"`
		VPC     struct {
			VPCID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	if err := xml.NewDecoder(vpcResp.Body).Decode(&vpcResult); err != nil {
		t.Fatalf("decode CreateVpc: %v", err)
	}
	vpcID := vpcResult.VPC.VPCID

	subnetResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateSubnet",
		"VpcId":            vpcID,
		"CidrBlock":        "10.0.1.0/24",
		"AvailabilityZone": "us-east-1a",
	})
	if subnetResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateSubnet: expected 200, got %d", subnetResp.StatusCode)
	}
	defer subnetResp.Body.Close() //nolint:errcheck
	var subnetResult struct {
		XMLName xml.Name `xml:"CreateSubnetResponse"`
		Subnet  struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	if err := xml.NewDecoder(subnetResp.Body).Decode(&subnetResult); err != nil {
		t.Fatalf("decode CreateSubnet: %v", err)
	}
	subnetID := subnetResult.Subnet.SubnetID

	// RunInstances into the non-default subnet.
	runResp := ec2Request(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
		"SubnetId":     subnetID,
	})
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", runResp.StatusCode)
	}
	defer runResp.Body.Close() //nolint:errcheck

	var runResult struct {
		XMLName   xml.Name `xml:"RunInstancesResponse"`
		Instances []struct {
			PublicIPAddr string `xml:"publicIpAddress"`
			PrivateDNS   string `xml:"privateDnsName"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(runResp.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode RunInstances: %v", err)
	}
	if len(runResult.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(runResult.Instances))
	}
	inst := runResult.Instances[0]

	// Non-default subnet without MapPublicIPOnLaunch: no public IP.
	if inst.PublicIPAddr != "" {
		t.Errorf("expected empty public IP for non-default subnet, got %q", inst.PublicIPAddr)
	}
	// Private DNS must still be set.
	if !strings.HasSuffix(inst.PrivateDNS, ".compute.internal") {
		t.Errorf("expected private DNS ending in .compute.internal, got %q", inst.PrivateDNS)
	}
}

// --- v0.41.0 tests ---

func TestEC2_DescribeAvailabilityZones(t *testing.T) {
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeAvailabilityZones"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	defer resp.Body.Close() //nolint:errcheck

	var result struct {
		XMLName xml.Name `xml:"DescribeAvailabilityZonesResponse"`
		AZs     []struct {
			ZoneName   string `xml:"zoneName"`
			State      string `xml:"zoneState"`
			RegionName string `xml:"regionName"`
		} `xml:"availabilityZoneInfo>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.AZs) != 3 {
		t.Fatalf("expected 3 AZs, got %d", len(result.AZs))
	}
	for i, suffix := range []string{"a", "b", "c"} {
		if !strings.HasSuffix(result.AZs[i].ZoneName, suffix) {
			t.Errorf("AZ[%d] name %q expected suffix %q", i, result.AZs[i].ZoneName, suffix)
		}
		if result.AZs[i].State != "available" {
			t.Errorf("AZ[%d] state %q, expected available", i, result.AZs[i].State)
		}
		if result.AZs[i].RegionName != "us-east-1" {
			t.Errorf("AZ[%d] region %q, expected us-east-1", i, result.AZs[i].RegionName)
		}
	}
}

func TestEC2_ModifySubnetAttribute(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC and subnet.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.0.0.0/16"})
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)

	subResp := ec2Request(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpcResult.Vpc.VpcID, "CidrBlock": "10.0.1.0/24"})
	defer subResp.Body.Close() //nolint:errcheck
	var subResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(subResp.Body).Decode(&subResult)
	subnetID := subResult.Subnet.SubnetID

	// Modify to enable MapPublicIPOnLaunch.
	modResp := ec2Request(t, ts, map[string]string{
		"Action":                    "ModifySubnetAttribute",
		"SubnetId":                  subnetID,
		"MapPublicIPOnLaunch.Value": "true",
	})
	defer modResp.Body.Close() //nolint:errcheck
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifySubnetAttribute: expected 200, got %d", modResp.StatusCode)
	}

	// Describe and verify.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeSubnets", "SubnetId.1": subnetID})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		Subnets []struct {
			MapPublicIPOnLaunch bool `xml:"mapPublicIpOnLaunch"`
		} `xml:"subnetSet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.Subnets) != 1 {
		t.Fatalf("expected 1 subnet, got %d", len(descResult.Subnets))
	}
	if !descResult.Subnets[0].MapPublicIPOnLaunch {
		t.Error("expected mapPublicIpOnLaunch=true after modify")
	}
}

func TestEC2_ModifyVpcAttribute(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.1.0.0/16"})
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcID := vpcResult.Vpc.VpcID

	// Enable DNS hostnames.
	modResp := ec2Request(t, ts, map[string]string{
		"Action":                   "ModifyVpcAttribute",
		"VpcId":                    vpcID,
		"EnableDNSHostnames.Value": "true",
	})
	defer modResp.Body.Close() //nolint:errcheck
	if modResp.StatusCode != http.StatusOK {
		t.Fatalf("ModifyVpcAttribute: expected 200, got %d", modResp.StatusCode)
	}

	// Disable DNS support.
	modResp2 := ec2Request(t, ts, map[string]string{
		"Action":                 "ModifyVpcAttribute",
		"VpcId":                  vpcID,
		"EnableDNSSupport.Value": "false",
	})
	defer modResp2.Body.Close() //nolint:errcheck
	if modResp2.StatusCode != http.StatusOK {
		t.Fatalf("ModifyVpcAttribute (dns support): expected 200, got %d", modResp2.StatusCode)
	}
}

func TestEC2_ElasticIP_AllocateRelease(t *testing.T) {
	ts := newEC2TestServer(t)

	// Allocate.
	allocResp := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"})
	defer allocResp.Body.Close() //nolint:errcheck
	if allocResp.StatusCode != http.StatusOK {
		t.Fatalf("AllocateAddress: expected 200, got %d", allocResp.StatusCode)
	}
	var allocResult struct {
		AllocationID string `xml:"allocationId"`
		PublicIP     string `xml:"publicIp"`
	}
	_ = xml.NewDecoder(allocResp.Body).Decode(&allocResult)
	if !strings.HasPrefix(allocResult.AllocationID, "eipalloc-") {
		t.Errorf("allocationId %q must start with eipalloc-", allocResult.AllocationID)
	}
	if !strings.HasPrefix(allocResult.PublicIP, "54.") {
		t.Errorf("publicIp %q must start with 54.", allocResult.PublicIP)
	}

	// DescribeAddresses — should see it.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeAddresses"})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		Addresses []struct {
			AllocationID string `xml:"allocationId"`
		} `xml:"addressesSet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.Addresses) != 1 {
		t.Fatalf("expected 1 EIP, got %d", len(descResult.Addresses))
	}

	// Release.
	relResp := ec2Request(t, ts, map[string]string{"Action": "ReleaseAddress", "AllocationId": allocResult.AllocationID})
	defer relResp.Body.Close() //nolint:errcheck
	if relResp.StatusCode != http.StatusOK {
		t.Fatalf("ReleaseAddress: expected 200, got %d", relResp.StatusCode)
	}

	// DescribeAddresses — should be empty.
	descResp2 := ec2Request(t, ts, map[string]string{"Action": "DescribeAddresses"})
	defer descResp2.Body.Close() //nolint:errcheck
	var descResult2 struct {
		Addresses []struct {
			AllocationID string `xml:"allocationId"`
		} `xml:"addressesSet>item"`
	}
	_ = xml.NewDecoder(descResp2.Body).Decode(&descResult2)
	if len(descResult2.Addresses) != 0 {
		t.Fatalf("expected 0 EIPs after release, got %d", len(descResult2.Addresses))
	}
}

func TestEC2_ElasticIP_AssociateDisassociate(t *testing.T) {
	ts := newEC2TestServer(t)

	// Allocate EIP.
	allocResp := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"})
	defer allocResp.Body.Close() //nolint:errcheck
	var allocResult struct {
		AllocationID string `xml:"allocationId"`
		PublicIP     string `xml:"publicIp"`
	}
	_ = xml.NewDecoder(allocResp.Body).Decode(&allocResult)

	// Launch instance.
	runResp := ec2Request(t, ts, map[string]string{"Action": "RunInstances", "ImageId": "ami-test", "MinCount": "1", "MaxCount": "1"})
	defer runResp.Body.Close() //nolint:errcheck
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	_ = xml.NewDecoder(runResp.Body).Decode(&runResult)
	instanceID := runResult.Instances[0].InstanceID

	// Associate.
	assocResp := ec2Request(t, ts, map[string]string{
		"Action":       "AssociateAddress",
		"AllocationId": allocResult.AllocationID,
		"InstanceId":   instanceID,
	})
	defer assocResp.Body.Close() //nolint:errcheck
	if assocResp.StatusCode != http.StatusOK {
		t.Fatalf("AssociateAddress: expected 200, got %d", assocResp.StatusCode)
	}
	var assocResult struct {
		AssociationID string `xml:"associationId"`
	}
	_ = xml.NewDecoder(assocResp.Body).Decode(&assocResult)
	if !strings.HasPrefix(assocResult.AssociationID, "eipassoc-") {
		t.Errorf("associationId %q must start with eipassoc-", assocResult.AssociationID)
	}

	// DescribeAddresses — should show instanceId.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeAddresses", "AllocationId.1": allocResult.AllocationID})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		Addresses []struct {
			InstanceID string `xml:"instanceId"`
			PublicIP   string `xml:"publicIp"`
		} `xml:"addressesSet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.Addresses) != 1 || descResult.Addresses[0].InstanceID != instanceID {
		t.Errorf("expected instanceId %q in DescribeAddresses, got %+v", instanceID, descResult.Addresses)
	}

	// DescribeInstances — public IP should match EIP.
	instResp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": instanceID})
	defer instResp.Body.Close() //nolint:errcheck
	var instResult struct {
		Reservations []struct {
			Instances []struct {
				PublicIP string `xml:"publicIpAddress"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	_ = xml.NewDecoder(instResp.Body).Decode(&instResult)
	if len(instResult.Reservations) < 1 || len(instResult.Reservations[0].Instances) < 1 {
		t.Fatal("expected instance in DescribeInstances")
	}
	if instResult.Reservations[0].Instances[0].PublicIP != allocResult.PublicIP {
		t.Errorf("instance publicIp %q, expected EIP %q", instResult.Reservations[0].Instances[0].PublicIP, allocResult.PublicIP)
	}

	// Disassociate.
	disResp := ec2Request(t, ts, map[string]string{"Action": "DisassociateAddress", "AssociationId": assocResult.AssociationID})
	defer disResp.Body.Close() //nolint:errcheck
	if disResp.StatusCode != http.StatusOK {
		t.Fatalf("DisassociateAddress: expected 200, got %d", disResp.StatusCode)
	}

	// DescribeAddresses — no instanceId.
	descResp2 := ec2Request(t, ts, map[string]string{"Action": "DescribeAddresses", "AllocationId.1": allocResult.AllocationID})
	defer descResp2.Body.Close() //nolint:errcheck
	var descResult2 struct {
		Addresses []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"addressesSet>item"`
	}
	_ = xml.NewDecoder(descResp2.Body).Decode(&descResult2)
	if len(descResult2.Addresses) != 1 || descResult2.Addresses[0].InstanceID != "" {
		t.Errorf("expected cleared instanceId after disassociate, got %+v", descResult2.Addresses)
	}
}

func TestEC2_ElasticIP_ReleaseWhileAssociated(t *testing.T) {
	ts := newEC2TestServer(t)

	allocResp := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"})
	defer allocResp.Body.Close() //nolint:errcheck
	var allocResult struct {
		AllocationID string `xml:"allocationId"`
	}
	_ = xml.NewDecoder(allocResp.Body).Decode(&allocResult)

	// Launch and associate.
	runResp := ec2Request(t, ts, map[string]string{"Action": "RunInstances", "ImageId": "ami-test", "MinCount": "1", "MaxCount": "1"})
	defer runResp.Body.Close() //nolint:errcheck
	var runResult struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	_ = xml.NewDecoder(runResp.Body).Decode(&runResult)

	assocResp := ec2Request(t, ts, map[string]string{
		"Action":       "AssociateAddress",
		"AllocationId": allocResult.AllocationID,
		"InstanceId":   runResult.Instances[0].InstanceID,
	})
	assocResp.Body.Close() //nolint:errcheck

	// Release while associated — should fail with InvalidIPAddress.InUse.
	relResp := ec2Request(t, ts, map[string]string{"Action": "ReleaseAddress", "AllocationId": allocResult.AllocationID})
	defer relResp.Body.Close() //nolint:errcheck
	if relResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for release of associated EIP, got %d", relResp.StatusCode)
	}
}

func TestEC2_DescribeAddresses_Filter(t *testing.T) {
	ts := newEC2TestServer(t)

	// Allocate 2 EIPs.
	r1 := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress"})
	defer r1.Body.Close() //nolint:errcheck
	var a1 struct {
		AllocationID string `xml:"allocationId"`
	}
	_ = xml.NewDecoder(r1.Body).Decode(&a1)

	r2 := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress"})
	defer r2.Body.Close() //nolint:errcheck
	var a2 struct {
		AllocationID string `xml:"allocationId"`
	}
	_ = xml.NewDecoder(r2.Body).Decode(&a2)
	_ = a2 // second EIP exists but is not filtered for

	// Filter by first allocationId only.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeAddresses", "AllocationId.1": a1.AllocationID})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		Addresses []struct {
			AllocationID string `xml:"allocationId"`
		} `xml:"addressesSet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.Addresses) != 1 {
		t.Fatalf("expected 1 filtered EIP, got %d", len(descResult.Addresses))
	}
	if descResult.Addresses[0].AllocationID != a1.AllocationID {
		t.Errorf("filtered EIP %q, expected %q", descResult.Addresses[0].AllocationID, a1.AllocationID)
	}
}

func TestEC2_NatGateway_CreateDescribeDelete(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC and subnet.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.2.0.0/16"})
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)
	vpcID := vpcResult.Vpc.VpcID

	subResp := ec2Request(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpcID, "CidrBlock": "10.2.1.0/24"})
	defer subResp.Body.Close() //nolint:errcheck
	var subResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(subResp.Body).Decode(&subResult)
	subnetID := subResult.Subnet.SubnetID

	// Allocate EIP.
	eipResp := ec2Request(t, ts, map[string]string{"Action": "AllocateAddress"})
	defer eipResp.Body.Close() //nolint:errcheck
	var eipResult struct {
		AllocationID string `xml:"allocationId"`
	}
	_ = xml.NewDecoder(eipResp.Body).Decode(&eipResult)

	// Create NAT gateway.
	natResp := ec2Request(t, ts, map[string]string{
		"Action":       "CreateNatGateway",
		"SubnetId":     subnetID,
		"AllocationId": eipResult.AllocationID,
	})
	defer natResp.Body.Close() //nolint:errcheck
	if natResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateNatGateway: expected 200, got %d", natResp.StatusCode)
	}
	var natResult struct {
		NatGateway struct {
			NatGatewayID     string `xml:"natGatewayId"`
			SubnetID         string `xml:"subnetId"`
			VpcID            string `xml:"vpcId"`
			State            string `xml:"state"`
			ConnectivityType string `xml:"connectivityType"`
		} `xml:"natGateway"`
	}
	_ = xml.NewDecoder(natResp.Body).Decode(&natResult)
	natID := natResult.NatGateway.NatGatewayID
	if !strings.HasPrefix(natID, "nat-") {
		t.Errorf("natGatewayId %q must start with nat-", natID)
	}
	if natResult.NatGateway.SubnetID != subnetID {
		t.Errorf("subnetId %q, expected %q", natResult.NatGateway.SubnetID, subnetID)
	}
	if natResult.NatGateway.VpcID != vpcID {
		t.Errorf("vpcId %q, expected %q", natResult.NatGateway.VpcID, vpcID)
	}
	if natResult.NatGateway.State != "available" {
		t.Errorf("state %q, expected available", natResult.NatGateway.State)
	}

	// DescribeNatGateways.
	descResp := ec2Request(t, ts, map[string]string{"Action": "DescribeNatGateways", "NatGatewayId.1": natID})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		NatGateways []struct {
			NatGatewayID string `xml:"natGatewayId"`
		} `xml:"natGatewaySet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.NatGateways) != 1 || descResult.NatGateways[0].NatGatewayID != natID {
		t.Fatalf("DescribeNatGateways: expected 1 with id %q", natID)
	}

	// Delete NAT gateway.
	delResp := ec2Request(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": natID})
	defer delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteNatGateway: expected 200, got %d", delResp.StatusCode)
	}
	var delResult struct {
		NatGatewayID string `xml:"natGatewayId"`
		State        string `xml:"state"`
	}
	_ = xml.NewDecoder(delResp.Body).Decode(&delResult)
	if delResult.State != "deleted" {
		t.Errorf("state after delete %q, expected deleted", delResult.State)
	}
}

func TestEC2_NatGateway_Private(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC and subnet.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.3.0.0/16"})
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)

	subResp := ec2Request(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpcResult.Vpc.VpcID, "CidrBlock": "10.3.1.0/24"})
	defer subResp.Body.Close() //nolint:errcheck
	var subResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(subResp.Body).Decode(&subResult)

	// Create private NAT gateway (no AllocationId needed).
	natResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateNatGateway",
		"SubnetId":         subResult.Subnet.SubnetID,
		"ConnectivityType": "private",
	})
	defer natResp.Body.Close() //nolint:errcheck
	if natResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateNatGateway private: expected 200, got %d", natResp.StatusCode)
	}
	var natResult struct {
		NatGateway struct {
			ConnectivityType string `xml:"connectivityType"`
			Addresses        []struct {
				PublicIP string `xml:"publicIp"`
			} `xml:"natGatewayAddressSet>item"`
		} `xml:"natGateway"`
	}
	_ = xml.NewDecoder(natResp.Body).Decode(&natResult)
	if natResult.NatGateway.ConnectivityType != "private" {
		t.Errorf("connectivityType %q, expected private", natResult.NatGateway.ConnectivityType)
	}
	// Private NAT gateway should have no public IP.
	for _, addr := range natResult.NatGateway.Addresses {
		if addr.PublicIP != "" {
			t.Errorf("private NAT GW should have no publicIp, got %q", addr.PublicIP)
		}
	}
}

func TestEC2_NatGateway_Filter(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create VPC and subnet.
	vpcResp := ec2Request(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.4.0.0/16"})
	defer vpcResp.Body.Close() //nolint:errcheck
	var vpcResult struct {
		Vpc struct {
			VpcID string `xml:"vpcId"`
		} `xml:"vpc"`
	}
	_ = xml.NewDecoder(vpcResp.Body).Decode(&vpcResult)

	subResp := ec2Request(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpcResult.Vpc.VpcID, "CidrBlock": "10.4.1.0/24"})
	defer subResp.Body.Close() //nolint:errcheck
	var subResult struct {
		Subnet struct {
			SubnetID string `xml:"subnetId"`
		} `xml:"subnet"`
	}
	_ = xml.NewDecoder(subResp.Body).Decode(&subResult)

	// Create 2 private NAT gateways.
	for range 2 {
		r := ec2Request(t, ts, map[string]string{
			"Action":           "CreateNatGateway",
			"SubnetId":         subResult.Subnet.SubnetID,
			"ConnectivityType": "private",
		})
		r.Body.Close() //nolint:errcheck
	}

	// Create a third and delete it.
	r3 := ec2Request(t, ts, map[string]string{
		"Action":           "CreateNatGateway",
		"SubnetId":         subResult.Subnet.SubnetID,
		"ConnectivityType": "private",
	})
	defer r3.Body.Close() //nolint:errcheck
	var r3Result struct {
		NatGateway struct {
			NatGatewayID string `xml:"natGatewayId"`
		} `xml:"natGateway"`
	}
	_ = xml.NewDecoder(r3.Body).Decode(&r3Result)
	delR := ec2Request(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": r3Result.NatGateway.NatGatewayID})
	delR.Body.Close() //nolint:errcheck

	// Filter by state=available — should return 2.
	descResp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeNatGateways",
		"Filter.1.Name":    "state",
		"Filter.1.Value.1": "available",
	})
	defer descResp.Body.Close() //nolint:errcheck
	var descResult struct {
		NatGateways []struct {
			NatGatewayID string `xml:"natGatewayId"`
		} `xml:"natGatewaySet>item"`
	}
	_ = xml.NewDecoder(descResp.Body).Decode(&descResult)
	if len(descResult.NatGateways) != 2 {
		t.Fatalf("expected 2 available NAT gateways, got %d", len(descResult.NatGateways))
	}
}

// TestEC2_DescribeInstanceTypes verifies the pre-seeded catalog is returned.
func TestEC2_DescribeInstanceTypes(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstanceTypes"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		InstanceTypes []struct {
			InstanceType string `xml:"instanceType"`
			VCpuInfo     struct {
				DefaultVCpus int `xml:"defaultVCpus"`
			} `xml:"vCpuInfo"`
			MemoryInfo struct {
				SizeInMiB int `xml:"sizeInMiB"`
			} `xml:"memoryInfo"`
		} `xml:"instanceTypeSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode DescribeInstanceTypes: %v", err)
	}
	if len(result.InstanceTypes) < 8 {
		t.Fatalf("expected at least 8 instance types, got %d", len(result.InstanceTypes))
	}

	// Spot-check t3.micro entry.
	found := false
	for _, it := range result.InstanceTypes {
		if it.InstanceType == "t3.micro" {
			found = true
			if it.VCpuInfo.DefaultVCpus != 2 {
				t.Errorf("t3.micro: expected 2 vCPUs, got %d", it.VCpuInfo.DefaultVCpus)
			}
			if it.MemoryInfo.SizeInMiB != 1024 {
				t.Errorf("t3.micro: expected 1024 MiB, got %d", it.MemoryInfo.SizeInMiB)
			}
		}
	}
	if !found {
		t.Error("t3.micro not found in catalog")
	}
}

// TestEC2_DescribeInstanceTypes_Filter verifies filtering by InstanceType.N works.
func TestEC2_DescribeInstanceTypes_Filter(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "c5.xlarge",
		"InstanceType.2": "r5.xlarge",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		InstanceTypes []struct {
			InstanceType string `xml:"instanceType"`
		} `xml:"instanceTypeSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.InstanceTypes) != 2 {
		t.Fatalf("expected 2 instance types, got %d", len(result.InstanceTypes))
	}
	got := map[string]bool{}
	for _, it := range result.InstanceTypes {
		got[it.InstanceType] = true
	}
	if !got["c5.xlarge"] || !got["r5.xlarge"] {
		t.Errorf("expected c5.xlarge and r5.xlarge, got %v", got)
	}
}

// TestEC2_DescribeInstanceTypeOfferings verifies offerings are returned per AZ.
func TestEC2_DescribeInstanceTypeOfferings(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstanceTypeOfferings"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Offerings []struct {
			InstanceType string `xml:"instanceType"`
			Location     string `xml:"location"`
		} `xml:"instanceTypeOfferingSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 8 types * 3 AZs = 24 offerings.
	if len(result.Offerings) != 24 {
		t.Fatalf("expected 24 offerings (8 types * 3 AZs), got %d", len(result.Offerings))
	}
}

// TestEC2_DescribeInstanceTypeOfferings_LocationFilter verifies AZ filtering.
func TestEC2_DescribeInstanceTypeOfferings_LocationFilter(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeInstanceTypeOfferings",
		"Filter.1.Name":    "location",
		"Filter.1.Value.1": "us-east-1a",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Offerings []struct {
			Location string `xml:"location"`
		} `xml:"instanceTypeOfferingSet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 8 types * 1 AZ = 8 offerings.
	if len(result.Offerings) != 8 {
		t.Fatalf("expected 8 offerings for us-east-1a, got %d", len(result.Offerings))
	}
	for _, o := range result.Offerings {
		if o.Location != "us-east-1a" {
			t.Errorf("expected location us-east-1a, got %q", o.Location)
		}
	}
}

// TestEC2_DescribeSpotPriceHistory verifies the stub catalog prices.
func TestEC2_DescribeSpotPriceHistory(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":         "DescribeSpotPriceHistory",
		"InstanceType.1": "t3.micro",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Items []struct {
			InstanceType string `xml:"instanceType"`
			SpotPrice    string `xml:"spotPrice"`
			AZ           string `xml:"availabilityZone"`
		} `xml:"spotPriceHistorySet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 1 type * 3 AZs = 3 items.
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 spot price items (t3.micro * 3 AZs), got %d", len(result.Items))
	}
	for _, item := range result.Items {
		if item.InstanceType != "t3.micro" {
			t.Errorf("expected t3.micro, got %q", item.InstanceType)
		}
		if item.SpotPrice != "0.0042" {
			t.Errorf("expected price 0.0042, got %q", item.SpotPrice)
		}
	}
}

// TestEC2_DescribeSpotPriceHistory_AZFilter verifies AZ filtering.
func TestEC2_DescribeSpotPriceHistory_AZFilter(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSpotPriceHistory",
		"AvailabilityZone": "us-east-1b",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Items []struct {
			AZ string `xml:"availabilityZone"`
		} `xml:"spotPriceHistorySet>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 8 types * 1 AZ = 8 items.
	if len(result.Items) != 8 {
		t.Fatalf("expected 8 spot price items (8 types * us-east-1b), got %d", len(result.Items))
	}
	for _, item := range result.Items {
		if item.AZ != "us-east-1b" {
			t.Errorf("expected us-east-1b, got %q", item.AZ)
		}
	}
}

// TestEC2_DescribeRegions verifies the pre-seeded region list is returned.
func TestEC2_DescribeRegions(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeRegions"})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Regions []struct {
			RegionName  string `xml:"regionName"`
			OptInStatus string `xml:"optInStatus"`
		} `xml:"regionInfo>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode DescribeRegions: %v", err)
	}
	if len(result.Regions) < 1 {
		t.Fatal("expected at least one region")
	}
	found := false
	for _, r := range result.Regions {
		if r.RegionName == "us-east-1" {
			found = true
			if r.OptInStatus != "opt-in-not-required" {
				t.Errorf("us-east-1: expected opt-in-not-required, got %q", r.OptInStatus)
			}
		}
	}
	if !found {
		t.Error("us-east-1 not found in DescribeRegions response")
	}
}

// TestEC2_DescribeRegions_Filter verifies RegionName.N filtering.
func TestEC2_DescribeRegions_Filter(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":       "DescribeRegions",
		"RegionName.1": "us-east-1",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Regions []struct {
			RegionName string `xml:"regionName"`
		} `xml:"regionInfo>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Regions) != 1 {
		t.Fatalf("expected 1 region after filter, got %d", len(result.Regions))
	}
	if result.Regions[0].RegionName != "us-east-1" {
		t.Errorf("expected us-east-1, got %q", result.Regions[0].RegionName)
	}
}

// TestEC2Plugin_CreateDescribeDeleteLaunchTemplate verifies full launch template lifecycle.
func TestEC2Plugin_CreateDescribeDeleteLaunchTemplate(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create
	resp := ec2Request(t, ts, map[string]string{
		"Action":                               "CreateLaunchTemplate",
		"LaunchTemplateName":                   "my-template",
		"LaunchTemplateData.ImageId":           "ami-0abcdef1234567890",
		"LaunchTemplateData.InstanceType":      "t3.small",
		"LaunchTemplateData.KeyName":           "my-key",
		"LaunchTemplateData.SecurityGroupId.1": "sg-12345678",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create: expected 200, got %d", resp.StatusCode)
	}
	var createResult struct {
		LaunchTemplate struct {
			LaunchTemplateID   string `xml:"launchTemplateId"`
			LaunchTemplateName string `xml:"launchTemplateName"`
			DefaultVersionNum  int64  `xml:"defaultVersionNumber"`
			LatestVersionNum   int64  `xml:"latestVersionNumber"`
		} `xml:"launchTemplate"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&createResult); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	ltID := createResult.LaunchTemplate.LaunchTemplateID
	if ltID == "" {
		t.Fatal("expected non-empty launchTemplateId")
	}
	if createResult.LaunchTemplate.LaunchTemplateName != "my-template" {
		t.Errorf("expected my-template, got %q", createResult.LaunchTemplate.LaunchTemplateName)
	}
	if createResult.LaunchTemplate.DefaultVersionNum != 1 {
		t.Errorf("expected defaultVersionNumber=1, got %d", createResult.LaunchTemplate.DefaultVersionNum)
	}

	// Describe all
	resp2 := ec2Request(t, ts, map[string]string{"Action": "DescribeLaunchTemplates"})
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("describe: expected 200, got %d", resp2.StatusCode)
	}
	var descResult struct {
		Items []struct {
			LaunchTemplateID   string `xml:"launchTemplateId"`
			LaunchTemplateName string `xml:"launchTemplateName"`
		} `xml:"launchTemplates>item"`
	}
	if err := xml.NewDecoder(resp2.Body).Decode(&descResult); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	if len(descResult.Items) != 1 {
		t.Fatalf("expected 1 template, got %d", len(descResult.Items))
	}
	if descResult.Items[0].LaunchTemplateID != ltID {
		t.Errorf("expected %q, got %q", ltID, descResult.Items[0].LaunchTemplateID)
	}

	// Delete
	resp3 := ec2Request(t, ts, map[string]string{
		"Action":           "DeleteLaunchTemplate",
		"LaunchTemplateId": ltID,
	})
	defer resp3.Body.Close() //nolint:errcheck
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("delete: expected 200, got %d", resp3.StatusCode)
	}

	// Confirm deleted — DescribeLaunchTemplates should return empty.
	resp4 := ec2Request(t, ts, map[string]string{"Action": "DescribeLaunchTemplates"})
	defer resp4.Body.Close() //nolint:errcheck
	var afterDelete struct {
		Items []struct {
			LaunchTemplateID string `xml:"launchTemplateId"`
		} `xml:"launchTemplates>item"`
	}
	if err := xml.NewDecoder(resp4.Body).Decode(&afterDelete); err != nil {
		t.Fatalf("decode after-delete: %v", err)
	}
	if len(afterDelete.Items) != 0 {
		t.Fatalf("expected 0 templates after delete, got %d", len(afterDelete.Items))
	}
}

// TestEC2Plugin_LaunchTemplate_DuplicateNameError verifies that creating two templates
// with the same name returns an error.
func TestEC2Plugin_LaunchTemplate_DuplicateNameError(t *testing.T) {
	ts := newEC2TestServer(t)

	params := map[string]string{
		"Action":             "CreateLaunchTemplate",
		"LaunchTemplateName": "dup-template",
	}
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first create: expected 200, got %d", resp.StatusCode)
	}

	resp2 := ec2Request(t, ts, params)
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode == http.StatusOK {
		t.Fatal("second create with same name: expected error, got 200")
	}
}

// TestEC2Plugin_RunInstances_ViaLaunchTemplate verifies that RunInstances resolves
// ImageId and InstanceType from a launch template when not supplied directly.
func TestEC2Plugin_RunInstances_ViaLaunchTemplate(t *testing.T) {
	ts := newEC2TestServer(t)

	// Create a launch template with a specific image and instance type.
	resp := ec2Request(t, ts, map[string]string{
		"Action":                          "CreateLaunchTemplate",
		"LaunchTemplateName":              "run-tmpl",
		"LaunchTemplateData.ImageId":      "ami-launch-template-test",
		"LaunchTemplateData.InstanceType": "m5.large",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create template: %d", resp.StatusCode)
	}
	var cr struct {
		LaunchTemplate struct {
			LaunchTemplateID string `xml:"launchTemplateId"`
		} `xml:"launchTemplate"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	ltID := cr.LaunchTemplate.LaunchTemplateID

	// RunInstances using only the launch template ID (no ImageId).
	resp2 := ec2Request(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"LaunchTemplate.LaunchTemplateId": ltID,
	})
	defer resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("run instances: expected 200, got %d", resp2.StatusCode)
	}

	var runResult struct {
		Instances []struct {
			InstanceID   string `xml:"instanceId"`
			ImageID      string `xml:"imageId"`
			InstanceType string `xml:"instanceType"`
		} `xml:"instancesSet>item"`
	}
	if err := xml.NewDecoder(resp2.Body).Decode(&runResult); err != nil {
		t.Fatalf("decode run: %v", err)
	}
	if len(runResult.Instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(runResult.Instances))
	}
	if runResult.Instances[0].ImageID != "ami-launch-template-test" {
		t.Errorf("expected ami-launch-template-test, got %q", runResult.Instances[0].ImageID)
	}
	if runResult.Instances[0].InstanceType != "m5.large" {
		t.Errorf("expected m5.large, got %q", runResult.Instances[0].InstanceType)
	}
}

// TestEC2Plugin_DescribeLaunchTemplates_FilterByName verifies filtering by name.
func TestEC2Plugin_DescribeLaunchTemplates_FilterByName(t *testing.T) {
	ts := newEC2TestServer(t)

	for _, n := range []string{"alpha", "beta", "gamma"} {
		r := ec2Request(t, ts, map[string]string{
			"Action":             "CreateLaunchTemplate",
			"LaunchTemplateName": n,
		})
		r.Body.Close() //nolint:errcheck
	}

	resp := ec2Request(t, ts, map[string]string{
		"Action":               "DescribeLaunchTemplates",
		"LaunchTemplateName.1": "beta",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result struct {
		Items []struct {
			LaunchTemplateName string `xml:"launchTemplateName"`
		} `xml:"launchTemplates>item"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result.Items))
	}
	if result.Items[0].LaunchTemplateName != "beta" {
		t.Errorf("expected beta, got %q", result.Items[0].LaunchTemplateName)
	}
}

// --- EBS volume tests (#256) ---

func TestEC2_EBS_CreateDescribeDeleteVolume(t *testing.T) {
	ts := newEC2TestServer(t)

	// CreateVolume.
	resp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "20",
		"VolumeType":       "gp3",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "vol-")
	assert.Contains(t, string(body), "available")

	// Extract volume ID.
	start := strings.Index(string(body), "vol-")
	end := strings.IndexAny(string(body)[start:], "<") + start
	volID := string(body)[start:end]

	// DescribeVolumes by ID.
	resp2 := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeVolumes",
		"VolumeId.1": volID,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2, _ := io.ReadAll(resp2.Body)
	assert.Contains(t, string(body2), volID)

	// DeleteVolume.
	resp3 := ec2Request(t, ts, map[string]string{
		"Action":   "DeleteVolume",
		"VolumeId": volID,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// DescribeVolumes should now return empty.
	resp4 := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeVolumes",
		"VolumeId.1": volID,
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)
	body4, _ := io.ReadAll(resp4.Body)
	assert.NotContains(t, string(body4), volID)
}

func TestEC2_EBS_AttachDetachVolume(t *testing.T) {
	ts := newEC2TestServer(t)

	// Launch an instance to attach to.
	runResp := ec2Request(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-test",
		"MinCount": "1",
		"MaxCount": "1",
	})
	assert.Equal(t, http.StatusOK, runResp.StatusCode)
	runBody, _ := io.ReadAll(runResp.Body)
	iStart := strings.Index(string(runBody), "i-")
	iEnd := strings.IndexAny(string(runBody)[iStart:], "<") + iStart
	instanceID := string(runBody)[iStart:iEnd]

	// Create a volume.
	cvResp := ec2Request(t, ts, map[string]string{
		"Action":           "CreateVolume",
		"AvailabilityZone": "us-east-1a",
		"Size":             "10",
	})
	cvBody, _ := io.ReadAll(cvResp.Body)
	vStart := strings.Index(string(cvBody), "vol-")
	vEnd := strings.IndexAny(string(cvBody)[vStart:], "<") + vStart
	volID := string(cvBody)[vStart:vEnd]

	// AttachVolume.
	attResp := ec2Request(t, ts, map[string]string{
		"Action":     "AttachVolume",
		"VolumeId":   volID,
		"InstanceId": instanceID,
		"Device":     "/dev/xvdf",
	})
	assert.Equal(t, http.StatusOK, attResp.StatusCode)
	attBody, _ := io.ReadAll(attResp.Body)
	assert.Contains(t, string(attBody), "attached")

	// DescribeVolumes should show in-use.
	descResp := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeVolumes",
		"VolumeId.1": volID,
	})
	descBody, _ := io.ReadAll(descResp.Body)
	assert.Contains(t, string(descBody), "in-use")

	// DetachVolume.
	detResp := ec2Request(t, ts, map[string]string{
		"Action":   "DetachVolume",
		"VolumeId": volID,
	})
	assert.Equal(t, http.StatusOK, detResp.StatusCode)

	// Volume should be available again.
	descResp2 := ec2Request(t, ts, map[string]string{
		"Action":     "DescribeVolumes",
		"VolumeId.1": volID,
	})
	descBody2, _ := io.ReadAll(descResp2.Body)
	assert.Contains(t, string(descBody2), "available")
}

func TestEC2_DeleteSnapshot_Stub(t *testing.T) {
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{
		"Action":     "DeleteSnapshot",
		"SnapshotId": "snap-0123456789abcdef0",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
