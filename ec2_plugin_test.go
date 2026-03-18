package substrate_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
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
		VPC struct{ VPCID string `xml:"vpcId"` } `xml:"vpc"`
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
	var sgResult struct{ GroupID string `xml:"groupId"` }
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
		KeyPairs []struct{ KeyName string `xml:"keyName"` } `xml:"keySet>item"`
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
		"Action":           "ImportKeyPair",
		"KeyName":          "imported-key",
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
		Instances []struct{ InstanceID string `xml:"instanceId"` } `xml:"instancesSet>item"`
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
		Instances []struct{ InstanceID string `xml:"instanceId"` } `xml:"instancesSet>item"`
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
		Instances []struct{ InstanceID string `xml:"instanceId"` } `xml:"instancesSet>item"`
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
		Instances []struct{ InstanceID string `xml:"instanceId"` } `xml:"instancesSet>item"`
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
		Images []struct{ ImageID string `xml:"imageId"` } `xml:"imagesSet>item"`
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
		"Action":       "RunInstances",
		"ImageId":      "ami-1",
		"InstanceType": "t3.micro",
		"MinCount":     "1",
		"MaxCount":     "1",
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
		"Action":          "DescribeSecurityGroups",
		"Filter.1.Name":   "group-name",
		"Filter.1.Value.1": "canopy-default",
	})
	if n := countSGs(nameResp); n != 2 {
		t.Errorf("group-name filter: expected 2, got %d", n)
	}

	// Filter by group-name + vpc-id: should return exactly 1.
	bothResp := ec2Request(t, ts, map[string]string{
		"Action":          "DescribeSecurityGroups",
		"Filter.1.Name":   "group-name",
		"Filter.1.Value.1": "canopy-default",
		"Filter.2.Name":   "vpc-id",
		"Filter.2.Value.1": vpc1,
	})
	if n := countSGs(bothResp); n != 1 {
		t.Errorf("group-name+vpc-id filter: expected 1, got %d", n)
	}

	// Filter by group-name=canopy-default + vpc-id=vpc1: name must be canopy-default.
	checkResp := ec2Request(t, ts, map[string]string{
		"Action":          "DescribeSecurityGroups",
		"Filter.1.Name":   "group-name",
		"Filter.1.Value.1": "canopy-default",
		"Filter.2.Name":   "vpc-id",
		"Filter.2.Value.1": vpc1,
	})
	if name := firstSGName(checkResp); name != "canopy-default" {
		t.Errorf("expected group-name=canopy-default, got %q", name)
	}

	// Filter by group-id: should return exactly the requested SG.
	idResp := ec2Request(t, ts, map[string]string{
		"Action":          "DescribeSecurityGroups",
		"Filter.1.Name":   "group-id",
		"Filter.1.Value.1": sg1ID,
	})
	if n := countSGs(idResp); n != 1 {
		t.Errorf("group-id filter: expected 1, got %d", n)
	}

	// Filter by group-name that doesn't exist: empty result.
	emptyResp := ec2Request(t, ts, map[string]string{
		"Action":          "DescribeSecurityGroups",
		"Filter.1.Name":   "group-name",
		"Filter.1.Value.1": "nonexistent-sg",
	})
	if n := countSGs(emptyResp); n != 0 {
		t.Errorf("nonexistent group-name filter: expected 0, got %d", n)
	}
}
