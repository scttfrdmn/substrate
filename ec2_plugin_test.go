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
		"Action":      "CreateSecurityGroup",
		"GroupName":   "test-sg",
		"Description": "test security group",
		"VpcId":       vpcResult.Vpc.VpcID,
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
		"Action":      "CreateSecurityGroup",
		"GroupName":   "test-sg",
		"Description": "test sg",
		"VpcId":       vpcID,
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
		"Description": "egress test", "VpcId": vpcResult.VPC.VPCID,
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
