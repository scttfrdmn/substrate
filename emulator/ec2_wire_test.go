package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for EC2's records (#756).
//
// Every EC2 record in emulator/ec2_types.go declares AccountID and Region under wire-visible `json`
// tags, because the record is what MemoryStateManager snapshots and a replay reads back. None of
// them carries an `xml` tag, and none reaches a body: each of the plugin's ec2XMLResponse sites is
// handed a response struct declared for that operation, no XML-tagged field anywhere is typed as a
// record, and no record is embedded in a response or held behind an interface. So the projection
// already exists in code, as it did for Redshift, and what was missing was this file, the citation
// the projected inventory requires before it will accept a record.
//
// # Why the member list is longer than the other services'
//
// The account member is spelled three ways across the records: `account_id` on most, `accountId`
// on EC2KeyPair and `accountID` on EC2LaunchTemplate. A case fold reconciles the last two with the
// Go name but not the snake_case one, so that spelling is listed in its own right, and so is
// `ever_tagged`. The Go names are listed because they are what encoding/xml would emit for a record
// handed to it whole, the records declaring no `xml` tag; the `json` names are what a member copied
// out of the stored document would be called.
//
// # Why this asserts absence rather than exact membership
//
// emulator/rds_wire_test.go, the strongest form in the suite, requires a record's element to hold
// exactly the published members. EC2's record shapes answer a fraction of what their pages publish,
// so an exact expectation would pin those gaps and have to be rewritten by every PR that closes one.
// What holds regardless is that no element anywhere in an EC2 response names a bookkeeping member,
// which is the claim the inventory needs and the form emulator/redshift_wire_test.go uses.

// ec2WireClock is the instant every fixture below starts the simulated clock at.
//
// A seeded baseline rather than the time.Now() the other EC2 harnesses use, so nothing here reads
// the wall clock. No assertion below equates a timestamp: TimeController.Now advances from its
// baseline by the wall time elapsed since it was set. This file only walks element names.
var ec2WireClock = time.Unix(1700000000, 0).UTC()

// ec2WireAccount and ec2WireRegion scope every state key the plugin writes.
const (
	ec2WireAccount = "123456789012"
	ec2WireRegion  = "us-east-1"
)

// ec2BookkeepingMembers are the members EC2's records declare and no EC2 shape publishes, in every
// spelling a leak could take.
//
// Compared case-insensitively by ec2WireAssertNoBookkeepingMember, which is what lets `AccountID`
// stand for `accountId` and `accountID` too. It is an equality rather than a substring test, so the
// account appearing inside a published ownerId, or in an ARN, is not a collision; that is also why
// each assertion is on the element name and never on the body as a whole.
var ec2BookkeepingMembers = []string{"AccountID", "account_id", "Region", "EverTagged", "ever_tagged", "CreatedAt"}

// setupEC2WirePlugin returns the EC2 plugin, a request context and the state manager behind it.
//
// The state manager is handed back because half of what each test asserts is that the record keeps
// the members its responses drop, and a record is the only place either can be read from, neither
// having a published home to read it back through. The ID mint is set because every create draws
// from it.
func setupEC2WirePlugin(t *testing.T) (*emulator.EC2Plugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.EC2Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(ec2WireClock)},
	}), "emulator.EC2Plugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: ec2WireAccount,
		Region:    ec2WireRegion,
		RequestID: "req-ec2-wire",
		IDs:       emulator.NewIDMint("req-ec2-wire"),
	}, state
}

// ec2Wire issues one query-protocol action and returns the raw response body, failing the test on
// anything but 200.
//
// The raw bytes are the point of this file. Every other EC2 test decodes into a Go struct, which is
// exactly the step that hides an element the struct does not declare.
func ec2Wire(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext, action string, params map[string]string) []byte {
	t.Helper()
	full := map[string]string{"Action": action, "Version": "2016-11-15"}
	maps.Copy(full, params)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "ec2",
		Operation: action,
		Path:      "/",
		Params:    full,
	})
	require.NoError(t, err, "%s", action)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, resp.Body)
	return resp.Body
}

// ec2WireElement returns the text of the first element named name in body, failing the test if
// there is none.
//
// It is how each test reads the identifier a create minted, so a later case can address the
// resource without decoding the response into a struct.
func ec2WireElement(t *testing.T, action string, body []byte, name string) string {
	t.Helper()
	m := regexp.MustCompile(`<` + name + `>([^<]+)</` + name + `>`).FindSubmatch(body)
	require.NotNilf(t, m, "%s answered no <%s>: %s", action, name, body)
	return string(m[1])
}

// ec2WireAssertNoBookkeepingMember fails if any element in the response document is named for a
// bookkeeping member, at any depth, reporting the path so a failure names the element rather than
// the service.
//
// The whole document rather than a record's subtree, which is strictly stronger: a member rendered
// on the envelope rather than on the record would fail here and pass a subtree walk.
func ec2WireAssertNoBookkeepingMember(t *testing.T, action string, body []byte) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "%s: decode body: %s", action, body)
		switch el := tok.(type) {
		case xml.StartElement:
			path = append(path, el.Name.Local)
			for _, member := range ec2BookkeepingMembers {
				assert.Falsef(t, strings.EqualFold(el.Name.Local, member),
					"%s answered %s: %s is substrate's bookkeeping and no EC2 shape publishes it",
					action, strings.Join(path, "/"), member)
			}
		case xml.EndElement:
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
		}
	}
}

// ec2WireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func ec2WireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "ec2", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// ec2WireRequireScoped requires that the stored record carries both scope members, reading the
// account under accountMember because the records spell it three ways.
//
// This is the presence anchor the absence assertions need on the record side: an assertion that a
// response omits a member its record never held would pass without testing anything.
func ec2WireRequireScoped(t *testing.T, state emulator.StateManager, key, accountMember string) {
	t.Helper()
	record := ec2WireRecord(t, state, key)
	require.JSONEqf(t, `"`+ec2WireAccount+`"`, string(record[accountMember]),
		"%s must persist %s before an absence assertion on it means anything", key, accountMember)
	require.JSONEqf(t, `"`+ec2WireRegion+`"`, string(record["region"]),
		"%s must persist region before an absence assertion on it means anything", key)
}

// ec2WireKey builds the scoped state key the plugin stores a record under.
func ec2WireKey(prefix, id string) string {
	return prefix + ":" + ec2WireAccount + "/" + ec2WireRegion + "/" + id
}

// ec2WireCase is one action driven by one of the tests below. A non-nil held reuses a response the
// test already has rather than issuing the action a second time.
type ec2WireCase struct {
	action string
	params map[string]string
	held   []byte
	anchor string
}

// ec2WireRun drives each case as a subtest, in order: the presence anchor first, so a body that did
// not render the record fails as a missing anchor rather than passing as an absence, then the walk.
func ec2WireRun(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext, cases []ec2WireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = ec2Wire(t, p, ctx, tc.action, tc.params)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.action, tc.anchor)
			ec2WireAssertNoBookkeepingMember(t, tc.action, body)
		})
	}
}

// ec2WireReturn is the anchor of every action whose published response is a bare boolean.
const ec2WireReturn = "<return>true</return>"

// ec2WireVPC creates a VPC and returns its id, for the tests whose resource has to live in one.
func ec2WireVPC(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext) string {
	t.Helper()
	body := ec2Wire(t, p, ctx, "CreateVpc", map[string]string{"CidrBlock": "10.0.0.0/16"})
	return ec2WireElement(t, "CreateVpc", body, "vpcId")
}

// ec2WireSubnet creates a subnet in vpcID and returns its id.
func ec2WireSubnet(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext, vpcID string) string {
	t.Helper()
	body := ec2Wire(t, p, ctx, "CreateSubnet", map[string]string{
		"VpcId":            vpcID,
		"CidrBlock":        "10.0.1.0/24",
		"AvailabilityZone": ec2WireRegion + "a",
	})
	return ec2WireElement(t, "CreateSubnet", body, "subnetId")
}

func TestEC2Wire_VPCResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)

	created := ec2Wire(t, p, ctx, "CreateVpc", map[string]string{"CidrBlock": "10.0.0.0/16"})
	vpcID := ec2WireElement(t, "CreateVpc", created, "vpcId")
	ec2WireRequireScoped(t, state, ec2WireKey("vpc", vpcID), "account_id")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateVpc", held: created, anchor: "<vpcId>" + vpcID + "</vpcId>"},
		{action: "DescribeVpcs", params: map[string]string{"VpcId.1": vpcID},
			anchor: "<vpcId>" + vpcID + "</vpcId>"},
		// The published parameter name. The handler reads EnableDNSHostnames.Value, so this modify is
		// discarded (#1151); the response it answers is the same either way, which is all this asserts.
		{action: "ModifyVpcAttribute", params: map[string]string{"VpcId": vpcID, "EnableDnsHostnames.Value": "true"},
			anchor: ec2WireReturn},
		// Last: it removes the record every case above reads.
		{action: "DeleteVpc", params: map[string]string{"VpcId": vpcID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_SubnetResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)

	created := ec2Wire(t, p, ctx, "CreateSubnet", map[string]string{
		"VpcId":            vpcID,
		"CidrBlock":        "10.0.1.0/24",
		"AvailabilityZone": ec2WireRegion + "a",
	})
	subnetID := ec2WireElement(t, "CreateSubnet", created, "subnetId")
	ec2WireRequireScoped(t, state, ec2WireKey("subnet", subnetID), "account_id")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateSubnet", held: created, anchor: "<subnetId>" + subnetID + "</subnetId>"},
		{action: "DescribeSubnets", params: map[string]string{"SubnetId.1": subnetID},
			anchor: "<subnetId>" + subnetID + "</subnetId>"},
		// The published parameter name, which the handler does not read (#1151).
		{action: "ModifySubnetAttribute", params: map[string]string{"SubnetId": subnetID, "MapPublicIpOnLaunch.Value": "true"},
			anchor: ec2WireReturn},
		{action: "DeleteSubnet", params: map[string]string{"SubnetId": subnetID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_SecurityGroupResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)

	created := ec2Wire(t, p, ctx, "CreateSecurityGroup", map[string]string{
		"GroupName":        "wire-sg",
		"GroupDescription": "ec2 wire",
		"VpcId":            vpcID,
	})
	sgID := ec2WireElement(t, "CreateSecurityGroup", created, "groupId")
	ec2WireRequireScoped(t, state, ec2WireKey("sg", sgID), "account_id")

	rule := map[string]string{
		"GroupId":                           sgID,
		"IpPermissions.1.IpProtocol":        "tcp",
		"IpPermissions.1.FromPort":          "443",
		"IpPermissions.1.ToPort":            "443",
		"IpPermissions.1.IpRanges.1.CidrIp": "10.0.0.0/8",
	}
	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateSecurityGroup", held: created, anchor: "<groupId>" + sgID + "</groupId>"},
		{action: "AuthorizeSecurityGroupIngress", params: rule, anchor: ec2WireReturn},
		{action: "AuthorizeSecurityGroupEgress", params: rule, anchor: ec2WireReturn},
		// After both authorizes, so the describe renders a rule in each direction and the walk reaches
		// the permission elements rather than an empty group.
		{action: "DescribeSecurityGroups", params: map[string]string{"GroupId.1": sgID},
			anchor: "<groupId>" + sgID + "</groupId>"},
		{action: "RevokeSecurityGroupIngress", params: rule, anchor: ec2WireReturn},
		{action: "RevokeSecurityGroupEgress", params: rule, anchor: ec2WireReturn},
		{action: "DeleteSecurityGroup", params: map[string]string{"GroupId": sgID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_InternetGatewayResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)

	created := ec2Wire(t, p, ctx, "CreateInternetGateway", nil)
	igwID := ec2WireElement(t, "CreateInternetGateway", created, "internetGatewayId")
	ec2WireRequireScoped(t, state, ec2WireKey("igw", igwID), "account_id")

	attach := map[string]string{"InternetGatewayId": igwID, "VpcId": vpcID}
	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateInternetGateway", held: created, anchor: "<internetGatewayId>" + igwID + "</internetGatewayId>"},
		{action: "AttachInternetGateway", params: attach, anchor: ec2WireReturn},
		// Between attach and detach, so the describe renders the attachment set.
		{action: "DescribeInternetGateways", params: map[string]string{"InternetGatewayId.1": igwID},
			anchor: "<vpcId>" + vpcID + "</vpcId>"},
		{action: "DetachInternetGateway", params: attach, anchor: ec2WireReturn},
		{action: "DeleteInternetGateway", params: map[string]string{"InternetGatewayId": igwID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_RouteTableResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)
	subnetID := ec2WireSubnet(t, p, ctx, vpcID)
	igwBody := ec2Wire(t, p, ctx, "CreateInternetGateway", nil)
	igwID := ec2WireElement(t, "CreateInternetGateway", igwBody, "internetGatewayId")
	ec2Wire(t, p, ctx, "AttachInternetGateway", map[string]string{"InternetGatewayId": igwID, "VpcId": vpcID})

	created := ec2Wire(t, p, ctx, "CreateRouteTable", map[string]string{"VpcId": vpcID})
	rtbID := ec2WireElement(t, "CreateRouteTable", created, "routeTableId")
	ec2WireRequireScoped(t, state, ec2WireKey("rtb", rtbID), "account_id")

	// A second table for ReplaceRouteTableAssociation to move the association onto.
	otherBody := ec2Wire(t, p, ctx, "CreateRouteTable", map[string]string{"VpcId": vpcID})
	otherID := ec2WireElement(t, "CreateRouteTable", otherBody, "routeTableId")

	associated := ec2Wire(t, p, ctx, "AssociateRouteTable", map[string]string{"RouteTableId": rtbID, "SubnetId": subnetID})
	assocID := ec2WireElement(t, "AssociateRouteTable", associated, "associationId")

	replaced := ec2Wire(t, p, ctx, "ReplaceRouteTableAssociation", map[string]string{"AssociationId": assocID, "RouteTableId": otherID})
	newAssocID := ec2WireElement(t, "ReplaceRouteTableAssociation", replaced, "newAssociationId")

	route := map[string]string{"RouteTableId": rtbID, "DestinationCidrBlock": "0.0.0.0/0", "GatewayId": igwID}
	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateRouteTable", held: created, anchor: "<routeTableId>" + rtbID + "</routeTableId>"},
		{action: "AssociateRouteTable", held: associated, anchor: "<associationId>" + assocID + "</associationId>"},
		{action: "ReplaceRouteTableAssociation", held: replaced, anchor: "<newAssociationId>" + newAssocID + "</newAssociationId>"},
		{action: "CreateRoute", params: route, anchor: ec2WireReturn},
		{action: "ReplaceRoute", params: route, anchor: ec2WireReturn},
		// After the route exists, so the describe renders a route beside the local one.
		{action: "DescribeRouteTables", params: map[string]string{"RouteTableId.1": rtbID},
			anchor: "<routeTableId>" + rtbID + "</routeTableId>"},
		{action: "DeleteRoute", params: map[string]string{"RouteTableId": rtbID, "DestinationCidrBlock": "0.0.0.0/0"},
			anchor: ec2WireReturn},
		{action: "DisassociateRouteTable", params: map[string]string{"AssociationId": newAssocID}, anchor: ec2WireReturn},
		{action: "DeleteRouteTable", params: map[string]string{"RouteTableId": rtbID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_NATGatewayResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	vpcID := ec2WireVPC(t, p, ctx)
	subnetID := ec2WireSubnet(t, p, ctx, vpcID)
	eipBody := ec2Wire(t, p, ctx, "AllocateAddress", map[string]string{"Domain": "vpc"})
	allocationID := ec2WireElement(t, "AllocateAddress", eipBody, "allocationId")

	created := ec2Wire(t, p, ctx, "CreateNatGateway", map[string]string{"SubnetId": subnetID, "AllocationId": allocationID})
	natID := ec2WireElement(t, "CreateNatGateway", created, "natGatewayId")
	ec2WireRequireScoped(t, state, ec2WireKey("nat", natID), "account_id")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateNatGateway", held: created, anchor: "<natGatewayId>" + natID + "</natGatewayId>"},
		{action: "DescribeNatGateways", params: map[string]string{"NatGatewayId.1": natID},
			anchor: "<natGatewayId>" + natID + "</natGatewayId>"},
		{action: "DeleteNatGateway", params: map[string]string{"NatGatewayId": natID},
			anchor: "<natGatewayId>" + natID + "</natGatewayId>"},
	})
}

func TestEC2Wire_ElasticIPResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)

	// AssociateAddress needs something to associate with.
	run := ec2Wire(t, p, ctx, "RunInstances", map[string]string{
		"ImageId": ec2TestImage, "InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	instanceID := ec2WireElement(t, "RunInstances", run, "instanceId")

	created := ec2Wire(t, p, ctx, "AllocateAddress", map[string]string{"Domain": "vpc"})
	allocationID := ec2WireElement(t, "AllocateAddress", created, "allocationId")
	ec2WireRequireScoped(t, state, ec2WireKey("eip", allocationID), "account_id")

	associated := ec2Wire(t, p, ctx, "AssociateAddress", map[string]string{"AllocationId": allocationID, "InstanceId": instanceID})
	assocID := ec2WireElement(t, "AssociateAddress", associated, "associationId")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "AllocateAddress", held: created, anchor: "<allocationId>" + allocationID + "</allocationId>"},
		{action: "AssociateAddress", held: associated, anchor: "<associationId>" + assocID + "</associationId>"},
		// While associated, so the describe renders the association members too.
		{action: "DescribeAddresses", params: map[string]string{"AllocationId.1": allocationID},
			anchor: "<instanceId>" + instanceID + "</instanceId>"},
		{action: "DisassociateAddress", params: map[string]string{"AssociationId": assocID}, anchor: ec2WireReturn},
		{action: "ReleaseAddress", params: map[string]string{"AllocationId": allocationID}, anchor: ec2WireReturn},
	})
}

// ec2WireInstance launches one instance and returns its id, for the tests whose resource has to be
// attached to, or built from, a running one.
func ec2WireInstance(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext) string {
	t.Helper()
	body := ec2Wire(t, p, ctx, "RunInstances", map[string]string{
		"ImageId": ec2TestImage, "InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	return ec2WireElement(t, "RunInstances", body, "instanceId")
}

// ec2WireVolume creates an 8 GiB gp3 volume in the test Region's first zone and returns the body
// and the id.
func ec2WireVolume(t *testing.T, p *emulator.EC2Plugin, ctx *emulator.RequestContext) ([]byte, string) {
	t.Helper()
	body := ec2Wire(t, p, ctx, "CreateVolume", map[string]string{
		"AvailabilityZone": ec2WireRegion + "a", "Size": "8", "VolumeType": "gp3",
	})
	return body, ec2WireElement(t, "CreateVolume", body, "volumeId")
}

func TestEC2Wire_VolumeResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	instanceID := ec2WireInstance(t, p, ctx)

	created, volumeID := ec2WireVolume(t, p, ctx)
	ec2WireRequireScoped(t, state, ec2WireKey("volume", volumeID), "account_id")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateVolume", held: created, anchor: "<volumeId>" + volumeID + "</volumeId>"},
		{action: "AttachVolume", params: map[string]string{"VolumeId": volumeID, "InstanceId": instanceID, "Device": "/dev/sdf"},
			anchor: "<instanceId>" + instanceID + "</instanceId>"},
		// While attached, so the describe renders the attachment set.
		{action: "DescribeVolumes", params: map[string]string{"VolumeId.1": volumeID},
			anchor: "<instanceId>" + instanceID + "</instanceId>"},
		{action: "DetachVolume", params: map[string]string{"VolumeId": volumeID},
			anchor: "<volumeId>" + volumeID + "</volumeId>"},
		{action: "DeleteVolume", params: map[string]string{"VolumeId": volumeID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_SnapshotResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	instanceID := ec2WireInstance(t, p, ctx)
	_, volumeID := ec2WireVolume(t, p, ctx)

	created := ec2Wire(t, p, ctx, "CreateSnapshot", map[string]string{"VolumeId": volumeID, "Description": "ec2 wire"})
	snapshotID := ec2WireElement(t, "CreateSnapshot", created, "snapshotId")
	ec2WireRequireScoped(t, state, ec2WireKey("snapshot", snapshotID), "account_id")

	// CreateSnapshots and CopySnapshot each write a record of their own; both are driven because each
	// answers one.
	multi := ec2Wire(t, p, ctx, "CreateSnapshots", map[string]string{"InstanceSpecification.InstanceId": instanceID})
	multiID := ec2WireElement(t, "CreateSnapshots", multi, "snapshotId")
	ec2WireRequireScoped(t, state, ec2WireKey("snapshot", multiID), "account_id")

	copied := ec2Wire(t, p, ctx, "CopySnapshot", map[string]string{"SourceRegion": ec2WireRegion, "SourceSnapshotId": snapshotID})
	copyID := ec2WireElement(t, "CopySnapshot", copied, "snapshotId")
	ec2WireRequireScoped(t, state, ec2WireKey("snapshot", copyID), "account_id")

	attribute := map[string]string{"SnapshotId": snapshotID, "Attribute": "createVolumePermission"}
	grant := map[string]string{
		"SnapshotId":                          snapshotID,
		"Attribute":                           "createVolumePermission",
		"OperationType":                       "add",
		"CreateVolumePermission.Add.1.UserId": "210987654321",
	}
	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateSnapshot", held: created, anchor: "<snapshotId>" + snapshotID + "</snapshotId>"},
		{action: "CreateSnapshots", held: multi, anchor: "<snapshotId>" + multiID + "</snapshotId>"},
		{action: "CopySnapshot", held: copied, anchor: "<snapshotId>" + copyID + "</snapshotId>"},
		{action: "DescribeSnapshots", params: map[string]string{"SnapshotId.1": snapshotID, "SnapshotId.2": multiID, "SnapshotId.3": copyID},
			anchor: "<snapshotId>" + copyID + "</snapshotId>"},
		{action: "ModifySnapshotAttribute", params: grant, anchor: ec2WireReturn},
		// After the grant, so the describe renders a permission entry rather than an empty set.
		{action: "DescribeSnapshotAttribute", params: attribute, anchor: "<userId>210987654321</userId>"},
		{action: "ResetSnapshotAttribute", params: attribute, anchor: ec2WireReturn},
		{action: "DeleteSnapshot", params: map[string]string{"SnapshotId": snapshotID}, anchor: ec2WireReturn},
	})
}

func TestEC2Wire_ImageResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupEC2WirePlugin(t)
	instanceID := ec2WireInstance(t, p, ctx)

	// A created image rather than a bundled one: a bundled catalog entry is not a record this account
	// wrote, so it is not where the scope members are proved to exist.
	created := ec2Wire(t, p, ctx, "CreateImage", map[string]string{"InstanceId": instanceID, "Name": "wire-image"})
	imageID := ec2WireElement(t, "CreateImage", created, "imageId")
	ec2WireRequireScoped(t, state, ec2WireKey("image", imageID), "account_id")

	registered := ec2Wire(t, p, ctx, "RegisterImage", map[string]string{
		"Name": "wire-registered", "Architecture": "x86_64", "RootDeviceName": "/dev/xvda",
		"VirtualizationType": "hvm",
	})
	registeredID := ec2WireElement(t, "RegisterImage", registered, "imageId")
	ec2WireRequireScoped(t, state, ec2WireKey("image", registeredID), "account_id")

	ec2WireRun(t, p, ctx, []ec2WireCase{
		{action: "CreateImage", held: created, anchor: "<imageId>" + imageID + "</imageId>"},
		{action: "RegisterImage", held: registered, anchor: "<imageId>" + registeredID + "</imageId>"},
		{action: "DescribeImages", params: map[string]string{"ImageId.1": imageID, "ImageId.2": registeredID},
			anchor: "<imageId>" + registeredID + "</imageId>"},
		{action: "DeregisterImage", params: map[string]string{"ImageId": imageID}, anchor: ec2WireReturn},
	})
}
