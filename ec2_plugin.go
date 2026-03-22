package substrate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// EC2Plugin emulates the Amazon EC2 and VPC APIs using query protocol.
// It handles instance lifecycle operations (RunInstances, DescribeInstances,
// TerminateInstances, StopInstances, StartInstances) and VPC networking
// (CreateVpc, CreateSubnet, CreateSecurityGroup, InternetGateway, RouteTable,
// and related operations).
type EC2Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "ec2".
func (p *EC2Plugin) Name() string { return "ec2" }

// Initialize sets up the EC2Plugin with the provided configuration.
func (p *EC2Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for EC2Plugin.
func (p *EC2Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EC2 query-protocol request to the appropriate handler.
func (p *EC2Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	// Instance operations
	case "RunInstances":
		return p.runInstances(ctx, req)
	case "DescribeInstances":
		return p.describeInstances(ctx, req)
	case "TerminateInstances":
		return p.terminateInstances(ctx, req)
	case "StopInstances":
		return p.stopInstances(ctx, req)
	case "StartInstances":
		return p.startInstances(ctx, req)
	case "DescribeInstanceStatus":
		return p.describeInstanceStatus(ctx, req)
	// VPC operations
	case "CreateVpc":
		return p.createVPC(ctx, req)
	case "DescribeVpcs":
		return p.describeVPCs(ctx, req)
	case "DeleteVpc":
		return p.deleteVPC(ctx, req)
	case "CreateSubnet":
		return p.createSubnet(ctx, req)
	case "DescribeSubnets":
		return p.describeSubnets(ctx, req)
	case "DeleteSubnet":
		return p.deleteSubnet(ctx, req)
	case "CreateSecurityGroup":
		return p.createSecurityGroup(ctx, req)
	case "DescribeSecurityGroups":
		return p.describeSecurityGroups(ctx, req)
	case "DeleteSecurityGroup":
		return p.deleteSecurityGroup(ctx, req)
	case "AuthorizeSecurityGroupIngress":
		return p.authorizeSecurityGroupIngress(ctx, req)
	case "RevokeSecurityGroupIngress":
		return p.revokeSecurityGroupIngress(ctx, req)
	case "AuthorizeSecurityGroupEgress":
		return p.authorizeSecurityGroupEgress(ctx, req)
	case "RevokeSecurityGroupEgress":
		return p.revokeSecurityGroupEgress(ctx, req)
	case "CreateInternetGateway":
		return p.createInternetGateway(ctx, req)
	case "DescribeInternetGateways":
		return p.describeInternetGateways(ctx, req)
	case "AttachInternetGateway":
		return p.attachInternetGateway(ctx, req)
	case "DetachInternetGateway":
		return p.detachInternetGateway(ctx, req)
	case "DeleteInternetGateway":
		return p.deleteInternetGateway(ctx, req)
	case "CreateRouteTable":
		return p.createRouteTable(ctx, req)
	case "DescribeRouteTables":
		return p.describeRouteTables(ctx, req)
	case "AssociateRouteTable":
		return p.associateRouteTable(ctx, req)
	case "DisassociateRouteTable":
		return p.disassociateRouteTable(ctx, req)
	case "CreateRoute":
		return p.createRoute(ctx, req)
	case "DeleteRoute":
		return p.deleteRoute(ctx, req)
	case "DeleteRouteTable":
		return p.deleteRouteTable(ctx, req)
	// Instance management operations
	case "RebootInstances":
		return p.rebootInstances(ctx, req)
	case "CreateTags":
		return p.createTags(ctx, req)
	case "DeleteTags":
		return p.deleteTags(ctx, req)
	case "ModifyInstanceAttribute":
		return p.modifyInstanceAttribute(ctx, req)
	// Key pair operations
	case "CreateKeyPair":
		return p.createKeyPair(ctx, req)
	case "DescribeKeyPairs":
		return p.describeKeyPairs(ctx, req)
	case "DeleteKeyPair":
		return p.deleteKeyPair(ctx, req)
	case "ImportKeyPair":
		return p.importKeyPair(ctx, req)
	// AMI operations
	case "CreateImage":
		return p.createImage(ctx, req)
	case "DescribeImages":
		return p.describeImages(ctx, req)
	case "DeregisterImage":
		return p.deregisterImage(ctx, req)
	// Availability Zone operations
	case "DescribeAvailabilityZones":
		return p.describeAvailabilityZones(ctx, req)
	// Subnet/VPC attribute operations
	case "ModifySubnetAttribute":
		return p.modifySubnetAttribute(ctx, req)
	case "ModifyVpcAttribute":
		return p.modifyVpcAttribute(ctx, req)
	// Elastic IP operations
	case "AllocateAddress":
		return p.allocateAddress(ctx, req)
	case "AssociateAddress":
		return p.associateAddress(ctx, req)
	case "DisassociateAddress":
		return p.disassociateAddress(ctx, req)
	case "ReleaseAddress":
		return p.releaseAddress(ctx, req)
	case "DescribeAddresses":
		return p.describeAddresses(ctx, req)
	// NAT Gateway operations
	case "CreateNatGateway":
		return p.createNatGateway(ctx, req)
	case "DescribeNatGateways":
		return p.describeNatGateways(ctx, req)
	case "DeleteNatGateway":
		return p.deleteNatGateway(ctx, req)
	// Region operations
	case "DescribeRegions":
		return p.describeRegions(ctx, req)
	// Instance type and spot price operations
	case "DescribeInstanceTypes":
		return p.describeInstanceTypes(ctx, req)
	case "DescribeInstanceTypeOfferings":
		return p.describeInstanceTypeOfferings(ctx, req)
	case "DescribeSpotPriceHistory":
		return p.describeSpotPriceHistory(ctx, req)
	// Launch template operations
	case "CreateLaunchTemplate":
		return p.createLaunchTemplate(ctx, req)
	case "DescribeLaunchTemplates":
		return p.describeLaunchTemplates(ctx, req)
	case "DeleteLaunchTemplate":
		return p.deleteLaunchTemplate(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "EC2Plugin: unknown action " + action,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Instance operations ---

func (p *EC2Plugin) runInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	imageID := req.Params["ImageId"]
	instanceType := req.Params["InstanceType"]
	if instanceType == "" {
		instanceType = "t3.micro"
	}
	minCount, _ := strconv.Atoi(req.Params["MinCount"])
	if minCount <= 0 {
		minCount = 1
	}
	maxCount, _ := strconv.Atoi(req.Params["MaxCount"])
	if maxCount <= 0 {
		maxCount = minCount
	}

	keyName := req.Params["KeyName"]
	subnetID := req.Params["SubnetId"]
	sgID := req.Params["SecurityGroupId.1"]
	if sgID == "" {
		sgID = req.Params["SecurityGroupIds.1"]
	}

	// Resolve launch template parameters when no ImageId is provided directly.
	if imageID == "" {
		ltID := req.Params["LaunchTemplate.LaunchTemplateId"]
		ltName := req.Params["LaunchTemplate.LaunchTemplateName"]
		if lt := p.resolveLaunchTemplate(context.Background(), reqCtx, ltID, ltName); lt != nil {
			imageID = lt.LatestData.ImageID
			if instanceType == "t3.micro" && lt.LatestData.InstanceType != "" {
				instanceType = lt.LatestData.InstanceType
			}
			if keyName == "" {
				keyName = lt.LatestData.KeyName
			}
			if sgID == "" && len(lt.LatestData.SecurityGroupIDs) > 0 {
				sgID = lt.LatestData.SecurityGroupIDs[0]
			}
		}
	}

	// Auto-create default VPC/subnet if none specified.
	if subnetID == "" {
		vpc, subnet, err := p.ensureDefaultVPC(context.Background(), reqCtx)
		if err != nil {
			return nil, err
		}
		subnetID = subnet.SubnetID
		if sgID == "" {
			// Use the default security group.
			sgIDs, listErr := p.state.List(context.Background(), ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
			if listErr == nil && len(sgIDs) > 0 {
				data, getErr := p.state.Get(context.Background(), ec2Namespace, sgIDs[0])
				if getErr == nil && data != nil {
					var sg EC2SecurityGroup
					if json.Unmarshal(data, &sg) == nil && sg.VPCID == vpc.VPCID {
						sgID = sg.GroupID
					}
				}
			}
		}
	}

	// Extract tags for instances from TagSpecification.N (ResourceType=instance).
	var launchTags []EC2Tag
	for n := 1; ; n++ {
		rt := req.Params[fmt.Sprintf("TagSpecification.%d.ResourceType", n)]
		if rt == "" {
			break
		}
		if rt != "instance" {
			continue
		}
		for m := 1; ; m++ {
			key := req.Params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", n, m)]
			if key == "" {
				break
			}
			launchTags = append(launchTags, EC2Tag{
				Key:   key,
				Value: req.Params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Value", n, m)],
			})
		}
	}

	reservationID := generateReservationID()
	now := p.tc.Now().UTC().Format(time.RFC3339)
	var instances []EC2Instance

	for i := 0; i < maxCount; i++ {
		inst := EC2Instance{
			InstanceID:       generateEC2InstanceID(),
			ReservationID:    reservationID,
			ImageID:          imageID,
			InstanceType:     instanceType,
			State:            EC2InstanceState{Code: 16, Name: "running"},
			SubnetID:         subnetID,
			PrivateIPAddress: fmt.Sprintf("172.31.%d.%d", i+1, i+10),
			SecurityGroupIDs: filterEmpty([]string{sgID}),
			LaunchTime:       now,
			AccountID:        reqCtx.AccountID,
			Region:           reqCtx.Region,
			KeyName:          keyName,
			Tags:             launchTags,
		}

		// Look up VPCID from subnet and decide whether to assign a public IP.
		subnetData, _ := p.state.Get(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+subnetID)
		if subnetData != nil {
			var subnet EC2Subnet
			if json.Unmarshal(subnetData, &subnet) == nil {
				inst.VPCID = subnet.VPCID
				// Always set private DNS name.
				inst.PrivateDNSName = ec2PrivateDNSName(inst.PrivateIPAddress, reqCtx.Region)
				// Assign public IP for default subnets or subnets with MapPublicIPOnLaunch.
				if subnet.IsDefault || subnet.MapPublicIPOnLaunch {
					inst.PublicIPAddress = generatePublicIP(inst.InstanceID)
					inst.PublicDNSName = ec2PublicDNSName(inst.PublicIPAddress, reqCtx.Region)
				}
			}
		}

		data, err := json.Marshal(inst)
		if err != nil {
			return nil, fmt.Errorf("ec2 runInstances marshal: %w", err)
		}
		stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + inst.InstanceID
		if err := p.state.Put(context.Background(), ec2Namespace, stateKey, data); err != nil {
			return nil, fmt.Errorf("ec2 runInstances state.Put: %w", err)
		}
		// Update instance_ids list.
		if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "instance_ids", inst.InstanceID); err != nil {
			return nil, err
		}
		instances = append(instances, inst)
	}

	return p.runInstancesResponse(instances, reservationID, reqCtx)
}

func (p *EC2Plugin) runInstancesResponse(instances []EC2Instance, reservationID string, reqCtx *RequestContext) (*AWSResponse, error) {
	type ec2InstanceItem struct {
		InstanceID       string `xml:"instanceId"`
		ImageID          string `xml:"imageId"`
		InstanceType     string `xml:"instanceType"`
		LaunchTime       string `xml:"launchTime"`
		PrivateIPAddress string `xml:"privateIpAddress"`
		PublicIPAddress  string `xml:"publicIpAddress,omitempty"`
		PublicDNSName    string `xml:"dnsName,omitempty"`
		PrivateDNSName   string `xml:"privateDnsName,omitempty"`
		SubnetID         string `xml:"subnetId"`
		VpcID            string `xml:"vpcId"`
		KeyName          string `xml:"keyName,omitempty"`
		State            struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"instanceState"`
	}
	type response struct {
		XMLName       xml.Name          `xml:"RunInstancesResponse"`
		XMLNS         string            `xml:"xmlns,attr"`
		ReservationID string            `xml:"reservationId"`
		OwnerID       string            `xml:"ownerId"`
		Instances     []ec2InstanceItem `xml:"instancesSet>item"`
	}

	resp := response{
		XMLNS:         "http://ec2.amazonaws.com/doc/2016-11-15/",
		ReservationID: reservationID,
		OwnerID:       reqCtx.AccountID,
	}
	for _, inst := range instances {
		item := ec2InstanceItem{
			InstanceID:       inst.InstanceID,
			ImageID:          inst.ImageID,
			InstanceType:     inst.InstanceType,
			LaunchTime:       inst.LaunchTime,
			PrivateIPAddress: inst.PrivateIPAddress,
			PublicIPAddress:  inst.PublicIPAddress,
			PublicDNSName:    inst.PublicDNSName,
			PrivateDNSName:   inst.PrivateDNSName,
			SubnetID:         inst.SubnetID,
			VpcID:            inst.VPCID,
			KeyName:          inst.KeyName,
		}
		item.State.Code = inst.State.Code
		item.State.Name = inst.State.Name
		resp.Instances = append(resp.Instances, item)
	}

	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) describeInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	filterNames := extractIndexedParams(req.Params, "Filter.1.Value")
	filterKey := req.Params["Filter.1.Name"]

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInstances list: %w", err)
	}

	type ec2StateItem struct {
		Code int    `xml:"code"`
		Name string `xml:"name"`
	}
	type tagItem struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type ec2InstanceItem struct {
		InstanceID       string       `xml:"instanceId"`
		ImageID          string       `xml:"imageId"`
		InstanceType     string       `xml:"instanceType"`
		LaunchTime       string       `xml:"launchTime"`
		PrivateIPAddress string       `xml:"privateIpAddress"`
		PublicIPAddress  string       `xml:"publicIpAddress,omitempty"`
		PublicDNSName    string       `xml:"dnsName,omitempty"`
		PrivateDNSName   string       `xml:"privateDnsName,omitempty"`
		SubnetID         string       `xml:"subnetId"`
		VpcID            string       `xml:"vpcId"`
		KeyName          string       `xml:"keyName,omitempty"`
		State            ec2StateItem `xml:"instanceState"`
		Tags             []tagItem    `xml:"tagSet>item"`
	}
	type reservationItem struct {
		ReservationID string            `xml:"reservationId"`
		OwnerID       string            `xml:"ownerId"`
		Instances     []ec2InstanceItem `xml:"instancesSet>item"`
	}
	type response struct {
		XMLName      xml.Name          `xml:"DescribeInstancesResponse"`
		XMLNS        string            `xml:"xmlns,attr"`
		Reservations []reservationItem `xml:"reservationSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	resMap := make(map[string]*reservationItem)

	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		// Filter by IDs.
		if len(ids) > 0 && !containsStr(ids, inst.InstanceID) {
			continue
		}
		// Filter by state name.
		if filterKey == "instance-state-name" && len(filterNames) > 0 && !containsStr(filterNames, inst.State.Name) {
			continue
		}

		item := ec2InstanceItem{
			InstanceID:       inst.InstanceID,
			ImageID:          inst.ImageID,
			InstanceType:     inst.InstanceType,
			LaunchTime:       inst.LaunchTime,
			PrivateIPAddress: inst.PrivateIPAddress,
			PublicIPAddress:  inst.PublicIPAddress,
			PublicDNSName:    inst.PublicDNSName,
			PrivateDNSName:   inst.PrivateDNSName,
			SubnetID:         inst.SubnetID,
			VpcID:            inst.VPCID,
			KeyName:          inst.KeyName,
		}
		item.State.Code = inst.State.Code
		item.State.Name = inst.State.Name
		for _, t := range inst.Tags {
			item.Tags = append(item.Tags, tagItem{Key: t.Key, Value: t.Value}) //nolint:staticcheck
		}

		if _, ok := resMap[inst.ReservationID]; !ok {
			resMap[inst.ReservationID] = &reservationItem{
				ReservationID: inst.ReservationID,
				OwnerID:       reqCtx.AccountID,
			}
		}
		resMap[inst.ReservationID].Instances = append(resMap[inst.ReservationID].Instances, item)
	}

	for _, res := range resMap {
		resp.Reservations = append(resp.Reservations, *res)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) terminateInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"TerminateInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 48, Name: "terminated"}
		inst.TerminatedTime = p.tc.Now().UTC().Format(time.RFC3339)
		newData, _ := json.Marshal(inst)
		_ = p.state.Put(context.Background(), ec2Namespace, key, newData)

		sc := stateChange{InstanceID: id}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) stopInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"StopInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 80, Name: "stopped"}
		newData, _ := json.Marshal(inst)
		_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
		sc := stateChange{InstanceID: id}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) startInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"StartInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 16, Name: "running"}
		newData, _ := json.Marshal(inst)
		_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
		sc := stateChange{InstanceID: id}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) describeInstanceStatus(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type statusItem struct {
		InstanceID    string `xml:"instanceId"`
		InstanceState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"instanceState"`
	}
	type response struct {
		XMLName xml.Name     `xml:"DescribeInstanceStatusResponse"`
		XMLNS   string       `xml:"xmlns,attr"`
		Items   []statusItem `xml:"instanceStatusSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInstanceStatus: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, inst.InstanceID) {
			continue
		}
		si := statusItem{InstanceID: inst.InstanceID}
		si.InstanceState.Code = inst.State.Code
		si.InstanceState.Name = inst.State.Name
		resp.Items = append(resp.Items, si)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- VPC operations ---

func (p *EC2Plugin) createVPC(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	cidr := req.Params["CidrBlock"]
	if cidr == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CidrBlock is required", HTTPStatus: http.StatusBadRequest}
	}
	vpcID := generateVPCID()
	vpc := EC2VPC{
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		IsDefault:        false,
		State:            "available",
		EnableDNSSupport: true,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	data, err := json.Marshal(vpc)
	if err != nil {
		return nil, fmt.Errorf("ec2 createVpc marshal: %w", err)
	}
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createVpc state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "vpc_ids", vpcID); err != nil {
		return nil, err
	}
	// Create default route table for VPC.
	if _, err := p.createRouteTableForVPC(reqCtx, vpcID, cidr, true); err != nil {
		p.logger.Warn("ec2: failed to create default route table", "err", err)
	}

	type vpcItem struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		IsDefault bool   `xml:"isDefault"`
		State     string `xml:"vpcState"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateVpcResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Vpc     vpcItem  `xml:"vpc"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		Vpc:   vpcItem{VpcID: vpcID, CIDRBlock: cidr, State: "available"},
	})
}

func (p *EC2Plugin) describeVPCs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "VpcId")
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeVpcs: %w", err)
	}
	type vpcItem struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		IsDefault bool   `xml:"isDefault"`
		State     string `xml:"vpcState"`
	}
	type response struct {
		XMLName xml.Name  `xml:"DescribeVpcsResponse"`
		XMLNS   string    `xml:"xmlns,attr"`
		Vpcs    []vpcItem `xml:"vpcSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vpc EC2VPC
		if json.Unmarshal(data, &vpc) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, vpc.VPCID) {
			continue
		}
		resp.Vpcs = append(resp.Vpcs, vpcItem{VpcID: vpc.VPCID, CIDRBlock: vpc.CIDRBlock, IsDefault: vpc.IsDefault, State: vpc.State})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteVPC(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	if vpcID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "VpcId is required", HTTPStatus: http.StatusBadRequest}
	}
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteVpc: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteVpcResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createSubnet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	cidr := req.Params["CidrBlock"]
	az := req.Params["AvailabilityZone"]
	if az == "" {
		az = reqCtx.Region + "a"
	}
	subnetID := generateSubnetID()
	subnet := EC2Subnet{
		SubnetID:         subnetID,
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		AvailabilityZone: az,
		State:            "available",
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	data, err := json.Marshal(subnet)
	if err != nil {
		return nil, fmt.Errorf("ec2 createSubnet marshal: %w", err)
	}
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createSubnet state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "subnet_ids", subnetID); err != nil {
		return nil, err
	}
	type subnetItem struct {
		SubnetID         string `xml:"subnetId"`
		VpcID            string `xml:"vpcId"`
		CIDRBlock        string `xml:"cidrBlock"`
		AvailabilityZone string `xml:"availabilityZone"`
		State            string `xml:"state"`
	}
	type response struct {
		XMLName xml.Name   `xml:"CreateSubnetResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Subnet  subnetItem `xml:"subnet"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:  "http://ec2.amazonaws.com/doc/2016-11-15/",
		Subnet: subnetItem{SubnetID: subnetID, VpcID: vpcID, CIDRBlock: cidr, AvailabilityZone: az, State: "available"},
	})
}

func (p *EC2Plugin) describeSubnets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "SubnetId")
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSubnets: %w", err)
	}
	type subnetItem struct {
		SubnetID            string `xml:"subnetId"`
		VpcID               string `xml:"vpcId"`
		CIDRBlock           string `xml:"cidrBlock"`
		AvailabilityZone    string `xml:"availabilityZone"`
		State               string `xml:"state"`
		MapPublicIPOnLaunch bool   `xml:"mapPublicIpOnLaunch"`
	}
	type response struct {
		XMLName xml.Name     `xml:"DescribeSubnetsResponse"`
		XMLNS   string       `xml:"xmlns,attr"`
		Subnets []subnetItem `xml:"subnetSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var subnet EC2Subnet
		if json.Unmarshal(data, &subnet) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, subnet.SubnetID) {
			continue
		}
		resp.Subnets = append(resp.Subnets, subnetItem{
			SubnetID:            subnet.SubnetID,
			VpcID:               subnet.VPCID,
			CIDRBlock:           subnet.CIDRBlock,
			AvailabilityZone:    subnet.AvailabilityZone,
			State:               subnet.State,
			MapPublicIPOnLaunch: subnet.MapPublicIPOnLaunch,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteSubnet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	_ = p.state.Delete(context.Background(), ec2Namespace, key)
	type response struct {
		XMLName xml.Name `xml:"DeleteSubnetResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createSecurityGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	groupName := req.Params["GroupName"]
	description := req.Params["GroupDescription"]
	vpcID := req.Params["VpcId"]
	sgID := generateSGID()
	sg := EC2SecurityGroup{
		GroupID:     sgID,
		GroupName:   groupName,
		Description: description,
		VPCID:       vpcID,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		EgressRules: []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}},
	}
	data, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("ec2 createSecurityGroup marshal: %w", err)
	}
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createSecurityGroup state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "sg_ids", sgID); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"CreateSecurityGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		GroupID string   `xml:"groupId"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", GroupID: sgID, Return: true})
}

func (p *EC2Plugin) describeSecurityGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "GroupId")
	filters := extractEC2Filters(req.Params)
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSecurityGroups: %w", err)
	}
	type sgItem struct {
		GroupID     string `xml:"groupId"`
		GroupName   string `xml:"groupName"`
		Description string `xml:"groupDescription"`
		VpcID       string `xml:"vpcId"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeSecurityGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Groups  []sgItem `xml:"securityGroupInfo>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(data, &sg) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, sg.GroupID) {
			continue
		}
		if vals, ok := filters["group-name"]; ok && len(vals) > 0 && !containsStr(vals, sg.GroupName) {
			continue
		}
		if vals, ok := filters["vpc-id"]; ok && len(vals) > 0 && !containsStr(vals, sg.VPCID) {
			continue
		}
		if vals, ok := filters["group-id"]; ok && len(vals) > 0 && !containsStr(vals, sg.GroupID) {
			continue
		}
		resp.Groups = append(resp.Groups, sgItem{GroupID: sg.GroupID, GroupName: sg.GroupName, Description: sg.Description, VpcID: sg.VPCID})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteSecurityGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	sgID := req.Params["GroupId"]
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	_ = p.state.Delete(context.Background(), ec2Namespace, key)
	type response struct {
		XMLName xml.Name `xml:"DeleteSecurityGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) authorizeSecurityGroupIngress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "ingress", true)
}

func (p *EC2Plugin) revokeSecurityGroupIngress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "ingress", false)
}

func (p *EC2Plugin) authorizeSecurityGroupEgress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "egress", true)
}

func (p *EC2Plugin) revokeSecurityGroupEgress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "egress", false)
}

func (p *EC2Plugin) modifySGRules(reqCtx *RequestContext, req *AWSRequest, direction string, add bool) (*AWSResponse, error) {
	sgID := req.Params["GroupId"]
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidGroup.NotFound", Message: "Security group not found", HTTPStatus: http.StatusBadRequest}
	}
	var sg EC2SecurityGroup
	if unmarshalErr := json.Unmarshal(data, &sg); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifySGRules unmarshal: %w", unmarshalErr)
	}

	// Parse permission from params (simplified: IpPermissions.1.*)
	proto := req.Params["IpPermissions.1.IpProtocol"]
	if proto == "" {
		proto = req.Params["IpProtocol"]
	}
	fromPort, _ := strconv.Atoi(req.Params["IpPermissions.1.FromPort"])
	if fromPort == 0 {
		fromPort, _ = strconv.Atoi(req.Params["FromPort"])
	}
	toPort, _ := strconv.Atoi(req.Params["IpPermissions.1.ToPort"])
	if toPort == 0 {
		toPort, _ = strconv.Atoi(req.Params["ToPort"])
	}
	cidrRange := req.Params["IpPermissions.1.IpRanges.1.CidrIp"]
	if cidrRange == "" {
		cidrRange = req.Params["CidrIp"]
	}

	perm := EC2IPPermission{
		IPProtocol: proto,
		FromPort:   fromPort,
		ToPort:     toPort,
	}
	if cidrRange != "" {
		perm.IPRanges = []string{cidrRange}
	}

	if direction == "ingress" {
		if add {
			sg.IngressRules = append(sg.IngressRules, perm)
		} else {
			sg.IngressRules = removePerm(sg.IngressRules, perm)
		}
	} else {
		if add {
			sg.EgressRules = append(sg.EgressRules, perm)
		} else {
			sg.EgressRules = removePerm(sg.EgressRules, perm)
		}
	}

	newData, _ := json.Marshal(sg)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)

	opName := "AuthorizeSecurityGroupIngressResponse"
	if direction == "egress" && add {
		opName = "AuthorizeSecurityGroupEgressResponse"
	} else if direction == "ingress" && !add {
		opName = "RevokeSecurityGroupIngressResponse"
	} else if direction == "egress" && !add {
		opName = "RevokeSecurityGroupEgressResponse"
	}

	body, _ := xml.Marshal(struct {
		XMLName xml.Name `xml:"response"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
	// Use dynamic XML element name.
	xmlStr := strings.Replace(string(body), "<response ", "<"+opName+" ", 1)
	xmlStr = strings.Replace(xmlStr, "</response>", "</"+opName+">", 1)
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "text/xml; charset=UTF-8"}, Body: []byte(xmlStr)}, nil
}

func (p *EC2Plugin) createInternetGateway(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	igwID := generateIGWID()
	igw := EC2InternetGateway{
		InternetGatewayID: igwID,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}
	data, _ := json.Marshal(igw)
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createInternetGateway: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "igw_ids", igwID); err != nil {
		return nil, err
	}
	type igwItem struct {
		InternetGatewayID string `xml:"internetGatewayId"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		IGW     igwItem  `xml:"internetGateway"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", IGW: igwItem{igwID}})
}

func (p *EC2Plugin) describeInternetGateways(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InternetGatewayId")
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "igw:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInternetGateways: %w", err)
	}
	type igwItem struct {
		InternetGatewayID string `xml:"internetGatewayId"`
	}
	type response struct {
		XMLName xml.Name  `xml:"DescribeInternetGatewaysResponse"`
		XMLNS   string    `xml:"xmlns,attr"`
		IGWs    []igwItem `xml:"internetGatewaySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var igw EC2InternetGateway
		if json.Unmarshal(data, &igw) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, igw.InternetGatewayID) {
			continue
		}
		resp.IGWs = append(resp.IGWs, igwItem{igw.InternetGatewayID})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) attachInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	vpcID := req.Params["VpcId"]
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidInternetGatewayID.NotFound", Message: "Internet gateway not found", HTTPStatus: http.StatusBadRequest}
	}
	var igw EC2InternetGateway
	if unmarshalErr := json.Unmarshal(data, &igw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 attachInternetGateway unmarshal: %w", unmarshalErr)
	}
	igw.Attachments = append(igw.Attachments, EC2IGWAttachment{VPCID: vpcID, State: "available"})
	newData, _ := json.Marshal(igw)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"AttachInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) detachInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	vpcID := req.Params["VpcId"]
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidInternetGatewayID.NotFound", Message: "Internet gateway not found", HTTPStatus: http.StatusBadRequest}
	}
	var igw EC2InternetGateway
	if unmarshalErr := json.Unmarshal(data, &igw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 detachInternetGateway unmarshal: %w", unmarshalErr)
	}
	newAttachments := igw.Attachments[:0]
	for _, a := range igw.Attachments {
		if a.VPCID != vpcID {
			newAttachments = append(newAttachments, a)
		}
	}
	igw.Attachments = newAttachments
	newData, _ := json.Marshal(igw)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"DetachInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) deleteInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	_ = p.state.Delete(context.Background(), ec2Namespace, key)
	type response struct {
		XMLName xml.Name `xml:"DeleteInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	rtbID, err := p.createRouteTableForVPC(reqCtx, vpcID, "", false)
	if err != nil {
		return nil, err
	}
	type rtbItem struct {
		RouteTableID string `xml:"routeTableId"`
		VpcID        string `xml:"vpcId"`
	}
	type response struct {
		XMLName    xml.Name `xml:"CreateRouteTableResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		RouteTable rtbItem  `xml:"routeTable"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", RouteTable: rtbItem{rtbID, vpcID}})
}

func (p *EC2Plugin) createRouteTableForVPC(reqCtx *RequestContext, vpcID, localCIDR string, main bool) (string, error) {
	rtbID := generateRTBID()
	rtb := EC2RouteTable{
		RouteTableID: rtbID,
		VPCID:        vpcID,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	if localCIDR != "" {
		rtb.Routes = []EC2Route{{DestinationCIDR: localCIDR, GatewayID: "local", State: "active"}}
	}
	if main {
		rtb.Associations = []EC2RTAssociation{{AssociationID: generateAssociationID(), Main: true}}
	}
	data, _ := json.Marshal(rtb)
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return "", fmt.Errorf("ec2 createRouteTable state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "rtb_ids", rtbID); err != nil {
		return "", err
	}
	return rtbID, nil
}

func (p *EC2Plugin) describeRouteTables(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "RouteTableId")
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "rtb:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeRouteTables: %w", err)
	}
	type rtbItem struct {
		RouteTableID string `xml:"routeTableId"`
		VpcID        string `xml:"vpcId"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"DescribeRouteTablesResponse"`
		XMLNS       string    `xml:"xmlns,attr"`
		RouteTables []rtbItem `xml:"routeTableSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var rtb EC2RouteTable
		if json.Unmarshal(data, &rtb) != nil {
			continue
		}
		if len(ids) > 0 && !containsStr(ids, rtb.RouteTableID) {
			continue
		}
		resp.RouteTables = append(resp.RouteTables, rtbItem{rtb.RouteTableID, rtb.VPCID})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) associateRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	subnetID := req.Params["SubnetId"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidRouteTableID.NotFound", Message: "Route table not found", HTTPStatus: http.StatusBadRequest}
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 associateRouteTable unmarshal: %w", unmarshalErr)
	}
	assocID := generateAssociationID()
	rtb.Associations = append(rtb.Associations, EC2RTAssociation{AssociationID: assocID, SubnetID: subnetID})
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName       xml.Name `xml:"AssociateRouteTableResponse"`
		XMLNS         string   `xml:"xmlns,attr"`
		AssociationID string   `xml:"associationId"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", AssociationID: assocID})
}

func (p *EC2Plugin) disassociateRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID := req.Params["AssociationId"]
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "rtb:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 disassociateRouteTable list: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var rtb EC2RouteTable
		if json.Unmarshal(data, &rtb) != nil {
			continue
		}
		newAssoc := rtb.Associations[:0]
		found := false
		for _, a := range rtb.Associations {
			if a.AssociationID == assocID {
				found = true
			} else {
				newAssoc = append(newAssoc, a)
			}
		}
		if found {
			rtb.Associations = newAssoc
			newData, _ := json.Marshal(rtb)
			_ = p.state.Put(context.Background(), ec2Namespace, k, newData)
			break
		}
	}
	type response struct {
		XMLName xml.Name `xml:"DisassociateRouteTableResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createRoute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	destCIDR := req.Params["DestinationCidrBlock"]
	gwID := req.Params["GatewayId"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidRouteTableID.NotFound", Message: "Route table not found", HTTPStatus: http.StatusBadRequest}
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 createRoute unmarshal: %w", unmarshalErr)
	}
	rtb.Routes = append(rtb.Routes, EC2Route{DestinationCIDR: destCIDR, GatewayID: gwID, State: "active"})
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"CreateRouteResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) deleteRoute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	destCIDR := req.Params["DestinationCidrBlock"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidRouteTableID.NotFound", Message: "Route table not found", HTTPStatus: http.StatusBadRequest}
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 deleteRoute unmarshal: %w", unmarshalErr)
	}
	newRoutes := rtb.Routes[:0]
	for _, r := range rtb.Routes {
		if r.DestinationCIDR != destCIDR {
			newRoutes = append(newRoutes, r)
		}
	}
	rtb.Routes = newRoutes
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"DeleteRouteResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) deleteRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	_ = p.state.Delete(context.Background(), ec2Namespace, key)
	type response struct {
		XMLName xml.Name `xml:"DeleteRouteTableResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// --- Instance management operations ---

// rebootInstances handles RebootInstances — a no-op in the emulator.
func (p *EC2Plugin) rebootInstances(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type response struct {
		XMLName xml.Name `xml:"RebootInstancesResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// createTags handles CreateTags — applies key-value tags to one or more EC2 resources.
func (p *EC2Plugin) createTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceIDs := extractIndexedParams(req.Params, "ResourceId")
	tags := extractEC2Tags(req.Params)
	for _, id := range resourceIDs {
		if err := p.applyTagsToResource(reqCtx, id, tags, false); err != nil {
			return nil, err
		}
	}
	type response struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// deleteTags handles DeleteTags — removes tags from one or more EC2 resources.
func (p *EC2Plugin) deleteTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceIDs := extractIndexedParams(req.Params, "ResourceId")
	tags := extractEC2Tags(req.Params)
	for _, id := range resourceIDs {
		if err := p.applyTagsToResource(reqCtx, id, tags, true); err != nil {
			return nil, err
		}
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// modifyInstanceAttribute handles ModifyInstanceAttribute — supports InstanceType changes.
func (p *EC2Plugin) modifyInstanceAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instID := req.Params["InstanceId"]
	if instID == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "InstanceId is required", HTTPStatus: http.StatusBadRequest}
	}

	stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instID
	data, err := p.state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "InvalidInstanceID.NotFound", Message: "Instance not found: " + instID, HTTPStatus: http.StatusBadRequest}
	}

	var inst EC2Instance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute unmarshal: %w", err)
	}

	// Apply supported attribute modifications.
	if v := req.Params["InstanceType.Value"]; v != "" {
		inst.InstanceType = v
	}

	updated, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"ModifyInstanceAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// applyTagsToResource loads the EC2 resource identified by id, merges or
// removes the provided tags, and saves the updated resource back to state.
// When remove is true, matching tag keys are deleted; otherwise tags are upserted.
func (p *EC2Plugin) applyTagsToResource(reqCtx *RequestContext, id string, tags []EC2Tag, remove bool) error {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	var stateKey string
	switch {
	case strings.HasPrefix(id, "i-"):
		stateKey = "instance:" + scope + "/" + id
	case strings.HasPrefix(id, "vpc-"):
		stateKey = "vpc:" + scope + "/" + id
	case strings.HasPrefix(id, "subnet-"):
		stateKey = "subnet:" + scope + "/" + id
	case strings.HasPrefix(id, "sg-"):
		stateKey = "sg:" + scope + "/" + id
	case strings.HasPrefix(id, "igw-"):
		stateKey = "igw:" + scope + "/" + id
	case strings.HasPrefix(id, "rtb-"):
		stateKey = "rtb:" + scope + "/" + id
	case strings.HasPrefix(id, "eipalloc-"):
		stateKey = "eip:" + scope + "/" + id
	case strings.HasPrefix(id, "nat-"):
		stateKey = "nat:" + scope + "/" + id
	default:
		// Unknown resource type — silently ignore (matches AWS behavior).
		return nil
	}

	data, err := p.state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil || data == nil {
		return nil //nolint:nilerr // Resource not found — ignore (matches AWS behavior).
	}

	// Use a generic map to avoid switching on concrete struct types.
	var resource map[string]json.RawMessage
	if err := json.Unmarshal(data, &resource); err != nil {
		return fmt.Errorf("ec2 applyTagsToResource unmarshal %s: %w", id, err)
	}

	// Load existing tags.
	var existing []EC2Tag
	if raw, ok := resource["tags"]; ok {
		_ = json.Unmarshal(raw, &existing)
	}

	if remove {
		// Build set of keys to remove.
		removeKeys := make(map[string]bool, len(tags))
		for _, t := range tags {
			removeKeys[t.Key] = true
		}
		filtered := existing[:0]
		for _, t := range existing {
			if !removeKeys[t.Key] {
				filtered = append(filtered, t)
			}
		}
		existing = filtered
	} else {
		// Upsert: update matching keys, append new ones.
		idx := make(map[string]int, len(existing))
		for i, t := range existing {
			idx[t.Key] = i
		}
		for _, t := range tags {
			if i, ok := idx[t.Key]; ok {
				existing[i].Value = t.Value
			} else {
				existing = append(existing, t)
			}
		}
	}

	tagsRaw, _ := json.Marshal(existing)
	resource["tags"] = json.RawMessage(tagsRaw)

	updated, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("ec2 applyTagsToResource marshal %s: %w", id, err)
	}
	return p.state.Put(context.Background(), ec2Namespace, stateKey, updated)
}

// extractEC2Tags extracts Tag.N.Key / Tag.N.Value pairs from query params.
func extractEC2Tags(params map[string]string) []EC2Tag {
	var tags []EC2Tag
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Tag.%d.Key", i)]
		if key == "" {
			break
		}
		tags = append(tags, EC2Tag{Key: key, Value: params[fmt.Sprintf("Tag.%d.Value", i)]})
	}
	return tags
}

// --- Key pair operations ---

func (p *EC2Plugin) createKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check for duplicate.
	existing, _ := p.state.Get(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
	if existing != nil {
		return nil, &AWSError{Code: "InvalidKeyPair.Duplicate", Message: "The keypair '" + name + "' already exists.", HTTPStatus: http.StatusBadRequest}
	}

	// Generate an EC P-256 key pair — fast and produces a compact PEM.
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair generate: %w", err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair marshal private key: %w", err)
	}
	keyMaterial := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}))

	pubDER, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair marshal public key: %w", err)
	}
	fp := ec2KeyFingerprint(pubDER)

	keyType := req.Params["KeyType"]
	if keyType == "" {
		keyType = "rsa"
	}

	kp := EC2KeyPair{
		KeyPairID:   generateKeyPairID(),
		KeyName:     name,
		Fingerprint: fp,
		KeyType:     keyType,
		CreatedAt:   p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	data, _ := json.Marshal(kp)
	if err := p.state.Put(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name, data); err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "keypair_names", name); err != nil {
		return nil, err
	}

	type response struct {
		XMLName        xml.Name `xml:"CreateKeyPairResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		KeyPairID      string   `xml:"keyPairId"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyType        string   `xml:"keyType"`
		KeyMaterial    string   `xml:"keyMaterial"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.Fingerprint,
		KeyType:        kp.KeyType,
		KeyMaterial:    keyMaterial,
	})
}

func (p *EC2Plugin) describeKeyPairs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterNames := extractIndexedParams(req.Params, "KeyName")

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeKeyPairs list: %w", err)
	}

	type kpItem struct {
		KeyPairID      string `xml:"keyPairId"`
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
		KeyType        string `xml:"keyType"`
		CreateTime     string `xml:"createTime,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:"DescribeKeyPairsResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		KeyPairs []kpItem `xml:"keySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var kp EC2KeyPair
		if json.Unmarshal(data, &kp) != nil {
			continue
		}
		if len(filterNames) > 0 && !containsStr(filterNames, kp.KeyName) {
			continue
		}
		resp.KeyPairs = append(resp.KeyPairs, kpItem{
			KeyPairID:      kp.KeyPairID,
			KeyName:        kp.KeyName,
			KeyFingerprint: kp.Fingerprint,
			KeyType:        kp.KeyType,
			CreateTime:     kp.CreatedAt,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	if name == "" {
		name = req.Params["KeyPairId"]
	}
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName or KeyPairId is required", HTTPStatus: http.StatusBadRequest}
	}

	// Support lookup by KeyPairId: scan for matching pair.
	stateKey := "keypair:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + name
	if strings.HasPrefix(name, "key-") {
		allKeys, _ := p.state.List(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
		for _, k := range allKeys {
			data, _ := p.state.Get(context.Background(), ec2Namespace, k)
			if data == nil {
				continue
			}
			var kp EC2KeyPair
			if json.Unmarshal(data, &kp) == nil && kp.KeyPairID == name {
				stateKey = k
				break
			}
		}
	}

	if err := p.state.Delete(context.Background(), ec2Namespace, stateKey); err != nil {
		return nil, fmt.Errorf("ec2 deleteKeyPair: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"DeleteKeyPairResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) importKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	pubKeyMaterial := req.Params["PublicKeyMaterial"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName is required", HTTPStatus: http.StatusBadRequest}
	}
	if pubKeyMaterial == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "PublicKeyMaterial is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check for duplicate.
	existing, _ := p.state.Get(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
	if existing != nil {
		return nil, &AWSError{Code: "InvalidKeyPair.Duplicate", Message: "The keypair '" + name + "' already exists.", HTTPStatus: http.StatusBadRequest}
	}

	// Decode the public key material and compute a fingerprint.
	pubBytes, err := base64.StdEncoding.DecodeString(pubKeyMaterial)
	if err != nil {
		// Treat as raw bytes if not base64.
		pubBytes = []byte(pubKeyMaterial)
	}
	fp := ec2KeyFingerprint(pubBytes)

	// Infer key type from SSH public key prefix.
	keyType := "rsa"
	pubStr := string(pubBytes)
	switch {
	case strings.HasPrefix(pubStr, "ssh-ed25519"):
		keyType = "ed25519"
	case strings.HasPrefix(pubStr, "ecdsa-"):
		keyType = "rsa" // EC keys are treated as rsa in EC2 API
	}

	kp := EC2KeyPair{
		KeyPairID:   generateKeyPairID(),
		KeyName:     name,
		Fingerprint: fp,
		KeyType:     keyType,
		CreatedAt:   p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	data, _ := json.Marshal(kp)
	if err := p.state.Put(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name, data); err != nil {
		return nil, fmt.Errorf("ec2 importKeyPair put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "keypair_names", name); err != nil {
		return nil, err
	}

	type response struct {
		XMLName        xml.Name `xml:"ImportKeyPairResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		KeyPairID      string   `xml:"keyPairId"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyType        string   `xml:"keyType"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.Fingerprint,
		KeyType:        kp.KeyType,
	})
}

// ec2KeyFingerprint returns a colon-separated SHA-256 hex fingerprint of
// the provided DER-encoded key bytes, matching the format AWS uses for
// EC2 key pairs.
func ec2KeyFingerprint(derBytes []byte) string {
	sum := sha256.Sum256(derBytes)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02x", b)
	}
	return strings.Join(parts, ":")
}

// --- Helper functions ---

// ensureDefaultVPC creates the default VPC and subnet if they don't already
// exist for the given account/region.
func (p *EC2Plugin) ensureDefaultVPC(ctx context.Context, reqCtx *RequestContext) (*EC2VPC, *EC2Subnet, error) {
	// Check for existing default VPC.
	vpcKeys, err := p.state.List(ctx, ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, nil, fmt.Errorf("ec2 ensureDefaultVPC list vpcs: %w", err)
	}
	for _, k := range vpcKeys {
		data, getErr := p.state.Get(ctx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vpc EC2VPC
		if json.Unmarshal(data, &vpc) == nil && vpc.IsDefault {
			// Found existing default VPC. Find its default subnet.
			subnetKeys, listErr := p.state.List(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
			if listErr != nil {
				return nil, nil, fmt.Errorf("ec2 ensureDefaultVPC list subnets: %w", listErr)
			}
			for _, sk := range subnetKeys {
				sdata, sErr := p.state.Get(ctx, ec2Namespace, sk)
				if sErr != nil || sdata == nil {
					continue
				}
				var subnet EC2Subnet
				if json.Unmarshal(sdata, &subnet) == nil && subnet.VPCID == vpc.VPCID && subnet.IsDefault {
					return &vpc, &subnet, nil
				}
			}
			// No default subnet found; create one.
			subnet, createErr := p.createDefaultSubnet(ctx, reqCtx, &vpc)
			if createErr != nil {
				return nil, nil, createErr
			}
			return &vpc, subnet, nil
		}
	}

	// Create default VPC.
	vpcID := generateVPCID()
	vpc := EC2VPC{
		VPCID:              vpcID,
		CIDRBlock:          "172.31.0.0/16",
		IsDefault:          true,
		State:              "available",
		EnableDNSSupport:   true,
		EnableDNSHostnames: true,
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
	}
	vpcData, _ := json.Marshal(vpc)
	vpcKey := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	if err := p.state.Put(ctx, ec2Namespace, vpcKey, vpcData); err != nil {
		return nil, nil, fmt.Errorf("ec2 ensureDefaultVPC create vpc: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "vpc_ids", vpcID); err != nil {
		return nil, nil, err
	}

	// Create default security group.
	sgID := generateSGID()
	sg := EC2SecurityGroup{
		GroupID:     sgID,
		GroupName:   "default",
		Description: "default VPC security group",
		VPCID:       vpcID,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		EgressRules: []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}},
	}
	sgData, _ := json.Marshal(sg)
	sgKey := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	if err := p.state.Put(ctx, ec2Namespace, sgKey, sgData); err != nil {
		p.logger.Warn("ec2: failed to create default sg", "err", err)
	}
	_ = p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "sg_ids", sgID)

	subnet, err := p.createDefaultSubnet(ctx, reqCtx, &vpc)
	if err != nil {
		return nil, nil, err
	}
	return &vpc, subnet, nil
}

func (p *EC2Plugin) createDefaultSubnet(ctx context.Context, reqCtx *RequestContext, vpc *EC2VPC) (*EC2Subnet, error) {
	subnetID := generateSubnetID()
	subnet := EC2Subnet{
		SubnetID:            subnetID,
		VPCID:               vpc.VPCID,
		CIDRBlock:           "172.31.0.0/20",
		AvailabilityZone:    reqCtx.Region + "a",
		IsDefault:           true,
		MapPublicIPOnLaunch: true,
		State:               "available",
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}
	data, _ := json.Marshal(subnet)
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	if err := p.state.Put(ctx, ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createDefaultSubnet: %w", err)
	}
	_ = p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "subnet_ids", subnetID)
	return &subnet, nil
}

// appendToList loads the JSON list at listKey, appends id, and saves it back.
func (p *EC2Plugin) appendToList(scope, listName, id string) error {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return fmt.Errorf("ec2 appendToList get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), ec2Namespace, key, newData)
}

// generatePublicIP returns a deterministic synthetic public IPv4 address in
// Amazon's 54.0.0.0/8 range, derived from the instance ID via FNV-32a hash.
func generatePublicIP(instanceID string) string {
	h := fnv.New32a()
	h.Write([]byte(instanceID))
	n := h.Sum32()
	return fmt.Sprintf("54.%d.%d.%d", (n>>16)&0xFF, (n>>8)&0xFF, n&0xFF)
}

// ec2PublicDNSName builds the AWS-format public DNS hostname for a public IP.
func ec2PublicDNSName(ip, region string) string {
	dashed := strings.ReplaceAll(ip, ".", "-")
	if region == "us-east-1" {
		return fmt.Sprintf("ec2-%s.compute-1.amazonaws.com", dashed)
	}
	return fmt.Sprintf("ec2-%s.%s.compute.amazonaws.com", dashed, region)
}

// ec2PrivateDNSName builds the AWS-format private DNS hostname for a private IP.
func ec2PrivateDNSName(ip, region string) string {
	dashed := strings.ReplaceAll(ip, ".", "-")
	return fmt.Sprintf("ip-%s.%s.compute.internal", dashed, region)
}

// ec2XMLResponse serializes v to XML and returns an AWSResponse.
func ec2XMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ec2 xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// extractIndexedParams returns values from params where keys match the pattern
// "prefix.1", "prefix.2", ... stopping at the first missing index.
func extractIndexedParams(params map[string]string, prefix string) []string {
	var vals []string
	for i := 1; ; i++ {
		v, ok := params[prefix+"."+strconv.Itoa(i)]
		if !ok {
			// Also check without index for single-value params.
			if i == 1 {
				if v2, ok2 := params[prefix]; ok2 {
					vals = append(vals, v2)
				}
			}
			break
		}
		vals = append(vals, v)
	}
	return vals
}

// extractEC2Filters parses EC2 query-protocol Filter.N.Name / Filter.N.Value.M
// parameters into a map of filter-name → allowed values.
func extractEC2Filters(params map[string]string) map[string][]string {
	filters := make(map[string][]string)
	for i := 1; ; i++ {
		name, ok := params[fmt.Sprintf("Filter.%d.Name", i)]
		if !ok {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v, ok := params[fmt.Sprintf("Filter.%d.Value.%d", i, j)]
			if !ok {
				break
			}
			vals = append(vals, v)
		}
		if name != "" {
			filters[name] = vals
		}
	}
	return filters
}

// containsStr reports whether s is in the slice.
func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// filterEmpty removes empty strings from slice.
func filterEmpty(slice []string) []string {
	var result []string
	for _, s := range slice {
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// removePerm removes the first matching permission from the slice.
func removePerm(perms []EC2IPPermission, target EC2IPPermission) []EC2IPPermission {
	for i, p := range perms {
		if p.IPProtocol == target.IPProtocol && p.FromPort == target.FromPort && p.ToPort == target.ToPort {
			return append(perms[:i], perms[i+1:]...)
		}
	}
	return perms
}

// --- AMI operations ----------------------------------------------------------

// ec2ImageStateKey returns the state key for an AMI.
func ec2ImageStateKey(accountID, region, imageID string) string {
	return "image:" + accountID + "/" + region + "/" + imageID
}

// createImage creates an AMI from a running or stopped instance.
func (p *EC2Plugin) createImage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instanceID := req.Params["InstanceId"]
	name := req.Params["Name"]
	description := req.Params["Description"]
	if instanceID == "" || name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "InstanceId and Name are required", HTTPStatus: http.StatusBadRequest}
	}

	// Parse TagSpecifications (TagSpecification.1.Tag.N.Key / Value).
	var tags []EC2Tag
	for i := 1; ; i++ {
		key := req.Params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", i)]
		if key == "" {
			break
		}
		tags = append(tags, EC2Tag{Key: key, Value: req.Params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", i)]})
	}

	imageID := generateImageID()
	img := EC2Image{
		ImageID:      imageID,
		Name:         name,
		Description:  description,
		InstanceID:   instanceID,
		State:        "available",
		CreationDate: p.tc.Now().UTC().Format(time.RFC3339),
		Tags:         tags,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	data, err := json.Marshal(img)
	if err != nil {
		return nil, fmt.Errorf("ec2 createImage marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, ec2ImageStateKey(reqCtx.AccountID, reqCtx.Region, imageID), data); err != nil {
		return nil, fmt.Errorf("ec2 createImage put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"CreateImageResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		ImageID string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		ImageID: imageID,
	})
}

// describeImages lists AMIs owned by the account, with optional tag filters.
// Supports Owners=["self"] and tag:<key>=<value> Filter entries.
func (p *EC2Plugin) describeImages(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "image:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeImages list: %w", err)
	}

	// Collect all Filter.N.Name / Filter.N.Value.M pairs.
	type filterEntry struct {
		name   string
		values []string
	}
	var filters []filterEntry
	for i := 1; ; i++ {
		fn := req.Params["Filter."+strconv.Itoa(i)+".Name"]
		if fn == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			fv := req.Params["Filter."+strconv.Itoa(i)+".Value."+strconv.Itoa(j)]
			if fv == "" {
				break
			}
			vals = append(vals, fv)
		}
		filters = append(filters, filterEntry{name: fn, values: vals})
	}

	type tagItem struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type imageItem struct {
		ImageID      string    `xml:"imageId"`
		Name         string    `xml:"name"`
		Description  string    `xml:"description,omitempty"`
		State        string    `xml:"imageState"`
		OwnerID      string    `xml:"imageOwnerId"`
		CreationDate string    `xml:"creationDate,omitempty"`
		Tags         []tagItem `xml:"tagSet>item"`
	}
	type response struct {
		XMLName xml.Name    `xml:"DescribeImagesResponse"`
		XMLNS   string      `xml:"xmlns,attr"`
		Images  []imageItem `xml:"imagesSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var img EC2Image
		if json.Unmarshal(data, &img) != nil {
			continue
		}

		// Apply filters.
		skip := false
		for _, f := range filters {
			if strings.HasPrefix(f.name, "tag:") {
				tagKey := f.name[4:]
				found := false
				for _, t := range img.Tags {
					if t.Key == tagKey && (len(f.values) == 0 || containsStr(f.values, t.Value)) {
						found = true
						break
					}
				}
				if !found {
					skip = true
					break
				}
			}
		}
		if skip {
			continue
		}

		item := imageItem{
			ImageID:      img.ImageID,
			Name:         img.Name,
			Description:  img.Description,
			State:        img.State,
			OwnerID:      img.AccountID,
			CreationDate: img.CreationDate,
		}
		for _, t := range img.Tags {
			item.Tags = append(item.Tags, tagItem{Key: t.Key, Value: t.Value}) //nolint:staticcheck
		}
		resp.Images = append(resp.Images, item)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// deregisterImage removes an AMI from state.
func (p *EC2Plugin) deregisterImage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	imageID := req.Params["ImageId"]
	if imageID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ImageId is required", HTTPStatus: http.StatusBadRequest}
	}
	key := ec2ImageStateKey(reqCtx.AccountID, reqCtx.Region, imageID)
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deregisterImage delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeregisterImageResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:  "http://ec2.amazonaws.com/doc/2016-11-15/",
		Return: true,
	})
}

// --- Availability Zone operations ---

func (p *EC2Plugin) describeAvailabilityZones(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	region := reqCtx.Region
	// Derive abbreviated region for zoneId (e.g. "use1" from "us-east-1").
	abbrev := azRegionAbbrev(region)
	azSuffixes := []string{"a", "b", "c"}
	type azItem struct {
		ZoneName   string `xml:"zoneName"`
		State      string `xml:"zoneState"`
		RegionName string `xml:"regionName"`
		ZoneID     string `xml:"zoneId"`
	}
	type response struct {
		XMLName           xml.Name `xml:"DescribeAvailabilityZonesResponse"`
		XMLNS             string   `xml:"xmlns,attr"`
		AvailabilityZones []azItem `xml:"availabilityZoneInfo>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for i, suffix := range azSuffixes {
		resp.AvailabilityZones = append(resp.AvailabilityZones, azItem{
			ZoneName:   region + suffix,
			State:      "available",
			RegionName: region,
			ZoneID:     abbrev + "-az" + strconv.Itoa(i+1),
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// azRegionAbbrev returns a short abbreviation for a region name used in zone IDs
// (e.g. "us-east-1" → "use1", "eu-west-2" → "euw2").
func azRegionAbbrev(region string) string {
	// Remove hyphens and digits, keep first letters of each segment plus trailing digit.
	parts := strings.Split(region, "-")
	if len(parts) < 2 {
		return region
	}
	var sb strings.Builder
	for _, p := range parts {
		if len(p) > 0 {
			sb.WriteByte(p[0])
			// Append trailing digit if present.
			if last := p[len(p)-1]; last >= '0' && last <= '9' {
				sb.WriteByte(last)
			}
		}
	}
	return sb.String()
}

// --- Subnet/VPC attribute operations ---

func (p *EC2Plugin) modifySubnetAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidSubnetID.NotFound", Message: "The subnet ID '" + subnetID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var subnet EC2Subnet
	if unmarshalErr := json.Unmarshal(data, &subnet); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifySubnetAttribute unmarshal: %w", unmarshalErr)
	}
	if v, ok := req.Params["MapPublicIPOnLaunch.Value"]; ok {
		subnet.MapPublicIPOnLaunch = v == "true"
	}
	newData, _ := json.Marshal(subnet)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 modifySubnetAttribute put: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ModifySubnetAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) modifyVpcAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidVpcID.NotFound", Message: "The vpc ID '" + vpcID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var vpc EC2VPC
	if unmarshalErr := json.Unmarshal(data, &vpc); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifyVpcAttribute unmarshal: %w", unmarshalErr)
	}
	if v, ok := req.Params["EnableDNSSupport.Value"]; ok {
		vpc.EnableDNSSupport = v == "true"
	}
	if v, ok := req.Params["EnableDNSHostnames.Value"]; ok {
		vpc.EnableDNSHostnames = v == "true"
	}
	newData, _ := json.Marshal(vpc)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 modifyVpcAttribute put: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyVpcAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// --- Elastic IP operations ---

func (p *EC2Plugin) allocateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	domain := req.Params["Domain"]
	if domain == "" {
		domain = "vpc"
	}
	allocationID := generateAllocationID()
	publicIP := generatePublicIP(allocationID)
	eip := EC2ElasticIP{
		AllocationID: allocationID,
		PublicIP:     publicIP,
		Domain:       domain,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	data, err := json.Marshal(eip)
	if err != nil {
		return nil, fmt.Errorf("ec2 allocateAddress marshal: %w", err)
	}
	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 allocateAddress put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "eip_ids", allocationID); err != nil {
		return nil, err
	}
	type response struct {
		XMLName            xml.Name `xml:"AllocateAddressResponse"`
		XMLNS              string   `xml:"xmlns,attr"`
		PublicIP           string   `xml:"publicIp"`
		AllocationID       string   `xml:"allocationId"`
		Domain             string   `xml:"domain"`
		NetworkBorderGroup string   `xml:"networkBorderGroup"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:              "http://ec2.amazonaws.com/doc/2016-11-15/",
		PublicIP:           publicIP,
		AllocationID:       allocationID,
		Domain:             domain,
		NetworkBorderGroup: reqCtx.Region,
	})
}

func (p *EC2Plugin) associateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allocationID := req.Params["AllocationId"]
	instanceID := req.Params["InstanceId"]
	networkInterfaceID := req.Params["NetworkInterfaceId"]

	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidAllocationID.NotFound", Message: "The allocation ID '" + allocationID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var eip EC2ElasticIP
	if unmarshalErr := json.Unmarshal(data, &eip); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 associateAddress unmarshal: %w", unmarshalErr)
	}

	assocID := generateEIPAssociationID()
	eip.AssociationID = assocID
	eip.InstanceID = instanceID
	eip.NetworkInterfaceID = networkInterfaceID

	// If associating with an instance, update the instance's public IP.
	if instanceID != "" {
		instKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instanceID
		instData, instErr := p.state.Get(context.Background(), ec2Namespace, instKey)
		if instErr == nil && instData != nil {
			var inst EC2Instance
			if json.Unmarshal(instData, &inst) == nil {
				eip.PrivateIPAddress = inst.PrivateIPAddress
				inst.PublicIPAddress = eip.PublicIP
				inst.PublicDNSName = ec2PublicDNSName(eip.PublicIP, reqCtx.Region)
				newInstData, _ := json.Marshal(inst)
				_ = p.state.Put(context.Background(), ec2Namespace, instKey, newInstData)
			}
		}
	}

	newData, _ := json.Marshal(eip)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 associateAddress put: %w", err)
	}

	type response struct {
		XMLName       xml.Name `xml:"AssociateAddressResponse"`
		XMLNS         string   `xml:"xmlns,attr"`
		AssociationID string   `xml:"associationId"`
		Return        bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:         "http://ec2.amazonaws.com/doc/2016-11-15/",
		AssociationID: assocID,
		Return:        true,
	})
}

func (p *EC2Plugin) disassociateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID := req.Params["AssociationId"]
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "eip:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 disassociateAddress list: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var eip EC2ElasticIP
		if json.Unmarshal(data, &eip) != nil || eip.AssociationID != assocID {
			continue
		}
		// Clear instance public IP if associated.
		if eip.InstanceID != "" {
			instKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + eip.InstanceID
			instData, instErr := p.state.Get(context.Background(), ec2Namespace, instKey)
			if instErr == nil && instData != nil {
				var inst EC2Instance
				if json.Unmarshal(instData, &inst) == nil {
					inst.PublicIPAddress = ""
					inst.PublicDNSName = ""
					newInstData, _ := json.Marshal(inst)
					_ = p.state.Put(context.Background(), ec2Namespace, instKey, newInstData)
				}
			}
		}
		eip.AssociationID = ""
		eip.InstanceID = ""
		eip.NetworkInterfaceID = ""
		eip.PrivateIPAddress = ""
		newData, _ := json.Marshal(eip)
		_ = p.state.Put(context.Background(), ec2Namespace, k, newData)
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DisassociateAddressResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) releaseAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allocationID := req.Params["AllocationId"]
	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "InvalidAllocationID.NotFound", Message: "The allocation ID '" + allocationID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var eip EC2ElasticIP
	if unmarshalErr := json.Unmarshal(data, &eip); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 releaseAddress unmarshal: %w", unmarshalErr)
	}
	if eip.AssociationID != "" {
		return nil, &AWSError{Code: "InvalidIPAddress.InUse", Message: "The address is currently in use and cannot be released", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 releaseAddress delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ReleaseAddressResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) describeAddresses(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterIDs := extractIndexedParams(req.Params, "AllocationId")
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "eip:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeAddresses list: %w", err)
	}
	type addressItem struct {
		AllocationID       string `xml:"allocationId"`
		PublicIP           string `xml:"publicIp"`
		AssociationID      string `xml:"associationId,omitempty"`
		InstanceID         string `xml:"instanceId,omitempty"`
		NetworkInterfaceID string `xml:"networkInterfaceId,omitempty"`
		PrivateIPAddress   string `xml:"privateIpAddress,omitempty"`
		Domain             string `xml:"domain"`
	}
	type response struct {
		XMLName   xml.Name      `xml:"DescribeAddressesResponse"`
		XMLNS     string        `xml:"xmlns,attr"`
		Addresses []addressItem `xml:"addressesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var eip EC2ElasticIP
		if json.Unmarshal(data, &eip) != nil {
			continue
		}
		if len(filterIDs) > 0 && !containsStr(filterIDs, eip.AllocationID) {
			continue
		}
		resp.Addresses = append(resp.Addresses, addressItem{
			AllocationID:       eip.AllocationID,
			PublicIP:           eip.PublicIP,
			AssociationID:      eip.AssociationID,
			InstanceID:         eip.InstanceID,
			NetworkInterfaceID: eip.NetworkInterfaceID,
			PrivateIPAddress:   eip.PrivateIPAddress,
			Domain:             eip.Domain,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- NAT Gateway operations ---

func (p *EC2Plugin) createNatGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	allocationID := req.Params["AllocationId"]
	connectivityType := req.Params["ConnectivityType"]
	if connectivityType == "" {
		connectivityType = "public"
	}

	// Look up subnet to get VPCID.
	subnetKey := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	subnetData, err := p.state.Get(context.Background(), ec2Namespace, subnetKey)
	if err != nil || subnetData == nil {
		return nil, &AWSError{Code: "InvalidSubnetID.NotFound", Message: "The subnet ID '" + subnetID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var subnet EC2Subnet
	if unmarshalErr := json.Unmarshal(subnetData, &subnet); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 createNatGateway unmarshal subnet: %w", unmarshalErr)
	}

	natID := generateNATGatewayID()

	// Compute a stable private IP using FNV hash on the NAT gateway ID.
	h := fnv.New32a()
	h.Write([]byte(natID))
	n := h.Sum32()
	privateIP := fmt.Sprintf("10.0.%d.%d", (n>>8)&0xFF, n&0xFF)

	gw := EC2NATGateway{
		NatGatewayID:     natID,
		SubnetID:         subnetID,
		VPCID:            subnet.VPCID,
		PrivateIP:        privateIP,
		State:            "available",
		ConnectivityType: connectivityType,
		CreateTime:       p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}

	// For public NAT gateways, look up the EIP.
	if connectivityType == "public" && allocationID != "" {
		eipKey := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
		eipData, eipErr := p.state.Get(context.Background(), ec2Namespace, eipKey)
		if eipErr == nil && eipData != nil {
			var eip EC2ElasticIP
			if json.Unmarshal(eipData, &eip) == nil {
				gw.AllocationID = allocationID
				gw.PublicIP = eip.PublicIP
			}
		}
	}

	// Extract tags from TagSpecification.N.
	for n := 1; ; n++ {
		rt := req.Params[fmt.Sprintf("TagSpecification.%d.ResourceType", n)]
		if rt == "" {
			break
		}
		if rt != "natgateway" {
			continue
		}
		for m := 1; ; m++ {
			key := req.Params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", n, m)]
			if key == "" {
				break
			}
			gw.Tags = append(gw.Tags, EC2Tag{
				Key:   key,
				Value: req.Params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Value", n, m)],
			})
		}
	}

	data, marshalErr := json.Marshal(gw)
	if marshalErr != nil {
		return nil, fmt.Errorf("ec2 createNatGateway marshal: %w", marshalErr)
	}
	stateKey := "nat:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + natID
	if err := p.state.Put(context.Background(), ec2Namespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("ec2 createNatGateway put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "nat_ids", natID); err != nil {
		return nil, err
	}

	type natAddrItem struct {
		AllocationID string `xml:"allocationId,omitempty"`
		PublicIP     string `xml:"publicIp,omitempty"`
		PrivateIP    string `xml:"privateIp"`
	}
	type natItem struct {
		NatGatewayID     string        `xml:"natGatewayId"`
		SubnetID         string        `xml:"subnetId"`
		VpcID            string        `xml:"vpcId"`
		State            string        `xml:"state"`
		ConnectivityType string        `xml:"connectivityType"`
		CreateTime       string        `xml:"createTime"`
		Addresses        []natAddrItem `xml:"natGatewayAddressSet>item"`
	}
	type response struct {
		XMLName    xml.Name `xml:"CreateNatGatewayResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		NatGateway natItem  `xml:"natGateway"`
	}
	item := natItem{
		NatGatewayID:     natID,
		SubnetID:         subnetID,
		VpcID:            subnet.VPCID,
		State:            "available",
		ConnectivityType: connectivityType,
		CreateTime:       gw.CreateTime,
		Addresses: []natAddrItem{{
			AllocationID: gw.AllocationID,
			PublicIP:     gw.PublicIP,
			PrivateIP:    privateIP,
		}},
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		NatGateway: item,
	})
}

func (p *EC2Plugin) describeNatGateways(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterIDs := extractIndexedParams(req.Params, "NatGatewayId")
	filters := extractEC2Filters(req.Params)

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "nat:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeNatGateways list: %w", err)
	}

	type natAddrItem struct {
		AllocationID string `xml:"allocationId,omitempty"`
		PublicIP     string `xml:"publicIp,omitempty"`
		PrivateIP    string `xml:"privateIp"`
	}
	type natItem struct {
		NatGatewayID     string        `xml:"natGatewayId"`
		SubnetID         string        `xml:"subnetId"`
		VpcID            string        `xml:"vpcId"`
		State            string        `xml:"state"`
		ConnectivityType string        `xml:"connectivityType"`
		CreateTime       string        `xml:"createTime"`
		Addresses        []natAddrItem `xml:"natGatewayAddressSet>item"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"DescribeNatGatewaysResponse"`
		XMLNS       string    `xml:"xmlns,attr"`
		NatGateways []natItem `xml:"natGatewaySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var gw EC2NATGateway
		if json.Unmarshal(data, &gw) != nil {
			continue
		}
		if len(filterIDs) > 0 && !containsStr(filterIDs, gw.NatGatewayID) {
			continue
		}
		// Apply filters.
		if stateVals, ok := filters["state"]; ok && !containsStr(stateVals, gw.State) {
			continue
		}
		if vpcVals, ok := filters["vpc-id"]; ok && !containsStr(vpcVals, gw.VPCID) {
			continue
		}
		resp.NatGateways = append(resp.NatGateways, natItem{
			NatGatewayID:     gw.NatGatewayID,
			SubnetID:         gw.SubnetID,
			VpcID:            gw.VPCID,
			State:            gw.State,
			ConnectivityType: gw.ConnectivityType,
			CreateTime:       gw.CreateTime,
			Addresses: []natAddrItem{{
				AllocationID: gw.AllocationID,
				PublicIP:     gw.PublicIP,
				PrivateIP:    gw.PrivateIP,
			}},
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteNatGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	natID := req.Params["NatGatewayId"]
	key := "nat:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + natID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "NatGatewayNotFound", Message: "The nat gateway ID '" + natID + "' does not exist", HTTPStatus: http.StatusBadRequest}
	}
	var gw EC2NATGateway
	if unmarshalErr := json.Unmarshal(data, &gw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 deleteNatGateway unmarshal: %w", unmarshalErr)
	}
	gw.State = "deleted"
	newData, _ := json.Marshal(gw)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 deleteNatGateway put: %w", err)
	}
	type response struct {
		XMLName      xml.Name `xml:"DeleteNatGatewayResponse"`
		XMLNS        string   `xml:"xmlns,attr"`
		NatGatewayID string   `xml:"natGatewayId"`
		State        string   `xml:"state"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:        "http://ec2.amazonaws.com/doc/2016-11-15/",
		NatGatewayID: natID,
		State:        "deleted",
	})
}

// --- DescribeRegions ---------------------------------------------------------

// ec2SeededRegions is the list of regions the emulator reports as enabled.
var ec2SeededRegions = []struct {
	Name     string
	Endpoint string
}{
	{"us-east-1", "ec2.us-east-1.amazonaws.com"},
	{"us-west-2", "ec2.us-west-2.amazonaws.com"},
	{"eu-west-1", "ec2.eu-west-1.amazonaws.com"},
}

func (p *EC2Plugin) describeRegions(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Build optional RegionName.N filter.
	wanted := map[string]bool{}
	for i := 1; ; i++ {
		v := req.Params[fmt.Sprintf("RegionName.%d", i)]
		if v == "" {
			break
		}
		wanted[v] = true
	}

	type regionItem struct {
		RegionName     string `xml:"regionName"`
		RegionEndpoint string `xml:"regionEndpoint"`
		OptInStatus    string `xml:"optInStatus"`
	}
	type response struct {
		XMLName    xml.Name     `xml:"DescribeRegionsResponse"`
		XMLNS      string       `xml:"xmlns,attr"`
		RegionInfo []regionItem `xml:"regionInfo>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, r := range ec2SeededRegions {
		if len(wanted) > 0 && !wanted[r.Name] {
			continue
		}
		resp.RegionInfo = append(resp.RegionInfo, regionItem{
			RegionName:     r.Name,
			RegionEndpoint: r.Endpoint,
			OptInStatus:    "opt-in-not-required",
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- Instance type and spot price catalog ------------------------------------

// ec2InstanceTypeCatalog is a pre-seeded catalog of common instance types
// available for use without real AWS credentials.
var ec2InstanceTypeCatalog = []ec2InstanceTypeInfo{
	{InstanceType: "t3.micro", VCpus: 2, MemoryMiB: 1024, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "c5.xlarge", VCpus: 4, MemoryMiB: 8192, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "c5.2xlarge", VCpus: 8, MemoryMiB: 16384, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "m5.large", VCpus: 2, MemoryMiB: 8192, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "r5.xlarge", VCpus: 4, MemoryMiB: 32768, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "p3.2xlarge", VCpus: 8, MemoryMiB: 62464, GPU: 1, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "g4dn.xlarge", VCpus: 4, MemoryMiB: 16384, GPU: 1, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
	{InstanceType: "inf1.xlarge", VCpus: 4, MemoryMiB: 8192, GPU: 0, SupportedArchs: []string{"x86_64"}, SupportedUsageClasses: []string{"on-demand", "spot"}},
}

// ec2SpotPriceCatalog maps instance type to stub spot price (USD/hr).
var ec2SpotPriceCatalog = map[string]string{
	"t3.micro":    "0.0042",
	"c5.xlarge":   "0.068",
	"c5.2xlarge":  "0.136",
	"m5.large":    "0.038",
	"r5.xlarge":   "0.076",
	"p3.2xlarge":  "0.918",
	"g4dn.xlarge": "0.188",
	"inf1.xlarge": "0.076",
}

// ec2InstanceTypeInfo holds the details for one instance type in the catalog.
type ec2InstanceTypeInfo struct {
	InstanceType          string
	VCpus                 int
	MemoryMiB             int
	GPU                   int
	SupportedArchs        []string
	SupportedUsageClasses []string
}

// --- DescribeInstanceTypes ---------------------------------------------------

func (p *EC2Plugin) describeInstanceTypes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Build filter set from InstanceType.N params.
	wanted := map[string]bool{}
	for i := 1; ; i++ {
		v := req.Params[fmt.Sprintf("InstanceType.%d", i)]
		if v == "" {
			break
		}
		wanted[v] = true
	}

	type gpuInfoItem struct {
		Count int `xml:"gpus>item>count"`
	}
	type processorInfo struct {
		SupportedArchitectures []string `xml:"supportedArchitectures>item"`
	}
	type memoryInfo struct {
		SizeInMiB int `xml:"sizeInMiB"`
	}
	type vcpuInfo struct {
		DefaultVCpus int `xml:"defaultVCpus"`
	}
	type usageClassItem struct {
		Value string `xml:",chardata"`
	}
	type instanceTypeItem struct {
		InstanceType          string           `xml:"instanceType"`
		CurrentGeneration     bool             `xml:"currentGeneration"`
		VCpuInfo              vcpuInfo         `xml:"vCpuInfo"`
		MemoryInfo            memoryInfo       `xml:"memoryInfo"`
		ProcessorInfo         processorInfo    `xml:"processorInfo"`
		SupportedUsageClasses []usageClassItem `xml:"supportedUsageClasses>item"`
		GpuInfo               *gpuInfoItem     `xml:"gpuInfo,omitempty"`
	}
	type response struct {
		XMLName       xml.Name           `xml:"DescribeInstanceTypesResponse"`
		XMLNS         string             `xml:"xmlns,attr"`
		InstanceTypes []instanceTypeItem `xml:"instanceTypeSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if len(wanted) > 0 && !wanted[info.InstanceType] {
			continue
		}
		item := instanceTypeItem{
			InstanceType:      info.InstanceType,
			CurrentGeneration: true,
			VCpuInfo:          vcpuInfo{DefaultVCpus: info.VCpus},
			MemoryInfo:        memoryInfo{SizeInMiB: info.MemoryMiB},
			ProcessorInfo:     processorInfo{SupportedArchitectures: info.SupportedArchs},
		}
		for _, uc := range info.SupportedUsageClasses {
			item.SupportedUsageClasses = append(item.SupportedUsageClasses, usageClassItem{Value: uc})
		}
		if info.GPU > 0 {
			item.GpuInfo = &gpuInfoItem{Count: info.GPU}
		}
		resp.InstanceTypes = append(resp.InstanceTypes, item)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- DescribeInstanceTypeOfferings ------------------------------------------

func (p *EC2Plugin) describeInstanceTypeOfferings(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Build filter map from Filter.N.{Name,Value.1} params.
	locationFilter := ""
	for i := 1; ; i++ {
		name := req.Params[fmt.Sprintf("Filter.%d.Name", i)]
		if name == "" {
			break
		}
		if name == "location" {
			locationFilter = req.Params[fmt.Sprintf("Filter.%d.Value.1", i)]
		}
	}
	// Build instance-type filter from InstanceType.N params.
	wantedTypes := map[string]bool{}
	for i := 1; ; i++ {
		v := req.Params[fmt.Sprintf("InstanceType.%d", i)]
		if v == "" {
			break
		}
		wantedTypes[v] = true
	}

	region := reqCtx.Region
	azSuffixes := []string{"a", "b", "c"}

	type offeringItem struct {
		InstanceType string `xml:"instanceType"`
		LocationType string `xml:"locationType"`
		Location     string `xml:"location"`
	}
	type response struct {
		XMLName               xml.Name       `xml:"DescribeInstanceTypeOfferingsResponse"`
		XMLNS                 string         `xml:"xmlns,attr"`
		InstanceTypeOfferings []offeringItem `xml:"instanceTypeOfferingSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if len(wantedTypes) > 0 && !wantedTypes[info.InstanceType] {
			continue
		}
		for _, suffix := range azSuffixes {
			az := region + suffix
			if locationFilter != "" && locationFilter != az && locationFilter != region {
				continue
			}
			resp.InstanceTypeOfferings = append(resp.InstanceTypeOfferings, offeringItem{
				InstanceType: info.InstanceType,
				LocationType: "availability-zone",
				Location:     az,
			})
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- DescribeSpotPriceHistory ------------------------------------------------

func (p *EC2Plugin) describeSpotPriceHistory(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Build instance-type filter from InstanceType.N params.
	wantedTypes := map[string]bool{}
	for i := 1; ; i++ {
		v := req.Params[fmt.Sprintf("InstanceType.%d", i)]
		if v == "" {
			break
		}
		wantedTypes[v] = true
	}
	// AZ filter.
	azFilter := req.Params["AvailabilityZone"]
	// ProductDescription filter (e.g. "Linux/UNIX").
	pdFilter := req.Params["ProductDescription.1"]

	region := reqCtx.Region
	azSuffixes := []string{"a", "b", "c"}
	// Stub timestamp: use time controller's current time.
	ts := p.tc.Now().UTC().Format(time.RFC3339)

	type spotPriceItem struct {
		InstanceType       string `xml:"instanceType"`
		ProductDescription string `xml:"productDescription"`
		SpotPrice          string `xml:"spotPrice"`
		Timestamp          string `xml:"timestamp"`
		AvailabilityZone   string `xml:"availabilityZone"`
	}
	type response struct {
		XMLName          xml.Name        `xml:"DescribeSpotPriceHistoryResponse"`
		XMLNS            string          `xml:"xmlns,attr"`
		SpotPriceHistory []spotPriceItem `xml:"spotPriceHistorySet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if len(wantedTypes) > 0 && !wantedTypes[info.InstanceType] {
			continue
		}
		price, ok := ec2SpotPriceCatalog[info.InstanceType]
		if !ok {
			continue
		}
		desc := "Linux/UNIX"
		if pdFilter != "" && pdFilter != desc {
			continue
		}
		for _, suffix := range azSuffixes {
			az := region + suffix
			if azFilter != "" && azFilter != az {
				continue
			}
			resp.SpotPriceHistory = append(resp.SpotPriceHistory, spotPriceItem{
				InstanceType:       info.InstanceType,
				ProductDescription: desc,
				SpotPrice:          price,
				Timestamp:          ts,
				AvailabilityZone:   az,
			})
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- Launch template operations ---

// resolveLaunchTemplate looks up a launch template by ID or name and returns it,
// or nil if not found.
func (p *EC2Plugin) resolveLaunchTemplate(goCtx context.Context, ctx *RequestContext, ltID, ltName string) *EC2LaunchTemplate {
	if ltID == "" && ltName == "" {
		return nil
	}
	if ltID == "" {
		// Look up ID by name.
		nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + ltName
		data, err := p.state.Get(goCtx, ec2Namespace, nameKey)
		if err != nil || data == nil {
			return nil
		}
		ltID = string(data)
	}
	key := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + ltID
	data, err := p.state.Get(goCtx, ec2Namespace, key)
	if err != nil || data == nil {
		return nil
	}
	var lt EC2LaunchTemplate
	if json.Unmarshal(data, &lt) != nil {
		return nil
	}
	return &lt
}

func (p *EC2Plugin) createLaunchTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["LaunchTemplateName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "LaunchTemplateName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()

	// Check for name collision.
	nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + name
	existing, _ := p.state.Get(goCtx, ec2Namespace, nameKey)
	if existing != nil {
		return nil, &AWSError{
			Code:       "InvalidLaunchTemplateName.AlreadyExistsException",
			Message:    "Launch template with name '" + name + "' already exists",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	ltID := generateLaunchTemplateID()
	now := p.tc.Now().UTC().Format(time.RFC3339)

	// Parse launch template data params.
	ltData := EC2LaunchTemplateData{
		ImageID:      req.Params["LaunchTemplateData.ImageId"],
		InstanceType: req.Params["LaunchTemplateData.InstanceType"],
		KeyName:      req.Params["LaunchTemplateData.KeyName"],
	}
	if sg1 := req.Params["LaunchTemplateData.SecurityGroupId.1"]; sg1 != "" {
		ltData.SecurityGroupIDs = []string{sg1}
	}
	if ud := req.Params["LaunchTemplateData.UserData"]; ud != "" {
		ltData.UserData = ud
	}

	lt := EC2LaunchTemplate{
		LaunchTemplateID:   ltID,
		LaunchTemplateName: name,
		DefaultVersionNum:  1,
		LatestVersionNum:   1,
		CreatedBy:          ctx.AccountID,
		CreateTime:         now,
		LatestData:         ltData,
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
	}

	ltJSON, err := json.Marshal(lt)
	if err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: marshal: %w", err)
	}
	ltKey := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + ltID
	if err := p.state.Put(goCtx, ec2Namespace, ltKey, ltJSON); err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: put lt: %w", err)
	}
	if err := p.state.Put(goCtx, ec2Namespace, nameKey, []byte(ltID)); err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: put lt_by_name: %w", err)
	}
	idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, ec2Namespace, idsKey, ltID)

	type ltItem struct {
		LaunchTemplateID   string `xml:"launchTemplateId"`
		LaunchTemplateName string `xml:"launchTemplateName"`
		CreateTime         string `xml:"createTime"`
		DefaultVersionNum  int64  `xml:"defaultVersionNumber"`
		LatestVersionNum   int64  `xml:"latestVersionNumber"`
	}
	type response struct {
		XMLName        xml.Name `xml:"CreateLaunchTemplateResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		LaunchTemplate ltItem   `xml:"launchTemplate"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		LaunchTemplate: ltItem{
			LaunchTemplateID:   ltID,
			LaunchTemplateName: name,
			CreateTime:         now,
			DefaultVersionNum:  1,
			LatestVersionNum:   1,
		},
	})
}

func (p *EC2Plugin) describeLaunchTemplates(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var lts []EC2LaunchTemplate

	filterID := req.Params["LaunchTemplateId.1"]
	filterName := req.Params["LaunchTemplateName.1"]

	switch {
	case filterID != "":
		if lt := p.resolveLaunchTemplate(goCtx, ctx, filterID, ""); lt != nil {
			lts = append(lts, *lt)
		}
	case filterName != "":
		if lt := p.resolveLaunchTemplate(goCtx, ctx, "", filterName); lt != nil {
			lts = append(lts, *lt)
		}
	default:
		idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region
		ids, _ := loadStringIndex(goCtx, p.state, ec2Namespace, idsKey)
		for _, id := range ids {
			key := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + id
			data, err := p.state.Get(goCtx, ec2Namespace, key)
			if err != nil || data == nil {
				continue
			}
			var lt EC2LaunchTemplate
			if json.Unmarshal(data, &lt) == nil {
				lts = append(lts, lt)
			}
		}
	}

	type ltItem struct {
		LaunchTemplateID   string `xml:"launchTemplateId"`
		LaunchTemplateName string `xml:"launchTemplateName"`
		CreateTime         string `xml:"createTime"`
		DefaultVersionNum  int64  `xml:"defaultVersionNumber"`
		LatestVersionNum   int64  `xml:"latestVersionNumber"`
	}
	type response struct {
		XMLName         xml.Name `xml:"DescribeLaunchTemplatesResponse"`
		XMLNS           string   `xml:"xmlns,attr"`
		LaunchTemplates []ltItem `xml:"launchTemplates>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, lt := range lts {
		resp.LaunchTemplates = append(resp.LaunchTemplates, ltItem{
			LaunchTemplateID:   lt.LaunchTemplateID,
			LaunchTemplateName: lt.LaunchTemplateName,
			CreateTime:         lt.CreateTime,
			DefaultVersionNum:  lt.DefaultVersionNum,
			LatestVersionNum:   lt.LatestVersionNum,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteLaunchTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ltID := req.Params["LaunchTemplateId"]
	ltName := req.Params["LaunchTemplateName"]

	lt := p.resolveLaunchTemplate(goCtx, ctx, ltID, ltName)
	if lt == nil {
		return nil, &AWSError{
			Code:       "InvalidLaunchTemplateId.NotFound",
			Message:    "The launch template was not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	ltKey := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + lt.LaunchTemplateID
	nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + lt.LaunchTemplateName
	idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region

	if err := p.state.Delete(goCtx, ec2Namespace, ltKey); err != nil {
		return nil, fmt.Errorf("deleteLaunchTemplate: delete lt: %w", err)
	}
	if err := p.state.Delete(goCtx, ec2Namespace, nameKey); err != nil {
		return nil, fmt.Errorf("deleteLaunchTemplate: delete lt_by_name: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, ec2Namespace, idsKey, lt.LaunchTemplateID)

	type ltItem struct {
		LaunchTemplateID   string `xml:"launchTemplateId"`
		LaunchTemplateName string `xml:"launchTemplateName"`
	}
	type response struct {
		XMLName        xml.Name `xml:"DeleteLaunchTemplateResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		LaunchTemplate ltItem   `xml:"launchTemplate"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		LaunchTemplate: ltItem{
			LaunchTemplateID:   lt.LaunchTemplateID,
			LaunchTemplateName: lt.LaunchTemplateName,
		},
	})
}
