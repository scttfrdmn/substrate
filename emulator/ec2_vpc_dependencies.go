package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// A VPC's dependents (#1492).
//
// API_DeleteVpc: "You must detach or delete all gateways and resources that are associated
// with the VPC before you can delete it. For example, you must terminate all instances running
// in the VPC, delete all security groups associated with the VPC (except the default one),
// delete all route tables associated with the VPC (except the default one), and so on. When you
// delete the VPC, it deletes the default security group, network ACL, and route table for the
// VPC."
//
// Through v0.127.0 DeleteVpc removed the VPC record and nothing else, whatever it still held, so
// a teardown that ran in the wrong order passed here and failed against AWS, and the subnets,
// groups and route tables it left behind stayed listed. The check below covers every dependent
// substrate stores; the ones it does not store (network interfaces, VPC endpoints, peering
// connections, egress-only gateways) cannot block a delete here.

// ec2DependencyViolationError is DependencyViolation, the client-error table's code for "The
// specified object has dependent resources." The message is that gloss's first sentence; no
// operation page publishes wording of its own for it.
func ec2DependencyViolationError() *AWSError {
	return &AWSError{
		Code:       "DependencyViolation",
		Message:    "The specified object has dependent resources.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2NewDefaultSecurityGroup returns the default security group a VPC is created with.
//
// The VPC User Guide's "Default security groups for your VPCs": "Your default VPCs and any VPCs
// that you create come with a default security group. The name of the default security group
// is "default"." Its default inbound rule's source "is the ID of this security group", and its
// outbound rule allows all IPv4 traffic to 0.0.0.0/0 — the ::/0 rule is "added only if your VPC
// has an associated IPv6 CIDR block", which substrate does not model. The description is the one
// AWS's DescribeSecurityGroups Example 4 renders for a default group.
func ec2NewDefaultSecurityGroup(reqCtx *RequestContext, vpcID, sgID string) EC2SecurityGroup {
	return EC2SecurityGroup{
		GroupID:     sgID,
		GroupName:   "default",
		Description: "default VPC security group",
		VPCID:       vpcID,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		IngressRules: []EC2IPPermission{{
			IPProtocol:       "-1",
			UserIDGroupPairs: []EC2UserIDGroupPair{{GroupID: sgID, UserID: reqCtx.AccountID}},
		}},
		EgressRules: []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}},
	}
}

// createDefaultSecurityGroup mints and stores vpcID's default security group.
func (p *EC2Plugin) createDefaultSecurityGroup(ctx context.Context, reqCtx *RequestContext, vpcID string) error {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	sgID := generateSGID(reqCtx.IDs)
	if err := p.putEC2Record(ctx, "sg:"+scope+"/"+sgID, ec2NewDefaultSecurityGroup(reqCtx, vpcID, sgID)); err != nil {
		return fmt.Errorf("ec2 default security group: %w", err)
	}
	if err := p.appendToList(scope, "sg_ids", sgID); err != nil {
		return fmt.Errorf("ec2 default security group index: %w", err)
	}
	return nil
}

// ec2ScanRecords decodes every record under prefix into a T and calls visit with its key.
//
// A record that cannot be read or decoded is an error rather than a skip. Its callers decide
// something from the whole set — whether a VPC is empty, whether an address is held, which
// resources the tagging API reports — and a skipped record would answer that question wrongly
// and silently, where a describe that skips one merely omits it.
func ec2ScanRecords[T any](ctx context.Context, state StateManager, prefix string, visit func(key string, rec T)) error {
	keys, err := state.List(ctx, ec2Namespace, prefix)
	if err != nil {
		return fmt.Errorf("list %s: %w", prefix, err)
	}
	for _, k := range keys {
		data, getErr := state.Get(ctx, ec2Namespace, k)
		if getErr != nil {
			return fmt.Errorf("get %s: %w", k, getErr)
		}
		if data == nil {
			continue
		}
		var rec T
		if err := json.Unmarshal(data, &rec); err != nil {
			return fmt.Errorf("unmarshal %s: %w", k, err)
		}
		visit(k, rec)
	}
	return nil
}

// vpcDependents reports whether vpcID still holds anything AWS requires to be gone before
// DeleteVpc, and returns the state keys of what the delete itself removes: the default security
// group and the main route table.
//
// A dependent is a subnet, an attached internet gateway, a security group other than the one
// named "default", a route table that is not the main one, a NAT gateway not yet deleted, or an
// instance not yet terminated. A NAT gateway answers `deleted` and an instance `terminated` for a
// while after their deletes, in AWS as here, and neither holds the VPC.
func (p *EC2Plugin) vpcDependents(reqCtx *RequestContext, vpcID string) (bool, []string, error) {
	ctx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region + "/"
	held := false
	var owned []string

	if err := ec2ScanRecords(ctx, p.state, "subnet:"+scope, func(_ string, s EC2Subnet) {
		held = held || s.VPCID == vpcID
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc subnets: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "igw:"+scope, func(_ string, igw EC2InternetGateway) {
		for _, a := range igw.Attachments {
			held = held || a.VPCID == vpcID
		}
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc internet gateways: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "sg:"+scope, func(k string, sg EC2SecurityGroup) {
		if sg.VPCID != vpcID {
			return
		}
		if sg.GroupName == "default" {
			owned = append(owned, k)
			return
		}
		held = true
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc security groups: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "rtb:"+scope, func(k string, rtb EC2RouteTable) {
		if rtb.VPCID != vpcID {
			return
		}
		for _, a := range rtb.Associations {
			if a.Main {
				owned = append(owned, k)
				return
			}
		}
		held = true
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc route tables: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "nat:"+scope, func(_ string, gw EC2NATGateway) {
		held = held || (gw.VPCID == vpcID && gw.State != "deleted")
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc NAT gateways: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		held = held || (inst.VPCID == vpcID && inst.State.Name != "terminated")
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc instances: %w", err)
	}
	return held, owned, nil
}
