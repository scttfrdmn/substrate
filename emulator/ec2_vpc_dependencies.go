package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
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

// ec2IsDefaultSecurityGroup reports whether sg is a VPC's default group (#1551).
//
// The group is flagged when it is minted ([ec2NewDefaultSecurityGroup]), so recognizing it does
// not depend on its name. A group written before the flag existed falls back to the name: the
// only group that can be called "default" is the one a VPC is created with, because
// CreateSecurityGroup refuses the name ("Cannot use reserved security group name").
func ec2IsDefaultSecurityGroup(sg EC2SecurityGroup) bool {
	return sg.IsDefault || sg.GroupName == "default"
}

// ec2CannotDeleteDefaultGroupError is the refusal for DeleteSecurityGroup on a default group.
//
// The VPC User Guide's "Default security groups for your VPCs": "You can't delete a default
// security group. If you try to delete a default security group, we return the following error
// code: Client.CannotDelete." The wire code is CannotDelete, the client-error table's entry; the
// "Client." prefix is the fault type, which the Query error writer renders separately. The
// message is that table's gloss for the code, verbatim.
func ec2CannotDeleteDefaultGroupError() *AWSError {
	return &AWSError{
		Code:       "CannotDelete",
		Message:    "You cannot delete the 'default' security group in your VPC, but you can change its rules.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2GroupDependencyViolationError is DependencyViolation for a security group still in use. The
// message is EC2's observed wording, "resource sg-… has a dependent object"
// (hashicorp/terraform-provider-aws#32745, terraform-aws-modules/terraform-aws-eks#2048); no AWS
// page publishes one.
func ec2GroupDependencyViolationError(sgID string) *AWSError {
	return &AWSError{
		Code:       "DependencyViolation",
		Message:    "resource " + sgID + " has a dependent object",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2SubnetDependencyViolationError is DependencyViolation for a subnet still holding resources.
// The message is EC2's observed wording, "The subnet '…' has dependencies and cannot be deleted."
// (terraform-aws-modules/terraform-aws-vpc#1081, chef-boneyard/chef-provisioning-aws#498); no AWS
// page publishes one.
func ec2SubnetDependencyViolationError(subnetID string) *AWSError {
	return &AWSError{
		Code:       "DependencyViolation",
		Message:    "The subnet '" + subnetID + "' has dependencies and cannot be deleted.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2InstanceUsesGroup reports whether inst carries sgID, on its primary interface's flat list
// or on any interface the launch declared.
func ec2InstanceUsesGroup(inst EC2Instance, sgID string) bool {
	if slices.Contains(inst.SecurityGroupIDs, sgID) {
		return true
	}
	for _, ifc := range inst.NetworkInterfaces {
		if slices.Contains(ifc.SecurityGroupIDs, sgID) {
			return true
		}
	}
	return false
}

// ec2PermissionsReference reports whether any rule names sgID as its referenced group.
func ec2PermissionsReference(rules []EC2IPPermission, sgID string) bool {
	for _, r := range rules {
		for _, pair := range r.UserIDGroupPairs {
			if pair.GroupID == sgID {
				return true
			}
		}
	}
	return false
}

// securityGroupInUse reports whether sgID still has a dependent DeleteSecurityGroup must refuse
// on (#1551).
//
// API_DeleteSecurityGroup: "If you attempt to delete a security group that is associated with an
// instance or network interface, is referenced by another security group in the same VPC, or has
// a VPC association, the operation fails with DependencyViolation." The page names that one code
// for a reference as well as for an association, so DependencyViolation answers both; the
// client-error table's InvalidGroup.InUse, glossed "in use by another security group", is not
// what this operation's own page says it returns.
//
// Each half is what substrate stores. An association is a non-terminated instance carrying the
// group on any of its interfaces — substrate stores no standalone network interface, so an
// interface's groups are read off the instance it was launched with. A reference is an ingress
// or egress rule of another group **in the same VPC** naming it, as the page limits it; a group's
// rules about itself (the default group's own inbound rule is one) go with it, and a reference
// from another VPC — a peered VPC's group — does not hold it. A terminated instance holds nothing, as it holds
// nothing for [EC2Plugin.vpcDependents]. VPC associations (AssociateSecurityGroupVpc) are not
// modeled, and resources other plugins hold (load balancers, Lambda functions, RDS instances)
// are not consulted.
func (p *EC2Plugin) securityGroupInUse(reqCtx *RequestContext, target EC2SecurityGroup) (bool, error) {
	ctx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region + "/"
	sgID := target.GroupID
	used := false
	if err := ec2ScanRecords(ctx, p.state, "sg:"+scope, func(_ string, sg EC2SecurityGroup) {
		if sg.GroupID == sgID || sg.VPCID != target.VPCID {
			return
		}
		used = used || ec2PermissionsReference(sg.IngressRules, sgID) ||
			ec2PermissionsReference(sg.EgressRules, sgID)
	}); err != nil {
		return false, fmt.Errorf("ec2 deleteSecurityGroup groups: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		used = used || (inst.State.Name != ec2StateTerminated && ec2InstanceUsesGroup(inst, sgID))
	}); err != nil {
		return false, fmt.Errorf("ec2 deleteSecurityGroup instances: %w", err)
	}
	return used, nil
}

// subnetInUse reports whether subnetID still holds a resource DeleteSubnet must refuse on
// (#1551): an instance not yet terminated with any interface in the subnet, or a NAT gateway not
// yet deleted.
//
// API_DeleteSubnet: "You must terminate all running instances in the subnet before you can
// delete the subnet." A NAT gateway occupies its subnet as an instance does, and is counted the
// same way [EC2Plugin.vpcDependents] counts it for the VPC; network interfaces are counted
// through the instances that carry them, since substrate stores no standalone interface.
func (p *EC2Plugin) subnetInUse(reqCtx *RequestContext, subnetID string) (bool, error) {
	ctx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region + "/"
	used := false
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		if inst.State.Name == ec2StateTerminated {
			return
		}
		used = used || inst.SubnetID == subnetID
		for _, ifc := range inst.NetworkInterfaces {
			used = used || ifc.SubnetID == subnetID
		}
	}); err != nil {
		return false, fmt.Errorf("ec2 deleteSubnet instances: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "nat:"+scope, func(_ string, gw EC2NATGateway) {
		used = used || (gw.SubnetID == subnetID && gw.State != "deleted")
	}); err != nil {
		return false, fmt.Errorf("ec2 deleteSubnet NAT gateways: %w", err)
	}
	return used, nil
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
		IsDefault:   true,
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
// group, the main route table, the default network ACL and the VPC's flow logs.
//
// A dependent is a subnet, an attached internet gateway, a security group other than the
// default one ([ec2IsDefaultSecurityGroup]), a route table that is not the main one, a network
// ACL that is not the default one, a NAT
// gateway not yet deleted, or an instance not yet terminated. A NAT gateway answers `deleted` and an instance `terminated` for a
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
		if ec2IsDefaultSecurityGroup(sg) {
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
	// A network ACL other than the default one holds the VPC; the default one and the VPC's flow
	// logs go with it (#1614).
	aclHeld, aclOwned, err := p.vpcNetworkControlDependents(ctx, reqCtx, vpcID)
	if err != nil {
		return false, owned, err
	}
	held = held || aclHeld
	owned = append(owned, aclOwned...)
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		held = held || (inst.VPCID == vpcID && inst.State.Name != ec2StateTerminated)
	}); err != nil {
		return false, owned, fmt.Errorf("ec2 deleteVpc instances: %w", err)
	}
	return held, owned, nil
}
