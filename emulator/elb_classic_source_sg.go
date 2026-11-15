package emulator

import (
	"encoding/xml"
	"errors"
	"fmt"
)

// A classic load balancer's network placement and source security group (#1421).
//
// `LoadBalancerDescription` publishes `SourceSecurityGroup` — "The security group for the load
// balancer, which you can use as part of your inbound rules for your registered instances" — and
// AWS::ElasticLoadBalancing::LoadBalancer publishes `SourceSecurityGroup.GroupName` and
// `SourceSecurityGroup.OwnerAlias` as return values. Until #1421 the record held neither, because
// the ELB plugin had no view of EC2's security groups.
//
// It reads them the way docs/contributing.md prescribes for one plugin acting on another: requests
// sent through the registry. The reads are plain Describe calls and change nothing, so a replay
// that re-runs the create re-runs them against the same state and gets the same answers.
//
// What is derived, and from what:
//
//   - **VPCId** is the VPC of the first subnet the create names, read with DescribeSubnets. The
//     member was omitted before, because nothing resolved a subnet to its VPC.
//   - **SecurityGroups**, when the create names none and the load balancer is in a VPC, is the
//     VPC's default group: "If you use the AWS CLI or API create a load balancer in a nondefault
//     VPC, but you don't specify a security group, your load balancer is automatically associated
//     with the default security group for the VPC" (Configure security groups for your Classic
//     Load Balancer). The same rule is applied in a default VPC, where AWS instead creates a
//     `default_elb_<id>` group per account; substrate does not mint that group, so the VPC's
//     default group stands in for it.
//   - **SourceSecurityGroup** is the first of those groups, its GroupName read with
//     DescribeSecurityGroups and its OwnerAlias the owning account's ID. Neither member is
//     described beyond "The name of the security group" and "The owner of the security group" on
//     the SourceSecurityGroup page; the account-ID owner alias for a VPC load balancer is observed
//     AWS behavior rather than published. A load balancer with no subnets and no security groups —
//     the EC2-Classic shape, which AWS no longer offers — records none, and the member is omitted.
//
// A subnet or group EC2 does not know is not a refusal here: the create accepted unknown IDs before
// #1421, and keeps accepting them, so the derived member is simply absent.

// ELBClassicSourceSecurityGroup is a classic load balancer's `SourceSecurityGroup`.
type ELBClassicSourceSecurityGroup struct {
	// GroupName is the security group's name.
	GroupName string `json:"GroupName"`

	// OwnerAlias is the group's owner: the account ID, for a load balancer in a VPC.
	OwnerAlias string `json:"OwnerAlias"`
}

// elbClassicPlacement is what [ELBPlugin.resolveClassicPlacement] derives.
type elbClassicPlacement struct {
	vpcID          string
	securityGroups []string
	source         *ELBClassicSourceSecurityGroup
}

// resolveClassicPlacement derives a classic load balancer's VPC, effective security groups and
// source security group from the subnets and groups its create named. A plugin without a registry
// — an embedder that registered ELB alone — derives nothing and keeps what the create named.
func (p *ELBPlugin) resolveClassicPlacement(reqCtx *RequestContext, subnets, groups []string) (elbClassicPlacement, error) {
	out := elbClassicPlacement{securityGroups: groups}
	if p.registry == nil {
		return out, nil
	}
	if len(subnets) > 0 {
		vpcID, err := p.ec2SubnetVPC(reqCtx, subnets[0])
		if err != nil {
			return out, err
		}
		out.vpcID = vpcID
	}
	if len(out.securityGroups) == 0 && out.vpcID != "" {
		id, _, err := p.ec2SecurityGroup(reqCtx, map[string]string{
			"Filter.1.Name": "vpc-id", "Filter.1.Value.1": out.vpcID,
			"Filter.2.Name": "group-name", "Filter.2.Value.1": "default",
		})
		if err != nil {
			return out, err
		}
		if id != "" {
			out.securityGroups = []string{id}
		}
	}
	if len(out.securityGroups) > 0 {
		_, name, err := p.ec2SecurityGroup(reqCtx, map[string]string{"GroupId.1": out.securityGroups[0]})
		if err != nil {
			return out, err
		}
		if name != "" {
			out.source = &ELBClassicSourceSecurityGroup{GroupName: name, OwnerAlias: reqCtx.AccountID}
		}
	}
	return out, nil
}

// ec2Describe sends a Describe request to the EC2 plugin and decodes its body into dst. An EC2
// refusal — an unknown subnet or group — is reported as found false rather than as an error.
func (p *ELBPlugin) ec2Describe(reqCtx *RequestContext, action string, params map[string]string, dst any) (bool, error) {
	all := map[string]string{"Action": action}
	for k, v := range params {
		all[k] = v
	}
	resp, err := p.registry.RouteRequest(reqCtx, &AWSRequest{
		Service: "ec2", Operation: action, Headers: map[string]string{}, Params: all,
	})
	if err != nil {
		var awsErr *AWSError
		if errors.As(err, &awsErr) {
			return false, nil
		}
		return false, fmt.Errorf("elb: %s: %w", action, err)
	}
	if resp == nil || resp.StatusCode >= 400 {
		return false, nil
	}
	if err := xml.Unmarshal(resp.Body, dst); err != nil {
		return false, fmt.Errorf("elb: decode %s: %w", action, err)
	}
	return true, nil
}

// ec2SubnetVPC returns the VPC of subnetID, or "" when EC2 does not know the subnet.
func (p *ELBPlugin) ec2SubnetVPC(reqCtx *RequestContext, subnetID string) (string, error) {
	var out struct {
		Subnets []struct {
			VpcID string `xml:"vpcId"`
		} `xml:"subnetSet>item"`
	}
	found, err := p.ec2Describe(reqCtx, "DescribeSubnets", map[string]string{"SubnetId.1": subnetID}, &out)
	if err != nil || !found || len(out.Subnets) == 0 {
		return "", err
	}
	return out.Subnets[0].VpcID, nil
}

// ec2SecurityGroup returns the ID and name of the first security group DescribeSecurityGroups
// answers for params, or empty strings when it answers none.
func (p *ELBPlugin) ec2SecurityGroup(reqCtx *RequestContext, params map[string]string) (id, name string, err error) {
	var out struct {
		Groups []struct {
			GroupID   string `xml:"groupId"`
			GroupName string `xml:"groupName"`
		} `xml:"securityGroupInfo>item"`
	}
	found, err := p.ec2Describe(reqCtx, "DescribeSecurityGroups", params, &out)
	if err != nil || !found || len(out.Groups) == 0 {
		return "", "", err
	}
	return out.Groups[0].GroupID, out.Groups[0].GroupName, nil
}
