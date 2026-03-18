package substrate

import (
	"context"
	"fmt"
)

// ----- v0.41.0 — Elastic IPs and NAT Gateways ---------------------------------

// deployEC2EIP allocates an Elastic IP address for the given CFN resource.
func (d *StackDeployer) deployEC2EIP(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	domain := resolveStringProp(props, "Domain", "vpc", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "AllocateAddress",
		Params: map[string]string{
			"Action": "AllocateAddress",
			"Domain": domain,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::EIP"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		allocationID := extractXMLField(resp.Body, "allocationId")
		publicIP := extractXMLField(resp.Body, "publicIp")
		dr.PhysicalID = allocationID
		dr.ARN = allocationID
		dr.Metadata = map[string]interface{}{
			"AllocationId": allocationID,
			"PublicIp":     publicIP,
		}
	}
	return dr, cost, nil
}

// deployEC2NatGateway creates a NAT gateway for the given CFN resource.
func (d *StackDeployer) deployEC2NatGateway(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	subnetID := resolveStringProp(props, "SubnetId", "", cctx)
	allocationID := resolveStringProp(props, "AllocationId", "", cctx)
	connectivityType := resolveStringProp(props, "ConnectivityType", "public", cctx)
	if subnetID == "" {
		dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::NatGateway", Error: "SubnetId is required"}
		return dr, 0, fmt.Errorf("deployEC2NatGateway: SubnetId is required")
	}
	params := map[string]string{
		"Action":           "CreateNatGateway",
		"SubnetId":         subnetID,
		"ConnectivityType": connectivityType,
	}
	if allocationID != "" {
		params["AllocationId"] = allocationID
	}
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateNatGateway",
		Params:    params,
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::NatGateway"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		natID := extractXMLField(resp.Body, "natGatewayId")
		dr.PhysicalID = natID
		dr.ARN = natID
	}
	return dr, cost, nil
}
