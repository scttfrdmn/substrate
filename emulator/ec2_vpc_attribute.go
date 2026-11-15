package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
)

// ec2AttributeBooleanValue is AWS's AttributeBooleanValue, rendered as <value>…</value>.
type ec2AttributeBooleanValue struct {
	Value bool `xml:"value"`
}

// describeVpcAttribute reports one attribute of a VPC (#1151).
//
// API_DescribeVpcAttribute: "Describes the specified attribute of the specified VPC. You can
// specify only one attribute at a time." Attribute is Required, with Valid Values
// enableDnsSupport | enableDnsHostnames | enableNetworkAddressUsageMetrics, and the response
// carries vpcId and the one attribute asked for, as an AttributeBooleanValue — the shape both
// of the page's sample responses show.
//
// It is what makes [EC2Plugin.modifyVpcAttribute] observable: DescribeVpcs publishes no member
// for any of the three, so until this operation was routed a modify could not be read back.
// enableNetworkAddressUsageMetrics is a recorded flag; no metric is produced.
//
// VpcId and Attribute absent are MissingParameter, and an Attribute outside the published values
// is InvalidParameterValue, both from the client-error table, as the page's Errors section is
// the Common Error Types boilerplate; that message is substrate's own.
func (p *EC2Plugin) describeVpcAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	attribute := req.Params["Attribute"]
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 describeVpcAttribute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VPCIDKind, "VpcId", vpcID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	if attribute == "" {
		return nil, ec2MissingParameter("Attribute")
	}
	var vpc EC2VPC
	if unmarshalErr := json.Unmarshal(data, &vpc); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 describeVpcAttribute unmarshal: %w", unmarshalErr)
	}
	type response struct {
		XMLName      xml.Name                  `xml:"DescribeVpcAttributeResponse"`
		XMLNS        string                    `xml:"xmlns,attr"`
		VpcID        string                    `xml:"vpcId"`
		DNSSupport   *ec2AttributeBooleanValue `xml:"enableDnsSupport,omitempty"`
		DNSHostnames *ec2AttributeBooleanValue `xml:"enableDnsHostnames,omitempty"`
		UsageMetrics *ec2AttributeBooleanValue `xml:"enableNetworkAddressUsageMetrics,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", VpcID: vpc.VPCID}
	switch attribute {
	case "enableDnsSupport":
		resp.DNSSupport = &ec2AttributeBooleanValue{Value: vpc.EnableDNSSupport}
	case "enableDnsHostnames":
		resp.DNSHostnames = &ec2AttributeBooleanValue{Value: vpc.EnableDNSHostnames}
	case "enableNetworkAddressUsageMetrics":
		resp.UsageMetrics = &ec2AttributeBooleanValue{Value: vpc.EnableNetworkAddressUsageMetrics}
	default:
		return nil, &AWSError{
			Code: "InvalidParameterValue",
			Message: fmt.Sprintf("Attribute %q is not one of enableDnsSupport, enableDnsHostnames and "+
				"enableNetworkAddressUsageMetrics", attribute),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}
