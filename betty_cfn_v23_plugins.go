package substrate

import (
	"context"
	"encoding/json"
)

// ----- v0.23.0 — Kinesis ---------------------------------------------------

// deployKinesisStream creates a Kinesis data stream for the given CFN resource.
func (d *StackDeployer) deployKinesisStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"StreamName": name,
		"ShardCount": 1,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "kinesis",
		Operation: "CreateStream",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "Kinesis_20131202.CreateStream"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := "arn:aws:kinesis:" + cctx.region + ":" + cctx.accountID + ":stream/" + name
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Kinesis::Stream",
		PhysicalID: name,
		ARN:        arn,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- v0.23.0 — CloudFront ------------------------------------------------

// deployCloudFrontDistribution creates a CloudFront distribution stub.
func (d *StackDeployer) deployCloudFrontDistribution(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	comment := resolveStringProp(props, "DistributionConfig.Comment", logicalID, cctx)

	body := []byte(`<DistributionConfig><Comment>` + comment + `</Comment><Enabled>true</Enabled></DistributionConfig>`)

	req := &AWSRequest{
		Service:   "cloudfront",
		Operation: "POST",
		Path:      "/2020-05-31/distribution",
		Body:      body,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudFront::Distribution",
		PhysicalID: logicalID,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Extract Id and DomainName from XML response.
		idVal := extractXMLField(resp.Body, "Id")
		domainVal := extractXMLField(resp.Body, "DomainName")
		if idVal != "" {
			dr.PhysicalID = idVal
			dr.ARN = "arn:aws:cloudfront::" + cctx.accountID + ":distribution/" + idVal
		}
		if domainVal != "" {
			dr.Metadata["DomainName"] = domainVal
		}
	}
	return dr, cost, nil
}

// deployCloudFrontOAI creates a CloudFront Origin Access Identity stub.
func (d *StackDeployer) deployCloudFrontOAI(
	_ context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	comment := resolveStringProp(props, "CloudFrontOriginAccessIdentityConfig.Comment", logicalID, cctx)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudFront::CloudFrontOriginAccessIdentity",
		PhysicalID: comment,
	}, 0, nil
}
