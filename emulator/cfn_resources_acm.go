package emulator

// StackDeployer helpers for CloudFormation's ACM resource type.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- ACM -----------------------------------------------------------------

// deployACMCertificate requests an ACM certificate stub for the given CFN resource.
func (d *StackDeployer) deployACMCertificate(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	domain := resolveStringProp(props, "DomainName", logicalID, cctx)

	body := map[string]interface{}{
		"DomainName": domain,
	}
	if sans, ok := props["SubjectAlternativeNames"]; ok {
		// A list of domain names, so a nested Ref or Fn::Split resolves and
		// splices (#526). ACM's RequestCertificate is natively PascalCase, so
		// the member name is forwarded as CloudFormation spells it.
		body["SubjectAlternativeNames"] = resolveValueList(sans, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal acm body: %w", err)
	}

	req := &AWSRequest{
		Service:   "acm",
		Operation: "RequestCertificate",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CertificateManager::Certificate",
		PhysicalID: domain,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			CertificateArn string `json:"CertificateArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.CertificateArn != "" {
			dr.ARN = result.CertificateArn
			dr.PhysicalID = result.CertificateArn
		}
	}
	return dr, cost, nil
}
