package emulator

// StackDeployer helpers for CloudFormation's SES v2 resource type.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- SES v2 ------------------------------------------------------------------

// deploySESv2EmailIdentity creates an SES v2 email identity for the given CFN resource.
func (d *StackDeployer) deploySESv2EmailIdentity(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	identityName := logicalID
	if name, ok := props["EmailIdentity"].(string); ok && name != "" {
		identityName = name
	}

	body := map[string]interface{}{
		"EmailIdentity": identityName,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal sesv2 email identity body: %w", err)
	}

	req := &AWSRequest{
		Service:   "sesv2",
		Operation: "POST",
		Path:      "/v2/email/identities",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SES::EmailIdentity",
		PhysicalID: identityName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	_ = cctx
	return dr, cost, nil
}
