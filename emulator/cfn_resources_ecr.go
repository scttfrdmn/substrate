package emulator

// StackDeployer helpers for CloudFormation's ECR resource types.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- ECR -----------------------------------------------------------------

// deployECRRepository creates an ECR repository for the given CFN resource.
func (d *StackDeployer) deployECRRepository(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "RepositoryName", logicalID, cctx)

	body := map[string]interface{}{"repositoryName": name}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployECRRepository marshal: %w", err)
	}

	req := &AWSRequest{
		Service:   "ecr",
		Operation: "CreateRepository",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerRegistry_V1_1_0.CreateRepository"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ECR::Repository",
		PhysicalID: name,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			Repository struct {
				RepositoryArn string `json:"repositoryArn"`
				RepositoryURI string `json:"repositoryUri"`
			} `json:"repository"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			if result.Repository.RepositoryArn != "" {
				dr.ARN = result.Repository.RepositoryArn
			}
			if result.Repository.RepositoryURI != "" {
				dr.Metadata["RepositoryUri"] = result.Repository.RepositoryURI
			}
		}
	}
	if dr.Error == "" {
		if lp, ok := props["LifecyclePolicy"].(map[string]interface{}); ok {
			lpCost, lpErr := d.putECRLifecyclePolicy(ctx, name, lp, streamID, cctx)
			cost += lpCost
			if lpErr != nil {
				// The repository exists, but a failed resource is not one a rollback deletes, so it
				// is deleted here; otherwise a rolled-back stack would leave it behind, and a retry
				// would be refused with RepositoryAlreadyExistsException.
				dr.Error = lpErr.Error()
				if delErr := d.deleteECRRepository(ctx, name, streamID); delErr != nil {
					dr.Error += " (and the repository could not be deleted: " + delErr.Error() + ")"
				}
			}
		}
	}
	return dr, cost, nil
}

// deleteECRRepository deletes a repository this deploy created, with its images.
func (d *StackDeployer) deleteECRRepository(ctx context.Context, name, streamID string) error {
	body, err := json.Marshal(map[string]interface{}{"repositoryName": name, "force": true})
	if err != nil {
		return fmt.Errorf("cfn deleteECRRepository marshal: %w", err)
	}
	_, _, routeErr := d.dispatch(ctx, &AWSRequest{
		Service:   "ecr",
		Operation: "DeleteRepository",
		Body:      body,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerRegistry_V1_1_0.DeleteRepository"},
		Params:    map[string]string{},
	}, streamID)
	return routeErr
}

// putECRLifecyclePolicy applies a repository's LifecyclePolicy property — "Creates or updates a
// lifecycle policy" — through PutLifecyclePolicy.
//
// This used to be the deploy helper of an AWS::ECR::LifecyclePolicy type, which CloudFormation
// does not have: the resource specification lists no such type, so a template declaring one is
// refused as unrecognized (#1605). The policy is the repository's own property, and only
// LifecyclePolicyText is applied; RegistryId names the account, which is the stack's.
func (d *StackDeployer) putECRLifecyclePolicy(
	ctx context.Context, repoName string, lp map[string]interface{}, streamID string, cctx *cfnContext,
) (float64, error) {
	// LifecyclePolicyText is published as a String; cfnJSONDocument passes that
	// form through unquoted and still accepts an object (#1153).
	policy := cfnJSONDocument(lp["LifecyclePolicyText"], cctx)
	if policy == "" {
		return 0, nil
	}

	body := map[string]interface{}{
		"repositoryName":      repoName,
		"lifecyclePolicyText": policy,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("cfn putECRLifecyclePolicy marshal: %w", err)
	}

	req := &AWSRequest{
		Service:   "ecr",
		Operation: "PutLifecyclePolicy",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerRegistry_V1_1_0.PutLifecyclePolicy"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	return cost, routeErr
}
