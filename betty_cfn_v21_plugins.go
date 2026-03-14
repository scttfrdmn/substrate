package substrate

import (
	"context"
	"encoding/json"
)

// ----- v0.21.0 — ECR -------------------------------------------------------

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
	bodyBytes, _ := json.Marshal(body)

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
	return dr, cost, nil
}

// deployECRLifecyclePolicy attaches a lifecycle policy to an ECR repository.
func (d *StackDeployer) deployECRLifecyclePolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	repoName := resolveStringProp(props, "RepositoryName", "", cctx)
	policy := marshalToJSON(props["LifecyclePolicyText"])
	if policy == "" {
		policy = resolveStringProp(props, "LifecyclePolicyText", "{}", cctx)
	}

	body := map[string]interface{}{
		"repositoryName":      repoName,
		"lifecyclePolicyText": policy,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "ecr",
		Operation: "PutLifecyclePolicy",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerRegistry_V1_1_0.PutLifecyclePolicy"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ECR::LifecyclePolicy", PhysicalID: repoName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- v0.21.0 — ECS -------------------------------------------------------

// deployECSCluster creates an ECS cluster for the given CFN resource.
func (d *StackDeployer) deployECSCluster(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "ClusterName", logicalID, cctx)

	body := map[string]interface{}{"clusterName": name}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "ecs",
		Operation: "CreateCluster",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerServiceV20141113.CreateCluster"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ECS::Cluster", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			Cluster struct {
				ClusterArn string `json:"clusterArn"`
			} `json:"cluster"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.Cluster.ClusterArn != "" {
			dr.ARN = result.Cluster.ClusterArn
		}
	}
	return dr, cost, nil
}

// deployECSTaskDefinition registers an ECS task definition for the given CFN resource.
func (d *StackDeployer) deployECSTaskDefinition(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	family := resolveStringProp(props, "Family", logicalID, cctx)

	body := map[string]interface{}{
		"family":                  family,
		"networkMode":             resolveStringProp(props, "NetworkMode", "bridge", cctx),
		"requiresCompatibilities": []string{resolveStringProp(props, "RequiresCompatibilities.0", "EC2", cctx)},
	}
	if cdefs, ok := props["ContainerDefinitions"]; ok {
		body["containerDefinitions"] = cdefs
	}
	if cpu, ok := props["Cpu"]; ok {
		body["cpu"] = resolveValue(cpu, cctx)
	}
	if mem, ok := props["Memory"]; ok {
		body["memory"] = resolveValue(mem, cctx)
	}
	if execRole, ok := props["ExecutionRoleArn"]; ok {
		body["executionRoleArn"] = resolveValue(execRole, cctx)
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "ecs",
		Operation: "RegisterTaskDefinition",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerServiceV20141113.RegisterTaskDefinition"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ECS::TaskDefinition", PhysicalID: family}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			TaskDefinition struct {
				TaskDefinitionArn string `json:"taskDefinitionArn"`
			} `json:"taskDefinition"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.TaskDefinition.TaskDefinitionArn != "" {
			dr.ARN = result.TaskDefinition.TaskDefinitionArn
			dr.PhysicalID = result.TaskDefinition.TaskDefinitionArn
		}
	}
	return dr, cost, nil
}

// deployECSService creates an ECS service for the given CFN resource.
func (d *StackDeployer) deployECSService(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	serviceName := resolveStringProp(props, "ServiceName", logicalID, cctx)
	cluster := resolveStringProp(props, "Cluster", "default", cctx)

	body := map[string]interface{}{
		"serviceName":    serviceName,
		"cluster":        cluster,
		"taskDefinition": resolveStringProp(props, "TaskDefinition", "", cctx),
		"desiredCount":   1,
		"launchType":     resolveStringProp(props, "LaunchType", "FARGATE", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "ecs",
		Operation: "CreateService",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonEC2ContainerServiceV20141113.CreateService"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ECS::Service", PhysicalID: serviceName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			Service struct {
				ServiceArn string `json:"serviceArn"`
			} `json:"service"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.Service.ServiceArn != "" {
			dr.ARN = result.Service.ServiceArn
		}
	}
	return dr, cost, nil
}

// deployECSCapacityProvider is a stub for AWS::ECS::CapacityProvider (no-op).
func (d *StackDeployer) deployECSCapacityProvider(
	_ context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ECS::CapacityProvider",
		PhysicalID: name,
	}, 0, nil
}
