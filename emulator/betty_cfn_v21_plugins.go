package emulator

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
		body["containerDefinitions"] = ecsContainerDefinitions(cdefs, cctx)
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

// ecsContainerDefinitionKeys maps CloudFormation's ContainerDefinition property
// names to the member names the ECS API uses.
//
// CloudFormation spells the property `Image` where every ECS SDK reads `image`,
// and the ECS plugin types ContainerDefinitions as []interface{}, so a
// PascalCase member was stored and echoed with nothing to reject it: the stack
// reported CREATE_COMPLETE, DescribeTaskDefinition answered 200, and
// `describe-task-definition --query 'taskDefinition.containerDefinitions'`
// returned `[{}]` (#527).
//
// The table is explicit rather than a first-letter-lowering function even though
// all 42 members happen to follow that rule, because the rule is a coincidence
// of this one type and not a property of AWS APIs: an unmapped key must pass
// through *unchanged* so a member substrate has not enumerated survives instead
// of being corrupted, and only a table can tell "not mapped" from "mapped to
// itself". It is also deliberately not a generic case converter — DynamoDB's
// verbatim PascalCase forwarding is correct, its API being natively PascalCase,
// and a generic converter is exactly the change that would break it.
//
// Member names are from the ECS ContainerDefinition API reference.
var ecsContainerDefinitionKeys = map[string]string{
	"Command":                "command",
	"Cpu":                    "cpu",
	"CredentialSpecs":        "credentialSpecs",
	"DependsOn":              "dependsOn",
	"DisableNetworking":      "disableNetworking",
	"DnsSearchDomains":       "dnsSearchDomains",
	"DnsServers":             "dnsServers",
	"DockerLabels":           "dockerLabels",
	"DockerSecurityOptions":  "dockerSecurityOptions",
	"EntryPoint":             "entryPoint",
	"Environment":            "environment",
	"EnvironmentFiles":       "environmentFiles",
	"Essential":              "essential",
	"ExtraHosts":             "extraHosts",
	"FirelensConfiguration":  "firelensConfiguration",
	"HealthCheck":            "healthCheck",
	"Hostname":               "hostname",
	"Image":                  "image",
	"Interactive":            "interactive",
	"Links":                  "links",
	"LinuxParameters":        "linuxParameters",
	"LogConfiguration":       "logConfiguration",
	"Memory":                 "memory",
	"MemoryReservation":      "memoryReservation",
	"MountPoints":            "mountPoints",
	"Name":                   "name",
	"PortMappings":           "portMappings",
	"Privileged":             "privileged",
	"PseudoTerminal":         "pseudoTerminal",
	"ReadonlyRootFilesystem": "readonlyRootFilesystem",
	"RepositoryCredentials":  "repositoryCredentials",
	"ResourceRequirements":   "resourceRequirements",
	"RestartPolicy":          "restartPolicy",
	"Secrets":                "secrets",
	"StartTimeout":           "startTimeout",
	"StopTimeout":            "stopTimeout",
	"SystemControls":         "systemControls",
	"Ulimits":                "ulimits",
	"User":                   "user",
	"VersionConsistency":     "versionConsistency",
	"VolumesFrom":            "volumesFrom",
	"WorkingDirectory":       "workingDirectory",
}

// ecsNestedContainerKeys maps the member names of the structured types nested
// inside a container definition, keyed by the container-definition member that
// holds them.
//
// Only the types whose members a consumer asserts on are enumerated; a member
// holding a type absent from this map has its *values* resolved but its keys
// left alone, which is the safe direction — an unmapped key reaching ECS is
// visible in a response, where a wrongly rewritten one is silently lost.
//
// That is also what keeps user-supplied keys intact, and it is why membership
// here is a decision rather than a completeness exercise: logConfiguration's
// `options` is keyed by log-driver option names (`awslogs-group`) and
// `dockerLabels` by whatever labels the consumer chose, so neither may ever
// appear here. Both are *named* by their parent's table — "Options" →
// "options" — and then left whole, because the walk cannot tell a member name
// it has never heard of from a user's label.
//
// Member names are from each type's own ECS API reference page.
var ecsNestedContainerKeys = map[string]map[string]string{
	"environment": {"Name": "name", "Value": "value"},
	"secrets":     {"Name": "name", "ValueFrom": "valueFrom"},
	"portMappings": {
		"AppProtocol":        "appProtocol",
		"ContainerPort":      "containerPort",
		"ContainerPortRange": "containerPortRange",
		"HostPort":           "hostPort",
		"Name":               "name",
		"Protocol":           "protocol",
	},
	"logConfiguration": {
		"LogDriver":     "logDriver",
		"Options":       "options",
		"SecretOptions": "secretOptions",
	},
	"mountPoints": {
		"ContainerPath": "containerPath",
		"ReadOnly":      "readOnly",
		"SourceVolume":  "sourceVolume",
	},
	"volumesFrom": {
		"ReadOnly":        "readOnly",
		"SourceContainer": "sourceContainer",
	},
	"healthCheck": {
		"Command":     "command",
		"Interval":    "interval",
		"Retries":     "retries",
		"StartPeriod": "startPeriod",
		"Timeout":     "timeout",
	},
	"ulimits": {
		"HardLimit": "hardLimit",
		"Name":      "name",
		"SoftLimit": "softLimit",
	},
	"dependsOn": {
		"Condition":     "condition",
		"ContainerName": "containerName",
	},
	"extraHosts": {
		"Hostname":  "hostname",
		"IpAddress": "ipAddress",
	},
	"systemControls": {
		"Namespace": "namespace",
		"Value":     "value",
	},
}

// ecsContainerDefinitions converts a CloudFormation ContainerDefinitions
// property into the shape RegisterTaskDefinition expects: intrinsics resolved at
// any depth (#526) and member names in the ECS API's case (#527).
//
// The two passes are separate on purpose: resolution must not rewrite keys, which
// is what would mangle logConfiguration.options.
//
// They also commute, and it is worth knowing why rather than relying on the
// order. ecsRewriteKeys touches keys only, through tables that contain no
// intrinsic name, so it cannot change whether a map is an intrinsic; resolveNested
// replaces values only, so it cannot change which table ecsRewriteKeys selects.
// Resolution runs first because reading the result of a single pass over final
// values is easier than reasoning about a rename applied to an unresolved tree —
// not because the other order is wrong.
func ecsContainerDefinitions(v interface{}, cctx *cfnContext) interface{} {
	return ecsRewriteKeys(resolveNested(v, cctx), ecsContainerDefinitionKeys)
}

// ecsRewriteKeys rewrites the keys of each map in v using table, recursing into
// nested members with whichever table ecsNestedContainerKeys names for them.
//
// An unmapped key passes through unchanged rather than being lowercased, for the
// same reason #516's expander warns about an unrecognized tag instead of
// dropping it: a member substrate has not enumerated is better delivered
// verbatim than guessed at.
func ecsRewriteKeys(v interface{}, table map[string]string) interface{} {
	switch val := v.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, item := range val {
			out = append(out, ecsRewriteKeys(item, table))
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			name, mapped := table[k]
			if !mapped {
				name = k
			}
			if nested, has := ecsNestedContainerKeys[name]; has {
				out[name] = ecsRewriteKeys(item, nested)
				continue
			}
			// No table for this member, so the walk stops here rather than
			// guessing: the value is carried through with neither its keys nor
			// its nested keys touched. This is what protects
			// logConfiguration.options and dockerLabels, whose keys are user
			// data — see ecsNestedContainerKeys.
			out[name] = item
		}
		return out
	}
	return v
}
