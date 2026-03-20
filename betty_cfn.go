package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// cfnTemplate is the top-level CloudFormation template structure.
type cfnTemplate struct {
	AWSTemplateFormatVersion string                 `json:"AWSTemplateFormatVersion" yaml:"AWSTemplateFormatVersion"`
	Description              string                 `json:"Description,omitempty"    yaml:"Description,omitempty"`
	Parameters               map[string]cfnParam    `json:"Parameters,omitempty"     yaml:"Parameters,omitempty"`
	Conditions               map[string]interface{} `json:"Conditions,omitempty"     yaml:"Conditions,omitempty"`
	Resources                map[string]cfnResource `json:"Resources"                yaml:"Resources"`
	Outputs                  map[string]cfnOutput   `json:"Outputs,omitempty"        yaml:"Outputs,omitempty"`
}

// cfnParam is a CloudFormation template parameter declaration.
type cfnParam struct {
	// Type is the CloudFormation parameter type (e.g., "String").
	Type string `json:"Type" yaml:"Type"`

	// Default is the default value for the parameter.
	Default string `json:"Default" yaml:"Default"`
}

// cfnOutput is a CloudFormation template output declaration.
type cfnOutput struct {
	// Value is the output value expression (may be an intrinsic function).
	Value interface{} `json:"Value" yaml:"Value"`

	// Description is a human-readable description of the output.
	Description string `json:"Description" yaml:"Description"`
}

// cfnResource is a single CloudFormation resource declaration.
type cfnResource struct {
	Type       string                 `json:"Type"                yaml:"Type"`
	Properties map[string]interface{} `json:"Properties"          yaml:"Properties"`
	DependsOn  interface{}            `json:"DependsOn,omitempty" yaml:"DependsOn,omitempty"`
	Condition  string                 `json:"Condition,omitempty" yaml:"Condition,omitempty"`
}

// CFNStackState holds persisted state for a deployed CloudFormation stack.
type CFNStackState struct {
	// StackName is the name of the CloudFormation stack.
	StackName string `json:"StackName"`

	// TemplateBody is the raw template body.
	TemplateBody string `json:"TemplateBody"`

	// Parameters holds the resolved parameter values used during deployment.
	Parameters map[string]string `json:"Parameters"`

	// Resources lists the deployed resources.
	Resources []DeployedResource `json:"Resources"`

	// Outputs holds resolved output values.
	Outputs map[string]string `json:"Outputs"`

	// Status is the stack status (e.g., "CREATE_COMPLETE").
	Status string `json:"Status"`

	// CreatedAt is the time the stack was first deployed.
	CreatedAt time.Time `json:"CreatedAt"`

	// UpdatedAt is the time the stack was last updated.
	UpdatedAt time.Time `json:"UpdatedAt"`
}

// cfnContext holds per-deployment resolution context for intrinsic functions.
type cfnContext struct {
	params     map[string]string           // caller-supplied + defaults
	conditions map[string]bool             // evaluated condition results
	resources  map[string]DeployedResource // logicalID → result for Ref/GetAtt
	region     string
	accountID  string
	stackName  string
}

// cfnNamespace is the state namespace for CloudFormation stack state.
const cfnNamespace = "cfn"

// cfnStubNamespace is the state namespace for generic CFN stub resource props.
const cfnStubNamespace = "cfn_stub"

// typePriority determines deployment order for CloudFormation resources.
// Lower numbers deploy first.
var typePriority = map[string]int{
	"AWS::IAM::Policy":                            0,
	"AWS::IAM::Role":                              1,
	"AWS::EC2::VPC":                               1,
	"AWS::Route53::HostedZone":                    1,
	"AWS::KMS::Key":                               1,
	"AWS::DynamoDB::Table":                        2,
	"AWS::S3::Bucket":                             2,
	"AWS::EC2::Subnet":                            2,
	"AWS::EC2::SecurityGroup":                     2,
	"AWS::EC2::InternetGateway":                   2,
	"AWS::KMS::Alias":                             2,
	"AWS::KMS::ReplicaKey":                        2,
	"AWS::SecretsManager::Secret":                 2,
	"AWS::SSM::Parameter":                         2,
	"AWS::Lambda::Function":                       3,
	"AWS::SQS::Queue":                             3,
	"AWS::EC2::RouteTable":                        3,
	"AWS::EC2::Instance":                          3,
	"AWS::ElasticLoadBalancingV2::TargetGroup":    3,
	"AWS::ElasticLoadBalancingV2::LoadBalancer":   3,
	"AWS::SNS::Topic":                             3,
	"AWS::ElasticLoadBalancingV2::Listener":       4,
	"AWS::ElasticLoadBalancingV2::ListenerRule":   5,
	"AWS::Route53::RecordSet":                     4,
	"AWS::Route53::RecordSetGroup":                4,
	"AWS::SNS::Subscription":                      4,
	"AWS::SNS::TopicPolicy":                       4,
	"AWS::SecretsManager::RotationSchedule":       5,
	"AWS::SecretsManager::SecretTargetAttachment": 5,
	"AWS::SSM::Association":                       5,
	"AWS::Logs::LogGroup":                         2,
	"AWS::Logs::LogStream":                        3,
	"AWS::Events::Rule":                           4,
	"AWS::CloudWatch::Alarm":                      4,
	// v0.19.0 — API Gateway and ACM.
	"AWS::CertificateManager::Certificate": 1,
	"AWS::ApiGateway::RestApi":             2,
	"AWS::ApiGateway::Authorizer":          3,
	"AWS::ApiGateway::Resource":            3,
	"AWS::ApiGateway::ApiKey":              3,
	"AWS::ApiGateway::Method":              4,
	"AWS::ApiGateway::Deployment":          4,
	"AWS::ApiGateway::UsagePlan":           4,
	"AWS::ApiGateway::Stage":               5,
	"AWS::ApiGateway::UsagePlanKey":        5,
	"AWS::ApiGatewayV2::Api":               2,
	"AWS::ApiGatewayV2::Authorizer":        3,
	"AWS::ApiGatewayV2::Integration":       3,
	"AWS::ApiGatewayV2::Route":             3,
	"AWS::ApiGatewayV2::Stage":             4,
	// v0.20.0 — Step Functions.
	"AWS::StepFunctions::Activity":     3,
	"AWS::StepFunctions::StateMachine": 4,
	// v0.21.0 — ECS and ECR.
	"AWS::ECR::Repository":       2,
	"AWS::ECR::LifecyclePolicy":  3,
	"AWS::ECS::Cluster":          2,
	"AWS::ECS::CapacityProvider": 3,
	"AWS::ECS::TaskDefinition":   3,
	"AWS::ECS::Service":          5,
	// v0.22.0 — Cognito.
	"AWS::Cognito::UserPool":                   2,
	"AWS::Cognito::IdentityPool":               2,
	"AWS::Cognito::UserPoolClient":             3,
	"AWS::Cognito::UserPoolGroup":              3,
	"AWS::Cognito::UserPoolDomain":             4,
	"AWS::Cognito::IdentityPoolRoleAttachment": 4,
	// v0.23.0 — Kinesis and CloudFront.
	"AWS::Kinesis::Stream":                            2,
	"AWS::CloudFront::CloudFrontOriginAccessIdentity": 2,
	"AWS::CloudFront::Distribution":                   3,
	// v0.25.0 — RDS and ElastiCache.
	"AWS::RDS::DBSubnetGroup":            2,
	"AWS::RDS::DBParameterGroup":         2,
	"AWS::RDS::DBCluster":                3,
	"AWS::RDS::DBInstance":               3,
	"AWS::ElastiCache::SubnetGroup":      2,
	"AWS::ElastiCache::ParameterGroup":   2,
	"AWS::ElastiCache::CacheCluster":     3,
	"AWS::ElastiCache::ReplicationGroup": 3,
	// v0.26.0 — EFS and Glue.
	"AWS::EFS::FileSystem":  2,
	"AWS::EFS::AccessPoint": 3,
	"AWS::EFS::MountTarget": 4,
	"AWS::Glue::Database":   2,
	"AWS::Glue::Connection": 2,
	"AWS::Glue::Table":      3,
	"AWS::Glue::Crawler":    3,
	"AWS::Glue::Job":        3,
	// v0.27.0 — Budgets.
	"AWS::Budgets::Budget": 3,
	// v0.28.0 — SES v2 and Firehose.
	"AWS::SES::EmailIdentity":              2,
	"AWS::KinesisFirehose::DeliveryStream": 3,
	// v0.41.0 — Elastic IPs and NAT Gateways.
	"AWS::EC2::EIP":        2,
	"AWS::EC2::NatGateway": 4,
	// v0.43.0 — FSx.
	"AWS::FSx::FileSystem": 3,
	// v0.30.0 — Lambda ESM.
	"AWS::Lambda::EventSourceMapping": 5,
	// v0.31.0 — AppSync.
	"AWS::AppSync::GraphQLApi":            2,
	"AWS::AppSync::DataSource":            3,
	"AWS::AppSync::Resolver":              4,
	"AWS::AppSync::FunctionConfiguration": 4,
	// v0.34.0 — RDS Aurora cluster and MSK.
	"AWS::MSK::Cluster": 3,
	// v0.32.0 — extended CFN stubs.
	"AWS::OpenSearchService::Domain":     2,
	"AWS::WAFv2::WebACL":                 2,
	"AWS::Backup::BackupPlan":            2,
	"AWS::CodeBuild::Project":            2,
	"AWS::CodePipeline::Pipeline":        3,
	"AWS::CodeDeploy::DeploymentGroup":   3,
	"AWS::CloudTrail::Trail":             2,
	"AWS::Config::ConfigRule":            3,
	"AWS::Config::ConfigurationRecorder": 2,
	"AWS::Transfer::Server":              2,
	"AWS::Athena::WorkGroup":             2,
}

// StackDeployer parses and deploys a CloudFormation template using in-process
// plugin dispatch.
type StackDeployer struct {
	registry *PluginRegistry
	store    *EventStore
	state    StateManager
	tc       *TimeController
	logger   Logger
	costs    *CostController
}

// NewStackDeployer creates a StackDeployer wired to the provided dependencies.
func NewStackDeployer(registry *PluginRegistry, store *EventStore, state StateManager, tc *TimeController, logger Logger, costs *CostController) *StackDeployer {
	return &StackDeployer{
		registry: registry,
		store:    store,
		state:    state,
		tc:       tc,
		logger:   logger,
		costs:    costs,
	}
}

// Deploy parses cfn and deploys all resources, returning a DeployResult.
// Resources are deployed in type-priority order. Unknown resource types are
// skipped with a warning. The optional params map overrides template parameter
// defaults.
func (d *StackDeployer) Deploy(ctx context.Context, cfn, streamID string, params map[string]string) (*DeployResult, error) {
	tmpl, err := parseCFNTemplate(cfn)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	stackName := streamID
	start := d.tc.Now()

	// Build resolution context.
	cctx := buildCFNContext(tmpl, params, defaultRegion, testAccountID, stackName)
	evaluateConditions(tmpl, cctx)

	// Sort logical IDs by type priority, then alphabetically for stability.
	type entry struct {
		logicalID string
		resource  cfnResource
		priority  int
	}
	entries := make([]entry, 0, len(tmpl.Resources))
	for logicalID, res := range tmpl.Resources {
		// Skip resources with a false condition.
		if res.Condition != "" {
			if val, ok := cctx.conditions[res.Condition]; ok && !val {
				d.logger.Info("cfn: skipping resource due to false condition",
					"logical_id", logicalID, "condition", res.Condition)
				continue
			}
		}
		p, ok := typePriority[res.Type]
		if !ok {
			p = 99
		}
		entries = append(entries, entry{logicalID: logicalID, resource: res, priority: p})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].priority != entries[j].priority {
			return entries[i].priority < entries[j].priority
		}
		return entries[i].logicalID < entries[j].logicalID
	})

	resources := make([]DeployedResource, 0, len(entries))
	var totalCost float64

	for _, e := range entries {
		dr, cost, deployErr := d.deployResource(ctx, e.logicalID, e.resource, streamID, cctx)
		if deployErr != nil {
			return nil, fmt.Errorf("deploy resource %s: %w", e.logicalID, deployErr)
		}
		totalCost += cost
		cctx.resources[e.logicalID] = dr
		resources = append(resources, dr)
	}

	// Resolve outputs.
	outputs := make(map[string]string)
	for outKey, outVal := range tmpl.Outputs {
		outputs[outKey] = resolveValue(outVal.Value, cctx)
	}

	duration := d.tc.Now().Sub(start)

	result := &DeployResult{
		StackName: stackName,
		Resources: resources,
		StreamID:  streamID,
		TotalCost: totalCost,
		Duration:  duration,
		Outputs:   outputs,
	}

	// Persist stack state if state manager is available.
	if d.state != nil {
		state := CFNStackState{
			StackName:    stackName,
			TemplateBody: cfn,
			Parameters:   cctx.params,
			Resources:    resources,
			Outputs:      outputs,
			Status:       "CREATE_COMPLETE",
			CreatedAt:    start,
			UpdatedAt:    d.tc.Now(),
		}
		d.persistStack(ctx, state)
	}

	return result, nil
}

// UpdateStack re-deploys a previously deployed stack with new template or parameters.
func (d *StackDeployer) UpdateStack(ctx context.Context, cfn, stackName string, params map[string]string) (*DeployResult, error) {
	result, err := d.Deploy(ctx, cfn, stackName, params)
	if err != nil {
		return nil, fmt.Errorf("update stack %s: %w", stackName, err)
	}
	// Overwrite the persisted status.
	if d.state != nil {
		data, getErr := d.state.Get(ctx, cfnNamespace, "stack:"+stackName)
		if getErr == nil && data != nil {
			var s CFNStackState
			if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr == nil {
				s.Status = "UPDATE_COMPLETE"
				s.UpdatedAt = d.tc.Now()
				d.persistStack(ctx, s)
			}
		}
	}
	return result, nil
}

// DeleteStack removes a deployed stack from state.
func (d *StackDeployer) DeleteStack(ctx context.Context, stackName string) error {
	if d.state == nil {
		return nil
	}
	if err := d.state.Delete(ctx, cfnNamespace, "stack:"+stackName); err != nil {
		return fmt.Errorf("delete stack %s: %w", stackName, err)
	}
	// Remove from stack names list.
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != stackName {
			newNames = append(newNames, n)
		}
	}
	return d.saveStackNames(ctx, newNames)
}

// ListStacks returns all persisted stack states.
func (d *StackDeployer) ListStacks(ctx context.Context) ([]CFNStackState, error) {
	if d.state == nil {
		return nil, nil
	}
	names, err := d.loadStackNames(ctx)
	if err != nil {
		return nil, err
	}
	stacks := make([]CFNStackState, 0, len(names))
	for _, name := range names {
		data, getErr := d.state.Get(ctx, cfnNamespace, "stack:"+name)
		if getErr != nil || data == nil {
			continue
		}
		var s CFNStackState
		if unmarshalErr := json.Unmarshal(data, &s); unmarshalErr != nil {
			continue
		}
		stacks = append(stacks, s)
	}
	return stacks, nil
}

func (d *StackDeployer) persistStack(ctx context.Context, s CFNStackState) {
	data, err := json.Marshal(s)
	if err != nil {
		d.logger.Warn("cfn: failed to marshal stack state", "err", err)
		return
	}
	if err := d.state.Put(ctx, cfnNamespace, "stack:"+s.StackName, data); err != nil {
		d.logger.Warn("cfn: failed to persist stack state", "err", err)
		return
	}
	names, _ := d.loadStackNames(ctx)
	for _, n := range names {
		if n == s.StackName {
			return
		}
	}
	names = append(names, s.StackName)
	_ = d.saveStackNames(ctx, names)
}

func (d *StackDeployer) loadStackNames(ctx context.Context) ([]string, error) {
	data, err := d.state.Get(ctx, cfnNamespace, "stack_names")
	if err != nil {
		return nil, fmt.Errorf("cfn loadStackNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("cfn loadStackNames unmarshal: %w", err)
	}
	return names, nil
}

func (d *StackDeployer) saveStackNames(ctx context.Context, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("cfn saveStackNames marshal: %w", err)
	}
	return d.state.Put(ctx, cfnNamespace, "stack_names", data)
}

// deployResource dispatches a single CFN resource to the correct deploy helper.
func (d *StackDeployer) deployResource(
	ctx context.Context,
	logicalID string,
	res cfnResource,
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	switch res.Type {
	case "AWS::IAM::Policy":
		return d.deployIAMPolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::IAM::Role":
		return d.deployIAMRole(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::S3::Bucket":
		return d.deployS3Bucket(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Lambda::Function":
		return d.deployLambdaFunction(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SQS::Queue":
		return d.deploySQSQueue(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::DynamoDB::Table":
		return d.deployDynamoDBTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::VPC":
		return d.deployEC2VPC(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::Subnet":
		return d.deployEC2Subnet(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::SecurityGroup":
		return d.deployEC2SecurityGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::InternetGateway":
		return d.deployEC2InternetGateway(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::RouteTable":
		return d.deployEC2RouteTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::Instance":
		return d.deployEC2Instance(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::TargetGroup":
		return d.deployELBTargetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::LoadBalancer":
		return d.deployELBLoadBalancer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::Listener":
		return d.deployELBListener(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElasticLoadBalancingV2::ListenerRule":
		return d.deployELBListenerRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::HostedZone":
		return d.deployRoute53HostedZone(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::RecordSet":
		return d.deployRoute53RecordSet(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Route53::RecordSetGroup":
		return d.deployRoute53RecordSetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::Key":
		return d.deployKMSKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::Alias":
		return d.deployKMSAlias(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KMS::ReplicaKey":
		return d.deployKMSReplicaKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::Secret":
		return d.deploySecret(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::RotationSchedule":
		return d.deploySecretRotationSchedule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SecretsManager::SecretTargetAttachment":
		return d.deploySecretTargetAttachment(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SSM::Parameter":
		return d.deploySSMParameter(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SSM::Association":
		return d.deploySSMAssociation(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::Topic":
		return d.deploySNSTopic(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::Subscription":
		return d.deploySNSSubscription(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::SNS::TopicPolicy":
		return d.deploySNSTopicPolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Logs::LogGroup":
		return d.deployLogsLogGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Logs::LogStream":
		return d.deployLogsLogStream(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Events::Rule":
		return d.deployEventsRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudWatch::Alarm":
		return d.deployCloudWatchAlarm(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.19.0 — API Gateway and ACM.
	case "AWS::CertificateManager::Certificate":
		return d.deployACMCertificate(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::RestApi":
		return d.deployAPIGatewayRestAPI(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Authorizer":
		return d.deployAPIGatewayAuthorizer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Resource":
		return d.deployAPIGatewayResource(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Method":
		return d.deployAPIGatewayMethod(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Deployment":
		return d.deployAPIGatewayDeployment(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::Stage":
		return d.deployAPIGatewayStage(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::ApiKey":
		return d.deployAPIGatewayAPIKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::UsagePlan":
		return d.deployAPIGatewayUsagePlan(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGateway::UsagePlanKey":
		return d.deployAPIGatewayUsagePlanKey(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Api":
		return d.deployAPIGatewayV2Api(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Route":
		return d.deployAPIGatewayV2Route(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Integration":
		return d.deployAPIGatewayV2Integration(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Stage":
		return d.deployAPIGatewayV2Stage(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ApiGatewayV2::Authorizer":
		return d.deployAPIGatewayV2Authorizer(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.20.0 — Step Functions.
	case "AWS::StepFunctions::StateMachine":
		return d.deployStepFunctionsStateMachine(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::StepFunctions::Activity":
		return d.deployStepFunctionsActivity(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.21.0 — ECS and ECR.
	case "AWS::ECR::Repository":
		return d.deployECRRepository(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECR::LifecyclePolicy":
		return d.deployECRLifecyclePolicy(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::Cluster":
		return d.deployECSCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::TaskDefinition":
		return d.deployECSTaskDefinition(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::Service":
		return d.deployECSService(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ECS::CapacityProvider":
		return d.deployECSCapacityProvider(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.22.0 — Cognito.
	case "AWS::Cognito::UserPool":
		return d.deployCognitoUserPool(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolClient":
		return d.deployCognitoUserPoolClient(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolGroup":
		return d.deployCognitoUserPoolGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::UserPoolDomain":
		return d.deployCognitoUserPoolDomain(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::IdentityPool":
		return d.deployCognitoIdentityPool(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		return d.deployCognitoIdentityPoolRoleAttachment(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.23.0 — Kinesis and CloudFront.
	case "AWS::Kinesis::Stream":
		return d.deployKinesisStream(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudFront::Distribution":
		return d.deployCloudFrontDistribution(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudFront::CloudFrontOriginAccessIdentity":
		return d.deployCloudFrontOAI(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.25.0 — RDS and ElastiCache.
	case "AWS::RDS::DBSubnetGroup":
		return d.deployRDSDBSubnetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBParameterGroup":
		return d.deployRDSDBParameterGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBCluster":
		return d.deployRDSDBCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::RDS::DBInstance":
		return d.deployRDSDBInstance(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::SubnetGroup":
		return d.deployElastiCacheSubnetGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::ParameterGroup":
		return d.deployElastiCacheParameterGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::CacheCluster":
		return d.deployElastiCacheCacheCluster(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::ElastiCache::ReplicationGroup":
		return d.deployElastiCacheReplicationGroup(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.26.0 — EFS and Glue.
	case "AWS::EFS::FileSystem":
		return d.deployEFSFileSystem(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EFS::AccessPoint":
		return d.deployEFSAccessPoint(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EFS::MountTarget":
		return d.deployEFSMountTarget(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Database":
		return d.deployGlueDatabase(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Connection":
		return d.deployGlueConnection(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Table":
		return d.deployGlueTable(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Crawler":
		return d.deployGlueCrawler(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Glue::Job":
		return d.deployGlueJob(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.27.0 — Budgets.
	case "AWS::Budgets::Budget":
		return d.deployBudgetsBudget(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.28.0 — SES v2 and Firehose.
	case "AWS::SES::EmailIdentity":
		return d.deploySESv2EmailIdentity(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::KinesisFirehose::DeliveryStream":
		return d.deployFirehoseDeliveryStream(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.41.0 — Elastic IPs and NAT Gateways.
	case "AWS::EC2::EIP":
		return d.deployEC2EIP(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::EC2::NatGateway":
		return d.deployEC2NatGateway(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.30.0 — Lambda ESM.
	case "AWS::Lambda::EventSourceMapping":
		return d.deployLambdaEventSourceMapping(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.31.0 — AppSync.
	case "AWS::AppSync::GraphQLApi":
		return d.deployAppSyncGraphQLApi(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::DataSource":
		return d.deployAppSyncDataSource(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::Resolver":
		return d.deployAppSyncResolver(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::AppSync::FunctionConfiguration":
		return d.deployAppSyncFunction(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.43.0 — FSx.
	case "AWS::FSx::FileSystem":
		return d.deployFSxFileSystem(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.34.0 — RDS Aurora cluster and MSK.
	case "AWS::MSK::Cluster":
		return d.deployMSKCluster(ctx, logicalID, res.Properties, streamID, cctx)
	// v0.32.0 — extended CFN stubs.
	case "AWS::OpenSearchService::Domain":
		return d.deployOpenSearchDomain(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::WAFv2::WebACL":
		return d.deployWAFv2WebACL(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Backup::BackupPlan":
		return d.deployBackupBackupPlan(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodeBuild::Project":
		return d.deployCodeBuildProject(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodePipeline::Pipeline":
		return d.deployCodePipelinePipeline(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CodeDeploy::DeploymentGroup":
		return d.deployCodeDeployDeploymentGroup(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::CloudTrail::Trail":
		return d.deployCloudTrailTrail(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Config::ConfigRule":
		return d.deployConfigConfigRule(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Config::ConfigurationRecorder":
		return d.deployConfigConfigurationRecorder(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Transfer::Server":
		return d.deployTransferServer(ctx, logicalID, res.Properties, streamID, cctx)
	case "AWS::Athena::WorkGroup":
		return d.deployAthenaWorkGroup(ctx, logicalID, res.Properties, streamID, cctx)
	default:
		d.logger.Warn("unknown CloudFormation resource type; using generic stub",
			"logical_id", logicalID,
			"type", res.Type,
		)
		return d.deployGenericStub(ctx, logicalID, res.Type, res.Properties, cctx)
	}
}

// deployS3Bucket creates an S3 bucket for the given CFN resource.
func (d *StackDeployer) deployS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	bucketName := strings.ToLower(resolveStringProp(props, "BucketName", logicalID, cctx))

	req := &AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucketName,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::S3::Bucket",
		PhysicalID: bucketName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	_ = resp

	// Apply VersioningConfiguration if present.
	if vc, ok := props["VersioningConfiguration"].(map[string]interface{}); ok {
		if status, _ := vc["Status"].(string); status == "Enabled" {
			vReq := &AWSRequest{
				Service:   "s3",
				Operation: "PUT",
				Path:      "/" + bucketName,
				Params:    map[string]string{"versioning": "1"},
				Headers:   map[string]string{"Content-Type": "application/xml"},
				Body:      []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
			}
			_, _, _ = d.dispatch(ctx, vReq, streamID)
		}
	}

	return dr, cost, nil
}

// deployIAMRole creates an IAM role for the given CFN resource.
func (d *StackDeployer) deployIAMRole(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	roleName := resolveStringProp(props, "RoleName", logicalID, cctx)

	body := map[string]string{
		"RoleName":                 roleName,
		"Path":                     resolveStringProp(props, "Path", "/", cctx),
		"AssumeRolePolicyDocument": marshalToJSON(props["AssumeRolePolicyDocument"]),
		"Description":              resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal role body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreateRole",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Role",
		PhysicalID: roleName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			Role struct {
				ARN string `json:"Arn"`
			} `json:"Role"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.Role.ARN
		}
	}

	return dr, cost, nil
}

// deployIAMPolicy creates an IAM managed policy for the given CFN resource.
func (d *StackDeployer) deployIAMPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	policyName := resolveStringProp(props, "PolicyName", logicalID, cctx)

	body := map[string]string{
		"PolicyName":     policyName,
		"Path":           resolveStringProp(props, "Path", "/", cctx),
		"PolicyDocument": marshalToJSON(props["PolicyDocument"]),
		"Description":    resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal policy body: %w", err)
	}

	req := &AWSRequest{
		Service:   "iam",
		Operation: "CreatePolicy",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::IAM::Policy",
		PhysicalID: policyName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			Policy struct {
				ARN string `json:"Arn"`
			} `json:"Policy"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.Policy.ARN
		}
	}

	return dr, cost, nil
}

// deployLambdaFunction creates a Lambda function for the given CFN resource.
func (d *StackDeployer) deployLambdaFunction(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	fnName := resolveStringProp(props, "FunctionName", logicalID, cctx)

	body := map[string]interface{}{
		"FunctionName": fnName,
		"Runtime":      resolveStringProp(props, "Runtime", "python3.12", cctx),
		"Role":         resolveStringProp(props, "Role", "", cctx),
		"Handler":      resolveStringProp(props, "Handler", "index.handler", cctx),
		"Description":  resolveStringProp(props, "Description", "", cctx),
	}
	if timeout := resolveStringProp(props, "Timeout", "", cctx); timeout != "" {
		body["Timeout"] = timeout
	}
	if memory := resolveStringProp(props, "MemorySize", "", cctx); memory != "" {
		body["MemorySize"] = memory
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal lambda body: %w", err)
	}

	req := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Lambda::Function",
		PhysicalID: fnName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			FunctionArn string `json:"FunctionArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.FunctionArn
		}
	}

	return dr, cost, nil
}

// deploySQSQueue creates an SQS queue for the given CFN resource.
func (d *StackDeployer) deploySQSQueue(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	queueName := resolveStringProp(props, "QueueName", logicalID, cctx)

	req := &AWSRequest{
		Service:   "sqs",
		Operation: "CreateQueue",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":    "CreateQueue",
			"QueueName": queueName,
		},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SQS::Queue",
		PhysicalID: queueName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else {
		dr.ARN = sqsQueueARN(cctx.region, cctx.accountID, queueName)
	}
	_ = resp

	return dr, cost, nil
}

// deployDynamoDBTable creates a DynamoDB table for the given CFN resource.
func (d *StackDeployer) deployDynamoDBTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	tableName := resolveStringProp(props, "TableName", logicalID, cctx)

	// Build the CreateTable body from CFN properties.
	body := map[string]interface{}{
		"TableName": tableName,
	}

	if keySchema, ok := props["KeySchema"]; ok {
		body["KeySchema"] = keySchema
	}
	if attrDefs, ok := props["AttributeDefinitions"]; ok {
		body["AttributeDefinitions"] = attrDefs
	}
	if billingMode, ok := props["BillingMode"]; ok {
		body["BillingMode"] = resolveValue(billingMode, cctx)
	}
	if pt, ok := props["ProvisionedThroughput"]; ok {
		body["ProvisionedThroughput"] = pt
	}
	if gsis, ok := props["GlobalSecondaryIndexes"]; ok {
		body["GlobalSecondaryIndexes"] = gsis
	}
	if lsis, ok := props["LocalSecondaryIndexes"]; ok {
		body["LocalSecondaryIndexes"] = lsis
	}
	if ss, ok := props["StreamSpecification"]; ok {
		body["StreamSpecification"] = ss
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal dynamodb body: %w", err)
	}

	req := &AWSRequest{
		Service:   "dynamodb",
		Operation: "CreateTable",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::DynamoDB::Table",
		PhysicalID: tableName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			TableDescription struct {
				TableARN string `json:"TableARN"`
			} `json:"TableDescription"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.ARN = result.TableDescription.TableARN
		}
	}

	// Handle TimeToLiveSpecification if present.
	if ttlSpec, ok := props["TimeToLiveSpecification"]; ok && routeErr == nil {
		ttlBody := map[string]interface{}{
			"TableName":               tableName,
			"TimeToLiveSpecification": ttlSpec,
		}
		ttlBytes, marshalErr := json.Marshal(ttlBody)
		if marshalErr == nil {
			ttlReq := &AWSRequest{
				Service:   "dynamodb",
				Operation: "UpdateTimeToLive",
				Body:      ttlBytes,
				Headers:   map[string]string{},
				Params:    map[string]string{},
			}
			_, _, _ = d.dispatch(ctx, ttlReq, streamID)
		}
	}

	return dr, cost, nil
}

// deployEC2VPC creates an EC2 VPC for the given CFN resource.
func (d *StackDeployer) deployEC2VPC(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	cidr := resolveStringProp(props, "CidrBlock", "10.0.0.0/16", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateVpc",
		Params:    map[string]string{"Action": "CreateVpc", "CidrBlock": cidr},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::VPC"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Extract vpcId from XML response body.
		dr.PhysicalID = extractXMLField(resp.Body, "vpcId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2Subnet creates an EC2 subnet for the given CFN resource.
func (d *StackDeployer) deployEC2Subnet(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	cidr := resolveStringProp(props, "CidrBlock", "10.0.0.0/24", cctx)
	az := resolveStringProp(props, "AvailabilityZone", cctx.region+"a", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateSubnet",
		Params: map[string]string{
			"Action":           "CreateSubnet",
			"VpcId":            vpcID,
			"CidrBlock":        cidr,
			"AvailabilityZone": az,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::Subnet"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "subnetId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2SecurityGroup creates an EC2 security group for the given CFN resource.
func (d *StackDeployer) deployEC2SecurityGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	groupName := resolveStringProp(props, "GroupName", logicalID, cctx)
	description := resolveStringProp(props, "GroupDescription", groupName, cctx)
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateSecurityGroup",
		Params: map[string]string{
			"Action":      "CreateSecurityGroup",
			"GroupName":   groupName,
			"Description": description,
			"VpcId":       vpcID,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::SecurityGroup"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "groupId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2InternetGateway creates an EC2 internet gateway for the given CFN resource.
func (d *StackDeployer) deployEC2InternetGateway(
	ctx context.Context,
	logicalID string,
	_ map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateInternetGateway",
		Params:    map[string]string{"Action": "CreateInternetGateway"},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::InternetGateway"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "internetGatewayId")
		dr.ARN = dr.PhysicalID
	}
	_ = cctx
	return dr, cost, nil
}

// deployEC2RouteTable creates an EC2 route table for the given CFN resource.
func (d *StackDeployer) deployEC2RouteTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	vpcID := resolveStringProp(props, "VpcId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "CreateRouteTable",
		Params:    map[string]string{"Action": "CreateRouteTable", "VpcId": vpcID},
		Headers:   map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::RouteTable"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "routeTableId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployEC2Instance launches EC2 instances for the given CFN resource.
func (d *StackDeployer) deployEC2Instance(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	imageID := resolveStringProp(props, "ImageId", "ami-00000000", cctx)
	instanceType := resolveStringProp(props, "InstanceType", "t3.micro", cctx)
	subnetID := resolveStringProp(props, "SubnetId", "", cctx)
	req := &AWSRequest{
		Service:   "ec2",
		Operation: "RunInstances",
		Params: map[string]string{
			"Action":       "RunInstances",
			"ImageId":      imageID,
			"InstanceType": instanceType,
			"MinCount":     "1",
			"MaxCount":     "1",
			"SubnetId":     subnetID,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::EC2::Instance"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.PhysicalID = extractXMLField(resp.Body, "instanceId")
		dr.ARN = dr.PhysicalID
	}
	return dr, cost, nil
}

// deployELBTargetGroup creates an ELBv2 target group for the given CFN resource.
func (d *StackDeployer) deployELBTargetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateTargetGroup",
		Params: map[string]string{
			"Action":     "CreateTargetGroup",
			"Name":       name,
			"Protocol":   resolveStringProp(props, "Protocol", "HTTP", cctx),
			"Port":       resolveStringProp(props, "Port", "80", cctx),
			"VpcId":      resolveStringProp(props, "VpcId", "", cctx),
			"TargetType": resolveStringProp(props, "TargetType", "instance", cctx),
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::TargetGroup", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "TargetGroupArn")
	}
	return dr, cost, nil
}

// deployELBLoadBalancer creates an ELBv2 load balancer for the given CFN resource.
func (d *StackDeployer) deployELBLoadBalancer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateLoadBalancer",
		Params: map[string]string{
			"Action": "CreateLoadBalancer",
			"Name":   name,
			"Type":   resolveStringProp(props, "Type", "application", cctx),
			"Scheme": resolveStringProp(props, "Scheme", "internet-facing", cctx),
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::LoadBalancer", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "LoadBalancerArn")
	}
	return dr, cost, nil
}

// deployELBListener creates an ELBv2 listener for the given CFN resource.
func (d *StackDeployer) deployELBListener(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	lbARN := resolveStringProp(props, "LoadBalancerArn", "", cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateListener",
		Params: map[string]string{
			"Action":          "CreateListener",
			"LoadBalancerArn": lbARN,
			"Protocol":        resolveStringProp(props, "Protocol", "HTTP", cctx),
			"Port":            resolveStringProp(props, "Port", "80", cctx),
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::Listener"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "ListenerArn")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

// deployELBListenerRule creates an ELBv2 listener rule for the given CFN resource.
func (d *StackDeployer) deployELBListenerRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	listenerARN := resolveStringProp(props, "ListenerArn", "", cctx)
	priority := resolveStringProp(props, "Priority", "1", cctx)
	req := &AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "CreateRule",
		Params: map[string]string{
			"Action":      "CreateRule",
			"ListenerArn": listenerARN,
			"Priority":    priority,
		},
		Headers: map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ElasticLoadBalancingV2::ListenerRule"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "RuleArn")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

// deployRoute53HostedZone creates a Route 53 hosted zone for the given CFN resource.
func (d *StackDeployer) deployRoute53HostedZone(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	body := `<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>` + name +
		`</Name><CallerReference>cfn-` + logicalID + `</CallerReference></CreateHostedZoneRequest>`
	req := &AWSRequest{
		Service:   "route53",
		Operation: "POST",
		Path:      "/2013-04-01/hostedzone",
		Body:      []byte(body),
		Headers:   map[string]string{"Content-Type": "application/xml"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Route53::HostedZone", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		dr.ARN = extractXMLField(resp.Body, "Id")
		dr.PhysicalID = dr.ARN
	}
	return dr, cost, nil
}

// deployRoute53RecordSet creates a Route 53 record set for the given CFN resource.
func (d *StackDeployer) deployRoute53RecordSet(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	zoneID := resolveStringProp(props, "HostedZoneId", "", cctx)
	name := resolveStringProp(props, "Name", "", cctx)
	rtype := resolveStringProp(props, "Type", "A", cctx)
	ttl := resolveStringProp(props, "TTL", "300", cctx)
	value := resolveStringProp(props, "ResourceRecords", "", cctx)
	body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>UPSERT</Action><ResourceRecordSet>` +
		`<Name>` + name + `</Name><Type>` + rtype + `</Type><TTL>` + ttl + `</TTL>` +
		`<ResourceRecords><ResourceRecord><Value>` + value + `</Value></ResourceRecord></ResourceRecords>` +
		`</ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	req := &AWSRequest{
		Service:   "route53",
		Operation: "POST",
		Path:      "/2013-04-01/hostedzone/" + zoneID + "/rrset",
		Body:      []byte(body),
		Headers:   map[string]string{"Content-Type": "application/xml"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Route53::RecordSet", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployRoute53RecordSetGroup creates multiple Route 53 record sets from a CFN
// RecordSetGroup resource by iterating over its RecordSets list.
func (d *StackDeployer) deployRoute53RecordSetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	var totalCost float64
	if rsList, ok := props["RecordSets"].([]interface{}); ok {
		for i, rsRaw := range rsList {
			rsProps, ok := rsRaw.(map[string]interface{})
			if !ok {
				continue
			}
			childID := fmt.Sprintf("%s-RecordSet%d", logicalID, i)
			_, cost, err := d.deployRoute53RecordSet(ctx, childID, rsProps, streamID, cctx)
			if err != nil {
				return DeployedResource{}, totalCost, fmt.Errorf("deployRoute53RecordSetGroup item %d: %w", i, err)
			}
			totalCost += cost
		}
	}
	return DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Route53::RecordSetGroup",
	}, totalCost, nil
}

// deployKMSKey creates a KMS key for the given CFN resource.
func (d *StackDeployer) deployKMSKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	body := map[string]interface{}{
		"Description": resolveStringProp(props, "Description", "", cctx),
		"KeyUsage":    resolveStringProp(props, "KeyUsage", "ENCRYPT_DECRYPT", cctx),
		"KeySpec":     resolveStringProp(props, "KeySpec", "SYMMETRIC_DEFAULT", cctx),
	}
	if enableKeyRotation, ok := props["EnableKeyRotation"]; ok {
		body["EnableKeyRotation"] = enableKeyRotation
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal kms key body: %w", err)
	}
	req := &AWSRequest{
		Service:   "kms",
		Operation: "CreateKey",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::KMS::Key"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			KeyMetadata struct {
				KeyID string `json:"KeyId"`
				ARN   string `json:"Arn"`
			} `json:"KeyMetadata"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.KeyMetadata.ARN
			dr.ARN = result.KeyMetadata.ARN
		}
	}
	return dr, cost, nil
}

// deployKMSAlias creates a KMS alias for the given CFN resource.
func (d *StackDeployer) deployKMSAlias(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	aliasName := resolveStringProp(props, "AliasName", "alias/"+logicalID, cctx)
	targetKeyID := resolveStringProp(props, "TargetKeyId", "", cctx)
	body := map[string]string{
		"AliasName":   aliasName,
		"TargetKeyId": targetKeyID,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal kms alias body: %w", err)
	}
	req := &AWSRequest{
		Service:   "kms",
		Operation: "CreateAlias",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::KMS::Alias", PhysicalID: aliasName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployKMSReplicaKey creates a KMS replica key (stub) for the given CFN resource.
func (d *StackDeployer) deployKMSReplicaKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Stub: treat as a standard symmetric key creation.
	return d.deployKMSKey(ctx, logicalID, props, streamID, cctx)
}

// deploySecret creates a Secrets Manager secret for the given CFN resource.
func (d *StackDeployer) deploySecret(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	body := map[string]interface{}{
		"Name":        name,
		"Description": resolveStringProp(props, "Description", "", cctx),
	}
	if sv, ok := props["SecretString"]; ok {
		body["SecretString"] = resolveValue(sv, cctx)
	}
	if kmsID, ok := props["KmsKeyId"]; ok {
		body["KmsKeyId"] = resolveValue(kmsID, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal secret body: %w", err)
	}
	req := &AWSRequest{
		Service:   "secretsmanager",
		Operation: "CreateSecret",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SecretsManager::Secret", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ARN string `json:"ARN"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ARN != "" {
			dr.ARN = result.ARN
			dr.PhysicalID = result.ARN
		}
	}
	return dr, cost, nil
}

// deploySecretRotationSchedule enables rotation on a Secrets Manager secret.
func (d *StackDeployer) deploySecretRotationSchedule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	secretID := resolveStringProp(props, "SecretId", "", cctx)
	body := map[string]string{"SecretId": secretID}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal rotation schedule body: %w", err)
	}
	req := &AWSRequest{
		Service:   "secretsmanager",
		Operation: "RotateSecret",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SecretsManager::RotationSchedule", PhysicalID: secretID, ARN: secretID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deploySecretTargetAttachment is a stub for SecretsManager::SecretTargetAttachment.
func (d *StackDeployer) deploySecretTargetAttachment(
	_ context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	secretID := resolveStringProp(props, "SecretId", logicalID, cctx)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SecretsManager::SecretTargetAttachment",
		PhysicalID: secretID,
		ARN:        secretID,
	}, 0, nil
}

// deploySSMParameter creates an SSM parameter for the given CFN resource.
func (d *StackDeployer) deploySSMParameter(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", "/"+logicalID, cctx)
	value := d.resolveDynamicRef(ctx, resolveStringProp(props, "Value", "", cctx), cctx)
	body := map[string]interface{}{
		"Name":      name,
		"Value":     value,
		"Type":      resolveStringProp(props, "Type", "String", cctx),
		"Overwrite": true,
	}
	if desc, ok := props["Description"]; ok {
		body["Description"] = resolveValue(desc, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal ssm parameter body: %w", err)
	}
	req := &AWSRequest{
		Service:   "ssm",
		Operation: "PutParameter",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SSM::Parameter", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deploySSMAssociation is a stub for SSM::Association resources.
func (d *StackDeployer) deploySSMAssociation(
	_ context.Context,
	logicalID string,
	_ map[string]interface{},
	_ string,
	_ *cfnContext,
) (DeployedResource, float64, error) {
	assocID := randomHex(16)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::SSM::Association",
		PhysicalID: assocID,
	}, 0, nil
}

// deploySNSTopic creates an SNS topic for the given CFN resource.
func (d *StackDeployer) deploySNSTopic(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicName := resolveStringProp(props, "TopicName", logicalID, cctx)
	req := &AWSRequest{
		Service:   "sns",
		Operation: "CreateTopic",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action": "CreateTopic",
			"Name":   topicName,
		},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::Topic", PhysicalID: topicName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		arn := extractXMLField(resp.Body, "TopicArn")
		if arn != "" {
			dr.ARN = arn
			dr.PhysicalID = arn
		}
	}
	return dr, cost, nil
}

// deploySNSSubscription creates an SNS subscription for the given CFN resource.
func (d *StackDeployer) deploySNSSubscription(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicARN := resolveStringProp(props, "TopicArn", "", cctx)
	protocol := resolveStringProp(props, "Protocol", "sqs", cctx)
	endpoint := resolveStringProp(props, "Endpoint", "", cctx)
	req := &AWSRequest{
		Service:   "sns",
		Operation: "Subscribe",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":   "Subscribe",
			"TopicArn": topicARN,
			"Protocol": protocol,
			"Endpoint": endpoint,
		},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::Subscription"}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		subARN := extractXMLField(resp.Body, "SubscriptionArn")
		if subARN != "" {
			dr.PhysicalID = subARN
			dr.ARN = subARN
		}
	}
	return dr, cost, nil
}

// deploySNSTopicPolicy sets a topic policy via SetTopicAttributes for the given CFN resource.
func (d *StackDeployer) deploySNSTopicPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	topicARN := resolveStringProp(props, "Topics", "", cctx)
	// Topics is a list; try to get the first entry.
	if topicsList, ok := props["Topics"].([]interface{}); ok && len(topicsList) > 0 {
		topicARN = resolveValue(topicsList[0], cctx)
	}
	policy := marshalToJSON(props["PolicyDocument"])
	req := &AWSRequest{
		Service:   "sns",
		Operation: "SetTopicAttributes",
		Body:      nil,
		Headers:   map[string]string{},
		Params: map[string]string{
			"Action":         "SetTopicAttributes",
			"TopicArn":       topicARN,
			"AttributeName":  "Policy",
			"AttributeValue": policy,
		},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::SNS::TopicPolicy", PhysicalID: topicARN, ARN: topicARN}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployLogsLogGroup creates a CloudWatch Logs log group for the given CFN resource.
func (d *StackDeployer) deployLogsLogGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	lgName := resolveStringProp(props, "LogGroupName", logicalID, cctx)
	body := map[string]interface{}{
		"logGroupName": lgName,
	}
	if retain, ok := props["RetentionInDays"]; ok {
		body["retentionInDays"] = retain
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal loggroup body: %w", err)
	}
	req := &AWSRequest{
		Service:   "logs",
		Operation: "CreateLogGroup",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Logs::LogGroup",
		PhysicalID: lgName,
		ARN:        cwLogGroupARN(cctx.region, cctx.accountID, lgName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployLogsLogStream creates a CloudWatch Logs log stream for the given CFN resource.
func (d *StackDeployer) deployLogsLogStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	groupName := resolveStringProp(props, "LogGroupName", "", cctx)
	streamName := resolveStringProp(props, "LogStreamName", logicalID, cctx)
	body := map[string]string{
		"logGroupName":  groupName,
		"logStreamName": streamName,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal logstream body: %w", err)
	}
	req := &AWSRequest{
		Service:   "logs",
		Operation: "CreateLogStream",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Logs::LogStream",
		PhysicalID: streamName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployEventsRule creates an EventBridge rule for the given CFN resource.
func (d *StackDeployer) deployEventsRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	ruleName := resolveStringProp(props, "Name", logicalID, cctx)
	body := map[string]interface{}{
		"Name":  ruleName,
		"State": resolveStringProp(props, "State", "ENABLED", cctx),
	}
	if ep, ok := props["EventPattern"]; ok {
		body["EventPattern"] = marshalToJSON(ep)
	}
	if se, ok := props["ScheduleExpression"]; ok {
		body["ScheduleExpression"] = resolveValue(se, cctx)
	}
	if desc, ok := props["Description"]; ok {
		body["Description"] = resolveValue(desc, cctx)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal events rule body: %w", err)
	}
	req := &AWSRequest{
		Service:   "eventbridge",
		Operation: "PutRule",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Events::Rule",
		PhysicalID: ruleName,
		ARN:        ebRuleARN(cctx.region, cctx.accountID, ruleName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			RuleArn string `json:"RuleArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.RuleArn != "" {
			dr.ARN = result.RuleArn
		}
	}
	return dr, cost, nil
}

// deployCloudWatchAlarm creates a CloudWatch alarm for the given CFN resource.
func (d *StackDeployer) deployCloudWatchAlarm(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	alarmName := resolveStringProp(props, "AlarmName", logicalID, cctx)
	params := map[string]string{
		"Action":             "PutMetricAlarm",
		"AlarmName":          alarmName,
		"MetricName":         resolveStringProp(props, "MetricName", "", cctx),
		"Namespace":          resolveStringProp(props, "Namespace", "", cctx),
		"ComparisonOperator": resolveStringProp(props, "ComparisonOperator", "", cctx),
		"Threshold":          resolveStringProp(props, "Threshold", "0", cctx),
		"EvaluationPeriods":  resolveStringProp(props, "EvaluationPeriods", "1", cctx),
		"Period":             resolveStringProp(props, "Period", "60", cctx),
	}
	if desc := resolveStringProp(props, "AlarmDescription", "", cctx); desc != "" {
		params["AlarmDescription"] = desc
	}
	if stat := resolveStringProp(props, "Statistic", "", cctx); stat != "" {
		params["Statistic"] = stat
	}
	req := &AWSRequest{
		Service:   "monitoring",
		Operation: "PutMetricAlarm",
		Body:      nil,
		Headers:   map[string]string{},
		Params:    params,
	}
	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudWatch::Alarm",
		PhysicalID: alarmName,
		ARN:        cwAlarmARN(cctx.region, cctx.accountID, alarmName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// extractXMLField extracts the text content of the first occurrence of an XML
// element with the given name from b.
func extractXMLField(b []byte, name string) string {
	openTag := "<" + name + ">"
	closeTag := "</" + name + ">"
	s := string(b)
	start := strings.Index(s, openTag)
	if start < 0 {
		return ""
	}
	start += len(openTag)
	end := strings.Index(s[start:], closeTag)
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

// dispatch performs in-process request routing, records the event, and returns
// the response, the estimated cost, and any routing error.
func (d *StackDeployer) dispatch(
	ctx context.Context,
	req *AWSRequest,
	streamID string,
) (*AWSResponse, float64, error) {
	reqCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: testAccountID,
		Region:    defaultRegion,
		Timestamp: d.tc.Now(),
		Metadata:  map[string]interface{}{"stream_id": streamID},
	}

	start := d.tc.Now()
	resp, routeErr := d.registry.RouteRequest(reqCtx, req)
	duration := time.Since(start)
	cost := d.costs.CostForRequest(req)

	_ = d.store.RecordRequest(ctx, reqCtx, req, resp, duration, cost, routeErr)

	return resp, cost, routeErr
}

// buildCFNContext constructs a cfnContext from template parameters and caller-supplied values.
func buildCFNContext(tmpl *cfnTemplate, callerParams map[string]string, region, accountID, stackName string) *cfnContext {
	params := make(map[string]string)
	// Start with template defaults.
	for name, p := range tmpl.Parameters {
		if p.Default != "" {
			params[name] = p.Default
		}
	}
	// Overlay caller-supplied params.
	for k, v := range callerParams {
		params[k] = v
	}
	return &cfnContext{
		params:     params,
		conditions: make(map[string]bool),
		resources:  make(map[string]DeployedResource),
		region:     region,
		accountID:  accountID,
		stackName:  stackName,
	}
}

// evaluateConditions evaluates all Conditions in the template into cctx.conditions.
func evaluateConditions(tmpl *cfnTemplate, cctx *cfnContext) {
	for name, expr := range tmpl.Conditions {
		cctx.conditions[name] = evalConditionExpr(expr, cctx)
	}
}

// evalConditionExpr evaluates a single condition expression.
func evalConditionExpr(expr interface{}, cctx *cfnContext) bool {
	m, ok := expr.(map[string]interface{})
	if !ok {
		return false
	}
	for fn, args := range m {
		switch fn {
		case "Fn::Equals":
			arr, ok := args.([]interface{})
			if !ok || len(arr) != 2 {
				return false
			}
			return resolveValue(arr[0], cctx) == resolveValue(arr[1], cctx)
		case "Fn::Not":
			arr, ok := args.([]interface{})
			if !ok || len(arr) != 1 {
				return true
			}
			return !evalConditionExpr(arr[0], cctx)
		case "Fn::And":
			arr, ok := args.([]interface{})
			if !ok {
				return false
			}
			for _, a := range arr {
				if !evalConditionExpr(a, cctx) {
					return false
				}
			}
			return true
		case "Fn::Or":
			arr, ok := args.([]interface{})
			if !ok {
				return false
			}
			for _, a := range arr {
				if evalConditionExpr(a, cctx) {
					return true
				}
			}
			return false
		case "Condition":
			name, ok := args.(string)
			if !ok {
				return false
			}
			return cctx.conditions[name]
		}
	}
	return false
}

// resolveValue resolves a CloudFormation value (literal, Ref, or intrinsic function).
func resolveValue(v interface{}, cctx *cfnContext) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case map[string]interface{}:
		for fn, args := range val {
			switch fn {
			case "Ref":
				ref, ok := args.(string)
				if !ok {
					return ""
				}
				return resolveRef(ref, cctx)
			case "Fn::Sub":
				return resolveFnSub(args, cctx)
			case "Fn::Join":
				return resolveFnJoin(args, cctx)
			case "Fn::Select":
				return resolveFnSelect(args, cctx)
			case "Fn::Split":
				// Returns a list; return first element as string.
				return resolveFnSplitFirst(args, cctx)
			case "Fn::Base64":
				return resolveFnBase64(args, cctx)
			case "Fn::GetAtt":
				return resolveFnGetAtt(args, cctx)
			case "Fn::If":
				return resolveFnIf(args, cctx)
			}
		}
	}
	// Fallback: JSON-encode.
	b, _ := json.Marshal(v)
	return string(b)
}

func resolveRef(ref string, cctx *cfnContext) string {
	// Pseudo-parameters.
	switch ref {
	case "AWS::Region":
		return cctx.region
	case "AWS::AccountId":
		return cctx.accountID
	case "AWS::StackName":
		return cctx.stackName
	case "AWS::NoValue":
		return ""
	}
	// Parameter reference.
	if v, ok := cctx.params[ref]; ok {
		return v
	}
	// Deployed resource Ref (physical ID).
	if dr, ok := cctx.resources[ref]; ok {
		return dr.PhysicalID
	}
	return ref
}

func resolveFnSub(args interface{}, cctx *cfnContext) string {
	switch v := args.(type) {
	case string:
		return substituteTemplate(v, cctx, nil)
	case []interface{}:
		if len(v) < 1 {
			return ""
		}
		tmplStr, ok := v[0].(string)
		if !ok {
			return ""
		}
		var extra map[string]string
		if len(v) >= 2 {
			if m, ok := v[1].(map[string]interface{}); ok {
				extra = make(map[string]string, len(m))
				for k, val := range m {
					extra[k] = resolveValue(val, cctx)
				}
			}
		}
		return substituteTemplate(tmplStr, cctx, extra)
	}
	return ""
}

func substituteTemplate(s string, cctx *cfnContext, extra map[string]string) string {
	var result strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '$' && i+1 < len(s) && s[i+1] == '{' {
			end := strings.Index(s[i+2:], "}")
			if end >= 0 {
				varName := s[i+2 : i+2+end]
				if v, ok := extra[varName]; ok {
					result.WriteString(v)
				} else {
					result.WriteString(resolveRef(varName, cctx))
				}
				i = i + 2 + end + 1
				continue
			}
		}
		result.WriteByte(s[i])
		i++
	}
	return result.String()
}

func resolveFnJoin(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	sep, ok := arr[0].(string)
	if !ok {
		sep = ""
	}
	items, ok := arr[1].([]interface{})
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, resolveValue(item, cctx))
	}
	return strings.Join(parts, sep)
}

func resolveFnSelect(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	idxStr := resolveValue(arr[0], cctx)
	var idx int
	_, _ = fmt.Sscanf(idxStr, "%d", &idx)
	items, ok := arr[1].([]interface{})
	if !ok || idx < 0 || idx >= len(items) {
		return ""
	}
	return resolveValue(items[idx], cctx)
}

func resolveFnSplitFirst(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	sep := resolveValue(arr[0], cctx)
	s := resolveValue(arr[1], cctx)
	parts := strings.SplitN(s, sep, 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return s
}

func resolveFnBase64(args interface{}, cctx *cfnContext) string {
	s := resolveValue(args, cctx)
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func resolveFnGetAtt(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 2 {
		return ""
	}
	logicalID, ok := arr[0].(string)
	if !ok {
		return ""
	}
	attr, ok := arr[1].(string)
	if !ok {
		return ""
	}
	if dr, ok := cctx.resources[logicalID]; ok {
		switch attr {
		case "Arn", "KeyArn":
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "TopicArn":
			// AWS::SNS::Topic GetAtt TopicArn returns the ARN.
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "Value":
			// AWS::SSM::Parameter GetAtt Value — physical ID is the parameter name.
			return dr.PhysicalID
		case "RootResourceId":
			// AWS::ApiGateway::RestApi GetAtt RootResourceId — stored as extra in PhysicalID with prefix.
			if strings.HasPrefix(dr.PhysicalID, "root:") {
				return strings.TrimPrefix(dr.PhysicalID, "root:")
			}
			// Fallback: the metadata map stores it separately.
			if v, ok := dr.Metadata["RootResourceId"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "InvokeURL":
			// AWS::ApiGateway::Stage GetAtt InvokeURL.
			if v, ok := dr.Metadata["InvokeURL"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "Name":
			// AWS::StepFunctions::StateMachine GetAtt Name.
			if v, ok := dr.Metadata["Name"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "RepositoryUri":
			// AWS::ECR::Repository GetAtt RepositoryUri.
			if v, ok := dr.Metadata["RepositoryUri"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.ARN
		case "ProviderName":
			// AWS::Cognito::UserPool GetAtt ProviderName.
			if v, ok := dr.Metadata["ProviderName"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "ProviderURL":
			// AWS::Cognito::UserPool GetAtt ProviderURL.
			if v, ok := dr.Metadata["ProviderURL"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "DomainName":
			// AWS::CloudFront::Distribution GetAtt DomainName.
			if v, ok := dr.Metadata["DomainName"]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		case "StreamArn":
			// AWS::Kinesis::Stream GetAtt StreamArn.
			if dr.ARN != "" {
				return dr.ARN
			}
			return dr.PhysicalID
		case "Endpoint.Address", "Endpoint.Port",
			"ConfigurationEndpoint.Address", "ConfigurationEndpoint.Port",
			"RedisEndPoint.Address", "RedisEndPoint.Port",
			"PrimaryEndPoint.Address", "PrimaryEndPoint.Port":
			// AWS::RDS::DBInstance and AWS::ElastiCache::* endpoint GetAtts.
			if v, ok := dr.Metadata[attr]; ok {
				return fmt.Sprintf("%v", v)
			}
			return dr.PhysicalID
		default:
			return dr.PhysicalID
		}
	}
	return logicalID + "." + attr
}

// resolveDynamicRef resolves {{resolve:ssm:/path}} and
// {{resolve:ssm-secure:/path}} dynamic references found inside CFN property
// strings. Other reference types are returned unchanged.
func (d *StackDeployer) resolveDynamicRef(ctx context.Context, s string, cctx *cfnContext) string {
	const prefix = "{{resolve:"
	const suffix = "}}"
	if !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, suffix) {
		return s
	}
	inner := s[len(prefix) : len(s)-len(suffix)]
	// inner is like "ssm:/my/param" or "ssm-secure:/my/param" or "ssm:/my/param:3"
	colonIdx := strings.Index(inner, ":")
	if colonIdx < 0 {
		return s
	}
	service := inner[:colonIdx]
	rest := inner[colonIdx+1:]
	switch service {
	case "ssm", "ssm-secure":
		// rest may be "/path" or "/path:version" — ignore version for now.
		paramName := rest
		if idx := strings.LastIndex(rest, ":"); idx > 0 {
			// Only strip version if the part after the last colon is a number.
			maybeSuffix := rest[idx+1:]
			isNum := len(maybeSuffix) > 0
			for _, ch := range maybeSuffix {
				if ch < '0' || ch > '9' {
					isNum = false
					break
				}
			}
			if isNum {
				paramName = rest[:idx]
			}
		}
		body := map[string]string{"Name": paramName}
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return s
		}
		req := &AWSRequest{
			Service:   "ssm",
			Operation: "GetParameter",
			Body:      bodyBytes,
			Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
			Params:    map[string]string{},
		}
		resp, _, routeErr := d.dispatch(ctx, req, cctx.stackName)
		if routeErr != nil || resp == nil {
			return s
		}
		var result struct {
			Parameter struct {
				Value string `json:"Value"`
			} `json:"Parameter"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr != nil {
			return s
		}
		return result.Parameter.Value
	default:
		return s
	}
}

func resolveFnIf(args interface{}, cctx *cfnContext) string {
	arr, ok := args.([]interface{})
	if !ok || len(arr) < 3 {
		return ""
	}
	condName, ok := arr[0].(string)
	if !ok {
		return ""
	}
	if cctx.conditions[condName] {
		return resolveValue(arr[1], cctx)
	}
	return resolveValue(arr[2], cctx)
}

// resolveStringProp resolves a property value from props using the cfnContext.
func resolveStringProp(props map[string]interface{}, key, fallback string, cctx *cfnContext) string {
	if props == nil {
		return fallback
	}
	v, ok := props[key]
	if !ok {
		return fallback
	}
	result := resolveValue(v, cctx)
	if result == "" {
		return fallback
	}
	return result
}

// deployGenericStub handles unknown CloudFormation resource types by generating a
// synthetic ARN and persisting the resource properties in cfnStubNamespace.
func (d *StackDeployer) deployGenericStub(
	ctx context.Context,
	logicalID string,
	resType string,
	props map[string]interface{},
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Build a deterministic ARN from the resource type and logical ID.
	// Format: arn:aws:{service}:{region}:{acct}:{typeSlug}/{logicalID}
	parts := strings.SplitN(resType, "::", 3) // ["AWS", "Service", "ResourceType"]
	service := ""
	rtype := logicalID
	if len(parts) == 3 {
		service = strings.ToLower(parts[1])
		rtype = strings.ToLower(parts[2])
	}
	arn := fmt.Sprintf("arn:aws:%s:%s:%s:%s/%s", service, cctx.region, cctx.accountID, rtype, logicalID)

	if d.state != nil && props != nil {
		data, err := json.Marshal(props)
		if err == nil {
			key := fmt.Sprintf("%s/%s/%s", cctx.accountID, cctx.region, logicalID)
			_ = d.state.Put(ctx, cfnStubNamespace, key, data)
		}
	}

	return DeployedResource{
		LogicalID:  logicalID,
		Type:       resType,
		PhysicalID: logicalID,
		ARN:        arn,
	}, 0, nil
}

// parseCFNTemplate attempts JSON then YAML unmarshalling of a CloudFormation template.
func parseCFNTemplate(cfn string) (*cfnTemplate, error) {
	var tmpl cfnTemplate
	if err := json.Unmarshal([]byte(cfn), &tmpl); err == nil {
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	if err := yaml.Unmarshal([]byte(cfn), &tmpl); err == nil {
		if len(tmpl.Resources) > 0 {
			return &tmpl, nil
		}
	}
	// Try once more with JSON for better error messages on empty templates.
	if err := json.Unmarshal([]byte(cfn), &tmpl); err != nil {
		return nil, fmt.Errorf("invalid CloudFormation template (JSON: %w)", err)
	}
	return &tmpl, nil
}

// marshalToJSON marshals v to a JSON string. Returns "" on nil or error.
func marshalToJSON(v interface{}) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// deployLambdaEventSourceMapping creates a Lambda event source mapping for the
// given CFN resource (AWS::Lambda::EventSourceMapping).
func (d *StackDeployer) deployLambdaEventSourceMapping(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	functionName := resolveStringProp(props, "FunctionName", "", cctx)
	eventSourceArn := resolveStringProp(props, "EventSourceArn", "", cctx)
	startingPosition := resolveStringProp(props, "StartingPosition", "TRIM_HORIZON", cctx)

	bodyMap := map[string]interface{}{
		"FunctionName":     functionName,
		"EventSourceArn":   eventSourceArn,
		"StartingPosition": startingPosition,
	}
	if batchSize, ok := props["BatchSize"]; ok {
		bodyMap["BatchSize"] = batchSize
	}
	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal esm body: %w", err)
	}

	req := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/event-source-mappings/",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
		Body:      bodyBytes,
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)

	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::Lambda::EventSourceMapping",
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result ESMConfig
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.UUID
		}
	}

	return dr, cost, nil
}
