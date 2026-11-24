package emulator

import (
	"context"
	"fmt"
	"strings"
)

// cfn_resource_table.go is the one list of the resource types substrate's CloudFormation
// emulates (#1605): the deploy helper each type dispatches to. [StackDeployer.dispatchResource]
// routes through it, and strict mode's "is this type emulated" question reads it, so the two
// cannot disagree — a type is emulated exactly when it has an entry here.

// cfnCDKMetadataType is the resource the AWS CDK adds to every stack it synthesizes.
const cfnCDKMetadataType = "AWS::CDK::Metadata"

// cfnDeployFunc deploys one resource of a type: the logical ID, its declared properties, the
// event stream it records into and the resolution context.
type cfnDeployFunc func(
	d *StackDeployer, ctx context.Context, logicalID string, props map[string]interface{},
	streamID string, cctx *cfnContext,
) (DeployedResource, float64, error)

// cfnDeployOpenSearch is the deploy helper for both OpenSearch domain types, which share one
// helper that is told which of the two it is deploying.
func cfnDeployOpenSearch(typ string) cfnDeployFunc {
	return func(d *StackDeployer, ctx context.Context, logicalID string, props map[string]interface{},
		_ string, cctx *cfnContext) (DeployedResource, float64, error) {
		return d.deployOpenSearchDomain(ctx, typ, logicalID, props, cctx)
	}
}

// cfnResourceDeployers maps each emulated resource type to its deploy helper. A type absent
// from it is not emulated: strict mode refuses a template declaring one (see
// [cfnUnemulatedTypes]), and lenient mode deploys it with [StackDeployer.deployGenericStub].
var cfnResourceDeployers = map[string]cfnDeployFunc{
	"AWS::IAM::Policy":                            (*StackDeployer).deployIAMPolicy,
	"AWS::IAM::Role":                              (*StackDeployer).deployIAMRole,
	"AWS::IAM::ManagedPolicy":                     (*StackDeployer).deployIAMManagedPolicy,
	"AWS::IAM::User":                              (*StackDeployer).deployIAMUser,
	"AWS::IAM::Group":                             (*StackDeployer).deployIAMGroup,
	"AWS::S3::Bucket":                             (*StackDeployer).deployS3Bucket,
	"AWS::S3::BucketPolicy":                       (*StackDeployer).deployS3BucketPolicy,
	"AWS::Lambda::Function":                       (*StackDeployer).deployLambdaFunction,
	"AWS::SQS::Queue":                             (*StackDeployer).deploySQSQueue,
	"AWS::DynamoDB::Table":                        (*StackDeployer).deployDynamoDBTable,
	"AWS::EC2::VPC":                               (*StackDeployer).deployEC2VPC,
	"AWS::EC2::Subnet":                            (*StackDeployer).deployEC2Subnet,
	"AWS::EC2::SecurityGroup":                     (*StackDeployer).deployEC2SecurityGroup,
	"AWS::EC2::InternetGateway":                   (*StackDeployer).deployEC2InternetGateway,
	"AWS::EC2::RouteTable":                        (*StackDeployer).deployEC2RouteTable,
	"AWS::EC2::Instance":                          (*StackDeployer).deployEC2Instance,
	"AWS::ElasticLoadBalancing::LoadBalancer":     (*StackDeployer).deployELBClassicLoadBalancer,
	"AWS::ElasticLoadBalancingV2::TargetGroup":    (*StackDeployer).deployELBTargetGroup,
	"AWS::ElasticLoadBalancingV2::LoadBalancer":   (*StackDeployer).deployELBLoadBalancer,
	"AWS::ElasticLoadBalancingV2::Listener":       (*StackDeployer).deployELBListener,
	"AWS::ElasticLoadBalancingV2::ListenerRule":   (*StackDeployer).deployELBListenerRule,
	"AWS::Route53::HostedZone":                    (*StackDeployer).deployRoute53HostedZone,
	"AWS::Route53::HealthCheck":                   (*StackDeployer).deployRoute53HealthCheck,
	"AWS::Route53::RecordSet":                     (*StackDeployer).deployRoute53RecordSet,
	"AWS::Route53::RecordSetGroup":                (*StackDeployer).deployRoute53RecordSetGroup,
	"AWS::KMS::Key":                               (*StackDeployer).deployKMSKey,
	"AWS::KMS::Alias":                             (*StackDeployer).deployKMSAlias,
	"AWS::KMS::ReplicaKey":                        (*StackDeployer).deployKMSReplicaKey,
	"AWS::SecretsManager::Secret":                 (*StackDeployer).deploySecret,
	"AWS::SecretsManager::RotationSchedule":       (*StackDeployer).deploySecretRotationSchedule,
	"AWS::SecretsManager::SecretTargetAttachment": (*StackDeployer).deploySecretTargetAttachment,
	"AWS::SSM::Parameter":                         (*StackDeployer).deploySSMParameter,
	"AWS::SSM::Association":                       (*StackDeployer).deploySSMAssociation,
	"AWS::SNS::Topic":                             (*StackDeployer).deploySNSTopic,
	"AWS::SNS::Subscription":                      (*StackDeployer).deploySNSSubscription,
	"AWS::SNS::TopicPolicy":                       (*StackDeployer).deploySNSTopicPolicy,
	"AWS::Logs::LogGroup":                         (*StackDeployer).deployLogsLogGroup,
	"AWS::Logs::LogStream":                        (*StackDeployer).deployLogsLogStream,
	"AWS::Events::Rule":                           (*StackDeployer).deployEventsRule,
	"AWS::Events::EventBus":                       (*StackDeployer).deployEventsEventBus,
	"AWS::CloudWatch::Alarm":                      (*StackDeployer).deployCloudWatchAlarm,
	"AWS::CloudWatch::Dashboard":                  (*StackDeployer).deployCloudWatchDashboard,
	// v0.19.0 — API Gateway and ACM.
	"AWS::CertificateManager::Certificate": (*StackDeployer).deployACMCertificate,
	"AWS::ApiGateway::RestApi":             (*StackDeployer).deployAPIGatewayRestAPI,
	"AWS::ApiGateway::Authorizer":          (*StackDeployer).deployAPIGatewayAuthorizer,
	"AWS::ApiGateway::Resource":            (*StackDeployer).deployAPIGatewayResource,
	"AWS::ApiGateway::Method":              (*StackDeployer).deployAPIGatewayMethod,
	"AWS::ApiGateway::Deployment":          (*StackDeployer).deployAPIGatewayDeployment,
	"AWS::ApiGateway::Stage":               (*StackDeployer).deployAPIGatewayStage,
	"AWS::ApiGateway::ApiKey":              (*StackDeployer).deployAPIGatewayAPIKey,
	"AWS::ApiGateway::UsagePlan":           (*StackDeployer).deployAPIGatewayUsagePlan,
	"AWS::ApiGateway::UsagePlanKey":        (*StackDeployer).deployAPIGatewayUsagePlanKey,
	"AWS::ApiGatewayV2::Api":               (*StackDeployer).deployAPIGatewayV2Api,
	"AWS::ApiGatewayV2::Route":             (*StackDeployer).deployAPIGatewayV2Route,
	"AWS::ApiGatewayV2::Integration":       (*StackDeployer).deployAPIGatewayV2Integration,
	"AWS::ApiGatewayV2::Stage":             (*StackDeployer).deployAPIGatewayV2Stage,
	"AWS::ApiGatewayV2::Authorizer":        (*StackDeployer).deployAPIGatewayV2Authorizer,
	// v0.20.0 — Step Functions.
	"AWS::StepFunctions::StateMachine": (*StackDeployer).deployStepFunctionsStateMachine,
	"AWS::StepFunctions::Activity":     (*StackDeployer).deployStepFunctionsActivity,
	// v0.21.0 — ECS and ECR.
	"AWS::ECR::Repository":       (*StackDeployer).deployECRRepository,
	"AWS::ECS::Cluster":          (*StackDeployer).deployECSCluster,
	"AWS::ECS::TaskDefinition":   (*StackDeployer).deployECSTaskDefinition,
	"AWS::ECS::Service":          (*StackDeployer).deployECSService,
	"AWS::ECS::CapacityProvider": (*StackDeployer).deployECSCapacityProvider,
	// v0.22.0 — Cognito.
	"AWS::Cognito::UserPool":                   (*StackDeployer).deployCognitoUserPool,
	"AWS::Cognito::UserPoolClient":             (*StackDeployer).deployCognitoUserPoolClient,
	"AWS::Cognito::UserPoolGroup":              (*StackDeployer).deployCognitoUserPoolGroup,
	"AWS::Cognito::UserPoolDomain":             (*StackDeployer).deployCognitoUserPoolDomain,
	"AWS::Cognito::IdentityPool":               (*StackDeployer).deployCognitoIdentityPool,
	"AWS::Cognito::IdentityPoolRoleAttachment": (*StackDeployer).deployCognitoIdentityPoolRoleAttachment,
	// v0.23.0 — Kinesis and CloudFront.
	"AWS::Kinesis::Stream":                            (*StackDeployer).deployKinesisStream,
	"AWS::Kinesis::StreamConsumer":                    (*StackDeployer).deployKinesisStreamConsumer,
	"AWS::CloudFront::Distribution":                   (*StackDeployer).deployCloudFrontDistribution,
	"AWS::CloudFront::CloudFrontOriginAccessIdentity": (*StackDeployer).deployCloudFrontOAI,
	// v0.25.0 — RDS and ElastiCache.
	"AWS::RDS::DBSubnetGroup":            (*StackDeployer).deployRDSDBSubnetGroup,
	"AWS::RDS::DBParameterGroup":         (*StackDeployer).deployRDSDBParameterGroup,
	"AWS::RDS::DBCluster":                (*StackDeployer).deployRDSDBCluster,
	"AWS::RDS::DBInstance":               (*StackDeployer).deployRDSDBInstance,
	"AWS::ElastiCache::SubnetGroup":      (*StackDeployer).deployElastiCacheSubnetGroup,
	"AWS::ElastiCache::ParameterGroup":   (*StackDeployer).deployElastiCacheParameterGroup,
	"AWS::ElastiCache::CacheCluster":     (*StackDeployer).deployElastiCacheCacheCluster,
	"AWS::ElastiCache::ReplicationGroup": (*StackDeployer).deployElastiCacheReplicationGroup,
	// v0.26.0 — EFS and Glue.
	"AWS::EFS::FileSystem":  (*StackDeployer).deployEFSFileSystem,
	"AWS::EFS::AccessPoint": (*StackDeployer).deployEFSAccessPoint,
	"AWS::EFS::MountTarget": (*StackDeployer).deployEFSMountTarget,
	"AWS::Glue::Database":   (*StackDeployer).deployGlueDatabase,
	"AWS::Glue::Connection": (*StackDeployer).deployGlueConnection,
	"AWS::Glue::Table":      (*StackDeployer).deployGlueTable,
	"AWS::Glue::Crawler":    (*StackDeployer).deployGlueCrawler,
	"AWS::Glue::Job":        (*StackDeployer).deployGlueJob,
	// v0.27.0 — Budgets.
	"AWS::Budgets::Budget": (*StackDeployer).deployBudgetsBudget,
	// v0.28.0 — SES v2 and Firehose.
	"AWS::SES::EmailIdentity":              (*StackDeployer).deploySESv2EmailIdentity,
	"AWS::KinesisFirehose::DeliveryStream": (*StackDeployer).deployFirehoseDeliveryStream,
	// v0.41.0 — Elastic IPs and NAT Gateways.
	"AWS::EC2::EIP":        (*StackDeployer).deployEC2EIP,
	"AWS::EC2::NatGateway": (*StackDeployer).deployEC2NatGateway,
	// v0.30.0 — Lambda ESM.
	"AWS::Lambda::EventSourceMapping": (*StackDeployer).deployLambdaEventSourceMapping,
	// v0.31.0 — AppSync.
	"AWS::AppSync::GraphQLApi":            (*StackDeployer).deployAppSyncGraphQLApi,
	"AWS::AppSync::DataSource":            (*StackDeployer).deployAppSyncDataSource,
	"AWS::AppSync::Resolver":              (*StackDeployer).deployAppSyncResolver,
	"AWS::AppSync::FunctionConfiguration": (*StackDeployer).deployAppSyncFunction,
	// v0.43.0 — FSx.
	"AWS::FSx::FileSystem": (*StackDeployer).deployFSxFileSystem,
	// v0.34.0 — RDS Aurora cluster and MSK.
	"AWS::MSK::Cluster": (*StackDeployer).deployMSKCluster,
	// v0.32.0 — extended CFN stubs.
	"AWS::OpenSearchService::Domain":     cfnDeployOpenSearch("AWS::OpenSearchService::Domain"),
	"AWS::Elasticsearch::Domain":         cfnDeployOpenSearch("AWS::Elasticsearch::Domain"),
	"AWS::WAFv2::WebACL":                 (*StackDeployer).deployWAFv2WebACL,
	"AWS::Backup::BackupPlan":            (*StackDeployer).deployBackupBackupPlan,
	"AWS::Backup::BackupSelection":       (*StackDeployer).deployBackupBackupSelection,
	"AWS::CodeBuild::Project":            (*StackDeployer).deployCodeBuildProject,
	"AWS::CodePipeline::Pipeline":        (*StackDeployer).deployCodePipelinePipeline,
	"AWS::CodeDeploy::Application":       (*StackDeployer).deployCodeDeployApplication,
	"AWS::CodeDeploy::DeploymentGroup":   (*StackDeployer).deployCodeDeployDeploymentGroup,
	"AWS::CloudTrail::Trail":             (*StackDeployer).deployCloudTrailTrail,
	"AWS::Config::ConfigRule":            (*StackDeployer).deployConfigConfigRule,
	"AWS::Config::ConfigurationRecorder": (*StackDeployer).deployConfigConfigurationRecorder,
	"AWS::Config::DeliveryChannel":       (*StackDeployer).deployConfigDeliveryChannel,
	"AWS::Transfer::Server":              (*StackDeployer).deployTransferServer,
	"AWS::Athena::WorkGroup":             (*StackDeployer).deployAthenaWorkGroup,
	// v0.77.0 — resource types whose API handlers already existed but which fell
	// through to the generic stub (#388).
	"AWS::EC2::LaunchTemplate":       (*StackDeployer).deployEC2LaunchTemplate,
	"AWS::IAM::InstanceProfile":      (*StackDeployer).deployIAMInstanceProfile,
	"AWS::EC2::SecurityGroupIngress": (*StackDeployer).deployEC2SecurityGroupIngress,
	"AWS::EC2::SecurityGroupEgress":  (*StackDeployer).deployEC2SecurityGroupEgress,
	"AWS::EC2::VPCGatewayAttachment": (*StackDeployer).deployEC2VPCGatewayAttachment,
	// v0.131.0 — the record the CDK adds to every synthesized stack (#1605), and the four types
	// every ordinary CDK app emits that had no helper, so strict mode would have refused the app.
	cfnCDKMetadataType:                      (*StackDeployer).deployCDKMetadata,
	"AWS::EC2::Route":                       (*StackDeployer).deployEC2Route,
	"AWS::EC2::SubnetRouteTableAssociation": (*StackDeployer).deployEC2SubnetRouteTableAssociation,
	"AWS::Lambda::Permission":               (*StackDeployer).deployLambdaPermission,
	"AWS::SQS::QueuePolicy":                 (*StackDeployer).deployQueuePolicy,
	// v0.134.0 — the types the CDK emits for a function's currentVersion and its aliases (#1613).
	"AWS::Lambda::Version": (*StackDeployer).deployLambdaVersion,
	"AWS::Lambda::Alias":   (*StackDeployer).deployLambdaAlias,
	// The identity providers a role's trust policy names (#1613).
	"AWS::IAM::OIDCProvider": (*StackDeployer).deployIAMOIDCProvider,
	"AWS::IAM::SAMLProvider": (*StackDeployer).deployIAMSAMLProvider,
}

// cfnEmulatedType reports whether substrate's CloudFormation emulates a resource type.
func cfnEmulatedType(typ string) bool {
	_, ok := cfnResourceDeployers[typ]
	return ok
}

// deployCDKMetadata deploys an AWS::CDK::Metadata resource, which is a modeled no-op.
//
// The CDK adds one to every stack it synthesizes to report which construct libraries the
// app used; CloudFormation records it and creates nothing in any service, so there is
// nothing for substrate to dispatch either. Its declared properties are kept in the stub
// namespace, as a stub type's are, so a delete has the same record to remove (see
// cfnStubDeleteTypes). The physical ID is the logical ID: AWS publishes no format for it.
func (d *StackDeployer) deployCDKMetadata(
	ctx context.Context, logicalID string, props map[string]interface{}, _ string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	if err := d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props); err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCDKMetadata stubStore: %w", err)
	}
	return DeployedResource{LogicalID: logicalID, Type: cfnCDKMetadataType, PhysicalID: logicalID}, 0, nil
}

// cfnTypeServiceOverrides maps CFN service segments whose lower-cased form is
// not the substrate plugin name.
var cfnTypeServiceOverrides = map[string]string{
	"elasticloadbalancingv2": "elbv2",
	"certificatemanager":     "acm",
	"opensearchservice":      "opensearch",
	"servicediscovery":       "servicediscovery",
	"elasticache":            "elasticache",
	"applicationautoscaling": "application-autoscaling",
	"secretsmanager":         "secretsmanager",
	"stepfunctions":          "states",
	"serverless":             "lambda",
}

// servicePluginLoaded reports whether a plugin for the CFN resource type's
// service is registered. A generic stub for a type whose plugin *is* loaded
// usually means the type needs wiring rather than that the service is
// unsupported, which is the distinction the stub warning should surface (#388).
func (d *StackDeployer) servicePluginLoaded(resType string) bool {
	parts := strings.SplitN(resType, "::", 3)
	if len(parts) < 2 {
		return false
	}
	service := strings.ToLower(parts[1])
	if override, ok := cfnTypeServiceOverrides[service]; ok {
		service = override
	}
	for _, name := range d.registry.Names() {
		if name == service {
			return true
		}
	}
	return false
}
