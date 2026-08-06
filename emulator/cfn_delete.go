package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// cfnDeleteRequestFunc builds the request that deletes one deployed resource.
//
// It returns a request rather than performing the delete so every entry in
// [cfnResourceDeleters] stays a data declaration: the sweep owns ordering, error
// handling and event recording once, and an entry can be unit-tested by inspecting
// the request it builds without dispatching anything. It also accommodates all four
// request shapes the deploy paths use — an action name with query parameters, a JSON
// body, a REST path, and a path plus parameters — which a single "delete by physical
// ID" helper could not.
//
// props and cctx are the resource's declared Properties and a resolution context
// rebuilt from the stored template, for the types whose delete needs a parent
// identifier the DeployedResource does not carry. A nil return means this resource
// cannot be deleted from what is known about it, which the sweep records as a skip
// rather than as a success.
type cfnDeleteRequestFunc func(d *StackDeployer, dr DeployedResource,
	props map[string]interface{}, cctx *cfnContext) *AWSRequest

// cfnResourceDeleters maps a CloudFormation resource type to the request that
// deletes it.
//
// A type that is absent has no modeled delete, and the sweep reports it as
// DELETE_SKIPPED naming the type rather than treating it as deleted — the whole
// point of #518 is that a claim of cleanliness must be true, so a gap is stated
// rather than implied.
//
// Two things this table deliberately does not do:
//
//   - It does not assume the physical ID is the delete identifier. SQS records the
//     queue *name* as its physical ID while DeleteQueue requires a QueueUrl; SNS
//     records the topic ARN while its own state key is name-derived; an API Gateway
//     child needs its parent's ID, which lives in the template rather than in the
//     child's DeployedResource.
//   - It does not guess an operation name from the type. KMS has no DeleteKey at
//     all — "you can schedule the deletion of a KMS key" — so its entry dispatches
//     ScheduleKeyDeletion. Each entry's operation and required parameters were read
//     off the plugin that must accept it.
var cfnResourceDeleters = map[string]cfnDeleteRequestFunc{
	// --- Storage and data ---------------------------------------------------
	"AWS::S3::Bucket": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		// The bucket must be empty, which DeleteBucket enforces. A stack whose
		// bucket holds objects the stack did not create reports DELETE_FAILED,
		// which is what real CloudFormation does too.
		return &AWSRequest{Service: "s3", Operation: "DELETE", Path: "/" + dr.PhysicalID,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::DynamoDB::Table": jsonBodyDeleter("dynamodb", "DeleteTable", "TableName"),
	"AWS::Kinesis::Stream": jsonBodyDeleter("kinesis", "DeleteStream", "StreamName"),
	"AWS::KinesisFirehose::DeliveryStream": jsonBodyDeleter("firehose", "DeleteDeliveryStream",
		"DeliveryStreamName"),
	"AWS::EFS::FileSystem":  pathDeleter("efs", "/2015-02-01/file-systems/"),
	"AWS::EFS::AccessPoint": pathDeleter("efs", "/2015-02-01/access-points/"),
	"AWS::EFS::MountTarget": pathDeleter("efs", "/2015-02-01/mount-targets/"),
	"AWS::FSx::FileSystem":  jsonBodyDeleter("fsx", "DeleteFileSystem", "FileSystemId"),

	// --- Messaging ----------------------------------------------------------
	"AWS::SQS::Queue": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// DeleteQueue takes a QueueUrl, and the physical ID is the queue name, so
		// the URL is rebuilt the same way CreateQueue derived it. Passing the name
		// would look right and delete nothing.
		return &AWSRequest{Service: "sqs", Operation: "DeleteQueue",
			Headers: map[string]string{},
			Params:  map[string]string{"QueueUrl": sqsQueueURL(cctx.region, cctx.accountID, dr.PhysicalID)}}
	},
	"AWS::SNS::Topic": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// The physical ID is already the ARN DeleteTopic wants, but a stack
		// deployed before that was recorded may hold a bare name, so it is
		// normalized rather than trusted.
		arn := dr.PhysicalID
		if !strings.HasPrefix(arn, "arn:") {
			arn = fmt.Sprintf("arn:aws:sns:%s:%s:%s", cctx.region, cctx.accountID, arn)
		}
		return &AWSRequest{Service: "sns", Operation: "DeleteTopic",
			Headers: map[string]string{}, Params: map[string]string{"TopicArn": arn}}
	},
	"AWS::SNS::Subscription": queryDeleter("sns", "Unsubscribe", "SubscriptionArn"),
	"AWS::SNS::TopicPolicy": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		// There is no DeleteTopicPolicy: a topic policy is an attribute, and
		// removing it means setting it back to empty — the mirror of the
		// SetTopicAttributes the deploy dispatched. The topic itself, if the stack
		// also owns it, is deleted separately by its own entry.
		return &AWSRequest{Service: "sns", Operation: "SetTopicAttributes",
			Headers: map[string]string{},
			Params: map[string]string{
				"Action": "SetTopicAttributes", "TopicArn": dr.PhysicalID,
				"AttributeName": "Policy", "AttributeValue": "",
			}}
	},
	"AWS::Events::Rule": jsonBodyDeleter("eventbridge", "DeleteRule", "Name"),

	// --- Compute ------------------------------------------------------------
	"AWS::Lambda::Function": pathDeleter("lambda", "/2015-03-31/functions/"),
	"AWS::Lambda::EventSourceMapping": pathDeleter("lambda",
		"/2015-03-31/event-source-mappings/"),
	"AWS::ECS::Cluster": jsonBodyDeleter("ecs", "DeleteCluster", "cluster"),
	"AWS::ECS::Service": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// A service is identified by cluster plus service; the cluster is the
		// template's, not the service's own physical ID.
		body, err := json.Marshal(map[string]interface{}{
			"cluster": resolveStringProp(props, "Cluster", "default", cctx),
			"service": dr.PhysicalID,
			// "You must have a service that is either ACTIVE or DRAINING" —
			// force lets a service with running tasks go, which is what a stack
			// delete needs.
			"force": true,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "ecs", Operation: "DeleteService", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::ECS::TaskDefinition": jsonBodyDeleter("ecs", "DeregisterTaskDefinition",
		"taskDefinition"),
	"AWS::ECR::Repository": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{
			"repositoryName": dr.PhysicalID,
			// A repository holding images is refused otherwise, and the images a
			// stack's repository holds were pushed by something other than the
			// stack.
			"force": true,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "ecr", Operation: "DeleteRepository", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},

	// --- EC2 and networking -------------------------------------------------
	"AWS::EC2::Instance": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		return &AWSRequest{Service: "ec2", Operation: "TerminateInstances",
			Headers: map[string]string{},
			Params:  map[string]string{"InstanceId.1": dr.PhysicalID}}
	},
	"AWS::EC2::VPC":             queryDeleter("ec2", "DeleteVpc", "VpcId"),
	"AWS::EC2::Subnet":          queryDeleter("ec2", "DeleteSubnet", "SubnetId"),
	"AWS::EC2::SecurityGroup":   queryDeleter("ec2", "DeleteSecurityGroup", "GroupId"),
	"AWS::EC2::InternetGateway": queryDeleter("ec2", "DeleteInternetGateway", "InternetGatewayId"),
	"AWS::EC2::RouteTable":      queryDeleter("ec2", "DeleteRouteTable", "RouteTableId"),
	"AWS::EC2::NatGateway":      queryDeleter("ec2", "DeleteNatGateway", "NatGatewayId"),
	"AWS::EC2::LaunchTemplate":  queryDeleter("ec2", "DeleteLaunchTemplate", "LaunchTemplateId"),
	"AWS::EC2::EIP": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		// An address is released, not deleted — there is no DeleteAddress.
		return &AWSRequest{Service: "ec2", Operation: "ReleaseAddress",
			Headers: map[string]string{},
			Params:  map[string]string{"AllocationId": dr.PhysicalID}}
	},
	"AWS::EC2::SecurityGroupIngress": sgRuleDeleter("RevokeSecurityGroupIngress"),
	"AWS::EC2::SecurityGroupEgress":  sgRuleDeleter("RevokeSecurityGroupEgress"),

	// --- Load balancing -----------------------------------------------------
	"AWS::ElasticLoadBalancingV2::LoadBalancer": queryDeleter("elasticloadbalancing",
		"DeleteLoadBalancer", "LoadBalancerArn"),
	"AWS::ElasticLoadBalancingV2::TargetGroup": queryDeleter("elasticloadbalancing",
		"DeleteTargetGroup", "TargetGroupArn"),
	"AWS::ElasticLoadBalancingV2::Listener": queryDeleter("elasticloadbalancing",
		"DeleteListener", "ListenerArn"),
	"AWS::ElasticLoadBalancingV2::ListenerRule": queryDeleter("elasticloadbalancing",
		"DeleteRule", "RuleArn"),

	// --- Identity and secrets -----------------------------------------------
	"AWS::IAM::Role":   iamDeleter("DeleteRole", "RoleName"),
	"AWS::IAM::Policy": iamDeleter("DeletePolicy", "PolicyArn"),
	"AWS::IAM::InstanceProfile": iamDeleter("DeleteInstanceProfile",
		"InstanceProfileName"),
	"AWS::KMS::Key": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		// KMS models no DeleteKey: a key is scheduled for deletion. The minimum
		// window is 7 days, which is what CloudFormation itself requests.
		body, err := json.Marshal(map[string]interface{}{
			"KeyId": dr.PhysicalID, "PendingWindowInDays": 7,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "kms", Operation: "ScheduleKeyDeletion", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	// A replica key deploys through deployKMSKey, so it is a key as far as substrate
	// is concerned and is scheduled for deletion the same way.
	"AWS::KMS::ReplicaKey": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{
			"KeyId": dr.PhysicalID, "PendingWindowInDays": 7,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "kms", Operation: "ScheduleKeyDeletion", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::KMS::Alias": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{"AliasName": dr.PhysicalID})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "kms", Operation: "DeleteAlias", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::SecretsManager::Secret": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{
			"SecretId": dr.PhysicalID,
			// "The default behavior of CloudFormation is to delete the secret
			// with the ForceDeleteWithoutRecovery flag" — so a recreated stack
			// does not collide with a secret still in its recovery window.
			"ForceDeleteWithoutRecovery": true,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "secretsmanager", Operation: "DeleteSecret", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::SSM::Parameter": jsonBodyDeleter("ssm", "DeleteParameter", "Name"),

	// --- Databases and caches -----------------------------------------------
	"AWS::RDS::DBInstance": queryDeleter("rds", "DeleteDBInstance", "DBInstanceIdentifier"),
	"AWS::RDS::DBCluster":  queryDeleter("rds", "DeleteDBCluster", "DBClusterIdentifier"),
	"AWS::RDS::DBParameterGroup": queryDeleter("rds", "DeleteDBParameterGroup",
		"DBParameterGroupName"),
	"AWS::RDS::DBSubnetGroup": queryDeleter("rds", "DeleteDBSubnetGroup",
		"DBSubnetGroupName"),
	"AWS::ElastiCache::CacheCluster": queryDeleter("elasticache", "DeleteCacheCluster",
		"CacheClusterId"),
	"AWS::ElastiCache::ReplicationGroup": queryDeleter("elasticache", "DeleteReplicationGroup",
		"ReplicationGroupId"),
	"AWS::ElastiCache::ParameterGroup": queryDeleter("elasticache", "DeleteCacheParameterGroup",
		"CacheParameterGroupName"),
	"AWS::ElastiCache::SubnetGroup": queryDeleter("elasticache", "DeleteCacheSubnetGroup",
		"CacheSubnetGroupName"),

	// --- Observability -------------------------------------------------------
	"AWS::Logs::LogGroup": jsonBodyDeleter("logs", "DeleteLogGroup", "logGroupName"),
	"AWS::Logs::LogStream": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		group := resolveStringProp(props, "LogGroupName", "", cctx)
		if group == "" {
			return nil
		}
		body, err := json.Marshal(map[string]interface{}{
			"logGroupName": group, "logStreamName": dr.PhysicalID,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "logs", Operation: "DeleteLogStream", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::CloudWatch::Alarm": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		// DeleteAlarms is plural and takes a list, even for one alarm.
		return &AWSRequest{Service: "monitoring", Operation: "DeleteAlarms",
			Headers: map[string]string{},
			Params:  map[string]string{"AlarmNames.member.1": dr.PhysicalID}}
	},

	// --- Analytics and ETL ---------------------------------------------------
	"AWS::Glue::Database":   jsonBodyDeleter("glue", "DeleteDatabase", "Name"),
	"AWS::Glue::Job":        jsonBodyDeleter("glue", "DeleteJob", "JobName"),
	"AWS::Glue::Crawler":    jsonBodyDeleter("glue", "DeleteCrawler", "Name"),
	"AWS::Glue::Connection": jsonBodyDeleter("glue", "DeleteConnection", "ConnectionName"),
	"AWS::Glue::Table": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		db := resolveStringProp(props, "DatabaseName", "", cctx)
		if db == "" {
			return nil
		}
		body, err := json.Marshal(map[string]interface{}{
			"DatabaseName": db, "Name": dr.PhysicalID,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "glue", Operation: "DeleteTable", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::MSK::Cluster": pathDeleter("msk", "/v1/clusters/"),

	// --- Application integration ---------------------------------------------
	"AWS::StepFunctions::StateMachine": jsonBodyDeleter("states", "DeleteStateMachine",
		"stateMachineArn"),
	"AWS::StepFunctions::Activity": jsonBodyDeleter("states", "DeleteActivity",
		"activityArn"),

	// --- API Gateway ---------------------------------------------------------
	// Every child here needs its parent's ID, which lives in the template rather
	// than in the child's own DeployedResource — this is what props and cctx are
	// for.
	"AWS::ApiGateway::RestApi": pathDeleter("apigateway", "/restapis/"),
	"AWS::ApiGateway::Resource": apiGatewayChildDeleter("RestApiId",
		func(api, id string) string { return "/restapis/" + api + "/resources/" + id }),
	"AWS::ApiGateway::Deployment": apiGatewayChildDeleter("RestApiId",
		func(api, id string) string { return "/restapis/" + api + "/deployments/" + id }),
	"AWS::ApiGateway::Stage": apiGatewayChildDeleter("RestApiId",
		func(api, id string) string { return "/restapis/" + api + "/stages/" + id }),
	"AWS::ApiGateway::Authorizer": apiGatewayChildDeleter("RestApiId",
		func(api, id string) string { return "/restapis/" + api + "/authorizers/" + id }),
	"AWS::ApiGateway::Method": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// A method is the only child here needing two parents: the API and the
		// resource it hangs off. Its physical ID is the HTTP verb.
		api := resolveStringProp(props, "RestApiId", "", cctx)
		res := resolveStringProp(props, "ResourceId", "", cctx)
		if api == "" || res == "" {
			return nil
		}
		return &AWSRequest{Service: "apigateway", Operation: "DELETE",
			Path:    "/restapis/" + api + "/resources/" + res + "/methods/" + dr.PhysicalID,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::ApiGateway::ApiKey":    pathDeleter("apigateway", "/apikeys/"),
	"AWS::ApiGateway::UsagePlan": pathDeleter("apigateway", "/usageplans/"),
	"AWS::ApiGatewayV2::Api":     pathDeleter("apigatewayv2", "/v2/apis/"),
	"AWS::ApiGatewayV2::Route": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v2/apis/" + api + "/routes/" + id }),
	"AWS::ApiGatewayV2::Integration": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v2/apis/" + api + "/integrations/" + id }),
	"AWS::ApiGatewayV2::Stage": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v2/apis/" + api + "/stages/" + id }),
	"AWS::ApiGatewayV2::Authorizer": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v2/apis/" + api + "/authorizers/" + id }),

	// --- AppSync -------------------------------------------------------------
	"AWS::AppSync::GraphQLApi": pathDeleter("appsync", "/v1/apis/"),
	"AWS::AppSync::DataSource": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v1/apis/" + api + "/DataSources/" + id }),
	"AWS::AppSync::FunctionConfiguration": apiGatewayChildDeleter("ApiId",
		func(api, id string) string { return "/v1/apis/" + api + "/functions/" + id }),
	"AWS::AppSync::Resolver": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// The physical ID is "TypeName.FieldName" and the path wants the two
		// separately, so the type comes from the template and the field from the
		// physical ID's tail — splitting on the last dot, since a GraphQL field
		// name cannot contain one but this way a type name could.
		api := resolveStringProp(props, "ApiId", "", cctx)
		typeName := resolveStringProp(props, "TypeName", "", cctx)
		field := dr.PhysicalID
		if i := strings.LastIndex(field, "."); i >= 0 {
			field = field[i+1:]
		}
		if api == "" || typeName == "" || field == "" {
			return nil
		}
		return &AWSRequest{Service: "appsync", Operation: "DELETE",
			Path:    "/v1/apis/" + api + "/types/" + typeName + "/resolvers/" + field,
			Headers: map[string]string{}, Params: map[string]string{}}
	},

	// --- Cognito -------------------------------------------------------------
	"AWS::Cognito::UserPool": jsonBodyDeleter("cognito-idp", "DeleteUserPool", "UserPoolId"),
	"AWS::Cognito::UserPoolClient": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		pool := resolveStringProp(props, "UserPoolId", "", cctx)
		if pool == "" {
			return nil
		}
		body, err := json.Marshal(map[string]interface{}{
			"UserPoolId": pool, "ClientId": dr.PhysicalID,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "cognito-idp", Operation: "DeleteUserPoolClient",
			Body: body, Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::Cognito::UserPoolGroup": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		pool := resolveStringProp(props, "UserPoolId", "", cctx)
		if pool == "" {
			return nil
		}
		body, err := json.Marshal(map[string]interface{}{
			"UserPoolId": pool, "GroupName": dr.PhysicalID,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "cognito-idp", Operation: "DeleteGroup", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::Cognito::UserPoolDomain": jsonBodyDeleter("cognito-idp", "DeleteUserPoolDomain",
		"Domain"),
	"AWS::Cognito::IdentityPool": jsonBodyDeleter("cognito-identity", "DeleteIdentityPool",
		"IdentityPoolId"),

	// --- Everything else -----------------------------------------------------
	"AWS::Route53::HostedZone": pathDeleter("route53", "/2013-04-01/hostedzone/"),
	"AWS::Route53::RecordSet": func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		// A record set has no delete operation of its own: it is removed by a
		// change batch carrying Action DELETE, the mirror of the UPSERT that
		// created it. The record is keyed by type and name, which is why both
		// come from the template rather than from the physical ID alone.
		zone := resolveStringProp(props, "HostedZoneId", "", cctx)
		if zone == "" || dr.PhysicalID == "" {
			return nil
		}
		rtype := resolveStringProp(props, "Type", "A", cctx)
		body := `<ChangeResourceRecordSetsRequest ` +
			`xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
			`<ChangeBatch><Changes><Change><Action>DELETE</Action>` +
			`<ResourceRecordSet><Name>` + dr.PhysicalID + `</Name>` +
			`<Type>` + rtype + `</Type></ResourceRecordSet>` +
			`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
		return &AWSRequest{Service: "route53", Operation: "POST",
			Path: "/2013-04-01/hostedzone/" + zone + "/rrset", Body: []byte(body),
			Headers: map[string]string{"Content-Type": "application/xml"},
			Params:  map[string]string{}}
	},
	"AWS::CertificateManager::Certificate": jsonBodyDeleter("acm", "DeleteCertificate",
		"CertificateArn"),
	"AWS::CloudFront::Distribution": pathDeleter("cloudfront", "/2020-05-31/distribution/"),
	"AWS::Budgets::Budget": func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, cctx *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{
			"AccountId": cctx.accountID, "BudgetName": dr.PhysicalID,
		})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "budgets", Operation: "DeleteBudget", Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	},
	"AWS::SES::EmailIdentity": pathDeleter("sesv2", "/v2/email/identities/"),
}

// cfnDeletePreStepFunc builds the requests that must succeed before a resource's own
// delete is dispatched, in the order they are returned. An empty or nil return means
// there is nothing to do, which is the common case.
type cfnDeletePreStepFunc func(d *StackDeployer, dr DeployedResource,
	props map[string]interface{}, cctx *cfnContext) []*AWSRequest

// cfnDeletePreSteps holds the detach-before-delete calls a resource needs, for the
// types whose delete is refused while a subordinate entity still references them.
//
// This is a separate hook rather than a change to [cfnDeleteRequestFunc] because that
// contract is deliberately one request per resource — that is what keeps every entry
// in [cfnResourceDeleters] a data declaration the sweep can inspect without
// dispatching. A pre-step keeps that property: the main request is still the one the
// table declares, and the steps run through [StackDeployer.dispatchResourceDelete] so
// absent-resource tolerance and event recording are identical for both.
//
// A pre-step failure fails the resource. That is the right outcome — a delete
// dispatched after a failed detach would be refused anyway, and reporting the detach's
// own error names what actually went wrong.
var cfnDeletePreSteps = map[string]cfnDeletePreStepFunc{
	// DeleteInstanceProfile is refused while the profile holds a role, and DeleteRole
	// is now refused while a profile holds it — correctly, per both references. So the
	// stack could not converge from either side until something made the detach call
	// (#581). The roles come from the template's Roles list, the same list
	// deployIAMInstanceProfile attached, resolved through the same context so a
	// !Ref to a role in the stack yields the role's generated physical name.
	"AWS::IAM::InstanceProfile": func(_ *StackDeployer, dr DeployedResource,
		props map[string]interface{}, cctx *cfnContext,
	) []*AWSRequest {
		roles := resolveStringList(props["Roles"], cctx)
		steps := make([]*AWSRequest, 0, len(roles))
		for _, roleName := range roles {
			body, err := json.Marshal(map[string]string{
				"InstanceProfileName": dr.PhysicalID,
				"RoleName":            roleName,
			})
			if err != nil {
				continue
			}
			steps = append(steps, &AWSRequest{Service: "iam",
				Operation: "RemoveRoleFromInstanceProfile", Body: body,
				Headers: map[string]string{}, Params: map[string]string{}})
		}
		return steps
	},
}

// cfnStubDeleteTypes are the resource types whose deploy writes properties into
// cfnStubNamespace and dispatches no API call.
//
// They need a delete of their own because a sweep that dispatched nothing for them
// would leave the stub key behind, and a stack redeployed under the same logical ID
// would then read the previous stack's properties — the same "recreated resource
// inherits the old one's configuration" leak #508 fixed inside S3. There is no
// request to build, so they are held here rather than in cfnResourceDeleters.
//
// The four remaining state-only types (CloudFront::CloudFrontOriginAccessIdentity,
// ECS::CapacityProvider, SSM::Association, SecretsManager::SecretTargetAttachment)
// write no state at all: their deploy returns a DeployedResource and nothing else,
// so there is nothing for a sweep to remove and they are reported as skipped.
// Route53::RecordSetGroup is state-only in the same sense but not inert — it
// dispatches one RecordSet per child, and those children are not recorded as
// resources of their own, so the record sets it wrote outlive the sweep. That gap
// is reported rather than hidden; see cfnDeleteInertTypes.
var cfnStubDeleteTypes = map[string]bool{
	"AWS::Athena::WorkGroup":             true,
	"AWS::Backup::BackupPlan":            true,
	"AWS::CloudTrail::Trail":             true,
	"AWS::CodeBuild::Project":            true,
	"AWS::CodeDeploy::DeploymentGroup":   true,
	"AWS::CodePipeline::Pipeline":        true,
	"AWS::Config::ConfigRule":            true,
	"AWS::Config::ConfigurationRecorder": true,
	"AWS::OpenSearchService::Domain":     true,
	"AWS::Transfer::Server":              true,
	"AWS::WAFv2::WebACL":                 true,
}

// cfnDeleteInertTypes maps a type whose sweep is a no-op to why, so the reason a
// resource is skipped names the cause rather than looking like an unimplemented
// deleter.
//
// Four write no state to remove. The rest are types with no delete to dispatch —
// the API genuinely has none, or substrate does not route the one it has — and each
// says which, because "no delete is modeled" and "AWS models no delete" are
// different facts and only the first is substrate's to fix.
var cfnDeleteInertTypes = map[string]string{
	"AWS::CloudFront::CloudFrontOriginAccessIdentity": "the deploy records no state to remove",
	"AWS::ECS::CapacityProvider":                      "the deploy records no state to remove",
	"AWS::SSM::Association":                           "the deploy records no state to remove",
	"AWS::SecretsManager::SecretTargetAttachment":     "the deploy records no state to remove",
	"AWS::Route53::RecordSetGroup": "the group's record sets were created as " +
		"AWS::Route53::RecordSet dispatches that the stack does not record, so they " +
		"are not swept",
	"AWS::ECR::LifecyclePolicy": "ECR's DeleteLifecyclePolicy is not routed; deleting " +
		"the repository removes its lifecycle policy with it",
	"AWS::ApiGateway::UsagePlanKey": "API Gateway's DeleteUsagePlanKey is not routed, " +
		"so the key stays attached until its usage plan is deleted",
	"AWS::Cognito::IdentityPoolRoleAttachment": "Cognito models no " +
		"DeleteIdentityPoolRoles; the roles are removed with the identity pool",
	"AWS::SecretsManager::RotationSchedule": "Secrets Manager's CancelRotateSecret is " +
		"not routed, so the schedule stays until the secret is deleted",
}

// queryDeleter builds a deleter for a query-protocol API: one action name and one
// parameter naming the resource, which covers most of EC2, RDS, ElastiCache and SNS.
func queryDeleter(service, operation, param string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		return &AWSRequest{Service: service, Operation: operation,
			Headers: map[string]string{},
			Params:  map[string]string{param: dr.PhysicalID}}
	}
}

// jsonBodyDeleter builds a deleter for a JSON-protocol API: one action name and a
// body carrying the single field that names the resource.
func jsonBodyDeleter(service, operation, field string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]interface{}{field: dr.PhysicalID})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: service, Operation: operation, Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	}
}

// pathDeleter builds a deleter for a REST API, where the operation is the HTTP
// method and the resource is named by the path. The plugin routes on the path, so
// Operation is "DELETE" rather than an action name — the same asymmetry the deploy
// side has, where these types dispatch "POST".
func pathDeleter(service, prefix string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		return &AWSRequest{Service: service, Operation: "DELETE",
			Path: prefix + dr.PhysicalID, Headers: map[string]string{},
			Params: map[string]string{}}
	}
}

// apiGatewayChildDeleter builds a deleter for a REST resource nested under a parent
// whose ID the child does not carry: the parent comes from the template property
// named by parentProp, resolved through the stack's own context so a Ref to the
// parent resolves to the ID the deploy used.
//
// A child whose parent property is absent or unresolvable yields nil, which the
// sweep reports as a skip. Guessing a path without the parent would produce a
// request the plugin routes somewhere else entirely.
func apiGatewayChildDeleter(parentProp string, path func(parentID, childID string) string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		parent := resolveStringProp(props, parentProp, "", cctx)
		if parent == "" {
			return nil
		}
		return &AWSRequest{Service: cfnServiceForPath(path(parent, dr.PhysicalID)),
			Operation: "DELETE", Path: path(parent, dr.PhysicalID),
			Headers: map[string]string{}, Params: map[string]string{}}
	}
}

// cfnServiceForPath names the service a nested REST delete path belongs to. The
// three families apiGatewayChildDeleter serves are distinguishable by their path
// prefix, which is what keeps that helper to one parameter instead of two.
func cfnServiceForPath(path string) string {
	switch {
	case strings.HasPrefix(path, "/v2/apis/"):
		return "apigatewayv2"
	case strings.HasPrefix(path, "/v1/apis/"):
		return "appsync"
	default:
		return "apigateway"
	}
}

// sgRuleDeleter builds the revoke for a standalone security-group rule.
//
// A rule is not addressed by an ID: EC2 assigns an opaque sgr- identifier that
// substrate does not model, so the deploy records the *group* as the physical ID and
// a revoke has to restate the permission it authorized. The parameters are rebuilt
// from the template with the same sgRuleParams the deploy used, which is what makes
// the revoke match — removePerm compares protocol, ports and source, so a revoke
// built any other way would silently remove nothing.
func sgRuleDeleter(action string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, props map[string]interface{}, cctx *cfnContext) *AWSRequest {
		if dr.PhysicalID == "" {
			return nil
		}
		params := map[string]string{"Action": action, "GroupId": dr.PhysicalID}
		for k, v := range sgRuleParams(props, "IpPermissions.1.", cctx) {
			params[k] = v
		}
		return &AWSRequest{Service: "ec2", Operation: action, Params: params,
			Headers: map[string]string{}}
	}
}

// iamDeleter builds a deleter for IAM, which accepts its parameters as a form body
// rather than as query parameters.
func iamDeleter(operation, field string) cfnDeleteRequestFunc {
	return func(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
		body, err := json.Marshal(map[string]string{field: dr.PhysicalID})
		if err != nil {
			return nil
		}
		return &AWSRequest{Service: "iam", Operation: operation, Body: body,
			Headers: map[string]string{}, Params: map[string]string{}}
	}
}

// CFNResourceDeletion records what the sweep did with one resource. It is the
// per-resource half of a DELETE_FAILED status: a stack that failed to delete has to
// say which resource failed and why, or a caller cannot act on it.
type CFNResourceDeletion struct {
	// LogicalID is the resource's logical ID in the template.
	LogicalID string `json:"LogicalID"`

	// Type is the CloudFormation resource type.
	Type string `json:"Type"`

	// Status is "DELETE_COMPLETE", "DELETE_SKIPPED" or "DELETE_FAILED".
	Status string `json:"Status"`

	// Reason explains a skip or a failure, and is empty for a completed delete.
	Reason string `json:"Reason,omitempty"`
}

// Resource deletion statuses, matching the values CloudFormation reports for a
// resource in DescribeStackEvents.
const (
	cfnDeleteComplete = "DELETE_COMPLETE"
	cfnDeleteSkipped  = "DELETE_SKIPPED"
	cfnDeleteFailed   = "DELETE_FAILED"
)

// deleteStackResources deletes a stack's resources in the reverse of the order
// Deploy created them, and reports what happened to each.
//
// Ordering is the exact inverse of Deploy's: descending typePriority, ties broken by
// descending logical ID. That matters because the priorities encode real
// dependencies — a subnet cannot go before the instances in it — and reversing the
// same map is what guarantees the teardown order is consistent with the build order
// rather than merely plausible.
//
// A resource that is already absent counts as deleted. A sweep's goal is that the
// resource not exist, and a resource deleted out of band has met it; treating
// not-found as a failure would wedge the stack permanently on a condition the caller
// cannot fix.
func (d *StackDeployer) deleteStackResources(
	ctx context.Context, stack *CFNStackState, streamID string, op cfnDeletionOp,
) []CFNResourceDeletion {
	// The template supplies the deletion policies and the properties a nested
	// resource's delete needs. A template that no longer parses is not a reason to
	// leave every resource behind: the sweep proceeds with no policies and no
	// properties, which deletes the types that need neither and skips the rest.
	var (
		policies = map[string]string{}
		props    = map[string]map[string]interface{}{}
		cctx     *cfnContext
	)
	if tmpl, err := d.parseCFNTemplate(stack.TemplateBody); err == nil {
		cctx = buildCFNContext(tmpl, stack.Parameters, d.identity.region, d.identity.accountID,
			stack.StackName)
		evaluateConditions(tmpl, cctx)
		for logicalID, res := range tmpl.Resources {
			policies[logicalID] = cfnDeletionPolicyFor(res)
			props[logicalID] = res.Properties
		}
	} else {
		cctx = &cfnContext{
			params: stack.Parameters, conditions: map[string]bool{},
			resources: map[string]DeployedResource{},
			region:    d.identity.region, accountID: d.identity.accountID,
			stackName: stack.StackName,
		}
		d.logger.Warn("cfn: stack template no longer parses; deleting without policies",
			"stack", stack.StackName, "error", err.Error())
	}

	// Ref and GetAtt in a surviving resource's properties must resolve to what the
	// deploy produced, so the context is seeded with the deployed resources rather
	// than left empty.
	for _, dr := range stack.Resources {
		cctx.resources[dr.LogicalID] = dr
	}

	ordered := make([]DeployedResource, len(stack.Resources))
	copy(ordered, stack.Resources)
	sort.SliceStable(ordered, func(i, j int) bool {
		pi, pj := cfnTypePriority(ordered[i].Type), cfnTypePriority(ordered[j].Type)
		if pi != pj {
			return pi > pj
		}
		return ordered[i].LogicalID > ordered[j].LogicalID
	})

	results := make([]CFNResourceDeletion, 0, len(ordered))
	for _, dr := range ordered {
		results = append(results, d.deleteOneResource(ctx, dr, policies[dr.LogicalID],
			props[dr.LogicalID], cctx, streamID, op))
	}
	return results
}

// deleteOneResource applies a resource's deletion policy and, if it calls for a
// delete, dispatches it.
func (d *StackDeployer) deleteOneResource(
	ctx context.Context, dr DeployedResource, policy string,
	props map[string]interface{}, cctx *cfnContext, streamID string, op cfnDeletionOp,
) CFNResourceDeletion {
	result := CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type}

	// A resource that never came up has nothing to delete. Its create was refused,
	// so there is no physical resource behind it — which is also why a rollback
	// sweep does not report the failed resource as a failed delete.
	if dr.Error != "" {
		result.Status = cfnDeleteSkipped
		result.Reason = "resource was not created: " + dr.Error
		return result
	}

	if policy == "" {
		policy = cfnPolicyDelete
	}
	if !cfnValidDeletionPolicies[policy] {
		result.Status = cfnDeleteFailed
		result.Reason = fmt.Sprintf("invalid DeletionPolicy %q", policy)
		return result
	}
	if cfnPolicyRetainsResource(policy, op) {
		result.Status = cfnDeleteSkipped
		result.Reason = "DeletionPolicy " + policy
		return result
	}
	if policy == cfnPolicySnapshot {
		// Substrate models no snapshot for any type, so saying so is the honest
		// answer: the resource is deleted, and the snapshot the template asked for
		// does not exist. Reporting DELETE_COMPLETE with no reason would imply one
		// was taken.
		reason := "Snapshot is not modeled; the resource was deleted without one"
		if !cfnSnapshotCapableTypes[dr.Type] {
			reason = "Snapshot is not supported for " + dr.Type +
				"; the resource was deleted without one"
		}
		d.logger.Warn("cfn: DeletionPolicy Snapshot is not modeled",
			"logical_id", dr.LogicalID, "type", dr.Type)
		snapshotResult := result
		snapshotResult.Reason = reason
		return d.deleteViaTable(ctx, dr, props, cctx, streamID, snapshotResult)
	}

	return d.deleteViaTable(ctx, dr, props, cctx, streamID, result)
}

// deleteViaTable looks a resource's deleter up, dispatches what it builds, and
// returns success carrying the reason already on result — which is how a Snapshot
// delete keeps its "no snapshot was taken" note while sharing this path.
func (d *StackDeployer) deleteViaTable(
	ctx context.Context, dr DeployedResource, props map[string]interface{},
	cctx *cfnContext, streamID string, result CFNResourceDeletion,
) CFNResourceDeletion {
	if cfnStubDeleteTypes[dr.Type] {
		if err := d.deleteStubKey(ctx, dr, cctx); err != nil {
			result.Status = cfnDeleteFailed
			result.Reason = err.Error()
			return result
		}
		result.Status = cfnDeleteComplete
		return result
	}
	if reason, inert := cfnDeleteInertTypes[dr.Type]; inert {
		result.Status = cfnDeleteSkipped
		result.Reason = reason
		return result
	}

	deleter, ok := cfnResourceDeleters[dr.Type]
	if !ok {
		// A type the deployer does not recognize at all went through
		// deployGenericStub, whose only trace is a cfnStubNamespace key — so
		// removing that key is the whole of its delete, and leaving it would let a
		// redeployed stack read the previous one's properties. The result is still a
		// skip rather than a completion: substrate never created a resource for this
		// type, and reporting DELETE_COMPLETE would claim it deleted one.
		if err := d.deleteStubKey(ctx, dr, cctx); err != nil {
			result.Status = cfnDeleteFailed
			result.Reason = err.Error()
			return result
		}
		result.Status = cfnDeleteSkipped
		result.Reason = "no delete is modeled for " + dr.Type
		return result
	}
	req := deleter(d, dr, props, cctx)
	if req == nil {
		result.Status = cfnDeleteSkipped
		result.Reason = "the delete request for " + dr.Type + " could not be built"
		return result
	}
	if pre, ok := cfnDeletePreSteps[dr.Type]; ok {
		for _, step := range pre(d, dr, props, cctx) {
			if failure := d.dispatchResourceDelete(ctx, dr, step, streamID); failure != nil {
				return *failure
			}
		}
	}
	if failure := d.dispatchResourceDelete(ctx, dr, req, streamID); failure != nil {
		return *failure
	}
	result.Status = cfnDeleteComplete
	return result
}

// deleteStubKey removes the cfnStubNamespace entry a state-only deploy wrote, keyed
// the way stubStore and deployGenericStub both key it: account, Region, logical ID.
func (d *StackDeployer) deleteStubKey(
	ctx context.Context, dr DeployedResource, cctx *cfnContext,
) error {
	key := fmt.Sprintf("%s/%s/%s", cctx.accountID, cctx.region, dr.LogicalID)
	if err := d.state.Delete(ctx, cfnStubNamespace, key); err != nil {
		return fmt.Errorf("delete stub state for %s: %w", dr.LogicalID, err)
	}
	return nil
}

// dispatchResourceDelete dispatches a resource's delete, returning a failure result
// or nil on success. A not-found is success: see [StackDeployer.deleteStackResources].
func (d *StackDeployer) dispatchResourceDelete(
	ctx context.Context, dr DeployedResource, req *AWSRequest, streamID string,
) *CFNResourceDeletion {
	_, _, err := d.dispatch(ctx, req, streamID)
	if err == nil || cfnDeleteIsAbsent(err) {
		return nil
	}
	return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type,
		Status: cfnDeleteFailed, Reason: err.Error()}
}

// cfnDeleteAbsentCodes is the set of error codes that mean "the resource is already
// gone", as the plugins reachable from [cfnResourceDeleters] actually spell it.
//
// Every entry was harvested by dispatching each table entry against a registry
// holding no resources and recording the code that came back — not read off the AWS
// references. The two sources disagree in ways that matter here: EC2 answers a
// well-formed but unknown route table with InvalidRouteTableID.NotFound while its
// security group is InvalidGroup.NotFound (no "ID"), and ECS reports a missing task
// definition as the generic ClientException. A hand-written list gets those wrong,
// and getting one wrong means a stack wedges in DELETE_FAILED on a resource that
// does not exist.
//
// Codes are deliberately absent when they are not unambiguously an absence:
// ValidationError and BadRequestException also carry malformed input, and treating
// those as success would report a resource deleted that was never asked about
// correctly.
var cfnDeleteAbsentCodes = map[string]bool{
	"ActivityDoesNotExist":              true,
	"CacheClusterNotFound":              true,
	"CacheParameterGroupNotFound":       true,
	"CacheSubnetGroupNotFoundFault":     true,
	"ClientException":                   true, // ECS's code for an absent task definition.
	"ClusterNotFoundException":          true,
	"DBClusterNotFoundFault":            true,
	"DBInstanceNotFound":                true,
	"DBParameterGroupNotFound":          true,
	"DBSubnetGroupNotFoundFault":        true,
	"FileSystemNotFound":                true,
	"InvalidAllocationID.NotFound":      true,
	"InvalidGroup.NotFound":             true,
	"InvalidInstanceID.NotFound":        true,
	"InvalidInternetGatewayID.NotFound": true,
	"InvalidLaunchTemplateId.NotFound":  true,
	"InvalidRouteTableID.NotFound":      true,
	"InvalidSubnetID.NotFound":          true,
	"InvalidVpcID.NotFound":             true,
	"NatGatewayNotFound":                true,
	"NoSuchBucket":                      true,
	"NoSuchDistribution":                true,
	"NoSuchEntity":                      true,
	"NoSuchHostedZone":                  true,
	"NotFound":                          true, // SNS DeleteTopic.
	"NotFoundException":                 true,
	"ParameterNotFound":                 true,
	"QueueDoesNotExist":                 true,
	"ReplicationGroupNotFoundFault":     true,
	"RepositoryNotFoundException":       true,
	"ResourceNotFoundException":         true,
	"ServiceNotFoundException":          true,
	"StateMachineDoesNotExist":          true,
}

// cfnDeleteIsAbsent reports whether a delete failed because the resource was
// already gone.
//
// The match is on the error's code alone, taken from the "Code: Message" prefix
// [AWSError.Error] and [cfnDispatchError] both produce. Matching the code rather
// than searching the whole message is what stops a message that merely mentions a
// resource being read as an absence — "NotFound" appears in plenty of prose — and
// means rewording a plugin's message cannot turn a tolerated absence into a
// stack-wedging failure.
func cfnDeleteIsAbsent(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	code := msg
	if i := strings.Index(msg, ": "); i >= 0 {
		code = msg[:i]
	}
	return cfnDeleteAbsentCodes[code]
}

// cfnTypePriority is typePriority's lookup with the same default Deploy uses, so the
// reverse sweep orders an unlisted type identically to the way Deploy ordered it.
func cfnTypePriority(resType string) int {
	if p, ok := typePriority[resType]; ok {
		return p
	}
	return 99
}
