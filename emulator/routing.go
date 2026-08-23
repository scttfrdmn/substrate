package emulator

// PluginRouting records how a real AWS client addresses one plugin: the
// X-Amz-Target namespace it sends, the endpoint hosts it dials, and the SigV4
// signing names it puts in the credential scope. Substrate resolves a request's
// service from those three signals in that order (see extractServiceSignals), so
// a plugin whose real identifiers reduce to nothing substrate recognizes is
// registered, initialized, unit-tested — and unreachable from every SDK.
//
// That failure has shipped three times: SSOPlugin (#561), OrganizationsPlugin and
// ConfigServicePlugin (#580), and EventBridgePlugin (#734), each found by a live
// run rather than by the suite. The reason it hides is that the target is signal
// #1: an unrecognized namespace short-circuits the host and signing-name paths
// that would both have answered correctly, and substrate is always reached with
// --endpoint-url, where the host is localhost and the target is the only signal.
//
// This table exists so the check is systematic rather than incidental (#739). It
// is the single source for the docs coverage matrix, the drift check that fails
// when a plugin is registered without routing, and the sweep test that asserts
// every identifier here resolves to a registered plugin.
type PluginRouting struct {
	// Display is the human-facing AWS service name.
	Display string
	// Protocol is the wire protocol the substrate plugin speaks, which is not always
	// the protocol the service model declares; where they differ, Why says so.
	Protocol string
	// TargetPrefix is the X-Amz-Target namespace a client sends, empty for a service
	// whose protocol carries no target (Query, EC2, REST/JSON, REST/XML).
	TargetPrefix string
	// Hosts are complete example endpoint hosts, one per distinct shape a client dials.
	Hosts []string
	// SigningNames are the SigV4 credential-scope service names a client may sign with.
	SigningNames []string
	// RoutesTo names the plugin that Hosts and SigningNames resolve to when it is
	// deliberately not this one. Empty means they resolve to this plugin.
	RoutesTo string
	// Why explains a RoutesTo, an identifier that is data rather than a service name,
	// or a divergence between the model and the plugin. Empty when there is nothing
	// to explain.
	Why string
	// Source names where the identifiers were read from. It names *which* source,
	// because botocore and aws-sdk-go-v2 do not always agree — CloudTrail is the
	// case in point.
	Source string
}

// pluginRouting maps each registered plugin name to its routing identifiers.
// Every entry was read from the service model bundled in the locally installed
// AWS CLI v2 (metadata.targetPrefix / endpointPrefix / signingName, plus
// endpoints.json for a non-regionalized host), not from recollection.
//
// Coverage limits worth stating rather than implying: all 67 entries were checked
// against botocore, 53 of the JSON ones were cross-checked against aws-sdk-go-v2
// serializers, and the Java, JavaScript and .NET SDKs were not checked. CloudTrail
// proves the cross-check matters — botocore sends a fully-qualified namespace where
// aws-sdk-go-v2 sends a terse one, so a table citing only one source would have
// recorded a prefix that half the world's clients do not send.
var pluginRouting = map[string]PluginRouting{
	"account": {
		Display:      "Account Management",
		Protocol:     "REST/JSON",
		Hosts:        []string{"account.us-east-1.amazonaws.com"},
		SigningNames: []string{"account"},
		Source:       "botocore account service-2.json",
	},
	"acm": {
		Display:      "ACM",
		Protocol:     "JSON",
		TargetPrefix: "CertificateManager",
		Hosts:        []string{"acm.us-east-1.amazonaws.com"},
		SigningNames: []string{"acm"},
		Source:       "botocore acm service-2.json",
	},
	"apigateway": {
		Display:      "API Gateway (REST)",
		Protocol:     "REST/JSON",
		Hosts:        []string{"apigateway.us-east-1.amazonaws.com"},
		SigningNames: []string{"apigateway"},
		Source:       "botocore apigateway service-2.json",
	},
	"apigatewayv2": {
		Display:      "API Gateway (HTTP)",
		Protocol:     "REST/JSON",
		Hosts:        []string{"apigateway.us-east-1.amazonaws.com"},
		SigningNames: []string{"apigateway"},
		RoutesTo:     "apigateway",
		Why: "The v2 model shares the v1 endpoint prefix and signing name, so no " +
			"routing signal distinguishes them. refineAPIGatewayVersion separates the " +
			"two on the /v2/ path segment instead (#529), which is what a client actually " +
			"sends.",
		Source: "botocore apigatewayv2 service-2.json",
	},
	"appsync": {
		Display:      "AppSync",
		Protocol:     "REST/JSON",
		Hosts:        []string{"appsync.us-east-1.amazonaws.com", "myapi.appsync-api.us-east-1.amazonaws.com"},
		SigningNames: []string{"appsync"},
		Source:       "botocore appsync service-2.json; AppSync developer guide for the execution host",
	},
	"athena": {
		Display:      "Athena",
		Protocol:     "JSON",
		TargetPrefix: "AmazonAthena",
		Hosts:        []string{"athena.us-east-1.amazonaws.com"},
		SigningNames: []string{"athena"},
		Source:       "botocore athena service-2.json",
	},
	"backup": {
		Display:      "Backup",
		Protocol:     "REST/JSON",
		Hosts:        []string{"backup.us-east-1.amazonaws.com"},
		SigningNames: []string{"backup"},
		Source:       "botocore backup service-2.json",
	},
	"batch": {
		Display:      "Batch",
		Protocol:     "REST/JSON",
		Hosts:        []string{"batch.us-east-1.amazonaws.com"},
		SigningNames: []string{"batch"},
		Source:       "botocore batch service-2.json",
	},
	"bedrock-runtime": {
		Display:      "Bedrock Runtime",
		Protocol:     "REST/JSON",
		Hosts:        []string{"bedrock-runtime.us-east-1.amazonaws.com"},
		SigningNames: []string{"bedrock"},
		Why:          "The runtime's signing name is the control plane's, so boto3 signs with bedrock.",
		Source:       "botocore bedrock-runtime service-2.json",
	},
	"budgets": {
		Display:      "Budgets",
		Protocol:     "JSON",
		TargetPrefix: "AWSBudgetServiceGateway",
		Hosts:        []string{"budgets.amazonaws.com"},
		SigningNames: []string{"budgets"},
		Why:          "Not regionalized: the partition endpoint carries no region label.",
		Source:       "botocore budgets service-2.json; botocore endpoints.json aws-global",
	},
	"ce": {
		Display:      "Cost Explorer",
		Protocol:     "JSON",
		TargetPrefix: "AWSInsightsIndexService",
		Hosts:        []string{"ce.us-east-1.amazonaws.com"},
		SigningNames: []string{"ce"},
		Source:       "botocore ce service-2.json",
	},
	"cloudformation": {
		Display:      "CloudFormation",
		Protocol:     "Query",
		Hosts:        []string{"cloudformation.us-east-1.amazonaws.com"},
		SigningNames: []string{"cloudformation"},
		Source:       "botocore cloudformation service-2.json",
	},
	"cloudfront": {
		Display:      "CloudFront",
		Protocol:     "REST/XML",
		Hosts:        []string{"cloudfront.amazonaws.com"},
		SigningNames: []string{"cloudfront"},
		Why:          "Not regionalized: the partition endpoint carries no region label.",
		Source:       "botocore cloudfront service-2.json; botocore endpoints.json aws-global",
	},
	"cloudtrail": {
		Display:      "CloudTrail",
		Protocol:     "JSON",
		TargetPrefix: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101",
		Hosts:        []string{"cloudtrail.us-east-1.amazonaws.com"},
		SigningNames: []string{"cloudtrail"},
		Why: "The only plugin whose two SDKs disagree: botocore sends the model's " +
			"fully-qualified namespace, aws-sdk-go-v2 sends the terse CloudTrail_20131101. " +
			"An alias cannot fix the long form — the first label is com, which would hijack " +
			"every fully-qualified prefix — so extractServiceFromTarget reduces a dotted " +
			"namespace to its last segment instead.",
		Source: "botocore cloudtrail service-2.json (long form); aws-sdk-go-v2 cloudtrail serializers (terse form)",
	},
	"codebuild": {
		Display:      "CodeBuild",
		Protocol:     "JSON",
		TargetPrefix: "CodeBuild_20161006",
		Hosts:        []string{"codebuild.us-east-1.amazonaws.com"},
		SigningNames: []string{"codebuild"},
		Source:       "botocore codebuild service-2.json",
	},
	"codedeploy": {
		Display:      "CodeDeploy",
		Protocol:     "JSON",
		TargetPrefix: "CodeDeploy_20141006",
		Hosts:        []string{"codedeploy.us-east-1.amazonaws.com"},
		SigningNames: []string{"codedeploy"},
		Source:       "botocore codedeploy service-2.json",
	},
	"codepipeline": {
		Display:      "CodePipeline",
		Protocol:     "JSON",
		TargetPrefix: "CodePipeline_20150709",
		Hosts:        []string{"codepipeline.us-east-1.amazonaws.com"},
		SigningNames: []string{"codepipeline"},
		Source:       "botocore codepipeline service-2.json",
	},
	"cognito-identity": {
		Display:      "Cognito Identity",
		Protocol:     "JSON",
		TargetPrefix: "AWSCognitoIdentityService",
		Hosts:        []string{"cognito-identity.us-east-1.amazonaws.com"},
		SigningNames: []string{"cognito-identity"},
		Source:       "botocore cognito-identity service-2.json",
	},
	"cognito-idp": {
		Display:      "Cognito Identity Provider",
		Protocol:     "JSON",
		TargetPrefix: "AWSCognitoIdentityProviderService",
		Hosts:        []string{"cognito-idp.us-east-1.amazonaws.com"},
		SigningNames: []string{"cognito-idp"},
		Source:       "botocore cognito-idp service-2.json",
	},
	"config": {
		Display:      "Config",
		Protocol:     "JSON",
		TargetPrefix: "StarlingDoveService",
		Hosts:        []string{"config.us-east-1.amazonaws.com"},
		SigningNames: []string{"config"},
		Why:          "StarlingDove is Config's internal code-name, the way TrentService is KMS's (#580).",
		Source:       "botocore config service-2.json",
	},
	"dynamodb": {
		Display:      "DynamoDB",
		Protocol:     "JSON",
		TargetPrefix: "DynamoDB_20120810",
		Hosts:        []string{"dynamodb.us-east-1.amazonaws.com"},
		SigningNames: []string{"dynamodb"},
		Source:       "botocore dynamodb service-2.json",
	},
	"ec2": {
		Display:      "EC2 / VPC",
		Protocol:     "Query",
		Hosts:        []string{"ec2.us-east-1.amazonaws.com"},
		SigningNames: []string{"ec2"},
		Source:       "botocore ec2 service-2.json",
	},
	"ecr": {
		Display:      "ECR",
		Protocol:     "JSON",
		TargetPrefix: "AmazonEC2ContainerRegistry_V20150921",
		Hosts:        []string{"api.ecr.us-east-1.amazonaws.com"},
		SigningNames: []string{"ecr"},
		Source:       "botocore ecr service-2.json",
	},
	"ecs": {
		Display:      "ECS",
		Protocol:     "JSON",
		TargetPrefix: "AmazonEC2ContainerServiceV20141113",
		Hosts:        []string{"ecs.us-east-1.amazonaws.com"},
		SigningNames: []string{"ecs"},
		Source:       "botocore ecs service-2.json",
	},
	"efs": {
		Display:      "EFS",
		Protocol:     "REST/JSON",
		Hosts:        []string{"elasticfilesystem.us-east-1.amazonaws.com"},
		SigningNames: []string{"elasticfilesystem"},
		Source:       "botocore efs service-2.json",
	},
	"elasticache": {
		Display:      "ElastiCache",
		Protocol:     "Query",
		Hosts:        []string{"elasticache.us-east-1.amazonaws.com"},
		SigningNames: []string{"elasticache"},
		Source:       "botocore elasticache service-2.json",
	},
	"elasticloadbalancing": {
		Display:      "ELBv2",
		Protocol:     "Query",
		Hosts:        []string{"elasticloadbalancing.us-east-1.amazonaws.com"},
		SigningNames: []string{"elasticloadbalancing"},
		Source:       "botocore elbv2 service-2.json",
	},
	"emrserverless": {
		Display:      "EMR Serverless",
		Protocol:     "REST/JSON",
		Hosts:        []string{"emr-serverless.us-east-1.amazonaws.com"},
		SigningNames: []string{"emr-serverless"},
		Source:       "botocore emr-serverless service-2.json",
	},
	"eventbridge": {
		Display:      "EventBridge",
		Protocol:     "JSON",
		TargetPrefix: "AWSEvents",
		Hosts:        []string{"events.us-east-1.amazonaws.com"},
		SigningNames: []string{"events"},
		Why:          "The prefix is AWSEvents, not AmazonEventBridge; host routing hid that until #734.",
		Source:       "botocore events service-2.json",
	},
	"execute-api": {
		Display:      "API Gateway (execute-api)",
		Protocol:     "REST/JSON",
		Hosts:        []string{"abc123def4.execute-api.us-east-1.amazonaws.com"},
		SigningNames: []string{"execute-api"},
		Why: "The first host label is the API's ID — data, not a service name — so the " +
			"host path matches on the execute-api label instead. There is no service model: " +
			"this is the runtime endpoint an SDK-generated client never calls.",
		Source: "API Gateway developer guide (Invoke an API)",
	},
	"firehose": {
		Display:      "Kinesis Data Firehose",
		Protocol:     "JSON",
		TargetPrefix: "Firehose_20150804",
		Hosts:        []string{"firehose.us-east-1.amazonaws.com"},
		SigningNames: []string{"firehose"},
		Source:       "botocore firehose service-2.json",
	},
	"fsx": {
		Display:      "FSx",
		Protocol:     "JSON",
		TargetPrefix: "AWSSimbaAPIService_v20180301",
		Hosts:        []string{"fsx.us-east-1.amazonaws.com"},
		SigningNames: []string{"fsx"},
		Source:       "botocore fsx service-2.json",
	},
	"glue": {
		Display:      "Glue",
		Protocol:     "JSON",
		TargetPrefix: "AWSGlue",
		Hosts:        []string{"glue.us-east-1.amazonaws.com"},
		SigningNames: []string{"glue"},
		Source:       "botocore glue service-2.json",
	},
	"health": {
		Display:      "Health",
		Protocol:     "JSON",
		TargetPrefix: "AWSHealth_20160804",
		Hosts:        []string{"global.health.amazonaws.com", "health.us-east-1.amazonaws.com"},
		SigningNames: []string{"health"},
		Why: "Health is not regionalized: its partition endpoint is global.health, whose " +
			"first label is not the service. Both this and the real target prefix were " +
			"missing, while the table held an invented healthservice — the #561 failure " +
			"repeated, and what hid this one (#739).",
		Source: "botocore health service-2.json; botocore endpoints.json aws-global",
	},
	"iam": {
		Display:      "IAM",
		Protocol:     "Query",
		Hosts:        []string{"iam.amazonaws.com"},
		SigningNames: []string{"iam"},
		Source:       "botocore iam service-2.json",
	},
	"kinesis": {
		Display:      "Kinesis Data Streams",
		Protocol:     "JSON",
		TargetPrefix: "Kinesis_20131202",
		Hosts:        []string{"kinesis.us-east-1.amazonaws.com"},
		SigningNames: []string{"kinesis"},
		Source:       "botocore kinesis service-2.json",
	},
	"kms": {
		Display:      "KMS",
		Protocol:     "JSON",
		TargetPrefix: "TrentService",
		Hosts:        []string{"kms.us-east-1.amazonaws.com"},
		SigningNames: []string{"kms"},
		Why:          "Trent is KMS's internal code-name; the prefix bears no relation to the endpoint.",
		Source:       "botocore kms service-2.json",
	},
	"lambda": {
		Display:      "Lambda",
		Protocol:     "REST/JSON",
		Hosts:        []string{"lambda.us-east-1.amazonaws.com"},
		SigningNames: []string{"lambda"},
		Source:       "botocore lambda service-2.json",
	},
	"logs": {
		Display:      "CloudWatch Logs",
		Protocol:     "JSON",
		TargetPrefix: "Logs_20140328",
		Hosts:        []string{"logs.us-east-1.amazonaws.com"},
		SigningNames: []string{"logs"},
		Source:       "botocore logs service-2.json",
	},
	"monitoring": {
		Display:      "CloudWatch",
		Protocol:     "Query",
		TargetPrefix: "GraniteServiceVersion20100801",
		Hosts:        []string{"monitoring.us-east-1.amazonaws.com"},
		SigningNames: []string{"monitoring"},
		Why: "The model declares three protocols and botocore resolves json first, so the " +
			"AWS CLI sends this target while aws-sdk-go-v2 takes the rpc-v2-cbor path " +
			"substrate already routed — green tests over an endpoint no CLI could reach " +
			"(#739). Routing it does not make the CLI's JSON body parseable by a plugin " +
			"that reads Query form parameters; that is filed separately.",
		Source: "botocore cloudwatch service-2.json (metadata.protocols, resolved by botocore's protocol priority)",
	},
	"msk": {
		Display:      "MSK",
		Protocol:     "REST/JSON",
		Hosts:        []string{"kafka.us-east-1.amazonaws.com"},
		SigningNames: []string{"kafka"},
		Source:       "botocore kafka service-2.json",
	},
	"omics": {
		Display:      "HealthOmics",
		Protocol:     "REST/JSON",
		Hosts:        []string{"omics.us-east-1.amazonaws.com"},
		SigningNames: []string{"omics"},
		Source:       "botocore omics service-2.json",
	},
	"opensearch": {
		Display:  "OpenSearch",
		Protocol: "REST/JSON",
		Hosts: []string{
			"es.us-east-1.amazonaws.com",
			"search-mydomain-abc123.us-east-1.es.amazonaws.com",
			"abc123.us-east-1.aoss.amazonaws.com",
		},
		SigningNames: []string{"es", "aoss"},
		Why: "A managed domain's and a Serverless collection's hosts both carry the name " +
			"as data ahead of the es/aoss label. The plugin is data-plane only, so the " +
			"OpenSearchServerless control-plane target prefix names operations substrate " +
			"does not implement and is deliberately absent.",
		Source: "botocore opensearch and opensearchserverless service-2.json",
	},
	"organizations": {
		Display:      "Organizations",
		Protocol:     "JSON",
		TargetPrefix: "AWSOrganizationsV20161128",
		Hosts:        []string{"organizations.us-east-1.amazonaws.com"},
		SigningNames: []string{"organizations"},
		Why:          "The version rides inside the prefix rather than after an underscore, so it never reduces on its own (#580).",
		Source:       "botocore organizations service-2.json",
	},
	"pricing": {
		Display:      "Price List Query API",
		Protocol:     "JSON",
		TargetPrefix: "AWSPriceListService",
		Hosts:        []string{"api.pricing.us-east-1.amazonaws.com"},
		SigningNames: []string{"pricing"},
		Source:       "botocore pricing service-2.json",
	},
	"quicksight": {
		Display:      "QuickSight",
		Protocol:     "REST/JSON",
		Hosts:        []string{"quicksight.us-east-1.amazonaws.com"},
		SigningNames: []string{"quicksight"},
		Source:       "botocore quicksight service-2.json",
	},
	"ram": {
		Display:      "RAM",
		Protocol:     "REST/JSON",
		Hosts:        []string{"ram.us-east-1.amazonaws.com"},
		SigningNames: []string{"ram"},
		Source:       "botocore ram service-2.json",
	},
	"rds": {
		Display:      "RDS",
		Protocol:     "Query",
		Hosts:        []string{"rds.us-east-1.amazonaws.com"},
		SigningNames: []string{"rds"},
		Source:       "botocore rds service-2.json",
	},
	"redshift": {
		Display:      "Redshift",
		Protocol:     "Query",
		Hosts:        []string{"redshift.us-east-1.amazonaws.com"},
		SigningNames: []string{"redshift"},
		Source:       "botocore redshift service-2.json",
	},
	"redshift-data": {
		Display:      "Redshift Data API",
		Protocol:     "JSON",
		TargetPrefix: "RedshiftData",
		Hosts:        []string{"redshift-data.us-east-1.amazonaws.com"},
		SigningNames: []string{"redshift-data"},
		Source:       "botocore redshift-data service-2.json",
	},
	"route53": {
		Display:      "Route 53",
		Protocol:     "REST/XML",
		Hosts:        []string{"route53.amazonaws.com"},
		SigningNames: []string{"route53"},
		Source:       "botocore route53 service-2.json",
	},
	"s3": {
		Display:      "S3",
		Protocol:     "REST/XML",
		Hosts:        []string{"s3.us-east-1.amazonaws.com"},
		SigningNames: []string{"s3"},
		Source:       "botocore s3 service-2.json",
	},
	"sagemaker": {
		Display:      "SageMaker",
		Protocol:     "JSON",
		TargetPrefix: "SageMaker",
		Hosts:        []string{"api.sagemaker.us-east-1.amazonaws.com"},
		SigningNames: []string{"sagemaker"},
		Source:       "botocore sagemaker service-2.json",
	},
	"scheduler": {
		Display:      "EventBridge Scheduler",
		Protocol:     "REST/JSON",
		Hosts:        []string{"scheduler.us-east-1.amazonaws.com"},
		SigningNames: []string{"scheduler"},
		Source:       "botocore scheduler service-2.json",
	},
	"secretsmanager": {
		Display:      "Secrets Manager",
		Protocol:     "JSON",
		TargetPrefix: "secretsmanager",
		Hosts:        []string{"secretsmanager.us-east-1.amazonaws.com"},
		SigningNames: []string{"secretsmanager"},
		Source:       "botocore secretsmanager service-2.json",
	},
	"servicequotas": {
		Display:      "Service Quotas",
		Protocol:     "JSON",
		TargetPrefix: "ServiceQuotasV20190624",
		Hosts:        []string{"servicequotas.us-east-1.amazonaws.com"},
		SigningNames: []string{"servicequotas"},
		Source:       "botocore service-quotas service-2.json",
	},
	"sesv2": {
		Display:      "SES v2",
		Protocol:     "REST/JSON",
		Hosts:        []string{"email.us-east-1.amazonaws.com"},
		SigningNames: []string{"ses"},
		Why:          "The v2 API keeps v1's email host and ses signing name.",
		Source:       "botocore sesv2 service-2.json",
	},
	"sns": {
		Display:      "SNS",
		Protocol:     "Query",
		Hosts:        []string{"sns.us-east-1.amazonaws.com"},
		SigningNames: []string{"sns"},
		Source:       "botocore sns service-2.json",
	},
	"sqs": {
		Display:      "SQS",
		Protocol:     "JSON",
		TargetPrefix: "AmazonSQS",
		Hosts:        []string{"sqs.us-east-1.amazonaws.com"},
		SigningNames: []string{"sqs"},
		Source:       "botocore sqs service-2.json",
	},
	"ssm": {
		Display:      "SSM",
		Protocol:     "JSON",
		TargetPrefix: "AmazonSSM",
		Hosts:        []string{"ssm.us-east-1.amazonaws.com"},
		SigningNames: []string{"ssm"},
		Source:       "botocore ssm service-2.json",
	},
	"sso": {
		Display:      "SSO / Identity Store",
		Protocol:     "REST/JSON",
		TargetPrefix: "SWBExternalService",
		Hosts:        []string{"sso.us-east-1.amazonaws.com"},
		SigningNames: []string{"sso"},
		Why: "The plugin emulates sso-admin, whose model is JSON 1.1 — substrate answers " +
			"REST/JSON and classifies its errors that way, which is filed as its own issue. " +
			"The guessed AWSSSOAdminService prefix that no client sends is what made the " +
			"plugin unreachable in #561.",
		Source: "botocore sso-admin service-2.json",
	},
	"states": {
		Display:      "Step Functions",
		Protocol:     "JSON",
		TargetPrefix: "AWSStepFunctions",
		Hosts:        []string{"states.us-east-1.amazonaws.com"},
		SigningNames: []string{"states"},
		Source:       "botocore stepfunctions service-2.json",
	},
	"sts": {
		Display:      "STS",
		Protocol:     "Query",
		Hosts:        []string{"sts.amazonaws.com", "sts.us-east-1.amazonaws.com"},
		SigningNames: []string{"sts"},
		Source:       "botocore sts service-2.json",
	},
	"tagging": {
		Display:      "Resource Groups Tagging",
		Protocol:     "JSON",
		TargetPrefix: "ResourceGroupsTaggingAPI_20170126",
		Hosts:        []string{"tagging.us-east-1.amazonaws.com"},
		SigningNames: []string{"tagging"},
		Source:       "botocore resourcegroupstaggingapi service-2.json",
	},
	"timestream": {
		Display:  "Timestream",
		Protocol: "JSON",
		// Write and Query share the prefix and the signing name; they differ only in host.
		TargetPrefix: "Timestream_20181101",
		Hosts: []string{
			"ingest.timestream.us-east-1.amazonaws.com",
			"query.timestream.us-east-1.amazonaws.com",
		},
		SigningNames: []string{"timestream"},
		Why: "Endpoint discovery hands a client an ingest. or query. host whose first label " +
			"is the operation class, not the service, so both reduced to nothing before #739. " +
			"A --endpoint-url caller was unaffected, which is why the suite stayed green.",
		Source: "botocore timestream-write and timestream-query service-2.json",
	},
	"transfer": {
		Display:      "Transfer Family",
		Protocol:     "JSON",
		TargetPrefix: "TransferService",
		Hosts:        []string{"transfer.us-east-1.amazonaws.com"},
		SigningNames: []string{"transfer"},
		Source:       "botocore transfer service-2.json",
	},
	"wafv2": {
		Display:      "WAFv2",
		Protocol:     "JSON",
		TargetPrefix: "AWSWAF_20190729",
		Hosts:        []string{"wafv2.us-east-1.amazonaws.com"},
		SigningNames: []string{"wafv2"},
		Source:       "botocore wafv2 service-2.json",
	},
}

// PluginRoutingCatalog returns a copy of the routing table, keyed by plugin name.
// Callers outside the package — the documentation generator and its drift check —
// read display names, protocols and routing identifiers from here, so a plugin
// cannot be registered without declaring how a client reaches it.
func PluginRoutingCatalog() map[string]PluginRouting {
	out := make(map[string]PluginRouting, len(pluginRouting))
	for name, r := range pluginRouting {
		out[name] = r
	}
	return out
}
