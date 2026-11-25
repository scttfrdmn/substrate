package main

// pluginModel names the botocore service models behind one substrate plugin.
type pluginModel struct {
	// plugin is the registry name, as emulator.RoutedServices lists it.
	plugin string
	// models are botocore data directory names. A plugin serving two models (ELB's
	// classic and v2 APIs, Timestream's write and query APIs) lists both, and an
	// operation present in more than one of them is left out of the generated tables,
	// because the operation name alone cannot say which model's input applies.
	models []string
	// unmodelled says why a plugin has no model; models is empty when it is set.
	unmodelled string
}

// pluginModels covers every routed plugin, in registry-name order. The botocore names are
// the ones emulator/routing.go's Source column cites for each plugin.
var pluginModels = []pluginModel{
	{plugin: "account", models: []string{"account"}},
	{plugin: "acm", models: []string{"acm"}},
	{plugin: "apigateway", models: []string{"apigateway"}},
	{plugin: "apigatewayv2", models: []string{"apigatewayv2"}},
	{plugin: "appsync", models: []string{"appsync"}},
	{plugin: "athena", models: []string{"athena"}},
	{plugin: "autoscaling", models: []string{"autoscaling"}},
	{plugin: "backup", models: []string{"backup"}},
	{plugin: "batch", models: []string{"batch"}},
	{plugin: "bedrock-runtime", models: []string{"bedrock-runtime", "bedrock"}},
	{plugin: "budgets", models: []string{"budgets"}},
	{plugin: "ce", models: []string{"ce"}},
	{plugin: "cloudformation", models: []string{"cloudformation"}},
	{plugin: "cloudfront", models: []string{"cloudfront"}},
	{plugin: "cloudtrail", models: []string{"cloudtrail"}},
	{plugin: "codebuild", models: []string{"codebuild"}},
	{plugin: "codedeploy", models: []string{"codedeploy"}},
	{plugin: "codepipeline", models: []string{"codepipeline"}},
	{plugin: "cognito-identity", models: []string{"cognito-identity"}},
	{plugin: "cognito-idp", models: []string{"cognito-idp"}},
	{plugin: "config", models: []string{"config"}},
	{plugin: "dynamodb", models: []string{"dynamodb", "dynamodbstreams"}},
	{plugin: "ec2", models: []string{"ec2"}},
	{plugin: "ecr", models: []string{"ecr"}},
	{plugin: "ecs", models: []string{"ecs"}},
	{plugin: "efs", models: []string{"efs"}},
	{plugin: "elasticache", models: []string{"elasticache"}},
	{plugin: "elasticloadbalancing", models: []string{"elbv2", "elb"}},
	{plugin: "emrserverless", models: []string{"emr-serverless"}},
	{plugin: "eventbridge", models: []string{"events"}},
	{plugin: "execute-api", unmodelled: "the execute-api data plane invokes a deployed API; botocore models no request shape for it"},
	{plugin: "firehose", models: []string{"firehose"}},
	{plugin: "fsx", models: []string{"fsx"}},
	{plugin: "glue", models: []string{"glue"}},
	{plugin: "health", models: []string{"health"}},
	{plugin: "iam", models: []string{"iam"}},
	{plugin: "kinesis", models: []string{"kinesis"}},
	{plugin: "kms", models: []string{"kms"}},
	{plugin: "lambda", models: []string{"lambda"}},
	{plugin: "logs", models: []string{"logs"}},
	{plugin: "monitoring", models: []string{"cloudwatch"}},
	{plugin: "msk", models: []string{"kafka"}},
	{plugin: "omics", models: []string{"omics"}},
	{plugin: "opensearch", unmodelled: "the plugin routes no operation names: it dispatches on an HTTP method and a path shape"},
	{plugin: "organizations", models: []string{"organizations"}},
	{plugin: "pricing", models: []string{"pricing"}},
	{plugin: "quicksight", models: []string{"quicksight"}},
	{plugin: "ram", models: []string{"ram"}},
	{plugin: "rds", models: []string{"rds"}},
	{plugin: "redshift", models: []string{"redshift"}},
	{plugin: "redshift-data", models: []string{"redshift-data"}},
	{plugin: "route53", models: []string{"route53"}},
	{plugin: "s3", models: []string{"s3"}},
	{plugin: "sagemaker", models: []string{"sagemaker"}},
	{plugin: "scheduler", models: []string{"scheduler"}},
	{plugin: "secretsmanager", models: []string{"secretsmanager"}},
	{plugin: "servicequotas", models: []string{"service-quotas"}},
	{plugin: "ses", models: []string{"ses"}},
	{plugin: "sesv2", models: []string{"sesv2"}},
	{plugin: "sns", models: []string{"sns"}},
	{plugin: "sqs", models: []string{"sqs"}},
	{plugin: "ssm", models: []string{"ssm"}},
	{plugin: "sso", models: []string{"sso-admin"}},
	{plugin: "states", models: []string{"stepfunctions"}},
	{plugin: "sts", models: []string{"sts"}},
	{plugin: "tagging", models: []string{"resourcegroupstaggingapi"}},
	{plugin: "timestream", models: []string{"timestream-write", "timestream-query"}},
	{plugin: "transfer", models: []string{"transfer"}},
	{plugin: "wafv2", models: []string{"wafv2"}},
}

// modelNames returns every botocore model pluginModels names, sorted and without repeats.
func modelNames() []string {
	seen := map[string]bool{}
	for _, pm := range pluginModels {
		for _, m := range pm.models {
			seen[m] = true
		}
	}
	return sortedKeys(seen)
}
