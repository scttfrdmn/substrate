package emulator

import (
	"context"
	"fmt"
	"time"
)

//go:generate go run ../cmd/gen-service-reference -out ../docs/services.md

// RegisterPluginsOption configures optional dependencies passed to the plugins
// [RegisterDefaultPlugins] registers.
//
// Variadic rather than another positional parameter because the function is
// exported and called from six places, most of which have nothing to pass: a new
// parameter would make every one of them say so explicitly.
type RegisterPluginsOption func(*registerPluginsSettings)

// registerPluginsSettings accumulates what the options set.
type registerPluginsSettings struct {
	auth *AuthController
}

// WithPluginAuth gives a plugin that dispatches requests of its own the controller
// that authorizes them.
//
// Two plugins take it, and both for the same reason: their calls do not arrive
// through the server, which is where every other request is authorized before
// routing. CloudFormation dispatches a stack's resource calls, and without this a
// template asking for a permission it did not have deployed cleanly. EC2's
// CreateFleet launches instances by calling its own RunInstances path, and without
// this a fleet launched resources no policy was consulted about — the guardrail a
// caller wrote for RunInstances was bypassed by asking for a fleet instead (#673).
//
// A plugin holding the controller still authorizes only requests carrying a
// principal, so substrate's own unenforced built-in caller is unaffected.
func WithPluginAuth(auth *AuthController) RegisterPluginsOption {
	return func(s *registerPluginsSettings) {
		s.auth = auth
	}
}

// pluginWiring is the dependency set every plugin registration shares.
//
// It exists so that [RegisterDefaultPlugins] builds a [PluginConfig] in exactly one
// place. Before it, each registration spelled its own config out, and fifteen of the
// sixty-seven left "time_controller" out of the options map — so those plugins fell
// back to a private wall-clock controller and ignored both the time control endpoints
// and the clock a replay sets per event, inside an emulator whose whole claim is a
// controlled clock (#904). The omission was invisible at each individual call site,
// because the fallback is silent by design; routing every registration through one
// method is what makes a new plugin unable to repeat it.
type pluginWiring struct {
	registry *PluginRegistry
	state    StateManager
	logger   Logger
	tc       *TimeController
}

// register initializes plugin with the shared wiring plus extra, and adds it to the
// registry. name appears in the error a failed initialization is wrapped with.
//
// extra is not mutated: its entries are copied into a fresh options map, so a caller
// may reuse one map across registrations, and "time_controller" cannot be shadowed by
// a stale entry from a previous plugin.
func (w pluginWiring) register(ctx context.Context, plugin Plugin, name string, extra map[string]any) error {
	options := make(map[string]any, len(extra)+1)
	for k, v := range extra {
		options[k] = v
	}
	// A nil controller is left out rather than stored. Every plugin reads the value
	// with a type assertion to *TimeController, which *succeeds* for a typed nil and
	// hands the plugin a clock that panics on its first call — where an absent key
	// takes the documented fallback instead. The Cost Explorer registration guarded
	// this individually before the wiring was shared.
	if w.tc != nil {
		options["time_controller"] = w.tc
	}
	if err := plugin.Initialize(ctx, PluginConfig{
		State:   w.state,
		Logger:  w.logger,
		Options: options,
	}); err != nil {
		return fmt.Errorf("initialize %s plugin: %w", name, err)
	}
	w.registry.Register(plugin)
	return nil
}

// RegisterDefaultPlugins initializes and registers all built-in service plugins
// into registry. This function is called by both the server binary and
// [StartTestServer] so the same plugin set is always available.
// store is optional; pass nil when the EventStore is unavailable (e.g. in
// test helpers that do not need cost-derived data in the Cost Explorer plugin).
// cfg is optional; pass nil to disable all Docker-backed features (Lambda
// Docker execution, RDS container engine).
// Adding a plugin here also requires a metadata entry in cmd/gen-service-reference
// (enforced by `make docs-reference-check` in CI).
//
// Every registration goes through [pluginWiring.register], which supplies the
// simulated clock; a plugin needing more than that passes it as extra options.
//
// opts carry the dependencies only some callers have; see [WithPluginAuth].
func RegisterDefaultPlugins(
	ctx context.Context,
	registry *PluginRegistry,
	state StateManager,
	tc *TimeController,
	logger Logger,
	store *EventStore,
	cfg *Config,
	opts ...RegisterPluginsOption,
) error {
	var settings registerPluginsSettings
	for _, opt := range opts {
		opt(&settings)
	}
	// Resolve optional Docker-backed executors from cfg.
	var lambdaExec *LambdaExecutor
	var rdsExec *RDSExecutor
	if cfg != nil && cfg.Lambda.DockerEnabled {
		ttl, err := time.ParseDuration(cfg.Lambda.WarmPoolTTL)
		if err != nil {
			ttl = 5 * time.Minute
		}
		lambdaExec = NewLambdaExecutor(LambdaExecCfg{
			ReplayMode:  cfg.Lambda.ReplayMode,
			WarmPoolTTL: ttl,
		}, logger)
	}
	if cfg != nil && cfg.RDS.Engine == "container" {
		rdsExec = NewRDSExecutor(logger)
	}

	w := pluginWiring{registry: registry, state: state, logger: logger, tc: tc}

	// registryOpt is the options map for a plugin that dispatches into the registry
	// rather than only serving its own requests.
	registryOpt := map[string]any{"registry": registry}

	lambdaOpts := map[string]any{"registry": registry}
	if lambdaExec != nil {
		lambdaOpts["lambda_exec"] = lambdaExec
	}

	ec2Opts := map[string]any{}
	if settings.auth != nil {
		// CreateFleet launches through EC2's own RunInstances path rather than through
		// the server, so the fleet's launches are authorized by the plugin itself.
		ec2Opts["auth_controller"] = settings.auth
	}

	// The CloudFormation plugin adapts the StackDeployer that [Client] already
	// drives in process, so it needs the registry to deploy each template resource
	// into. It is registered after S3/IAM/Lambda only for readability: the registry
	// is captured as a pointer and read at request time, so ordering does not
	// affect which resource types a template can create.
	cfnOpts := map[string]any{"registry": registry}
	if store != nil {
		cfnOpts["event_store"] = store
	}
	if settings.auth != nil {
		cfnOpts["auth_controller"] = settings.auth
	}

	rdsOpts := map[string]any{}
	if rdsExec != nil {
		rdsOpts["rds_executor"] = rdsExec
	}

	ceOpts := map[string]any{}
	if store != nil {
		ceOpts["event_store"] = store
	}

	registrations := []struct {
		plugin Plugin
		name   string
		extra  map[string]any
	}{
		{&IAMPlugin{}, "iam", nil},
		{&STSPlugin{}, "sts", nil},
		{&LambdaPlugin{}, "lambda", lambdaOpts},
		{&SQSPlugin{}, "sqs", nil},
		{&DynamoDBPlugin{}, "dynamodb", nil},
		{&EC2Plugin{}, "ec2", ec2Opts},
		{&S3Plugin{}, "s3", registryOpt},
		{&CloudFormationPlugin{}, "cloudformation", cfnOpts},
		{&ELBPlugin{}, "elb", nil},
		{&Route53Plugin{}, "route53", nil},
		{&TaggingPlugin{}, "tagging", nil},
		{&SNSPlugin{}, "sns", registryOpt},
		{&SecretsManagerPlugin{}, "secretsmanager", nil},
		{&SSMPlugin{}, "ssm", nil},
		{&KMSPlugin{}, "kms", nil},
		{&CloudWatchLogsPlugin{}, "cloudwatchlogs", nil},
		{&EventBridgePlugin{}, "eventbridge", nil},
		{&AccountPlugin{}, "account", nil},
		{&ConfigServicePlugin{}, "config", nil},
		{&SchedulerPlugin{}, "scheduler", nil},
		{&CloudWatchPlugin{}, "cloudwatch", nil},
		{&ACMPlugin{}, "acm", nil},
		{&APIGatewayPlugin{}, "apigateway", nil},
		{&APIGatewayV2Plugin{}, "apigatewayv2", nil},
		{&APIGatewayProxyPlugin{}, "apigateway-proxy", registryOpt},
		{&StepFunctionsPlugin{}, "stepfunctions", registryOpt},
		{&ECRPlugin{}, "ecr", nil},
		{&ECSPlugin{}, "ecs", nil},
		{&CognitoIDPPlugin{}, "cognito-idp", nil},
		{&CognitoIdentityPlugin{}, "cognito-identity", nil},
		{&KinesisPlugin{}, "kinesis", nil},
		{&CloudFrontPlugin{}, "cloudfront", nil},
		{&RDSPlugin{}, "rds", rdsOpts},
		{&ElastiCachePlugin{}, "elasticache", nil},
		{&EFSPlugin{}, "efs", nil},
		{&GluePlugin{}, "glue", nil},
		{&CEPlugin{}, "ce", ceOpts},
		{&BudgetsPlugin{}, "budgets", nil},
		{&HealthPlugin{}, "health", nil},
		{&PriceListPlugin{}, "pricing", nil},
		{&OrganizationsPlugin{}, "organizations", nil},
		{&SESv2Plugin{}, "sesv2", nil},
		{&FirehosePlugin{}, "firehose", nil},
		{&ServiceQuotasPlugin{}, "servicequotas", nil},
		{&AppSyncPlugin{}, "appsync", nil},
		{&MSKPlugin{}, "msk", nil},
		{&FSxPlugin{}, "fsx", nil},
		{&BatchPlugin{}, "batch", nil},
		{&SageMakerPlugin{}, "sagemaker", nil},
		{&EMRServerlessPlugin{}, "emrserverless", nil},
		{&OmicsPlugin{}, "omics", nil},
		{&QuickSightPlugin{}, "quicksight", nil},
		{&BedrockRuntimePlugin{}, "bedrock-runtime", nil},
		{&AthenaPlugin{}, "athena", nil},
		{&OpenSearchPlugin{}, "opensearch", nil},
		{&WAFv2Plugin{}, "wafv2", nil},
		{&CloudTrailPlugin{}, "cloudtrail", nil},
		{&CodeBuildPlugin{}, "codebuild", nil},
		{&CodePipelinePlugin{}, "codepipeline", nil},
		{&CodeDeployPlugin{}, "codedeploy", nil},
		{&BackupPlugin{}, "backup", nil},
		{&TransferPlugin{}, "transfer", nil},
		{&SSOPlugin{}, "sso", nil},
		{&RAMPlugin{}, "ram", nil},
		{&RedshiftPlugin{}, "redshift", nil},
		{&RedshiftDataPlugin{}, "redshift-data", nil},
		{&TimestreamPlugin{}, "timestream", nil},
	}
	for _, r := range registrations {
		if err := w.register(ctx, r.plugin, r.name, r.extra); err != nil {
			return err
		}
	}

	return nil
}
