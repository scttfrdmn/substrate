package substrate

import (
	"context"
	"fmt"
)

// RegisterDefaultPlugins initialises and registers all built-in service plugins
// into registry. This function is called by both the server binary and
// [StartTestServer] so the same plugin set is always available.
// store is optional; pass nil when the EventStore is unavailable (e.g. in
// test helpers that do not need cost-derived data in the Cost Explorer plugin).
func RegisterDefaultPlugins(
	ctx context.Context,
	registry *PluginRegistry,
	state StateManager,
	tc *TimeController,
	logger Logger,
	store *EventStore,
) error {
	iamPlugin := &IAMPlugin{}
	if err := iamPlugin.Initialize(ctx, PluginConfig{State: state, Logger: logger}); err != nil {
		return fmt.Errorf("initialize iam plugin: %w", err)
	}
	registry.Register(iamPlugin)

	stsPlugin := &STSPlugin{}
	if err := stsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize sts plugin: %w", err)
	}
	registry.Register(stsPlugin)

	lambdaPlugin := &LambdaPlugin{}
	if err := lambdaPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize lambda plugin: %w", err)
	}
	registry.Register(lambdaPlugin)

	sqsPlugin := &SQSPlugin{}
	if err := sqsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize sqs plugin: %w", err)
	}
	registry.Register(sqsPlugin)

	dynamodbPlugin := &DynamoDBPlugin{}
	if err := dynamodbPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize dynamodb plugin: %w", err)
	}
	registry.Register(dynamodbPlugin)

	ec2Plugin := &EC2Plugin{}
	if err := ec2Plugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize ec2 plugin: %w", err)
	}
	registry.Register(ec2Plugin)

	s3Plugin := &S3Plugin{}
	if err := s3Plugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}); err != nil {
		return fmt.Errorf("initialize s3 plugin: %w", err)
	}
	registry.Register(s3Plugin)

	elbPlugin := &ELBPlugin{}
	if err := elbPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize elb plugin: %w", err)
	}
	registry.Register(elbPlugin)

	r53Plugin := &Route53Plugin{}
	if err := r53Plugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize route53 plugin: %w", err)
	}
	registry.Register(r53Plugin)

	taggingPlugin := &TaggingPlugin{}
	if err := taggingPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize tagging plugin: %w", err)
	}
	registry.Register(taggingPlugin)

	snsPlugin := &SNSPlugin{}
	if err := snsPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
		},
	}); err != nil {
		return fmt.Errorf("initialize sns plugin: %w", err)
	}
	registry.Register(snsPlugin)

	smPlugin := &SecretsManagerPlugin{}
	if err := smPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize secretsmanager plugin: %w", err)
	}
	registry.Register(smPlugin)

	ssmPlugin := &SSMPlugin{}
	if err := ssmPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize ssm plugin: %w", err)
	}
	registry.Register(ssmPlugin)

	kmsPlugin := &KMSPlugin{}
	if err := kmsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize kms plugin: %w", err)
	}
	registry.Register(kmsPlugin)

	cwLogsPlugin := &CloudWatchLogsPlugin{}
	if err := cwLogsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize cloudwatchlogs plugin: %w", err)
	}
	registry.Register(cwLogsPlugin)

	ebPlugin := &EventBridgePlugin{}
	if err := ebPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize eventbridge plugin: %w", err)
	}
	registry.Register(ebPlugin)

	cwPlugin := &CloudWatchPlugin{}
	if err := cwPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize cloudwatch plugin: %w", err)
	}
	registry.Register(cwPlugin)

	acmPlugin := &ACMPlugin{}
	if err := acmPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize acm plugin: %w", err)
	}
	registry.Register(acmPlugin)

	apigwPlugin := &APIGatewayPlugin{}
	if err := apigwPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize apigateway plugin: %w", err)
	}
	registry.Register(apigwPlugin)

	apigwv2Plugin := &APIGatewayV2Plugin{}
	if err := apigwv2Plugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize apigatewayv2 plugin: %w", err)
	}
	registry.Register(apigwv2Plugin)

	sfnPlugin := &StepFunctionsPlugin{}
	if err := sfnPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize stepfunctions plugin: %w", err)
	}
	registry.Register(sfnPlugin)

	ecrPlugin := &ECRPlugin{}
	if err := ecrPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize ecr plugin: %w", err)
	}
	registry.Register(ecrPlugin)

	ecsPlugin := &ECSPlugin{}
	if err := ecsPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize ecs plugin: %w", err)
	}
	registry.Register(ecsPlugin)

	cognitoIDPPlugin := &CognitoIDPPlugin{}
	if err := cognitoIDPPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize cognito-idp plugin: %w", err)
	}
	registry.Register(cognitoIDPPlugin)

	cognitoIdentityPlugin := &CognitoIdentityPlugin{}
	if err := cognitoIdentityPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize cognito-identity plugin: %w", err)
	}
	registry.Register(cognitoIdentityPlugin)

	kinesisPlugin := &KinesisPlugin{}
	if err := kinesisPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize kinesis plugin: %w", err)
	}
	registry.Register(kinesisPlugin)

	cfPlugin := &CloudFrontPlugin{}
	if err := cfPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize cloudfront plugin: %w", err)
	}
	registry.Register(cfPlugin)

	rdsPlugin := &RDSPlugin{}
	if err := rdsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize rds plugin: %w", err)
	}
	registry.Register(rdsPlugin)

	elasticachePlugin := &ElastiCachePlugin{}
	if err := elasticachePlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize elasticache plugin: %w", err)
	}
	registry.Register(elasticachePlugin)

	efsPlugin := &EFSPlugin{}
	if err := efsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize efs plugin: %w", err)
	}
	registry.Register(efsPlugin)

	gluePlugin := &GluePlugin{}
	if err := gluePlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize glue plugin: %w", err)
	}
	registry.Register(gluePlugin)

	cePlugin := &CEPlugin{}
	ceOpts := map[string]any{}
	if store != nil {
		ceOpts["event_store"] = store
	}
	if err := cePlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: ceOpts,
	}); err != nil {
		return fmt.Errorf("initialize ce plugin: %w", err)
	}
	registry.Register(cePlugin)

	budgetsPlugin := &BudgetsPlugin{}
	if err := budgetsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize budgets plugin: %w", err)
	}
	registry.Register(budgetsPlugin)

	healthPlugin := &HealthPlugin{}
	if err := healthPlugin.Initialize(ctx, PluginConfig{
		State:  state,
		Logger: logger,
	}); err != nil {
		return fmt.Errorf("initialize health plugin: %w", err)
	}
	registry.Register(healthPlugin)

	orgsPlugin := &OrganizationsPlugin{}
	if err := orgsPlugin.Initialize(ctx, PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		return fmt.Errorf("initialize organizations plugin: %w", err)
	}
	registry.Register(orgsPlugin)

	return nil
}
