package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ----- v0.32.0 — Extended CFN stubs -------------------------------------------

// stubStore persists resource properties into cfnStubNamespace.
func (d *StackDeployer) stubStore(ctx context.Context, acct, region, logicalID string, props map[string]interface{}) {
	if d.state == nil || props == nil {
		return
	}
	data, err := json.Marshal(props)
	if err != nil {
		return
	}
	key := fmt.Sprintf("%s/%s/%s", acct, region, logicalID)
	_ = d.state.Put(ctx, cfnStubNamespace, key, data)
}

// deployOpenSearchDomain creates an OpenSearch domain stub.
// The Ref value is the domain name.
func (d *StackDeployer) deployOpenSearchDomain(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DomainName", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:es:%s:%s:domain/%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::OpenSearchService::Domain",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployWAFv2WebACL creates a WAFv2 WebACL stub.
// The Ref value is the WebACL ID.
func (d *StackDeployer) deployWAFv2WebACL(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:wafv2:%s:%s:regional/webacl/%s/%s", cctx.region, cctx.accountID, name, logicalID)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::WAFv2::WebACL",
		PhysicalID: logicalID,
		ARN:        arn,
	}, 0, nil
}

// deployBackupBackupPlan creates an AWS Backup plan stub.
// The Ref value is the backup plan ID.
func (d *StackDeployer) deployBackupBackupPlan(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	arn := fmt.Sprintf("arn:aws:backup:%s:%s:backup-plan:%s", cctx.region, cctx.accountID, logicalID)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Backup::BackupPlan",
		PhysicalID: logicalID,
		ARN:        arn,
	}, 0, nil
}

// deployCodeBuildProject creates a CodeBuild project stub.
// The Ref value is the project name.
func (d *StackDeployer) deployCodeBuildProject(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:codebuild:%s:%s:project/%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CodeBuild::Project",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployCodePipelinePipeline creates a CodePipeline pipeline stub.
// The Ref value is the pipeline name.
func (d *StackDeployer) deployCodePipelinePipeline(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// Pipeline name may be nested inside a PipelineConfig or as a top-level Name.
	name := resolveStringProp(props, "Name", logicalID, cctx)
	// CodePipeline ARN format has no resource-type prefix.
	arn := fmt.Sprintf("arn:aws:codepipeline:%s:%s:%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CodePipeline::Pipeline",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployCodeDeployDeploymentGroup creates a CodeDeploy deployment group stub.
// The Ref value is the deployment group name.
func (d *StackDeployer) deployCodeDeployDeploymentGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	appName := resolveStringProp(props, "ApplicationName", logicalID, cctx)
	name := resolveStringProp(props, "DeploymentGroupName", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:codedeploy:%s:%s:deploymentgroup:%s/%s", cctx.region, cctx.accountID, appName, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CodeDeploy::DeploymentGroup",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployCloudTrailTrail creates a CloudTrail trail stub.
// The Ref value is the trail ARN.
func (d *StackDeployer) deployCloudTrailTrail(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "TrailName", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:cloudtrail:%s:%s:trail/%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudTrail::Trail",
		PhysicalID: arn,
		ARN:        arn,
	}, 0, nil
}

// deployConfigConfigRule creates an AWS Config rule stub.
// The Ref value is the config rule name.
func (d *StackDeployer) deployConfigConfigRule(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "ConfigRuleName", logicalID, cctx)
	// Config rule ARN suffix is "config-rule-{logicalID}".
	arn := fmt.Sprintf("arn:aws:config:%s:%s:config-rule/config-rule-%s", cctx.region, cctx.accountID, logicalID)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Config::ConfigRule",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployConfigConfigurationRecorder creates an AWS Config recorder stub.
// The Ref value is the recorder name.
func (d *StackDeployer) deployConfigConfigurationRecorder(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:config:%s:%s:recorder/%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Config::ConfigurationRecorder",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}

// deployTransferServer creates an AWS Transfer Family server stub.
// The Ref value is the server ID.
func (d *StackDeployer) deployTransferServer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	serverID := "s-" + strings.ToLower(logicalID)
	arn := fmt.Sprintf("arn:aws:transfer:%s:%s:server/%s", cctx.region, cctx.accountID, serverID)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Transfer::Server",
		PhysicalID: serverID,
		ARN:        arn,
	}, 0, nil
}

// deployAthenaWorkGroup creates an Athena WorkGroup stub.
// The Ref value is the workgroup name.
func (d *StackDeployer) deployAthenaWorkGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s", cctx.region, cctx.accountID, name)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Athena::WorkGroup",
		PhysicalID: name,
		ARN:        arn,
	}, 0, nil
}
