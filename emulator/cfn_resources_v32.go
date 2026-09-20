package emulator

// cfn_resources_v32.go holds the StackDeployer.deployResource helpers for
// Athena, Backup, CloudTrail, the CodeSuite, OpenSearch, Transfer and WAFv2.
// The name records the substrate release that added them (v0.32.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

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
//
// Ref returns "name|id|scope", so the name and the scope are recorded in Metadata: the ARN
// carries the name and the ID but not the scope, and the scope is not derivable from the ARN
// once it is in the segment (#827).
//
// Scope is a required property whose allowed values are CLOUDFRONT and REGIONAL, and it
// selects the ARN's scope segment — "cloudfront" or "regional". That segment used to be
// hardcoded to "regional", so a CLOUDFRONT web ACL reported an ARN naming a scope it does not
// have. It is corrected here rather than left wrong beside a Metadata entry recording the real
// scope two lines away.
//
// The ARN comes from wafv2ARN, which the WAFv2 plugin also uses. Fixing the segment here alone
// left the plugin still hardcoding "regional", so the same logical web ACL reported two
// different ARNs depending on which path created it; one builder is what stops that recurring.
func (d *StackDeployer) deployWAFv2WebACL(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	scope := resolveStringProp(props, "Scope", "REGIONAL", cctx)
	arn := wafv2ARN(cctx.region, cctx.accountID, scope, "webacl", name, logicalID)
	d.stubStore(ctx, cctx.accountID, cctx.region, logicalID, props)
	return DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::WAFv2::WebACL",
		PhysicalID: logicalID,
		ARN:        arn,
		Metadata: map[string]interface{}{
			"Name":  name,
			"Scope": scope,
		},
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
//
// Ref returns the trail's resource name, not its ARN, so the name is recorded rather than cut
// back out of the ARN at resolve time (#827).
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
		Metadata:   map[string]interface{}{"TrailName": name},
	}, 0, nil
}

// The AWS::Config::ConfigRule and AWS::Config::ConfigurationRecorder stubs that
// shipped here in v0.32.0 are gone: they now dispatch real Config operations from
// cfn_resources_v101.go, which became possible once the service's handlers existed
// (#580).

// deployTransferServer creates an AWS Transfer Family server stub.
//
// The Ref value is the server **ARN**, not the server ID (#1234 corrected this comment, which had
// said the ID and was the opposite of both the page and the tree). AWS::Transfer::Server's Return
// values section publishes "Ref returns the server ARN, such as
// arn:aws:transfer:us-east-1:123456789012:server/s-01234567890abcdef", and offers the ID only as the
// ServerId Fn::GetAtt attribute. cfn_intrinsics.go resolves it that way already; only the comment
// here had drifted, presumably from the v0.32.0 stub that shipped before the Ref was settled.
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
