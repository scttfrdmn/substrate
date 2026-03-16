package substrate_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCFN_GenericFallback verifies that truly unknown resource types produce a
// synthetic ARN rather than an error or empty PhysicalID.
func TestCFN_GenericFallback(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyWidget": {
				"Type": "AWS::SomeNewService::Widget",
				"Properties": { "Name": "my-widget" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "generic-fallback-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::SomeNewService::Widget", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.ARN)
	assert.Contains(t, r.ARN, "somenewservice")
	assert.Equal(t, "MyWidget", r.PhysicalID)
}

// TestCFN_OpenSearchDomain verifies the OpenSearch domain stub.
func TestCFN_OpenSearchDomain(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDomain": {
				"Type": "AWS::OpenSearchService::Domain",
				"Properties": { "DomainName": "my-search-domain" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "opensearch-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::OpenSearchService::Domain", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-search-domain", r.PhysicalID)
	assert.Contains(t, r.ARN, "arn:aws:es:")
	assert.Contains(t, r.ARN, "domain/my-search-domain")
}

// TestCFN_WAFv2WebACL verifies the WAFv2 WebACL stub.
func TestCFN_WAFv2WebACL(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyACL": {
				"Type": "AWS::WAFv2::WebACL",
				"Properties": { "Name": "my-acl", "Scope": "REGIONAL", "DefaultAction": {"Allow": {}} }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "wafv2-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::WAFv2::WebACL", r.Type)
	assert.Empty(t, r.Error)
	assert.Contains(t, r.ARN, "arn:aws:wafv2:")
	assert.Contains(t, r.ARN, "webacl/my-acl")
}

// TestCFN_BackupBackupPlan verifies the Backup plan stub.
func TestCFN_BackupBackupPlan(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPlan": {
				"Type": "AWS::Backup::BackupPlan",
				"Properties": {
					"BackupPlan": { "BackupPlanName": "daily-plan", "BackupPlanRule": [] }
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "backup-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Backup::BackupPlan", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "MyPlan", r.PhysicalID)
	assert.Contains(t, r.ARN, "arn:aws:backup:")
	assert.Contains(t, r.ARN, "backup-plan:MyPlan")
}

// TestCFN_CodeBuildProject verifies the CodeBuild project stub.
func TestCFN_CodeBuildProject(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyProject": {
				"Type": "AWS::CodeBuild::Project",
				"Properties": { "Name": "my-build-project" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "codebuild-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::CodeBuild::Project", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-build-project", r.PhysicalID)
	assert.Contains(t, r.ARN, "arn:aws:codebuild:")
	assert.Contains(t, r.ARN, "project/my-build-project")
}

// TestCFN_CodePipelinePipeline verifies the CodePipeline pipeline stub.
func TestCFN_CodePipelinePipeline(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPipeline": {
				"Type": "AWS::CodePipeline::Pipeline",
				"Properties": { "Name": "my-pipeline" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "codepipeline-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::CodePipeline::Pipeline", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-pipeline", r.PhysicalID)
	assert.Contains(t, r.ARN, "arn:aws:codepipeline:")
	assert.Contains(t, r.ARN, ":my-pipeline")
}

// TestCFN_CodeDeployDeploymentGroup verifies the CodeDeploy deployment group stub.
func TestCFN_CodeDeployDeploymentGroup(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyGroup": {
				"Type": "AWS::CodeDeploy::DeploymentGroup",
				"Properties": {
					"ApplicationName": "my-app",
					"DeploymentGroupName": "my-group"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "codedeploy-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::CodeDeploy::DeploymentGroup", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-group", r.PhysicalID)
	assert.Contains(t, r.ARN, "deploymentgroup:my-app/my-group")
}

// TestCFN_CloudTrailTrail verifies the CloudTrail trail stub.
func TestCFN_CloudTrailTrail(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyTrail": {
				"Type": "AWS::CloudTrail::Trail",
				"Properties": { "TrailName": "my-trail", "S3BucketName": "my-logs", "IsLogging": true }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cloudtrail-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::CloudTrail::Trail", r.Type)
	assert.Empty(t, r.Error)
	assert.Contains(t, r.ARN, "arn:aws:cloudtrail:")
	assert.Contains(t, r.ARN, "trail/my-trail")
}

// TestCFN_ConfigConfigRule verifies the Config rule stub.
func TestCFN_ConfigConfigRule(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": { "ConfigRuleName": "my-config-rule" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-rule-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Config::ConfigRule", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-config-rule", r.PhysicalID)
	assert.Contains(t, r.ARN, "config-rule/config-rule-MyRule")
}

// TestCFN_ConfigConfigurationRecorder verifies the Config recorder stub.
func TestCFN_ConfigConfigurationRecorder(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": { "Name": "default" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-recorder-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Config::ConfigurationRecorder", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "default", r.PhysicalID)
	assert.Contains(t, r.ARN, "recorder/default")
}

// TestCFN_TransferServer verifies the Transfer Family server stub.
func TestCFN_TransferServer(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyServer": {
				"Type": "AWS::Transfer::Server",
				"Properties": { "Protocols": ["SFTP"] }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "transfer-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Transfer::Server", r.Type)
	assert.Empty(t, r.Error)
	assert.Contains(t, r.PhysicalID, "s-")
	assert.Contains(t, r.ARN, "arn:aws:transfer:")
	assert.Contains(t, r.ARN, "server/s-")
}

// TestCFN_AthenaWorkGroup verifies the Athena WorkGroup stub.
func TestCFN_AthenaWorkGroup(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyWorkGroup": {
				"Type": "AWS::Athena::WorkGroup",
				"Properties": { "Name": "my-workgroup" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "athena-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Athena::WorkGroup", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "my-workgroup", r.PhysicalID)
	assert.Contains(t, r.ARN, "arn:aws:athena:")
	assert.Contains(t, r.ARN, "workgroup/my-workgroup")
}

// TestCFN_GlueRegression verifies that the existing Glue dispatch still works
// after the v0.32.0 changes (regression guard).
func TestCFN_GlueRegression(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDB": {
				"Type": "AWS::Glue::Database",
				"Properties": {
					"CatalogId": "123456789012",
					"DatabaseInput": { "Name": "my-glue-db" }
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "glue-regression-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Glue::Database", r.Type)
	assert.Empty(t, r.Error)
}
