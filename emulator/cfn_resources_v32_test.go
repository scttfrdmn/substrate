package emulator_test

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

// The Config rule and recorder tests that lived here asserted the stub behavior
// this release removed, and could not be carried over by adjusting their
// expectations: each template was invalid against the real API — the rule's carried
// no Source, the recorder's no RoleARN — and newTestDeployer registers no Config
// plugin, so both went green while creating nothing. That is the defect, not the
// baseline. Their replacements are in cfn_resources_v101_test.go, against a deployer
// that registers Config.

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
