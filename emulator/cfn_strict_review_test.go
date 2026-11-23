package emulator_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfn_strict_review_test.go pins the review fixes to #1605's strict mode.

// cfnSAMHelloWorld is the template `sam init`'s hello-world Python application synthesizes. Its
// Outputs read ServerlessRestApi and HelloWorldFunctionRole, which only the SAM transform
// declares.
const cfnSAMHelloWorld = `AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: sam-app
Globals:
  Function:
    Timeout: 3
Resources:
  HelloWorldFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: hello_world/
      Handler: app.lambda_handler
      Runtime: python3.13
      Architectures:
        - x86_64
      Events:
        HelloWorld:
          Type: Api
          Properties:
            Path: /hello
            Method: get
Outputs:
  HelloWorldApi:
    Description: API Gateway endpoint URL for Prod stage for Hello World function
    Value: !Sub "https://${ServerlessRestApi}.execute-api.${AWS::Region}.amazonaws.com/Prod/hello/"
  HelloWorldFunction:
    Description: Hello World Lambda Function ARN
    Value: !GetAtt HelloWorldFunction.Arn
  HelloWorldFunctionIamRole:
    Description: Implicit IAM Role created for Hello World function
    Value: !GetAtt HelloWorldFunctionRole.Arn
`

// TestCFNStrict_SAMTemplateNamesWhatItsMacroDeclares pins that a template declaring a Transform is
// not refused for the names its macro generates: lenient mode deploys the SAM hello-world app,
// ValidateTemplate accepts it, and strict mode refuses it only as unemulated.
func TestCFNStrict_SAMTemplateNamesWhatItsMacroDeclares(t *testing.T) {
	result, err := newStrictDeployer(t, emulator.WithDeployerLenient(true)).
		Deploy(context.Background(), cfnSAMHelloWorld, "sam-app", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, "CREATE_COMPLETE", result.Status, result.StatusReason)

	_, err = newStrictDeployer(t).Deploy(context.Background(), cfnSAMHelloWorld, "sam-app", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrCFNUnemulatedType)

	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": cfnSAMHelloWorld})
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<member>AWS::Serverless-2016-10-31</member>")
}

// TestCFNStrict_ValidateTemplateReadsATemplateURL pins that ValidateTemplate reads a TemplateURL
// from substrate's S3 through the resolver CreateStack uses (#1606), and that a missing object is
// refused.
func TestCFNStrict_ValidateTemplateReadsATemplateURL(t *testing.T) {
	ts := emulator.StartTestServer(t)
	put := func(path, body string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Host = "s3.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
	}
	put("/templates", "")
	put("/templates/t.json", `{"Description":"from s3","Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`)

	code, body := cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{
		"TemplateURL": "https://s3.us-east-1.amazonaws.com/templates/t.json"})
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<Description>from s3</Description>")

	code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{
		"TemplateURL": "https://s3.us-east-1.amazonaws.com/templates/missing.json"})
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Contains(t, cfnErrorMessage(t, body), "S3 error:")
}

// TestCFNStrict_TransformObjectForm pins the `{Name: …, Parameters: …}` form of a macro, alone and
// inside a list.
func TestCFNStrict_TransformObjectForm(t *testing.T) {
	ts := emulator.StartTestServer(t)
	for body, want := range map[string][]string{
		`{"Transform":{"Name":"AWS::Include","Parameters":{"Location":"s3://b/k"}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`: {
			"AWS::Include"},
		`{"Transform":["AWS::LanguageExtensions",{"Name":"AWS::Include","Parameters":{}}],"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`: {
			"AWS::LanguageExtensions", "AWS::Include"},
	} {
		code, resp := cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": body})
		require.Equal(t, http.StatusOK, code, resp)
		var r struct {
			Transforms []string `xml:"ValidateTemplateResult>DeclaredTransforms>member"`
		}
		require.NoError(t, xml.Unmarshal([]byte(resp), &r))
		assert.Equal(t, want, r.Transforms)
	}
}

// TestCFNStrict_ValidateTemplateEvaluatesNoCondition pins that ValidateTemplate, given no
// parameter values, orders every resource: a DependsOn naming a resource whose Condition is false
// under the parameters' defaults is not refused.
func TestCFNStrict_ValidateTemplateEvaluatesNoCondition(t *testing.T) {
	const tmpl = `{"Parameters":{"Make":{"Type":"String","Default":"no"}},
		"Conditions":{"Wanted":{"Fn::Equals":[{"Ref":"Make"},"yes"]}},
		"Resources":{"Maybe":{"Type":"AWS::SQS::Queue","Condition":"Wanted"},
			"After":{"Type":"AWS::SQS::Queue","DependsOn":"Maybe"}}}`
	ts := emulator.StartTestServer(t)
	code, body := cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": tmpl})
	assert.Equal(t, http.StatusOK, code, body)

	code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": `{"Resources":{` +
		`"After":{"Type":"AWS::SQS::Queue","DependsOn":"Nowhere"}}}`})
	assert.Equal(t, http.StatusBadRequest, code, "an undeclared DependsOn is still refused: %s", body)
}

// TestCFNStrict_RegistryAndModuleTypes pins that a third-party, private-registry or module type is
// recognized but not emulated: strict mode refuses it with substrate's message, and lenient mode
// stubs it. An AWS:: type the specification does not list is still unrecognized.
func TestCFNStrict_RegistryAndModuleTypes(t *testing.T) {
	const tmpl = `{"Resources":{"Mon":{"Type":"Datadog::Monitors::Monitor","Properties":{"Query":"q"}},
		"Atlas":{"Type":"MongoDB::Atlas::Cluster"},"Mod":{"Type":"MyOrg::Network::Vpc::MODULE"}}}`
	_, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "registry", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrCFNUnemulatedType)
	for _, typ := range []string{"Datadog::Monitors::Monitor (Mon)", "MongoDB::Atlas::Cluster (Atlas)", "MyOrg::Network::Vpc::MODULE (Mod)"} {
		assert.Contains(t, err.Error(), typ)
	}

	result, err := newStrictDeployer(t, emulator.WithDeployerLenient(true)).Deploy(context.Background(), tmpl, "registry", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	_, err = newStrictDeployer(t, emulator.WithDeployerLenient(true)).Deploy(context.Background(),
		`{"Resources":{"X":{"Type":"AWS::Nope::Thing"},"Y":{"Type":"AWS::Lambda::Function::MODULE"}}}`, "aws", nil)
	require.Error(t, err)
	assert.Equal(t, "Template format error: Unrecognized resource types: [AWS::Nope::Thing]", err.Error(),
		"an AWS module is recognized; an unlisted AWS type is not")
}

// TestCFNStrict_RollbackIsNotHeldToPropertyValidation pins that the rollback of a failed update
// converges on the previous template even when it carries a property the type does not declare,
// as a template deployed before property validation existed may. The update changes the queue in
// place, so the rollback has to re-apply the previous template's declaration to it.
func TestCFNStrict_RollbackIsNotHeldToPropertyValidation(t *testing.T) {
	d := newStrictDeployer(t)
	ctx := context.Background()
	_, err := d.Deploy(ctx, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"legacy-q","VisibilityTimeout":30}}}}`, "legacy", nil)
	require.NoError(t, err)
	require.NoError(t, d.RewriteStackTemplateForTest(ctx, "legacy",
		`{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"legacy-q","VisibilityTimeout":30,"Retired":"yes"}}}}`))

	result, err := d.UpdateStack(ctx, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"legacy-q","VisibilityTimeout":60}},`+
		`"Bad":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"Not_A_Valid_Bucket"}}}}`, "legacy", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)
}

// TestCFNStrict_ARefusedLifecyclePolicyDeletesItsRepository pins that a repository whose
// LifecyclePolicy is refused is not left behind: the resource fails, and a failed resource is not
// one the rollback deletes, so the deploy deletes it.
func TestCFNStrict_ARefusedLifecyclePolicyDeletesItsRepository(t *testing.T) {
	ts := emulator.StartTestServer(t)
	cfnSeedRole(t, ts.StateManager(), "ecrnarrow", []string{"ecr:CreateRepository", "ecr:DeleteRepository"})
	client := &cfnRoleTestClient{t: t, baseURL: ts.URL}
	code, body := client.call("CreateStack", map[string]string{
		"StackName": "repo",
		"TemplateBody": `{"Resources":{"Repo":{"Type":"AWS::ECR::Repository","Properties":{` +
			`"RepositoryName":"orphan-check","LifecyclePolicy":{"LifecyclePolicyText":"{\"rules\":[]}"}}}}}`,
		"RoleARN": "arn:aws:iam::123456789012:role/ecrnarrow",
	})
	require.Equal(t, http.StatusOK, code, body)
	_, body = client.call("DescribeStacks", map[string]string{"StackName": "repo"})
	status, _, reason := cfnStackStatus(t, body)
	assert.Equal(t, "ROLLBACK_COMPLETE", status)
	assert.Contains(t, reason, "AccessDenied")

	code, raw := sjJSON(t, ts, "api.ecr.us-east-1.amazonaws.com", "AmazonEC2ContainerRegistry_V1_1_0.DescribeRepositories",
		map[string]any{"repositoryNames": []string{"orphan-check"}})
	assert.Equal(t, http.StatusBadRequest, code, "the repository was deleted: %s", raw)
	assert.Contains(t, string(raw), "RepositoryNotFoundException")
}

// TestCFNStrict_HostedZoneIDWithoutRecordedMetadata pins the fallback for a load balancer recorded
// before CanonicalHostedZoneID was: its type is read from its ARN, and a Region the table does not
// list is still refused.
func TestCFNStrict_HostedZoneIDWithoutRecordedMetadata(t *testing.T) {
	lb := func(arn string) emulator.DeployedResource {
		return emulator.DeployedResource{LogicalID: "Lb", Type: "AWS::ElasticLoadBalancingV2::LoadBalancer",
			PhysicalID: "lb", ARN: arn}
	}
	v, failures := emulator.CFNGetAttValueForTest(lb("arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/lb/abc"),
		"CanonicalHostedZoneID", "us-east-1")
	assert.Equal(t, "Z35SXDOTRQ7X7K", v)
	assert.Empty(t, failures)
	v, _ = emulator.CFNGetAttValueForTest(lb("arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/net/lb/abc"),
		"CanonicalHostedZoneID", "us-west-2")
	assert.Equal(t, "Z18D5FSROUN65G", v)
	v, failures = emulator.CFNGetAttValueForTest(lb("arn:aws:elasticloadbalancing:xx-nowhere-1:123456789012:loadbalancer/app/lb/abc"),
		"CanonicalHostedZoneID", "xx-nowhere-1")
	assert.Empty(t, v)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[0], "has no value in substrate")
}

// TestCFNStrict_GeneratedNamesForUnnamedTypes pins that a cache parameter group and an EFS file
// system, neither of which declares a name property, are named per stack: two stacks declaring
// the same logical ID get two resources, not one shared one.
func TestCFNStrict_GeneratedNamesForUnnamedTypes(t *testing.T) {
	d := newStrictDeployer(t)
	ctx := context.Background()
	const tmpl = `{"Resources":{"Pg":{"Type":"AWS::ElastiCache::ParameterGroup",` +
		`"Properties":{"CacheParameterGroupFamily":"redis7","Description":"d"}},` +
		`"Fs":{"Type":"AWS::EFS::FileSystem"}}}`
	a, err := d.Deploy(ctx, tmpl, "stack-a", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, a)
	b, err := d.Deploy(ctx, tmpl, "stack-b", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, b)

	pgA, pgB := findResource(t, a, "Pg").PhysicalID, findResource(t, b, "Pg").PhysicalID
	assert.NotEqual(t, pgA, pgB)
	assert.True(t, strings.HasPrefix(pgA, "stack-a-pg-"), pgA)
	assert.Equal(t, strings.ToLower(pgA), pgA, "ElastiCache stores the name lowercase")
	assert.NotEqual(t, findResource(t, a, "Fs").PhysicalID, findResource(t, b, "Fs").PhysicalID)
}
