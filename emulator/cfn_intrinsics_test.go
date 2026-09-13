package emulator_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// newRefDeployer builds a deployer over the full default plugin set.
//
// It does not reuse newTestDeployer, which registers six plugins by hand: a Ref value is
// whatever the deploy helper recorded, and a helper whose service is missing from the registry
// records a ServiceNotAvailable error instead — so the assertion would pass or fail for a reason
// that has nothing to do with the resolver. RegisterDefaultPlugins is also the registry the
// server runs, which is the configuration #827 was filed against.
func newRefDeployer(t *testing.T) *emulator.StackDeployer {
	t.Helper()
	ctx := context.Background()
	cfg := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	registry := emulator.NewPluginRegistry()
	require.NoError(t, emulator.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})
	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs)
}

// requireNoResourceErrors asserts every resource in the result deployed, naming the ones that
// did not.
//
// Deploy reports a failed resource on the result rather than as an error, the way CreateStack
// does, so a test that only checks err would pass against a stack that rolled back.
func requireNoResourceErrors(t *testing.T, result *emulator.DeployResult) {
	t.Helper()
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s (%s) failed to deploy", r.LogicalID, r.Type)
	}
}

// refELBTemplate is AWS's own shape for a load-balanced listener: the listener names its load
// balancer with `!Ref` and its default action names the target group the same way.
//
// This is #827's headline case. Both of those Refs resolved to a bare name, and CreateListener
// refuses a name where a load balancer ARN belongs — so the template did not deploy at all,
// which is as strong a statement of the defect as exists.
const refELBTemplate = `{
	"Resources": {
		"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}},
		"SubnetA": {"Type": "AWS::EC2::Subnet", "Properties": {
			"VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.0.1.0/24"}},
		"SubnetB": {"Type": "AWS::EC2::Subnet", "Properties": {
			"VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.0.2.0/24"}},
		"LB": {"Type": "AWS::ElasticLoadBalancingV2::LoadBalancer", "Properties": {
			"Name": "ref-lb",
			"Subnets": [{"Ref": "SubnetA"}, {"Ref": "SubnetB"}]}},
		"TG": {"Type": "AWS::ElasticLoadBalancingV2::TargetGroup", "Properties": {
			"Name": "ref-tg", "Port": "80", "Protocol": "HTTP", "VpcId": {"Ref": "Vpc"}}},
		"Listener": {"Type": "AWS::ElasticLoadBalancingV2::Listener", "Properties": {
			"LoadBalancerArn": {"Ref": "LB"},
			"Port": "80",
			"Protocol": "HTTP",
			"DefaultActions": [{"Type": "forward", "TargetGroupArn": {"Ref": "TG"}}]}}
	},
	"Outputs": {
		"LBRef": {"Value": {"Ref": "LB"}},
		"TGRef": {"Value": {"Ref": "TG"}},
		"ListenerRef": {"Value": {"Ref": "Listener"}}
	}
}`

// TestCFNRef_ELBv2TemplateInAWSShape is #827's second acceptance criterion: AWS's own listener
// shape deploys, and the listener the emulator stored routes to the target group.
func TestCFNRef_ELBv2TemplateInAWSShape(t *testing.T) {
	d := newRefDeployer(t)

	result, err := d.Deploy(context.Background(), refELBTemplate, "ref-elb-stack", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, "CREATE_COMPLETE", result.Status)

	lbARN := result.Outputs["LBRef"]
	tgARN := result.Outputs["TGRef"]
	assert.Contains(t, lbARN, ":loadbalancer/", "Ref on a load balancer is its ARN")
	assert.Contains(t, tgARN, ":targetgroup/", "Ref on a target group is its ARN")
	assert.Contains(t, result.Outputs["ListenerRef"], ":listener/",
		"Ref on a listener is its own ARN, which it already was")

	// The listener as the emulator stored it, not as the template asked for it: the default
	// action is the only place the resolved target-group Ref ends up, so this is what makes
	// the resolved value observable through an API call rather than only through an Output.
	resp, dispatchErr := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service:   "elasticloadbalancing",
		Operation: "DescribeListeners",
		Params:    map[string]string{"Action": "DescribeListeners", "LoadBalancerArn": lbARN},
		Headers:   map[string]string{},
	}, "probe")
	require.NoError(t, dispatchErr)
	require.NotNil(t, resp)
	body := string(resp.Body)
	assert.Contains(t, body, "<Type>forward</Type>")
	assert.Contains(t, body, "<TargetGroupArn>"+tgARN+"</TargetGroupArn>",
		"the stored listener's default action carries the target group's ARN")
}

// TestCFNRef_PerResourceType asserts the value AWS documents for each type whose Ref is not its
// physical ID, read out through a stack Output the way a template consumes it.
//
// One stack per case rather than one stack for all of them: a resource that fails takes the
// stack's other resources into rollback, so a single stack would report one defect as many.
func TestCFNRef_PerResourceType(t *testing.T) {
	tests := []struct {
		name      string
		resources string
		refOf     string
		check     func(t *testing.T, ref string)
	}{
		{
			// "Ref returns the queue URL" — not the name, and not the ARN.
			name:      "sqs queue is the queue URL",
			resources: `"Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "ref-queue"}}`,
			refOf:     "Q",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.Equal(t, "http://sqs.us-east-1.localhost/123456789012/ref-queue", ref)
			},
		},
		{
			// "Ref returns the key ID, such as 1234abcd-12ab-34cd-56ef-1234567890ab."
			name:      "kms key is the key ID",
			resources: `"Key": {"Type": "AWS::KMS::Key", "Properties": {"Description": "ref"}}`,
			refOf:     "Key",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.NotEmpty(t, ref)
				assert.NotContains(t, ref, "arn:", "the key ID, not the key ARN")
				assert.NotContains(t, ref, "/", "a key ID has no ARN resource segment")
			},
		},
		{
			// "Ref returns the resource name" — where substrate's physical ID is the ARN.
			name: "cloudtrail trail is the trail name",
			resources: `"Trail": {"Type": "AWS::CloudTrail::Trail", "Properties": {
				"TrailName": "ref-trail", "S3BucketName": "ref-bucket", "IsLogging": true}}`,
			refOf: "Trail",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.Equal(t, "ref-trail", ref)
			},
		},
		{
			// "Ref returns the server ARN, such as
			// arn:aws:transfer:us-east-1:123456789012:server/s-01234567890abcdef."
			name:      "transfer server is the server ARN",
			resources: `"Server": {"Type": "AWS::Transfer::Server", "Properties": {"Protocols": ["SFTP"]}}`,
			refOf:     "Server",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.True(t, strings.HasPrefix(ref, "arn:aws:transfer:"), "got %q", ref)
				assert.Contains(t, ref, ":server/s-")
			},
		},
		{
			// "The Ref for the resource, containing the resource name, physical ID, and
			// scope, formatted as follows: name|id|scope."
			name: "wafv2 web acl is name|id|scope",
			resources: `"Acl": {"Type": "AWS::WAFv2::WebACL", "Properties": {
				"Name": "ref-acl", "Scope": "CLOUDFRONT",
				"DefaultAction": {"Allow": {}}, "VisibilityConfig": {}}}`,
			refOf: "Acl",
			check: func(t *testing.T, ref string) {
				t.Helper()
				parts := strings.Split(ref, "|")
				require.Len(t, parts, 3, "got %q", ref)
				assert.Equal(t, "ref-acl", parts[0])
				assert.NotEmpty(t, parts[1])
				assert.Equal(t, "CLOUDFRONT", parts[2],
					"the scope is the template's, spelled as AWS spells it in the example")
			},
		},
		{
			// "Ref returns the Elastic IP address" — not the allocation ID.
			name:      "ec2 eip is the public IP address",
			resources: `"IP": {"Type": "AWS::EC2::EIP", "Properties": {"Domain": "vpc"}}`,
			refOf:     "IP",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.NotContains(t, ref, "eipalloc-", "the address, not the allocation ID")
				assert.Len(t, strings.Split(ref, "."), 4, "a dotted-quad address, got %q", ref)
			},
		},
		{
			// "Ref returns the ARN of the created state machine."
			name: "step functions state machine is the ARN",
			resources: `"SM": {"Type": "AWS::StepFunctions::StateMachine", "Properties": {
				"StateMachineName": "ref-sm", "RoleArn": "arn:aws:iam::123456789012:role/r"}}`,
			refOf: "SM",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.Contains(t, ref, ":stateMachine:ref-sm", "got %q", ref)
			},
		},
		{
			// "Ref returns the ARN of the created activity."
			name:      "step functions activity is the ARN",
			resources: `"Act": {"Type": "AWS::StepFunctions::Activity", "Properties": {"Name": "ref-act"}}`,
			refOf:     "Act",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.Contains(t, ref, ":activity:ref-act", "got %q", ref)
			},
		},
		{
			// "the function returns the ARN of the GraphQL API, such as
			// arn:aws:appsync:us-east-1:123456789012:apis/graphqlapiid."
			name: "appsync graphql api is the ARN",
			resources: `"Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": {
				"Name": "refapi", "AuthenticationType": "API_KEY"}}`,
			refOf: "Api",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.True(t, strings.HasPrefix(ref, "arn:aws:appsync:"), "got %q", ref)
				assert.Contains(t, ref, ":apis/", "AWS's form is …:123456789012:apis/graphqlapiid")
			},
		},
		{
			// "the function returns the ARN of the Data Source, such as
			// arn:aws:appsync:…:apis/graphqlapiid/datasources/datasourcename."
			name: "appsync data source is the ARN",
			resources: `"Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": {
				"Name": "refapi2", "AuthenticationType": "API_KEY"}},
			"DS": {"Type": "AWS::AppSync::DataSource", "Properties": {
				"ApiId": {"Fn::GetAtt": ["Api", "ApiId"]}, "Name": "refds", "Type": "NONE"}}`,
			refOf: "DS",
			check: func(t *testing.T, ref string) {
				t.Helper()
				assert.Contains(t, ref, "/datasources/refds", "got %q", ref)
			},
		},
		{
			// "Ref returns the ID of the key and ID of the usage plan combined with a ':',
			// such as 123abcdef:abc123."
			name: "api gateway usage plan key is keyId:usagePlanId",
			resources: `"Key": {"Type": "AWS::ApiGateway::ApiKey", "Properties": {"Name": "refkey"}},
			"Plan": {"Type": "AWS::ApiGateway::UsagePlan", "Properties": {"UsagePlanName": "refplan"}},
			"PlanKey": {"Type": "AWS::ApiGateway::UsagePlanKey", "Properties": {
				"KeyId": {"Ref": "Key"}, "UsagePlanId": {"Ref": "Plan"}}}`,
			refOf: "PlanKey",
			check: func(t *testing.T, ref string) {
				t.Helper()
				parts := strings.Split(ref, ":")
				require.Len(t, parts, 2, "got %q", ref)
				assert.NotEmpty(t, parts[0], "the key ID")
				assert.NotEmpty(t, parts[1], "the usage plan ID")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newRefDeployer(t)
			tmpl := `{"Resources": {` + tt.resources + `},
				"Outputs": {"TheRef": {"Value": {"Ref": "` + tt.refOf + `"}}}}`
			result, err := d.Deploy(context.Background(), tmpl, "ref-"+tt.refOf+"-stack", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, result)
			tt.check(t, result.Outputs["TheRef"])
		})
	}
}

// TestCFNRef_SQSQueueURLIsUsableAsAQueueURL is the point of resolving a queue Ref to a URL: the
// value a template hands a consumer is the one SQS itself accepts.
//
// A shape assertion alone would not show that. The URL substrate builds points at its own
// endpoint rather than at amazonaws.com, so the only assertion worth making is that its own SQS
// operations take it.
func TestCFNRef_SQSQueueURLIsUsableAsAQueueURL(t *testing.T) {
	d := newRefDeployer(t)
	tmpl := `{
		"Resources": {"Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "ref-usable"}}},
		"Outputs": {"QueueURL": {"Value": {"Ref": "Q"}}}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ref-sqs-usable-stack", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	queueURL := result.Outputs["QueueURL"]
	require.NotEmpty(t, queueURL)
	resp, dispatchErr := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service:   "sqs",
		Operation: "GetQueueAttributes",
		Params: map[string]string{
			"Action":          "GetQueueAttributes",
			"QueueUrl":        queueURL,
			"AttributeName.1": "QueueArn",
		},
		Headers: map[string]string{},
	}, "probe")
	require.NoError(t, dispatchErr, "the resolved Ref is rejected as a QueueUrl")
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), ":ref-usable")
}

// TestCFNRef_PhysicalIDRemainsTheDefault pins the majority case. For most types the documented
// Ref value *is* the identifier substrate stores, so a per-type table is only correct if it
// leaves those alone — and a table keyed on a type is exactly the kind of thing that grows an arm
// it should not have.
func TestCFNRef_PhysicalIDRemainsTheDefault(t *testing.T) {
	d := newRefDeployer(t)
	tmpl := `{
		"Resources": {
			"Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "ref-default-bucket"}},
			"Role": {"Type": "AWS::IAM::Role", "Properties": {
				"RoleName": "ref-default-role",
				"AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}}},
			"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.0.0.0/16"}}
		},
		"Outputs": {
			"BucketRef": {"Value": {"Ref": "Bucket"}},
			"RoleRef": {"Value": {"Ref": "Role"}},
			"VpcRef": {"Value": {"Ref": "Vpc"}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ref-default-stack", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	physical := make(map[string]string, len(result.Resources))
	for _, r := range result.Resources {
		physical[r.LogicalID] = r.PhysicalID
	}
	assert.Equal(t, "ref-default-bucket", result.Outputs["BucketRef"])
	assert.Equal(t, "ref-default-role", result.Outputs["RoleRef"])
	assert.True(t, strings.HasPrefix(result.Outputs["VpcRef"], "vpc-"),
		"a VPC Refs to its ID, got %q", result.Outputs["VpcRef"])
	for logicalID, output := range map[string]string{
		"Bucket": result.Outputs["BucketRef"],
		"Role":   result.Outputs["RoleRef"],
		"Vpc":    result.Outputs["VpcRef"],
	} {
		assert.Equal(t, physical[logicalID], output,
			"%s still Refs to its physical ID", logicalID)
	}
}
