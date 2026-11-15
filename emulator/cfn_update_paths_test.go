package emulator_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// v129Dispatch sends one request through the deployer's registry and returns the routing error.
func v129Dispatch(t *testing.T, d *emulator.StackDeployer, req *emulator.AWSRequest) error {
	t.Helper()
	if req.Headers == nil {
		req.Headers = map[string]string{}
	}
	if req.Params == nil {
		req.Params = map[string]string{}
	}
	_, err := d.DispatchForTest(context.Background(), req, "probe")
	return err
}

// v129JSONBody marshals a request body.
func v129JSONBody(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

// TestCFN_UpdaterRefusalFailsTheResource pins each in-place updater's refusal path: the resource is
// deleted out of band, so the update call the updater makes is refused, the resource fails with the
// service's code, and the update rolls back.
func TestCFN_UpdaterRefusalFailsTheResource(t *testing.T) {
	const role = `"Role":"arn:aws:iam::123456789012:role/lambda-exec","Runtime":"python3.12","Handler":"index.handler"`
	fn := func(extra string) string {
		return `{"Resources":{"R":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"v129-fn",` + role +
			`,"Code":{"ZipFile":"x"}` + extra + `}}}}`
	}
	table := func(extra string) string {
		return `{"Resources":{"R":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"v129-table",
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]` + extra + `}}}}`
	}
	ecs := func(count string) string {
		return `{"Resources":{
			"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-c"}},
			"Task":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-t",
				"ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"R":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-s","Cluster":{"Ref":"Cluster"},
				"TaskDefinition":{"Ref":"Task"},"DesiredCount":` + count + `}}}}`
	}
	cases := []struct {
		name, before, after, want string
		remove                    func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource)
	}{
		{
			name: "lambda configuration", before: fn(""), after: fn(`,"Timeout":9`), want: "R (AWS::Lambda::Function)",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "lambda", Operation: "DELETE",
					Path: "/2015-03-31/functions/" + dr.PhysicalID}))
			},
		},
		{
			name: "lambda code", before: fn(""), after: strings.Replace(fn(""), `"ZipFile":"x"`, `"ZipFile":"y"`, 1),
			want: "R (AWS::Lambda::Function)",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "lambda", Operation: "DELETE",
					Path: "/2015-03-31/functions/" + dr.PhysicalID}))
			},
		},
		{
			name: "dynamodb billing mode", before: table(`,"BillingMode":"PAY_PER_REQUEST"`),
			after: table(`,"BillingMode":"PROVISIONED","ProvisionedThroughput":{"ReadCapacityUnits":1,"WriteCapacityUnits":1}`),
			want:  "ResourceNotFoundException",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "dynamodb", Operation: "DeleteTable",
					Body: v129JSONBody(t, map[string]any{"TableName": dr.PhysicalID})}))
			},
		},
		{
			name: "dynamodb ttl", before: table(`,"BillingMode":"PAY_PER_REQUEST"`),
			after: table(`,"BillingMode":"PAY_PER_REQUEST","TimeToLiveSpecification":{"AttributeName":"exp","Enabled":true}`),
			want:  "ResourceNotFoundException",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "dynamodb", Operation: "DeleteTable",
					Body: v129JSONBody(t, map[string]any{"TableName": dr.PhysicalID})}))
			},
		},
		{
			name:   "sqs attributes",
			before: `{"Resources":{"R":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"v129-q"}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"v129-q","DelaySeconds":3}}}}`,
			want:   "QueueDoesNotExist",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "sqs", Operation: "DeleteQueue",
					Params: map[string]string{"Action": "DeleteQueue",
						"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/" + dr.PhysicalID}}))
			},
		},
		{
			name:   "sns display name",
			before: `{"Resources":{"R":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"v129-t","DisplayName":"a"}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"v129-t"}}}}`,
			want:   "NotFound",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "sns", Operation: "DeleteTopic",
					Params: map[string]string{"Action": "DeleteTopic", "TopicArn": dr.PhysicalID}}))
			},
		},
		{
			name:   "s3 versioning",
			before: `{"Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"v129-bucket"}}}}`,
			after: `{"Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"v129-bucket",
				"VersioningConfiguration":{"Status":"Enabled"}}}}}`,
			want: "NoSuchBucket",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "s3", Operation: "DELETE",
					Path: "/" + dr.PhysicalID}))
			},
		},
		{
			name:   "kms description",
			before: `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"Description":"a"}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"Description":"b"}}}}`,
			want:   "KMSInvalidStateException",
			remove: v129ScheduleKeyDeletion,
		},
		{
			name:   "kms rotation",
			before: `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"EnableKeyRotation":true}}}}`,
			want:   "KMSInvalidStateException",
			remove: v129ScheduleKeyDeletion,
		},
		{
			name:   "kms enabled",
			before: `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"Enabled":false}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"Enabled":true}}}}`,
			want:   "KMSInvalidStateException",
			remove: v129ScheduleKeyDeletion,
		},
		{
			name:   "kms disable",
			before: `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"Enabled":false}}}}`,
			want:   "KMSInvalidStateException",
			remove: v129ScheduleKeyDeletion,
		},
		{
			name: "security group gone",
			before: `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x",
				"SecurityGroupIngress":[{"IpProtocol":"tcp","FromPort":22,"ToPort":22,"CidrIp":"10.0.0.0/8"}]}}}}`,
			after: `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x"}}}}`,
			want:  "InvalidGroup.NotFound",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "ec2", Operation: "DeleteSecurityGroup",
					Params: map[string]string{"Action": "DeleteSecurityGroup", "GroupId": dr.PhysicalID}}))
			},
		},
		{
			name:   "security group rule refused",
			before: `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x"}}}}`,
			after: `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"x",
				"SecurityGroupIngress":[{"IpProtocol":"tcp","FromPort":22,"ToPort":22,"CidrIp":"not-a-cidr"}]}}}}`,
			want: "InvalidParameterValue",
		},
		{
			name: "ecs desired count", before: ecs("1"), after: ecs("2"), want: "R (AWS::ECS::Service)",
			remove: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				require.NoError(t, v129Dispatch(t, d, &emulator.AWSRequest{Service: "ecs", Operation: "DeleteService",
					Body: v129JSONBody(t, map[string]any{"cluster": "v129-c", "service": dr.PhysicalID, "force": true})}))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.before, "refused-update", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, created)
			if tc.remove != nil {
				tc.remove(t, d, resourceByLogicalID(t, created, "R"))
			}
			result, err := d.UpdateStack(ctx, tc.after, "refused-update", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(result.Status, "UPDATE_ROLLBACK"), result.Status)
			assert.Contains(t, result.StatusReason, tc.want)
		})
	}
}

// v129ScheduleKeyDeletion puts a key into PendingDeletion, a state every KMS update call but
// PutKeyPolicy refuses.
func v129ScheduleKeyDeletion(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
	t.Helper()
	cfnStubDispatch(t, d, "kms", "ScheduleKeyDeletion", map[string]any{"KeyId": dr.PhysicalID, "PendingWindowInDays": 7})
}

// TestCFN_UpdaterNoOpsAndResets covers the changes an updater makes no call for — a property its
// create does not forward either — and the per-type resets a removed property returns to.
func TestCFN_UpdaterNoOpsAndResets(t *testing.T) {
	tags := func(v string) string { return `"Tags":[{"Key":"k","Value":"` + v + `"}]` }
	cases := []struct {
		name, before, after string
		check               func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource)
	}{
		{"sqs tags only", `{"Resources":{"R":{"Type":"AWS::SQS::Queue","Properties":{` + tags("a") + `}}}}`,
			`{"Resources":{"R":{"Type":"AWS::SQS::Queue","Properties":{` + tags("b") + `}}}}`, nil},
		{"sns tags only", `{"Resources":{"R":{"Type":"AWS::SNS::Topic","Properties":{` + tags("a") + `}}}}`,
			`{"Resources":{"R":{"Type":"AWS::SNS::Topic","Properties":{` + tags("b") + `}}}}`, nil},
		{"security group tags only", `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup","Properties":{
			"GroupDescription":"x",` + tags("a") + `}}}}`, `{"Resources":{"R":{"Type":"AWS::EC2::SecurityGroup",
			"Properties":{"GroupDescription":"x",` + tags("b") + `}}}}`, nil},
		{"lambda environment removed", `{"Resources":{"R":{"Type":"AWS::Lambda::Function","Properties":{
			"Role":"arn:aws:iam::123456789012:role/x","Code":{"ZipFile":"x"},"Environment":{"Variables":{"A":"1"}}}}}}`,
			`{"Resources":{"R":{"Type":"AWS::Lambda::Function","Properties":{
			"Role":"arn:aws:iam::123456789012:role/x","Code":{"ZipFile":"x"}}}}}`,
			func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				var fn struct {
					Configuration struct {
						Environment struct{ Variables map[string]string }
					}
				}
				v129REST(t, d, "lambda", "GET", "/2015-03-31/functions/"+dr.PhysicalID, nil, &fn)
				assert.Empty(t, fn.Configuration.Environment.Variables)
			}},
		{"kms key policy set and reset", `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{
			"KeyPolicy":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
			"Action":"kms:*","Resource":"*"}]}}}}}`, `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{}}}}`,
			func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				pol := cfnStubDispatch(t, d, "kms", "GetKeyPolicy", map[string]any{"KeyId": dr.PhysicalID, "PolicyName": "default"})
				assert.NotContains(t, pol["Policy"], `"Principal":{"AWS":"*"}`, "the default policy is back")
			}},
		{"ecs task definition", `{"Resources":{
			"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-c2"}},
			"T1":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-a","ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"T2":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-b","ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"R":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-s2","Cluster":{"Ref":"Cluster"},"TaskDefinition":{"Ref":"T1"}}}}}`,
			`{"Resources":{
			"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-c2"}},
			"T1":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-a","ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"T2":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-b","ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"R":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-s2","Cluster":{"Ref":"Cluster"},"TaskDefinition":{"Ref":"T2"},
			` + tags("x") + `}}}}`,
			func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				out := cfnStubDispatch(t, d, "ecs", "DescribeServices",
					map[string]any{"cluster": "v129-c2", "services": []string{dr.PhysicalID}})
				services, _ := out["services"].([]any)
				require.Len(t, services, 1)
				svc, _ := services[0].(map[string]any)
				assert.Contains(t, svc["taskDefinition"], "v129-b")
			}},
		{"s3 cors with max age and bucket key as a string", `{"Resources":{"R":{"Type":"AWS::S3::Bucket",
			"Properties":{"VersioningConfiguration":{"Status":"Suspended"}}}}}`,
			`{"Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{
			"CorsConfiguration":{"CorsRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"],"MaxAge":600,"Id":"r1"}]},
			"BucketEncryption":{"ServerSideEncryptionConfiguration":[{"ServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},
				"BucketKeyEnabled":"true"}]}}}}}`,
			func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{Service: "s3", Operation: "GET",
					Path: "/" + dr.PhysicalID, Params: map[string]string{"cors": ""}, Headers: map[string]string{}}, "probe")
				require.NotNil(t, resp)
				assert.Contains(t, string(resp.Body), "<MaxAgeSeconds>600</MaxAgeSeconds>")
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			created, updated := v129Update(t, d, "noop", tc.before, tc.after)
			assert.Equal(t, resourceByLogicalID(t, created, "R").PhysicalID, resourceByLogicalID(t, updated, "R").PhysicalID)
			if tc.check != nil {
				tc.check(t, d, resourceByLogicalID(t, updated, "R"))
			}
		})
	}
}

// TestCFN_UnmodelledPropertyRefusals pins the remaining cfnUpdateUnmodelledProperties entries: each
// refuses with the property and the reason, and the update rolls back.
func TestCFN_UnmodelledPropertyRefusals(t *testing.T) {
	table := func(extra string) string {
		return `{"Resources":{"R":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST",
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]` + extra + `}}}}`
	}
	svc := func(lt string) string {
		return `{"Resources":{
			"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-c3"}},
			"Task":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-t3",
				"ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
			"R":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-s3","Cluster":{"Ref":"Cluster"},
				"TaskDefinition":{"Ref":"Task"},"LaunchType":"` + lt + `"}}}}`
	}
	cases := []struct{ name, before, after, want string }{
		{"stream specification", table(""), table(`,"StreamSpecification":{"StreamViewType":"NEW_IMAGE"}`),
			"StreamSpecification (substrate's UpdateTable takes no StreamSpecification)"},
		{"ecs launch type", svc("FARGATE"), svc("EC2"), "LaunchType (UpdateService takes no launch type)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.before, "unmodelled", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, created)
			result, err := d.UpdateStack(ctx, tc.after, "unmodelled", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
			assert.Contains(t, result.StatusReason, tc.want)
		})
	}
}

// TestCFN_CreateConfigurationRefusalFailsTheResource pins that a configuration call the create now
// makes — a bucket's encryption, a key's rotation — fails the resource when it is refused.
func TestCFN_CreateConfigurationRefusalFailsTheResource(t *testing.T) {
	cases := []struct{ name, tmpl, want string }{
		{"bucket encryption", `{"Resources":{"R":{"Type":"AWS::S3::Bucket","Properties":{"BucketEncryption":{
			"ServerSideEncryptionConfiguration":[{"ServerSideEncryptionByDefault":{"SSEAlgorithm":"rot13"}}]}}}}}`,
			"InvalidArgument"},
		{"key rotation on an HMAC key", `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{
			"KeySpec":"HMAC_256","KeyUsage":"GENERATE_VERIFY_MAC","EnableKeyRotation":true}}}}`, "UnsupportedOperation"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			result, err := d.Deploy(context.Background(), tc.tmpl, "create-config", nil)
			require.NoError(t, err)
			assert.Equal(t, "ROLLBACK_COMPLETE", result.Status)
			assert.Contains(t, result.StatusReason, tc.want)
		})
	}
}

// TestCFN_RemovedResourceDeleteFailureCompletesTheUpdate pins the troubleshooting guide's "Resource
// removed from stack but not deleted": a removed resource whose delete fails leaves the update
// UPDATE_COMPLETE with a reason naming it. An unrecognized DeletionPolicy is the failure, since it is
// read only when the resource is deleted.
func TestCFN_RemovedResourceDeleteFailureCompletesTheUpdate(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	_, err := d.Deploy(ctx, `{"Resources":{
		"Keep":{"Type":"AWS::SQS::Queue"},
		"Gone":{"Type":"AWS::SQS::Queue","DeletionPolicy":"Retian"}}}`, "removed-fails", nil)
	require.NoError(t, err)
	result, err := d.UpdateStack(ctx, `{"Resources":{"Keep":{"Type":"AWS::SQS::Queue"}}}`, "removed-fails", nil,
		emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", result.Status)
	assert.Contains(t, result.StatusReason, "the update could not delete the resources removed from the template: Gone")
}

// TestCFN_SecurityGroupRuleSources covers each rule source the declaration diff keys on — an IPv6
// range, a referenced group, and a prefix list — added and then removed.
func TestCFN_SecurityGroupRuleSources(t *testing.T) {
	tmpl := func(rules string) string {
		return `{"Resources":{
			"Peer":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"peer"}},
			"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web",
				"SecurityGroupIngress":[` + rules + `]}}}}`
	}
	const v6 = `{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIpv6":"2001:db8::/32"}`
	const peer = `{"IpProtocol":"tcp","FromPort":5432,"ToPort":5432,"SourceSecurityGroupId":{"Fn::GetAtt":["Peer","GroupId"]},
		"Description":"from peer"}`
	d, _, _, _ := newSweepDeployer(t)
	created, _ := v129Update(t, d, "sources", tmpl(""), tmpl(v6+","+peer))
	sg := resourceByLogicalID(t, created, "SG").PhysicalID
	body, err := v129Query(t, d, "ec2", map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": sg})
	require.NoError(t, err)
	assert.Contains(t, body, "2001:db8::/32")
	assert.Contains(t, body, resourceByLogicalID(t, created, "Peer").PhysicalID)

	updated, err := d.UpdateStack(context.Background(), tmpl(""), "sources", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	ingress, _ := v129SGRules(t, d, sg)
	assert.Empty(t, ingress)
	body, err = v129Query(t, d, "ec2", map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": sg})
	require.NoError(t, err)
	assert.NotContains(t, body, "2001:db8::/32")
	assert.NotContains(t, body, "from peer")
}

// TestCFN_ResolveStructuredEdges pins cfnResolveStructured's edges: a top-level AWS::NoValue is
// absent, a nested Fn::If chooses through to an object, a NoValue member is dropped, and a
// list-valued intrinsic in a list splices while a scalar one is one element.
func TestCFN_ResolveStructuredEdges(t *testing.T) {
	conds := map[string]bool{"On": true, "Off": false}
	cases := []struct {
		name string
		in   string
		want any
	}{
		{"no value", `{"Ref":"AWS::NoValue"}`, nil},
		{"if choosing no value", `{"Fn::If":["Off",{"a":"b"},{"Ref":"AWS::NoValue"}]}`, nil},
		{"nested if", `{"Fn::If":["On",{"Fn::If":["Off","x",{"k":"v"}]},"y"]}`, map[string]any{"k": "v"}},
		{"member no value", `{"keep":"1","drop":{"Fn::If":["Off","2",{"Ref":"AWS::NoValue"}]}}`,
			map[string]any{"keep": "1"}},
		{"list splices a split", `["a",{"Fn::Split":[",","b,c"]},{"Fn::Join":["-",["d","e"]]}]`,
			[]any{"a", "b", "c", "d-e"}},
		{"list drops no value", `[{"Fn::If":["Off","a",{"Ref":"AWS::NoValue"}]},"b"]`, []any{"b"}},
		{"scalar", `"plain"`, "plain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var in any
			require.NoError(t, json.Unmarshal([]byte(tc.in), &in))
			assert.Equal(t, tc.want, emulator.CFNResolveStructuredForTest(in, conds))
		})
	}
}

// TestKMSUpdateKeyDescriptionRefusals pins UpdateKeyDescription's request refusals: a body that is
// not JSON, and a KeyId ARN that names no key.
func TestKMSUpdateKeyDescriptionRefusals(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	err := v129Dispatch(t, d, &emulator.AWSRequest{Service: "kms", Operation: "UpdateKeyDescription", Body: []byte("{")})
	require.Error(t, err)
	err = v129Dispatch(t, d, &emulator.AWSRequest{Service: "kms", Operation: "UpdateKeyDescription",
		Body: v129JSONBody(t, map[string]any{"KeyId": "arn:aws:kms:us-east-1:123456789012:alias", "Description": "x"})})
	require.Error(t, err)
}

// TestCFN_OutputValidationSkipsAConditionedOutResource pins that an Output reading an unmodelled
// attribute of a resource that does not deploy is not refused: the resource is absent, so the read
// names nothing substrate would have to answer.
func TestCFN_OutputValidationSkipsAConditionedOutResource(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	_, err := d.Deploy(context.Background(), `{
		"Conditions":{"Never":{"Fn::Equals":["a","b"]}},
		"Resources":{"Q":{"Type":"AWS::SQS::Queue"},
			"Server":{"Type":"AWS::Transfer::Server","Condition":"Never","Properties":{"Protocols":["AS2"]}}},
		"Outputs":{"Egress":{"Value":{"Fn::If":["Never",{"Fn::GetAtt":["Server","As2ServiceManagedEgressIpAddresses"]},"none"]}}}}`,
		"cond-resource", nil)
	assert.NotErrorIs(t, err, emulator.ErrCFNUnsupportedAttribute)
}
