package emulator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cfnFailingBucket is a resource whose create S3 refuses, so a template carrying it fails and an
// update carrying it rolls back.
const cfnFailingBucket = `"Bad":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}`

// TestCFN_SecurityGroupUpdateLeavesStandaloneRules pins that an inline-rule update diffs the two
// declarations rather than the group's live rules: a rule an AWS::EC2::SecurityGroupIngress
// resource added is in neither declaration and survives.
func TestCFN_SecurityGroupUpdateLeavesStandaloneRules(t *testing.T) {
	tmpl := func(port string) string {
		return `{"Resources":{
			"Net":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"10.0.0.0/16"}},
			"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web","VpcId":{"Ref":"Net"},
				"SecurityGroupIngress":[{"IpProtocol":"tcp","FromPort":` + port + `,"ToPort":` + port + `,"CidrIp":"10.0.0.0/8"}]}},
			"Extra":{"Type":"AWS::EC2::SecurityGroupIngress","Properties":{
				"GroupId":{"Ref":"SG"},"IpProtocol":"tcp","FromPort":5432,"ToPort":5432,
				"CidrIp":"172.16.0.0/12"}}}}`
	}
	d, _, _, _ := newSweepDeployer(t)
	created, _ := v129Update(t, d, "standalone", tmpl("22"), tmpl("443"))
	ingress, _ := v129SGRules(t, d, resourceByLogicalID(t, created, "SG").PhysicalID)
	assert.Equal(t, []string{"tcp 443-443 10.0.0.0/8", "tcp 5432-5432 172.16.0.0/12"}, ingress)
}

// TestCFN_StructuredPropertiesResolveFnIf pins that a security group's rule list and a function's
// Environment are resolved structurally: an Fn::If choosing a rule is that rule, one choosing
// AWS::NoValue is no rule, and an Fn::If choosing a variables map is that map.
func TestCFN_StructuredPropertiesResolveFnIf(t *testing.T) {
	const conds = `"Parameters":{"Stage":{"Type":"String","Default":"dev"}},
		"Conditions":{"IsProd":{"Fn::Equals":[{"Ref":"Stage"},"prod"]}},`
	tmpl := conds + `"Resources":{
		"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web",
			"SecurityGroupIngress":[
				{"IpProtocol":"tcp","FromPort":22,"ToPort":22,"CidrIp":"10.0.0.0/8"},
				{"Fn::If":["IsProd",{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"0.0.0.0/0"},
					{"Ref":"AWS::NoValue"}]}]}},
		"Fn":{"Type":"AWS::Lambda::Function","Properties":{
			"Role":"arn:aws:iam::123456789012:role/lambda-exec","Runtime":"python3.12","Handler":"index.handler",
			"Code":{"ZipFile":"def handler(e, c):\n    return 1\n"},
			"Environment":{"Fn::If":["IsProd",{"Variables":{"STAGE":"prod"}},{"Variables":{"STAGE":"dev"}}]}}}}}`
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	created, err := d.Deploy(ctx, "{"+tmpl, "fnif", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	sg := resourceByLogicalID(t, created, "SG").PhysicalID
	ingress, _ := v129SGRules(t, d, sg)
	assert.Equal(t, []string{"tcp 22-22 10.0.0.0/8"}, ingress, "the NoValue element is no rule")

	updated, err := d.UpdateStack(ctx, "{"+tmpl, "fnif", map[string]string{"Stage": "prod"}, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	ingress, _ = v129SGRules(t, d, sg)
	assert.Equal(t, []string{"tcp 22-22 10.0.0.0/8", "tcp 443-443 0.0.0.0/0"}, ingress, "the chosen rule is applied")
	var fn struct {
		Configuration struct {
			Environment struct{ Variables map[string]string }
		}
	}
	v129REST(t, d, "lambda", "GET", "/2015-03-31/functions/"+resourceByLogicalID(t, updated, "Fn").PhysicalID, nil, &fn)
	assert.Equal(t, map[string]string{"STAGE": "prod"}, fn.Configuration.Environment.Variables)
}

// TestCFN_RollbackResetsWhatTheFailedUpdateAdded pins that a rollback applies the difference
// between the failed update's declaration and the previous one, in both directions: a property
// the failed update newly declared returns to the value an undeclared one has.
func TestCFN_RollbackResetsWhatTheFailedUpdateAdded(t *testing.T) {
	const before = `{"Resources":{
		"Queue":{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":1}},
		"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web"}},
		"Data":{"Type":"AWS::S3::Bucket","Properties":{"Tags":[{"Key":"a","Value":"b"}]}}}}`
	const failing = `{"Resources":{
		"Queue":{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":1,"VisibilityTimeout":60}},
		"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"web",
			"SecurityGroupEgress":[{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"0.0.0.0/0"}]}},
		"Data":{"Type":"AWS::S3::Bucket","Properties":{"Tags":[{"Key":"a","Value":"b"}],
			"BucketEncryption":{"ServerSideEncryptionConfiguration":[
				{"ServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms"}}]}}},
		` + cfnFailingBucket + `}}`
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	created, err := d.Deploy(ctx, before, "rollback-reset", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	result, err := d.UpdateStack(ctx, failing, "rollback-reset", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)

	queue := resourceByLogicalID(t, result, "Queue").PhysicalID
	body, err := v129Query(t, d, "sqs", map[string]string{"Action": "GetQueueAttributes",
		"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/" + queue, "AttributeName.1": "All"})
	require.NoError(t, err)
	assert.Contains(t, body, "<Name>VisibilityTimeout</Name><Value>30</Value>")

	_, egress := v129SGRules(t, d, resourceByLogicalID(t, result, "SG").PhysicalID)
	assert.Equal(t, []string{"-1 0-0 0.0.0.0/0"}, egress, "the default egress rule is back, the declared one gone")

	resp, _ := d.DispatchForTest(ctx, &emulator.AWSRequest{Service: "s3", Operation: "GET",
		Path: "/" + resourceByLogicalID(t, result, "Data").PhysicalID, Params: map[string]string{"encryption": ""},
		Headers: map[string]string{}}, "probe")
	require.NotNil(t, resp)
	assert.NotContains(t, string(resp.Body), "aws:kms", "the failed update's encryption was deleted")
}

// TestCFN_KMSRotationOnADisabledKey pins that a rotation change on a key that is disabled, and stays
// so, is applied — EnableKeyRotation refuses a disabled key, so the key is enabled for the call and
// disabled again — and that a rollback of such a change completes.
func TestCFN_KMSRotationOnADisabledKey(t *testing.T) {
	key := func(rotation string) string {
		return `{"Resources":{"Key":{"Type":"AWS::KMS::Key","Properties":{"Enabled":false` + rotation + `}}` +
			`%s}}`
	}
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	created, updated := v129Update(t, d, "kms-disabled",
		strings.Replace(key(""), "%s", "", 1), strings.Replace(key(`,"EnableKeyRotation":true`), "%s", "", 1))
	keyID := resourceByLogicalID(t, created, "Key").PhysicalID
	require.Equal(t, keyID, resourceByLogicalID(t, updated, "Key").PhysicalID)
	desc := cfnStubDispatch(t, d, "kms", "DescribeKey", map[string]any{"KeyId": keyID})
	meta, _ := desc["KeyMetadata"].(map[string]any)
	assert.Equal(t, "Disabled", meta["KeyState"], "the key stays disabled")

	// GetKeyRotationStatus answers for a disabled key; the rotation took.
	rot := cfnStubDispatch(t, d, "kms", "GetKeyRotationStatus", map[string]any{"KeyId": keyID})
	assert.Equal(t, true, rot["KeyRotationEnabled"])

	result, err := d.UpdateStack(ctx, strings.Replace(key(""), "%s", ","+cfnFailingBucket, 1), "kms-disabled", nil,
		emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)
	rot = cfnStubDispatch(t, d, "kms", "GetKeyRotationStatus", map[string]any{"KeyId": keyID})
	assert.Equal(t, true, rot["KeyRotationEnabled"], "the rollback restored the rotation the failed update turned off")
}

// TestCFN_ConditionedOutputsAreNotResolved pins that an Output under a false Condition is neither
// resolved nor validated: a refused attribute or an unresolvable import in it is not an error, and
// the stack publishes no value for it.
func TestCFN_ConditionedOutputsAreNotResolved(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{
		"Conditions":{"Never":{"Fn::Equals":["a","b"]}},
		"Resources":{"Server":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["AS2"]}}},
		"Outputs":{
			"Egress":{"Condition":"Never","Value":{"Fn::GetAtt":["Server","As2ServiceManagedEgressIpAddresses"]}},
			"Imported":{"Condition":"Never","Value":{"Fn::ImportValue":"no-such-export"}},
			"Id":{"Value":{"Ref":"Server"}}}}`, "cond-outputs", nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", result.Status, result.StatusReason)
	assert.NotContains(t, result.Outputs, "Egress")
	assert.NotContains(t, result.Outputs, "Imported")
	assert.NotEmpty(t, result.Outputs["Id"])
}

// TestCFN_ReplacementMintsOnlyForAGeneratedName pins which replacements draw a fresh name from the
// request's mint: a resource substrate names, and not one the template names or one named by its
// logical ID — so their replacements leave every later identifier where it was.
func TestCFN_ReplacementMintsOnlyForAGeneratedName(t *testing.T) {
	cases := []struct {
		name, typ string
		props     map[string]interface{}
		want      bool
	}{
		{"generated security group", "AWS::EC2::SecurityGroup", map[string]interface{}{"GroupDescription": "x"}, true},
		{"named security group", "AWS::EC2::SecurityGroup", map[string]interface{}{"GroupName": "web"}, false},
		{"named queue", "AWS::SQS::Queue", map[string]interface{}{"QueueName": "q"}, false},
		{"generated queue", "AWS::SQS::Queue", map[string]interface{}{}, true},
		{"a type named by its logical ID", "AWS::EC2::VPC", map[string]interface{}{}, false},
		{"a type with no name property", "AWS::ApiGateway::Method", map[string]interface{}{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, emulator.CFNUsesGeneratedNameForTest(tc.typ, tc.props))
		})
	}
}

// TestCFN_TableTTLRemovalDisablesOnlyAnEnabledTTL pins that removing TimeToLiveSpecification
// disables TTL only when the previous declaration enabled it.
func TestCFN_TableTTLRemovalDisablesOnlyAnEnabledTTL(t *testing.T) {
	table := func(ttl string) string {
		return `{"Resources":{"T":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST",
			"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
			"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]` + ttl + `}}}}`
	}
	cases := []struct {
		name, ttl  string
		wantCall   bool
		wantStatus string
	}{
		{"enabled", `,"TimeToLiveSpecification":{"AttributeName":"exp","Enabled":true}`, true, "DISABLED"},
		{"already disabled", `,"TimeToLiveSpecification":{"AttributeName":"exp","Enabled":false}`, false, "DISABLED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			_, created := v129Update(t, d, "ttl", table(tc.ttl), table(tc.ttl))
			before := len(sweepOperations(t, store, "ttl"))
			updated, err := d.UpdateStack(context.Background(), table(""), "ttl", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
			ops := sweepOperations(t, store, "ttl")[before:]
			assert.Equal(t, tc.wantCall, len(ops) > 0 && ops[0] == "UpdateTimeToLive", "%v", ops)
			out := cfnStubDispatch(t, d, "dynamodb", "DescribeTimeToLive",
				map[string]any{"TableName": resourceByLogicalID(t, created, "T").PhysicalID})
			desc, _ := out["TimeToLiveDescription"].(map[string]any)
			assert.Equal(t, tc.wantStatus, desc["TimeToLiveStatus"])
		})
	}
}
