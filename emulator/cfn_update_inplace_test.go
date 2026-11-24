package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// v129Query sends one query-protocol operation through the deployer's registry and returns the
// response body and the routing error, so a test can assert on either.
func v129Query(t *testing.T, d *emulator.StackDeployer, service string, params map[string]string) (string, error) {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: service, Operation: params["Action"], Params: params, Headers: map[string]string{},
	}, "probe")
	if resp == nil {
		return "", err
	}
	return string(resp.Body), err
}

// v129REST sends one REST operation and decodes a JSON answer into out when it is non-nil.
func v129REST(t *testing.T, d *emulator.StackDeployer, service, method, path string, params map[string]string, out any) int {
	t.Helper()
	if params == nil {
		params = map[string]string{}
	}
	resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: service, Operation: method, Path: path, Params: params, Headers: map[string]string{},
	}, "probe")
	require.NotNil(t, resp)
	if out != nil {
		require.NoError(t, json.Unmarshal(resp.Body, out), string(resp.Body))
	}
	return resp.StatusCode
}

// v129Update deploys before, updates to after, and requires the update to complete.
func v129Update(t *testing.T, d *emulator.StackDeployer, stack, before, after string) (created, updated *emulator.DeployResult) {
	t.Helper()
	ctx := context.Background()
	created, err := d.Deploy(ctx, before, stack, nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	updated, err = d.UpdateStack(ctx, after, stack, nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	return created, updated
}

// v129SGRules reads a group's rules through DescribeSecurityGroups, each as "proto from-to cidr".
func v129SGRules(t *testing.T, d *emulator.StackDeployer, groupID string) (ingress, egress []string) {
	t.Helper()
	var doc struct {
		Groups []struct {
			In  []v129Perm `xml:"ipPermissions>item"`
			Out []v129Perm `xml:"ipPermissionsEgress>item"`
		} `xml:"securityGroupInfo>item"`
	}
	cfnDescribeEC2(t, d, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": groupID}, &doc)
	require.Len(t, doc.Groups, 1)
	return v129Flatten(doc.Groups[0].In), v129Flatten(doc.Groups[0].Out)
}

type v129Perm struct {
	Proto string   `xml:"ipProtocol"`
	From  string   `xml:"fromPort"`
	To    string   `xml:"toPort"`
	CIDRs []string `xml:"ipRanges>item>cidrIp"`
}

func v129Flatten(perms []v129Perm) []string {
	var out []string
	for _, p := range perms {
		for _, c := range p.CIDRs {
			out = append(out, p.Proto+" "+p.From+"-"+p.To+" "+c)
		}
	}
	sort.Strings(out)
	return out
}

// TestCFN_UpdateInPlaceKeepsThePhysicalID is #1545's acceptance criterion for each priority type:
// an UpdateStack changing an updatable property keeps the physical ID, makes the change through the
// service's update operation, and the service reads it back.
func TestCFN_UpdateInPlaceKeepsThePhysicalID(t *testing.T) {
	const role = `"Role": "arn:aws:iam::123456789012:role/lambda-exec"`
	cases := []struct {
		name, logical, before, after string
		check                        func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource)
	}{
		{
			name: "lambda configuration and environment", logical: "Fn",
			before: `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{` + role + `,
				"Runtime":"python3.12","Handler":"index.handler","Timeout":10,
				"Code":{"ZipFile":"def handler(e, c):\n    return 1\n"}}}}}`,
			after: `{"Resources":{"Fn":{"Type":"AWS::Lambda::Function","Properties":{` + role + `,
				"Runtime":"python3.12","Handler":"index.handler","MemorySize":512,
				"Environment":{"Variables":{"STAGE":"prod","PORT":8080}},
				"Code":{"ZipFile":"def handler(e, c):\n    return 2\n"}}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				var fn struct {
					Configuration struct {
						Timeout     int
						MemorySize  int
						Environment struct{ Variables map[string]string }
					}
				}
				v129REST(t, d, "lambda", "GET", "/2015-03-31/functions/"+dr.PhysicalID, nil, &fn)
				cfg := fn.Configuration
				assert.Equal(t, 3, cfg.Timeout, "a removed Timeout resets to the page's default")
				assert.Equal(t, 512, cfg.MemorySize)
				assert.Equal(t, map[string]string{"STAGE": "prod", "PORT": "8080"}, cfg.Environment.Variables)
			},
		},
		{
			name: "dynamodb billing mode", logical: "Table",
			before: `{"Resources":{"Table":{"Type":"AWS::DynamoDB::Table","Properties":{
				"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
				"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
				"BillingMode":"PROVISIONED",
				"ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5}}}}}`,
			after: `{"Resources":{"Table":{"Type":"AWS::DynamoDB::Table","Properties":{
				"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
				"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],
				"BillingMode":"PAY_PER_REQUEST"}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				out := cfnStubDispatch(t, d, "dynamodb", "DescribeTable", map[string]any{"TableName": dr.PhysicalID})
				table, _ := out["Table"].(map[string]any)
				summary, _ := table["BillingModeSummary"].(map[string]any)
				assert.Equal(t, "PAY_PER_REQUEST", summary["BillingMode"])
			},
		},
		{
			name: "sqs attributes, set and reset", logical: "Queue",
			before: `{"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"VisibilityTimeout":60}}}}`,
			after:  `{"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"DelaySeconds":5}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				body, err := v129Query(t, d, "sqs", map[string]string{"Action": "GetQueueAttributes",
					"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/" + dr.PhysicalID, "AttributeName.1": "All"})
				require.NoError(t, err)
				assert.Contains(t, body, "<Name>VisibilityTimeout</Name><Value>30</Value>",
					"a removed attribute resets to the page's default")
				assert.Contains(t, body, "<Name>DelaySeconds</Name><Value>5</Value>")
			},
		},
		{
			name: "sns display name", logical: "Topic",
			before: `{"Resources":{"Topic":{"Type":"AWS::SNS::Topic","Properties":{"DisplayName":"one"}}}}`,
			after:  `{"Resources":{"Topic":{"Type":"AWS::SNS::Topic","Properties":{"DisplayName":"two"}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				body, err := v129Query(t, d, "sns", map[string]string{"Action": "GetTopicAttributes", "TopicArn": dr.PhysicalID})
				require.NoError(t, err)
				assert.Contains(t, body, "<key>DisplayName</key><value>two</value>")
			},
		},
		{
			name: "s3 versioning, encryption and cors", logical: "Bucket",
			before: `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{
				"CorsConfiguration":{"CorsRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"]}]}}}}}`,
			after: `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{
				"VersioningConfiguration":{"Status":"Enabled"},
				"BucketEncryption":{"ServerSideEncryptionConfiguration":[
					{"ServerSideEncryptionByDefault":{"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"alias/data"},
					 "BucketKeyEnabled":true}]}}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				get := func(sub string) (int, string) {
					resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{Service: "s3",
						Operation: "GET", Path: "/" + dr.PhysicalID, Params: map[string]string{sub: ""},
						Headers: map[string]string{}}, "probe")
					require.NotNil(t, resp)
					return resp.StatusCode, string(resp.Body)
				}
				_, versioning := get("versioning")
				assert.Contains(t, versioning, "<Status>Enabled</Status>")
				_, enc := get("encryption")
				assert.Contains(t, enc, "aws:kms")
				assert.Contains(t, enc, "alias/data")
				code, _ := get("cors")
				assert.Equal(t, http.StatusNotFound, code, "a removed CorsConfiguration is deleted")
			},
		},
		{
			name: "kms description, rotation and enabled", logical: "Key",
			before: `{"Resources":{"Key":{"Type":"AWS::KMS::Key","Properties":{"Description":"first"}}}}`,
			after: `{"Resources":{"Key":{"Type":"AWS::KMS::Key","Properties":{"Description":"second",
				"EnableKeyRotation":true,"RotationPeriodInDays":90}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				out := cfnStubDispatch(t, d, "kms", "DescribeKey", map[string]any{"KeyId": dr.PhysicalID})
				meta, _ := out["KeyMetadata"].(map[string]any)
				assert.Equal(t, "second", meta["Description"])
				rot := cfnStubDispatch(t, d, "kms", "GetKeyRotationStatus", map[string]any{"KeyId": dr.PhysicalID})
				assert.Equal(t, true, rot["KeyRotationEnabled"])
				assert.EqualValues(t, 90, rot["RotationPeriodInDays"])
			},
		},
		{
			name: "ecs desired count and task definition", logical: "Svc",
			before: `{"Resources":{
				"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-cluster"}},
				"Task":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-task",
					"ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
				"Svc":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-svc",
					"Cluster":{"Ref":"Cluster"},"TaskDefinition":{"Ref":"Task"},"DesiredCount":1}}}}`,
			after: `{"Resources":{
				"Cluster":{"Type":"AWS::ECS::Cluster","Properties":{"ClusterName":"v129-cluster"}},
				"Task":{"Type":"AWS::ECS::TaskDefinition","Properties":{"Family":"v129-task",
					"ContainerDefinitions":[{"Name":"c","Image":"nginx","Memory":256}]}},
				"Svc":{"Type":"AWS::ECS::Service","Properties":{"ServiceName":"v129-svc",
					"Cluster":{"Ref":"Cluster"},"TaskDefinition":{"Ref":"Task"},"DesiredCount":3}}}}`,
			check: func(t *testing.T, d *emulator.StackDeployer, dr emulator.DeployedResource) {
				out := cfnStubDispatch(t, d, "ecs", "DescribeServices",
					map[string]any{"cluster": "v129-cluster", "services": []string{dr.PhysicalID}})
				services, _ := out["services"].([]any)
				require.Len(t, services, 1)
				svc, _ := services[0].(map[string]any)
				assert.EqualValues(t, 3, svc["desiredCount"])
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			created, updated := v129Update(t, d, "inplace", tc.before, tc.after)
			before := resourceByLogicalID(t, created, tc.logical)
			after := resourceByLogicalID(t, updated, tc.logical)
			assert.Equal(t, before.PhysicalID, after.PhysicalID, "updated in place")
			assert.Empty(t, updated.ResourceDeletions, "nothing was replaced, so nothing was deleted")
			tc.check(t, d, after)
		})
	}
}

// TestCFN_UpdateRefusesWhatItCannotApply pins the two refusals an updater's type can answer: a
// property the create forwards and substrate cannot update (cfnUpdateUnmodelledProperties), and a
// KMS property the page says fails the update rather than replacing the key.
func TestCFN_UpdateRefusesWhatItCannotApply(t *testing.T) {
	cases := []struct {
		name, before, after, want string
	}{
		{
			name: "dynamodb key schema",
			before: `{"Resources":{"R":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST",
				"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
				"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}}}}`,
			after: `{"Resources":{"R":{"Type":"AWS::DynamoDB::Table","Properties":{"BillingMode":"PAY_PER_REQUEST",
				"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
				"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}}}}`,
			want: "UpdateTable takes no KeySchema",
		},
		{
			name:   "kms key spec",
			before: `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"KeySpec":"SYMMETRIC_DEFAULT"}}}}`,
			after:  `{"Resources":{"R":{"Type":"AWS::KMS::Key","Properties":{"KeySpec":"HMAC_256","KeyUsage":"GENERATE_VERIFY_MAC"}}}}`,
			want:   "KeySpec of AWS::KMS::Key cannot be updated",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.before, "refused", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, created)
			result, err := d.UpdateStack(ctx, tc.after, "refused", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
			assert.Contains(t, result.StatusReason, tc.want)
			assert.Equal(t, resourceByLogicalID(t, created, "R").PhysicalID,
				resourceByLogicalID(t, result, "R").PhysicalID, "the resource stands")
		})
	}
}

// TestCFN_KMSKeyCreateAppliesRotationAndPolicy pins two create-side gaps #1545 closed beside the
// update: EnableKeyRotation reached CreateKey, which has no such member, and KeyPolicy was not sent.
func TestCFN_KMSKeyCreateAppliesRotationAndPolicy(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{"Resources":{"Key":{"Type":"AWS::KMS::Key","Properties":{
		"EnableKeyRotation":true,
		"KeyPolicy":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},
			"Action":"kms:*","Resource":"*"}]}}}}}`, "kms-create", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	keyID := resourceByLogicalID(t, result, "Key").PhysicalID
	rot := cfnStubDispatch(t, d, "kms", "GetKeyRotationStatus", map[string]any{"KeyId": keyID})
	assert.Equal(t, true, rot["KeyRotationEnabled"])
	pol := cfnStubDispatch(t, d, "kms", "GetKeyPolicy", map[string]any{"KeyId": keyID, "PolicyName": "default"})
	assert.Contains(t, pol["Policy"], `"Principal":{"AWS":"*"}`)
}

// TestCFN_SecurityGroupUpdateAppliesRules is #1550: a changed group's rules are applied to the
// existing group, which keeps its ID, in the default VPC where a re-create collided on the name.
func TestCFN_SecurityGroupUpdateAppliesRules(t *testing.T) {
	group := func(rules string) string {
		return `{"Resources":{"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{
			"GroupDescription":"web"` + rules + `}}}}`
	}
	ssh := `{"IpProtocol":"tcp","FromPort":22,"ToPort":22,"CidrIp":"10.0.0.0/8"}`
	https := `{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"0.0.0.0/0"}`
	cases := []struct {
		name, before, after string
		ingress, egress     []string
	}{
		{"unchanged", group(`,"SecurityGroupIngress":[` + ssh + `]`), group(`,"SecurityGroupIngress":[` + ssh + `]`),
			[]string{"tcp 22-22 10.0.0.0/8"}, []string{"-1 0-0 0.0.0.0/0"}},
		{"a rule added", group(`,"SecurityGroupIngress":[` + ssh + `]`), group(`,"SecurityGroupIngress":[` + ssh + `,` + https + `]`),
			[]string{"tcp 22-22 10.0.0.0/8", "tcp 443-443 0.0.0.0/0"}, []string{"-1 0-0 0.0.0.0/0"}},
		{"a rule replaced", group(`,"SecurityGroupIngress":[` + ssh + `]`), group(`,"SecurityGroupIngress":[` + https + `]`),
			[]string{"tcp 443-443 0.0.0.0/0"}, []string{"-1 0-0 0.0.0.0/0"}},
		{"ingress removed", group(`,"SecurityGroupIngress":[` + ssh + `]`), group(``),
			nil, []string{"-1 0-0 0.0.0.0/0"}},
		{"egress declared where the default stood", group(``), group(`,"SecurityGroupEgress":[` + https + `]`),
			nil, []string{"tcp 443-443 0.0.0.0/0"}},
		{"declared egress removed leaves none", group(`,"SecurityGroupEgress":[` + https + `]`), group(``),
			nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			created, updated := v129Update(t, d, "sg-update", tc.before, tc.after)
			id := resourceByLogicalID(t, created, "SG").PhysicalID
			require.Equal(t, id, resourceByLogicalID(t, updated, "SG").PhysicalID, "the group keeps its ID")
			ingress, egress := v129SGRules(t, d, id)
			assert.Equal(t, tc.ingress, ingress)
			assert.Equal(t, tc.egress, egress)
		})
	}
}

// TestCFN_ReplacementGetsAFreshGeneratedName pins that a resource replaced for an "Update requires:
// Replacement" property is a new resource under a new generated name, so the update does not collide
// with the resource it replaces, and that the minted name is reproducible from the request.
func TestCFN_ReplacementGetsAFreshGeneratedName(t *testing.T) {
	group := func(desc string) string {
		return `{"Resources":{"SG":{"Type":"AWS::EC2::SecurityGroup","Properties":{"GroupDescription":"` + desc + `"}}}}`
	}
	name := func(seed string) string {
		d := newMintedSweepDeployer(t, seed)
		created, updated := v129Update(t, d, "replaced", group("one"), group("two"))
		oldID := resourceByLogicalID(t, created, "SG").PhysicalID
		newID := resourceByLogicalID(t, updated, "SG").PhysicalID
		require.NotEqual(t, oldID, newID, "GroupDescription is Update requires: Replacement")
		require.Len(t, updated.ResourceDeletions, 1)
		assert.Equal(t, "DELETE_COMPLETE", updated.ResourceDeletions[0].Status)
		assert.Equal(t, oldID, updated.ResourceDeletions[0].PhysicalID)

		var doc struct {
			Names []string `xml:"securityGroupInfo>item>groupName"`
		}
		cfnDescribeEC2(t, d, map[string]string{"Action": "DescribeSecurityGroups", "GroupId.1": newID}, &doc)
		require.Len(t, doc.Names, 1)
		return doc.Names[0]
	}
	a, b := name("req-seed-1"), name("req-seed-1")
	assert.Equal(t, a, b, "the same request mints the same name, so a replay reproduces it")
	assert.True(t, strings.HasPrefix(a, "replaced-SG-"), a)
	assert.NotEqual(t, "replaced-SG-"+generatedSuffixFor(t, "replaced", "SG"), a,
		"not the derived name the replaced group holds")
}

// newMintedSweepDeployer is newSweepDeployer with the deployer's identifiers derived from seed,
// as a CloudFormation request's are, so two deployers built from one seed mint the same values.
func newMintedSweepDeployer(t *testing.T, seed string) *emulator.StackDeployer {
	t.Helper()
	ctx := context.Background()
	cfg := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	registry := emulator.NewPluginRegistry()
	require.NoError(t, emulator.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil))
	return emulator.NewStackDeployer(registry, store, state, tc, logger,
		emulator.NewCostController(emulator.CostConfig{Enabled: true}),
		emulator.WithDeployerMint(emulator.NewIDMint(seed)))
}

// generatedSuffixFor deploys a group into a fresh stack and reads the derived suffix back, so the
// test does not restate the hash.
func generatedSuffixFor(t *testing.T, stack, logical string) string {
	t.Helper()
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{"Resources":{"`+logical+`":{"Type":"AWS::EC2::SecurityGroup",
		"Properties":{"GroupDescription":"x"}}}}`, stack, nil)
	require.NoError(t, err)
	var doc struct {
		Names []string `xml:"securityGroupInfo>item>groupName"`
	}
	cfnDescribeEC2(t, d, map[string]string{"Action": "DescribeSecurityGroups",
		"GroupId.1": resourceByLogicalID(t, result, logical).PhysicalID}, &doc)
	require.Len(t, doc.Names, 1)
	return strings.TrimPrefix(doc.Names[0], stack+"-"+logical+"-")
}

// TestCFN_UpdateDeletesARemovedResource is #1547: a resource the new template no longer deploys —
// removed outright, or under a Condition now false — is deleted after the update, honoring its
// DeletionPolicy, and a failed update deletes none of them.
func TestCFN_UpdateDeletesARemovedResource(t *testing.T) {
	const keep = `"Keep":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"v129-keep"}}`
	gone := func(policy string) string {
		p := ""
		if policy != "" {
			p = `"DeletionPolicy":"` + policy + `",`
		}
		return `"Gone":{"Type":"AWS::SQS::Queue",` + p + `"Properties":{"QueueName":"v129-gone"}}`
	}
	cases := []struct {
		name, before, after string
		params              map[string]string
		wantStatus          string
		wantExists          bool
	}{
		{"removed", `{"Resources":{` + keep + `,` + gone("") + `}}`, `{"Resources":{` + keep + `}}`,
			nil, "DELETE_COMPLETE", false},
		{"removed with Retain", `{"Resources":{` + keep + `,` + gone("Retain") + `}}`, `{"Resources":{` + keep + `}}`,
			nil, "DELETE_SKIPPED", true},
		{"removed with RetainExceptOnCreate", `{"Resources":{` + keep + `,` + gone("RetainExceptOnCreate") + `}}`,
			`{"Resources":{` + keep + `}}`, nil, "DELETE_SKIPPED", true},
		{"condition now false",
			`{"Parameters":{"On":{"Type":"String","Default":"yes"}},"Conditions":{"IsOn":{"Fn::Equals":[{"Ref":"On"},"yes"]}},
			  "Resources":{` + keep + `,"Gone":{"Type":"AWS::SQS::Queue","Condition":"IsOn","Properties":{"QueueName":"v129-gone"}}}}`,
			"", map[string]string{"On": "no"}, "DELETE_COMPLETE", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			_, err := d.Deploy(ctx, tc.before, "removal", nil)
			require.NoError(t, err)
			after := tc.after
			if after == "" {
				after = tc.before
			}
			result, err := d.UpdateStack(ctx, after, "removal", tc.params, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			require.Equal(t, "UPDATE_COMPLETE", result.Status, result.StatusReason)
			require.Len(t, result.ResourceDeletions, 1)
			assert.Equal(t, "Gone", result.ResourceDeletions[0].LogicalID)
			assert.Equal(t, tc.wantStatus, result.ResourceDeletions[0].Status)
			_, urlErr := v129Query(t, d, "sqs", map[string]string{"Action": "GetQueueUrl", "QueueName": "v129-gone"})
			if tc.wantExists {
				assert.NoError(t, urlErr)
			} else {
				require.Error(t, urlErr)
				assert.Contains(t, urlErr.Error(), "QueueDoesNotExist")
			}
			for _, r := range result.Resources {
				assert.NotEqual(t, "Gone", r.LogicalID, "the stack no longer names it")
			}
			// The stack is deletable afterwards and does not reach the retained queue.
			require.NoError(t, d.DeleteStack(ctx, "removal"))
		})
	}

	t.Run("a failed update deletes nothing it dropped", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		ctx := context.Background()
		_, err := d.Deploy(ctx, `{"Resources":{`+keep+`,`+gone("")+`}}`, "removal-fails", nil)
		require.NoError(t, err)
		result, err := d.UpdateStack(ctx, `{"Resources":{`+keep+`,
			"Bad":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}}}`, "removal-fails", nil,
			emulator.CFNDeployOptions{})
		require.NoError(t, err)
		assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
		_, urlErr := v129Query(t, d, "sqs", map[string]string{"Action": "GetQueueUrl", "QueueName": "v129-gone"})
		assert.NoError(t, urlErr, "the rollback restored the previous template, which still holds the queue")
		resourceByLogicalID(t, result, "Gone")
	})

	t.Run("replaced and removed share one sweep", func(t *testing.T) {
		d, _, _, store := newSweepDeployer(t)
		ctx := context.Background()
		_, err := d.Deploy(ctx, `{"Resources":{`+keep+`,`+gone("")+`}}`, "removal-both", nil)
		require.NoError(t, err)
		result, err := d.UpdateStack(ctx, `{"Resources":{"Keep":{"Type":"AWS::SQS::Queue",
			"Properties":{"QueueName":"v129-keep-2"}}}}`, "removal-both", nil, emulator.CFNDeployOptions{})
		require.NoError(t, err)
		require.Equal(t, "UPDATE_COMPLETE", result.Status, result.StatusReason)
		var ids []string
		for _, del := range result.ResourceDeletions {
			assert.Equal(t, "DELETE_COMPLETE", del.Status, del.LogicalID)
			ids = append(ids, del.LogicalID)
		}
		sort.Strings(ids)
		assert.Equal(t, []string{"Gone", "Keep"}, ids)
		ops := sweepOperations(t, store, "removal-both")
		assert.Equal(t, "DeleteQueue", ops[len(ops)-1])
		assert.Equal(t, "DeleteQueue", ops[len(ops)-2])
	})
}

// TestCFN_OutputFailures is #1429. An Output reading an attribute substrate cannot answer is refused
// at the request with CloudFormation's template-validation error; any other Output that does not
// resolve fails the stack, which rolls back.
func TestCFN_OutputFailures(t *testing.T) {
	const server = `"Server":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["AS2"]}}`
	refused := []struct{ name, output string }{
		{"Fn::GetAtt", `{"Fn::GetAtt":["Server","As2ServiceManagedEgressIpAddresses"]}`},
		{"Fn::Sub", `{"Fn::Sub":"${Server.As2ServiceManagedEgressIpAddresses}"}`},
	}
	const wantMsg = "Template error: resource Server does not support attribute type " +
		"As2ServiceManagedEgressIpAddresses in Fn::GetAtt"
	for _, tc := range refused {
		t.Run("refused "+tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			tmpl := `{"Resources":{` + server + `},"Outputs":{"Egress":{"Value":` + tc.output + `}}}`
			_, err := d.Deploy(ctx, tmpl, "out-refused", nil)
			require.ErrorIs(t, err, emulator.ErrCFNUnsupportedAttribute)
			assert.Contains(t, err.Error(), wantMsg)
			stacks, listErr := d.ListStacks(ctx)
			require.NoError(t, listErr)
			assert.Empty(t, stacks, "a refused request leaves no stack")

			_, err = d.Deploy(ctx, `{"Resources":{`+server+`}}`, "out-refused", nil)
			require.NoError(t, err)
			_, err = d.UpdateStack(ctx, tmpl, "out-refused", nil, emulator.CFNDeployOptions{})
			require.ErrorIs(t, err, emulator.ErrCFNUnsupportedAttribute)
			_, err = d.CreateChangeSet(ctx, "out-refused", "cs", tmpl, nil, nil)
			require.ErrorIs(t, err, emulator.ErrCFNUnsupportedAttribute)
		})
	}

	t.Run("refused on the wire", func(t *testing.T) {
		ts := newCFNTestServer(t)
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{"StackName": "wire-out",
			"TemplateBody": `{"Resources":{"B":{"Type":"AWS::S3::Bucket"},` + server + `},
				"Outputs":{"E":{"Value":{"Fn::GetAtt":["Server","As2ServiceManagedEgressIpAddresses"]}}}}`})
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
		var doc struct {
			Message string `xml:"Error>Message"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc))
		assert.Equal(t, wantMsg, doc.Message)
	})

	failing := []struct{ name, tmpl, want string }{
		{"Fn::ImportValue", `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},
			"Outputs":{"Imported":{"Value":{"Fn::ImportValue":"no-such-export"}}}}`,
			`Output Imported: Fn::ImportValue: no exported output named "no-such-export"`},
		{"Fn::FindInMap", `{"Mappings":{"M":{"a":{"b":"c"}}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},
			"Outputs":{"Mapped":{"Value":{"Fn::FindInMap":["M","a","nope"]}}}}`, "Output Mapped: "},
		{"Fn::Cidr", `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},
			"Outputs":{"Blocks":{"Value":{"Fn::Select":[0,{"Fn::Cidr":["10.0.0.0/16","0","8"]}]}}}}`,
			"Output Blocks: Fn::Cidr: count"},
	}
	for _, tc := range failing {
		t.Run("fails the stack "+tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			result, err := d.Deploy(ctx, tc.tmpl, "out-fails", nil)
			require.NoError(t, err)
			assert.Equal(t, "ROLLBACK_COMPLETE", result.Status)
			assert.Contains(t, result.StatusReason, tc.want)
			assert.Empty(t, result.Outputs, "a failed stack publishes no outputs")

			// The same Output added by an update rolls the update back.
			require.NoError(t, d.DeleteStack(ctx, "out-fails"))
			_, err = d.Deploy(ctx, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},
				"Outputs":{"Ok":{"Value":"fine"}}}`, "out-fails", nil)
			require.NoError(t, err)
			updated, err := d.UpdateStack(ctx, tc.tmpl, "out-fails", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", updated.Status)
			assert.Contains(t, updated.StatusReason, tc.want)
		})
	}
}

// TestKMSUpdateKeyDescription pins API_UpdateKeyDescription: the description is replaced and read
// back by DescribeKey, an empty one is accepted, an unknown key is NotFoundException, and a key
// pending deletion is KMSInvalidStateException, per the key-state table.
func TestKMSUpdateKeyDescription(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	created := cfnStubDispatch(t, d, "kms", "CreateKey", map[string]any{"Description": "before"})
	meta, _ := created["KeyMetadata"].(map[string]any)
	keyID, _ := meta["KeyId"].(string)
	require.NotEmpty(t, keyID)

	for _, desc := range []string{"after", ""} {
		cfnStubDispatch(t, d, "kms", "UpdateKeyDescription", map[string]any{"KeyId": keyID, "Description": desc})
		out := cfnStubDispatch(t, d, "kms", "DescribeKey", map[string]any{"KeyId": keyID})
		m, _ := out["KeyMetadata"].(map[string]any)
		assert.Equal(t, desc, m["Description"])
	}

	call := func(body map[string]any) error {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		_, routeErr := d.DispatchForTest(context.Background(), &emulator.AWSRequest{Service: "kms",
			Operation: "UpdateKeyDescription", Body: data, Headers: map[string]string{}, Params: map[string]string{}}, "probe")
		return routeErr
	}
	err := call(map[string]any{"KeyId": "11111111-2222-3333-4444-555555555555", "Description": "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NotFoundException")

	cfnStubDispatch(t, d, "kms", "ScheduleKeyDeletion", map[string]any{"KeyId": keyID, "PendingWindowInDays": 7})
	err = call(map[string]any{"KeyId": keyID, "Description": "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KMSInvalidStateException")
}
