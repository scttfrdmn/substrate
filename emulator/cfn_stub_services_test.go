package emulator_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1203 and #1182. The five types #1203 names were deployed as stubs (or, for FSx and
// MSK, with dropped properties), so a stack created a resource its own service's API could not see,
// or could see with the wrong properties. Each test deploys a stack and then asks the owning
// service, through the deployer's own registry, about the value the template's Ref resolved to.

// cfnStubDispatch sends one JSON-body operation through the deployer's registry and decodes the
// answer.
func cfnStubDispatch(t *testing.T, d *emulator.StackDeployer, service, operation string, body any) map[string]any {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: service, Operation: operation, Body: data,
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	require.NoError(t, err, "%s %s", service, operation)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return out
}

// TestCFN_TransferServerIsVisibleToTransfer pins that the stack's server is the Transfer plugin's:
// its ID is one CreateServer minted, DescribeServer answers it with the template's protocols, and
// each published attribute resolves.
func TestCFN_TransferServerIsVisibleToTransfer(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{
		"Resources": {
			"Server": {"Type": "AWS::Transfer::Server", "Properties": {
				"Protocols": ["SFTP"],
				"Tags": [{"Key": "team", "Value": {"Fn::Sub": "${AWS::StackName}-ops"}}]
			}}
		},
		"Outputs": {
			"Ref":      {"Value": {"Ref": "Server"}},
			"Arn":      {"Value": {"Fn::GetAtt": ["Server", "Arn"]}},
			"ServerId": {"Value": {"Fn::GetAtt": ["Server", "ServerId"]}},
			"State":    {"Value": {"Fn::GetAtt": ["Server", "State"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "transfer-real", nil)
	require.NoError(t, err)
	r := resourceByLogicalID(t, result, "Server")
	require.Empty(t, r.Error)

	// API_DescribedServer: ServerId, "Fixed length of 19", "Pattern: s-([0-9a-f]{17})".
	assert.Regexp(t, regexp.MustCompile(`^s-[0-9a-f]{17}$`), r.PhysicalID)
	assert.Equal(t, r.PhysicalID, result.Outputs["ServerId"])
	assert.Equal(t, r.ARN, result.Outputs["Ref"], "Ref returns the server ARN")
	assert.Equal(t, r.ARN, result.Outputs["Arn"])
	assert.Equal(t, "ONLINE", result.Outputs["State"])

	out := cfnStubDispatch(t, d, "transfer", "DescribeServer", map[string]any{"ServerId": r.PhysicalID})
	server, ok := out["Server"].(map[string]any)
	require.True(t, ok, "DescribeServer answers the stack's server: %v", out)
	assert.Equal(t, []any{"SFTP"}, server["Protocols"])
	assert.Equal(t, []any{map[string]any{"Key": "team", "Value": "transfer-real-ops"}}, server["Tags"])

	// The sweep deletes it through DeleteServer rather than removing a stub key.
	require.NoError(t, d.DeleteStack(context.Background(), "transfer-real"))
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "transfer", Operation: "DescribeServer",
		Body:    []byte(`{"ServerId":"` + r.PhysicalID + `"}`),
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	require.Error(t, err, "the deleted server is gone: %v", resp)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

// TestCFN_UnmodelledGetAttIsRefused pins that a published attribute substrate holds no value for
// fails the resource that reads it, naming the attribute, rather than resolving to "".
func TestCFN_UnmodelledGetAttIsRefused(t *testing.T) {
	cases := []struct {
		name, resource, attr string
	}{
		{"transfer AS2 egress addresses", `{"Type": "AWS::Transfer::Server", "Properties": {"Protocols": ["SFTP"]}}`,
			"As2ServiceManagedEgressIpAddresses"},
		{"opensearch dual-stack endpoint", `{"Type": "AWS::OpenSearchService::Domain", "Properties": {"DomainName": "logs"}}`,
			"DomainEndpointV2"},
		{"fsx root volume", `{"Type": "AWS::FSx::FileSystem", "Properties": {"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": ["subnet-0123456789abcdef0"]}}`,
			"RootVolumeId"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			// The reader is a CloudWatch alarm because its deploy priority (4) is later than every
			// target's, so the target is deployed before the attribute is resolved.
			tmpl := `{"Resources": {
				"Target": ` + tc.resource + `,
				"Reader": {"Type": "AWS::CloudWatch::Alarm", "Properties": {
					"AlarmDescription": {"Fn::GetAtt": ["Target", "` + tc.attr + `"]},
					"MetricName": "Errors", "Namespace": "App", "Statistic": "Sum", "Period": 60,
					"EvaluationPeriods": 1, "Threshold": 1, "ComparisonOperator": "GreaterThanThreshold"}}
			}}`
			result, err := d.Deploy(context.Background(), tmpl, "getatt-refused", nil)
			require.NoError(t, err)
			require.Empty(t, resourceByLogicalID(t, result, "Target").Error)
			reader := resourceByLogicalID(t, result, "Reader")
			assert.Contains(t, reader.Error, "Target."+tc.attr)
			assert.Contains(t, reader.Error, "has no value in substrate")
		})
	}
}

// TestCFN_CodeDeployGroupIsVisibleToCodeDeploy pins that a stack's application and deployment
// group are the CodeDeploy plugin's, with the template's PascalCase properties renamed to the API's
// lowerCamel members, and the tag set reshaped from Ec2TagGroup objects to filter lists.
func TestCFN_CodeDeployGroupIsVisibleToCodeDeploy(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{
		"Resources": {
			"App": {"Type": "AWS::CodeDeploy::Application", "Properties": {"ApplicationName": "web"}},
			"Group": {"Type": "AWS::CodeDeploy::DeploymentGroup", "Properties": {
				"ApplicationName": {"Ref": "App"},
				"DeploymentGroupName": "web-prod",
				"ServiceRoleArn": "arn:aws:iam::123456789012:role/CodeDeployRole",
				"DeploymentStyle": {"DeploymentOption": "WITH_TRAFFIC_CONTROL", "DeploymentType": "IN_PLACE"},
				"Ec2TagSet": {"Ec2TagSetList": [
					{"Ec2TagGroup": [{"Key": "role", "Value": "web", "Type": "KEY_AND_VALUE"}]}
				]},
				"ECSServices": [{"ClusterName": "c", "ServiceName": "s"}]
			}}
		},
		"Outputs": {"Ref": {"Value": {"Ref": "Group"}}}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "codedeploy-real", nil)
	require.NoError(t, err)
	require.Empty(t, resourceByLogicalID(t, result, "App").Error)
	require.Empty(t, resourceByLogicalID(t, result, "Group").Error)
	assert.Equal(t, "web-prod", result.Outputs["Ref"])

	out := cfnStubDispatch(t, d, "codedeploy", "GetDeploymentGroup",
		map[string]any{"applicationName": "web", "deploymentGroupName": "web-prod"})
	info, ok := out["deploymentGroupInfo"].(map[string]any)
	require.True(t, ok, "GetDeploymentGroup answers the stack's group: %v", out)
	assert.Equal(t, "arn:aws:iam::123456789012:role/CodeDeployRole", info["serviceRoleArn"])
	assert.Equal(t, map[string]any{"deploymentOption": "WITH_TRAFFIC_CONTROL", "deploymentType": "IN_PLACE"},
		info["deploymentStyle"])
	assert.Equal(t, map[string]any{"ec2TagSetList": []any{[]any{
		map[string]any{"key": "role", "value": "web", "type": "KEY_AND_VALUE"}}}}, info["ec2TagSet"])
	assert.Equal(t, []any{map[string]any{"clusterName": "c", "serviceName": "s"}}, info["ecsServices"])

	require.NoError(t, d.DeleteStack(context.Background(), "codedeploy-real"))
	_, err = d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "codedeploy", Operation: "GetApplication", Body: []byte(`{"applicationName":"web"}`),
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ApplicationDoesNotExistException")
}

// TestCFN_FSxConfigurationBlocksReachFSx pins that LustreConfiguration is sent, so a PERSISTENT_2
// template produces a PERSISTENT_2 file system rather than one of the default type.
func TestCFN_FSxConfigurationBlocksReachFSx(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const tmpl = `{"Resources": {"FS": {"Type": "AWS::FSx::FileSystem", "Properties": {
		"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": ["subnet-0123456789abcdef0"],
		"LustreConfiguration": {"DeploymentType": "PERSISTENT_2", "PerUnitStorageThroughput": 125}
	}}}}`
	result, err := d.Deploy(context.Background(), tmpl, "fsx-persistent", nil)
	require.NoError(t, err)
	r := resourceByLogicalID(t, result, "FS")
	require.Empty(t, r.Error)

	out := cfnStubDispatch(t, d, "fsx", "DescribeFileSystems", map[string]any{"FileSystemIds": []string{r.PhysicalID}})
	systems, ok := out["FileSystems"].([]any)
	require.True(t, ok)
	require.Len(t, systems, 1)
	lustre := systems[0].(map[string]any)["LustreConfiguration"].(map[string]any)
	assert.Equal(t, "PERSISTENT_2", lustre["DeploymentType"])
	assert.NotEqual(t, "fsx", lustre["MountName"], "only SCRATCH_1 mounts as fsx")
}

// TestCFN_MSKBrokerCount pins that NumberOfBrokerNodes is read from the template, and that its
// absence, which the property's Required: Yes forbids, fails the resource.
func TestCFN_MSKBrokerCount(t *testing.T) {
	cases := []struct {
		name, brokers string
		want          float64
		wantErr       string
	}{
		{"three brokers", `"NumberOfBrokerNodes": 3,`, 3, ""},
		{"a numeric string", `"NumberOfBrokerNodes": "6",`, 6, ""},
		{"absent", ``, 0, "NumberOfBrokerNodes is a required integer property"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			tmpl := `{"Resources": {"Kafka": {"Type": "AWS::MSK::Cluster", "Properties": {
				"ClusterName": "events", "KafkaVersion": "3.5.1", ` + tc.brokers + `
				"BrokerNodeGroupInfo": {"InstanceType": "kafka.m5.large", "ClientSubnets": ["subnet-a"]}
			}}}}`
			result, err := d.Deploy(context.Background(), tmpl, "msk-brokers", nil)
			require.NoError(t, err)
			r := resourceByLogicalID(t, result, "Kafka")
			if tc.wantErr != "" {
				assert.Contains(t, r.Error, tc.wantErr)
				return
			}
			require.Empty(t, r.Error)
			resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
				Service: "msk", Operation: "GET", Path: "/v1/clusters/" + r.ARN,
				Headers: map[string]string{}, Params: map[string]string{},
			}, "probe")
			require.NoError(t, err)
			var out struct {
				ClusterInfo struct {
					NumberOfBrokerNodes float64 `json:"numberOfBrokerNodes"`
				} `json:"clusterInfo"`
			}
			require.NoError(t, json.Unmarshal(resp.Body, &out))
			assert.Equal(t, tc.want, out.ClusterInfo.NumberOfBrokerNodes)
		})
	}
}

// TestCFN_SearchDomainAttributes pins the OpenSearch and legacy Elasticsearch domain attributes:
// DomainEndpoint is a host the OpenSearch data plane serves, rather than "", and Id is
// {account}/{name}.
func TestCFN_SearchDomainAttributes(t *testing.T) {
	for _, resType := range []string{"AWS::OpenSearchService::Domain", "AWS::Elasticsearch::Domain"} {
		t.Run(resType, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			tmpl := `{"Resources": {"Search": {"Type": "` + resType + `", "Properties": {"DomainName": "logs"}}},
				"Outputs": {
					"Ref":       {"Value": {"Ref": "Search"}},
					"Arn":       {"Value": {"Fn::GetAtt": ["Search", "Arn"]}},
					"DomainArn": {"Value": {"Fn::GetAtt": ["Search", "DomainArn"]}},
					"Endpoint":  {"Value": {"Fn::GetAtt": ["Search", "DomainEndpoint"]}},
					"Id":        {"Value": {"Fn::GetAtt": ["Search", "Id"]}}
				}}`
			result, err := d.Deploy(context.Background(), tmpl, "search-stack", nil)
			require.NoError(t, err)
			require.Empty(t, resourceByLogicalID(t, result, "Search").Error)
			assert.Equal(t, "logs", result.Outputs["Ref"])
			assert.Equal(t, "arn:aws:es:us-east-1:123456789012:domain/logs", result.Outputs["Arn"])
			assert.Equal(t, result.Outputs["Arn"], result.Outputs["DomainArn"])
			assert.Regexp(t, regexp.MustCompile(`^search-logs-[0-9a-z]{12}\.us-east-1\.es\.amazonaws\.com$`),
				result.Outputs["Endpoint"])
			if resType == "AWS::OpenSearchService::Domain" {
				assert.Equal(t, "123456789012/logs", result.Outputs["Id"])
			}

			// The same stack redeployed keeps its endpoint, so an UpdateStack does not move it.
			again, err := d.Deploy(context.Background(), tmpl, "search-stack", nil)
			require.NoError(t, err)
			assert.Equal(t, result.Outputs["Endpoint"], again.Outputs["Endpoint"])
		})
	}
}

// TestCFN_BackupPlanRefAndAttributes pins #1182: Ref is the plan ID rather than the logical ID, the
// three published attributes resolve, and the IDs survive an unchanged redeploy.
func TestCFN_BackupPlanRefAndAttributes(t *testing.T) {
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	tmpl := func(rule string) string {
		return `{"Resources": {"Plan": {"Type": "AWS::Backup::BackupPlan", "Properties": {
			"BackupPlan": {"BackupPlanName": "daily", "BackupPlanRule": [` + rule + `]}}}},
			"Outputs": {
				"Ref":       {"Value": {"Ref": "Plan"}},
				"Id":        {"Value": {"Fn::GetAtt": ["Plan", "BackupPlanId"]}},
				"Arn":       {"Value": {"Fn::GetAtt": ["Plan", "BackupPlanArn"]}},
				"VersionId": {"Value": {"Fn::GetAtt": ["Plan", "VersionId"]}}
			}}`
	}
	d, _, _, _ := newSweepDeployer(t)
	first, err := d.Deploy(context.Background(), tmpl(``), "backup-stack", nil)
	require.NoError(t, err)
	require.Empty(t, resourceByLogicalID(t, first, "Plan").Error)

	assert.Regexp(t, uuid, first.Outputs["Ref"])
	assert.NotEqual(t, "Plan", first.Outputs["Ref"])
	assert.Equal(t, first.Outputs["Ref"], first.Outputs["Id"])
	assert.Equal(t, "arn:aws:backup:us-east-1:123456789012:backup-plan:"+first.Outputs["Ref"], first.Outputs["Arn"])
	assert.Regexp(t, uuid, first.Outputs["VersionId"])

	same, err := d.Deploy(context.Background(), tmpl(``), "backup-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, first.Outputs["Ref"], same.Outputs["Ref"], "an unchanged redeploy keeps the plan ID")
	assert.Equal(t, first.Outputs["VersionId"], same.Outputs["VersionId"])

	changed, err := d.Deploy(context.Background(), tmpl(`{"RuleName": "r", "TargetBackupVault": "v"}`), "backup-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, first.Outputs["Ref"], changed.Outputs["Ref"])
	assert.NotEqual(t, first.Outputs["VersionId"], changed.Outputs["VersionId"], "a changed plan is a new version")

	// BackupPlan is Required: Yes.
	bare, err := d.Deploy(context.Background(), `{"Resources": {"Plan": {"Type": "AWS::Backup::BackupPlan", "Properties": {}}}}`,
		"backup-bare", nil)
	require.NoError(t, err)
	assert.Contains(t, resourceByLogicalID(t, bare, "Plan").Error, "BackupPlan is a required property")
}
