package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Tests for #1427: what UpdateStack does with each resource. An unchanged resource keeps its
// physical resource and no call is made; a changed one is updated in place where the type's update
// operation is modeled, replaced where its "Update requires" says Replacement, and refused where AWS
// would update it in place through an operation substrate does not route.

// countOps counts how many times op was recorded on a stream.
func countOps(ops []string, op string) int {
	n := 0
	for _, o := range ops {
		if o == op {
			n++
		}
	}
	return n
}

// cfnRawDispatch sends one request through the deployer's registry and returns the response and
// error as they came back, for a probe that expects a refusal.
func cfnRawDispatch(d *emulator.StackDeployer, req *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	if req.Headers == nil {
		req.Headers = map[string]string{}
	}
	if req.Params == nil {
		req.Params = map[string]string{}
	}
	return d.DispatchForTest(context.Background(), req, "probe")
}

// transferServerCount answers how many servers ListServers reports.
func transferServerCount(t *testing.T, d *emulator.StackDeployer) int {
	t.Helper()
	out := cfnStubDispatch(t, d, "transfer", "ListServers", map[string]any{})
	servers, _ := out["Servers"].([]any)
	return len(servers)
}

// fsxFileSystemCount answers how many file systems DescribeFileSystems reports.
func fsxFileSystemCount(t *testing.T, d *emulator.StackDeployer) int {
	t.Helper()
	out := cfnStubDispatch(t, d, "fsx", "DescribeFileSystems", map[string]any{})
	systems, _ := out["FileSystems"].([]any)
	return len(systems)
}

// mskClusterCount answers how many clusters ListClusters reports.
func mskClusterCount(t *testing.T, d *emulator.StackDeployer) int {
	t.Helper()
	resp, err := cfnRawDispatch(d, &emulator.AWSRequest{Service: "msk", Operation: "GET", Path: "/v1/clusters"})
	require.NoError(t, err)
	var out struct {
		ClusterInfoList []any `json:"clusterInfoList"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &out))
	return len(out.ClusterInfoList)
}

// TestCFN_UnchangedUpdateKeepsTheResource is #1427's acceptance criterion for each of the three
// types it names, and for an internet gateway, the generic case: an UpdateStack of an unchanged
// template keeps the physical ID, the service holds one resource, and no create is recorded.
func TestCFN_UnchangedUpdateKeepsTheResource(t *testing.T) {
	tests := []struct {
		name, tmpl, create string
		count              func(*testing.T, *emulator.StackDeployer) int
	}{
		{"transfer", `{"Resources":{"R":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"]}}}}`,
			"CreateServer", transferServerCount},
		{"fsx", `{"Resources":{"R":{"Type":"AWS::FSx::FileSystem","Properties":{"FileSystemType":"LUSTRE",
			"StorageCapacity":1200,"SubnetIds":["subnet-0123456789abcdef0"]}}}}`,
			"CreateFileSystem", fsxFileSystemCount},
		{"msk", `{"Resources":{"R":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"events",
			"KafkaVersion":"3.5.1","NumberOfBrokerNodes":3,
			"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-a"]}}}}}`,
			"CreateCluster", mskClusterCount},
		{"internet gateway", `{"Resources":{"R":{"Type":"AWS::EC2::InternetGateway","Properties":{}}}}`,
			"CreateInternetGateway", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.tmpl, "unchanged", nil)
			require.NoError(t, err)
			first := resourceByLogicalID(t, created, "R")
			require.Empty(t, first.Error)
			before := len(sweepOperations(t, store, "unchanged"))

			updated, err := d.UpdateStack(ctx, tc.tmpl, "unchanged", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_COMPLETE", updated.Status)
			assert.Equal(t, first.PhysicalID, resourceByLogicalID(t, updated, "R").PhysicalID,
				"the physical ID survives the update")
			ops := sweepOperations(t, store, "unchanged")[before:]
			assert.Zero(t, countOps(ops, tc.create), "no create is issued for an unchanged resource: %v", ops)
			if tc.count != nil {
				assert.Equal(t, 1, tc.count(t, d), "the service holds one resource, not two")
			}
		})
	}
}

// TestCFN_TransferUpdatesInPlace pins the in-place path: a changed banner goes through UpdateServer
// and the server keeps its ID, which DescribeServer then reports with the new value.
func TestCFN_TransferUpdatesInPlace(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(banner string) string {
		return `{"Resources":{"Server":{"Type":"AWS::Transfer::Server","Properties":{
			"Protocols":["SFTP"],"PreAuthenticationLoginBanner":"` + banner + `"}}}}`
	}
	created, err := d.Deploy(ctx, tmpl("before"), "transfer-update", nil)
	require.NoError(t, err)
	id := resourceByLogicalID(t, created, "Server").PhysicalID

	updated, err := d.UpdateStack(ctx, tmpl("after"), "transfer-update", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, id, resourceByLogicalID(t, updated, "Server").PhysicalID)
	assert.Equal(t, 1, countOps(sweepOperations(t, store, "transfer-update"), "UpdateServer"))
	assert.Equal(t, 1, transferServerCount(t, d))

	out := cfnStubDispatch(t, d, "transfer", "DescribeServer", map[string]any{"ServerId": id})
	server, _ := out["Server"].(map[string]any)
	assert.Equal(t, "after", server["PreAuthenticationLoginBanner"])
}

// TestCFN_ReplacementCreatesThenDeletes pins the replacement path for each type #1427 names, with
// the property each page marks Update requires: Replacement. The new resource has a new ID, the old
// one is deleted after it, and the service holds one.
func TestCFN_ReplacementCreatesThenDeletes(t *testing.T) {
	tests := []struct {
		name           string
		before, after  string
		count          func(*testing.T, *emulator.StackDeployer) int
		create, delete string
	}{
		{"transfer Domain",
			`{"Resources":{"R":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],"Domain":"S3"}}}}`,
			`{"Resources":{"R":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],"Domain":"EFS"}}}}`,
			transferServerCount, "CreateServer", "DeleteServer"},
		{"fsx SubnetIds",
			`{"Resources":{"R":{"Type":"AWS::FSx::FileSystem","Properties":{"FileSystemType":"LUSTRE",
				"StorageCapacity":1200,"SubnetIds":["subnet-0123456789abcdef0"]}}}}`,
			`{"Resources":{"R":{"Type":"AWS::FSx::FileSystem","Properties":{"FileSystemType":"LUSTRE",
				"StorageCapacity":1200,"SubnetIds":["subnet-0fedcba9876543210"]}}}}`,
			fsxFileSystemCount, "CreateFileSystem", "DeleteFileSystem"},
		{"msk ClusterName",
			`{"Resources":{"R":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"events","KafkaVersion":"3.5.1",
				"NumberOfBrokerNodes":3,"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-a"]}}}}}`,
			`{"Resources":{"R":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"events-v2","KafkaVersion":"3.5.1",
				"NumberOfBrokerNodes":3,"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-a"]}}}}}`,
			mskClusterCount, "CreateCluster", "DeleteCluster"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.before, "replaced", nil)
			require.NoError(t, err)
			old := resourceByLogicalID(t, created, "R")
			require.Empty(t, old.Error)
			before := len(sweepOperations(t, store, "replaced"))

			updated, err := d.UpdateStack(ctx, tc.after, "replaced", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
			fresh := resourceByLogicalID(t, updated, "R")
			assert.NotEqual(t, old.PhysicalID, fresh.PhysicalID, "a replacement has a new physical ID")
			require.Len(t, updated.ResourceDeletions, 1)
			assert.Equal(t, "DELETE_COMPLETE", updated.ResourceDeletions[0].Status)
			assert.Equal(t, 1, tc.count(t, d), "the replaced resource is gone")

			ops := sweepOperations(t, store, "replaced")[before:]
			createAt, deleteAt := -1, -1
			for i, op := range ops {
				if op == tc.create && createAt < 0 {
					createAt = i
				}
				if op == tc.delete {
					deleteAt = i
				}
			}
			require.GreaterOrEqual(t, createAt, 0, "%v", ops)
			assert.Greater(t, deleteAt, createAt, "the new resource is created before the old is deleted: %v", ops)
		})
	}
}

// TestCFN_UnroutedInPlaceUpdateIsRefused pins the third path: AWS updates FSx's StorageCapacity and
// MSK's broker count in place, through operations substrate does not route, so the resource fails
// with a reason naming the gap and the update rolls back onto the original, which keeps its ID.
func TestCFN_UnroutedInPlaceUpdateIsRefused(t *testing.T) {
	tests := []struct {
		name, before, after, want string
		count                     func(*testing.T, *emulator.StackDeployer) int
	}{
		{"fsx StorageCapacity",
			`{"Resources":{"R":{"Type":"AWS::FSx::FileSystem","Properties":{"FileSystemType":"LUSTRE",
				"StorageCapacity":1200,"SubnetIds":["subnet-0123456789abcdef0"]}}}}`,
			`{"Resources":{"R":{"Type":"AWS::FSx::FileSystem","Properties":{"FileSystemType":"LUSTRE",
				"StorageCapacity":2400,"SubnetIds":["subnet-0123456789abcdef0"]}}}}`,
			"updating StorageCapacity of AWS::FSx::FileSystem is not modeled: FSx updates this property " +
				"in place through UpdateFileSystem, which substrate does not route",
			fsxFileSystemCount},
		{"msk NumberOfBrokerNodes",
			`{"Resources":{"R":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"events","KafkaVersion":"3.5.1",
				"NumberOfBrokerNodes":3,"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-a"]}}}}}`,
			`{"Resources":{"R":{"Type":"AWS::MSK::Cluster","Properties":{"ClusterName":"events","KafkaVersion":"3.5.1",
				"NumberOfBrokerNodes":6,"BrokerNodeGroupInfo":{"InstanceType":"kafka.m5.large","ClientSubnets":["subnet-a"]}}}}}`,
			"updating NumberOfBrokerNodes of AWS::MSK::Cluster is not modeled",
			mskClusterCount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			created, err := d.Deploy(ctx, tc.before, "refused-update", nil)
			require.NoError(t, err)
			id := resourceByLogicalID(t, created, "R").PhysicalID

			updated, err := d.UpdateStack(ctx, tc.after, "refused-update", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", updated.Status)
			assert.Contains(t, updated.StatusReason, tc.want)
			assert.Equal(t, id, resourceByLogicalID(t, updated, "R").PhysicalID, "the rollback keeps the original")
			assert.Equal(t, 1, tc.count(t, d))
		})
	}
}

// TestCFN_ChangedInputUpdatesAnUnchangedDeclaration pins that "unchanged" is decided on resolved
// values: a queue whose text is identical but whose name comes from a parameter that changed is
// re-created under the new name, and the queue under the old name is deleted once the update is
// through.
func TestCFN_ChangedInputUpdatesAnUnchangedDeclaration(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const tmpl = `{"Parameters":{"Name":{"Type":"String"}},
		"Resources":{"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Ref":"Name"}}}}}`
	_, err := d.Deploy(ctx, tmpl, "param-change", map[string]string{"Name": "param-before"})
	require.NoError(t, err)
	require.True(t, queueExists(t, d, "param-before"))

	updated, err := d.UpdateStack(ctx, tmpl, "param-change", map[string]string{"Name": "param-after"},
		emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.True(t, queueExists(t, d, "param-after"))
	assert.False(t, queueExists(t, d, "param-before"), "the replaced queue is deleted")
}

// TestCFN_UpdateReplacePolicyRetainKeepsTheOldResource pins the attribute the replacement delete
// consults: "Using the UpdateReplacePolicy, you can specify that CloudFormation retain" the old
// resource, and a bucket renamed under Retain leaves the old bucket in place.
func TestCFN_UpdateReplacePolicyRetainKeepsTheOldResource(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(name string) string {
		return `{"Resources":{"Data":{"Type":"AWS::S3::Bucket","UpdateReplacePolicy":"Retain",
			"Properties":{"BucketName":"` + name + `"}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("retain-before"), "retain-replace", nil)
	require.NoError(t, err)
	updated, err := d.UpdateStack(ctx, tmpl("retain-after"), "retain-replace", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status)
	require.Len(t, updated.ResourceDeletions, 1)
	assert.Equal(t, "DELETE_SKIPPED", updated.ResourceDeletions[0].Status)
	assert.Equal(t, "UpdateReplacePolicy Retain", updated.ResourceDeletions[0].Reason)
	assert.Equal(t, http.StatusOK, headBucket(t, d, "retain-before"), "the old bucket is retained")
	assert.Equal(t, http.StatusOK, headBucket(t, d, "retain-after"))
}

// TestCFN_AFailedUpdateRestoresWhatItReplaced pins the rollback against the record the update
// started from: the replacement the failed update created is deleted, the server it replaced is the
// stack's again under its original ID, and a server it changed in place gets its old value back.
func TestCFN_AFailedUpdateRestoresWhatItReplaced(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const before = `{"Resources":{
		"Replaced":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],"Domain":"S3"}},
		"InPlace":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],
			"PreAuthenticationLoginBanner":"original"}}}}`
	const failing = `{"Resources":{
		"Replaced":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],"Domain":"EFS"}},
		"InPlace":{"Type":"AWS::Transfer::Server","Properties":{"Protocols":["SFTP"],
			"PreAuthenticationLoginBanner":"changed"}},
		"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}}}`
	created, err := d.Deploy(ctx, before, "restore", nil)
	require.NoError(t, err)
	replacedID := resourceByLogicalID(t, created, "Replaced").PhysicalID
	inPlaceID := resourceByLogicalID(t, created, "InPlace").PhysicalID

	result, err := d.UpdateStack(ctx, failing, "restore", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)
	assert.Equal(t, replacedID, resourceByLogicalID(t, result, "Replaced").PhysicalID,
		"the server the failed update replaced is the stack's again")
	assert.Equal(t, inPlaceID, resourceByLogicalID(t, result, "InPlace").PhysicalID)
	assert.Equal(t, 2, transferServerCount(t, d), "the failed update's replacement is deleted")
	require.Len(t, result.ResourceDeletions, 1)
	assert.Equal(t, "Replaced", result.ResourceDeletions[0].LogicalID)

	out := cfnStubDispatch(t, d, "transfer", "DescribeServer", map[string]any{"ServerId": inPlaceID})
	server, _ := out["Server"].(map[string]any)
	assert.Equal(t, "original", server["PreAuthenticationLoginBanner"],
		"the in-place change is applied back")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	for _, r := range stacks[0].Resources {
		if r.LogicalID == "Replaced" {
			assert.Equal(t, replacedID, r.PhysicalID, "and the record says so")
		}
	}
}
