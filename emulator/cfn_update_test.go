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

	// The sweep of the failed update's replacement is persisted, and is told apart from the
	// restored server by its physical ID: DescribeStackResources still reports that one complete.
	require.Len(t, stacks[0].ResourceDeletions, 1)
	swept := stacks[0].ResourceDeletions[0]
	assert.Equal(t, "Replaced", swept.LogicalID)
	assert.NotEqual(t, replacedID, swept.PhysicalID)
	assert.Equal(t, "DELETE_COMPLETE", swept.Status)
	for _, r := range stacks[0].Resources {
		status, _ := emulator.CFNResourceStatusForTest(stacks[0], r)
		assert.Equal(t, "CREATE_COMPLETE", status, "%s is the stack's", r.LogicalID)
	}
}

// TestCFN_ASweptRecordHoldsNoPhantoms pins that a deployment does not "keep" a resource its stack
// record names but a sweep removed. After ROLLBACK_COMPLETE every created resource is gone; after
// DELETE_FAILED the ones that deleted are gone and the one that failed is still there. A second
// in-process deploy of the stream recreates what is gone rather than reusing its ID.
func TestCFN_ASweptRecordHoldsNoPhantoms(t *testing.T) {
	t.Run("ROLLBACK_COMPLETE", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		ctx := context.Background()
		failed, err := d.Deploy(ctx, cfnRollbackTemplate, "phantom-rollback", nil)
		require.NoError(t, err)
		require.Equal(t, "ROLLBACK_COMPLETE", failed.Status)
		require.False(t, queueExists(t, d, "rollback-trailing"), "the rollback swept the queue")

		again, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "phantom-rollback", nil)
		require.NoError(t, err)
		assert.Equal(t, "CREATE_COMPLETE", again.Status, again.StatusReason)
		assert.True(t, queueExists(t, d, "rollback-trailing"), "the queue is created again, not kept")
		assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"))
	})
	t.Run("DELETE_FAILED", func(t *testing.T) {
		d, _, _, _ := newSweepDeployer(t)
		ctx := context.Background()
		const tmpl = `{"Resources":{
			"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"phantom-data"}},
			"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"phantom-queue"}}}}`
		_, err := d.Deploy(ctx, tmpl, "phantom-delete", nil)
		require.NoError(t, err)
		_, err = d.DispatchForTest(ctx, &emulator.AWSRequest{Service: "s3", Operation: "PUT",
			Path: "/phantom-data/held.txt", Body: []byte("x"), Headers: map[string]string{},
			Params: map[string]string{}}, "probe")
		require.NoError(t, err)
		require.ErrorIs(t, d.DeleteStack(ctx, "phantom-delete"), emulator.ErrCFNDeleteFailed)
		require.False(t, queueExists(t, d, "phantom-queue"))

		again, err := d.Deploy(ctx, tmpl, "phantom-delete", nil)
		require.NoError(t, err)
		assert.Equal(t, "CREATE_COMPLETE", again.Status, again.StatusReason)
		assert.True(t, queueExists(t, d, "phantom-queue"), "the deleted queue is created again")
	})
}

// TestCFN_AStackThatAdmitsNoUpdateIsRefused pins the status codes page's rule — ROLLBACK_COMPLETE:
// "only a delete operation can be performed"; DELETE_FAILED: "you can't work with or update the
// stack" — for UpdateStack and CreateChangeSet, in process and on the wire.
func TestCFN_AStackThatAdmitsNoUpdateIsRefused(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	failed, err := d.Deploy(ctx, cfnRollbackTemplate, "not-updatable", nil)
	require.NoError(t, err)
	require.Equal(t, "ROLLBACK_COMPLETE", failed.Status)

	_, err = d.UpdateStack(ctx, cfnRollbackCleanTemplate, "not-updatable", nil, emulator.CFNDeployOptions{})
	require.ErrorIs(t, err, emulator.ErrCFNStackNotUpdatable)
	assert.Contains(t, err.Error(), "is in ROLLBACK_COMPLETE state and can not be updated.")
	_, err = d.CreateChangeSet(ctx, "not-updatable", "cs", cfnRollbackCleanTemplate, nil, nil)
	require.ErrorIs(t, err, emulator.ErrCFNStackNotUpdatable)

	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{"StackName": "wire-rolled",
		"TemplateBody": `{"Resources":{"Refused":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}}}`})
	require.Equal(t, http.StatusOK, code, body)
	code, body = cfnAction(t, ts, "UpdateStack", map[string]string{"StackName": "wire-rolled",
		"TemplateBody": cfnBucketTemplate})
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Regexp(t, `Stack:arn:aws:cloudformation:us-east-1:\d{12}:stack/wire-rolled/[0-9a-f-]+ is in `+
		`ROLLBACK_COMPLETE state and can not be updated\.`, body)
}

// TestCFN_ARefusalForChangedInputsFailsTheResource pins that a re-create refused as already
// existing is cleared only when the resource's resolved inputs are identical. A named secret whose
// Description Refs a VPC the update re-creates under a new ID resolves differently; keeping its
// previous record would leave it naming the VPC the update deletes. So the refusal fails the
// secret, the update rolls back, and the original VPC is still the stack's and still exists.
//
// A VPC and a secret because both are still on the re-create path (#1565): the KMS key and IAM
// role this used before are updated in place as of #1545, so neither re-creates.
func TestCFN_ARefusalForChangedInputsFailsTheResource(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(cidr string) string {
		return `{"Resources":{
			"Net":{"Type":"AWS::EC2::VPC","Properties":{"CidrBlock":"` + cidr + `"}},
			"Secret":{"Type":"AWS::SecretsManager::Secret","Properties":{"Name":"refused-secret",
				"Description":{"Ref":"Net"}}}}}`
	}
	created, err := d.Deploy(ctx, tmpl("10.0.0.0/16"), "changed-inputs", nil)
	require.NoError(t, err)
	vpcID := resourceByLogicalID(t, created, "Net").PhysicalID

	result, err := d.UpdateStack(ctx, tmpl("10.1.0.0/16"), "changed-inputs", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
	assert.Contains(t, result.StatusReason, "Secret")
	assert.Contains(t, result.StatusReason, "ResourceExistsException")
	assert.Equal(t, vpcID, resourceByLogicalID(t, result, "Net").PhysicalID)

	var vpcs struct {
		IDs []string `xml:"vpcSet>item>vpcId"`
	}
	cfnDescribeEC2(t, d, map[string]string{
		"Action": "DescribeVpcs", "Filter.1.Name": "vpc-id", "Filter.1.Value.1": vpcID,
	}, &vpcs)
	assert.Equal(t, []string{vpcID}, vpcs.IDs, "the original VPC was not deleted")
}

// TestCFN_ARollbackOrdersAPreviousTemplateTolerantly pins that the rollback of a failed update
// converges on a previous template the request-level validation would refuse — one recorded
// before #1420, with a DependsOn naming a resource it does not declare.
func TestCFN_ARollbackOrdersAPreviousTemplateTolerantly(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	_, err := d.Deploy(ctx, cfnRollbackCleanTemplate, "legacy-order", nil)
	require.NoError(t, err)
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	legacy := stacks[0]
	legacy.TemplateBody = `{"Resources":{` +
		`"Kept":{"Type":"AWS::S3::Bucket","DependsOn":"Gone","Properties":{"BucketName":"rollback-kept"}},` +
		`"Trailing":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"rollback-trailing"}}}}`
	require.NoError(t, d.PersistStackForTest(ctx, legacy))

	result, err := d.UpdateStack(ctx, cfnRollbackTemplate, "legacy-order", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err, "the rollback is not refused for its previous template's DependsOn")
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status, result.StatusReason)
	assert.Equal(t, http.StatusOK, headBucket(t, d, "rollback-kept"))
	assert.True(t, queueExists(t, d, "rollback-trailing"))
}

// TestCFN_TransferCreateWithHostKeyCannotChange pins the resource page's rule: "If you change its
// value in a later stack update, the update fails and the server is left unchanged." The update rolls
// back, and no call reaches the server.
func TestCFN_TransferCreateWithHostKeyCannotChange(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(v string) string {
		return `{"Resources":{"Server":{"Type":"AWS::Transfer::Server","Properties":{
			"Protocols":["SFTP"],"CreateWithHostKey":` + v + `}}}}`
	}
	created, err := d.Deploy(ctx, tmpl("true"), "host-key", nil)
	require.NoError(t, err)
	id := resourceByLogicalID(t, created, "Server").PhysicalID
	before := len(sweepOperations(t, store, "host-key"))

	result, err := d.UpdateStack(ctx, tmpl("false"), "host-key", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
	assert.Contains(t, result.StatusReason, "CreateWithHostKey of AWS::Transfer::Server cannot be updated")
	assert.Equal(t, id, resourceByLogicalID(t, result, "Server").PhysicalID)
	ops := sweepOperations(t, store, "host-key")[before:]
	assert.Zero(t, countOps(ops, "UpdateServer")+countOps(ops, "CreateServer"),
		"the server is left unchanged: %v", ops)
}

// TestCFN_UpdateReplacePolicyIsValidated pins that an UpdateReplacePolicy outside the page's three
// options — RetainExceptOnCreate is a DeletionPolicy value only — refuses the template at the
// request, before anything deploys.
func TestCFN_UpdateReplacePolicyIsValidated(t *testing.T) {
	for _, policy := range []string{"RetainExceptOnCreate", "Retian"} {
		t.Run(policy, func(t *testing.T) {
			d, _, _, store := newSweepDeployer(t)
			_, err := d.Deploy(context.Background(), `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",
				"UpdateReplacePolicy":"`+policy+`","Properties":{"BucketName":"urp-bucket"}}}}`, "urp", nil)
			require.ErrorIs(t, err, emulator.ErrCFNTemplateInvalid)
			assert.Contains(t, err.Error(), `Invalid UpdateReplacePolicy "`+policy+`" for resource Data`)
			assert.Empty(t, sweepOperations(t, store, "urp"))
		})
	}
	d, _, _, _ := newSweepDeployer(t)
	for _, policy := range []string{"Delete", "Retain", "Snapshot"} {
		_, err := d.Deploy(context.Background(), `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",
			"UpdateReplacePolicy":"`+policy+`","Properties":{"BucketName":"urp-ok"}}}}`, "urp-ok", nil)
		require.NoError(t, err, policy)
	}
}
