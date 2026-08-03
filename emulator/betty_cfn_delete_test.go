package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The templates the sweep tests deploy. They are declared here rather than inline
// because several tests deploy the same stack and assert different halves of the
// outcome, and a divergence between two copies of a template would look like a
// divergence in the sweep.
const (
	// cfnSweepBucketTemplate is #518's own reproduction: one bucket in one stack.
	cfnSweepBucketTemplate = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"sweep-data"}}}}`

	// cfnSweepRetainTemplate declares two buckets, one retained. A template with
	// both is what proves the policy is read per resource rather than per stack.
	cfnSweepRetainTemplate = `{"Resources":{` +
		`"Gone":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"sweep-gone"}},` +
		`"Kept":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain",` +
		`"Properties":{"BucketName":"sweep-kept"}}}}`

	// cfnSweepOrderedTemplate spans four type priorities so the reverse ordering is
	// observable: IAM::Role is 1, S3::Bucket is 2, and SQS::Queue and SNS::Topic are
	// both 3 — the tie is what exercises the descending logical-ID tiebreak.
	cfnSweepOrderedTemplate = `{"Resources":{` +
		`"Role":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"sweep-role",` +
		`"AssumeRolePolicyDocument":{"Statement":[]}}},` +
		`"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"sweep-ordered"}},` +
		`"Queue":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"sweep-queue"}},` +
		`"Topic":{"Type":"AWS::SNS::Topic","Properties":{"TopicName":"sweep-topic"}}}}`
)

// newSweepDeployer builds a deployer over the full default plugin set with the
// event store recording, and returns it with the state manager, the object mirror
// and the store.
//
// It does not reuse newTestDeployer: that helper registers six plugins by hand, and
// a sweep dispatches into whichever service the template names — so a test asserting
// "the resource is gone" against a registry missing that service would be asserting
// ServiceNotAvailable. RegisterDefaultPlugins is also what the server uses, which is
// the configuration the issue was filed against.
//
// The event store matters as much: the reverse-order assertion reads the recorded
// operations, so recording has to be on. It is on in DefaultConfig, and stating that
// here is cheaper than discovering later that the ordering test asserted nothing.
func newSweepDeployer(t *testing.T) (*emulator.StackDeployer,
	*emulator.MemoryStateManager, afero.Fs, *emulator.EventStore) {
	t.Helper()
	ctx := context.Background()
	cfg := emulator.DefaultConfig()
	require.True(t, cfg.EventStore.Enabled,
		"the ordering assertion reads recorded events, so recording must be on")

	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	registry := emulator.NewPluginRegistry()
	require.NoError(t, emulator.RegisterDefaultPlugins(ctx, registry, state, tc, logger,
		store, nil))

	// The S3 plugin RegisterDefaultPlugins built owns its own in-memory mirror, and
	// a HEAD goes through the plugin rather than the filesystem, so the mirror is
	// returned only for the tests that inspect object bodies.
	fs := afero.NewMemMapFs()
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})
	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs),
		state, fs, store
}

// headBucket reports the status a HeadBucket against name returns, which is the
// observable #518 is about: "head-bucket → 404, was 200".
//
// The error the dispatch returns is deliberately ignored: a 404 is reported as one,
// and a 404 is the answer these tests are looking for. The status on the response is
// the observable either way, so requiring no error would make the probe fail on
// exactly the outcome it exists to detect.
func headBucket(t *testing.T, d *emulator.StackDeployer, name string) int {
	t.Helper()
	resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "s3", Operation: "HEAD", Path: "/" + name,
		Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	require.NotNil(t, resp, "the probe must reach the plugin")
	return resp.StatusCode
}

// sweepOperations returns the operations recorded for a stream, in order. The
// sweep's ordering is only observable through this: the resources vanish either
// way, and the order they vanished in is recorded nowhere else.
func sweepOperations(t *testing.T, store *emulator.EventStore, streamID string) []string {
	t.Helper()
	events, err := store.GetEvents(context.Background(),
		emulator.EventFilter{StreamID: streamID})
	require.NoError(t, err)
	ops := make([]string, 0, len(events))
	for _, ev := range events {
		ops = append(ops, ev.Operation)
	}
	return ops
}

// TestCFN_DeleteStackDeletesItsResources is #518's own reproduction. Before the
// sweep, DeleteStack removed the stack record and the bucket stayed: a test that
// tore its stack down and asserted cleanliness passed for the wrong reason.
func TestCFN_DeleteStackDeletesItsResources(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepBucketTemplate, "sweep", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, headBucket(t, d, "sweep-data"),
		"the deploy must have created the bucket, or the delete proves nothing")

	require.NoError(t, d.DeleteStack(ctx, "sweep"))

	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "sweep-data"),
		"the bucket dies with its stack")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks, "a fully swept stack removes its own record")
}

// TestCFN_DeletionPolicyRetainKeepsTheResource pins both halves of Retain: the
// retained resource survives and the stack still goes away.
//
// Only asserting the first half would pass on an implementation that refused the
// whole delete, which is the opposite of what Retain means — "keeps the resource,
// removing it from the stack's scope only".
func TestCFN_DeletionPolicyRetainKeepsTheResource(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepRetainTemplate, "retained", nil)
	require.NoError(t, err)

	require.NoError(t, d.DeleteStack(ctx, "retained"))

	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "sweep-gone"),
		"the unmarked bucket defaults to Delete")
	assert.Equal(t, http.StatusOK, headBucket(t, d, "sweep-kept"),
		"DeletionPolicy Retain keeps the bucket")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks,
		"a retained resource is not a failed delete: the stack still goes")
}

// TestCFN_SweepOrderIsTheInverseOfDeploy is the ordering gate, and it asserts the
// recorded operation *sequence* rather than set membership.
//
// The priorities encode real dependencies — a subnet cannot go before the instances
// in it — so a sweep in any other order is a sweep that happens to work on templates
// whose resources are independent. Reversing the same map Deploy sorts by is what
// makes the teardown order consistent with the build order rather than merely
// plausible, and the recorded events are the only place that is observable.
func TestCFN_SweepOrderIsTheInverseOfDeploy(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepOrderedTemplate, "ordered", nil)
	require.NoError(t, err)

	created := sweepOperations(t, store, "ordered")
	require.Equal(t, []string{"CreateRole", "CreateBucket", "CreateQueue", "CreateTopic"},
		created, "ascending priority, ties by ascending logical ID")

	require.NoError(t, d.DeleteStack(ctx, "ordered"))

	all := sweepOperations(t, store, "ordered")
	deleted := all[len(created):]
	assert.Equal(t, []string{"DeleteTopic", "DeleteQueue", "DeleteBucket", "DeleteRole"},
		deleted, "the sweep is the exact inverse: descending priority, ties descending")
}

// TestCFN_SweepToleratesAResourceDeletedOutOfBand pins that a not-found is a
// success for a sweep.
//
// A sweep's goal is that the resource not exist, and a resource someone deleted by
// hand has met it. Treating not-found as a failure would wedge the stack in
// DELETE_FAILED permanently on a condition no caller can fix — the resource cannot
// be un-deleted.
func TestCFN_SweepToleratesAResourceDeletedOutOfBand(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepRetainTemplate, "outofband", nil)
	require.NoError(t, err)

	// Delete one of the buckets behind the stack's back, exactly as a caller
	// reaching for the S3 API directly would.
	resp, err := d.DispatchForTest(ctx, &emulator.AWSRequest{
		Service: "s3", Operation: "DELETE", Path: "/sweep-gone",
		Headers: map[string]string{}, Params: map[string]string{},
	}, "outofband")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	assert.NoError(t, d.DeleteStack(ctx, "outofband"),
		"an already-absent resource is not a failed delete")

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks, "so the stack record still goes")
}

// TestCFN_AFailedDeleteRetainsTheStack pins the retention rule: a stack that
// reports DELETE_FAILED stays visible, with the failing resource named.
//
// A stack that reports a failed delete and then vanishes from DescribeStacks is a
// worse lie than the leak: the caller has no way to retry and no way to learn which
// resource is holding it. The failure here is real rather than injected — a bucket
// holding an object the stack did not create is refused by DeleteBucket, which is
// what CloudFormation does too.
func TestCFN_AFailedDeleteRetainsTheStack(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepBucketTemplate, "wedged", nil)
	require.NoError(t, err)

	resp, err := d.DispatchForTest(ctx, &emulator.AWSRequest{
		Service: "s3", Operation: "PUT", Path: "/sweep-data/left-behind.txt",
		Body: []byte("not the stack's"), Headers: map[string]string{},
		Params: map[string]string{},
	}, "wedged")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	deleteErr := d.DeleteStack(ctx, "wedged")
	require.Error(t, deleteErr, "a non-empty bucket cannot be deleted")
	assert.ErrorIs(t, deleteErr, emulator.ErrCFNDeleteFailed)
	assert.Contains(t, deleteErr.Error(), "Data",
		"the error names the resource holding the stack")

	stacks, err := d.ListStacks(ctx)
	require.Len(t, stacks, 1, "the stack stays visible so the delete can be retried")
	require.NoError(t, err)
	assert.Equal(t, "DELETE_FAILED", stacks[0].Status)
	require.Len(t, stacks[0].ResourceDeletions, 1)
	assert.Equal(t, "Data", stacks[0].ResourceDeletions[0].LogicalID)
	assert.Equal(t, "DELETE_FAILED", stacks[0].ResourceDeletions[0].Status)
	assert.Contains(t, stacks[0].ResourceDeletions[0].Reason, "BucketNotEmpty",
		"the reason is the plugin's own refusal, not a paraphrase")

	assert.Equal(t, http.StatusOK, headBucket(t, d, "sweep-data"),
		"and the bucket is still there, which is why the delete failed")
}

// TestCFN_AnUnmodeledTypeIsReportedAsSkipped pins the difference between a known
// gap and a false claim of cleanliness.
//
// A type substrate does not deploy through any real API has nothing to delete, and
// the sweep says so — naming the type — rather than reporting DELETE_COMPLETE for a
// resource it never touched. The stack still deletes: an unmodeled type is not a
// failure, it is a stated absence.
func TestCFN_AnUnmodeledTypeIsReportedAsSkipped(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	// AWS::ECR::LifecyclePolicy is in the table's inert set: ECR routes no
	// DeleteLifecyclePolicy, so there is nothing to dispatch.
	tmpl := `{"Resources":{` +
		`"Repo":{"Type":"AWS::ECR::Repository","Properties":{"RepositoryName":"sweep-repo"}},` +
		`"Policy":{"Type":"AWS::ECR::LifecyclePolicy","Properties":{` +
		`"RepositoryName":"sweep-repo","LifecyclePolicyText":"{}"}}}}`
	_, err := d.Deploy(ctx, tmpl, "gapped", nil)
	require.NoError(t, err)

	deletions := d.DeleteStackResourcesForTest(ctx, "gapped")
	byID := map[string]emulator.CFNResourceDeletion{}
	for _, del := range deletions {
		byID[del.LogicalID] = del
	}
	require.Contains(t, byID, "Policy")
	assert.Equal(t, "DELETE_SKIPPED", byID["Policy"].Status,
		"a type with no modeled delete is skipped, not silently completed")
	assert.Contains(t, byID["Policy"].Reason, "DeleteLifecyclePolicy",
		"and the reason names what is missing")
	assert.Equal(t, "DELETE_COMPLETE", byID["Repo"].Status,
		"the repository next to it still deletes")

	// A skip is not a failure, so the record still goes.
	require.NoError(t, d.DeleteStack(ctx, "gapped"))
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	assert.Empty(t, stacks)
}

// TestCFN_AStateOnlyTypeLosesItsStubKey pins the leak the stub set exists to close.
//
// A state-only resource writes its properties into the cfn_stub namespace and
// dispatches nothing. A sweep that dispatched nothing for it would leave that key
// behind, and a stack redeployed under the same logical ID would read the previous
// stack's properties — the same "recreated resource inherits the old one's
// configuration" shape #508 fixed inside S3.
func TestCFN_AStateOnlyTypeLosesItsStubKey(t *testing.T) {
	d, state, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	tmpl := `{"Resources":{"ACL":{"Type":"AWS::WAFv2::WebACL","Properties":{` +
		`"Name":"sweep-acl","Scope":"REGIONAL"}}}}`
	_, err := d.Deploy(ctx, tmpl, "stubbed", nil)
	require.NoError(t, err)

	keys, err := state.List(ctx, "cfn_stub", "")
	require.NoError(t, err)
	require.Contains(t, keys, "123456789012/us-east-1/ACL",
		"the deploy records the properties, which is the state that can leak")

	require.NoError(t, d.DeleteStack(ctx, "stubbed"))

	keys, err = state.List(ctx, "cfn_stub", "")
	require.NoError(t, err)
	assert.NotContains(t, keys, "123456789012/us-east-1/ACL",
		"and the sweep removes them, so a redeploy starts clean")
}

// TestCFN_AnInvalidDeletionPolicyFailsTheDelete pins the reason DeletionPolicy is
// validated rather than defaulted.
//
// A typo'd "Retian" that silently fell back to Delete would destroy the resource the
// attribute was written to protect — the one failure mode the attribute exists to
// prevent. CloudFormation rejects such a template outright; substrate refuses the
// delete and names the value, which keeps the resource.
func TestCFN_AnInvalidDeletionPolicyFailsTheDelete(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	tmpl := `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"DeletionPolicy":"Retian","Properties":{"BucketName":"sweep-typo"}}}}`
	_, err := d.Deploy(ctx, tmpl, "typo", nil)
	require.NoError(t, err)

	deleteErr := d.DeleteStack(ctx, "typo")
	require.Error(t, deleteErr)
	assert.ErrorIs(t, deleteErr, emulator.ErrCFNDeleteFailed)
	assert.Contains(t, deleteErr.Error(), `invalid DeletionPolicy "Retian"`)

	assert.Equal(t, http.StatusOK, headBucket(t, d, "sweep-typo"),
		"the bucket is kept rather than deleted on a guess")
}

// TestCFN_SnapshotDeletesAndSaysNoSnapshotWasTaken pins the honest answer for a
// policy substrate does not model.
//
// Snapshot does not retain — it snapshots, then deletes — so reporting the resource
// as retained would be wrong, and reporting DELETE_COMPLETE with no reason would
// imply a snapshot exists. The resource goes and the absence is stated.
func TestCFN_SnapshotDeletesAndSaysNoSnapshotWasTaken(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	// An S3 bucket does not support Snapshot at all, which is the case that has to
	// be distinguished from a type that does.
	tmpl := `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"DeletionPolicy":"Snapshot","Properties":{"BucketName":"sweep-snap"}}}}`
	_, err := d.Deploy(ctx, tmpl, "snap", nil)
	require.NoError(t, err)

	deletions := d.DeleteStackResourcesForTest(ctx, "snap")
	require.Len(t, deletions, 1)
	assert.Equal(t, "DELETE_COMPLETE", deletions[0].Status,
		"Snapshot deletes; it does not retain")
	assert.Contains(t, deletions[0].Reason, "not supported",
		"and the reason records that no snapshot exists")
	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "sweep-snap"))
}

// TestCFN_RDSDefaultsToSnapshotWithoutADeclaration pins the default the reference
// carves out: "the default policy is Snapshot for AWS::RDS::DBCluster resources and
// for AWS::RDS::DBInstance resources that don't specify the DBClusterIdentifier
// property".
//
// The observable is the reason, not the outcome: Snapshot and Delete both delete, so
// a sweep that resolved the default as Delete would look identical except that it
// would not record a snapshot was expected. An instance inside a cluster follows the
// cluster's fate, so its own default is Delete — asserted here so the property test
// cannot quietly become a type test.
func TestCFN_RDSDefaultsToSnapshotWithoutADeclaration(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	tmpl := `{"Resources":{` +
		`"Standalone":{"Type":"AWS::RDS::DBInstance","Properties":{` +
		`"DBInstanceIdentifier":"sweep-solo","DBInstanceClass":"db.t3.micro",` +
		`"Engine":"postgres"}},` +
		`"Member":{"Type":"AWS::RDS::DBInstance","Properties":{` +
		`"DBInstanceIdentifier":"sweep-member","DBInstanceClass":"db.t3.micro",` +
		`"Engine":"aurora-postgresql","DBClusterIdentifier":"sweep-cluster"}}}}`
	_, err := d.Deploy(ctx, tmpl, "rds", nil)
	require.NoError(t, err)

	deletions := d.DeleteStackResourcesForTest(ctx, "rds")
	byID := map[string]emulator.CFNResourceDeletion{}
	for _, del := range deletions {
		byID[del.LogicalID] = del
	}
	require.Contains(t, byID, "Standalone")
	require.Contains(t, byID, "Member")
	assert.Contains(t, byID["Standalone"].Reason, "Snapshot",
		"an instance outside a cluster defaults to Snapshot")
	assert.Empty(t, byID["Member"].Reason,
		"an instance inside a cluster defaults to Delete, so there is nothing to note")
}

// TestCFN_ARefusedResourceIsNotAFailedDelete pins the interaction with #516: a
// resource whose create was refused has no physical resource behind it, so the
// sweep has nothing to delete and must not report a failure.
//
// This is also what keeps a create rollback honest — the resource that failed the
// deploy is the one thing the rollback definitely cannot delete.
func TestCFN_ARefusedResourceIsNotAFailedDelete(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	// An uppercase bucket name is refused with InvalidBucketName.
	tmpl := `{"Resources":{"Bad":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"Sweep_Invalid_Name"}},` +
		`"Good":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"sweep-ok"}}}}`
	result, err := d.Deploy(ctx, tmpl, "refused", nil)
	require.NoError(t, err)
	var refused bool
	for _, r := range result.Resources {
		if r.LogicalID == "Bad" && r.Error != "" {
			refused = true
		}
	}
	require.True(t, refused, "the bucket must have been refused, or this proves nothing")

	deletions := d.DeleteStackResourcesForTest(ctx, "refused")
	byID := map[string]emulator.CFNResourceDeletion{}
	for _, del := range deletions {
		byID[del.LogicalID] = del
	}
	assert.Equal(t, "DELETE_SKIPPED", byID["Bad"].Status)
	assert.Contains(t, byID["Bad"].Reason, "not created")
	assert.Equal(t, "DELETE_COMPLETE", byID["Good"].Status)

	assert.NoError(t, d.DeleteStack(ctx, "refused"),
		"a resource that never existed does not wedge the stack")
}

// TestCFN_DeletingAnAbsentStackSucceeds pins the boundary the sweep must not break:
// DeleteStack documents no not-found error, so deleting a stack that is not there
// sweeps nothing and succeeds.
func TestCFN_DeletingAnAbsentStackSucceeds(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	assert.NoError(t, d.DeleteStack(context.Background(), "never-deployed"))
}

// TestCFN_ASweepWhoseTemplateNoLongerParsesStillDeletes pins the fallback: a stack
// record whose template has become unparseable is not a reason to leave every
// resource behind.
//
// The template supplies deletion policies and the properties a nested delete needs,
// so without it the sweep deletes the types that need neither and skips the rest —
// which is strictly better than skipping all of them, and it is reported per
// resource either way.
func TestCFN_ASweepWhoseTemplateNoLongerParsesStillDeletes(t *testing.T) {
	d, state, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, cfnSweepBucketTemplate, "corrupt", nil)
	require.NoError(t, err)

	// Rewrite the persisted record with a template body that parses as neither JSON
	// nor YAML, leaving the deployed resource list intact.
	data, err := state.Get(ctx, "cfn", "stack:corrupt")
	require.NoError(t, err)
	var stack emulator.CFNStackState
	require.NoError(t, json.Unmarshal(data, &stack))
	stack.TemplateBody = "{\"Resources\": [this is not a template"
	rewritten, err := json.Marshal(stack)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "cfn", "stack:corrupt", rewritten))

	require.NoError(t, d.DeleteStack(ctx, "corrupt"))
	assert.Equal(t, http.StatusNotFound, headBucket(t, d, "sweep-data"),
		"a bucket needs no template properties to delete, so it still goes")
}
